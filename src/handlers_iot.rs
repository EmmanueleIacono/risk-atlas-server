use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{Extension, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use futures_util::{StreamExt, SinkExt}; // for socket.split()
use rumqttc::{
    AsyncClient,
    Event,
    EventLoop,
    MqttOptions,
    Packet,
    QoS,
};
use std::{
    sync::Arc,
    time::Duration
};
use sqlx::Row;
use serde_json::Value;
use tokio::{sync::broadcast, time::sleep};

use crate::AppState;

pub async fn spawn_mqtt_listener_with_broadcast(
    host: &str,
    port: u16,
    client_id: &str,
    tx: Arc<broadcast::Sender<String>>,
) {
    // Configuring MQTT client to connect to MQTT broker
    let mut opts = MqttOptions::new(client_id, host, port);
    opts.set_keep_alive(Duration::from_secs(30));

    // Creating client & event loop
    let (client, mut event_loop): (AsyncClient, EventLoop) = AsyncClient::new(opts, 10); // 10 is capacity

    // Subscribing to all sensor topics (later could be refactored better)
    client
        .subscribe("sensors/#", QoS::AtLeastOnce)
        .await
        .expect("failed to subscribe");

    // Process events forever
    loop {
        match event_loop.poll().await {
            Ok(Event::Incoming(Packet::Publish(p))) => { // handling only Publish notifications
                let topic = p.topic.clone();
                let payload = String::from_utf8_lossy(&p.payload).to_string();
                println!("[MQTT] {} => {}", topic, payload);
                let _ = tx.send(payload);
            }
            Ok(_) => {} // ignoring all other successful notifications
            Err(err) => {
                eprintln!("[MQTT] error: {} - retrying in 1s...", err);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Extension(tx): Extension<Arc<broadcast::Sender<String>>>,
) -> impl IntoResponse {
    // subscribe to get a new Receiver for this client
    let rx = tx.subscribe();
    // on_upgrade hands control to "handle_ws"
    ws.on_upgrade(move |socket| handle_ws(socket, rx))
}

// WebSocket connection handler
async fn handle_ws(
    socket: WebSocket,
    mut rx: broadcast::Receiver<String>
) {
    // splitting the socket into sender and receiver
    let (mut sender, mut receiver) = socket.split();

    // if need to read incoming client messages
    tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(txt) = msg {
                println!("[WS client] {}", txt);
            }
        }
    });

    // forwarding each MQTT message (as JSON string) to the client
    while let Ok(json_payload) = rx.recv().await {
        let msg = Message::Text(json_payload.into());
        if sender.send(msg).await.is_err() {
            break; // client disconnected
        }
    }
}

pub async fn get_available_sensors(
    State(state): State<AppState>
) -> impl IntoResponse {
    let query = r#"SELECT sensor_id, name, description, lat, lon, ground_h, project_id, element_id
                         FROM iot.sensors
                         ORDER BY name"#;

    let rows = sqlx::query(query)
        .fetch_all(&state.pool)
        .await;

    match rows {
        Ok(rows) => {
            let result_json: Vec<Value> = rows.into_iter().map(|_row| {
                let sensor_id: String = _row.try_get("sensor_id").unwrap_or_default();
                let name: String = _row.try_get("name").unwrap_or_default();
                let description: String = _row.try_get("description").unwrap_or_default();
                let lat: f64 = _row.try_get("lat").unwrap_or(0.0);
                let lon: f64 = _row.try_get("lon").unwrap_or(0.0);
                let ground_h: f64 = _row.try_get("ground_h").unwrap_or(0.0);
                let project_id: Option<String> = _row.try_get("project_id").ok();
                let element_id: Option<String> = _row.try_get("element_id").ok();

                serde_json::json!({
                    "sensor_id": sensor_id,
                    "name": name,
                    "description": description,
                    "lat": lat,
                    "lon": lon,
                    "ground_h": ground_h,
                    "project_id": project_id,
                    "element_id": element_id,
                })
            })
            .collect();

            (StatusCode::OK, Json(result_json)).into_response()
        }
        Err(err) => {
            eprintln!("Error while fetching available sensors: {}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response()
        }
    }
}
