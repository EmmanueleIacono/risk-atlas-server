use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::Extension,
    response::IntoResponse,
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
use tokio::{sync::broadcast, time::sleep};

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
