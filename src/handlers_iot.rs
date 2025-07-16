use rumqttc::{
    AsyncClient,
    Event,
    EventLoop,
    MqttOptions,
    Packet,
    QoS,
};
use std::time::Duration;
use tokio::time::sleep;

pub async fn spawn_mqtt_listener(host: &str, port: u16, client_id: &str) {
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
            Ok(notification) => {
                // different kinds of notifications could be matched
                // for simplicity, for now, just printing out publishes
                if let Event::Incoming(Packet::Publish(p)) = notification {
                    let topic = p.topic.clone();
                    let payload = String::from_utf8_lossy(&p.payload);
                    println!("[MQTT] {} => {}", topic, payload);
                }
            }
            Err(e) => {
                eprintln!("[MQTT] error: {} - retrying in 1s...", e);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
