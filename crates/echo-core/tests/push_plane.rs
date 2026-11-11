//! The Push send module against the embedded real server: pushing over
//! the V2 JSON route (protocol §2.2) and observing delivery on the
//! target Device's Realtime Stream fan-out (§3.4).

mod common;

use common::sse::SseStream;
use echo_core::push::{Push, PushClient};
use echo_core::server::ServerClient;

#[tokio::test]
async fn pushing_to_a_stream_device_delivers_on_its_realtime_stream() {
    let server = common::TestServer::spawn().await;
    let server_client = ServerClient::new(&server.base_url);
    let device = server_client
        .register_stream_device()
        .await
        .expect("register stream device");
    let token = server_client
        .mint_stream_token(&device.device_key, "push-test-reader")
        .await
        .expect("mint stream token");
    let mut stream = SseStream::open(&server, &token.secret).await;

    PushClient::new(&server.base_url)
        .send(
            &Push::to(&device.device_key)
                .title("Flight")
                .subtitle("SFO")
                .body("Gate 7"),
        )
        .await
        .expect("push accepted");

    let event = stream.next_push_event().await;
    assert_eq!(event["title"], "Flight", "title rides through: {event}");
    assert_eq!(event["subtitle"], "SFO", "subtitle rides through: {event}");
    assert_eq!(event["body"], "Gate 7", "body rides through: {event}");
    assert_eq!(
        event["device_key"], device.device_key,
        "the event is for the pushed Device: {event}"
    );

    server.shutdown().await;
}
