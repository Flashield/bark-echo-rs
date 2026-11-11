//! The Push send module against the embedded real server: pushing over
//! the V2 JSON route (protocol §2.2) and observing delivery on the
//! target Device's Realtime Stream fan-out (§3.4).

mod common;

use common::sse::SseStream;
use echo_core::push::{Push, PushClient, PushError};
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

#[tokio::test]
async fn ext_params_ride_through_into_the_received_event() {
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
                .body("Boarding")
                .ext_param("url", "https://example.com/boarding")
                .ext_param("group", "trips")
                .ext_param("level", "timeSensitive")
                .ext_param("call", "1"),
        )
        .await
        .expect("push accepted");

    let event = stream.next_push_event().await;
    // Ext Params appear in the fan-out event's ext_params (§3.4) —
    // unrecognized params as-is, `group` dual-written by the server.
    let ext = &event["ext_params"];
    assert_eq!(ext["url"], "https://example.com/boarding", "url: {event}");
    assert_eq!(ext["group"], "trips", "group: {event}");
    assert_eq!(ext["level"], "timeSensitive", "level: {event}");
    assert_eq!(ext["call"], "1", "call: {event}");

    server.shutdown().await;
}

#[tokio::test]
async fn pushing_to_an_unknown_device_key_reports_the_server_message_and_code() {
    let server = common::TestServer::spawn().await;
    let client = PushClient::new(&server.base_url);
    // A never-registered 22-char key (same shape as the Device/Token
    // plane's unknown-key probes).
    let unknown_key = "zzzzzzzzzzzzzzzzzzzzzz";

    let err = client
        .send(&Push::to(unknown_key).body("anyone?"))
        .await
        .expect_err("unknown key must fail");

    // The server's message rides verbatim with its code (protocol §2.2:
    // unknown device_key quotes the key back).
    let PushError::Server { code, message } = &err else {
        panic!("wrong error face: {err}")
    };
    assert_eq!(*code, 400, "code mirrors the HTTP status: {err}");
    assert_eq!(
        message,
        "failed to get device token: failed to get [zzzzzzzzzzzzzzzzzzzzzz] device token from database",
        "the server's message surfaces verbatim"
    );

    server.shutdown().await;
}
