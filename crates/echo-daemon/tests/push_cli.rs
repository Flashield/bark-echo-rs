//! The `push` subcommand against the embedded real server: the CLI is the
//! send module's first consumer (issue #6) — its flags map onto the V2
//! JSON Push (protocol §2.2), delivery is observed on the target Device's
//! Realtime Stream fan-out (§3.4), and failures exit non-zero.

mod common;

// The child wait must be async: the embedded server shares this test's
// tokio runtime, and a blocking `std::process::Command::output` on the
// current_thread runtime would starve it mid-request.
use tokio::process::Command;

use common::sse::SseStream;
use echo_core::server::ServerClient;

#[tokio::test]
async fn pushing_from_the_cli_delivers_on_the_device_realtime_stream() {
    let server = common::TestServer::spawn().await;
    let server_client = ServerClient::new(&server.base_url);
    let device = server_client
        .register_stream_device()
        .await
        .expect("register stream device");
    let token = server_client
        .mint_stream_token(&device.device_key, "push-cli-reader")
        .await
        .expect("mint stream token");
    let mut stream = SseStream::open(&server, &token.secret).await;

    let output = Command::new(env!("CARGO_BIN_EXE_echo-daemon"))
        .args([
            "push",
            "--server",
            &server.base_url,
            "--device-key",
            &device.device_key,
            "--title",
            "Flight",
            "--subtitle",
            "SFO",
            "--body",
            "Gate 7",
        ])
        .output()
        .await
        .expect("spawn echo-daemon push");

    assert!(
        output.status.success(),
        "push must exit 0, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
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
async fn cli_extra_flags_ride_through_as_ext_params() {
    let server = common::TestServer::spawn().await;
    let server_client = ServerClient::new(&server.base_url);
    let device = server_client
        .register_stream_device()
        .await
        .expect("register stream device");
    let token = server_client
        .mint_stream_token(&device.device_key, "push-cli-reader")
        .await
        .expect("mint stream token");
    let mut stream = SseStream::open(&server, &token.secret).await;

    // Extra flags pass through as Ext Params exactly like the server's
    // V2 JSON route (§2.2) — the CLI neither names nor filters them.
    let output = Command::new(env!("CARGO_BIN_EXE_echo-daemon"))
        .args([
            "push",
            "--server",
            &server.base_url,
            "--device-key",
            &device.device_key,
            "--body",
            "Boarding",
            "--url",
            "https://example.com/boarding",
            "--group",
            "trips",
            "--level",
            "timeSensitive",
            "--call",
            "1",
        ])
        .output()
        .await
        .expect("spawn echo-daemon push");

    assert!(
        output.status.success(),
        "push must exit 0, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let event = stream.next_push_event().await;
    let ext = &event["ext_params"];
    assert_eq!(ext["url"], "https://example.com/boarding", "url: {event}");
    assert_eq!(ext["group"], "trips", "group: {event}");
    assert_eq!(ext["level"], "timeSensitive", "level: {event}");
    assert_eq!(ext["call"], "1", "call: {event}");

    server.shutdown().await;
}
