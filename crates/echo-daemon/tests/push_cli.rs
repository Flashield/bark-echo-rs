//! The `push` subcommand against the embedded real server: the CLI is the
//! send module's first consumer (issue #6) — its flags map onto the V2
//! JSON Push (protocol §2.2), delivery is observed on the target Device's
//! Realtime Stream fan-out (§3.4), and failures exit non-zero. Every run
//! rides the test convention: a temp `HOME` with `--plain-secrets`, so no
//! run touches a real keyring or a real config dir (ticket #10 — these
//! tests predated the convention and probed the CI runner's keyless
//! keyring instead).

mod common;

use common::{Harness, sse::SseStream};
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
    let harness = Harness::new();

    let run = harness
        .run(&[
            "--plain-secrets",
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
        .await;

    assert!(run.success, "push must exit 0, stderr: {}", run.stderr);
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
    let harness = Harness::new();

    // Extra flags pass through as Ext Params exactly like the server's
    // V2 JSON route (§2.2) — the CLI neither names nor filters them.
    let run = harness
        .run(&[
            "--plain-secrets",
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
        .await;

    assert!(run.success, "push must exit 0, stderr: {}", run.stderr);
    let event = stream.next_push_event().await;
    let ext = &event["ext_params"];
    assert_eq!(ext["url"], "https://example.com/boarding", "url: {event}");
    assert_eq!(ext["group"], "trips", "group: {event}");
    assert_eq!(ext["level"], "timeSensitive", "level: {event}");
    assert_eq!(ext["call"], "1", "call: {event}");

    server.shutdown().await;
}

#[tokio::test]
async fn cli_unknown_device_key_reports_the_server_message_and_exits_non_zero() {
    let server = common::TestServer::spawn().await;
    // A never-registered 22-char key (same shape as the unknown-key
    // probes elsewhere).
    let unknown_key = "zzzzzzzzzzzzzzzzzzzzzz";
    let harness = Harness::new();

    let run = harness
        .run(&[
            "--plain-secrets",
            "push",
            "--server",
            &server.base_url,
            "--device-key",
            unknown_key,
            "--body",
            "anyone?",
        ])
        .await;

    assert!(!run.success, "an unknown Device Key must exit non-zero");
    // The server's error message and code surface verbatim (§2.2:
    // unknown device_key quotes the key back). The plain-file store in
    // the temp home answers NotFound cleanly, so no store noise rides
    // along.
    assert_eq!(
        run.stderr.trim(),
        "push rejected by the bark server (code 400): failed to get device token: failed to get [zzzzzzzzzzzzzzzzzzzzzz] device token from database",
        "the server's message surfaces verbatim: {}",
        run.stderr
    );

    server.shutdown().await;
}

#[tokio::test]
async fn cli_basic_auth_credentials_are_sent_and_wrong_ones_exit_non_zero() {
    let server = common::TestServer::spawn_with_basic_auth("operator", "hunter2").await;
    // /register is Basic-Auth-exempt (protocol §1.4), so setup needs no
    // credentials; /push is not.
    let device = ServerClient::new(&server.base_url)
        .register_stream_device()
        .await
        .expect("register stream device");
    let harness = Harness::new();

    // With the operator's credentials the push is accepted.
    let run = harness
        .run(&[
            "--plain-secrets",
            "push",
            "--server",
            &server.base_url,
            "--device-key",
            &device.device_key,
            "--body",
            "behind auth",
            "--user",
            "operator",
            "--password",
            "hunter2",
        ])
        .await;
    assert!(
        run.success,
        "correct credentials must be accepted, stderr: {}",
        run.stderr
    );

    // Wrong password: the server answers 418 Teapot (§1.4), non-zero exit.
    let run = harness
        .run(&[
            "--plain-secrets",
            "push",
            "--server",
            &server.base_url,
            "--device-key",
            &device.device_key,
            "--body",
            "behind auth",
            "--user",
            "operator",
            "--password",
            "wrong-password",
        ])
        .await;
    assert!(!run.success, "wrong credentials must exit non-zero");
    assert!(
        run.stderr.contains("418"),
        "the 418 Teapot refusal surfaces: {}",
        run.stderr
    );

    server.shutdown().await;
}

#[tokio::test]
async fn push_survives_a_headless_machine_without_a_secret_service() {
    // The CI shape: no secret service answers, the push carries no auth
    // flags, the server is open — the stored credentials are irrelevant,
    // so the unreachable store degrades to a one-line note instead of
    // failing the push. The credential-required seams (the daemon's
    // streams, the token plane) keep the actionable error; see
    // tests/run.rs and tests/basic_auth_store.rs for their pins.
    let server = common::TestServer::spawn().await;
    let device = ServerClient::new(&server.base_url)
        .register_stream_device()
        .await
        .expect("register stream device");
    let harness = Harness::new();

    let output = harness
        .spawn(&[
            "push",
            "--server",
            &server.base_url,
            "--device-key",
            &device.device_key,
            "--title",
            "Headless",
            "--body",
            "no keyring here",
        ])
        // Headless simulation: no reachable session bus. Dropping
        // XDG_RUNTIME_DIR alone is not enough — zbus falls back to
        // /run/user/<uid>/bus, which exists on developer machines — so
        // the session bus address also points at a dead socket, pinning
        // the store-unreachable condition on every Linux box.
        .env_remove("XDG_RUNTIME_DIR")
        .env(
            "DBUS_SESSION_BUS_ADDRESS",
            "unix:path=/nonexistent/bark-echo-ci-bus",
        )
        .output()
        .await
        .expect("spawn bark-echo push");

    assert!(
        output.status.success(),
        "push must survive an unreachable secret store, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    #[cfg(target_os = "linux")]
    let stderr = String::from_utf8_lossy(&output.stderr);
    // The degradation is visible, not silent: one note line naming the
    // store and both fixes. (Only where the store truly cannot answer —
    // the simulation guarantees that on Linux; macOS CI reaches its
    // native keychain and takes the plain no-stored-credentials path.)
    #[cfg(target_os = "linux")]
    {
        assert!(
            stderr.contains("secret store unavailable")
                && stderr.contains("--plain-secrets")
                && stderr.lines().count() == 1,
            "the one-line note must name the store and the fallback: {stderr}"
        );
    }

    server.shutdown().await;
}
