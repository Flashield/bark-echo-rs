//! The `status` command over the CLI seam (spec Testing Decisions): a
//! subprocess against a temp `HOME` in plain-secrets mode, with the real
//! embedded server behind it. The report's stdout is the contract: the
//! daemon's liveness, per-Subscription stream state, and the server's
//! live `/info` statistics.

mod common;

use std::time::{Duration, Instant};

/// Add one Subscription to this machine (`--own`), asserting it took and
/// returning the Device Key (the push target `add` prints).
async fn subscribe_own(harness: &common::Harness, server: &str) -> String {
    let output = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            server,
            "--own",
            "--label",
            "status-test",
        ])
        .await;
    let stdout = output.stdout_or_die("subscribe add");
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("device key: "))
        .expect("the device key is printed first")
        .trim()
        .to_owned()
}

/// Push one V2 JSON Push carrying `body` to `device_key`.
async fn push(server: &common::TestServer, device_key: &str, body: &str) {
    let response = reqwest::Client::new()
        .post(format!("{}/push", server.base_url))
        .json(&serde_json::json!({ "device_key": device_key, "body": body }))
        .send()
        .await
        .expect("POST /push");
    let status = response.status();
    let envelope: serde_json::Value = response.json().await.expect("push envelope");
    assert!(
        status.is_success() && envelope["code"] == 200,
        "push failed: {status} {envelope}"
    );
}

/// Poll `status` until `needle` shows up in its stdout.
async fn wait_for_status(harness: &common::Harness, needle: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let output = harness.run(&["status"]).await;
        assert!(
            output.success,
            "status must exit 0: {} {}",
            output.stderr, output.stdout
        );
        if output.stdout.contains(needle) {
            return output.stdout;
        }
        assert!(
            Instant::now() < deadline,
            "status never showed {needle:?}; last:\n{}",
            output.stdout
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

#[tokio::test]
async fn status_without_a_daemon_reports_not_running_with_the_server_stats() {
    let server = common::TestServer::spawn().await;
    let harness = common::Harness::new();
    let device_key = subscribe_own(&harness, &server.base_url).await;

    // Pushes with no daemon connected are retained for replay (§3.7) —
    // exactly what the pending count must show.
    push(&server, &device_key, "waiting for the desktop").await;
    push(&server, &device_key, "and another").await;

    let output = harness.run(&["status"]).await;
    assert!(
        output.success,
        "status must exit 0: {} {}",
        output.stderr, output.stdout
    );
    assert!(
        output.stdout.contains("daemon: not running"),
        "no daemon has run here: {}",
        output.stdout
    );
    assert!(
        output.stdout.contains("stream: unknown"),
        "no runtime state exists: {}",
        output.stdout
    );
    assert!(
        output.stdout.contains("pending_messages 2"),
        "the server holds both pushes for replay: {}",
        output.stdout
    );
    assert!(
        output.stdout.contains("active_streams 0"),
        "no stream is open: {}",
        output.stdout
    );

    server.shutdown().await;
}

#[tokio::test]
async fn status_follows_a_running_daemons_stream_state_and_shuts_down_clean() {
    let server = common::TestServer::spawn().await;
    let harness = common::Harness::new();
    let device_key = subscribe_own(&harness, &server.base_url).await;

    // The daemon runs as a real subprocess: it must connect on its own.
    let mut daemon = harness
        .spawn(&["--plain-secrets", "run"])
        .spawn()
        .expect("spawn run");

    let stdout = wait_for_status(&harness, "stream: running").await;
    assert!(
        stdout.contains("daemon: running"),
        "the lock is held by the subprocess: {stdout}"
    );
    assert!(
        stdout.contains("active_streams 1"),
        "the daemon's stream is open: {stdout}"
    );

    // A push arrives while the daemon runs — the server keeps it retained
    // (retention-aware pending count) and the daemon is the one mirroring.
    push(&server, &device_key, "live push").await;

    // Killing the daemon releases the lock: status flips back to
    // not running, and the server's stream count drops.
    daemon.kill().await.expect("kill daemon");
    let _ = daemon.wait().await;
    let stdout = wait_for_status(&harness, "daemon: not running").await;
    assert!(
        stdout.contains("active_streams 0"),
        "the killed daemon's stream is closed: {stdout}"
    );

    server.shutdown().await;
}
