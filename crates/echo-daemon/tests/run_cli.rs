//! The `run` command over the CLI seam (spec Testing Decisions): a
//! subprocess against a temp `HOME` in plain-secrets mode. The contract:
//! a daemon with nothing to mirror refuses with the actionable fix, a
//! second daemon refuses on the single-instance lock (ADR-0002), and a
//! started daemon holds its Realtime Stream until killed.

mod common;

use std::time::{Duration, Instant};

/// Poll `/info` until `active_streams` is `n`.
async fn wait_active_streams(server: &common::TestServer, n: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let info: serde_json::Value = reqwest::Client::new()
            .get(format!("{}/info", server.base_url))
            .send()
            .await
            .expect("GET /info")
            .json()
            .await
            .expect("/info json");
        if info["active_streams"] == n {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "active_streams never reached {n}: {info}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn run_without_subscriptions_fails_with_the_actionable_fix() {
    let harness = common::Harness::new();
    let output = harness.run(&["--plain-secrets", "run"]).await;
    assert!(
        !output.success,
        "a daemon with nothing to mirror must not start"
    );
    assert!(
        output.stderr.contains("subscribe add"),
        "the error points at the fix: {}",
        output.stderr
    );
}

#[tokio::test]
async fn run_refuses_to_start_while_another_instance_holds_the_lock() {
    let server = common::TestServer::spawn().await;
    let harness = common::Harness::new();
    let added = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--own",
            "--label",
            "run-test",
        ])
        .await;
    added.stdout_or_die("subscribe add");

    // Another live process (here: the test itself) holds the lock the
    // subprocess is about to claim.
    let _other =
        echo_daemon::lock::DaemonLock::acquire(harness.paths().lock_file()).expect("lock taken");

    let output = harness.run(&["--plain-secrets", "run"]).await;
    assert!(
        !output.success,
        "a contested lock must refuse to start; stdout: {}",
        output.stdout
    );
    assert!(
        output.stderr.contains("already running"),
        "the error names the single-instance rule: {}",
        output.stderr
    );
    assert!(
        output.stderr.contains("daemon.lock"),
        "the error names where the lock lives: {}",
        output.stderr
    );

    server.shutdown().await;
}

#[tokio::test]
async fn run_holds_its_stream_until_killed() {
    let server = common::TestServer::spawn().await;
    let harness = common::Harness::new();
    let added = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--own",
            "--label",
            "run-test",
        ])
        .await;
    added.stdout_or_die("subscribe add");

    // The daemon runs as a real subprocess and must connect on its own.
    let mut daemon = harness
        .spawn(&["--plain-secrets", "run"])
        .spawn()
        .expect("spawn run");
    wait_active_streams(&server, 1).await;

    // Killing the daemon closes the stream: the OS releases the lock the
    // process held, so the next run can start without cleanup.
    daemon.kill().await.expect("kill daemon");
    let _ = daemon.wait().await;
    wait_active_streams(&server, 0).await;
    let next = harness.run(&["--plain-secrets", "subscribe", "list"]).await;
    assert!(
        next.success,
        "the config is still intact after the kill: {} {}",
        next.stderr, next.stdout
    );

    server.shutdown().await;
}
