//! `subscribe add/rm` over the CLI seam while a daemon holds the lock
//! (ticket #14, ADR-0004): the change rides the IPC channel and takes
//! effect without a restart, and the daemon's refusals surface
//! readably on stderr. Without a daemon, today's direct-write path
//! stands — cli.rs pins it.

mod common;

use std::time::{Duration, Instant};

/// The Subscription id `add` printed — the stdout line
/// `subscription {id} added (label: …)`.
fn printed_id(stdout: &str, what: &str) -> String {
    stdout
        .lines()
        .find_map(|line| {
            line.strip_prefix("subscription ")
                .and_then(|rest| rest.split_once(" added"))
                .map(|(id, _)| id.to_owned())
        })
        .unwrap_or_else(|| panic!("{what} prints the subscription id: {stdout}"))
}

/// Poll `status` until the daemon's line count settles on `n` — the
/// live answer only a channel-riding change can produce (the daemon
/// holds the lock; no restart ever happens here).
async fn wait_subscribed_lines(harness: &common::Harness, n: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let output = harness.run(&["--plain-secrets", "status"]).await;
        assert!(
            output.success,
            "status must exit 0: {} {}",
            output.stderr, output.stdout
        );
        if output.stdout.contains(&format!("subscription")) && count_lines(&output.stdout) == n {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "status never showed {n} Subscriptions; last:\n{}",
            output.stdout
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

/// How many `subscription …` lines the status report holds.
fn count_lines(stdout: &str) -> String {
    stdout
        .lines()
        .filter(|line| line.starts_with("subscription "))
        .count()
        .to_string()
}

#[tokio::test]
async fn subscribe_add_and_rm_ridethe_channel_while_a_daemon_holds_the_lock() {
    let server = common::TestServer::spawn().await;
    let harness = common::Harness::new();

    // The first Subscription goes through today's direct path — no
    // daemon holds the lock yet.
    let first = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--own",
            "--label",
            "first",
        ])
        .await;
    first.stdout_or_die("direct subscribe add");

    // The daemon starts and holds the lock.
    let mut daemon = harness
        .spawn(&["--plain-secrets", "run"])
        .spawn()
        .expect("spawn run");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let output = harness.run(&["--plain-secrets", "status"]).await;
        if output.stdout.contains("daemon: running") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the daemon never came up; last:\n{}",
            output.stdout
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    common::wait_active_streams(&server, 1).await;

    // The second add rides the channel: it takes effect without a
    // restart, and its answer reads like the direct path's.
    let second = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--own",
            "--label",
            "second",
        ])
        .await;
    let stdout = second.stdout_or_die("channel subscribe add");
    let second_key = stdout
        .lines()
        .find_map(|line| line.strip_prefix("device key: "))
        .expect("the channel add prints the Device Key")
        .trim()
        .to_owned();
    let second_id = printed_id(&stdout, "the channel add");
    // The Device Key is the real push target, minted by the daemon.
    let tokens = echo_core::server::ServerClient::new(&server.base_url)
        .list_stream_tokens(&second_key)
        .await
        .expect("list the second Device's tokens");
    assert_eq!(tokens[0].client_label, "second");

    // Hot: the daemon serves both streams now — no restart happened.
    common::wait_active_streams(&server, 2).await;
    wait_subscribed_lines(&harness, "2").await;

    // The remove rides the channel too: that stream stops immediately,
    // the other keeps running.
    let removed = harness
        .run(&["--plain-secrets", "subscribe", "rm", &second_id])
        .await;
    removed.stdout_or_die("channel subscribe rm");
    assert!(
        removed
            .stdout
            .contains(&format!("subscription {second_id} removed")),
        "the remove's answer reads like the direct path's: {}",
        removed.stdout
    );
    common::wait_active_streams(&server, 1).await;
    wait_subscribed_lines(&harness, "1").await;

    daemon.kill().await.expect("kill daemon");
    let _ = daemon.wait().await;
    server.shutdown().await;
}

#[tokio::test]
async fn a_refused_add_through_the_channel_is_actionable_on_stderr() {
    let server = common::TestServer::spawn().await;
    let harness = common::Harness::new();

    let mut daemon = harness
        .spawn(&["--plain-secrets", "run"])
        .spawn()
        .expect("spawn run");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let output = harness.run(&["--plain-secrets", "status"]).await;
        if output.stdout.contains("daemon: running") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the daemon never came up; last:\n{}",
            output.stdout
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // A bad Device Key: the daemon's refusal reaches the CLI's stderr
    // as the actionable words, and the command exits non-zero.
    let refused = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--device-key",
            "notARealDeviceKey0000",
            "--label",
            "doomed",
        ])
        .await;
    assert!(
        !refused.success,
        "a refused add exits non-zero: {} {}",
        refused.stderr, refused.stdout
    );
    assert!(
        refused.stderr.contains("device not found"),
        "the server's refusal surfaces: {}",
        refused.stderr
    );

    // And the daemon still holds: an unknown id on `rm` refuses the
    // same way.
    let unknown = harness
        .run(&["--plain-secrets", "subscribe", "rm", "0123abcdef456789"])
        .await;
    assert!(
        !unknown.success,
        "an unknown id must exit non-zero: {} {}",
        unknown.stderr, unknown.stdout
    );
    assert!(
        unknown.stderr.contains("subscribe list"),
        "the unknown-id error points at the list: {}",
        unknown.stderr
    );

    daemon.kill().await.expect("kill daemon");
    let _ = daemon.wait().await;
    server.shutdown().await;
}
