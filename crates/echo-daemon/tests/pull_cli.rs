//! The `pull` subcommand (ticket #7): pushes sent while no daemon was
//! running are recovered from the command line — one-shot replay landed
//! silently in the Subscription's History Log, the ingested count
//! reported. Driven against the embedded real server through the CLI's
//! own config/secrets resolution (temp `HOME`, plain-secrets mode).

mod common;

use common::Harness;
use echo_daemon::config::Config;

/// Push one V2 JSON Push carrying `body` (the daemon is not running —
/// the Push piles up in the server's store-and-forward store), with
/// operator Basic Auth credentials when the server demands them (§1.4).
async fn push(
    server: &common::TestServer,
    device_key: &str,
    body: &str,
    auth: Option<(&str, &str)>,
) {
    let mut request = reqwest::Client::new()
        .post(format!("{}/push", server.base_url))
        .json(&serde_json::json!({ "device_key": device_key, "body": body }));
    if let Some((user, password)) = auth {
        request = request.basic_auth(user, Some(password));
    }
    let response = request.send().await.expect("POST /push");
    let envelope: serde_json::Value = response.json().await.expect("push envelope");
    assert_eq!(envelope["code"], 200, "push accepted: {envelope}");
}

#[tokio::test]
async fn pull_after_offline_pushes_lands_the_history_log_and_reports_the_count() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();

    let stdout = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--own",
            "--label",
            "laptop",
        ])
        .await
        .stdout_or_die("subscribe add");
    let device_key = stdout
        .lines()
        .find_map(|line| line.strip_prefix("device key: "))
        .expect("add prints the Device Key")
        .to_owned();

    // No daemon running: the Pushes are retained for replay (§3.7).
    for body in ["first", "second", "third"] {
        push(&server, &device_key, body, None).await;
    }

    // One pull reports the ingested count.
    let run = harness.run(&["--plain-secrets", "pull"]).await;
    assert!(
        run.success && run.stdout.contains("3"),
        "pull reports three ingested, got: {}/{}",
        run.stderr,
        run.stdout
    );

    // The Subscription's History Log holds the three, in id order.
    let paths = harness.paths();
    let config = Config::load(&paths.config_file()).expect("config loads");
    let subscription = &config.subscriptions[0];
    let log = echo_core::history::HistoryLog::new(paths.history_log(&subscription.id));
    let entries = log.entries().expect("history entries");
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.body.as_str())
            .collect::<Vec<_>>(),
        vec!["first", "second", "third"],
        "the History Log reads in id order"
    );

    // A second pull adds nothing — the dedup path is shared with the
    // live loop (§3.7).
    let run = harness.run(&["--plain-secrets", "pull"]).await;
    assert!(
        run.success && run.stdout.contains("0"),
        "the second pull reports zero, got: {}/{}",
        run.stderr,
        run.stdout
    );
    assert_eq!(
        log.entries().expect("entries").len(),
        3,
        "no duplicates landed"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn pull_rides_the_stored_basic_auth_credentials() {
    // Behind operator Basic Auth (protocol §1.4) the credentials are laid
    // in once at add time — the password into the secret store (story
    // 18) — and the pull rides them with no flags at all.
    let server = common::TestServer::spawn_with_basic_auth("operator", "hunter2").await;
    let harness = Harness::new();

    let stdout = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--own",
            "--label",
            "laptop",
            "--auth-user",
            "operator",
            "--auth-password",
            "hunter2",
        ])
        .await
        .stdout_or_die("subscribe add behind Basic Auth");
    let device_key = stdout
        .lines()
        .find_map(|line| line.strip_prefix("device key: "))
        .expect("device key")
        .to_owned();

    // No daemon running: the Push is retained for replay (§3.7).
    push(
        &server,
        &device_key,
        "behind auth",
        Some(("operator", "hunter2")),
    )
    .await;

    let run = harness.run(&["--plain-secrets", "pull"]).await;
    assert!(
        run.success && run.stdout.contains("1"),
        "the pull rides the stored Basic Auth credentials, got: {}/{}",
        run.stderr,
        run.stdout
    );

    server.shutdown().await;
}

#[tokio::test]
async fn pull_resolves_the_sole_subscription_and_demands_a_label_when_ambiguous() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();

    // Nothing held: actionable, pointing at subscribe add.
    let run = harness.run(&["--plain-secrets", "pull"]).await;
    assert!(
        !run.success && run.stderr.contains("subscribe add"),
        "no subscriptions names the way out, got: {}",
        run.stderr
    );

    let stdout = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--own",
            "--label",
            "laptop",
        ])
        .await
        .stdout_or_die("subscribe add");
    let device_key = stdout
        .lines()
        .find_map(|line| line.strip_prefix("device key: "))
        .expect("device key")
        .to_owned();
    push(&server, &device_key, "anyone?", None).await;

    // An unknown label is actionable, pointing at subscribe list.
    let run = harness
        .run(&["--plain-secrets", "pull", "--sub", "ghost"])
        .await;
    assert!(
        !run.success && run.stderr.contains("ghost") && run.stderr.contains("subscribe list"),
        "unknown label names itself and the way out, got: {}",
        run.stderr
    );

    // With one Subscription held, the bare pull resolves it.
    let run = harness.run(&["--plain-secrets", "pull"]).await;
    assert!(
        run.success && run.stdout.contains("1"),
        "the sole Subscription is addressed without --sub, got: {}/{}",
        run.stderr,
        run.stdout
    );

    // With two held, the bare pull refuses and names --sub.
    harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--device-key",
            &device_key,
            "--label",
            "desk",
        ])
        .await
        .stdout_or_die("second subscribe add");
    let run = harness.run(&["--plain-secrets", "pull"]).await;
    assert!(
        !run.success && run.stderr.contains("--sub"),
        "several Subscriptions demand --sub, got: {}",
        run.stderr
    );

    server.shutdown().await;
}
