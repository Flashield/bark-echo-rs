//! The `log` subcommand (ticket #7): the Subscription's History Log read
//! back for auditing — human-readable lines in id order, `--tail N` for
//! the recent end, `--sub LABEL` when several Subscriptions are held.

mod common;

use common::Harness;

/// Subscribe once (plain-secrets, temp HOME) and return the Device Key.
async fn subscribe(harness: &Harness, server_url: &str, label: &str) -> String {
    let stdout = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            server_url,
            "--own",
            "--label",
            label,
        ])
        .await
        .stdout_or_die("subscribe add");
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("device key: "))
        .expect("add prints the Device Key")
        .to_owned()
}

/// Push one V2 JSON Push carrying `body` while no daemon runs.
async fn push(server: &common::TestServer, device_key: &str, body: &str) {
    let response = reqwest::Client::new()
        .post(format!("{}/push", server.base_url))
        .json(&serde_json::json!({ "device_key": device_key, "body": body }))
        .send()
        .await
        .expect("POST /push");
    let envelope: serde_json::Value = response.json().await.expect("push envelope");
    assert_eq!(envelope["code"], 200, "push accepted: {envelope}");
}

/// The scenario both log tests build on: three offline pushes pulled back
/// into the Subscription's History Log.
async fn pulled_log(server: &common::TestServer, harness: &Harness) {
    let device_key = subscribe(harness, &server.base_url, "laptop").await;
    for body in ["first", "second", "third"] {
        push(server, &device_key, body).await;
    }
    harness
        .run(&["--plain-secrets", "pull"])
        .await
        .stdout_or_die("pull");
}

#[tokio::test]
async fn log_reads_the_history_back_in_id_order_after_a_pull() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();
    pulled_log(&server, &harness).await;

    // The whole log, in id order: one line per Message, each carrying the
    // id, a human-readable timestamp, and the alert text.
    let stdout = harness
        .run(&["--plain-secrets", "log"])
        .await
        .stdout_or_die("log");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 3, "one line per Message: {stdout}");
    for (id, line) in [1, 2, 3].iter().zip(lines.iter()) {
        assert!(
            line.starts_with(&format!("#{id} ")),
            "the id leads the line: {line}"
        );
        assert!(
            !line.is_char_boundary(20) || line.chars().count() > 10,
            "line has substance: {line}"
        );
    }
    assert!(
        lines[0].contains("first") && lines[1].contains("second") && lines[2].contains("third"),
        "id order equals push order: {stdout}"
    );
    // The timestamp is human-readable: RFC 3339 in UTC, 20 characters
    // ending in Z (e.g. 2026-11-12T10:33:21Z) between the id and the text.
    let (_, rest) = lines[0].split_once(' ').expect("id then stamp");
    let (stamp, _) = rest.split_once(' ').expect("stamp then text");
    assert_eq!(
        stamp.len(),
        20,
        "RFC 3339 shape: {line_all}",
        line_all = lines[0]
    );
    assert!(
        stamp.contains('T') && stamp.ends_with('Z'),
        "RFC 3339 UTC stamp, got: {stamp}"
    );

    // --tail N keeps only the last N entries.
    let stdout = harness
        .run(&["--plain-secrets", "log", "--tail", "2"])
        .await
        .stdout_or_die("log --tail 2");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 2, "tail keeps two: {stdout}");
    assert!(
        !lines[0].contains("first") && lines[0].contains("second") && lines[1].contains("third"),
        "the tail is the newest end: {stdout}"
    );

    // An empty tail window (larger than the log) shows everything.
    let stdout = harness
        .run(&["--plain-secrets", "log", "--tail", "50"])
        .await
        .stdout_or_die("log --tail 50");
    assert_eq!(stdout.lines().count(), 3);

    server.shutdown().await;
}

#[tokio::test]
async fn log_resolves_subscriptions_like_pull_does() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();

    // Nothing held: the same actionable resolution error as pull.
    let run = harness.run(&["--plain-secrets", "log"]).await;
    assert!(
        !run.success && run.stderr.contains("subscribe add"),
        "no subscriptions names the way out, got: {}",
        run.stderr
    );

    pulled_log(&server, &harness).await;

    // The label resolves.
    let stdout = harness
        .run(&["--plain-secrets", "log", "--sub", "laptop"])
        .await
        .stdout_or_die("log --sub laptop");
    assert_eq!(stdout.lines().count(), 3);

    // An unknown label is actionable.
    let run = harness
        .run(&["--plain-secrets", "log", "--sub", "ghost"])
        .await;
    assert!(
        !run.success && run.stderr.contains("ghost") && run.stderr.contains("subscribe list"),
        "unknown label names itself and the way out, got: {}",
        run.stderr
    );

    server.shutdown().await;
}
