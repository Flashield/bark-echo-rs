//! `log --follow` (ticket #7): new History Log lines stream to stdout as
//! they land, live. The appender here is a concurrent `pull` — the same
//! `HistoryLog::append` path the daemon's receive loop uses — so the seam
//! is exactly the file both processes share. The child's stdout is read
//! incrementally (tokio::process: the embedded server shares this test's
//! runtime; a blocking read would starve it).

mod common;

use std::time::Duration;

use common::Harness;
use tokio::io::AsyncReadExt;
use tokio::process::ChildStdout;

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

/// Read the follow child's stdout until every needle has appeared, within
/// a generous deadline.
async fn read_until(stdout: &mut ChildStdout, needles: &[&str]) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let collected = String::from_utf8_lossy(&buf).into_owned();
        if needles.iter().all(|needle| collected.contains(needle)) {
            return collected;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "follow never showed {needles:?}; got: {collected}"
        );
        let mut chunk = [0u8; 1024];
        match tokio::time::timeout(Duration::from_millis(400), stdout.read(&mut chunk)).await {
            Ok(Ok(0)) => panic!("follow stdout closed early; got: {collected}"),
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(err)) => panic!("follow stdout read error: {err}"),
            Err(_) => {} // no data yet: keep polling toward the deadline
        }
    }
}

#[tokio::test]
async fn log_follow_streams_lines_as_a_concurrent_pull_lands_them() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();
    let device_key = subscribe(&harness, &server.base_url, "laptop").await;

    // Pushes pile up offline, the History Log is still empty, and the
    // follow child is watching — printing nothing so far.
    for body in ["first", "second", "third"] {
        push(&server, &device_key, body).await;
    }
    let mut child = harness
        .spawn(&["--plain-secrets", "log", "--follow"])
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn log --follow");
    let mut stdout = child.stdout.take().expect("piped stdout");

    // The pull backfills the three; the follow child streams them as they
    // land — the daemon-running-live shape of the same append path.
    harness.run(&["--plain-secrets", "pull"]).await;
    let seen = read_until(&mut stdout, &["#1", "#2", "#3", "first", "second", "third"]).await;
    assert!(
        seen.contains("#1 ") && seen.contains("#2 ") && seen.contains("#3 "),
        "the streamed lines carry their ids: {seen}"
    );

    // The stream keeps going: more pushes, another pull, the follow child
    // shows the new line without being restarted.
    push(&server, &device_key, "fourth, later").await;
    harness.run(&["--plain-secrets", "pull"]).await;
    let seen = read_until(&mut stdout, &["#4", "fourth, later"]).await;
    assert!(
        !seen.contains("pulled"),
        "the report belongs to the pull process, not the follow stream: {seen}"
    );

    child.kill().await.expect("kill follow child");
    server.shutdown().await;
}

#[tokio::test]
async fn log_follow_with_tail_keeps_the_recent_end_and_still_streams() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();
    let device_key = subscribe(&harness, &server.base_url, "laptop").await;
    for body in ["first", "second", "third"] {
        push(&server, &device_key, body).await;
    }
    harness.run(&["--plain-secrets", "pull"]).await;

    // --tail 2 prunes the initial dump to the newest end; new lines still
    // stream in full.
    let mut child = harness
        .spawn(&["--plain-secrets", "log", "--follow", "--tail", "2"])
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn log --follow --tail 2");
    let mut stdout = child.stdout.take().expect("piped stdout");

    push(&server, &device_key, "fourth, live").await;
    harness.run(&["--plain-secrets", "pull"]).await;

    let seen = read_until(&mut stdout, &["#4", "fourth, live"]).await;
    let lines: Vec<&str> = seen.lines().collect();
    assert_eq!(
        lines.len(),
        3,
        "the initial tail of two, then the new line: {seen}"
    );
    assert!(
        !seen.contains("first"),
        "--tail 2 drops the oldest from the initial dump: {seen}"
    );
    assert!(
        lines[0].contains("#2") && lines[0].contains("second"),
        "the tail starts at the newest retained end: {seen}"
    );
    assert!(
        lines[2].contains("#4") && lines[2].contains("fourth, live"),
        "the new line streams after the initial tail: {seen}"
    );

    child.kill().await.expect("kill follow child");
    server.shutdown().await;
}
