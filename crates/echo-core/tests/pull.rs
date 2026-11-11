//! The one-shot Pull against the embedded real server (ticket #7): pushes
//! sent while no stream was open are replayed from a chosen cursor into
//! the Subscription's History Log — silently. Every assertion is external
//! — the Pull report, History Log contents — never connection internals.
//! There is deliberately no intent output on the Pull seam: `PullConfig`
//! carries no channel and `pull` takes no consumer, so a Pull cannot
//! produce a notification intent (spec: "same SSE client with
//! notification output off").

mod common;

use std::time::Duration;

use echo_core::history::HistoryLog;
use echo_core::pull::{PullConfig, pull};

/// A Pull config for one Subscription assembled from its pieces: the
/// server URL, the Stream Token secret, and the History Log path. The
/// quiet window is short — the embedded server replays back-to-back under
/// one lock (protocol §3.5), so 300 ms of silence means the replay drained.
fn pull_config(base_url: &str, stream_token: &str, history_path: &std::path::Path) -> PullConfig {
    let mut config = PullConfig::new(base_url, stream_token, HistoryLog::new(history_path));
    config.quiet_window = Duration::from_millis(300);
    config
}

/// A Pull config pointing at the harness with its History Log.
fn config_for(harness: &common::Subscribed) -> PullConfig {
    pull_config(
        &harness.server.base_url,
        &harness.stream_token,
        &harness.history_path(),
    )
}

#[tokio::test]
async fn a_pull_lands_retained_messages_silently_in_the_history_log_in_id_order() {
    // No daemon, no stream: the Pushes pile up in the server's
    // store-and-forward store (§3.7) — the gap a Pull must heal.
    let harness = common::Subscribed::spawn().await;
    harness
        .push(serde_json::json!({ "title": "Flight", "subtitle": "SFO", "body": "Gate 7" }))
        .await;
    harness
        .push(serde_json::json!({ "title": "Second", "body": "rows 12-14" }))
        .await;
    harness
        .push(serde_json::json!({ "body": "third, after the laptop slept" }))
        .await;

    // Cursor 0 = everything retained (protocol §3.5).
    let report = pull(config_for(&harness)).await.expect("pull succeeds");

    assert_eq!(report.ingested, 3, "every retained Message ingested once");
    let log = HistoryLog::new(harness.history_path());
    let entries = log.entries().expect("history entries");
    assert_eq!(
        entries.iter().map(|entry| entry.id).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "the History Log reads in id order (§3.4: delivery order per Device)"
    );
    assert_eq!(entries[0].title, "Flight");
    assert_eq!(entries[0].subtitle, "SFO");
    assert_eq!(entries[0].body, "Gate 7");
    assert_eq!(entries[1].title, "Second");
    assert_eq!(entries[1].body, "rows 12-14");
    assert_eq!(entries[2].body, "third, after the laptop slept");
    // The entries are exactly what the live receive loop would have
    // resolved — the Ext Params mapping is the same code path — and carry
    // the server-assigned stamps.
    assert!(entries[0].created_at > 0);
    assert_eq!(entries[0].click_action, None);

    // Silent means silent: the History Log is the Pull's only effect.
    // (The Pull seam carries no intent output at all — see the module
    // comment — so zero notifications and zero intents is structural.)
    assert_eq!(log.cursor().expect("cursor"), Some(3));

    harness.server.shutdown().await;
}

#[tokio::test]
async fn a_second_pull_with_the_same_cursor_adds_no_duplicates() {
    let harness = common::Subscribed::spawn().await;
    harness.push(serde_json::json!({ "body": "one" })).await;
    harness.push(serde_json::json!({ "body": "two" })).await;

    let first = pull(config_for(&harness)).await.expect("first pull");
    assert_eq!(first.ingested, 2);

    // The same cursor again: the server replays the same retained window,
    // and the dedup path — the receive loop's rule, per-Device id vs. the
    // History Log cursor (§3.7) — suppresses every line of it.
    let second = pull(config_for(&harness)).await.expect("second pull");
    assert_eq!(second.ingested, 0, "no duplicates on an overlapping pull");
    let log = HistoryLog::new(harness.history_path());
    assert_eq!(
        log.entries()
            .expect("entries")
            .iter()
            .map(|entry| entry.body.as_str())
            .collect::<Vec<_>>(),
        vec!["one", "two"],
        "the log still holds exactly one line per Message"
    );

    harness.server.shutdown().await;
}

#[tokio::test]
async fn a_pull_after_the_live_receive_loop_only_heals_the_gap() {
    // The dedup path is shared with the live loop: what the loop already
    // processed and logged must never re-land through a pull.
    let harness = common::Subscribed::spawn().await;
    let log = HistoryLog::new(harness.history_path());
    let config = echo_core::stream::StreamConfig {
        base_url: harness.server.base_url.clone(),
        stream_token: harness.stream_token.clone(),
        basic_auth: None,
        history_log: log.clone(),
        backoff: echo_core::stream::BackoffConfig {
            base: Duration::from_millis(150),
            cap: Duration::from_secs(2),
        },
        events: None,
    };
    let (handle, mut intents) = echo_core::stream::RealtimeStream::spawn(config);
    common::wait_active_streams(&harness.server, 1).await;
    harness
        .push(serde_json::json!({ "body": "live one" }))
        .await;
    harness
        .push(serde_json::json!({ "body": "live two" }))
        .await;
    let first = tokio::time::timeout(Duration::from_secs(5), intents.recv())
        .await
        .expect("live intent within 5s")
        .expect("loop alive");
    let second = tokio::time::timeout(Duration::from_secs(5), intents.recv())
        .await
        .expect("live intent within 5s")
        .expect("loop alive");
    assert_eq!((first.id, second.id), (1, 2));
    handle.stop().await;

    // Offline pushes after the loop stopped: the gap.
    harness.push(serde_json::json!({ "body": "gap one" })).await;
    harness.push(serde_json::json!({ "body": "gap two" })).await;

    // Cursor 0 asks for everything retained — the loop's already-logged
    // ids 1 and 2 are suppressed by the shared dedup floor.
    let report = pull(config_for(&harness)).await.expect("pull");
    assert_eq!(report.ingested, 2, "exactly the gap");
    assert_eq!(
        log.entries()
            .expect("entries")
            .iter()
            .map(|entry| entry.body.as_str())
            .collect::<Vec<_>>(),
        vec!["live one", "live two", "gap one", "gap two"],
        "live lines and pulled lines interleave in id order, no duplicates"
    );

    harness.server.shutdown().await;
}

#[tokio::test]
async fn a_pull_cursor_beyond_the_retention_window_clamps_to_the_oldest_retained() {
    // §3.6: the retention cap drops the oldest first — here only the last
    // two of four Pushes stay replayable. Cursor 0 is older than every
    // retained id: the window is the floor, not an error (§3.5).
    let server = common::TestServer::spawn_with(|config| {
        config.sse_message_cap = 2;
    })
    .await;
    let client = echo_core::server::ServerClient::new(&server.base_url);
    let device = client.register_stream_device().await.expect("register");
    let minted = client
        .mint_stream_token(&device.device_key, "echo-test-client")
        .await
        .expect("mint");
    let dir = tempfile::tempdir().expect("temp dir");
    let config = pull_config(
        &server.base_url,
        &minted.secret,
        &dir.path().join("history.jsonl"),
    );

    for body in ["dropped one", "dropped two", "kept one", "kept two"] {
        let response = reqwest::Client::new()
            .post(format!("{}/push", server.base_url))
            .json(&serde_json::json!({ "device_key": device.device_key, "body": body }))
            .send()
            .await
            .expect("POST /push");
        let envelope: serde_json::Value = response.json().await.expect("push envelope");
        assert_eq!(envelope["code"], 200, "push accepted: {envelope}");
    }

    let report = pull(config).await.expect("pull succeeds, no error");
    assert_eq!(report.ingested, 2, "clamped to the oldest retained");
    let log = HistoryLog::new(dir.path().join("history.jsonl"));
    assert_eq!(
        log.entries()
            .expect("entries")
            .iter()
            .map(|entry| entry.body.as_str())
            .collect::<Vec<_>>(),
        vec!["kept one", "kept two"],
        "the retained tail, in id order"
    );

    server.shutdown().await;
}

/// The operator credentials a Basic Auth server demands (protocol §1.4).
const OPERATOR_USER: &str = "operator";
const OPERATOR_PASSWORD: &str = "s3cret";

/// A Basic-Auth server plus a registered Stream Device and its minted
/// Stream Token: `/register` is exempt (§1.4), minting is not, so the mint
/// carries the operator credentials — exactly what the daemon would send.
async fn spawn_basic_auth_harness() -> (common::TestServer, String, String) {
    let server = common::TestServer::spawn_with(|config| {
        config.basic_auth_user = OPERATOR_USER.to_owned();
        config.basic_auth_password = OPERATOR_PASSWORD.to_owned();
    })
    .await;
    let response = reqwest::Client::new()
        .post(format!("{}/register", server.base_url))
        .json(&serde_json::json!({ "device_type": "stream" }))
        .send()
        .await
        .expect("register (exempt from Basic Auth)");
    let envelope: serde_json::Value = response.json().await.expect("register envelope");
    let device_key = envelope["data"]["device_key"]
        .as_str()
        .expect("device key")
        .to_owned();
    let mint = reqwest::Client::new()
        .post(format!(
            "{}/realtime/v1/devices/{}/streams",
            server.base_url, device_key
        ))
        .header("X-Device-Key", &device_key)
        .basic_auth(OPERATOR_USER, Some(OPERATOR_PASSWORD))
        .json(&serde_json::json!({ "client_label": "echo-test-client" }))
        .send()
        .await
        .expect("mint request");
    let minted: serde_json::Value = mint.json().await.expect("mint envelope");
    assert_eq!(minted["code"], 200, "mint accepted: {minted}");
    let token = minted["data"]["token"].as_str().expect("token").to_owned();
    (server, device_key, token)
}

/// Push one V2 JSON Push carrying `body` — Basic Auth demands the operator
/// credentials on `/push` too (§1.4).
async fn push_with_basic_auth(server: &common::TestServer, device_key: &str, body: &str) {
    reqwest::Client::new()
        .post(format!("{}/push", server.base_url))
        .basic_auth(OPERATOR_USER, Some(OPERATOR_PASSWORD))
        .json(&serde_json::json!({ "device_key": device_key, "body": body }))
        .send()
        .await
        .expect("POST /push");
}

#[tokio::test]
async fn basic_auth_credentials_ride_alongside_the_stream_token_on_a_pull() {
    let (server, device_key, token) = spawn_basic_auth_harness().await;
    push_with_basic_auth(&server, &device_key, "behind auth").await;

    // Without the operator credentials the pull is refused (§1.4); with
    // them the replay lands.
    let dir = tempfile::tempdir().expect("temp dir");
    let refused = pull(pull_config(
        &server.base_url,
        &token,
        &dir.path().join("refused.jsonl"),
    ))
    .await
    .expect_err("no Basic Auth credentials, no pull");
    assert!(
        refused.to_string().contains("418"),
        "the Basic Auth refusal surfaces: {refused}"
    );
    assert_eq!(
        HistoryLog::new(dir.path().join("refused.jsonl"))
            .entries()
            .expect("entries")
            .len(),
        0,
        "nothing landed without credentials"
    );

    let mut config = pull_config(&server.base_url, &token, &dir.path().join("history.jsonl"));
    config.basic_auth = Some(echo_core::stream::BasicAuth {
        user: OPERATOR_USER.to_owned(),
        password: OPERATOR_PASSWORD.to_owned(),
    });
    let report = pull(config).await.expect("pull with credentials");
    assert_eq!(report.ingested, 1);
    assert_eq!(
        HistoryLog::new(dir.path().join("history.jsonl"))
            .entries()
            .expect("entries")[0]
            .body,
        "behind auth"
    );

    server.shutdown().await;
}
