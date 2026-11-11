//! The Realtime Stream receive loop against the embedded real server
//! (ticket #4): handshake → notification intents → History Log. Every
//! assertion is external — intents surfaced on the core channel, History
//! Log contents, server-observable state — never loop internals.

mod common;

use std::time::Duration;

use echo_core::history::HistoryLog;
use echo_core::intent::{ClickAction, NotificationIntent};
use echo_core::stream::{BackoffConfig, RealtimeStream, StreamConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Backoff fast enough for tests (the default base is a second).
fn fast_backoff() -> BackoffConfig {
    BackoffConfig {
        base: Duration::from_millis(150),
        cap: Duration::from_secs(2),
    }
}

/// A stream config for one Subscription assembled from its pieces: the
/// server URL, the Stream Token secret, and the History Log path.
fn stream_config(base_url: &str, stream_token: &str, history_path: &std::path::Path) -> StreamConfig {
    StreamConfig {
        base_url: base_url.to_owned(),
        stream_token: stream_token.to_owned(),
        basic_auth: None,
        history_log: HistoryLog::new(history_path),
        backoff: fast_backoff(),
    }
}

/// A stream config pointing at the harness with its History Log.
fn config_for(harness: &common::Subscribed) -> StreamConfig {
    stream_config(
        &harness.server.base_url,
        &harness.stream_token,
        &harness.history_path(),
    )
}

/// A stream config pointing at a hand-rolled wire with a fresh History Log.
fn config_for_wire(base_url: &str, dir: &tempfile::TempDir) -> StreamConfig {
    // The wire double ignores request headers; any token shape works.
    stream_config(base_url, "st_wire-double", &dir.path().join("history.jsonl"))
}

/// One `event: message` frame's `data:` payload for the wire doubles: the
/// push JSON the server writes (protocol §3.4), minus fields the doubles
/// don't exercise.
fn push_event(body: &str) -> String {
    format!(
        r#"{{"title":"","subtitle":"","body":"{body}","ext_params":{{}},"created_at":1760000000}}"#
    )
}

/// A stand-in wire for misbehavior the embedded real server cannot produce
/// on demand: the conforming server never resends an id at or below the
/// presented `Last-Event-ID` (§3.5), so the at-least-once duplicate of §3.7
/// exists only on a misbehaving wire. Accepts connection after connection;
/// each is served an HTTP/1.1 response head followed by its `frames` —
/// every frame followed by its pause in milliseconds, so a test scripts
/// the wire's timing — and then held open until the client goes away.
/// Abort the returned task to stop it.
async fn scripted_sse_server(frames: Vec<(String, u64)>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind wire double");
    let addr = listener.local_addr().expect("local addr");
    let head =
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n";
    let handle = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            if socket.write_all(head.as_bytes()).await.is_err() {
                continue;
            }
            for (frame, pause_ms) in &frames {
                if socket.write_all(frame.as_bytes()).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(*pause_ms)).await;
            }
            // Hold the connection open — the client must stay connected —
            // draining whatever the peer sends until it hangs up.
            let mut scratch = [0u8; 512];
            while socket.read(&mut scratch).await.is_ok_and(|n| n > 0) {}
        }
    });
    (format!("http://{addr}"), handle)
}

/// The next intent on the channel, within a generous test timeout.
async fn next_intent(intents: &mut tokio::sync::mpsc::Receiver<NotificationIntent>) -> NotificationIntent {
    tokio::time::timeout(Duration::from_secs(5), intents.recv())
        .await
        .expect("intent within 5s")
        .expect("receive loop still running")
}

/// Assert no further intent arrives for `ms`.
async fn assert_no_intent(
    intents: &mut tokio::sync::mpsc::Receiver<NotificationIntent>,
    ms: u64,
) {
    match tokio::time::timeout(Duration::from_millis(ms), intents.recv()).await {
        Ok(intent) => panic!("unexpected intent: {intent:?}"),
        Err(_) => {}
    }
}

#[tokio::test]
async fn pushing_to_the_device_surfaces_the_matching_intent_in_order() {
    let harness = common::Subscribed::spawn().await;
    let (handle, mut intents) = RealtimeStream::spawn(config_for(&harness));

    // The stream must be live before pushing (protocol §3.3: only pushes
    // published after the `: connected` handshake are mirrored live).
    common::wait_active_streams(&harness.server, 1).await;

    harness
        .push(serde_json::json!({ "title": "Flight", "subtitle": "SFO", "body": "Gate 7" }))
        .await;
    harness.push(serde_json::json!({ "title": "Second", "body": "rows 12-14" })).await;

    let first = next_intent(&mut intents).await;
    let second = next_intent(&mut intents).await;

    assert_eq!((first.title.as_str(), first.subtitle.as_str(), first.body.as_str()), ("Flight", "SFO", "Gate 7"));
    assert_eq!((second.title.as_str(), second.subtitle.as_str(), second.body.as_str()), ("Second", "", "rows 12-14"));
    // Per-Device monotonic ids, in push order (protocol §3.4).
    assert_eq!(first.id, 1);
    assert_eq!(second.id, 2);
    assert!(second.id > first.id, "delivery order equals id order");
    // created_at is the server-assigned Unix-seconds stamp.
    assert!(first.created_at > 0);

    // History Log lines appear only for emitted intents; the last line's
    // id equals the reconnect cursor (GLOSSARY: History Log).
    let log = HistoryLog::new(harness.history_path());
    let entries = log.entries().expect("history entries");
    assert_eq!(
        entries,
        vec![first.clone(), second.clone()],
        "one line per emitted intent"
    );
    assert_eq!(log.cursor().expect("cursor"), Some(second.id));

    handle.stop().await;
    harness.server.shutdown().await;
}

#[tokio::test]
async fn history_log_stays_empty_until_the_first_intent_is_emitted() {
    let harness = common::Subscribed::spawn().await;
    let log = HistoryLog::new(harness.history_path());
    let (handle, mut intents) = RealtimeStream::spawn(config_for(&harness));

    common::wait_active_streams(&harness.server, 1).await;
    // Connected and idle: handshake and keep-alive territory, no intents,
    // no History Log lines.
    assert_no_intent(&mut intents, 300).await;
    assert_eq!(log.entries().expect("entries").len(), 0, "no lines before any intent");

    harness.push(serde_json::json!({ "body": "one" })).await;
    let intent = next_intent(&mut intents).await;
    assert_eq!(log.entries().expect("entries"), vec![intent]);

    handle.stop().await;
    harness.server.shutdown().await;
}

#[tokio::test]
async fn keep_alive_comments_and_the_handshake_produce_no_intents() {
    // A server that emits `: keep-alive` every second (protocol §3.4);
    // assembled by hand because Subscribed::spawn uses default settings.
    let server = common::TestServer::spawn_with(|config| {
        config.sse_keepalive_secs = 1;
    })
    .await;
    let client = echo_core::server::ServerClient::new(&server.base_url);
    let device = client.register_stream_device().await.expect("register");
    let minted = client
        .mint_stream_token(&device.device_key, "echo-test-client")
        .await
        .expect("mint");
    let harness = common::Subscribed {
        server,
        device_key: device.device_key,
        stream_token: minted.secret,
        dir: tempfile::tempdir().expect("temp dir"),
    };

    let (handle, mut intents) = RealtimeStream::spawn(config_for(&harness));
    common::wait_active_streams(&harness.server, 1).await;

    // Two-plus keep-alive comments cross the wire (1 s apart). They are
    // comments, not events: no intent, no History Log line.
    assert_no_intent(&mut intents, 2_600).await;
    assert_eq!(
        HistoryLog::new(harness.history_path())
            .entries()
            .expect("entries")
            .len(),
        0
    );

    // And the stream is still live after them: a push still surfaces.
    harness.push(serde_json::json!({ "body": "still alive" })).await;
    let intent = next_intent(&mut intents).await;
    assert_eq!(intent.body, "still alive");

    handle.stop().await;
    let common::Subscribed { server, .. } = harness;
    server.shutdown().await;
}

#[tokio::test]
async fn duplicate_ids_at_or_below_the_cursor_never_emit_intents() {
    // §3.7: delivery is at-least-once, and the client owns exactly-once —
    // a wire that resends already-processed ids must not re-notify. The
    // script: events 1 and 2, then id 2 again (a duplicate at the cursor),
    // then id 1 again (below the cursor), and — only after the suppression
    // window has passed — the genuinely new id 3.
    let frame = |id: i64, body: &str| {
        format!("event: message\nid: {id}\ndata: {}\n\n", push_event(body))
    };
    let frames = vec![
        (": connected\n\n".to_owned(), 50),
        (frame(1, "first"), 50),
        (frame(2, "second"), 50),
        (frame(2, "duplicate at the cursor"), 50),
        (frame(1, "below the cursor"), 500),
        (frame(3, "third"), 50),
    ];
    let (base_url, wire) = scripted_sse_server(frames).await;
    let dir = tempfile::tempdir().expect("temp dir");
    let (handle, mut intents) = RealtimeStream::spawn(config_for_wire(&base_url, &dir));

    let first = next_intent(&mut intents).await;
    let second = next_intent(&mut intents).await;
    assert_eq!((first.id, first.body.as_str()), (1, "first"));
    assert_eq!((second.id, second.body.as_str()), (2, "second"));

    // The resent 2 and the below-cursor 1 cross the wire (both land well
    // inside this window) but surface nothing.
    assert_no_intent(&mut intents, 300).await;

    let third = next_intent(&mut intents).await;
    assert_eq!((third.id, third.body.as_str()), (3, "third"));

    // The cursor never went backwards: the log holds exactly one line per
    // emitted intent, in id order.
    let log = HistoryLog::new(dir.path().join("history.jsonl"));
    assert_eq!(
        log.entries().expect("history entries"),
        vec![first, second, third],
        "duplicates never reached the History Log either"
    );
    assert_eq!(log.cursor().expect("cursor"), Some(3));

    handle.stop().await;
    wire.abort();
}

#[tokio::test]
async fn killing_and_reopening_the_stream_replays_exactly_the_gap() {
    let harness = common::Subscribed::spawn().await;
    let (handle, mut intents) = RealtimeStream::spawn(config_for(&harness));
    common::wait_active_streams(&harness.server, 1).await;

    // Two pushes while connected: both surface, in id order.
    harness.push(serde_json::json!({ "body": "one" })).await;
    harness.push(serde_json::json!({ "body": "two" })).await;
    let one = next_intent(&mut intents).await;
    let two = next_intent(&mut intents).await;
    assert_eq!((one.id, one.body.as_str()), (1, "one"));
    assert_eq!((two.id, two.body.as_str()), (2, "two"));

    // Kill the stream mid-sequence.
    handle.stop().await;

    // Pushes with no live stream are persisted for replay (§3.7) — this is
    // the gap the reopened stream must heal.
    harness.push(serde_json::json!({ "body": "three" })).await;
    harness.push(serde_json::json!({ "body": "four" })).await;

    // Reopen. The History Log's last line (id 2) is the reconnect cursor
    // (GLOSSARY) and rides as Last-Event-ID; the server replays exactly
    // the gap, then hands off to live delivery (§3.5).
    let (handle, mut intents) = RealtimeStream::spawn(config_for(&harness));
    let three = next_intent(&mut intents).await;
    let four = next_intent(&mut intents).await;
    assert_eq!((three.id, three.body.as_str()), (3, "three"));
    assert_eq!((four.id, four.body.as_str()), (4, "four"));

    // Live again: a fresh push surfaces once, and nothing old re-surfaces.
    harness.push(serde_json::json!({ "body": "five" })).await;
    let five = next_intent(&mut intents).await;
    assert_eq!((five.id, five.body.as_str()), (5, "five"));
    assert_no_intent(&mut intents, 300).await;

    // The whole sequence, exactly once: no loss, no duplicate reached the
    // intent output, and the cursor sits on the tail.
    let log = HistoryLog::new(harness.history_path());
    let entries = log.entries().expect("history entries");
    assert_eq!(
        entries.iter().map(|e| e.id).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(
        entries.iter().map(|e| e.body.as_str()).collect::<Vec<_>>(),
        vec!["one", "two", "three", "four", "five"]
    );
    assert_eq!(log.cursor().expect("cursor"), Some(5));

    handle.stop().await;
    harness.server.shutdown().await;
}

/// Push one V2 JSON Push carrying `body` to `device_key` and assert the
/// server took it — the bare push a restart test needs (no harness).
async fn push_to(server: &common::TestServer, device_key: &str, body: &str) {
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

#[tokio::test]
async fn a_server_initiated_close_reconnects_and_recovers_the_gap() {
    // Caller-owned scratch and port: the store outlives the first server
    // process, and the second one rebinds the same address, so the
    // client's config never changes.
    let dir = tempfile::tempdir().expect("temp dir");
    let history_path = dir.path().join("history.jsonl");
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind port")
        .local_addr()
        .expect("local addr");
    let server = common::TestServer::spawn_on(
        std::net::TcpListener::bind(addr).expect("bind first server"),
        dir.path().to_path_buf(),
        |_| {},
    )
    .await;
    let client = echo_core::server::ServerClient::new(&server.base_url);
    let device = client.register_stream_device().await.expect("register");
    let minted = client
        .mint_stream_token(&device.device_key, "echo-test-client")
        .await
        .expect("mint");
    let config = stream_config(&server.base_url, &minted.secret, &history_path);

    let (handle, mut intents) = RealtimeStream::spawn(config);
    common::wait_active_streams(&server, 1).await;
    push_to(&server, &device.device_key, "before the close").await;
    let first = next_intent(&mut intents).await;
    assert_eq!((first.id, first.body.as_str()), (1, "before the close"));

    // The server shuts down: the open Realtime Stream is closed under the
    // client (a server-initiated disconnect, §3.5).
    server.shutdown().await;

    // The restarted server on the same port and store. The loop retries
    // through the downtime; the pushes below land while no stream is open
    // and are persisted for replay (§3.7).
    let server = common::TestServer::spawn_on(
        std::net::TcpListener::bind(addr).expect("rebind same port"),
        dir.path().to_path_buf(),
        |_| {},
    )
    .await;
    push_to(&server, &device.device_key, "replayed one").await;
    push_to(&server, &device.device_key, "replayed two").await;

    // Reconnect heals the gap: replay first, then the live handoff — no
    // loss, no duplicate of what was already processed.
    let second = next_intent(&mut intents).await;
    let third = next_intent(&mut intents).await;
    assert_eq!((second.id, second.body.as_str()), (2, "replayed one"));
    assert_eq!((third.id, third.body.as_str()), (3, "replayed two"));
    push_to(&server, &device.device_key, "after the reconnect").await;
    let fourth = next_intent(&mut intents).await;
    assert_eq!((fourth.id, fourth.body.as_str()), (4, "after the reconnect"));
    assert_no_intent(&mut intents, 300).await;

    let log = HistoryLog::new(&history_path);
    assert_eq!(
        log.entries()
            .expect("history entries")
            .iter()
            .map(|e| e.id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4],
        "the whole sequence exactly once, across the server restart"
    );
    assert_eq!(log.cursor().expect("cursor"), Some(4));

    handle.stop().await;
    server.shutdown().await;
}

#[tokio::test]
async fn ext_params_map_to_click_actions_with_url_winning_over_copy() {
    let harness = common::Subscribed::spawn().await;
    let (handle, mut intents) = RealtimeStream::spawn(config_for(&harness));
    common::wait_active_streams(&harness.server, 1).await;

    harness
        .push(serde_json::json!({ "body": "link", "url": "https://example.com/boarding" }))
        .await;
    harness
        .push(serde_json::json!({ "body": "snippet", "copy": "the text to copy" }))
        .await;
    harness
        .push(serde_json::json!({
            "body": "both",
            "url": "https://example.com/wins",
            "copy": "ignored when url is present",
        }))
        .await;
    harness.push(serde_json::json!({ "body": "neither" })).await;

    let with_url = next_intent(&mut intents).await;
    let with_copy = next_intent(&mut intents).await;
    let with_both = next_intent(&mut intents).await;
    let with_neither = next_intent(&mut intents).await;

    assert_eq!(
        with_url.click_action,
        Some(ClickAction::OpenUrl { url: "https://example.com/boarding".to_owned() }),
        "url → open the default browser"
    );
    assert_eq!(
        with_copy.click_action,
        Some(ClickAction::Copy { text: "the text to copy".to_owned() }),
        "copy without url → clipboard"
    );
    assert_eq!(
        with_both.click_action,
        Some(ClickAction::OpenUrl { url: "https://example.com/wins".to_owned() }),
        "url wins when both ride along"
    );
    assert_eq!(with_neither.click_action, None, "no url, no copy → no action");
    // The alert text rides unchanged.
    assert_eq!(with_neither.body, "neither");

    handle.stop().await;
    harness.server.shutdown().await;
}

#[tokio::test]
async fn an_encrypted_push_renders_the_placeholder_intent_not_ciphertext_noise() {
    let harness = common::Subscribed::spawn().await;
    let (handle, mut intents) = RealtimeStream::spawn(config_for(&harness));
    common::wait_active_streams(&harness.server, 1).await;

    harness
        .push(serde_json::json!({
            "ciphertext": "BASE64CIPHERTEXT",
            "iv": "BASE64IV",
        }))
        .await;

    let intent = next_intent(&mut intents).await;

    // A clear placeholder instead of ciphertext noise (spec user story 13).
    assert_eq!(intent.title, "Encrypted notification");
    assert!(
        !intent.body.contains("BASE64CIPHERTEXT"),
        "no ciphertext in the rendered body: {}",
        intent.body
    );
    assert_eq!(intent.click_action, None, "no click action to resolve");
    // The id still rides along: dedup and the History Log cursor stay exact.
    let log = HistoryLog::new(harness.history_path());
    assert_eq!(log.cursor().expect("cursor"), Some(intent.id));

    handle.stop().await;
    harness.server.shutdown().await;
}
