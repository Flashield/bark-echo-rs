//! The receive loop's connection-state face (ticket #5): `StreamEvent`
//! surfaces the Connected/Disconnected transitions a runtime reports per
//! Subscription, and a rejected token's exact classification — 401
//! `stream token invalid` ends the loop as [`StreamEnd::TokenInvalid`]
//! (the actionable re-subscribe case), never as a retry spin.

mod common;

use std::time::Duration;

use echo_core::history::HistoryLog;
use echo_core::stream::{BackoffConfig, RealtimeStream, StreamConfig, StreamEnd, StreamEvent};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Close a wire-fake connection the way a real server does: drain the
/// request bytes, then FIN. A bare drop sends RST while receive-buffer
/// data sits unread (BSD/macOS), which destroys the peer's buffered
/// response before it reads the envelope — the 401 body must land.
async fn close_gracefully(socket: &mut tokio::net::TcpStream) {
    let mut sink = [0u8; 4096];
    let _ = socket.read(&mut sink).await;
    let _ = socket.shutdown().await;
}

/// Backoff fast enough for tests (the default base is a second).
fn fast_backoff() -> BackoffConfig {
    BackoffConfig {
        base: Duration::from_millis(150),
        cap: Duration::from_secs(2),
    }
}

/// A stream config for the harness with an events observer attached.
fn config_with_events(
    harness: &common::Subscribed,
) -> (StreamConfig, tokio::sync::mpsc::Receiver<StreamEvent>) {
    let (events_tx, events_rx) = tokio::sync::mpsc::channel(64);
    let config = StreamConfig {
        base_url: harness.server.base_url.clone(),
        stream_token: harness.stream_token.clone(),
        basic_auth: None,
        history_log: HistoryLog::new(harness.history_path()),
        backoff: fast_backoff(),
        keepalive_interval: None,
        events: Some(events_tx),
    };
    (config, events_rx)
}

/// The next event on the channel, within a generous test timeout.
async fn next_event(events: &mut tokio::sync::mpsc::Receiver<StreamEvent>) -> StreamEvent {
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("stream event within 5s")
        .expect("receive loop still running")
}

#[tokio::test]
async fn a_revoked_stream_token_ends_the_loop_as_token_invalid_not_a_retry_spin() {
    let harness = common::Subscribed::spawn().await;
    let client = echo_core::server::ServerClient::new(&harness.server.base_url);
    // Revoked from elsewhere (spec story 21): the secret is structurally
    // fine but unknown-or-revoked server-side from here on.
    let summaries = client
        .list_stream_tokens(&harness.device_key)
        .await
        .expect("list tokens");
    client
        .revoke_stream_token(&harness.device_key, &summaries[0].token_id)
        .await
        .expect("revoke");

    let (config, _events) = config_with_events(&harness);
    let (handle, mut intents) = RealtimeStream::spawn(config);

    // The very first connect is rejected (§3.3) and the loop ends on its
    // own: the intents channel closes with nothing ever surfaced.
    let ended = tokio::time::timeout(Duration::from_secs(5), intents.recv())
        .await
        .expect("loop ends within 5s");
    assert!(ended.is_none(), "no intent can surface on a rejected token");

    let end = handle.stop().await;
    assert_eq!(end, StreamEnd::TokenInvalid);

    harness.server.shutdown().await;
}

#[tokio::test]
async fn a_token_invalidated_mid_stream_rejects_the_reconnect_with_token_invalid() {
    // The exact ticket flow on a scripted wire: connection 1 is served the
    // handshake and one live event, then dropped; while the loop is down
    // the token is revoked from elsewhere, so connection 2 is answered 401
    // `stream token invalid`. The loop must end TokenInvalid having
    // retried exactly once — no backoff spin against a dead credential.
    let head =
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n";
    let rejected = "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\n\r\n\
{\"code\":401,\"message\":\"stream token invalid\",\"timestamp\":1760000000}";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind wire double");
    let addr = listener.local_addr().expect("local addr");
    let connections = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let wire_connections = connections.clone();
    let wire = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let n = wire_connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                // Connection 1: handshake, one event, then the drop.
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket
                    .write_all(
                        b": connected\n\nevent: message\nid: 1\ndata: \
{\"body\":\"live\",\"created_at\":1760000000}\n\n",
                    )
                    .await;
                // hang up (EOF): the reconnect must happen
                close_gracefully(&mut socket).await;
                continue;
            }
            // Connection 2 (and any further): the revoked-token 401.
            let _ = socket.write_all(rejected.as_bytes()).await;
            close_gracefully(&mut socket).await;
            break;
        }
    });
    let dir = tempfile::tempdir().expect("temp dir");
    let (events_tx, mut events) = tokio::sync::mpsc::channel(64);
    let config = StreamConfig {
        base_url: format!("http://{addr}"),
        stream_token: "st_now-revoked".to_owned(),
        basic_auth: None,
        history_log: HistoryLog::new(dir.path().join("history.jsonl")),
        backoff: fast_backoff(),
        keepalive_interval: None,
        events: Some(events_tx),
    };
    let (handle, mut intents) = RealtimeStream::spawn(config);

    // The live event surfaces first (connection 1 was fine), and the
    // handshake's Connected preceded it.
    assert_eq!(next_event(&mut events).await, StreamEvent::Connected);
    let intent = tokio::time::timeout(Duration::from_secs(5), intents.recv())
        .await
        .expect("intent within 5s")
        .expect("intent on the live connection");
    assert_eq!(intent.body, "live");

    // The drop surfaces as Disconnected; then the reconnect hits the 401
    // and the loop ends TokenInvalid — the intents channel closes.
    let disconnected = next_event(&mut events).await;
    assert!(
        matches!(disconnected, StreamEvent::Disconnected { .. }),
        "the drop must surface as Disconnected: {disconnected:?}"
    );
    let ended = tokio::time::timeout(Duration::from_secs(5), intents.recv())
        .await
        .expect("loop ends within 5s");
    assert!(ended.is_none(), "a rejected token never delivers intents");

    let end = handle.stop().await;
    assert_eq!(end, StreamEnd::TokenInvalid);
    assert_eq!(
        connections.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "exactly the live connection plus one reconnect — no spin"
    );

    wire.abort();
}

#[tokio::test]
async fn other_rejections_stay_auth_rejected_not_token_invalid() {
    // 401 `stream token required` (§5) is a different actionable problem
    // from `stream token invalid`: the envelope message decides, so this
    // stays AuthRejected — and the loop still refuses to spin.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind wire double");
    let addr = listener.local_addr().expect("local addr");
    let connections = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let wire_connections = connections.clone();
    let wire = tokio::spawn(async move {
        // One answer is all this wire serves: the loop must end on it,
        // never retry-spin.
        if let Ok((mut socket, _)) = listener.accept().await {
            wire_connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = socket
                .write_all(
                    b"HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\n\r\n\
{\"code\":401,\"message\":\"stream token required\",\"timestamp\":1760000000}",
                )
                .await;
            close_gracefully(&mut socket).await;
        }
    });
    let dir = tempfile::tempdir().expect("temp dir");
    let config = StreamConfig {
        base_url: format!("http://{addr}"),
        stream_token: "st_missing".to_owned(),
        basic_auth: None,
        history_log: HistoryLog::new(dir.path().join("history.jsonl")),
        backoff: fast_backoff(),
        keepalive_interval: None,
        events: None,
    };
    let (handle, mut intents) = RealtimeStream::spawn(config);

    let ended = tokio::time::timeout(Duration::from_secs(5), intents.recv())
        .await
        .expect("loop ends within 5s");
    assert!(ended.is_none());

    let end = handle.stop().await;
    assert_eq!(
        end,
        StreamEnd::AuthRejected {
            status: 401,
            message: "stream token required".to_owned(),
        },
        "the envelope message is surfaced, not the raw body"
    );
    assert_eq!(
        connections.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "one attempt, no retry spin"
    );

    wire.abort();
}

#[tokio::test]
async fn connection_transitions_surface_as_connected_and_disconnected_events() {
    // Handshake → Connected; a server shutdown drops the stream →
    // Disconnected (and every failed retry during the outage is one too);
    // a restarted server accepts the reconnect → Connected again, and a
    // fresh push surfaces — the event face matches reality throughout.
    let dir = tempfile::tempdir().expect("temp dir");
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
    let (config, mut events) = {
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(64);
        let config = StreamConfig {
            base_url: server.base_url.clone(),
            stream_token: minted.secret.clone(),
            basic_auth: None,
            history_log: HistoryLog::new(dir.path().join("history.jsonl")),
            backoff: fast_backoff(),
            keepalive_interval: None,
            events: Some(events_tx),
        };
        (config, events_rx)
    };
    let (handle, mut intents) = RealtimeStream::spawn(config);

    assert_eq!(next_event(&mut events).await, StreamEvent::Connected);

    // The outage: the server-initiated close (§3.5) surfaces as
    // Disconnected, and so does every failed reconnect attempt below it.
    server.shutdown().await;
    let first = next_event(&mut events).await;
    assert!(
        matches!(first, StreamEvent::Disconnected { .. }),
        "the drop must surface as Disconnected: {first:?}"
    );

    // The restarted server on the same port and store: the loop retries
    // through the downtime and reconnects — Connected again.
    let server = common::TestServer::spawn_on(
        std::net::TcpListener::bind(addr).expect("rebind same port"),
        dir.path().to_path_buf(),
        |_| {},
    )
    .await;
    assert_eq!(next_event(&mut events).await, StreamEvent::Connected);

    // Connected is not wishful thinking: a push surfaces through the
    // live stream.
    let response = reqwest::Client::new()
        .post(format!("{}/push", server.base_url))
        .json(&serde_json::json!({ "device_key": device.device_key, "body": "after reconnect" }))
        .send()
        .await
        .expect("POST /push");
    assert!(response.status().is_success());
    let intent = tokio::time::timeout(Duration::from_secs(5), intents.recv())
        .await
        .expect("intent within 5s")
        .expect("intent after the reconnect");
    assert_eq!(intent.body, "after reconnect");

    handle.stop().await;
    server.shutdown().await;
}
