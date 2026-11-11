//! The Realtime Stream receive loop against the embedded real server
//! (ticket #4): handshake → notification intents → History Log. Every
//! assertion is external — intents surfaced on the core channel, History
//! Log contents, server-observable state — never loop internals.

mod common;

use std::time::Duration;

use echo_core::history::HistoryLog;
use echo_core::intent::{ClickAction, NotificationIntent};
use echo_core::stream::{BackoffConfig, RealtimeStream, StreamConfig};

/// Backoff fast enough for tests (the default base is a second).
fn fast_backoff() -> BackoffConfig {
    BackoffConfig {
        base: Duration::from_millis(150),
        cap: Duration::from_secs(2),
    }
}

/// A stream config pointing at the harness with its History Log.
fn config_for(harness: &common::Subscribed) -> StreamConfig {
    StreamConfig {
        base_url: harness.server.base_url.clone(),
        stream_token: harness.stream_token.clone(),
        basic_auth: None,
        history_log: HistoryLog::new(harness.history_path()),
        backoff: fast_backoff(),
    }
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
