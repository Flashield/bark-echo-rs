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
