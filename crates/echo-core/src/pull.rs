//! The one-shot Pull (GLOSSARY): recover a Subscription's retained Messages
//! from the server's store-and-forward store — one stream connection with
//! an explicit cursor (`0` = everything retained, protocol §3.5) — and
//! land every replayed Message **silently** in the Subscription's History
//! Log: no notification intents, no notifications. The same SSE client as
//! the receive loop ([`crate::stream`]) with notification output off —
//! connection, parsing, and dedup are that loop's machinery, shared here.
//!
//! Termination: the server hands off from replay to live delivery without
//! any on-the-wire marker (§3.5 — replayed and live events are
//! indistinguishable), so the pull closes after [`PullConfig::quiet_window`]
//! without a new replayed event. A rerun heals anything missed: dedup
//! suppresses what already landed.

use std::time::Duration;

use crate::history::HistoryLog;
use crate::stream::{ConnectFailure, Frame, PushEventWire, SseParser, connect_stream, stream_url};

/// Everything one Pull needs for one Subscription.
#[derive(Debug, Clone)]
pub struct PullConfig {
    /// The server base URL (e.g. `http://127.0.0.1:8080`).
    pub base_url: String,
    /// The Stream Token secret, presented as `X-Stream-Token`.
    pub stream_token: String,
    /// Operator Basic Auth, sent alongside the Stream Token (§1.4).
    pub basic_auth: Option<crate::stream::BasicAuth>,
    /// The Subscription's History Log: where replayed Messages land.
    pub history_log: HistoryLog,
    /// The replay cursor (§3.5): `0` replays everything currently
    /// retained; ids at or below the History Log's own cursor are
    /// suppressed by dedup, so overlapping pulls add nothing.
    pub cursor: i64,
    /// How long without a new replayed event before the one-shot
    /// connection closes — the replay-drained heuristic.
    pub quiet_window: Duration,
}

impl PullConfig {
    /// A Pull for one Subscription: cursor `0` (everything retained) and
    /// the default one-second quiet window.
    pub fn new(
        base_url: impl Into<String>,
        stream_token: impl Into<String>,
        history_log: HistoryLog,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            stream_token: stream_token.into(),
            basic_auth: None,
            history_log,
            cursor: 0,
            quiet_window: Duration::from_secs(1),
        }
    }
}

/// What one Pull did: how many Messages landed in the History Log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PullReport {
    /// The number of History Log lines the Pull appended — the count the
    /// CLI reports. Zero on an overlapping pull (dedup) or when nothing
    /// was retained past the cursor.
    pub ingested: usize,
}

/// Failures of a Pull: transport problems, a credential rejection, or a
/// History Log write failure — durability is broken, so the pull stops
/// rather than report Messages it did not land.
#[derive(Debug, thiserror::Error)]
pub enum PullError {
    /// The request never completed (connection refused, DNS, timeout).
    #[error("transport failure talking to the bark server: {0}")]
    Transport(#[from] reqwest::Error),
    /// The server rejected the credentials (401/418): retrying the same
    /// credentials would spin — re-subscribe or fix the Basic Auth
    /// credentials.
    #[error(
        "bark server rejected the read credentials (HTTP {status}): re-subscribe or fix the Basic Auth credentials"
    )]
    AuthRejected { status: u16, message: String },
    /// Any other non-success status.
    #[error("bark server answered {status}: {message}")]
    Unexpected { status: u16, message: String },
    /// The History Log could not be written.
    #[error("cannot append to the History Log: {0}")]
    HistoryWriteFailed(String),
}

/// Run one Pull: connect with the cursor, replay into the History Log
/// silently, and report the ingested count. The cursor ride-along and the
/// dedup floor mirror the receive loop exactly — the dedup path is shared.
pub async fn pull(config: PullConfig) -> Result<PullReport, PullError> {
    // The dedup floor: the explicit cursor AND the History Log's own
    // cursor — the same rule the receive loop applies (§3.7: delivery is
    // at-least-once, and the client owns exactly-once, by per-Device id).
    let floor = config
        .history_log
        .cursor()
        .ok()
        .flatten()
        .unwrap_or(0)
        .max(config.cursor);

    let url = stream_url(&config.base_url);
    let http = reqwest::Client::new();
    // The presented replay cursor is the floor itself: ids the History Log
    // already holds are never re-requested — an overlapping pull replays
    // nothing (and anything the server sent anyway would hit the same
    // dedup floor below).
    let mut response = match connect_stream(
        &http,
        &url,
        &config.stream_token,
        config.basic_auth.as_ref(),
        Some(floor),
    )
    .await
    {
        Ok(response) => response,
        Err(ConnectFailure::Transport(err)) => return Err(PullError::Transport(err)),
        Err(ConnectFailure::AuthRejected { status, message }) => {
            return Err(PullError::AuthRejected { status, message });
        }
        Err(ConnectFailure::Unexpected { status, message }) => {
            return Err(PullError::Unexpected { status, message });
        }
    };

    // Read the body: handshake comment, then replay (§3.5) — replayed and
    // live events are indistinguishable, so everything that arrives inside
    // the quiet window lands in the History Log.
    let mut parser = SseParser::default();
    // The quiet deadline: every replayed event extends it; comments (the
    // `: connected` handshake, keep-alives) do not — they say nothing
    // about the replay.
    let mut deadline = tokio::time::Instant::now() + config.quiet_window;
    // The highest id whose Message landed in this pull.
    let mut processed = floor;
    let mut ingested: usize = 0;
    loop {
        let chunk = match tokio::time::timeout_at(deadline, response.chunk()).await {
            Ok(Ok(Some(chunk))) => chunk,
            // EOF: the server ended the stream — the pull completes with
            // what it landed. A transport error mid-read ends it too:
            // rerunning the pull heals, dedup suppresses the overlap.
            Ok(_) => break,
            // The quiet window closed with no new event: the replay has
            // drained (§3.5 replays back-to-back under one lock).
            Err(_) => break,
        };
        for frame in parser.push(&chunk) {
            let Frame::Event { event, id, data } = frame else {
                continue; // handshake and keep-alive comments: ignored
            };
            // Only `message` events exist today; unknown event types
            // coexist on the stream until known (§3.4).
            if event.as_deref().unwrap_or("message") != "message" {
                continue;
            }
            let Some(id) = id else { continue };
            deadline = tokio::time::Instant::now() + config.quiet_window;
            let Ok(event) = serde_json::from_str::<PushEventWire>(&data) else {
                continue;
            };
            if id <= processed {
                // At-least-once delivery (§3.7): the client owns
                // exactly-once, by per-Device id — the same dedup rule as
                // the receive loop.
                continue;
            }
            let intent = event.into_intent(id);
            if let Err(err) = config.history_log.append(&intent) {
                return Err(PullError::HistoryWriteFailed(err.to_string()));
            }
            processed = id;
            ingested += 1;
        }
    }
    Ok(PullReport { ingested })
}
