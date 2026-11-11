//! The Realtime Stream receive loop (ticket #4): hold one SSE connection
//! per Subscription open against `GET /realtime/v1/stream` (protocol §3.3),
//! turn each `message` event into a [`NotificationIntent`], append it to
//! the Subscription's [`HistoryLog`] only after the intent is emitted, and
//! reconnect on any disconnect — including server-initiated lag closes —
//! with jittered exponential backoff and `Last-Event-ID` resume (§3.5).
//!
//! Protocol behavior pinned here (§6, the client checklist):
//!
//! - `X-Stream-Token` header authorizes the read; Basic Auth credentials
//!   (§1.4) ride along when configured.
//! - The first body bytes are the `: connected` comment — the liveness
//!   handshake. Keep-alive comments are ignored. Neither produces intents.
//! - The per-Device monotonic `id:` line is the dedup key and the cursor:
//!   ids at or below the cursor never emit.
//! - Events other than `message` are ignored until known.
//! - An auth rejection (401/418) is terminal: the credential will not heal
//!   by retrying, so the loop stops instead of spin-retrying.
//!
//! The connection, parsing, and dedup machinery below is shared with the
//! one-shot Pull ([`crate::pull`]) — the same SSE client with notification
//! output off (spec: Pull lands replayed Messages silently in the History
//! Log).

use std::time::Duration;

use rand::Rng;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::history::HistoryLog;
use crate::intent::NotificationIntent;

/// The Realtime Stream's URL for one server base (`GET /realtime/v1/stream`,
/// protocol §3.3). Shared by the receive loop and the one-shot
/// [`crate::pull`] — the same SSE client face.
pub(crate) fn stream_url(base_url: &str) -> String {
    format!("{}/realtime/v1/stream", base_url.trim_end_matches('/'))
}

/// Why opening one stream connection failed: a transport problem, a
/// credential rejection (§3.3: 401/418 — retrying cannot heal a bad
/// credential), or another non-success status for the caller to classify.
pub(crate) enum ConnectFailure {
    /// The request never completed (connection refused, DNS, timeout).
    Transport(reqwest::Error),
    /// The server rejected the credentials (401/418).
    AuthRejected { status: u16, message: String },
    /// Any other non-success status.
    Unexpected { status: u16, message: String },
}

/// Open one Realtime Stream connection: `X-Stream-Token` authorizes the
/// read (§3.3), operator Basic Auth rides along when given (§1.4), and a
/// presented replay cursor rides as `Last-Event-ID` (§3.5) — `Some(0)`
/// replays everything retained, `None` opens a fresh stream. Shared by the
/// receive loop and the one-shot [`crate::pull`]: replayed and live events
/// are indistinguishable on the wire (§3.4), so both read the same way.
pub(crate) async fn connect_stream(
    http: &reqwest::Client,
    url: &str,
    stream_token: &str,
    basic_auth: Option<&BasicAuth>,
    cursor: Option<i64>,
) -> Result<reqwest::Response, ConnectFailure> {
    let mut request = http.get(url).header("X-Stream-Token", stream_token);
    if let Some(auth) = basic_auth {
        request = request.basic_auth(&auth.user, Some(&auth.password));
    }
    if let Some(cursor) = cursor {
        request = request.header("Last-Event-ID", cursor.to_string());
    }
    let response = request.send().await.map_err(ConnectFailure::Transport)?;
    let status = response.status().as_u16();
    if status == 401 || status == 418 {
        // A rejection never becomes a token-validity oracle (§3.3).
        return Err(ConnectFailure::AuthRejected {
            status,
            message: response.text().await.unwrap_or_default(),
        });
    }
    if !response.status().is_success() {
        return Err(ConnectFailure::Unexpected {
            status,
            message: response.text().await.unwrap_or_default(),
        });
    }
    Ok(response)
}

/// One Subscription's read credentials for the receive loop.
#[derive(Debug, Clone)]
pub struct BasicAuth {
    /// The operator's Basic Auth user (protocol §1.4).
    pub user: String,
    /// The operator's Basic Auth password.
    pub password: String,
}

/// Reconnect policy: sleep `min(cap, base * 2^attempt)` plus up to 30% of
/// `base` of jitter before each retry; the attempt counter resets after a
/// connection completes its `: connected` handshake.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// The first retry's delay (before jitter).
    pub base: Duration,
    /// The exponential term's ceiling.
    pub cap: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            cap: Duration::from_secs(30),
        }
    }
}

/// Everything the receive loop needs for one Subscription.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// The server base URL (e.g. `http://127.0.0.1:8080`).
    pub base_url: String,
    /// The Stream Token secret, presented as `X-Stream-Token`.
    pub stream_token: String,
    /// Operator Basic Auth, sent alongside the Stream Token (§1.4).
    pub basic_auth: Option<BasicAuth>,
    /// The Subscription's History Log: durability and reconnect cursor.
    pub history_log: HistoryLog,
    /// Reconnect backoff policy.
    pub backoff: BackoffConfig,
}

/// Why the receive loop ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEnd {
    /// Stopped by the owner (`handle.stop()`) or by the intent consumer
    /// going away.
    Stopped,
    /// The server rejected the credentials (401/418): retrying the same
    /// credentials would spin, so the loop surfaced the failure —
    /// re-subscribe or fix the Basic Auth credentials.
    AuthRejected { status: u16, message: String },
    /// The History Log could not be written; durability is broken, so the
    /// loop stopped rather than emit unlogged intents.
    HistoryWriteFailed(String),
}

/// A running receive loop. Stop it with [`StreamHandle::stop`]; the intent
/// output is the channel returned alongside.
#[derive(Debug)]
pub struct StreamHandle {
    task: tokio::task::JoinHandle<StreamEnd>,
}

impl StreamHandle {
    /// Stop the loop and report how it ended. Interrupting mid-event is
    /// safe: an intent either reached the channel and the History Log, or
    /// the event is replayed on the next connect and suppressed by dedup.
    pub async fn stop(self) -> StreamEnd {
        self.task.abort();
        match self.task.await {
            Ok(end) => end,
            // Cancelled by our own abort: never an auth or history failure.
            Err(_) => StreamEnd::Stopped,
        }
    }
}

/// The Realtime Stream client: spawn the receive loop for one Subscription.
#[derive(Debug, Clone)]
pub struct RealtimeStream;

impl RealtimeStream {
    /// Start the loop. Returns the handle plus the channel every processed
    /// Push's [`NotificationIntent`] surfaces on, in delivery order.
    pub fn spawn(config: StreamConfig) -> (StreamHandle, mpsc::Receiver<NotificationIntent>) {
        let (tx, rx) = mpsc::channel(256);
        let task = tokio::spawn(run(config, tx));
        (StreamHandle { task }, rx)
    }
}

/// The receive loop proper.
async fn run(config: StreamConfig, intents: mpsc::Sender<NotificationIntent>) -> StreamEnd {
    let url = stream_url(&config.base_url);
    let http = reqwest::Client::new();
    // Consecutive failed attempts since the last completed handshake.
    let mut attempt: u32 = 0;
    // The highest id whose intent was emitted in this session (ids are
    // per-Device and monotonic, protocol §3.4).
    let mut processed: i64 = 0;
    loop {
        // The History Log's last line is the resume cursor (GLOSSARY);
        // this session's processed max only ever raises it.
        let cursor = config
            .history_log
            .cursor()
            .ok()
            .flatten()
            .unwrap_or(0)
            .max(processed);

        // A cursor of 0 means "nothing processed yet": a fresh stream, no
        // replay header — the daemon's first connect must not storm the
        // user with the server's whole retained backlog as notifications.
        let mut response = match connect_stream(
            &http,
            &url,
            &config.stream_token,
            config.basic_auth.as_ref(),
            (cursor > 0).then_some(cursor),
        )
        .await
        {
            Ok(response) => response,
            // Transport failure: retry after backoff.
            Err(ConnectFailure::Transport(_)) => {
                sleep_backoff(&config.backoff, attempt).await;
                attempt += 1;
                continue;
            }
            // A rejection never heals by retrying: stop and surface it.
            Err(ConnectFailure::AuthRejected { status, message }) => {
                return StreamEnd::AuthRejected { status, message };
            }
            // Any other non-success status: retry after backoff.
            Err(ConnectFailure::Unexpected { .. }) => {
                sleep_backoff(&config.backoff, attempt).await;
                attempt += 1;
                continue;
            }
        };

        // Read the body: handshake comment, then replay and live events —
        // indistinguishable on the wire (§3.4).
        let mut parser = SseParser::default();
        let mut handshake_seen = false;
        loop {
            let chunk = match response.chunk().await {
                Ok(Some(chunk)) => chunk,
                // EOF or transport error: any disconnect reconnects.
                _ => break,
            };
            for frame in parser.push(&chunk) {
                match frame {
                    Frame::Comment(text) => {
                        if !handshake_seen && text.trim() == "connected" {
                            // Liveness handshake (§3.3): the stream is
                            // guaranteed every push published from here on.
                            handshake_seen = true;
                            attempt = 0;
                        }
                        // Keep-alive comments and anything else: ignored.
                    }
                    Frame::Event { event, id, data } => {
                        // Only `message` events exist today; unknown event
                        // types coexist on the stream until known (§3.4).
                        if event.as_deref().unwrap_or("message") != "message" {
                            continue;
                        }
                        let Some(id) = id else { continue };
                        let Ok(event) = serde_json::from_str::<PushEventWire>(&data) else {
                            continue;
                        };
                        if id <= cursor.max(processed) {
                            // At-least-once delivery (§3.7): the client owns
                            // exactly-once, by per-Device id.
                            continue;
                        }
                        let intent = event.into_intent(id);
                        if intents.send(intent.clone()).await.is_err() {
                            // The consumer went away.
                            return StreamEnd::Stopped;
                        }
                        if let Err(err) = config.history_log.append(&intent) {
                            return StreamEnd::HistoryWriteFailed(err.to_string());
                        }
                        processed = id;
                    }
                }
            }
        }
        // Any disconnect — server lag close, server restart, network drop —
        // reconnects with `Last-Event-ID` (§3.5).
        sleep_backoff(&config.backoff, attempt).await;
        attempt = attempt.saturating_add(1);
    }
}

/// One jittered backoff step: `min(cap, base * 2^attempt)` plus up to 30%
/// of that as jitter. The jitter draw happens (and its rng is dropped)
/// before the sleep so no `!Send` state crosses an await.
async fn sleep_backoff(config: &BackoffConfig, attempt: u32) {
    let exponential = config
        .base
        .saturating_mul(2u32.saturating_pow(attempt))
        .min(config.cap);
    let jitter_span = (exponential.as_millis() as u64 * 3) / 10; // ≤ 30%
    let jitter = Duration::from_millis(rand::rng().random_range(0..=jitter_span.max(1)));
    tokio::time::sleep(exponential + jitter).await;
}

/// One SSE frame off the wire: a comment, or an event with its fields.
// Shared with the one-shot Pull's read loop (crate::pull).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Frame {
    /// A `:`-prefixed line (handshake, keep-alive): never an event.
    Comment(String),
    /// An event frame: `event`, `id`, `data` fields (any may be absent).
    Event {
        event: Option<String>,
        id: Option<i64>,
        data: String,
    },
}

/// Incremental SSE byte-stream parser: feed chunks, get complete frames.
/// Tolerant of `\n` and `\r\n` line endings (SSE spec).
// Shared with the one-shot Pull's read loop (crate::pull).
#[derive(Debug, Default)]
pub(crate) struct SseParser {
    buffer: Vec<u8>,
    event: Option<String>,
    id: Option<i64>,
    data_lines: Vec<String>,
    comment: Option<String>,
}

impl SseParser {
    /// Feed one body chunk; return every frame it completed.
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Vec<Frame> {
        self.buffer.extend_from_slice(chunk);
        let mut frames = Vec::new();
        while let Some(pos) = self.buffer.iter().position(|&b| b == b'\n') {
            let mut line: Vec<u8> = self.buffer.drain(..=pos).collect();
            line.pop(); // the '\n'
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            self.line(&line, &mut frames);
        }
        frames
    }

    /// Process one line; blank lines dispatch the accumulated frame.
    fn line(&mut self, line: &[u8], frames: &mut Vec<Frame>) {
        if line.is_empty() {
            // Dispatch whatever this frame accumulated.
            if !self.data_lines.is_empty() || self.id.is_some() || self.event.is_some() {
                frames.push(Frame::Event {
                    event: self.event.take(),
                    id: self.id.take(),
                    data: self.data_lines.join("\n"),
                });
                self.data_lines.clear();
            } else if let Some(comment) = self.comment.take() {
                frames.push(Frame::Comment(comment));
            }
            return;
        }
        if line[0] == b':' {
            // Comment line: keep the first one of the frame.
            let text = String::from_utf8_lossy(&line[1..]).trim().to_owned();
            if self.comment.is_none() {
                self.comment = Some(text);
            }
            return;
        }
        let text = String::from_utf8_lossy(line);
        let text = text.as_ref();
        let (name, value) = match text.split_once(':') {
            Some((name, value)) => (name, value.strip_prefix(' ').unwrap_or(value)),
            None => (text, ""),
        };
        match name {
            "event" => self.event = Some(value.to_owned()),
            // A non-numeric id must be ignored, not treated as absent-id
            // data (SSE spec); the dedup key is decimal (§3.4).
            "id" => {
                if let Ok(id) = value.parse::<i64>() {
                    self.id = Some(id);
                }
            }
            "data" => self.data_lines.push(value.to_owned()),
            _ => {} // retry lines and unknown fields: ignored
        }
    }
}

/// The `data:` JSON of a push event, exactly as the server writes it
/// (protocol §3.4).
#[derive(Debug, Deserialize)]
pub(crate) struct PushEventWire {
    #[serde(default)]
    title: String,
    #[serde(default)]
    subtitle: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    ext_params: serde_json::Map<String, serde_json::Value>,
    created_at: i64,
}

impl PushEventWire {
    /// Resolve the event into the intent the renderer gets (the mapping is
    /// spec'd at the core seam): alert text plus the click action from Ext
    /// Params — `url` wins, `copy` applies only without `url`, a
    /// `ciphertext` push renders the encrypted placeholder.
    pub(crate) fn into_intent(self, id: i64) -> NotificationIntent {
        NotificationIntent::from_push_fields(
            id,
            self.title,
            self.subtitle,
            self.body,
            &self.ext_params,
            self.created_at,
        )
    }
}
