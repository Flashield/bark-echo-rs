//! The mock state: shapes and events lifted 1:1 from `echo-ipc`'s
//! vocabulary (ADR-0004) so the page is exercised against the wire
//! vocabulary the real daemon will speak, not a prototype dialect.
//!
//! Prototype-only deviations, kept visible in the data instead of hidden:
//! - History Log entries carry an extra `origin` field ("received" |
//!   "sent") so a Composer send is distinguishable in the log view; the
//!   real `NotificationIntent` has no such field.
//! - `StreamState` keeps its serde shape (`tag = "kind"`, `content =
//!   "detail"`) but the page's indicator collapses `token_invalid` and
//!   `failed` into one "down" word (running/starting/reconnecting/down),
//!   per ticket #23's indicator set.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

/// The demo management token (#24's resolution: the token is the root
/// credential; `POST /v1/session` exchanges it for an HttpOnly +
/// SameSite=Strict session cookie). Mock: any token equal to this works.
pub const MANAGEMENT_TOKEN: &str = "demo";

/// Session cookie name and TTL. The real daemon uses a short TTL; the
/// mock matches the mechanics, not the ops posture.
pub const SESSION_COOKIE: &str = "bark_echo_session";
pub const SESSION_TTL_SECS: u64 = 8 * 3600;

/// Realtime event fan-out. Payloads are pre-serialized JSON envelopes
/// (one `String` per event) so every subscriber receives identical
/// bytes. Capacity bounds memory; a lagging SSE client is told to
/// resync rather than replayed (the mock keeps no event store).
#[derive(Clone)]
pub struct Hub {
    tx: broadcast::Sender<String>,
}

impl Hub {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        Self { tx }
    }

    /// Publish one event envelope to every open SSE stream.
    pub fn publish(&self, event: &serde_json::Value) {
        // No receivers is the normal no-page-open case; lag is the
        // subscriber's business (its stream announces `resync`).
        let _ = self.tx.send(event.to_string());
    }

    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.tx.subscribe()
    }
}

/// Per-Subscription stream state — the serde shape `echo-ipc`'s
/// `StreamState` writes (the runtime state file's tags).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum StreamState {
    /// Spawned, no `: connected` handshake yet.
    Starting,
    /// Handshake completed — pushes published now are delivered.
    Running,
    /// The connection dropped or a connect failed; retrying with backoff.
    Disconnected { detail: String },
    /// 401 `stream token invalid`: actionable — re-subscribe. Never retried.
    TokenInvalid,
    /// The loop stopped for another reason; the detail names it.
    Failed { detail: String },
}

/// The resolved click action of a processed Push (core seam vocabulary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClickAction {
    OpenUrl { url: String },
    Copy { text: String },
}

/// One History Log line — the `NotificationIntent` fields plus the
/// prototype-only `origin` marker (see module doc).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// The per-Device monotonic message id — the log line key.
    pub id: i64,
    pub title: String,
    pub subtitle: String,
    pub body: String,
    pub click_action: Option<ClickAction>,
    /// Unix seconds, assigned by the server at persistence.
    pub created_at: i64,
    /// Prototype-only: "received" (simulated stream) or "sent" (Composer).
    pub origin: String,
}

/// One Subscription's runtime line in the status snapshot — the
/// `SubscriptionRuntime` fields (label, state, cursor, server,
/// token_hint; no secrets ride the snapshot). The Device Key only
/// travels in the `AddedSubscription` response, like the real add.
#[derive(Debug, Clone)]
pub struct SubLine {
    pub label: String,
    pub server: String,
    pub token_hint: String,
    pub state: StreamState,
    pub last_message_id: Option<i64>,
    pub updated_at: i64,
    /// Simulator role: `Flaky` cycles reconnecting/down so the page
    /// exercises every indicator; `Steady` mostly stays running.
    pub flaky: bool,
}

impl SubLine {
    /// The snapshot line — exactly `SubscriptionRuntime`'s fields.
    pub fn runtime_json(&self) -> serde_json::Value {
        serde_json::json!({
            "label": self.label,
            "state": self.state,
            "last_message_id": self.last_message_id,
            "updated_at": self.updated_at,
            "server": self.server,
            "token_hint": self.token_hint,
        })
    }

    /// The History Log line for a received Push (real intents are built
    /// by `NotificationIntent::from_push_fields`; the mock reuses the
    /// same url-wins/copy-fallback resolution).
    pub fn received_entry(
        &self,
        id: i64,
        title: &str,
        subtitle: &str,
        body: &str,
        click: Option<ClickAction>,
    ) -> LogEntry {
        LogEntry {
            id,
            title: title.to_string(),
            subtitle: subtitle.to_string(),
            body: body.to_string(),
            click_action: click,
            created_at: unix_now(),
            origin: "received".to_string(),
        }
    }
}

pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Everything mutable the handlers and the simulator touch.
pub struct MockState {
    pub hub: Hub,
    pub started_at: i64,
    /// Valid sessions → expiry instants (in-memory only; rotate kills
    /// every session in the real design — the mock has no rotate).
    pub sessions: Mutex<std::collections::HashMap<String, std::time::Instant>>,
    /// Consecutive failed token entries (brute-force guard, #24 point 5).
    pub failures: std::sync::atomic::AtomicU32,
    /// Monotonic counter for mock Subscription ids.
    pub next_sub: std::sync::atomic::AtomicU64,
    pub inner: Mutex<Inner>,
}

pub struct Inner {
    pub subs: BTreeMap<String, SubLine>,
    /// One History Log per Subscription (GLOSSARY), oldest last.
    pub logs: BTreeMap<String, VecDeque<LogEntry>>,
}

impl MockState {
    pub fn new() -> std::sync::Arc<Self> {
        let mut inner = Inner {
            subs: BTreeMap::new(),
            logs: BTreeMap::new(),
        };
        seed(
            &mut inner,
            "mock-0001",
            "Home Server",
            "https://push.example.net",
            "tk_9f2e1a3b4c5d",
            false,
            StreamState::Running,
        );
        seed(
            &mut inner,
            "mock-0002",
            "Office Relay",
            "https://bark.office.example.com",
            "tk_5c8d0e2f6a1b",
            false,
            StreamState::Running,
        );
        seed(
            &mut inner,
            "mock-0003",
            "VPS Monitor",
            "https://bark.myvps.example.org",
            "tk_c3a9b7d1e4f0",
            true,
            StreamState::Disconnected {
                detail: "connect timeout after 5s; retrying with backoff".to_string(),
            },
        );
        std::sync::Arc::new(Self {
            hub: Hub::new(),
            started_at: unix_now(),
            sessions: Mutex::new(std::collections::HashMap::new()),
            failures: std::sync::atomic::AtomicU32::new(0),
            next_sub: std::sync::atomic::AtomicU64::new(4),
            inner: Mutex::new(inner),
        })
    }

    /// Flip one Subscription's stream state and announce it at the same
    /// moment the runtime records the flip.
    pub fn set_stream_state(&self, sub_id: &str, state: StreamState) {
        let envelope;
        {
            let mut inner = self.inner.lock().unwrap();
            let Some(line) = inner.subs.get_mut(sub_id) else {
                return;
            };
            line.state = state.clone();
            line.updated_at = unix_now();
            envelope = serde_json::json!({
                "type": "stream_state",
                "subscription_id": sub_id,
                "label": line.label.clone(),
                "state": state,
            });
        }
        self.hub.publish(&envelope);
    }
}

/// Seed one Subscription with a populated History Log so the log view
/// has content on open.
#[allow(clippy::too_many_arguments)]
fn seed(
    inner: &mut Inner,
    id: &str,
    label: &str,
    server: &str,
    token_prefix: &str,
    flaky: bool,
    state: StreamState,
) {
    let now = unix_now();
    let line = SubLine {
        label: label.to_string(),
        server: server.to_string(),
        token_hint: format!("{}...", &token_prefix[..token_prefix.len().min(12)]),
        state,
        last_message_id: None,
        updated_at: now,
        flaky,
    };
    let mut log = VecDeque::new();
    let mut next_id = 101i64;
    let canned = canned_pushes();
    for (offset, (title, subtitle, body, click)) in canned.iter().enumerate() {
        let entry = LogEntry {
            id: next_id,
            title: (*title).to_string(),
            subtitle: (*subtitle).to_string(),
            body: (*body).to_string(),
            click_action: click.clone(),
            created_at: now - ((canned.len() - offset) as i64 * 420),
            origin: "received".to_string(),
        };
        next_id += 1;
        log.push_back(entry);
    }
    line_last_id(&mut inner.subs, id, line, next_id - 1);
    inner.logs.insert(id.to_string(), log);
}

fn line_last_id(subs: &mut BTreeMap<String, SubLine>, id: &str, mut line: SubLine, last_id: i64) {
    line.last_message_id = Some(last_id);
    subs.insert(id.to_string(), line);
}

/// Rotating fake push content — realistic desktop-client noise, two CJK
/// entries on purpose so the driver sees CJK rendering in the log too.
type Canned = (
    &'static str,
    &'static str,
    &'static str,
    Option<ClickAction>,
);

/// Built at runtime (ClickAction carries Strings; a `static` would need
/// const construction).
fn canned_pushes() -> Vec<Canned> {
    vec![
        (
            "CI",
            "bark-echo-rs",
            "Build #214 passed — 3 platforms green, artifacts uploaded",
            Some(ClickAction::OpenUrl {
                url: "https://ci.example.net/runs/214".to_string(),
            }),
        ),
        (
            "Backup",
            "nas.home",
            "Nightly snapshot finished: 41.2 GB written, 3 files corrected",
            None,
        ),
        (
            "监控",
            "vps-frankfurt",
            "CPU 92% for 5 min — load average 4.8",
            Some(ClickAction::OpenUrl {
                url: "https://grafana.example.net/d/vps".to_string(),
            }),
        ),
        (
            "GitLab",
            "pipeline #5812",
            "Deploy to staging succeeded — smoke tests green",
            Some(ClickAction::OpenUrl {
                url: "https://staging.example.net".to_string(),
            }),
        ),
        (
            "磁盘警告",
            "nas.home",
            "/srv free space below 10% (42 GB left)",
            None,
        ),
        (
            "Alert",
            "uptime-robot",
            "api.example.net answered 502 three checks in a row",
            Some(ClickAction::OpenUrl {
                url: "https://api.example.net/health".to_string(),
            }),
        ),
        (
            "下载完成",
            "workstation",
            "ubuntu-26.04-desktop.iso finished (5.8 GB)",
            None,
        ),
        (
            "Calendar",
            "reminder-bot",
            "Standup in 10 minutes",
            Some(ClickAction::Copy {
                text: "standup notes: https://notes.example.net/standup".to_string(),
            }),
        ),
    ]
}

/// Backoff details the reconnecting indicator cycles through.
static DROP_DETAILS: [&str; 3] = [
    "connection reset by peer; retrying with backoff",
    "connect timeout after 5s; retrying with backoff",
    "stream ended (server restart); resuming from cursor",
];

/// The simulated liveness: a fake Push every 4s on a rotating running
/// Subscription (its History Log append lands over SSE, no reload), and
/// a stream-state flip pass every 8s — the flaky Subscription cycles
/// running → reconnecting → running → down so every indicator shows up
/// within half a minute, and a steady Subscription takes an occasional
/// brief hiccup.
pub fn spawn_simulator(state: std::sync::Arc<MockState>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(4));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut push_cursor: usize = 0;
        let mut flip_cursor: usize = 0;
        let mut tick_count: u64 = 0;
        loop {
            tick.tick().await;
            emit_fake_push(&state, &mut push_cursor);
            tick_count += 1;
            if tick_count.is_multiple_of(2) {
                flip_states(&state, &mut flip_cursor);
            }
        }
    });
}

/// One fake push: land it on the next running Subscription. Received
/// pushes only arrive while connected — a `starting`/`disconnected`
/// stream receives nothing (that honesty is the point of the states).
fn emit_fake_push(state: &std::sync::Arc<MockState>, cursor: &mut usize) {
    let mut inner = state.inner.lock().unwrap();
    let running: Vec<String> = inner
        .subs
        .iter()
        .filter(|(_, l)| l.state == StreamState::Running)
        .map(|(id, _)| id.clone())
        .collect();
    if running.is_empty() {
        return;
    }
    let sub_id = running[*cursor % running.len()].clone();
    *cursor += 1;
    let canned = canned_pushes();
    let (title, subtitle, body, click) = {
        let c = &canned[*cursor % canned.len()];
        (c.0, c.1, c.2, c.3.clone())
    };
    let line = inner.subs.get(&sub_id).expect("picked from live map");
    let id = line.last_message_id.unwrap_or(100) + 1;
    let entry = line.received_entry(id, title, subtitle, body, click);
    // Inline the append (already holding the lock).
    let sub = inner.subs.get_mut(&sub_id).expect("same line");
    sub.last_message_id = Some(id);
    sub.updated_at = unix_now();
    let label = sub.label.clone();
    let log = inner.logs.entry(sub_id.clone()).or_default();
    log.push_back(entry.clone());
    if log.len() > 200 {
        log.pop_front();
    }
    drop(inner);
    state.hub.publish(&serde_json::json!({
        "type": "history_appended",
        "subscription_id": sub_id,
        "label": label,
        "entry": entry,
    }));
}

/// One flip pass: the flaky Subscription turns over every pass (a fixed
/// running → reconnecting → running → down cycle, so all indicators show
/// up within half a minute), and a cursor picks one steady Subscription
/// for a rare brief hiccup — never two drops in the same instant, and
/// never more than one steady line down at a time (the drop heals itself
/// after one pass).
fn flip_states(state: &std::sync::Arc<MockState>, cursor: &mut usize) {
    let (ids, flaky_id): (Vec<String>, Option<String>) = {
        let inner = state.inner.lock().unwrap();
        let ids: Vec<String> = inner.subs.keys().cloned().collect();
        let flaky_id = inner
            .subs
            .iter()
            .find(|(_, l)| l.flaky)
            .map(|(id, _)| id.clone());
        (ids, flaky_id)
    };
    if ids.is_empty() {
        return;
    }
    // Steady lines only — the flaky line gets its own pass below.
    let steady: Vec<&String> = ids
        .iter()
        .filter(|id| Some((*id).clone()) != flaky_id)
        .collect();
    if let Some(id) = steady.get(*cursor % steady.len().max(1)) {
        flip_steady(state, id, *cursor);
    }
    *cursor += 1;
    // The flaky line turns over every pass so the indicator changes are
    // visible within a minute of watching.
    if let Some(id) = &flaky_id {
        flip_flaky(state, id, *cursor);
    }
}

fn flip_steady(state: &std::sync::Arc<MockState>, id: &str, cursor: usize) {
    let current = {
        let inner = state.inner.lock().unwrap();
        inner.subs.get(id).map(|l| l.state.clone())
    };
    match current {
        // Rare hiccup (about one per minute of watching): drop for one
        // pass, then the heal task reconnects it.
        Some(StreamState::Running) if cursor.is_multiple_of(6) && cursor != 0 => {
            state.set_stream_state(
                id,
                StreamState::Disconnected {
                    detail: DROP_DETAILS[cursor % DROP_DETAILS.len()].to_string(),
                },
            );
            spawn_heal(state.clone(), id.to_string(), Duration::from_secs(8));
        }
        // Heal stuck starts and hiccups the heal task missed.
        Some(StreamState::Disconnected { .. })
        | Some(StreamState::Starting)
        | Some(StreamState::TokenInvalid)
        | Some(StreamState::Failed { .. }) => {
            state.set_stream_state(id, StreamState::Running);
        }
        _ => {}
    }
}

fn flip_flaky(state: &std::sync::Arc<MockState>, id: &str, cursor: usize) {
    // A fixed cycle: running, reconnecting, running, down — every
    // indicator reachable within four passes.
    let next = match cursor % 4 {
        1 => StreamState::Disconnected {
            detail: DROP_DETAILS[cursor % DROP_DETAILS.len()].to_string(),
        },
        3 => StreamState::TokenInvalid,
        _ => StreamState::Running,
    };
    state.set_stream_state(id, next);
}

/// Heal a steady Subscription's simulated drop: after `delay`, flip
/// Disconnected → Running (the supervisor's backoff reconnected it).
fn spawn_heal(state: std::sync::Arc<MockState>, sub_id: String, delay: Duration) {
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let dropped = {
            let inner = state.inner.lock().unwrap();
            inner
                .subs
                .get(&sub_id)
                .is_some_and(|l| matches!(l.state, StreamState::Disconnected { .. }))
        };
        if dropped {
            state.set_stream_state(&sub_id, StreamState::Running);
        }
    });
}

/// The subscribe_add follow-through: the new line starts at `starting`
/// and its flip to running arrives as the announcement (ADR-0004). The
/// mock sleeps the supervisor's handshake, then flips — unless the
/// Subscription was removed in between.
pub fn spawn_handshake(state: std::sync::Arc<MockState>, sub_id: String) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(2500)).await;
        let still_starting = {
            let inner = state.inner.lock().unwrap();
            inner
                .subs
                .get(&sub_id)
                .is_some_and(|l| l.state == StreamState::Starting)
        };
        if still_starting {
            state.set_stream_state(&sub_id, StreamState::Running);
        }
    });
}
