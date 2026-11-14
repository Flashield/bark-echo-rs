//! The JSON Lines message vocabulary (ADR-0004): requests and responses
//! carry an id; daemon-pushed events carry none (ticket #11). The
//! status snapshot's shape is the daemon's live per-Subscription state —
//! the same vocabulary the runtime state file holds, so the state file
//! and the channel answer read as one thing (spec story 24: the wire
//! format cannot fork).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

// The History Log line's vocabulary (GLOSSARY) is echo-core's: the
// daemon appends `NotificationIntent` lines to the log, and the channel
// relays appended lines as-is — one type, so the event payload cannot
// fork the log format (spec story 24). Re-exported here so every
// consumer of the channel names it from the channel's crate, with the
// resolved `ClickAction` the lines carry (ticket #15's viewer renders
// them).
pub use echo_core::intent::{ClickAction, NotificationIntent};

/// One client → daemon frame: the request plus the id its response must
/// carry. The first frame on every connection is [`Request::Hello`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestFrame {
    /// The caller-chosen id; the daemon echoes it verbatim.
    pub id: u64,
    pub request: Request,
}

/// The requests the channel answers. Openings are snapshot requests
/// (ADR-0004); the daemon's own pushes — a stream state flip, a History
/// Log append — arrive unsolicited as [`Event`] frames, without an id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    /// The mandatory first frame: announce the client's protocol
    /// version. A mismatch ends the connection with an explanatory
    /// error — never a silent dialect confusion.
    Hello {
        /// The client's [`PROTOCOL_VERSION`].
        version: u32,
    },
    /// Ask for the daemon's live per-Subscription snapshot.
    Status,
    // ---- The Push Composer's send (ticket #16) ----
    /// Send one Push (GLOSSARY) on the Composer's behalf — the send is
    /// one request-response (ADR-0004). The daemon performs it with its
    /// own stored secrets and connection policy (ADR-0004): the send's
    /// context is the named Subscription — its server is where the Push
    /// goes, and the Basic Auth credentials for it, if any, are the
    /// daemon's own (spec story 13) — while the target Device is the
    /// request's explicit Device Key field. Any-server-with-temporary-
    /// credentials sends remain the `push` command's territory (v1).
    SendPush {
        /// The Push itself: target, alert fields, Ext Params.
        push: SendPush,
    },
    // ---- The graceful stop (ticket #12) ----
    /// Ask the daemon for a graceful shutdown (ticket #12, ADR-0004):
    /// the Tray Menu's Quit — and any client that can name the wish.
    /// The daemon answers before it begins stopping, and its graceful
    /// stop closes every connection; the client reads that close as the
    /// stop's announcement. The same path a Ctrl-C rides — one
    /// shutdown vocabulary, not two.
    Shutdown,

    // ---- History Log plane (ticket #15). New planes append their
    // variants here, at the enum's end — the wire is additive, and a
    // section comment keeps parallel tickets' additions from colliding.
    /// Ask for the recent tail of one Subscription's History Log
    /// (ticket #15): the daemon reads its own files — the logs stay
    /// daemon-private (ADR-0004) — and answers with the content
    /// semantics of the CLI's `log --tail N`, over the channel.
    HistorySnapshot {
        /// The Subscription whose History Log to read (its Stream
        /// Token's `token_id`).
        subscription_id: String,
        /// At most this many entries: the bounded snapshot that keeps a
        /// large History Log from weighing on the viewer.
        tail: u32,
    },
}

/// One Composer send's content (ticket #16): the fields of the `push`
/// command (GLOSSARY: a Push is one notification request for one
/// Device), minus the server and credentials — those come from the
/// Subscription the send rides, on the daemon's side. The alert fields
/// and Ext Params pass through exactly as the server's V2 JSON route
/// takes them (protocol §2.2); an Ext Param sharing a name with an
/// alert field rides as-is and the server resolves the overlap.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SendPush {
    /// The Subscription the send rides — the snapshot's key. The
    /// daemon resolves its server (and its Basic Auth, if stored).
    pub subscription_id: String,
    /// The target Device's Device Key — the Composer's explicit field,
    /// any Device the user can name (mirroring `push --device-key`).
    pub device_key: String,
    /// The notification's title, when the Composer has one.
    pub title: Option<String>,
    /// The notification's subtitle, when the Composer has one.
    pub subtitle: Option<String>,
    /// The notification's content, when the Composer has one.
    pub body: Option<String>,
    /// The Ext Params (GLOSSARY) — `url`, `group`, `sound`, and any
    /// other key-value pair, passed through verbatim (protocol §2.2).
    pub ext_params: BTreeMap<String, String>,
}

/// The daemon-pushed events (ADR-0004, ticket #11): frames without an
/// id, interleaved with the request/response traffic on the same
/// connection. The client cannot answer an event — it just learns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// A Subscription's Realtime Stream flipped state — pushed at the
    /// same moment the runtime records the flip.
    StreamState {
        /// The Subscription the state belongs to (its Stream Token's
        /// `token_id`).
        subscription_id: String,
        /// The Subscription's client label, so a reader can name the
        /// line even before (or without) taking a snapshot.
        label: String,
        /// The state the stream flipped to.
        state: StreamState,
    },
    /// A processed Push landed in the Subscription's History Log —
    /// announced only after the line is durably on disk, carrying the
    /// line itself: the exact vocabulary the log file holds.
    HistoryAppended {
        /// The Subscription the line was appended to.
        subscription_id: String,
        /// The Subscription's client label.
        label: String,
        /// The appended History Log line.
        entry: NotificationIntent,
    },
}

/// One daemon → client frame: the response plus the answered request's
/// id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseFrame {
    /// The answered request's id, verbatim.
    pub id: u64,
    pub response: Response,
}

/// The responses the daemon sends. Every one names itself with a type
/// tag; an error is a first-class answer, not a dropped connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    /// The handshake's answer: the daemon's protocol version.
    Hello {
        /// The daemon's [`PROTOCOL_VERSION`].
        version: u32,
    },
    /// The live snapshot [`Request::Status`] asked for.
    Status { snapshot: StatusSnapshot },
    /// The daemon accepted [`Request::Shutdown`] and is beginning its
    /// graceful stop (ticket #12). The answer leaves first; every
    /// connection closes as the stop proceeds, and the daemon exits
    /// with a clean code — one a `Restart=on-failure` /
    /// `KeepAlive SuccessfulExit=false` supervisor does not resurrect.
    ShuttingDown,
    /// The request could not be answered as phrased; the message says
    /// what the daemon does know.
    Error { message: String },
    // ---- The Push Composer's send (ticket #16) ----
    /// The send [`Request::SendPush`] asked for was attempted: this is
    /// the bark server's verdict on it, verbatim. A send the daemon
    /// could not even attempt (unknown Subscription, unreachable
    /// server) is [`Response::Error`] instead — no verdict, no lie.
    PushSent {
        /// How the send ended.
        outcome: SendPushOutcome,
    },

    // ---- History Log plane (ticket #15), matching the request plane's
    // section above.
    /// The recent History Log lines [`Request::HistorySnapshot`] asked
    /// for, oldest last — the log file's own line vocabulary (the same
    /// one [`Event::HistoryAppended`] carries), so the viewer renders
    /// snapshot lines and live appends as one thing.
    HistorySnapshot {
        /// The entries, oldest last.
        entries: Vec<NotificationIntent>,
    },
}

/// The bark server's verdict on a Composer send (ticket #16). The
/// rejection keeps the server's own code and message — the window
/// shows what the server said, never a paraphrase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SendPushOutcome {
    /// The server accepted the Push (envelope code 200) — the target
    /// Device's Realtime Streams fan out from there.
    Sent,
    /// The server refused it: the envelope's code when it answered with
    /// one (a bad Device Key answers `400 failed to get device
    /// token...`), or `418` for the plain-text Basic Auth refusal
    /// (protocol §1.4), and the message verbatim.
    Rejected {
        /// The server's envelope/status code, when one was seen.
        code: Option<u16>,
        /// The server's own message.
        message: String,
    },
}

/// The daemon's live view of every Subscription — the snapshot
/// [`Request::Status`] returns, and the same shape the runtime state
/// file (`daemon-state.json`) persists: one vocabulary, two faces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusSnapshot {
    /// The daemon process's pid — ties the snapshot to `ps` output.
    pub pid: u32,
    /// Unix seconds when this daemon instance started.
    pub started_at: i64,
    /// Per-Subscription lines, keyed by Subscription id (the Stream
    /// Token's `token_id`).
    pub subscriptions: BTreeMap<String, SubscriptionRuntime>,
}

/// One Subscription's runtime line in the snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionRuntime {
    /// The Subscription's client label — the Tray Menu's line name
    /// (ticket #11). `#[serde(default)]` because state files written
    /// before labels rode the snapshot must still parse; the next daemon
    /// run rewrites them with labels.
    #[serde(default)]
    pub label: String,
    /// How the stream is doing right now.
    pub state: StreamState,
    /// The id of the last processed notification — the History Log's
    /// cursor (GLOSSARY).
    pub last_message_id: Option<i64>,
    /// Unix seconds of the last change to this line.
    pub updated_at: i64,
    /// The Subscription's server base URL — public config data (the
    /// config TOML holds non-secret fields only), riding the snapshot
    /// since ticket #13 so the Management Window can list it without the
    /// GUI touching a bark-echo file (ADR-0004). `#[serde(default)]` for
    /// files written before the identity rode the snapshot; the next
    /// daemon run rewrites them.
    #[serde(default)]
    pub server: String,
    /// The Stream Token's public hint (first 12 characters + `...`,
    /// config.rs) — names the token without re-exposing it (protocol
    /// §3.2); the secret itself never leaves the daemon (spec story 13).
    /// Same `#[serde(default)]` story as `server`.
    #[serde(default)]
    pub token_hint: String,
}

/// Per-Subscription stream state as the runtime sees it (ticket #5's
/// vocabulary; ADR-0004 makes it the channel's word too). The serde
/// shape is the runtime state file's — the daemon has written these
/// tags since before the channel existed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum StreamState {
    /// Spawned, no `: connected` handshake yet.
    Starting,
    /// Handshake completed — pushes published now are delivered (§3.3).
    Running,
    /// The connection dropped or a connect failed; retrying with backoff.
    Disconnected {
        /// What happened, as the receive loop phrased it.
        detail: String,
    },
    /// 401 `stream token invalid` (§5): actionable — re-subscribe this
    /// Subscription (spec story 21). Never retried.
    TokenInvalid,
    /// The loop stopped for another reason (Basic Auth teapot, History
    /// Log failure). The detail names it.
    Failed {
        /// What stopped the loop, as the receive loop phrased it.
        detail: String,
    },
}

impl StreamState {
    /// The state's name — the word `status` prints.
    pub fn as_str(&self) -> &'static str {
        match self {
            StreamState::Starting => "starting",
            StreamState::Running => "running",
            StreamState::Disconnected { .. } => "disconnected",
            StreamState::TokenInvalid => "token invalid",
            StreamState::Failed { .. } => "failed",
        }
    }
}
