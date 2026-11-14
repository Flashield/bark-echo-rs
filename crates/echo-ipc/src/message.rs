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
// consumer of the channel names it from the channel's crate.
pub use echo_core::intent::NotificationIntent;

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

    // --- Subscription CRUD (ticket #14, ADR-0004's hot-apply): the
    // --- daemon is the single writer of bark-echo state while it runs,
    // --- so changes ride the channel as requests — it registers/mints
    // --- on the server, stores the secrets, writes the config, and
    // --- starts/stops that stream's supervisor immediately.
    /// Add a Subscription (the CLI's `subscribe add`, the window's add
    /// form): register or mirror a Device, mint a labelled Stream
    /// Token, and start the Realtime Stream hot.
    SubscribeAdd {
        /// The bark server base URL, e.g. `http://127.0.0.1:8080`.
        server: String,
        /// Where the Subscription's Device comes from.
        source: AddSource,
        /// The client label naming the Stream Token (GLOSSARY) —
        /// required; the daemon refuses an empty one before touching
        /// the server.
        label: String,
        /// The operator Basic Auth credentials (§1.4) to present and
        /// store; `None` rides the already-stored credentials for that
        /// server. The secrets stay in the daemon (spec story 13).
        basic_auth: Option<BasicAuthCredentials>,
    },
    /// Remove one Subscription by id (its Stream Token's `token_id`,
    /// as `subscribe list` and the snapshot show it): its Realtime
    /// Stream stops, its secrets are erased, the config is updated.
    SubscribeRemove {
        /// The Subscription to remove.
        id: String,
    },
}

/// Where a new Subscription's Device comes from (GLOSSARY.md: one
/// Subscription mirrors exactly one Device) — the wire face of the CLI
/// `--own` / `--device-key` pair and the window's add form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AddSource {
    /// Register a fresh Stream Device for this desktop and push from
    /// it — the minted Device Key is the outcome.
    Own,
    /// Mirror an existing Device by its Device Key — e.g. a phone; the
    /// key is the user's own.
    DeviceKey {
        /// The Device Key to mirror.
        device_key: String,
    },
}

/// The operator Basic Auth credentials (protocol §1.4) a client hands
/// the daemon to present and store. The daemon keeps the password in
/// the secret store; the config file carries the non-secret user and
/// marker only (spec story 18). An empty field means "not given" — the
/// credentials the CLI flags build, since a server may enable the check
/// on user and password independently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BasicAuthCredentials {
    /// The operator Basic Auth user.
    pub user: String,
    /// The operator Basic Auth password.
    pub password: String,
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

    // --- Subscription CRUD (ticket #14): hot-apply announces its
    // --- effects, so every watching client — the Tray Menu, a second
    // --- Management Window — follows without a poll. The add needs no
    // --- event of its own: the stream's Starting flip arrives as a
    // --- [`Event::StreamState`], which carries the label and grows the
    // --- line in place.
    /// A Subscription was removed while clients watched: its line
    /// leaves the Tray Menu and the window at once — a removed
    /// Subscription's name must not linger as a stale row.
    SubscriptionRemoved {
        /// The id of the Subscription that is gone.
        subscription_id: String,
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
    /// The request could not be answered as phrased; the message says
    /// what the daemon does know.
    Error { message: String },

    // --- Subscription CRUD (ticket #14): the add/remove answers are
    // --- typed outcomes; a refusal is [`Response::Error`] with the
    // --- reason as its message — a refused add never half-applied.
    /// A [`Request::SubscribeAdd`] succeeded: the Subscription exists
    /// and its Realtime Stream is starting. The Device Key rides the
    /// answer — the push target for scripts, shown to the user here
    /// and never persisted in any client.
    SubscriptionAdded {
        /// The new Subscription's id (its Stream Token's `token_id`).
        id: String,
        /// The label the Stream Token was minted with.
        label: String,
        /// The server the Subscription subscribes to.
        server: String,
        /// First 12 characters of the Stream Token + `...` — the hint
        /// that names the token without re-exposing it (§3.2).
        token_hint: String,
        /// The Subscription's Device Key — the push target.
        device_key: String,
    },
    /// A [`Request::SubscribeRemove`] succeeded: the stream stopped,
    /// the secrets are erased, the config updated.
    SubscriptionRemoved {
        /// The removed Subscription's id.
        id: String,
        /// The label it carried — for the confirmation line.
        label: String,
    },
}

/// What a successful [`Request::SubscribeAdd`] produced — the
/// [`Response::SubscriptionAdded`] fields as one value (ticket #14).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddedSubscription {
    /// The new Subscription's id (its Stream Token's `token_id`).
    pub id: String,
    /// The label the Stream Token was minted with.
    pub label: String,
    /// The server the Subscription subscribes to.
    pub server: String,
    /// First 12 characters of the Stream Token + `...`.
    pub token_hint: String,
    /// The Subscription's Device Key — the push target.
    pub device_key: String,
}

/// What a successful [`Request::SubscribeRemove`] removed — the
/// [`Response::SubscriptionRemoved`] fields as one value (ticket #14).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemovedSubscription {
    /// The removed Subscription's id.
    pub id: String,
    /// The label it carried.
    pub label: String,
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
