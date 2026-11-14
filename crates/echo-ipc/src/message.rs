//! The JSON Lines message vocabulary (ADR-0004): requests and responses
//! carry an id; daemon-pushed events will carry none (ticket #11). The
//! status snapshot's shape is the daemon's live per-Subscription state —
//! the same vocabulary the runtime state file holds, so the state file
//! and the channel answer read as one thing (spec story 24: the wire
//! format cannot fork).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One client → daemon frame: the request plus the id its response must
/// carry. The first frame on every connection is [`Request::Hello`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestFrame {
    /// The caller-chosen id; the daemon echoes it verbatim.
    pub id: u64,
    pub request: Request,
}

/// The requests the channel answers. This slice is request/response
/// only — openings are snapshot requests (ADR-0004); the daemon-pushed
/// event stream (a stream state flip, a History Log append) is ticket
/// #11 and will ride frames without an id.
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
    /// How the stream is doing right now.
    pub state: StreamState,
    /// The id of the last processed notification — the History Log's
    /// cursor (GLOSSARY).
    pub last_message_id: Option<i64>,
    /// Unix seconds of the last change to this line.
    pub updated_at: i64,
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
