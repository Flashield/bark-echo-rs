//! echo-ipc: the local IPC Channel's shared vocabulary (ADR-0004).
//! Serde message types one crate owns so the daemon, the GUI, and the
//! CLI cannot fork the wire format (spec story 24), plus a
//! transport-agnostic connect/bind — JSON Lines over a Unix domain
//! socket (Linux/macOS) or a named pipe (Windows).

pub mod message;

pub use message::{
    Request, RequestFrame, Response, ResponseFrame, StatusSnapshot, StreamState,
    SubscriptionRuntime,
};

/// The wire protocol's version. The handshake is the connection's first
/// frame exchange (`Hello`); a mismatch ends it with an explanatory
/// error, never a silent dialect confusion.
pub const PROTOCOL_VERSION: u32 = 1;
