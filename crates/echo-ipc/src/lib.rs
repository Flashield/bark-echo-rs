//! echo-ipc: the local IPC Channel's shared vocabulary and protocol
//! (ADR-0004). Serde message types one crate owns so the daemon, the
//! GUI, and the CLI cannot fork the wire format (spec story 24), plus a
//! transport-agnostic connect/bind — JSON Lines over a Unix domain
//! socket (Linux/macOS) or a named pipe (Windows) — with a protocol
//! version handshake, id-carrying request/response frames, and the
//! daemon's id-less event stream (ticket #11): stream state flips and
//! History Log appends, pushed unsolicited on the same connection.

mod client;
mod connection;
pub mod error;
pub(crate) mod framing;
pub mod handshake;
pub mod message;
pub mod server;
pub mod transport;

pub use client::Client;
pub use error::IpcError;
pub use handshake::{client_handshake, server_handshake};
pub use message::{
    AddSource, AddedSubscription, BasicAuthCredentials, ClickAction, Event, NotificationIntent,
    RemovedSubscription, Request, RequestFrame, Response, ResponseFrame, SendPush, SendPushOutcome,
    StatusSnapshot, StreamState, SubscriptionRuntime,
};
pub use server::serve;
pub use transport::{Endpoint, IpcServer, connect_raw};

/// The wire protocol's version. The handshake is the connection's first
/// frame exchange (`Hello`); a mismatch ends it with an explanatory
/// error, never a silent dialect confusion.
pub const PROTOCOL_VERSION: u32 = 1;
