//! Why a channel exchange failed. Every variant is actionable: the OS
//! failure names what and where, a protocol break names the dialect
//! disagreement, and a refused request carries the daemon's own message.

/// What went wrong on the IPC Channel (ADR-0004).
#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    /// Binding, connecting, or accepting failed at the OS level.
    #[error("cannot {doing} the IPC channel at {endpoint}: {source}")]
    Io {
        /// What the caller was trying to do (`connect to`, `accept a
        /// client on`, …).
        doing: &'static str,
        /// The channel endpoint involved.
        endpoint: String,
        #[source]
        source: std::io::Error,
    },
    /// The peer broke the protocol — a version mismatch, a frame out of
    /// order, an id that does not match the request. The connection is
    /// over.
    #[error("IPC channel protocol error: {0}")]
    Protocol(String),
    /// The daemon answered with an error response; its message is the
    /// actionable part.
    #[error("the daemon refused the request: {0}")]
    Rejected(String),
    /// The channel closed before the exchange completed.
    #[error("the IPC channel closed before the exchange completed")]
    Closed,
    /// The channel broke mid-exchange at the transport level.
    #[error("the IPC channel broke during the exchange: {source}")]
    Broken {
        #[source]
        source: std::io::Error,
    },
}
