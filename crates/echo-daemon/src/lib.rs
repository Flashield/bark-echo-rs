//! echo-daemon: the desktop Realtime Client binary — config, the OS
//! secret store, and the CLI subcommands (ADR-0002). The pure-logic core
//! lives in echo-core; this crate is everything that touches the machine.

pub(crate) mod atomic;
pub mod auth;
pub(crate) mod channel;
pub mod composer;
pub mod config;
pub mod history;
pub mod lifecycle;
pub mod lock;
/// The management face (issue #31): the daemon's in-process HTTP
/// surface — the `/v1` API, the SSE stream, and the security posture
/// (issue #24). Public for the CLI's `management` commands; the face
/// itself serves only from a live `run`.
pub mod management;
pub mod notify;
pub mod paths;
pub mod run;
pub mod secrets;
pub mod state;
pub mod status;
pub mod subscribe;
pub mod toast_activation;
pub mod tray_host;
