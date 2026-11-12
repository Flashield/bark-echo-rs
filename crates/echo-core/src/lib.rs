//! echo-core: the pure-logic core of the desktop Realtime Client —
//! subscriptions, the History Log, and the bark-server client face.
//! Platform I/O (keyring, notifications, config files) lives in
//! echo-daemon, never here.

mod envelope;
pub mod history;
pub mod intent;
pub mod pull;
pub mod push;
pub mod server;
pub mod stream;
