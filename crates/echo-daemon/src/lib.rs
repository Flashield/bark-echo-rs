//! echo-daemon: the desktop Realtime Client binary — config, the OS
//! secret store, and the CLI subcommands (ADR-0002). The pure-logic core
//! lives in echo-core; this crate is everything that touches the machine.

pub mod config;
pub mod lock;
pub mod notify;
pub mod paths;
pub mod secrets;
pub mod state;
pub mod subscribe;
