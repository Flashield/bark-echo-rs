//! echo-gui: the GUI stage's crate (ADR-0003 — egui/eframe with the
//! glow renderer). Ticket #11's face is the Tray Menu: an always-present
//! tray icon whose menu lists each Subscription's label with its live
//! stream state, updated by the daemon's pushed events — a thin
//! renderer over the echo-ipc client's state machine, never a second
//! runtime.

pub mod model;

pub use model::{LinkState, MenuItem, SubscriptionLine, TrayModel};
