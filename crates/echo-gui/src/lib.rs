//! echo-gui: the GUI stage's crate (ADR-0003 — egui/eframe with the
//! glow renderer). The Tray Menu (ticket #11) is the always-present
//! face; the Management Window (tickets #13 and #14) lists each
//! Subscription and adds and removes them over the channel — thin
//! renderers over the echo-ipc client's state machines, never a second
//! runtime.
//!
//! Entry: [`app::run`] on the main thread — the binary's `gui`
//! subcommand calls it.

pub mod app;
pub mod connection;
pub mod crud;
pub mod fonts;
pub mod history_view;
pub mod model;
pub mod shutdown;
pub mod tray;
pub mod window;

pub use app::run;
pub use connection::{CrudCommand, UiMsg};
pub use crud::CrudModel;
pub use model::{LinkState, MenuItem, SubscriptionLine, TrayModel};
pub use window::{RowView, WindowModel};
