//! Quit (ticket #12): the Tray Menu's Quit asks the daemon for a
//! graceful shutdown over the channel — an ordinary request that the
//! daemon answers before it begins stopping (ADR-0004) — and waits a
//! bounded while for the daemon's own confirmation, which is the
//! channel closing underneath the answer. The host exits either way: a
//! stuck or dead daemon must not hold the tray hostage, and the
//! outcome is reported honestly.
//!
//! The exchange runs on its own thread with its own one-shot runtime,
//! the way the connection loop does — the winit event loop owns the
//! main thread (macOS requires the tray there), and Quit must work
//! whether that loop happens to be connected, retrying, or gone. It is
//! a separate one-shot connection on purpose: the loop's connection
//! belongs to the tray's watching job, and Quit's is a different job.
//!
//! (Resurrected from the dormant egui face's shutdown module —
//! `egui-face-dormant:crates/echo-gui/src/shutdown.rs` — unchanged but
//! for the handoff: the outcome now lands in the host's own message
//! queue, not the egui shell's.)

use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use echo_ipc::{Client, Endpoint};

/// The Quit exchange's whole budget: reach for the daemon, get the
/// answer, watch the channel close. A daemon silent past this is
/// reported unconfirmed and the host quits anyway — the user's Quit is
/// not negotiable.
pub const QUIT_BUDGET: Duration = Duration::from_secs(10);

/// How long Quit reaches for a daemon before concluding it is absent —
/// short, because a dead daemon must not stall the user's Quit. It
/// covers the daemon that is mid-stop (another Quit, a `kill`): its
/// endpoint is gone within milliseconds.
const CONNECT_WINDOW: Duration = Duration::from_secs(1);

/// The pace between failed connect attempts inside the window.
const CONNECT_RETRY: Duration = Duration::from_millis(300);

/// What Quit learned from the daemon. The host exits on every variant;
/// the difference is how honest the exit is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuitOutcome {
    /// The daemon accepted the shutdown and closed the channel: its
    /// graceful stop (streams fenced, state persisted, lock released,
    /// clean exit) is underway — confirmed, not assumed.
    Confirmed,
    /// No daemon answered within the connect window: it was already
    /// gone when Quit asked.
    DaemonAbsent,
    /// A daemon held the channel but never confirmed within the
    /// budget — it refused, or it stopped answering. The host quits
    /// anyway and says so.
    Unconfirmed,
}

/// Run the exchange on its own thread and land the outcome in the
/// host's message queue (the same channel the connection loop reports
/// through — one pump, one wake path).
pub fn spawn_quit(endpoint: Endpoint, sender: Sender<crate::tray_host::connection::UiMsg>) {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the Quit exchange's runtime");
        let outcome = runtime.block_on(quit_exchange(&endpoint, QUIT_BUDGET));
        let _ = sender.send(crate::tray_host::connection::UiMsg::ShutdownOutcome(
            outcome,
        ));
    });
}

/// The exchange: connect (within its own short window) → shutdown
/// request → answer → wait for the daemon to close the channel. The
/// trailing events — the streams' final flips — drain while waiting;
/// the close itself is the confirmation.
pub async fn quit_exchange(endpoint: &Endpoint, budget: Duration) -> QuitOutcome {
    let started = Instant::now();
    let deadline = started + budget;
    let connect_deadline = deadline.min(started + CONNECT_WINDOW);
    let mut client = None;
    while Instant::now() < connect_deadline {
        match Client::connect(endpoint).await {
            Ok(connected) => {
                client = Some(connected);
                break;
            }
            Err(_) => {
                let pause =
                    CONNECT_RETRY.min(connect_deadline.saturating_duration_since(Instant::now()));
                tokio::time::sleep(pause).await;
            }
        }
    }
    let Some(mut client) = client else {
        return QuitOutcome::DaemonAbsent;
    };

    // The answer: the daemon accepts before it begins stopping. Past
    // the budget, every failure is the same word — unconfirmed.
    match tokio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        client.shutdown(),
    )
    .await
    {
        Ok(Ok(())) => {}
        _ => return QuitOutcome::Unconfirmed,
    }

    // The stop closes every connection (ADR-0004): that close is the
    // confirmation the tray reads. Anything the daemon says meanwhile —
    // the streams' final flips — drains without effect.
    loop {
        match tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            client.next_event(),
        )
        .await
        {
            Ok(Ok(_event)) => {}
            Ok(Err(_closed)) => return QuitOutcome::Confirmed,
            Err(_budget) => return QuitOutcome::Unconfirmed,
        }
    }
}
