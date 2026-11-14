//! The GUI's connection to the daemon (ADR-0004): one echo-ipc client
//! driven for the GUI's lifetime — connect, take the opening snapshot,
//! then listen to the daemon's pushed events until the channel breaks;
//! then retry. Every change the tray must paint leaves as a
//! [`UiMsg`]; nothing here knows about menus or windows.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::time::Duration;

use echo_ipc::{Client, Endpoint};

/// How long between reconnect attempts in production: fast enough that a
/// restarted daemon is covered again within seconds, slow enough to stay
/// invisible next to the tray's "reconnecting…" line.
pub const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// How often the listener re-checks the stop flag while the channel is
/// quiet — the granularity at which a Quit can end the loop.
const STOP_POLL: Duration = Duration::from_millis(200);

/// A change the tray must paint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UiMsg {
    /// The opening snapshot of a (re)connected channel — fresh
    /// authority, replacing everything previously believed.
    Snapshot(echo_ipc::message::StatusSnapshot),
    /// A daemon-pushed event (ticket #11): a stream state flip or a
    /// History Log append.
    Event(echo_ipc::message::Event),
    /// The channel broke — the tray names the absence instead of
    /// leaving a stale picture.
    LinkDown,
}

/// Drive the connection loop until `stop` flips: connect (retry every
/// `reconnect_delay`), take the opening snapshot, listen for events.
/// Every outcome becomes a [`UiMsg`]; the loop never panics on a dead
/// daemon — an unreachable daemon is a state, not a crash.
pub async fn run_connection_loop(
    endpoint: Endpoint,
    sender: Sender<UiMsg>,
    stop: Arc<AtomicBool>,
    reconnect_delay: Duration,
) {
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        if let Ok(mut client) = Client::connect(&endpoint).await {
            // Connected but the snapshot refused: the daemon is not
            // itself yet — fall through to the retry.
            if let Ok(snapshot) = client.status().await {
                let _ = sender.send(UiMsg::Snapshot(snapshot));
                // The channel is up: listen until it breaks. The stop
                // check rides a short timeout so Quit works while the
                // daemon is quiet.
                loop {
                    match tokio::time::timeout(STOP_POLL, client.next_event()).await {
                        Ok(Ok(event)) => {
                            let _ = sender.send(UiMsg::Event(event));
                        }
                        Ok(Err(_)) => break,
                        Err(_timeout) => {}
                    }
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                }
            }
        }
        let _ = sender.send(UiMsg::LinkDown);
        if stop.load(Ordering::Relaxed) {
            return;
        }
        tokio::time::sleep(reconnect_delay).await;
    }
}

/// Run the loop on its own thread with its own tiny runtime: the GUI's
/// event loop owns the main thread (macOS requires the tray there —
/// ADR-0003), so the channel work lives elsewhere and reaches it over
/// the mpsc sender.
pub fn spawn_connection_loop(
    endpoint: Endpoint,
    sender: Sender<UiMsg>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the GUI's channel runtime");
        runtime.block_on(run_connection_loop(endpoint, sender, stop, RECONNECT_DELAY));
    })
}
