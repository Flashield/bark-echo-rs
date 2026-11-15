//! The GUI's connection to the daemon (ADR-0004): one echo-ipc client
//! driven for the GUI's lifetime — connect, take the opening snapshot,
//! then listen to the daemon's pushed events until the channel breaks;
//! then retry. The window's CRUD actions ride this loop too (ticket
//! #14 — the loop owns the client), each answered with its outcome and
//! a fresh snapshot. Every change the tray must paint leaves as a
//! [`UiMsg`]; nothing here knows about menus or windows.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;

use echo_ipc::{Client, Endpoint};

use crate::history_view::HISTORY_TAIL;

/// How long between reconnect attempts in production: fast enough that a
/// restarted daemon is covered again within seconds, slow enough to stay
/// invisible next to the tray's "reconnecting…" line.
pub const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// How often the listener re-checks the stop flag while the channel is
/// quiet — the granularity at which a Quit can end the loop and a pick
/// gets its answer.
const STOP_POLL: Duration = Duration::from_millis(200);

/// A request the UI wants made over the connection's own client (the
/// loop owns the client; the surfaces never touch the channel —
/// ADR-0004). Served between events on the same connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientAsk {
    /// The picked Subscription's recent History Log lines (ticket #15):
    /// a bounded tail — [`HISTORY_TAIL`] — answered over the channel.
    HistorySnapshot {
        /// The Subscription whose tail to load (its Stream Token's
        /// `token_id`).
        subscription_id: String,
    },
}

/// A change a surface must paint (the tray's glance, the window's list
/// and History Log viewer).
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
    /// A CRUD action's outcome (ticket #14): success needs no words —
    /// the fresh snapshot that follows repaints the rows; a refusal
    /// carries the daemon's reason verbatim for the form to show.
    ActionResult(Result<(), String>),
    /// The Quit exchange's answer (ticket #12): what the daemon said
    /// when Quit asked it to stop. The shell exits either way; this
    /// names how honest the exit is.
    ShutdownOutcome(crate::shutdown::QuitOutcome),
    /// A [`ClientAsk::HistorySnapshot`] answered: the picked
    /// Subscription's bounded tail, oldest last (ticket #15).
    HistoryLoaded {
        /// The Subscription the answer belongs to.
        subscription_id: String,
        /// The entries, oldest last.
        entries: Vec<echo_ipc::message::NotificationIntent>,
    },
    /// The History Log snapshot request failed — named, not blank
    /// (ticket #15): the daemon refused it or the channel broke.
    HistoryUnavailable {
        /// The Subscription the failed ask belonged to.
        subscription_id: String,
        /// What went wrong, as the channel phrased it.
        reason: String,
    },
}

/// A CRUD action the window asked the connection loop to perform
/// (ticket #14). The loop owns the client, so the window's submits
/// arrive here over a plain channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrudCommand {
    /// Add a Subscription: server, Device source, the required label,
    /// and the optional Basic Auth credentials (the daemon stores them;
    /// the GUI keeps no copy).
    Add {
        /// The bark server base URL.
        server: String,
        /// Own Device or mirror by Device Key.
        source: echo_ipc::message::AddSource,
        /// The client label the Stream Token is minted with.
        label: String,
        /// The operator Basic Auth credentials, when collected.
        basic_auth: Option<echo_ipc::message::BasicAuthCredentials>,
    },
    /// Remove one Subscription by id.
    Remove {
        /// The Subscription's id (its Stream Token's `token_id`).
        id: String,
    },
}

/// Perform one CRUD command on the live client — the daemon's refusal
/// (a bad Device Key, an unknown id) is the actionable error string.
async fn perform(client: &mut Client, command: CrudCommand) -> Result<(), String> {
    match command {
        CrudCommand::Add {
            server,
            source,
            label,
            basic_auth,
        } => client
            .subscribe_add(&server, source, &label, basic_auth)
            .await
            .map(|_| ())
            .map_err(|err| err.to_string()),
        CrudCommand::Remove { id } => client
            .subscribe_remove(&id)
            .await
            .map(|_| ())
            .map_err(|err| err.to_string()),
    }
}

/// Drive the connection loop until `stop` flips: connect (retry every
/// `reconnect_delay`), take the opening snapshot, listen for events,
/// serve the surfaces' asks and perform the window's CRUD actions on
/// the same connection — each answered with its outcome and, for the
/// CRUD changes, a fresh snapshot. Every outcome
/// becomes a [`UiMsg`]; the loop never panics on a dead daemon — an
/// unreachable daemon is a state, not a crash.
pub async fn run_connection_loop(
    endpoint: Endpoint,
    sender: Sender<UiMsg>,
    asks: Receiver<ClientAsk>,
    stop: Arc<AtomicBool>,
    reconnect_delay: Duration,
    commands: Receiver<CrudCommand>,
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
                // daemon is quiet, and on the same beat the surfaces'
                // asks and the window's CRUD actions are drained — a
                // pick is answered within one poll, and anything that
                // queued during an outage is served the moment the
                // daemon returns. The CRUD actions (ticket #14) are
                // each answered, then a fresh snapshot repaints the
                // rows with the change.
                loop {
                    while let Ok(ask) = asks.try_recv() {
                        match ask {
                            ClientAsk::HistorySnapshot { subscription_id } => {
                                match client
                                    .history_snapshot(&subscription_id, HISTORY_TAIL)
                                    .await
                                {
                                    Ok(entries) => {
                                        let _ = sender.send(UiMsg::HistoryLoaded {
                                            subscription_id,
                                            entries,
                                        });
                                    }
                                    Err(err) => {
                                        let _ = sender.send(UiMsg::HistoryUnavailable {
                                            subscription_id,
                                            reason: err.to_string(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                    while let Ok(command) = commands.try_recv() {
                        let outcome = perform(&mut client, command).await;
                        let _ = sender.send(UiMsg::ActionResult(outcome));
                        if let Ok(fresh) = client.status().await {
                            let _ = sender.send(UiMsg::Snapshot(fresh));
                        }
                    }
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
        // Not connected: any queued action fails at once — the form
        // stays open with the reason, nothing was applied.
        while let Ok(_command) = commands.try_recv() {
            let _ = sender.send(UiMsg::ActionResult(Err(
                "the daemon is not reachable — the change was not applied".to_owned(),
            )));
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
/// the mpsc sender. The window's CRUD commands travel the other way on
/// the plain `commands` channel the shell wires up.
pub fn spawn_connection_loop(
    endpoint: Endpoint,
    sender: Sender<UiMsg>,
    asks: Receiver<ClientAsk>,
    stop: Arc<AtomicBool>,
    commands: Receiver<CrudCommand>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the GUI's channel runtime");
        runtime.block_on(run_connection_loop(
            endpoint,
            sender,
            asks,
            stop,
            RECONNECT_DELAY,
            commands,
        ));
    })
}
