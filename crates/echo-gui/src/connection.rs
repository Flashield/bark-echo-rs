//! The GUI's connection to the daemon (ADR-0004): one echo-ipc client
//! driven for the GUI's lifetime — connect, take the opening snapshot,
//! then listen to the daemon's pushed events until the channel breaks;
//! then retry. Every change the tray must paint leaves as a
//! [`UiMsg`]; the GUI's own asks — the Composer's send (ticket #16) —
//! come in as [`GuiRequest`]s and are performed on this connection,
//! since no other component ever holds a client. Nothing here knows
//! about menus or windows.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
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
    /// A [`GuiRequest::SendPush`] was answered (ticket #16): the bark
    /// server's verdict, or — `Err` — why the send never went out (the
    /// daemon away, the channel broken). Every queued send is answered
    /// exactly once; the Composer's state machine never hangs.
    SendFinished(Result<echo_ipc::message::SendPushOutcome, String>),
    /// The channel broke — the tray names the absence instead of
    /// leaving a stale picture.
    LinkDown,
}

/// An ask the GUI's surfaces hand to the connection loop: work that
/// needs the one client, performed on the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuiRequest {
    /// Send one Push on the Composer's behalf (ticket #16). Answered as
    /// exactly one [`UiMsg::SendFinished`].
    SendPush(echo_ipc::message::SendPush),
}

/// Drive the connection loop until `stop` flips: connect (retry every
/// `reconnect_delay`), take the opening snapshot, listen for events,
/// and perform the queued [`GuiRequest`]s on the live connection. Every
/// outcome becomes a [`UiMsg`]; the loop never panics on a dead daemon
/// — an unreachable daemon is a state, not a crash.
pub async fn run_connection_loop(
    endpoint: Endpoint,
    sender: Sender<UiMsg>,
    requests: Receiver<GuiRequest>,
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
                    // The Composer's sends ride this same connection
                    // (ticket #16): every queued request is performed
                    // here and answered exactly once. Events that arrive
                    // while a send waits are queued by the client's
                    // demux and surface on the next listen.
                    while let Ok(request) = requests.try_recv() {
                        let GuiRequest::SendPush(push) = request;
                        let outcome = client.send_push(push).await.map_err(|err| err.to_string());
                        let _ = sender.send(UiMsg::SendFinished(outcome));
                    }
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                }
            }
        }
        // The link is down: a queued send cannot go out — answer it
        // here, so a send never vanishes into the outage (the tray's
        // "never stale silence" rule, applied to the form). The
        // irrefutable pattern is deliberate: adding a variant to
        // [`GuiRequest`] breaks this compile and forces the new ask a
        // downed-link answer too.
        while let Ok(request) = requests.try_recv() {
            let GuiRequest::SendPush(_) = request;
            let _ = sender.send(UiMsg::SendFinished(Err(
                "the daemon is away — the Push did not go out".to_owned(),
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
/// the mpsc sender, while the GUI's asks go the other way through
/// `requests`.
pub fn spawn_connection_loop(
    endpoint: Endpoint,
    sender: Sender<UiMsg>,
    requests: Receiver<GuiRequest>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the GUI's channel runtime");
        runtime.block_on(run_connection_loop(
            endpoint,
            sender,
            requests,
            stop,
            RECONNECT_DELAY,
        ));
    })
}
