//! The tray host's two echo-ipc client seams against the real channel
//! transport (UDS or named pipe) — the seams the spec gives the tray
//! host (the host itself is a thin shell: lock probe, spawn, shutdown
//! request, all over tested commands). Resurrected from the dormant
//! egui face's tests — `egui-face-dormant:crates/echo-gui/tests/
//! {shutdown,connection}.rs` — stripped of the window surfaces' asks:
//! no CRUD, no Composer, no History Log tail (the web face's actions
//! now, ADR-0005).
//!
//! The Quit exchange: a live daemon answers `shutting_down` and closes
//! the channel behind it — that close is the confirmation. A dead
//! daemon, a refusal, and a daemon that never closes are the bounded
//! wait's edges: Quit leaves either way, and it says which.
//!
//! The connection loop: one echo-ipc client driven for the host's
//! lifetime — snapshot on connect, the daemon's events as they come,
//! and a self-healing reconnect when the daemon dies and returns.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use echo_daemon::tray_host::connection::{UiMsg, run_connection_loop};
use echo_daemon::tray_host::shutdown::{QuitOutcome, quit_exchange};
use echo_ipc::message::{Event, Request, Response, StatusSnapshot, StreamState};
use echo_ipc::{Endpoint, IpcServer, serve};

type BoxedHandler =
    Box<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync + 'static>;

fn snapshot_with(state: StreamState) -> StatusSnapshot {
    StatusSnapshot {
        pid: 4242,
        started_at: 1_700_000_000,
        subscriptions: BTreeMap::from([(
            "tok".to_owned(),
            echo_ipc::message::SubscriptionRuntime {
                label: "desktop".to_owned(),
                state,
                last_message_id: None,
                updated_at: 1_700_000_042,
                server: "http://127.0.0.1:8080".to_owned(),
                token_hint: "abcdef123456...".to_owned(),
            },
        )]),
    }
}

fn status_handler(state: StreamState) -> BoxedHandler {
    let snapshot = snapshot_with(state);
    Box::new(move |_request| {
        let snapshot = snapshot.clone();
        Box::pin(async move { Response::Status { snapshot } })
    })
}

/// One daemon incarnation: bind the endpoint and serve until the
/// returned sender fires — the way a daemon's stop closes its clients.
async fn serve_on(
    endpoint: &Endpoint,
    handler: BoxedHandler,
) -> (
    tokio::sync::broadcast::Sender<Event>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (hub, _keep) = tokio::sync::broadcast::channel::<Event>(16);
    let server = IpcServer::bind(endpoint).expect("bind");
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(
        server,
        async move {
            let _ = stop_rx.await;
        },
        hub.clone(),
        handler,
    ));
    (hub, stop_tx)
}

/// The next message matching `check`, with a generous deadline. Polls
/// without blocking the runtime thread — the loop task shares this test
/// runtime and must stay scheduled.
async fn wait_for(rx: &mpsc::Receiver<UiMsg>, what: &str, check: impl Fn(&UiMsg) -> bool) -> UiMsg {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match rx.try_recv() {
            Ok(msg) if check(&msg) => return msg,
            Ok(_) => {}
            Err(mpsc::TryRecvError::Empty) => {
                assert!(Instant::now() < deadline, "never saw {what}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(mpsc::TryRecvError::Disconnected) => panic!("the loop hung up before {what}"),
        }
    }
}

#[tokio::test]
async fn the_loop_snapshots_hears_events_and_self_heals_after_a_daemon_restart() {
    let dir = tempfile::tempdir().expect("temp dir");
    let endpoint = Endpoint::in_config_dir(dir.path());
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel();
    let loop_stop = stop.clone();
    let loop_endpoint = endpoint.clone();
    let loop_task = tokio::spawn(async move {
        // A fast reconnect delay: the behavior under test is the shape of
        // the loop, not the production pause between attempts.
        run_connection_loop(loop_endpoint, tx, loop_stop, Duration::from_millis(50)).await
    });

    // The host starts before the daemon: reaching for it is the loop's
    // first business (its attempt failures surface as LinkDown and the
    // tray shows the reaching state — the model's job).
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (hub, first_stop_tx) = serve_on(&endpoint, status_handler(StreamState::Running)).await;

    // The daemon answers: the opening snapshot arrives on its own.
    let msg = wait_for(&rx, "the opening snapshot", |msg| {
        matches!(msg, UiMsg::Snapshot(_))
    })
    .await;
    let UiMsg::Snapshot(snapshot) = msg else {
        panic!("unreachable")
    };
    assert_eq!(
        snapshot.subscriptions["tok"].state,
        StreamState::Running,
        "the snapshot paints the live state"
    );

    // A daemon-pushed event arrives without any request.
    hub.send(Event::StreamState {
        subscription_id: "tok".to_owned(),
        label: "desktop".to_owned(),
        state: StreamState::Disconnected {
            detail: "the server hung up".to_owned(),
        },
    })
    .expect("broadcast");
    wait_for(&rx, "the pushed event", |msg| {
        matches!(msg, UiMsg::Event(Event::StreamState { .. }))
    })
    .await;

    // The daemon dies — the serve the loop is connected to ends, and
    // with it every connection it holds: the loop reports the loss…
    first_stop_tx.send(()).expect("stop the serve");
    wait_for(&rx, "the link-down report", |msg| {
        matches!(msg, UiMsg::LinkDown)
    })
    .await;

    // …and when the daemon returns at the same endpoint, the loop
    // snapshots again by itself. The tray heals without user action.
    let _second = serve_on(&endpoint, status_handler(StreamState::Running)).await;
    wait_for(&rx, "the self-healed snapshot", |msg| {
        matches!(msg, UiMsg::Snapshot(_))
    })
    .await;

    stop.store(true, Ordering::Relaxed);
    loop_task.abort();
}

/// The honest daemon face: Shutdown is answered `shutting_down`, and —
/// like a real daemon's stop — the server is pulled a beat later, after
/// the answer has reached the wire. The pull lands through `closer`
/// (the sender this test wires to `serve_on`'s stop).
fn confirming_handler(stop_pull_tx: tokio::sync::mpsc::UnboundedSender<()>) -> BoxedHandler {
    Box::new(move |request: Request| {
        let stop_pull_tx = stop_pull_tx.clone();
        Box::pin(async move {
            match request {
                Request::Shutdown => {
                    tokio::spawn(async move {
                        let _ = stop_pull_tx.send(());
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    });
                    Response::ShuttingDown
                }
                other => Response::Error {
                    message: format!("this fake daemon does not answer {other:?}"),
                },
            }
        }) as Pin<Box<dyn Future<Output = Response> + Send>>
    })
}

/// The daemon that refuses: an error response is a first-class answer,
/// not a confirmation.
fn refusing_handler() -> BoxedHandler {
    Box::new(move |_request: Request| {
        Box::pin(async {
            Response::Error {
                message: "not now".to_owned(),
            }
        }) as Pin<Box<dyn Future<Output = Response> + Send>>
    })
}

#[tokio::test]
async fn a_live_daemon_confirms_by_closing_the_channel() {
    let dir = tempfile::tempdir().expect("temp dir");
    let endpoint = Endpoint::in_config_dir(dir.path());
    let (stop_pull_tx, mut stop_pull_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let (_hub, stop_tx) = serve_on(&endpoint, confirming_handler(stop_pull_tx)).await;
    let closer = tokio::spawn(async move {
        let _ = stop_pull_rx.recv().await;
        let _ = stop_tx.send(());
    });

    let outcome = quit_exchange(&endpoint, Duration::from_secs(5)).await;
    assert_eq!(
        outcome,
        QuitOutcome::Confirmed,
        "the ack followed by the channel closing is the confirmation"
    );
    let _ = closer.await;
}

#[tokio::test]
async fn quit_with_no_daemon_reports_absence_and_leaves_fast() {
    let dir = tempfile::tempdir().expect("temp dir");
    let endpoint = Endpoint::in_config_dir(dir.path()); // nothing ever bound

    let started = Instant::now();
    let outcome = quit_exchange(&endpoint, Duration::from_secs(10)).await;
    assert_eq!(outcome, QuitOutcome::DaemonAbsent);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "a dead daemon must not stall the user's Quit: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_daemon_that_refuses_leaves_quit_unconfirmed() {
    let dir = tempfile::tempdir().expect("temp dir");
    let endpoint = Endpoint::in_config_dir(dir.path());
    let (_hub, _stop_tx) = serve_on(&endpoint, refusing_handler()).await;

    let outcome = quit_exchange(&endpoint, Duration::from_secs(5)).await;
    assert_eq!(
        outcome,
        QuitOutcome::Unconfirmed,
        "a refusal is an answer, not a confirmation — and Quit leaves anyway"
    );
}

#[tokio::test]
async fn a_daemon_that_never_closes_the_channel_leaves_quit_unconfirmed() {
    // The stuck daemon: it answers the ack and then serves forever. The
    // budget — not the daemon — is what ends the wait.
    let dir = tempfile::tempdir().expect("temp dir");
    let endpoint = Endpoint::in_config_dir(dir.path());
    let (stop_pull_tx, _keep) = tokio::sync::mpsc::unbounded_channel::<()>();
    let (_hub, _stop_tx) = serve_on(&endpoint, confirming_handler(stop_pull_tx)).await;

    let started = Instant::now();
    let outcome = quit_exchange(&endpoint, Duration::from_millis(400)).await;
    assert_eq!(outcome, QuitOutcome::Unconfirmed);
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "the budget was actually the bound: {:?}",
        started.elapsed()
    );
}
