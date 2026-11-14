//! The Quit exchange (ticket #12) against the real channel transport: a
//! live daemon answers `shutting_down` and closes the channel behind it
//! — that close is the confirmation. A dead daemon, a refusal, and a
//! daemon that never closes are the bounded wait's edges: Quit leaves
//! either way, and it says which.
//!
//! This is the seam the spec gives the GUI — the echo-ipc client state
//! machine over the real transport, no window system anywhere.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use echo_gui::shutdown::{QuitOutcome, quit_exchange};
use echo_ipc::message::{Event, Request, Response};
use echo_ipc::{Endpoint, IpcServer, serve};

type BoxedHandler =
    Box<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync + 'static>;

/// One daemon incarnation: bind the endpoint and serve until the
/// returned sender fires — the way a daemon's stop closes its clients.
async fn serve_on(endpoint: &Endpoint, handler: BoxedHandler) -> tokio::sync::oneshot::Sender<()> {
    let (hub, _keep) = tokio::sync::broadcast::channel::<Event>(16);
    let server = IpcServer::bind(endpoint).expect("bind");
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(
        server,
        async move {
            let _ = stop_rx.await;
        },
        hub,
        handler,
    ));
    stop_tx
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
    let stop_tx = serve_on(&endpoint, confirming_handler(stop_pull_tx)).await;
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
    let _stop_tx = serve_on(&endpoint, refusing_handler()).await;

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
    let _stop_tx = serve_on(&endpoint, confirming_handler(stop_pull_tx)).await;

    let started = Instant::now();
    let outcome = quit_exchange(&endpoint, Duration::from_millis(400)).await;
    assert_eq!(outcome, QuitOutcome::Unconfirmed);
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "the budget was actually the bound: {:?}",
        started.elapsed()
    );
}
