//! The GUI's connection loop against the real channel transport (UDS or
//! named pipe): one echo-ipc client driven for the GUI's lifetime —
//! snapshot on connect, the daemon's events as they come, and a
//! self-healing reconnect when the daemon dies and returns. This is the
//! seam the spec gives the GUI: the echo-ipc client state machine, no
//! window system anywhere.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use echo_gui::connection::{CrudCommand, UiMsg, run_connection_loop};
use echo_ipc::message::{AddSource, Event, Request, Response, StatusSnapshot, StreamState};
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

/// One daemon incarnation: bind the endpoint and serve until dropped.
/// The returned shutdown sender ends the serve and every connection it
/// holds — the way a daemon's death closes its clients.
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
    let (_commands_tx, commands_rx) = mpsc::channel();
    let loop_stop = stop.clone();
    let loop_endpoint = endpoint.clone();
    let loop_task = tokio::spawn(async move {
        // A fast reconnect delay: the behavior under test is the shape of
        // the loop, not the production pause between attempts.
        run_connection_loop(
            loop_endpoint,
            tx,
            loop_stop,
            Duration::from_millis(50),
            commands_rx,
        )
        .await
    });

    // The GUI starts before the daemon: reaching for it is the loop's
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

/// A handler that answers Status and applies the CRUD vocabulary the
/// way the daemon does: an add echoes a typed `SubscriptionAdded`, a
/// remove a typed `SubscriptionRemoved`, refusals as errors.
fn crud_handler() -> BoxedHandler {
    Box::new(move |request| {
        Box::pin(async move {
            match request {
                Request::Status => Response::Status {
                    snapshot: StatusSnapshot {
                        pid: 4242,
                        started_at: 0,
                        subscriptions: BTreeMap::new(),
                    },
                },
                Request::SubscribeAdd { label, .. } => Response::SubscriptionAdded {
                    id: "tok-new".to_owned(),
                    label,
                    server: "http://127.0.0.1:8080".to_owned(),
                    token_hint: "abcdef123456...".to_owned(),
                    device_key: "AbCdEf1234567890GhIjKl".to_owned(),
                },
                Request::SubscribeRemove { id } => {
                    if id == "tok-new" {
                        Response::SubscriptionRemoved {
                            id,
                            label: "hot-add".to_owned(),
                        }
                    } else {
                        Response::Error {
                            message: format!("no Subscription with id `{id}`"),
                        }
                    }
                }
                other => Response::Error {
                    message: format!("unexpected {other:?}"),
                },
            }
        }) as Pin<Box<dyn Future<Output = Response> + Send>>
    })
}

#[tokio::test]
async fn the_window_s_crud_actions_ride_the_loop_and_get_their_outcomes() {
    // Ticket #14: the form's submits travel the connection loop — the
    // loop owns the client — and come back as an outcome plus a fresh
    // snapshot, so the rows repaint with the change without a reopen.
    let dir = tempfile::tempdir().expect("temp dir");
    let endpoint = Endpoint::in_config_dir(dir.path());
    let (hub, _keep) = tokio::sync::broadcast::channel::<Event>(16);
    let server = IpcServer::bind(&endpoint).expect("bind");
    let (serve_stop, serve_stop_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(
        server,
        async move {
            let _ = serve_stop_rx.await;
        },
        hub,
        crud_handler(),
    ));

    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel();
    let (commands_tx, commands_rx) = mpsc::channel();
    let loop_stop = stop.clone();
    let loop_endpoint = endpoint.clone();
    let _loop_task = tokio::spawn(async move {
        run_connection_loop(
            loop_endpoint,
            tx,
            loop_stop,
            Duration::from_millis(50),
            commands_rx,
        )
        .await
    });

    wait_for(&rx, "the opening snapshot", |msg| {
        matches!(msg, UiMsg::Snapshot(_))
    })
    .await;

    // The add: the outcome is the success half — the refusal would
    // carry the daemon's words.
    commands_tx
        .send(CrudCommand::Add {
            server: "http://127.0.0.1:8080".to_owned(),
            source: AddSource::Own,
            label: "hot-add".to_owned(),
            basic_auth: None,
        })
        .expect("send the add");
    let outcome = wait_for(&rx, "the add's outcome", |msg| {
        matches!(msg, UiMsg::ActionResult(_))
    })
    .await;
    assert_eq!(
        outcome,
        UiMsg::ActionResult(Ok(())),
        "the add succeeded on the fake daemon"
    );
    // The fresh snapshot follows the outcome.
    wait_for(&rx, "the post-add snapshot", |msg| {
        matches!(msg, UiMsg::Snapshot(_))
    })
    .await;

    // The remove of an unknown id: the daemon's refusal surfaces
    // verbatim for the form to show.
    commands_tx
        .send(CrudCommand::Remove {
            id: "no-such-id".to_owned(),
        })
        .expect("send the remove");
    let outcome = wait_for(&rx, "the refusal's outcome", |msg| {
        matches!(msg, UiMsg::ActionResult(_))
    })
    .await;
    assert_eq!(
        outcome,
        UiMsg::ActionResult(Err(
            "the daemon refused the request: no Subscription with id `no-such-id`".to_owned()
        )),
        "the daemon's reason rides verbatim"
    );

    stop.store(true, Ordering::Relaxed);
    let _ = serve_stop.send(());
}
