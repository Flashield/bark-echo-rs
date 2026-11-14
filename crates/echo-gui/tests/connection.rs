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

use echo_gui::connection::{GuiRequest, UiMsg, run_connection_loop};
use echo_ipc::message::{Event, Request, Response, SendPush, StatusSnapshot, StreamState};
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
    let (_requests_tx, requests_rx) = mpsc::channel();
    let loop_stop = stop.clone();
    let loop_endpoint = endpoint.clone();
    let loop_task = tokio::spawn(async move {
        // A fast reconnect delay: the behavior under test is the shape of
        // the loop, not the production pause between attempts.
        run_connection_loop(
            loop_endpoint,
            tx,
            requests_rx,
            loop_stop,
            Duration::from_millis(50),
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

// ---- The Push Composer's send rides the loop (ticket #16) ----

fn a_send() -> SendPush {
    SendPush {
        subscription_id: "tok".to_owned(),
        device_key: "device-key-1".to_owned(),
        title: Some("from the Composer".to_owned()),
        subtitle: None,
        body: Some("through the loop".to_owned()),
        ext_params: Default::default(),
    }
}

#[tokio::test]
async fn a_composer_send_rides_the_loop_and_the_verdict_comes_back() {
    // The Composer's send is performed on the connection the loop owns
    // — the GUI's surfaces never hold a client — and the bark server's
    // verdict comes back as its own message.
    let dir = tempfile::tempdir().expect("temp dir");
    let endpoint = Endpoint::in_config_dir(dir.path());
    let handler: BoxedHandler = Box::new(|request| {
        Box::pin(async move {
            match request {
                Request::SendPush { push } => {
                    assert_eq!(push.device_key, "device-key-1");
                    Response::PushSent {
                        outcome: echo_ipc::message::SendPushOutcome::Sent,
                    }
                }
                Request::Status => Response::Status {
                    snapshot: snapshot_with(StreamState::Running),
                },
                other => panic!("the handler saw an unexpected request: {other:?}"),
            }
        })
    });
    let _serve = serve_on(&endpoint, handler).await;

    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel();
    let (requests_tx, requests_rx) = mpsc::channel();
    let loop_stop = stop.clone();
    let loop_endpoint = endpoint.clone();
    let loop_task = tokio::spawn(async move {
        run_connection_loop(
            loop_endpoint,
            tx,
            requests_rx,
            loop_stop,
            Duration::from_millis(50),
        )
        .await
    });

    // Connected before asking: the request queues once the loop is
    // listening, the way the shell forwards a Send click.
    wait_for(&rx, "the opening snapshot", |msg| {
        matches!(msg, UiMsg::Snapshot(_))
    })
    .await;
    requests_tx
        .send(GuiRequest::SendPush(a_send()))
        .expect("queue the send");
    let msg = wait_for(&rx, "the send's verdict", |msg| {
        matches!(msg, UiMsg::SendFinished(_))
    })
    .await;
    match msg {
        UiMsg::SendFinished(Ok(outcome)) => {
            assert_eq!(
                outcome,
                echo_ipc::message::SendPushOutcome::Sent,
                "the server's verdict surfaced"
            );
        }
        other => panic!("the send's verdict, not: {other:?}"),
    }

    stop.store(true, Ordering::Relaxed);
    loop_task.abort();
}

#[tokio::test]
async fn a_send_queued_while_the_daemon_is_away_is_failed_not_lost() {
    // No daemon anywhere: the queued send is answered with why it did
    // not go out — the Composer's state machine never hangs on
    // Sending, and the absence is named (the tray's "never stale
    // silence" rule, applied to the form).
    let dir = tempfile::tempdir().expect("temp dir");
    let endpoint = Endpoint::in_config_dir(dir.path());
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel();
    let (requests_tx, requests_rx) = mpsc::channel();
    let loop_stop = stop.clone();
    let loop_endpoint = endpoint.clone();
    let loop_task = tokio::spawn(async move {
        run_connection_loop(
            loop_endpoint,
            tx,
            requests_rx,
            loop_stop,
            Duration::from_millis(50),
        )
        .await
    });

    requests_tx
        .send(GuiRequest::SendPush(a_send()))
        .expect("queue the send");
    let msg = wait_for(&rx, "the failed send", |msg| {
        matches!(msg, UiMsg::SendFinished(_))
    })
    .await;
    match msg {
        UiMsg::SendFinished(Err(message)) => {
            assert!(
                message.contains("away"),
                "the failure names the daemon's absence: {message}"
            );
        }
        other => panic!("a queued send with no daemon fails, not: {other:?}"),
    }

    stop.store(true, Ordering::Relaxed);
    loop_task.abort();
}
