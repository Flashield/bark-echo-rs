//! The daemon-pushed event stream (ticket #11, ADR-0004): events carry
//! no id and ride the same connection as the request/response traffic,
//! unsolicited — every connected client hears them without asking. The
//! endpoint here is a real UDS or a real named pipe (whatever the
//! platform provides), so this suite runs unchanged on Linux, macOS,
//! and Windows — the same rule the protocol suite obeys.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use echo_ipc::message::{
    Event, NotificationIntent, Request, RequestFrame, Response, ResponseFrame, StreamState,
};
use echo_ipc::{Client, Endpoint, IpcError, IpcServer, PROTOCOL_VERSION, connect_raw, serve};

type BoxedHandler =
    Box<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync + 'static>;

fn canned_snapshot_response() -> Response {
    Response::Status {
        snapshot: echo_ipc::message::StatusSnapshot {
            pid: 4242,
            started_at: 1_700_000_000,
            subscriptions: BTreeMap::new(),
        },
    }
}

fn stream_state_event(state: StreamState) -> Event {
    Event::StreamState {
        subscription_id: "tok".to_owned(),
        label: "desktop".to_owned(),
        state,
    }
}

fn appended_event(id: i64, body: &str) -> Event {
    Event::HistoryAppended {
        subscription_id: "tok".to_owned(),
        label: "desktop".to_owned(),
        entry: NotificationIntent {
            id,
            title: String::new(),
            subtitle: String::new(),
            body: body.to_owned(),
            click_action: None,
            created_at: 1_700_000_000,
        },
    }
}

/// Bind the channel on a fresh endpoint and serve with `handler`, with
/// `hub` as the event source every connection hears from. The temp dir
/// must be held by the caller — the endpoint lives inside it.
async fn serving(
    handler: BoxedHandler,
    hub: tokio::sync::broadcast::Sender<Event>,
) -> (
    tempfile::TempDir,
    Endpoint,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), IpcError>>,
) {
    let dir = tempfile::tempdir().expect("temp dir");
    let endpoint = Endpoint::in_config_dir(dir.path());
    let server = IpcServer::bind(&endpoint).expect("bind");
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(serve(
        server,
        async move {
            let _ = stop_rx.await;
        },
        hub,
        handler,
    ));
    (dir, endpoint, stop_tx, handle)
}

fn canned_handler() -> BoxedHandler {
    Box::new(|_request| Box::pin(async { canned_snapshot_response() }))
}

/// Shut the server down and assert it ended cleanly.
async fn shutdown_cleanly(
    stop_tx: tokio::sync::oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<Result<(), IpcError>>,
) {
    let _ = stop_tx.send(());
    let outcome = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("serve ends after shutdown")
        .expect("serve task joined");
    assert!(outcome.is_ok(), "serve must end cleanly: {outcome:?}");
}

#[tokio::test]
async fn an_event_reaches_a_connected_client_without_being_asked() {
    let (hub, _keep_open) = tokio::sync::broadcast::channel::<Event>(16);
    let (_dir, endpoint, stop_tx, serve_task) = serving(canned_handler(), hub.clone()).await;

    let mut client = Client::connect(&endpoint).await.expect("connect");
    // The request/response face works as always.
    assert_eq!(
        client.call(Request::Status).await.expect("call"),
        canned_snapshot_response()
    );

    // An event pushed while the connection sits idle arrives unsolicited.
    hub.send(stream_state_event(StreamState::Running))
        .expect("broadcast");
    let event = tokio::time::timeout(Duration::from_secs(5), client.next_event())
        .await
        .expect("the event arrives in time")
        .expect("the event is heard");
    assert_eq!(event, stream_state_event(StreamState::Running));

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn the_event_frame_carries_no_id_on_the_wire() {
    // ADR-0004: requests and responses carry an id, daemon-pushed events
    // carry none. A raw peer reads the exact bytes to pin that shape.
    let (hub, _keep_open) = tokio::sync::broadcast::channel::<Event>(16);
    let (_dir, endpoint, stop_tx, serve_task) = serving(canned_handler(), hub.clone()).await;

    let mut raw = connect_raw(&endpoint).await.expect("raw connect");
    raw.send(&RequestFrame {
        id: 0,
        request: Request::Hello {
            version: PROTOCOL_VERSION,
        },
    })
    .await
    .expect("hello");
    let answered: ResponseFrame = raw
        .receive()
        .await
        .expect("read")
        .expect("handshake answer");
    assert_eq!(answered.id, 0, "the handshake response carries the id");

    hub.send(appended_event(41, "the push landed"))
        .expect("broadcast");
    let deadline = Instant::now() + Duration::from_secs(5);
    let frame = loop {
        match raw.receive::<serde_json::Value>().await {
            Ok(Some(value)) => break value,
            Ok(None) => panic!("the connection ended before the event arrived"),
            Err(_) => assert!(Instant::now() < deadline, "no event frame arrived"),
        }
    };
    assert_eq!(frame["type"], "history_appended", "{frame}");
    assert!(
        frame.get("id").is_none(),
        "an event carries no id (ADR-0004): {frame}"
    );
    assert_eq!(frame["subscription_id"], "tok", "{frame}");
    assert_eq!(frame["label"], "desktop", "{frame}");
    assert_eq!(frame["entry"]["id"], 41, "{frame}");
    assert_eq!(frame["entry"]["body"], "the push landed", "{frame}");

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn an_event_arriving_while_a_request_is_in_flight_does_not_confuse_the_answer() {
    // The handler parks the first request until the test says go; an
    // event is broadcast in that window. The client's call must skip the
    // event (queueing it), read the response with the right id, and the
    // event must still be there afterwards.
    let gate = Arc::new((AtomicBool::new(false), tokio::sync::Notify::new()));
    let handler_gate = gate.clone();
    let handler = Box::new(move |_request: Request| {
        let gate = handler_gate.clone();
        Box::pin(async move {
            let (arrived, release) = &*gate;
            if !arrived.swap(true, Ordering::SeqCst) {
                release.notified().await;
            }
            canned_snapshot_response()
        }) as Pin<Box<dyn Future<Output = Response> + Send>>
    });
    let (hub, _keep_open) = tokio::sync::broadcast::channel::<Event>(16);
    let (_dir, endpoint, stop_tx, serve_task) = serving(handler, hub.clone()).await;

    let mut client = Client::connect(&endpoint).await.expect("connect");
    let answer = tokio::spawn(async move { client.call(Request::Status).await.map(|_| client) });

    let deadline = Instant::now() + Duration::from_secs(5);
    while !gate.0.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < deadline,
            "the handler never saw the request"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    hub.send(stream_state_event(StreamState::Disconnected {
        detail: "the server hung up".to_owned(),
    }))
    .expect("broadcast");
    gate.1.notify_waiters();

    let mut client = answer
        .await
        .expect("call task joined")
        .expect("the response won despite the interleaved event");
    let event = client.next_event().await.expect("the queued event");
    assert!(
        matches!(event, Event::StreamState { .. }),
        "the event survived the request/response exchange: {event:?}"
    );

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn every_connected_client_hears_the_events_in_order() {
    let (hub, _keep_open) = tokio::sync::broadcast::channel::<Event>(16);
    let (_dir, endpoint, stop_tx, serve_task) = serving(canned_handler(), hub.clone()).await;

    let mut client_a = Client::connect(&endpoint).await.expect("connect a");
    let mut client_b = Client::connect(&endpoint).await.expect("connect b");

    hub.send(stream_state_event(StreamState::Running))
        .expect("first");
    hub.send(appended_event(7, "first push")).expect("second");
    hub.send(appended_event(8, "second push")).expect("third");

    for (client, name) in [(&mut client_a, "a"), (&mut client_b, "b")] {
        let first = tokio::time::timeout(Duration::from_secs(5), client.next_event())
            .await
            .unwrap_or_else(|_| panic!("{name}: the first event never arrived"))
            .expect("event");
        let second = tokio::time::timeout(Duration::from_secs(5), client.next_event())
            .await
            .unwrap_or_else(|_| panic!("{name}: the second event never arrived"))
            .expect("event");
        let third = tokio::time::timeout(Duration::from_secs(5), client.next_event())
            .await
            .unwrap_or_else(|_| panic!("{name}: the third event never arrived"))
            .expect("event");
        assert_eq!(first, stream_state_event(StreamState::Running), "{name}");
        assert_eq!(second, appended_event(7, "first push"), "{name}");
        assert_eq!(third, appended_event(8, "second push"), "{name}");
    }

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn a_connection_that_lags_the_event_stream_is_dropped_not_served_stale() {
    // A client that falls further behind than the hub's buffer would
    // silently miss events — stale state instead of fresh. The connection
    // ends instead; the client's reconnect re-snapshots (its own rule).
    let (hub, _keep_open) = tokio::sync::broadcast::channel::<Event>(1);
    let (_dir, endpoint, stop_tx, serve_task) = serving(canned_handler(), hub.clone()).await;

    let mut client = Client::connect(&endpoint).await.expect("connect");

    // Prove the subscription is live: one event, received.
    hub.send(stream_state_event(StreamState::Starting))
        .expect("send");
    let seen = tokio::time::timeout(Duration::from_secs(5), client.next_event())
        .await
        .expect("the first event arrives")
        .expect("delivered");
    assert!(
        matches!(seen, Event::StreamState { .. }),
        "sanity: the first event is delivered"
    );

    // Flood past the buffer while the client reads nothing: the
    // connection must end, never continue stale.
    for _ in 0..3 {
        hub.send(stream_state_event(StreamState::Running))
            .expect("flood");
    }
    let outcome = tokio::time::timeout(Duration::from_secs(5), client.next_event()).await;
    assert!(
        matches!(outcome, Ok(Err(_))),
        "a lagging connection is closed, not served stale: {outcome:?}"
    );

    shutdown_cleanly(stop_tx, serve_task).await;
}
