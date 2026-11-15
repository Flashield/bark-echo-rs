//! The transport suites' shared harness (prior art: echo-daemon's
//! tests/common): bind the channel on a fresh endpoint and serve with
//! a handler until the test pulls the stop, the canned handler for the
//! request/response assertions, and the clean-shutdown assertion. The
//! endpoint is a real UDS or a real named pipe (whatever the platform
//! provides), so every suite here runs unchanged on Linux, macOS, and
//! Windows.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use echo_ipc::message::{
    Event, Request, Response, StatusSnapshot, StreamState, SubscriptionRuntime,
};
use echo_ipc::{Endpoint, IpcError, IpcServer, serve};

// common/ is compiled into every test target; helpers are shared per
// target, not all used everywhere.
#[allow(dead_code)]
pub type BoxedHandler =
    Box<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync + 'static>;

/// One live Subscription line (`tok`, running, its cursor set) — the
/// snapshot the canned handler answers every request with.
pub fn canned_snapshot() -> StatusSnapshot {
    StatusSnapshot {
        pid: 4242,
        started_at: 1_700_000_000,
        subscriptions: BTreeMap::from([(
            "tok".to_owned(),
            SubscriptionRuntime {
                label: "desktop".to_owned(),
                state: StreamState::Running,
                last_message_id: Some(7),
                updated_at: 1_700_000_042,
                server: "http://127.0.0.1:8080".to_owned(),
                token_hint: "abcdef123456...".to_owned(),
            },
        )]),
    }
}

/// The canned snapshot as the response frame carries it.
// common/ is compiled into every test target; helpers are shared per
// target, not all used everywhere.
#[allow(dead_code)]
pub fn canned_snapshot_response() -> Response {
    Response::Status {
        snapshot: canned_snapshot(),
    }
}

/// The "answer everything with the canned snapshot" handler.
pub fn canned_handler() -> BoxedHandler {
    Box::new(|_request| {
        Box::pin(async {
            Response::Status {
                snapshot: canned_snapshot(),
            }
        })
    })
}

/// Bind the channel on a fresh endpoint and serve with `handler` until
/// the returned trigger fires. The temp dir must be held by the caller —
/// the endpoint lives inside it. The request/response-only tests get an
/// event hub nobody broadcasts on; the event suite passes its own.
// common/ is compiled into every test target; helpers are shared per
// target, not all used everywhere.
#[allow(dead_code)]
pub async fn serving(
    handler: BoxedHandler,
) -> (
    tempfile::TempDir,
    Endpoint,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), IpcError>>,
) {
    let (hub, _unused) = tokio::sync::broadcast::channel::<Event>(16);
    let (dir, endpoint, stop_tx, handle) = serving_with_hub(handler, hub).await;
    (dir, endpoint, stop_tx, handle)
}

/// [`serving`] with the caller's event hub as the source every
/// connection hears from.
pub async fn serving_with_hub(
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

/// Shut the server down and assert it ended cleanly.
pub async fn shutdown_cleanly(
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
