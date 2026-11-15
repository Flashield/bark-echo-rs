//! The protocol over the real transport (ADR-0004): handshake, ids,
//! concurrent clients, and the malformed-bytes contract — a connection
//! that misbehaves drops alone. The endpoint here is a real UDS or a
//! real named pipe (whatever the platform provides), so this suite runs
//! unchanged on Linux, macOS, and Windows.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use echo_ipc::message::{
    AddSource, BasicAuthCredentials, Event, Request, RequestFrame, Response, ResponseFrame,
    SendPush, SendPushOutcome, StatusSnapshot, StreamState, SubscriptionRuntime,
};
use echo_ipc::{Client, Endpoint, IpcError, IpcServer, PROTOCOL_VERSION, connect_raw, serve};

type BoxedHandler =
    Box<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync + 'static>;

/// One History Log line as the log file holds it (GLOSSARY) — the
/// vocabulary the History plane's answers carry.
fn intent(id: i64) -> echo_ipc::NotificationIntent {
    echo_ipc::NotificationIntent {
        id,
        title: "the title".to_owned(),
        subtitle: String::new(),
        body: "the body".to_owned(),
        click_action: None,
        created_at: 1_700_000_000,
    }
}

fn canned_snapshot() -> StatusSnapshot {
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

/// Bind the channel on a fresh endpoint and serve with `handler` until
/// the returned trigger fires. The temp dir must be held by the caller —
/// the endpoint lives inside it. The request/response-only tests get an
/// event hub nobody broadcasts on; the event suite has its own harness.
async fn serving(
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

async fn serving_with_hub(
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

/// The "answer everything with the canned snapshot" handler.
fn canned_handler() -> BoxedHandler {
    Box::new(|_request| {
        Box::pin(async {
            Response::Status {
                snapshot: canned_snapshot(),
            }
        })
    })
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
async fn a_client_connects_handshakes_and_reads_the_snapshot() {
    let (_dir, endpoint, stop_tx, serve_task) = serving(canned_handler()).await;

    let mut client = Client::connect(&endpoint).await.expect("connect");
    assert_eq!(
        client.server_version, PROTOCOL_VERSION,
        "the handshake agreed on the dialect"
    );
    assert_eq!(client.status().await.expect("status"), canned_snapshot());

    // The generic face answers too, echoing the request's id.
    match client.call(Request::Status).await.expect("call") {
        Response::Status { snapshot } => assert_eq!(snapshot, canned_snapshot()),
        other => panic!("a status call answers with a snapshot, got {other:?}"),
    }

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn a_client_speaking_another_version_is_answered_then_let_go() {
    let (_dir, endpoint, stop_tx, serve_task) = serving(canned_handler()).await;

    // A raw peer plays a client from the protocol's future.
    let mut future_client = connect_raw(&endpoint).await.expect("raw connect");
    future_client
        .send(&RequestFrame {
            id: 0,
            request: Request::Hello {
                version: PROTOCOL_VERSION + 1,
            },
        })
        .await
        .expect("hello");
    let answered: ResponseFrame = future_client
        .receive()
        .await
        .expect("read")
        .expect("the mismatch deserves an answer");
    assert!(
        answered.id == 0 && matches!(answered.response, Response::Error { .. }),
        "the mismatch is answered with an error carrying the hello's id: {answered:?}"
    );
    let Response::Error { message } = answered.response else {
        panic!("unreachable after the match above")
    };
    assert!(
        message.contains(&PROTOCOL_VERSION.to_string()),
        "the explanation names the daemon's dialect: {message}"
    );
    // …and the connection ends: the next read is the end of stream.
    let ended: Option<ResponseFrame> = future_client.receive().await.expect("eof read");
    assert_eq!(ended, None, "a mismatched client is let go");

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn a_connection_whose_first_frame_is_not_a_hello_is_answered_then_let_go() {
    let (_dir, endpoint, stop_tx, serve_task) = serving(canned_handler()).await;

    let mut confused = connect_raw(&endpoint).await.expect("raw connect");
    confused
        .send(&RequestFrame {
            id: 5,
            request: Request::Status,
        })
        .await
        .expect("status before hello");
    let answered: ResponseFrame = confused
        .receive()
        .await
        .expect("read")
        .expect("the confusion deserves an answer");
    assert!(
        answered.id == 5 && matches!(answered.response, Response::Error { .. }),
        "the wrong first frame is answered with an error carrying its id: {answered:?}"
    );
    let ended: Option<ResponseFrame> = confused.receive().await.expect("eof read");
    assert_eq!(ended, None);

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn malformed_bytes_drop_only_their_connection() {
    let (_dir, endpoint, stop_tx, serve_task) = serving(canned_handler()).await;

    // Hostile bytes: NULs, garbage, and a broken JSON line. The
    // connection drops — and the daemon keeps serving everyone else.
    let mut hostile = connect_raw(&endpoint).await.expect("raw connect");
    hostile
        .write_raw(b"\x00\xff\xfe not json\n")
        .await
        .expect("garbage");
    let ended: Option<RequestFrame> = hostile.receive().await.expect("eof read");
    assert_eq!(
        ended, None,
        "the hostile connection ends; the daemon never interprets garbage"
    );

    // The channel is healthy right after: a proper client connects and
    // reads the snapshot.
    let mut client = Client::connect(&endpoint).await.expect("reconnect");
    assert_eq!(client.status().await.expect("status"), canned_snapshot());

    // …and once more, because the second hostile connection must not be
    // special either.
    let mut hostile_again = connect_raw(&endpoint).await.expect("raw connect");
    hostile_again
        .write_raw(b"}{ broken \n")
        .await
        .expect("garbage");
    let ended: Option<RequestFrame> = hostile_again.receive().await.expect("eof read");
    assert_eq!(ended, None);
    let mut client = Client::connect(&endpoint).await.expect("reconnect again");
    assert_eq!(client.status().await.expect("status"), canned_snapshot());

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn an_oversized_line_drops_its_connection() {
    let (_dir, endpoint, stop_tx, serve_task) = serving(canned_handler()).await;

    // One line without a newline, longer than the frame cap: the reader
    // must refuse to buffer it, drop the connection, and keep serving.
    let mut bloated = connect_raw(&endpoint).await.expect("raw connect");
    let mut line = vec![b'x'; 64 * 1024 + 1];
    line.push(b'\n');
    bloated.write_raw(&line).await.expect("bloated line");
    let ended: Option<RequestFrame> = bloated.receive().await.expect("eof read");
    assert_eq!(ended, None, "the oversized connection is dropped");

    let mut client = Client::connect(&endpoint).await.expect("reconnect");
    assert_eq!(client.status().await.expect("status"), canned_snapshot());

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn a_second_client_is_served_while_the_first_request_is_in_flight() {
    // The gate holds the first client's request inside the handler
    // until the test says otherwise — a structural proof (no timers)
    // that the daemon answers client B while client A's request is
    // still outstanding.
    let gate = Arc::new((AtomicBool::new(false), tokio::sync::Notify::new()));
    let handler_gate = gate.clone();
    let handler = Box::new(move |_request: Request| {
        let gate = handler_gate.clone();
        Box::pin(async move {
            let (arrived, release) = &*gate;
            if !arrived.swap(true, Ordering::SeqCst) {
                release.notified().await;
            }
            Response::Status {
                snapshot: canned_snapshot(),
            }
        }) as Pin<Box<dyn Future<Output = Response> + Send>>
    });
    let (_dir, endpoint, stop_tx, serve_task) = serving(handler).await;

    let mut client_a = Client::connect(&endpoint).await.expect("connect a");
    let mut client_b = Client::connect(&endpoint).await.expect("connect b");

    let a_answer = tokio::spawn(async move { client_a.status().await });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !gate.0.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < deadline,
            "the handler never saw client a's request"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // A's request is parked in the handler; B is served regardless.
    assert_eq!(
        client_b.status().await.expect("b status"),
        canned_snapshot()
    );

    // Released, A finally gets its answer too.
    gate.1.notify_waiters();
    assert_eq!(
        a_answer.await.expect("a task joined").expect("a status"),
        canned_snapshot()
    );

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn responses_must_carry_the_request_id() {
    // A hand-rolled server drives the id rule directly: the echo id is
    // accepted, a mismatched id is a protocol error on the client.
    let dir = tempfile::tempdir().expect("temp dir");
    let endpoint = Endpoint::in_config_dir(dir.path());
    let mut server = IpcServer::bind(&endpoint).expect("bind");

    let connecter = tokio::spawn({
        let endpoint = endpoint.clone();
        async move { Client::connect(&endpoint).await }
    });
    // The hand-rolled server drives its side inline: accept, handshake —
    // only then does the client's connect future resolve.
    let mut server_side = server.accept().await.expect("accept");
    echo_ipc::server_handshake(&mut server_side)
        .await
        .expect("handshake");
    let mut client = connecter.await.expect("task").expect("connect");

    // The client makes its two calls in a task (its receives must not
    // block this test's server-side reads); the results come back on a
    // channel in order.
    let (results_tx, mut results_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        results_tx
            .send(client.status().await)
            .expect("first result");
        results_tx
            .send(client.status().await)
            .expect("second result");
    });

    // The honest echo: the response carries the request's id.
    let asked: RequestFrame = server_side.receive().await.expect("read").expect("a frame");
    server_side
        .send(&ResponseFrame {
            id: asked.id,
            response: Response::Status {
                snapshot: canned_snapshot(),
            },
        })
        .await
        .expect("echo the id");
    assert_eq!(
        results_rx.recv().await.expect("result").expect("status"),
        canned_snapshot()
    );

    // The broken echo: an id from nowhere is refused.
    let asked: RequestFrame = server_side.receive().await.expect("read").expect("a frame");
    server_side
        .send(&ResponseFrame {
            id: asked.id + 500,
            response: Response::Status {
                snapshot: canned_snapshot(),
            },
        })
        .await
        .expect("mismatch the id");
    let outcome = results_rx.recv().await.expect("result");
    assert!(
        matches!(outcome, Err(IpcError::Protocol(_))),
        "an id that matches no request is a protocol error: {outcome:?}"
    );
}

#[tokio::test]
async fn a_history_snapshot_request_reaches_the_daemon_and_answers_with_entries() {
    // Ticket #15 over the real transport (this suite runs unchanged on
    // Windows CI): the History plane's request is an ordinary
    // request/response exchange — its id matches, its Subscription and
    // tail reach the handler, and the answer carries the entries.
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
    let handler = Box::new(move |request: Request| {
        let seen_tx = seen_tx.clone();
        Box::pin(async move {
            let _ = seen_tx.send(request.clone());
            match request {
                Request::HistorySnapshot { .. } => Response::HistorySnapshot {
                    entries: vec![intent(40), intent(41), intent(42), intent(43)],
                },
                other => Response::Error {
                    message: format!("this handler answers HistorySnapshot only, got {other:?}"),
                },
            }
        }) as Pin<Box<dyn Future<Output = Response> + Send>>
    });
    let (_dir, endpoint, stop_tx, serve_task) = serving(handler).await;

    let mut client = Client::connect(&endpoint).await.expect("connect");
    let entries = client
        .history_snapshot("tok", 3)
        .await
        .expect("history snapshot");
    assert_eq!(
        entries.len(),
        4,
        "the answer is whatever the daemon chose to send"
    );
    assert_eq!(entries[0].id, 40, "oldest first");
    assert_eq!(entries[3].id, 43);
    // The daemon side saw exactly what the viewer asked for.
    match seen_rx
        .recv()
        .await
        .expect("the request reached the handler")
    {
        Request::HistorySnapshot {
            subscription_id,
            tail,
        } => {
            assert_eq!(subscription_id, "tok");
            assert_eq!(tail, 3);
        }
        other => panic!("a history snapshot request went out, got {other:?}"),
    }

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn the_daemons_error_response_surfaces_to_the_client() {
    let handler = Box::new(|_request: Request| {
        Box::pin(async {
            Response::Error {
                message: "the snapshot lock is poisoned".to_owned(),
            }
        }) as Pin<Box<dyn Future<Output = Response> + Send>>
    });
    let (_dir, endpoint, stop_tx, serve_task) = serving(handler).await;

    let mut client = Client::connect(&endpoint).await.expect("connect");
    let outcome = client.status().await;
    match outcome {
        Err(IpcError::Rejected(message)) => {
            assert!(
                message.contains("poisoned"),
                "the daemon's own message surfaces: {message}"
            );
        }
        other => panic!("an error response is a Rejected error, got {other:?}"),
    }

    // The connection survives a refused request: the next call still
    // goes through the same client.
    let _still_connected = client.call(Request::Status).await;

    shutdown_cleanly(stop_tx, serve_task).await;
}

// ---- The Push Composer's send (ticket #16) ----

#[tokio::test]
async fn a_send_push_exchange_returns_the_servers_verdict() {
    // The daemon answers the Composer's send with what the bark server
    // said — here a rejection, carried verbatim to the window.
    let handler = Box::new(|request: Request| {
        Box::pin(async move {
            match request {
                Request::SendPush { push } => {
                    assert_eq!(push.subscription_id, "tok");
                    assert_eq!(push.device_key, "device-key-1");
                    assert_eq!(push.title.as_deref(), Some("from the Composer"));
                    assert_eq!(
                        push.ext_params.get("url").map(String::as_str),
                        Some("https://example.com"),
                        "the Ext Params rode the request"
                    );
                    Response::PushSent {
                        outcome: SendPushOutcome::Rejected {
                            code: Some(400),
                            message: "failed to get [key] device token from database".to_owned(),
                        },
                    }
                }
                other => panic!("the handler saw an unexpected request: {other:?}"),
            }
        }) as Pin<Box<dyn Future<Output = Response> + Send>>
    });
    let (_dir, endpoint, stop_tx, serve_task) = serving(handler).await;

    let mut client = Client::connect(&endpoint).await.expect("connect");
    let outcome = client
        .send_push(SendPush {
            subscription_id: "tok".to_owned(),
            device_key: "device-key-1".to_owned(),
            title: Some("from the Composer".to_owned()),
            subtitle: None,
            body: None,
            ext_params: BTreeMap::from([("url".to_owned(), "https://example.com".to_owned())]),
        })
        .await
        .expect("the verdict answers");
    assert_eq!(
        outcome,
        SendPushOutcome::Rejected {
            code: Some(400),
            message: "failed to get [key] device token from database".to_owned(),
        }
    );

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn a_send_push_the_daemon_cannot_attempt_is_a_rejected_request() {
    // A refusal to attempt (here: the daemon does not know the
    // Subscription) is the channel's first-class error answer — the
    // connection survives it and the message is actionable.
    let handler = Box::new(|request: Request| {
        Box::pin(async move {
            match request {
                Request::SendPush { .. } => Response::Error {
                    message: "no Subscription with id `tok` is configured".to_owned(),
                },
                other => panic!("the handler saw an unexpected request: {other:?}"),
            }
        }) as Pin<Box<dyn Future<Output = Response> + Send>>
    });
    let (_dir, endpoint, stop_tx, serve_task) = serving(handler).await;

    let mut client = Client::connect(&endpoint).await.expect("connect");
    let outcome = client
        .send_push(SendPush {
            subscription_id: "tok".to_owned(),
            device_key: "device-key-1".to_owned(),
            title: None,
            subtitle: None,
            body: None,
            ext_params: BTreeMap::new(),
        })
        .await;
    match outcome {
        Err(IpcError::Rejected(message)) => {
            assert!(
                message.contains("no Subscription with id `tok`"),
                "the refusal names the problem: {message}"
            );
        }
        other => panic!("a refusal-to-attempt is Rejected, got {other:?}"),
    }

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn a_crud_request_travels_the_stack_and_its_typed_answer_comes_back() {
    // Ticket #14: add/remove ride the same connection as the snapshot —
    // request/response with ids, the answer typed (`SubscriptionAdded`
    // carries the push target once). A refusal is an error response on
    // the same connection, the exchange never fatal.
    let handler = Box::new(|request: Request| {
        Box::pin(async move {
            match request {
                Request::SubscribeAdd {
                    server,
                    source: AddSource::Own,
                    label,
                    basic_auth,
                } => {
                    assert_eq!(server, "http://127.0.0.1:8080");
                    assert_eq!(label, "desktop");
                    assert_eq!(
                        basic_auth,
                        Some(BasicAuthCredentials {
                            user: "operator".to_owned(),
                            password: "op-secret".to_owned(),
                        })
                    );
                    Response::SubscriptionAdded {
                        id: "0123abcdef456789".to_owned(),
                        label,
                        server,
                        token_hint: "abcdef123456...".to_owned(),
                        device_key: "AbCdEf1234567890GhIjKl".to_owned(),
                    }
                }
                Request::SubscribeRemove { id } => {
                    assert_eq!(id, "0123abcdef456789");
                    Response::SubscriptionRemoved {
                        id,
                        label: "desktop".to_owned(),
                    }
                }
                other => Response::Error {
                    message: format!("unexpected {other:?}"),
                },
            }
        }) as Pin<Box<dyn Future<Output = Response> + Send>>
    });
    let (_dir, endpoint, stop_tx, serve_task) = serving(handler).await;

    let mut client = Client::connect(&endpoint).await.expect("connect");
    let added = client
        .subscribe_add(
            "http://127.0.0.1:8080",
            AddSource::Own,
            "desktop",
            Some(BasicAuthCredentials {
                user: "operator".to_owned(),
                password: "op-secret".to_owned(),
            }),
        )
        .await
        .expect("the add is answered");
    assert_eq!(added.id, "0123abcdef456789");
    assert_eq!(added.label, "desktop");
    assert_eq!(added.server, "http://127.0.0.1:8080");
    assert_eq!(added.token_hint, "abcdef123456...");
    assert_eq!(added.device_key, "AbCdEf1234567890GhIjKl");

    let removed = client
        .subscribe_remove("0123abcdef456789")
        .await
        .expect("the remove is answered");
    assert_eq!(removed.id, "0123abcdef456789");
    assert_eq!(removed.label, "desktop");

    shutdown_cleanly(stop_tx, serve_task).await;
}

#[tokio::test]
async fn a_shutdown_request_is_answered_and_the_channel_closes_when_the_daemon_stops() {
    // The Quit contract (ticket #12) at the serve level: the Shutdown
    // request is answered first — id echoed, `shutting_down` tagged —
    // and the daemon's stop closes every connection behind it. That
    // close is the confirmation a Quitting client reads. The fake
    // handler pulls its own stop behind the answer, the way the real
    // daemon's run pulls its stop behind the handler's answer; the
    // beat of delay stands in for the answer reaching the wire first.
    let (stop_pull_tx, mut stop_pull_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let handler = Box::new(move |request: Request| {
        let stop_pull_tx = stop_pull_tx.clone();
        Box::pin(async move {
            match request {
                Request::Shutdown => {
                    tokio::spawn(async move {
                        // The beat is real: the stop fires only after the
                        // answer had 50 ms to reach the wire. A named-pipe
                        // write completes at the kernel asynchronously —
                        // the writer is told the frame is out when it is
                        // merely enqueued — so an instant stop races the
                        // answer and cuts the frame mid-delivery on
                        // Windows (a Unix socket's write is kernel-buffered
                        // the moment the call returns, which is why the
                        // race never showed there).
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        let _ = stop_pull_tx.send(());
                    });
                    Response::ShuttingDown
                }
                other => Response::Error {
                    message: format!("this server does not answer {other:?}"),
                },
            }
        }) as Pin<Box<dyn Future<Output = Response> + Send>>
    });
    let (_dir, endpoint, stop_tx, serve_task) = serving(handler).await;

    let closer = tokio::spawn(async move {
        let _ = stop_pull_rx.recv().await;
        let _ = stop_tx.send(());
    });

    let mut client = Client::connect(&endpoint).await.expect("connect");
    match client.shutdown().await {
        Ok(()) => {}
        other => panic!("the daemon accepts the shutdown request: {other:?}"),
    }

    // The stop closes the channel: the next read is the end of the
    // stream — an event, a response, anything would mean the daemon is
    // serving after having accepted a shutdown.
    let outcome = tokio::time::timeout(Duration::from_secs(5), client.next_event()).await;
    assert!(
        matches!(outcome, Ok(Err(_))),
        "the channel closes after the shutdown: {outcome:?}"
    );

    // The serve itself ended cleanly when the fake's stop fired.
    let _ = closer.await;
    let outcome = tokio::time::timeout(Duration::from_secs(5), serve_task)
        .await
        .expect("serve ends after shutdown")
        .expect("serve task joined");
    assert!(outcome.is_ok(), "serve must end cleanly: {outcome:?}");
}
