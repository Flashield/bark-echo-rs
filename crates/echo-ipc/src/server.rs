//! The daemon face of the channel (ADR-0004): accept clients, handshake
//! each, and answer requests while relaying the daemon-pushed events —
//! one task per connection, several clients served concurrently. A
//! connection that misbehaves (garbage bytes, an oversized line, a
//! broken dialect) drops by itself; the accept loop and every other
//! client are untouched. That isolation is the malformed-bytes contract.

use std::future::Future;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::broadcast;

use crate::connection::Connection;
use crate::error::IpcError;
use crate::handshake::server_handshake;
use crate::message::{Event, Request, RequestFrame, Response, ResponseFrame};
use crate::transport::IpcServer;

/// Serve the channel until `shutdown` resolves: one task per client
/// connection, each answering `handler(request)` responses and hearing
/// every `events` broadcast (ticket #11). Requests a handler cannot
/// answer get [`Response::Error`] from the handler — protocol-level
/// misbehavior never reaches it. The hub must be held for the whole
/// serve (the caller keeps its sender): a connection that falls further
/// behind than the hub's buffer is dropped rather than served stale.
pub async fn serve<F, Fut>(
    mut server: IpcServer,
    shutdown: impl Future<Output = ()>,
    events: broadcast::Sender<Event>,
    handler: F,
) -> Result<(), IpcError>
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    let handler = Arc::new(handler);
    tokio::pin!(shutdown);
    let mut connections = Vec::new();
    let outcome = loop {
        tokio::select! {
            _ = &mut shutdown => break Ok(()),
            accepted = server.accept() => match accepted {
                Ok(connection) => {
                    connections.push(tokio::spawn(handle_connection(
                        connection,
                        events.clone(),
                        handler.clone(),
                    )));
                }
                Err(source) => {
                    break Err(IpcError::Io {
                        doing: "accept a client on",
                        endpoint: server.endpoint().to_string(),
                        source,
                    });
                }
            },
        }
    };
    // Shutdown or accept failure: stop every connection task. Their
    // sockets close with the aborted tasks — the daemon's run is over.
    for connection in connections.drain(..) {
        connection.abort();
    }
    outcome
}

/// One client's whole visit: handshake, then requests answered and
/// events relayed until the client goes away or misbehaves. Every exit
/// path here drops only this connection.
async fn handle_connection<S, F, Fut>(
    mut connection: Connection<S>,
    events: broadcast::Sender<Event>,
    handler: Arc<F>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    // The front door: a client that cannot say hello in this daemon's
    // dialect is answered (wrong version, wrong frame) or silently
    // dropped (garbage) — and nothing else happens on the connection.
    if server_handshake(&mut connection).await.is_err() {
        return;
    }
    // Subscribed after the handshake: no event can precede the response
    // the client's handshake is waiting for. Anything broadcast before
    // the subscription is covered by the opening snapshot the client
    // takes right after connecting.
    let mut event_rx = events.subscribe();
    loop {
        tokio::select! {
            frame = connection.receive::<RequestFrame>() => match frame {
                Ok(Some(frame)) => {
                    let response = handler(frame.request).await;
                    if connection
                        .send(&ResponseFrame {
                            id: frame.id,
                            response,
                        })
                        .await
                        .is_err()
                    {
                        // The peer is gone; there is nothing left to answer.
                        return;
                    }
                }
                // Clean end of stream, garbage, or a broken pipe — the
                // connection drops, the daemon stays healthy.
                Ok(None) | Err(_) => return,
            },
            event = event_rx.recv() => match event {
                Ok(event) => {
                    if connection.send(&event).await.is_err() {
                        return;
                    }
                }
                // This client fell further behind than the hub's buffer:
                // continuing would mean silent gaps — a missed state flip
                // is invisible forever. The connection ends instead; the
                // client's own reconnect re-snapshots and catches up.
                Err(broadcast::error::RecvError::Lagged(_)) => return,
                // serve holds its sender clone for the whole run, so the
                // hub cannot close mid-serve; if it ever does, the event
                // stream is gone and the connection ends with it.
                Err(broadcast::error::RecvError::Closed) => return,
            },
        }
    }
}
