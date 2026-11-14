//! The daemon face of the channel (ADR-0004): accept clients, handshake
//! each, and answer requests — one task per connection, several clients
//! served concurrently. A connection that misbehaves (garbage bytes, an
//! oversized line, a broken dialect) drops by itself; the accept loop
//! and every other client are untouched. That isolation is the
//! malformed-bytes contract.

use std::future::Future;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::connection::Connection;
use crate::error::IpcError;
use crate::handshake::server_handshake;
use crate::message::{Request, RequestFrame, Response, ResponseFrame};
use crate::transport::IpcServer;

/// Serve the channel until `shutdown` resolves: one task per client
/// connection, each answering `handler(request)` responses. Requests a
/// handler cannot answer get [`Response::Error`] from the handler —
/// protocol-level misbehavior never reaches it.
pub async fn serve<F, Fut>(
    mut server: IpcServer,
    shutdown: impl Future<Output = ()>,
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

/// One client's whole visit: handshake, then request/response until the
/// client goes away or misbehaves. Every exit path here drops only this
/// connection.
async fn handle_connection<S, F, Fut>(mut connection: Connection<S>, handler: Arc<F>)
where
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
    loop {
        match connection.receive::<RequestFrame>().await {
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
        }
    }
}
