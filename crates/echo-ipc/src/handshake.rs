//! The protocol version handshake (ADR-0004): the connection's first
//! frame exchange. A client that speaks another dialect is answered
//! with an explanation and let go — never a silent confusion; garbage
//! before the hello is simply dropped with its connection.

use tokio::io::{AsyncRead, AsyncWrite};

use crate::PROTOCOL_VERSION;
use crate::connection::Connection;
use crate::error::IpcError;
use crate::message::{Request, RequestFrame, Response, ResponseFrame};

/// The client side: say hello, hear the daemon's version back. Returns
/// the daemon's protocol version — the dialect the rest of this
/// connection speaks.
pub async fn client_handshake<S: AsyncRead + AsyncWrite + Unpin>(
    connection: &mut Connection<S>,
) -> Result<u32, IpcError> {
    connection
        .send(&RequestFrame {
            id: 0,
            request: Request::Hello {
                version: PROTOCOL_VERSION,
            },
        })
        .await
        .map_err(|source| IpcError::Broken { source })?;
    let frame: ResponseFrame = connection
        .receive()
        .await
        .map_err(|source| IpcError::Broken { source })?
        .ok_or(IpcError::Closed)?;
    if frame.id != 0 {
        return Err(IpcError::Protocol(format!(
            "the handshake response carries id {}, not the hello's 0",
            frame.id
        )));
    }
    match frame.response {
        Response::Hello { version } if version == PROTOCOL_VERSION => Ok(version),
        Response::Hello { version } => Err(IpcError::Protocol(format!(
            "the daemon speaks protocol {version}, this client speaks {PROTOCOL_VERSION}"
        ))),
        Response::Error { message } => Err(IpcError::Rejected(message)),
        other => Err(IpcError::Protocol(format!(
            "unexpected handshake response: {other:?}"
        ))),
    }
}

/// The server side: hear the hello, check the dialect, answer. Errors
/// are answered first — a wrong version or a confused first frame gets
/// an explanatory error response, then the connection ends.
pub async fn server_handshake<S: AsyncRead + AsyncWrite + Unpin>(
    connection: &mut Connection<S>,
) -> Result<(), IpcError> {
    let frame: RequestFrame = connection
        .receive()
        .await
        .map_err(|source| IpcError::Broken { source })?
        .ok_or(IpcError::Closed)?;
    match frame.request {
        Request::Hello {
            version: PROTOCOL_VERSION,
        } => {
            connection
                .send(&ResponseFrame {
                    id: frame.id,
                    response: Response::Hello {
                        version: PROTOCOL_VERSION,
                    },
                })
                .await
                .map_err(|source| IpcError::Broken { source })?;
            Ok(())
        }
        Request::Hello { version } => {
            let message = format!(
                "the client speaks protocol {version}, this daemon speaks {PROTOCOL_VERSION}"
            );
            // Best effort: the answer is worth sending even if the peer
            // has already hung up.
            let _ = connection
                .send(&ResponseFrame {
                    id: frame.id,
                    response: Response::Error {
                        message: message.clone(),
                    },
                })
                .await;
            Err(IpcError::Rejected(message))
        }
        other => {
            let message = format!(
                "the connection's first frame must be the hello, got {:?}",
                other
            );
            let _ = connection
                .send(&ResponseFrame {
                    id: frame.id,
                    response: Response::Error {
                        message: message.clone(),
                    },
                })
                .await;
            Err(IpcError::Rejected(message))
        }
    }
}
