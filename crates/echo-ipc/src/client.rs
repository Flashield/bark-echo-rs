//! The client face of the channel (ADR-0004): connect, handshake, then
//! request/response with matching ids. One connection per client — the
//! daemon serves several.

use crate::connection::Connection;
use crate::error::IpcError;
use crate::handshake;
use crate::message::{Request, RequestFrame, Response, ResponseFrame, StatusSnapshot};
use crate::transport::{self, ClientStream, Endpoint};

/// A connected client of the daemon's channel.
pub struct Client {
    connection: Connection<ClientStream>,
    /// The id of the next request — ids count up from the handshake's 0.
    next_id: u64,
    /// The dialect the daemon answered the handshake with. Today always
    /// [`PROTOCOL_VERSION`] (exact match required); the negotiation
    /// input if the protocol ever grows.
    pub server_version: u32,
}

impl Client {
    /// Connect to the channel at `endpoint` and run the version
    /// handshake. A daemon that speaks another protocol version is an
    /// error before any request is sent.
    pub async fn connect(endpoint: &Endpoint) -> Result<Client, IpcError> {
        let mut connection =
            transport::connect_raw(endpoint)
                .await
                .map_err(|source| IpcError::Io {
                    doing: "connect to",
                    endpoint: endpoint.to_string(),
                    source,
                })?;
        let server_version = handshake::client_handshake(&mut connection).await?;
        Ok(Client {
            connection,
            next_id: 1,
            server_version,
        })
    }

    /// One request/response exchange: the request goes out with the
    /// next id, and the answer must carry that id. An error response is
    /// [`IpcError::Rejected`] with the daemon's message.
    pub async fn call(&mut self, request: Request) -> Result<Response, IpcError> {
        let id = self.next_id;
        self.next_id += 1;
        self.connection
            .send(&RequestFrame { id, request })
            .await
            .map_err(|source| IpcError::Broken { source })?;
        let frame: ResponseFrame = self
            .connection
            .receive()
            .await
            .map_err(|source| IpcError::Broken { source })?
            .ok_or(IpcError::Closed)?;
        if frame.id != id {
            return Err(IpcError::Protocol(format!(
                "the response carries id {}, not the request's {id}",
                frame.id
            )));
        }
        match frame.response {
            Response::Error { message } => Err(IpcError::Rejected(message)),
            other => Ok(other),
        }
    }

    /// The daemon's live per-Subscription snapshot.
    pub async fn status(&mut self) -> Result<StatusSnapshot, IpcError> {
        match self.call(Request::Status).await? {
            Response::Status { snapshot } => Ok(snapshot),
            other => Err(IpcError::Protocol(format!(
                "unexpected response to a status request: {other:?}"
            ))),
        }
    }
}
