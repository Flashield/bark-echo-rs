//! The client face of the channel (ADR-0004): connect, handshake, then
//! request/response with matching ids — with the daemon-pushed events
//! (ticket #11) interleaved on the same connection and demultiplexed:
//! an exchange never loses its id match to an event, and no event is
//! ever lost to an exchange. One connection per client — the daemon
//! serves several.

use std::collections::VecDeque;

use crate::connection::Connection;
use crate::error::IpcError;
use crate::handshake;
use crate::message::{
    Event, Request, RequestFrame, Response, ResponseFrame, SendPush, SendPushOutcome,
    StatusSnapshot,
};
use crate::transport::{self, ClientStream, Endpoint};

/// A connected client of the daemon's channel.
pub struct Client {
    connection: Connection<ClientStream>,
    /// The id of the next request — ids count up from the handshake's 0.
    next_id: u64,
    /// The dialect the daemon answered the handshake with. Today always
    /// [`PROTOCOL_VERSION`](crate::PROTOCOL_VERSION) (exact match
    /// required); the negotiation input if the protocol ever grows.
    pub server_version: u32,
    /// Events heard while an exchange was reading for its response —
    /// queued, not lost; [`Client::next_event`] surfaces them first.
    events: VecDeque<Event>,
}

/// One daemon → client frame, demultiplexed: a response envelope
/// carrying an id, or a bare id-less event (ADR-0004).
enum Incoming {
    Response(ResponseFrame),
    Event(Event),
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
            events: VecDeque::new(),
        })
    }

    /// One request/response exchange: the request goes out with the
    /// next id, and the answer must carry that id. Events broadcast
    /// while the answer is in flight are queued for
    /// [`Client::next_event`]. An error response is
    /// [`IpcError::Rejected`] with the daemon's message.
    pub async fn call(&mut self, request: Request) -> Result<Response, IpcError> {
        let id = self.next_id;
        self.next_id += 1;
        self.connection
            .send(&RequestFrame { id, request })
            .await
            .map_err(|source| IpcError::Broken { source })?;
        loop {
            match self.receive_frame().await? {
                Incoming::Event(event) => self.events.push_back(event),
                Incoming::Response(frame) => {
                    if frame.id != id {
                        return Err(IpcError::Protocol(format!(
                            "the response carries id {}, not the request's {id}",
                            frame.id
                        )));
                    }
                    match frame.response {
                        Response::Error { message } => return Err(IpcError::Rejected(message)),
                        other => return Ok(other),
                    }
                }
            }
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

    /// Send one Push on the Composer's behalf (ticket #16): the daemon
    /// performs the send with its own stored secrets and answers with
    /// the bark server's verdict. A send the daemon could not attempt
    /// (unknown Subscription, unreachable server) is
    /// [`IpcError::Rejected`] carrying its explanation; the server's
    /// acceptance or refusal is the [`SendPushOutcome`].
    pub async fn send_push(&mut self, push: SendPush) -> Result<SendPushOutcome, IpcError> {
        match self.call(Request::SendPush { push }).await? {
            Response::PushSent { outcome } => Ok(outcome),
            other => Err(IpcError::Protocol(format!(
                "unexpected response to a send-push request: {other:?}"
            ))),
        }
    }

    /// Ask the daemon for a graceful shutdown (ticket #12): the answer
    /// is the acceptance, not the completion — the daemon answers
    /// before it begins stopping, and its graceful stop closes every
    /// connection. The client's next read after [`Client::shutdown`]
    /// therefore ends in [`IpcError::Closed`] when the daemon is
    /// healthy; a client that must observe the lock release or the
    /// process exit polls the files (the daemon's IPC seam tests do).
    pub async fn shutdown(&mut self) -> Result<(), IpcError> {
        match self.call(Request::Shutdown).await? {
            Response::ShuttingDown => Ok(()),
            other => Err(IpcError::Protocol(format!(
                "unexpected response to a shutdown request: {other:?}"
            ))),
        }
    }

    /// The next daemon-pushed event, oldest first. Events heard during
    /// an exchange surface here before anything new arrives; while
    /// waiting on the wire, a response frame is a protocol error —
    /// [`Client::call`] always drains until its own response.
    pub async fn next_event(&mut self) -> Result<Event, IpcError> {
        if let Some(event) = self.events.pop_front() {
            return Ok(event);
        }
        match self.receive_frame().await? {
            Incoming::Event(event) => Ok(event),
            Incoming::Response(frame) => Err(IpcError::Protocol(format!(
                "a response (id {}) arrived while no request was outstanding",
                frame.id
            ))),
        }
    }

    /// The next daemon → client frame off the wire, demultiplexed by
    /// shape: a response envelope has an `id`, an event does not
    /// (ADR-0004). Anything else is a protocol error; the caller drops
    /// the connection.
    async fn receive_frame(&mut self) -> Result<Incoming, IpcError> {
        let raw: serde_json::Value = self
            .connection
            .receive()
            .await
            .map_err(|source| IpcError::Broken { source })?
            .ok_or(IpcError::Closed)?;
        if raw.get("id").is_some() {
            let frame: ResponseFrame = serde_json::from_value(raw).map_err(|err| {
                IpcError::Protocol(format!("the response frame does not parse: {err}"))
            })?;
            Ok(Incoming::Response(frame))
        } else {
            let event: Event = serde_json::from_value(raw).map_err(|err| {
                IpcError::Protocol(format!("the event frame does not parse: {err}"))
            })?;
            Ok(Incoming::Event(event))
        }
    }
}
