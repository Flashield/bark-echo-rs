//! One connection's serde face: typed frames in and out over any byte
//! stream — the real Unix socket or named pipe in production, tokio's
//! in-memory duplex in tests. The raw write escape hatch is the
//! protocol-test surface: hostile bytes go in through here so the drop
//! behavior can be proven against the real thing.

use std::io;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::framing::{FrameReader, write_frame};

/// One side of a JSON Lines connection (ADR-0004): every frame is one
/// serde value on one line. A frame that does not parse is an error —
/// the caller drops the connection — never a guessed interpretation.
pub struct Connection<S> {
    reader: FrameReader<tokio::io::ReadHalf<S>>,
    writer: tokio::io::WriteHalf<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Connection<S> {
    pub fn new(stream: S) -> Self {
        let (read, write) = tokio::io::split(stream);
        Self {
            reader: FrameReader::new(read),
            writer: write,
        }
    }

    /// Send one envelope as a line, flushed — the frame is on the wire
    /// the moment this resolves.
    pub async fn send<E: Serialize>(&mut self, envelope: &E) -> io::Result<()> {
        let line = serde_json::to_vec(envelope).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame serialization: {err}"),
            )
        })?;
        write_frame(&mut self.writer, &line).await
    }

    /// The next envelope. `None` is the peer's clean end of stream
    /// between frames; an unparseable frame or broken stream is an
    /// error, and the caller's contract is to drop the connection.
    pub async fn receive<E: DeserializeOwned>(&mut self) -> io::Result<Option<E>> {
        match self.reader.next_frame().await? {
            None => Ok(None),
            Some(line) => serde_json::from_slice(&line).map(Some).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("frame does not parse: {err}"),
                )
            }),
        }
    }

    /// Raw bytes onto the wire with no framing — how a test plays a
    /// hostile or confused peer.
    pub async fn write_raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        use tokio::io::AsyncWriteExt;
        self.writer.write_all(bytes).await?;
        self.writer.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tokio::io::duplex;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Ping {
        word: String,
    }

    #[tokio::test]
    async fn a_sent_envelope_is_received_on_the_other_half() {
        let (client, server) = duplex(1024);
        let (mut sender, mut receiver) = (Connection::new(client), Connection::new(server));
        sender
            .send(&Ping {
                word: "status".to_owned(),
            })
            .await
            .expect("send");
        let back: Ping = receiver.receive().await.expect("receive").expect("a frame");
        assert_eq!(back.word, "status");
    }

    #[tokio::test]
    async fn a_clean_close_reads_as_none_between_frames() {
        let (client, server) = duplex(1024);
        let (mut sender, mut receiver) = (Connection::new(client), Connection::new(server));
        sender
            .send(&Ping {
                word: "x".to_owned(),
            })
            .await
            .expect("send");
        drop(sender);
        let first: Option<Ping> = receiver.receive().await.expect("receive");
        assert!(first.is_some());
        let second: Option<Ping> = receiver.receive().await.expect("receive");
        assert_eq!(second, None, "end of stream is None, not an error");
    }

    #[tokio::test]
    async fn an_unparseable_frame_is_an_error_not_a_guess() {
        let (client, server) = duplex(1024);
        let (mut sender, mut receiver) = (Connection::new(client), Connection::new(server));
        sender
            .write_raw(b"this is not json\n")
            .await
            .expect("raw write");
        let outcome: io::Result<Option<Ping>> = receiver.receive().await;
        assert!(
            outcome.is_err(),
            "garbage must surface as an error for the caller to drop the connection"
        );
    }
}
