//! JSON Lines framing (ADR-0004): one JSON object per newline-terminated
//! line. The reader is bounded — a line longer than the cap is a broken
//! or hostile peer, and the connection drops before memory becomes the
//! victim; that is half of the "malformed bytes never hurt the daemon"
//! contract. The writer appends the newline and flushes: a frame is on
//! the wire the moment its future resolves.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// One line's maximum size, newline excluded. A status snapshot for a
/// realistic Subscription list is a few KiB; 64 KiB leaves headroom
/// without inviting unbounded buffering.
pub(crate) const MAX_FRAME_BYTES: usize = 64 * 1024;

/// A newline-terminated frame reader over any byte stream. Owns the
/// leftover bytes between frames: a chunk may contain the end of one
/// frame and the start of the next (pipelined writes), and none of
/// either may be lost.
pub(crate) struct FrameReader<R> {
    reader: R,
    /// Raw bytes read so far; `cursor` is how much of it is consumed.
    pending: Vec<u8>,
    cursor: usize,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader,
            pending: Vec::new(),
            cursor: 0,
        }
    }

    /// The next frame's bytes (newline stripped), or `None` on a clean
    /// end of stream between frames. Errors on I/O failure, end of
    /// stream inside a frame, or a line over [`MAX_FRAME_BYTES`].
    pub(crate) async fn next_frame(&mut self) -> io::Result<Option<Vec<u8>>> {
        loop {
            if let Some(newline) = self.pending[self.cursor..]
                .iter()
                .position(|&byte| byte == b'\n')
            {
                let frame = self.pending[self.cursor..self.cursor + newline].to_vec();
                self.pending.drain(..self.cursor + newline + 1);
                self.cursor = 0;
                if frame.len() > MAX_FRAME_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "frame exceeds the line cap",
                    ));
                }
                return Ok(Some(frame));
            }
            if self.pending.len() - self.cursor > MAX_FRAME_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "frame exceeds the line cap",
                ));
            }

            let mut chunk = [0u8; 4096];
            let read = self.reader.read(&mut chunk).await?;
            if read == 0 {
                if self.cursor == self.pending.len() {
                    return Ok(None);
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed mid-frame",
                ));
            }
            self.pending.extend_from_slice(&chunk[..read]);
        }
    }
}

/// Write one frame: the bytes, the terminating newline, then flush.
/// The bytes must not contain a newline — JSON never does, and every
/// caller feeds `serde_json` output.
pub(crate) async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &[u8],
) -> io::Result<()> {
    writer.write_all(frame).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn a_frame_written_is_the_frame_read() {
        let (client, server) = duplex(64);
        tokio::spawn(async move {
            let mut writer = client;
            writer
                .write_all(b"{\"type\":\"status\"}\n")
                .await
                .expect("write frame");
        });
        let mut reader = FrameReader::new(server);
        let frame = reader.next_frame().await.expect("read frame");
        assert_eq!(frame, Some(b"{\"type\":\"status\"}".to_vec()));
    }

    #[tokio::test]
    async fn two_frames_in_one_chunk_read_as_two() {
        // Pipelined writes: one chunk carries a whole frame and the start
        // of the next — the leftover buffer must keep both.
        let (mut client, server) = duplex(MAX_FRAME_BYTES + 1024);
        tokio::spawn(async move {
            client
                .write_all(b"{\"id\":1}\n{\"id\":2}\n")
                .await
                .expect("write both frames in one call");
        });
        let mut reader = FrameReader::new(server);
        let first = reader.next_frame().await.expect("first frame");
        assert_eq!(first, Some(b"{\"id\":1}".to_vec()));
        let second = reader.next_frame().await.expect("second frame");
        assert_eq!(second, Some(b"{\"id\":2}".to_vec()));
    }

    #[tokio::test]
    async fn a_frame_split_across_writes_still_reads_whole() {
        let (client, server) = duplex(64);
        tokio::spawn(async move {
            let mut writer = client;
            writer.write_all(b"{\"partial\"").await.expect("first half");
            writer.flush().await.expect("first half flush");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            writer
                .write_all(b",\"rest\"}\n")
                .await
                .expect("second half");
        });
        let mut reader = FrameReader::new(server);
        let frame = reader.next_frame().await.expect("read frame");
        assert_eq!(frame, Some(b"{\"partial\",\"rest\"}".to_vec()));
    }

    #[tokio::test]
    async fn eof_between_frames_reads_as_none() {
        let (client, server) = duplex(64);
        tokio::spawn(async move {
            let mut writer = client;
            writer.write_all(b"{\"only\"}\n").await.expect("write");
        });
        // The writer task is dropped after its write: the reader sees
        // the frame, then a clean end of stream.
        let mut reader = FrameReader::new(server);
        assert_eq!(
            reader.next_frame().await.expect("first frame"),
            Some(b"{\"only\"}".to_vec())
        );
        assert_eq!(reader.next_frame().await.expect("eof"), None);
    }

    #[tokio::test]
    async fn eof_mid_frame_is_an_error() {
        let (client, server) = duplex(64);
        tokio::spawn(async move {
            let mut writer = client;
            writer.write_all(b"{\"unterminated\"").await.expect("write");
        });
        let mut reader = FrameReader::new(server);
        let outcome = reader.next_frame().await;
        assert!(
            outcome.is_err(),
            "a frame without its newline must not read as a frame or as None"
        );
    }

    #[tokio::test]
    async fn a_line_over_the_cap_is_an_error() {
        let (client, server) = duplex(MAX_FRAME_BYTES + 1024);
        tokio::spawn(async move {
            let mut writer = client;
            let bloated = vec![b'x'; MAX_FRAME_BYTES + 1];
            writer.write_all(&bloated).await.expect("write bloated");
        });
        let mut reader = FrameReader::new(server);
        let outcome = reader.next_frame().await;
        assert!(
            outcome.is_err(),
            "a line over the cap must drop the connection, not buffer it"
        );
    }

    #[tokio::test]
    async fn a_frame_at_the_cap_exactly_still_reads() {
        // The cap is inclusive: a frame of exactly MAX_FRAME_BYTES bytes
        // (newline excluded) is legal.
        let body = vec![b'x'; MAX_FRAME_BYTES];
        let (mut client, server) = duplex(MAX_FRAME_BYTES + 1024);
        let body_for_task = body.clone();
        tokio::spawn(async move {
            let mut frame = body_for_task;
            frame.push(b'\n');
            client.write_all(&frame).await.expect("write at cap");
        });
        let mut reader = FrameReader::new(server);
        assert_eq!(reader.next_frame().await.expect("frame at cap"), Some(body));
    }

    #[tokio::test]
    async fn write_frame_appends_the_newline_and_flushes() {
        let (mut client, mut server) = duplex(64);
        write_frame(&mut client, b"hello")
            .await
            .expect("write frame");
        // Drop the writer half: read_to_end needs the end of stream, and
        // a duplex half only ends when dropped.
        drop(client);
        let mut buf = Vec::new();
        server.read_to_end(&mut buf).await.expect("read back");
        assert_eq!(buf, b"hello\n");
    }
}
