//! Minimal SSE reading for stream-side assertions: open a Realtime Stream
//! (protocol §3.3) with a Stream Token and read `event: message` frames'
//! data as JSON. Frames follow the server repo's hand-written parser shape
//! (`\n\n`-terminated blocks); comment lines (`: connected`,
//! `: keep-alive`) are skipped per §3.4.

use std::time::Duration;

use serde_json::Value;

use super::TestServer;

/// One parsed SSE frame (a block terminated by a blank line).
#[derive(Debug, Default, PartialEq)]
struct SseFrame {
    event: Option<String>,
    data: String,
}

/// A connected Realtime Stream: the raw response plus a read buffer.
pub struct SseStream {
    res: reqwest::Response,
    buf: Vec<u8>,
}

impl SseStream {
    /// Open the stream with `X-Stream-Token` auth (§3.3): no total-request
    /// timeout — the stream is long-lived.
    pub async fn open(server: &TestServer, token: &str) -> Self {
        let res = reqwest::Client::new()
            .get(format!("{}/realtime/v1/stream", server.base_url))
            .header("X-Stream-Token", token)
            .send()
            .await
            .expect("stream connect");
        assert_eq!(res.status(), 200, "valid token connects");
        Self {
            res,
            buf: Vec::new(),
        }
    }

    /// Read until the first `event: message` frame arrives; its data
    /// parsed as JSON. Panics on a 10 s deadline — tests must not hang.
    pub async fn next_push_event(&mut self) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(!remaining.is_zero(), "timed out waiting for a push event");
            let chunk = tokio::time::timeout(remaining, self.res.chunk())
                .await
                .expect("read deadline hit waiting for a push event")
                .expect("stream read error")
                .expect("stream ended before a push event");
            self.buf.extend_from_slice(&chunk);
            let (frames, rest) = parse_frames(&self.buf);
            self.buf = rest;
            for frame in frames {
                if frame.event.as_deref() == Some("message") {
                    return serde_json::from_str(&frame.data).expect("event data is JSON");
                }
            }
        }
    }
}

/// Parse every complete frame out of a raw byte buffer, returning the
/// frames and the leftover partial bytes.
fn parse_frames(buf: &[u8]) -> (Vec<SseFrame>, Vec<u8>) {
    let text = String::from_utf8_lossy(buf).into_owned();
    let mut frames = Vec::new();
    let mut rest = &text[..];
    while let Some(pos) = rest.find("\n\n") {
        let (frame_text, tail) = rest.split_at(pos);
        let mut frame = SseFrame::default();
        let mut data_lines = Vec::new();
        for line in frame_text.split('\n') {
            if line.starts_with(':') {
                // Comment: connected / keep-alive — not an event.
            } else if let Some(value) = line.strip_prefix("event: ") {
                frame.event = Some(value.to_string());
            } else if let Some(value) = line.strip_prefix("id: ") {
                let _ = value; // ids matter to the receive loop, not here
            } else if let Some(value) = line.strip_prefix("data: ") {
                data_lines.push(value.to_string());
            } else if line == "data:" {
                data_lines.push(String::new());
            }
        }
        frame.data = data_lines.join("\n");
        frames.push(frame);
        rest = &tail[2..];
    }
    (frames, rest.as_bytes().to_vec())
}
