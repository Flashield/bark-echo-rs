//! The History Log (GLOSSARY): one append-only JSONL file per Subscription.
//! A line is appended only after the notification intent was emitted; the
//! last complete line's message id is the `Last-Event-ID` reconnect cursor
//! (protocol §3.5). A crash mid-append leaves at most one torn line, which
//! is ignored — the replayed event is suppressed by dedup.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::intent::NotificationIntent;

/// The append-only per-Subscription History Log.
#[derive(Debug, Clone)]
pub struct HistoryLog {
    path: PathBuf,
}

impl HistoryLog {
    /// The History Log living at `path` (one per Subscription; the daemon
    /// picks the directory, core owns the format).
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The log's file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one processed notification as a JSONL line. Called only after
    /// the intent was emitted; one append is one single write, so an
    /// interrupted process leaves either a complete line or none. Parent
    /// directories are created as needed, so a first-ever append — live or
    /// pulled — works before anything else laid out the directory.
    pub fn append(&self, entry: &NotificationIntent) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut line = serde_json::to_string(entry).map_err(io_error)?;
        line.push('\n');
        file.write_all(line.as_bytes())
    }

    /// The reconnect cursor: the id on the last complete line, `None` when
    /// the log is missing, empty, or fully torn. A torn final line (a
    /// crashed write) does not become the cursor.
    pub fn cursor(&self) -> std::io::Result<Option<i64>> {
        Ok(self.entries()?.last().map(|entry| entry.id))
    }

    /// All complete entries, oldest first — the face the daemon's `log`
    /// command reads. Torn or malformed lines are skipped.
    pub fn entries(&self) -> std::io::Result<Vec<NotificationIntent>> {
        let raw = match std::fs::read_to_string(&self.path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(err) => return Err(err),
        };
        Ok(raw
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect())
    }
}

/// Wrap a serialization failure in the file-facing error type.
fn io_error(err: serde_json::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, err)
}
