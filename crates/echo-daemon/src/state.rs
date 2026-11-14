//! The daemon's runtime state file: per-Subscription stream state,
//! written atomically on every change. The vocabulary matches the
//! ticket's face — starting, running, disconnected, token invalid,
//! failed — and every record carries the processed notification's id so
//! `status` can show the History Log cursor alongside the live state.
//!
//! The shape is [`echo_ipc::StatusSnapshot`] — the same snapshot the
//! daemon serves over the IPC Channel while it runs (ADR-0004, spec
//! story 24): one vocabulary owned by `echo-ipc`, so the file the
//! daemon persists and the answer its channel gives cannot fork.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

// The state vocabulary is the wire vocabulary (ADR-0004): echo-ipc owns
// StreamState and the snapshot shape; the daemon writes them to disk and
// serves them over the channel.
pub use echo_ipc::message::{StatusSnapshot, StreamState, SubscriptionRuntime};

/// The whole state file: which process is running it and how every
/// Subscription's stream is doing. The same bytes describe the channel's
/// status snapshot.
pub type DaemonState = StatusSnapshot;

/// Failures of reading or persisting the state file.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Reading or writing failed at the OS level, or serialization broke.
    #[error("cannot {doing} the daemon state file at {path}: {source}")]
    Io {
        doing: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// The state file's read face — what `status` (and any future local
/// observer) uses. Missing means "no daemon state recorded"; torn or
/// malformed means an error, never silence.
pub struct StateFile;

impl StateFile {
    /// Read the state file at `path`: `None` when absent, an error when
    /// present but unreadable or unparseable.
    pub fn read(path: &Path) -> Result<Option<DaemonState>, StateError> {
        let raw = match std::fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(StateError::Io {
                    doing: "read",
                    path: path.to_path_buf(),
                    source,
                });
            }
        };
        serde_json::from_str(&raw)
            .map(Some)
            .map_err(|source| StateError::Io {
                doing: "parse",
                path: path.to_path_buf(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
            })
    }
}

/// The runtime's live handle onto the state file: every mutation updates
/// the in-memory snapshot and persists it (atomic temp-file + rename, so
/// a crash never leaves a half-written state for `status` to trip on).
pub struct StateWriter {
    path: PathBuf,
    state: Mutex<DaemonState>,
}

impl StateWriter {
    /// Create (or overwrite) the state file at `path` for a fresh daemon
    /// run: every listed `(id, label)` starts as
    /// [`StreamState::Starting`]. The label rides the snapshot (ticket
    /// #11) so channel clients — the Tray Menu — can name each line
    /// without touching the config.
    pub fn create(
        path: &Path,
        pid: u32,
        subscriptions: &[(String, String)],
    ) -> Result<StateWriter, StateError> {
        let now = unix_now();
        let writer = StateWriter {
            path: path.to_path_buf(),
            state: Mutex::new(DaemonState {
                pid,
                started_at: now,
                subscriptions: subscriptions
                    .iter()
                    .map(|(id, label)| {
                        (
                            id.clone(),
                            SubscriptionRuntime {
                                label: label.clone(),
                                state: StreamState::Starting,
                                last_message_id: None,
                                updated_at: now,
                            },
                        )
                    })
                    .collect(),
            }),
        };
        writer.persist()?;
        Ok(writer)
    }

    /// The current snapshot — the writer's own view, no file read.
    pub fn snapshot(&self) -> DaemonState {
        self.state.lock().expect("state mutex poisoned").clone()
    }

    /// Record a Subscription's stream state change and persist.
    pub fn set(&self, id: &str, state: StreamState) -> Result<(), StateError> {
        self.mutate(id, |runtime| runtime.state = state)
    }

    /// Record the id of a Subscription's last processed notification and
    /// persist.
    pub fn set_last_message_id(&self, id: &str, message_id: i64) -> Result<(), StateError> {
        self.mutate(id, move |runtime| {
            runtime.last_message_id = Some(message_id)
        })
    }

    fn mutate(
        &self,
        id: &str,
        change: impl FnOnce(&mut SubscriptionRuntime),
    ) -> Result<(), StateError> {
        let mut guard = self.state.lock().expect("state mutex poisoned");
        let Some(runtime) = guard.subscriptions.get_mut(id) else {
            // The ids come from the same config the writer was created
            // from, so this never happens in a run.
            return Ok(());
        };
        change(runtime);
        runtime.updated_at = unix_now();
        self.persist_locked(&guard)
    }

    /// Persist the current snapshot. Callers hold no lock.
    fn persist(&self) -> Result<(), StateError> {
        let guard = self.state.lock().expect("state mutex poisoned");
        self.persist_locked(&guard)
    }

    /// Persist under an already-held state lock. Atomic: written to a
    /// sibling temp file first, renamed into place.
    fn persist_locked(&self, state: &DaemonState) -> Result<(), StateError> {
        let raw = serde_json::to_string_pretty(state).map_err(|source| StateError::Io {
            doing: "serialize",
            path: self.path.clone(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
        })?;
        crate::atomic::write(&self.path, &raw, false).map_err(|source| StateError::Io {
            doing: "write",
            path: self.path.clone(),
            source,
        })
    }
}

/// Unix seconds now — the state file's timestamps.
fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or(0)
}
