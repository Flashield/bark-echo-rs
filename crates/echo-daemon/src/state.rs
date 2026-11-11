//! The daemon's runtime state file (ADR-0002 MVP: `status` reads files
//! the runtime writes — no IPC yet): per-Subscription stream state,
//! written atomically on every change. The vocabulary matches the
//! ticket's face — starting, running, disconnected, token invalid,
//! failed — and every record carries the processed notification's id so
//! `status` can show the History Log cursor alongside the live state.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Per-Subscription stream state as the runtime sees it (ticket #5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum StreamState {
    /// Spawned, no `: connected` handshake yet.
    Starting,
    /// Handshake completed — pushes published now are delivered (§3.3).
    Running,
    /// The connection dropped or a connect failed; retrying with backoff.
    Disconnected {
        /// What happened, as the receive loop phrased it.
        detail: String,
    },
    /// 401 `stream token invalid` (§5): actionable — re-subscribe this
    /// Subscription (spec story 21). Never retried.
    TokenInvalid,
    /// The loop stopped for another reason (Basic Auth teapot, History
    /// Log failure). The detail names it.
    Failed {
        /// What stopped the loop, as the receive loop phrased it.
        detail: String,
    },
}

impl StreamState {
    /// The state's name — the word `status` prints.
    pub fn as_str(&self) -> &'static str {
        match self {
            StreamState::Starting => "starting",
            StreamState::Running => "running",
            StreamState::Disconnected { .. } => "disconnected",
            StreamState::TokenInvalid => "token invalid",
            StreamState::Failed { .. } => "failed",
        }
    }
}

/// One Subscription's runtime line in the state file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionRuntime {
    /// How the stream is doing right now.
    pub state: StreamState,
    /// The id of the last processed notification — the History Log's
    /// cursor (GLOSSARY).
    pub last_message_id: Option<i64>,
    /// Unix seconds of the last change to this line.
    pub updated_at: i64,
}

/// The whole state file: which process is running it and how every
/// Subscription's stream is doing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonState {
    /// The daemon process's pid — ties the file to `ps` output.
    pub pid: u32,
    /// Unix seconds when this daemon instance started.
    pub started_at: i64,
    /// Per-Subscription lines, keyed by Subscription id (the Stream
    /// Token's `token_id`).
    pub subscriptions: BTreeMap<String, SubscriptionRuntime>,
}

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
    /// run: every listed Subscription starts as [`StreamState::Starting`].
    pub fn create(
        path: &Path,
        pid: u32,
        subscription_ids: &[String],
    ) -> Result<StateWriter, StateError> {
        let now = unix_now();
        let writer = StateWriter {
            path: path.to_path_buf(),
            state: Mutex::new(DaemonState {
                pid,
                started_at: now,
                subscriptions: subscription_ids
                    .iter()
                    .map(|id| {
                        (
                            id.clone(),
                            SubscriptionRuntime {
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
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| StateError::Io {
                doing: "create the directory for",
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, raw).map_err(|source| StateError::Io {
            doing: "write",
            path: tmp.clone(),
            source,
        })?;
        std::fs::rename(&tmp, &self.path).map_err(|source| StateError::Io {
            doing: "rename",
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
