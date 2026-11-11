//! The daemon runtime (ticket #5, ADR-0002): the sole owner of every
//! Realtime Stream. `run` takes the single-instance lock, opens one
//! stream per Subscription via the core receive loop, renders every
//! notification intent through the injected renderer, records the
//! per-Subscription state (and the History Log cursor) into the state
//! file `status` reads, and rides out server restarts — the receive
//! loop reconnects with replay while the runtime keeps rendering.
//!
//! A 401 `stream token invalid` is the one terminal-but-isolated case:
//! that Subscription is flagged `token invalid` (actionable —
//! re-subscribe, spec story 21) and never retried, while the other
//! Subscriptions carry on.

use std::sync::Arc;

use echo_core::history::HistoryLog;
use echo_core::intent::NotificationIntent;
use echo_core::stream::{RealtimeStream, StreamConfig, StreamEnd, StreamEvent};
use tokio::sync::mpsc;

use crate::config::Config;
use crate::lock::{DaemonLock, LockError};
use crate::notify::NotificationRenderer;
use crate::paths::Paths;
use crate::secrets::{SecretKey, SecretKind, SecretStore};
use crate::state::{StateWriter, StreamState};

/// Everything a daemon run needs, handed in by the CLI (or a test):
/// where the files live, the loaded Subscription list, the secret store,
/// and the renderer the intents flow into.
pub struct DaemonSpec {
    /// The daemon's file locations.
    pub paths: Paths,
    /// The loaded Subscription list (ADR-0001) with per-server entries.
    pub config: Config,
    /// The secret store the Stream Tokens come from.
    pub store: Arc<dyn SecretStore>,
    /// The notification renderer — real in production, a fake in tests.
    pub renderer: Arc<dyn NotificationRenderer>,
}

/// Why a run could not start. In-run problems (a render hiccup, a
/// rejected token) never land here — they are logged or recorded in the
/// state file, and the run carries on.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// Another live instance holds the single-instance lock (ADR-0002).
    #[error(transparent)]
    Lock(#[from] crate::lock::LockError),
    /// The state file could not be set up at startup.
    #[error(transparent)]
    State(#[from] crate::state::StateError),
    /// Nothing to mirror — the actionable fix is `subscribe add`.
    #[error("no subscriptions configured — add one with `subscribe add` first")]
    NoSubscriptions,
}

/// Run the daemon until `shutdown` resolves: one supervised Realtime
/// Stream per Subscription, every intent rendered, the state file kept
/// current. The single-instance lock is held for the whole run and
/// released on return.
pub async fn run(
    spec: DaemonSpec,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), RunError> {
    let ids: Vec<String> = spec
        .config
        .subscriptions
        .iter()
        .map(|subscription| subscription.id.clone())
        .collect();
    if ids.is_empty() {
        return Err(RunError::NoSubscriptions);
    }

    // Two daemons would double-display every Push (ADR-0002): the loser
    // of the lock leaves with a clear error. The lock can also be held
    // for a heartbeat by a `status` probe, so a starting daemon absorbs
    // those transients before concluding a real second instance exists.
    let mut attempts = 0;
    let _lock = loop {
        match DaemonLock::acquire(spec.paths.lock_file()) {
            Ok(lock) => break lock,
            Err(LockError::Held { .. }) if attempts < 6 => {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(err) => return Err(err.into()),
        }
    };
    // The History Logs land here, one per Subscription (GLOSSARY); the
    // directory must exist before the first append or the receive loop
    // stops rather than emit unlogged intents.
    std::fs::create_dir_all(spec.paths.history_dir()).map_err(|source| {
        crate::state::StateError::Io {
            doing: "create the history directory",
            path: spec.paths.history_dir(),
            source,
        }
    })?;
    let state = Arc::new(StateWriter::create(
        &spec.paths.state_file(),
        std::process::id(),
        &ids,
    )?);

    // One supervisor per Subscription; each owns its stream handle and
    // is stopped through its own fence at shutdown.
    let mut supervisors = Vec::new();
    let mut fences = Vec::new();
    for subscription in &spec.config.subscriptions {
        let (fence_tx, fence_rx) = mpsc::channel::<()>(1);
        fences.push(fence_tx);

        let secret = match spec.store.get(
            &SecretKey {
                server: subscription.server.clone(),
                token_id: subscription.id.clone(),
            },
            SecretKind::StreamToken,
        ) {
            Ok(secret) => secret,
            Err(err) => {
                // No read credential: this Subscription cannot run, but
                // the others still must. Record and move on.
                record(
                    &state,
                    &subscription.id,
                    StreamState::Failed {
                        detail: err.to_string(),
                    },
                );
                continue;
            }
        };

        let (events_tx, events_rx) = mpsc::channel(64);
        let config = StreamConfig {
            base_url: subscription.server.clone(),
            stream_token: secret,
            basic_auth: spec.config.basic_auth(&subscription.server),
            history_log: HistoryLog::new(
                spec.paths
                    .history_dir()
                    .join(format!("{}.jsonl", subscription.id)),
            ),
            backoff: Default::default(),
            events: Some(events_tx),
        };
        let (handle, intents) = RealtimeStream::spawn(config);
        supervisors.push(tokio::spawn(supervise(
            subscription.id.clone(),
            handle,
            intents,
            events_rx,
            state.clone(),
            spec.renderer.clone(),
            fence_rx,
        )));
    }

    // Hold the runtime until the owner lets go (ctrl-C in production, a
    // fence in tests).
    shutdown.await;

    // Graceful stop: every supervisor closes its stream before the lock
    // is released with this function's scope.
    for fence in &fences {
        let _ = fence.send(()).await;
    }
    for supervisor in supervisors {
        let _ = supervisor.await;
    }
    Ok(())
}

/// One Subscription's runtime: forward the stream's intents into the
/// renderer and its connection-state changes into the state file, until
/// either the fence (shutdown) or the loop's own end. A self-ended loop
/// is classified into its terminal state — token invalid means
/// re-subscribe, never retry.
async fn supervise(
    id: String,
    handle: echo_core::stream::StreamHandle,
    mut intents: mpsc::Receiver<NotificationIntent>,
    mut events: mpsc::Receiver<StreamEvent>,
    state: Arc<StateWriter>,
    renderer: Arc<dyn NotificationRenderer>,
    mut fence: mpsc::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = fence.recv() => break,
            change = events.recv() => match change {
                Some(StreamEvent::Connected) => {
                    record(&state, &id, StreamState::Running);
                }
                Some(StreamEvent::Disconnected { detail }) => {
                    record(&state, &id, StreamState::Disconnected { detail });
                }
                // The loop ended by itself; the terminal state is
                // resolved below.
                None => break,
            },
            intent = intents.recv() => match intent {
                Some(intent) => {
                    if let Err(err) = renderer.render(&intent) {
                        eprintln!("notification render failed: {err}");
                    }
                    // The History Log line exists by now (the core
                    // appends before the intent is handed over), so the
                    // cursor follows it regardless of render success.
                    if let Err(err) = state.set_last_message_id(&id, intent.id) {
                        eprintln!("state file update failed: {err}");
                    }
                }
                None => break,
            },
        }
    }

    match handle.stop().await {
        StreamEnd::TokenInvalid => {
            // 401 `stream token invalid` (§5): actionable, not retryable
            // (spec story 21).
            record(&state, &id, StreamState::TokenInvalid);
        }
        StreamEnd::AuthRejected { status, message } => {
            record(
                &state,
                &id,
                StreamState::Failed {
                    detail: format!("the server rejected the credentials ({status}): {message}"),
                },
            );
        }
        StreamEnd::HistoryWriteFailed(err) => {
            record(
                &state,
                &id,
                StreamState::Failed {
                    detail: format!("history log write failed: {err}"),
                },
            );
        }
        // A stop we initiated (shutdown) or a loop that was already
        // cancelled: the last live state stands.
        StreamEnd::Stopped => {}
    }
}

/// Record a state change, tolerating a persist hiccup (the daemon keeps
/// running; `status` may lag).
fn record(state: &StateWriter, id: &str, change: StreamState) {
    if let Err(err) = state.set(id, change) {
        eprintln!("state file update failed: {err}");
    }
}
