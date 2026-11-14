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
    /// The IPC Channel could not be bound beside the lock (ADR-0004):
    /// the run refuses to start rather than serve streams nobody can
    /// query — `status`'s live answer would silently fall back to the
    /// state file forever.
    #[error("cannot bind the IPC channel at {path}: {source}")]
    IpcBind {
        /// The endpoint the channel was to listen on.
        path: String,
        #[source]
        source: std::io::Error,
    },
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

    // The IPC Channel hangs beside the lock (ADR-0004): while the run
    // lives, any local client can ask for the live snapshot. The lock is
    // already ours, so bind may clean a dead daemon's leftover socket;
    // a bind failure is a startup failure, not a degraded run.
    let endpoint = spec.paths.ipc_endpoint();
    let server = echo_ipc::IpcServer::bind(&endpoint).map_err(|source| RunError::IpcBind {
        path: endpoint.to_string(),
        source,
    })?;
    let (channel_stop_tx, channel_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let channel_state = state.clone();
    let mut channel = tokio::spawn(echo_ipc::serve(
        server,
        async move {
            let _ = channel_stop_rx.await;
        },
        move |request| {
            let state = channel_state.clone();
            async move {
                match request {
                    echo_ipc::Request::Status => echo_ipc::Response::Status {
                        snapshot: state.snapshot(),
                    },
                    // Only post-handshake requests arrive here; the
                    // vocabulary has no other request yet (the event
                    // stream is ticket #11), but an unknown request is
                    // answered, never fatal.
                    other => echo_ipc::Response::Error {
                        message: format!("this daemon does not answer {other:?}"),
                    },
                }
            }
        },
    ));

    // One supervisor per Subscription; each owns its stream handle and
    // is stopped through its own fence at shutdown.
    let mut supervisors = Vec::new();
    let mut fences = Vec::new();
    for subscription in &spec.config.subscriptions {
        let (fence_tx, fence_rx) = mpsc::channel::<()>(1);
        fences.push(fence_tx);

        let secret = match spec.store.get(
            &SecretKey::for_subscription(&subscription.server, &subscription.id),
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
        // The operator Basic Auth credentials (§1.4) out of the store
        // (story 18): an unreachable store is this Subscription's failure
        // to record, not the run's.
        let basic_auth = match crate::auth::resolve(
            &spec.config,
            spec.store.as_ref(),
            &subscription.server,
            None,
            None,
        ) {
            Ok(auth) => auth,
            Err(err) => {
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
            basic_auth,
            history_log: HistoryLog::new(spec.paths.history_log(&subscription.id)),
            backoff: Default::default(),
            // The server's default keep-alive interval (§3.4): the idle
            // watchdog declares a silent connection dead at 45 s.
            keepalive_interval: Some(std::time::Duration::from_secs(15)),
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
    // fence in tests). The channel ending early does not end the run:
    // mirroring streams is the daemon's core job, and `status` falls
    // back to the state file — but the failure is logged loudly, and
    // the owner still decides when the run is over.
    tokio::pin!(shutdown);
    tokio::select! {
        _ = &mut shutdown => {}
        served = &mut channel => {
            match served {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!(
                    "the IPC channel stopped: {err}; `status` falls back to the state file"
                ),
                Err(join_error) => eprintln!(
                    "the IPC channel task failed: {join_error}; `status` falls back to the state file"
                ),
            }
            // The owner has not spoken yet; the run holds until it does.
            (&mut shutdown).await;
        }
    }

    // Graceful stop: the channel closes first (its connections die with
    // the aborted tasks), then every supervisor closes its stream — all
    // before the lock is released with this function's scope.
    let _ = channel_stop_tx.send(());
    match channel.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => eprintln!("the IPC channel ended with an error: {err}"),
        Err(join_error) => eprintln!("the IPC channel task failed: {join_error}"),
    }
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
                    // The core hands the intent over first and appends the
                    // History Log line right after, so this state-file
                    // cursor can land a beat before the log line does.
                    // That is safe: the History Log — not this file — is
                    // the reconnect cursor (GLOSSARY) and the durability
                    // anchor; this id only feeds `status`'s display.
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
