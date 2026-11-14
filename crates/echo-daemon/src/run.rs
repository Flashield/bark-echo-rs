//! The daemon runtime (ticket #5, ADR-0002): the sole owner of every
//! Realtime Stream. `run` takes the single-instance lock, opens one
//! stream per Subscription via the core receive loop, renders every
//! notification intent through the injected renderer, records the
//! per-Subscription state (and the History Log cursor) into the state
//! file `status` reads, and rides out server restarts — the receive
//! loop reconnects with replay while the runtime keeps rendering.
//!
//! The supervisor set is dynamic (ticket #14, ADR-0004's hot-apply):
//! subscriptions start and stop by id while the run lives — the CRUD
//! handlers spawn and retire one stream's supervisor each, and zero
//! Subscriptions is a healthy, actionable state, not a failure.
//!
//! A 401 `stream token invalid` is the one terminal-but-isolated case:
//! that Subscription is flagged `token invalid` (actionable —
//! re-subscribe, spec story 21) and never retried, while the other
//! Subscriptions carry on.

use std::collections::BTreeMap;
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
/// rejected token, a hot apply refused) never land here — they are
/// logged, recorded in the state file, or answered over the channel,
/// and the run carries on.
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
}

/// One live Subscription's handles: how shutdown and hot removal stop
/// that stream (ticket #14 — subscriptions start and stop by id now).
struct LiveStream {
    /// The fence that ends the supervise task. `None` when the stream
    /// never started — its secret could not be read, its `Failed` state
    /// stands, and there is nothing to stop.
    fence: Option<mpsc::Sender<()>>,
    /// The supervise task, joined at stop so no late state recording
    /// races the removal that follows the stop.
    task: Option<tokio::task::JoinHandle<()>>,
}

/// The daemon's mutable world (ADR-0004: the single writer). The
/// Subscription list and the live supervisor set live here; every
/// mutation — the startup spawn and the hot CRUD handlers — serializes
/// on the mutex, so two requests can never interleave a half-apply.
struct Manager {
    /// The daemon's file locations — the config file the single writer
    /// keeps current (ADR-0004).
    paths: Paths,
    /// The secret store the Stream Tokens come from and go back to.
    store: Arc<dyn SecretStore>,
    /// The renderer the intents flow into.
    renderer: Arc<dyn NotificationRenderer>,
    /// The state file writer — the snapshot's source of truth.
    state: Arc<StateWriter>,
    /// The channel's event hub the flips and appends are announced on.
    event_hub: tokio::sync::broadcast::Sender<echo_ipc::Event>,
    /// The Subscription list plus the live supervisor set.
    core: tokio::sync::Mutex<Core>,
}

struct Core {
    /// The Subscription list as the single writer holds it (ADR-0001).
    config: Config,
    /// One entry per Subscription whose stream was started, keyed by
    /// Subscription id. A Subscription whose stream never started (its
    /// secret read failed at spawn) has no entry — removal skips the
    /// stop and cleans the files.
    streams: BTreeMap<String, LiveStream>,
}

impl Manager {
    /// Start one Subscription's Realtime Stream (ADR-0002): the Stream
    /// Token and the operator Basic Auth credentials out of the store,
    /// the core receive loop spawned, a supervisor watching it. The
    /// startup spawn and the hot-apply add go through this one door, so
    /// a hot-added stream is configured exactly like a startup one.
    fn start_stream(&self, core: &mut Core, subscription: &crate::config::Subscription) {
        let secret = match self.store.get(
            &SecretKey::for_subscription(&subscription.server, &subscription.id),
            SecretKind::StreamToken,
        ) {
            Ok(secret) => secret,
            Err(err) => {
                // No read credential: this Subscription cannot run, but
                // the others still must. Record and move on.
                record(
                    &self.state,
                    &self.event_hub,
                    &subscription.id,
                    &subscription.label,
                    StreamState::Failed {
                        detail: err.to_string(),
                    },
                );
                return;
            }
        };
        // The operator Basic Auth credentials (§1.4) out of the store
        // (story 18): an unreachable store is this Subscription's failure
        // to record, not the run's.
        let basic_auth = match crate::auth::resolve(
            &core.config,
            self.store.as_ref(),
            &subscription.server,
            None,
            None,
        ) {
            Ok(auth) => auth,
            Err(err) => {
                record(
                    &self.state,
                    &self.event_hub,
                    &subscription.id,
                    &subscription.label,
                    StreamState::Failed {
                        detail: err.to_string(),
                    },
                );
                return;
            }
        };

        let (events_tx, events_rx) = mpsc::channel(64);
        let (appends_tx, appends_rx) = mpsc::channel(64);
        let config = StreamConfig {
            base_url: subscription.server.clone(),
            stream_token: secret,
            basic_auth,
            history_log: HistoryLog::new(self.paths.history_log(&subscription.id)),
            backoff: Default::default(),
            // The server's default keep-alive interval (§3.4): the idle
            // watchdog declares a silent connection dead at 45 s.
            keepalive_interval: Some(std::time::Duration::from_secs(15)),
            events: Some(events_tx),
            appends: Some(appends_tx),
        };
        let (handle, intents) = RealtimeStream::spawn(config);
        let (fence_tx, fence_rx) = mpsc::channel::<()>(1);
        let task = tokio::spawn(supervise(
            Supervised {
                id: subscription.id.clone(),
                label: subscription.label.clone(),
                state: self.state.clone(),
                renderer: self.renderer.clone(),
                event_hub: self.event_hub.clone(),
            },
            handle,
            intents,
            events_rx,
            appends_rx,
            fence_rx,
        ));
        core.streams.insert(
            subscription.id.clone(),
            LiveStream {
                fence: Some(fence_tx),
                task: Some(task),
            },
        );
    }

    /// Stop one Subscription's stream: fence, then join — the loop is
    /// done recording when this returns, so a removal's file changes
    /// never race a late state write.
    async fn stop_stream(stream: LiveStream) {
        if let Some(fence) = stream.fence {
            let _ = fence.send(()).await;
        }
        if let Some(task) = stream.task {
            let _ = task.await;
        }
    }
}

/// Run the daemon until `shutdown` resolves: one supervised Realtime
/// Stream per Subscription, every intent rendered, the state file kept
/// current. The single-instance lock is held for the whole run and
/// released on return. Zero Subscriptions is healthy: the channel
/// serves an empty snapshot and CRUD can add to it (ticket #14).
pub async fn run(
    spec: DaemonSpec,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), RunError> {
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
        &spec.config.subscriptions,
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
    // The event hub (ticket #11): every state flip and History Log
    // append the runtime makes is broadcast here, and `serve` relays it
    // to every connected client. Capacity: a burst of pushes appends a
    // line each; a client further behind than this is dropped by `serve`
    // and re-snapshots on reconnect — never served stale.
    let (event_hub, _unused_receiver) = tokio::sync::broadcast::channel::<echo_ipc::Event>(256);
    // The single writer's world (ADR-0004): the config the CRUD handlers
    // mutate and the supervisor set they start and stop by id.
    let manager = Arc::new(Manager {
        paths: spec.paths.clone(),
        store: spec.store.clone(),
        renderer: spec.renderer.clone(),
        state: state.clone(),
        event_hub: event_hub.clone(),
        core: tokio::sync::Mutex::new(Core {
            config: spec.config,
            streams: BTreeMap::new(),
        }),
    });

    // The opening supervisor set: one stream per configured Subscription
    // (ADR-0002) — through the same door a hot add uses later, so both
    // start streams exactly alike.
    {
        let mut core = manager.core.lock().await;
        let subscriptions = core.config.subscriptions.clone();
        for subscription in &subscriptions {
            manager.start_stream(&mut core, subscription);
        }
    }

    let (channel_stop_tx, channel_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let channel_manager = manager.clone();
    let mut channel = tokio::spawn(echo_ipc::serve(
        server,
        async move {
            let _ = channel_stop_rx.await;
        },
        event_hub.clone(),
        move |request| {
            let manager = channel_manager.clone();
            async move {
                match request {
                    echo_ipc::Request::Status => echo_ipc::Response::Status {
                        snapshot: manager.state.snapshot(),
                    },
                    // Only post-handshake requests arrive here; the
                    // CRUD vocabulary is answered by the handlers that
                    // land with ticket #14, but an unknown request is
                    // answered, never fatal.
                    other => echo_ipc::Response::Error {
                        message: format!("this daemon does not answer {other:?}"),
                    },
                }
            }
        },
    ));

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
    // the aborted tasks), then every supervisor still in the set closes
    // its stream — the set is whatever hot CRUD left running (ticket
    // #14), read from the map, not remembered from startup — all before
    // the lock is released with this function's scope.
    let _ = channel_stop_tx.send(());
    match channel.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => eprintln!("the IPC channel ended with an error: {err}"),
        Err(join_error) => eprintln!("the IPC channel task failed: {join_error}"),
    }
    let streams = {
        let mut core = manager.core.lock().await;
        std::mem::take(&mut core.streams)
    };
    for (_, stream) in streams {
        Manager::stop_stream(stream).await;
    }
    Ok(())
}

/// One Subscription's runtime: forward the stream's intents into the
/// renderer and its connection-state changes into the state file, until
/// either the fence (shutdown or a hot removal) or the loop's own end.
/// A self-ended loop is classified into its terminal state — token
/// invalid means re-subscribe, never retry.
/// Everything one supervised Subscription's loop touches beyond its
/// stream channels: who it is (id, label — the channel's and the
/// snapshot's name for it), where its flips are recorded, what renders
/// its intents, and which hub hears its flips and appends (ticket #11).
struct Supervised {
    /// The Subscription's id (its Stream Token's `token_id`).
    id: String,
    /// The Subscription's client label.
    label: String,
    /// The state file writer the flips are recorded into.
    state: Arc<StateWriter>,
    /// The renderer the intents flow into.
    renderer: Arc<dyn NotificationRenderer>,
    /// The channel's event hub the flips and appends are announced on.
    event_hub: tokio::sync::broadcast::Sender<echo_ipc::Event>,
}

/// One Subscription's runtime: forward the stream's intents into the
/// renderer, its connection-state changes into the state file — each
/// flip announced on the channel as it is recorded — and its History
/// Log appends onto the channel as events, until either the fence
/// (shutdown or a hot removal) or the loop's own end. A self-ended loop
/// is classified into its terminal state — token invalid means
/// re-subscribe, never retry.
async fn supervise(
    subscription: Supervised,
    handle: echo_core::stream::StreamHandle,
    mut intents: mpsc::Receiver<NotificationIntent>,
    mut events: mpsc::Receiver<StreamEvent>,
    mut appends: mpsc::Receiver<NotificationIntent>,
    mut fence: mpsc::Receiver<()>,
) {
    let Supervised {
        id,
        label,
        state,
        renderer,
        event_hub,
    } = subscription;
    loop {
        tokio::select! {
            _ = fence.recv() => break,
            change = events.recv() => match change {
                Some(StreamEvent::Connected) => {
                    record(&state, &event_hub, &id, &label, StreamState::Running);
                }
                Some(StreamEvent::Disconnected { detail }) => {
                    record(
                        &state,
                        &event_hub,
                        &id,
                        &label,
                        StreamState::Disconnected { detail },
                    );
                }
                // The loop ended by itself; the terminal state is
                // resolved below.
                None => break,
            },
            // The core announces an append only after the line is durably
            // on disk (echo-core's own rule); relaying it here makes the
            // channel's event the line's echo (ticket #11).
            appended = appends.recv() => match appended {
                Some(entry) => {
                    let _ = event_hub.send(echo_ipc::Event::HistoryAppended {
                        subscription_id: id.clone(),
                        label: label.clone(),
                        entry,
                    });
                }
                // The receive loop is gone; its state close follows on the
                // events channel (both senders live in that task).
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
            record(&state, &event_hub, &id, &label, StreamState::TokenInvalid);
        }
        StreamEnd::AuthRejected { status, message } => {
            record(
                &state,
                &event_hub,
                &id,
                &label,
                StreamState::Failed {
                    detail: format!("the server rejected the credentials ({status}): {message}"),
                },
            );
        }
        StreamEnd::HistoryWriteFailed(err) => {
            record(
                &state,
                &event_hub,
                &id,
                &label,
                StreamState::Failed {
                    detail: format!("history log write failed: {err}"),
                },
            );
        }
        // A stop we initiated (shutdown or a hot removal) or a loop that
        // was already cancelled: the last live state stands.
        StreamEnd::Stopped => {}
    }
}

/// Record a state change and announce the flip on the channel — the
/// Tray Menu learns a flip at the moment the runtime records it, not at
/// its next poll (ticket #11). Tolerates a persist hiccup (the daemon
/// keeps running; `status` may lag); the announcement is best-effort —
/// a moment with no listeners costs nothing, the opening snapshot
/// covers a fresh client.
fn record(
    state: &StateWriter,
    events: &tokio::sync::broadcast::Sender<echo_ipc::Event>,
    id: &str,
    label: &str,
    change: StreamState,
) {
    if let Err(err) = state.set(id, change.clone()) {
        eprintln!("state file update failed: {err}");
    }
    let _ = events.send(echo_ipc::Event::StreamState {
        subscription_id: id.to_owned(),
        label: label.to_owned(),
        state: change,
    });
}
