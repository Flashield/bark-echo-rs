//! The `status` face (ADR-0002 MVP: the CLI reads the files the runtime
//! writes — no IPC yet): per-Subscription stream state from the runtime
//! state file, plus the server's live `/info` statistics (protocol §2.3)
//! fetched fresh over HTTP. Whether the daemon is alive is answered by
//! the single-instance lock itself.

use echo_core::server::{ServerClient, ServerInfo};

use crate::auth;
use crate::config::Config;
use crate::lock::{DaemonLock, LockError};
use crate::paths::Paths;
use crate::secrets::SecretStore;
use crate::state::{StateError, StateFile, StreamState};

/// Why the status report could not be assembled.
#[derive(Debug, thiserror::Error)]
pub enum StatusError {
    /// The lock file could not be probed at the OS level.
    #[error(transparent)]
    Lock(#[from] LockError),
    /// The state file existed but could not be read.
    #[error(transparent)]
    State(#[from] StateError),
}

/// One Subscription's status: its stream state (when the daemon has an
/// opinion) and its server's live statistics.
#[derive(Debug, Clone)]
pub struct SubscriptionStatus {
    /// The Subscription's id (its Stream Token's `token_id`).
    pub id: String,
    /// The Subscription's client label.
    pub label: String,
    /// The server base URL.
    pub server: String,
    /// The stream state as the runtime recorded it — `None` when no
    /// daemon has ever run or the Subscription predates this daemon run.
    pub state: Option<StreamState>,
    /// The id of the last processed notification — the History Log
    /// cursor (GLOSSARY).
    pub last_message_id: Option<i64>,
    /// The server's live `/info` answer, or the fetch error.
    pub info: Result<ServerInfo, String>,
}

/// The whole report: whether the daemon is running and every
/// Subscription's line.
#[derive(Debug, Clone)]
pub struct StatusReport {
    /// True when another live process holds the daemon lock.
    pub daemon_running: bool,
    /// One line per configured Subscription, in config order.
    pub subscriptions: Vec<SubscriptionStatus>,
}

/// Assemble the report: probe the lock, read the state file, fetch each
/// server's `/info` live. Displayed data is all public; the only secret
/// read is the operator Basic Auth password out of the store (story 18)
/// when a protected server's `/info` demands it — never displayed.
pub async fn report(
    paths: &Paths,
    config: &Config,
    store: &dyn SecretStore,
) -> Result<StatusReport, StatusError> {
    let daemon_running = DaemonLock::probe(paths.lock_file())?;
    let state = StateFile::read(&paths.state_file())?;

    let mut subscriptions = Vec::new();
    for subscription in &config.subscriptions {
        let runtime = state
            .as_ref()
            .and_then(|state| state.subscriptions.get(&subscription.id));
        // The server's live statistics (§2.3): an open server answers
        // /info anonymously — no credentials, so the secret store is not
        // consulted (a headless machine has none, and the stream state
        // above must still report). Only a server with Basic Auth
        // configured (§1.4) resolves credentials; an unreachable store on
        // one of those stays the actionable error on the server line.
        let client = ServerClient::new(&subscription.server);
        let info = if config.basic_auth_configured(&subscription.server) {
            match auth::resolve(config, store, &subscription.server, None, None) {
                Ok(Some(credentials)) => client
                    .with_basic_auth(credentials.user, credentials.password)
                    .info()
                    .await
                    .map_err(|err| err.to_string()),
                Ok(None) => client.info().await.map_err(|err| err.to_string()),
                Err(err) => Err(err.to_string()),
            }
        } else {
            client.info().await.map_err(|err| err.to_string())
        };
        subscriptions.push(SubscriptionStatus {
            id: subscription.id.clone(),
            label: subscription.label.clone(),
            server: subscription.server.clone(),
            state: runtime.map(|runtime| runtime.state.clone()),
            last_message_id: runtime.and_then(|runtime| runtime.last_message_id),
            info,
        });
    }
    Ok(StatusReport {
        daemon_running,
        subscriptions,
    })
}

impl StatusReport {
    /// The human-readable face — the command's stdout.
    pub fn render(&self) -> String {
        let mut out = String::new();
        if self.daemon_running {
            out.push_str("daemon: running\n");
        } else {
            out.push_str("daemon: not running\n");
        }
        if self.subscriptions.is_empty() {
            out.push_str("no subscriptions\n");
            return out;
        }
        for subscription in &self.subscriptions {
            out.push_str(&format!(
                "subscription {}  {}  {}\n",
                subscription.id, subscription.label, subscription.server
            ));
            let cursor = match subscription.last_message_id {
                Some(id) => format!(" (last processed id {id})"),
                None => String::new(),
            };
            match &subscription.state {
                Some(StreamState::Running) => {
                    out.push_str(&format!("  stream: running{cursor}\n"));
                }
                Some(StreamState::Starting) => {
                    out.push_str(&format!("  stream: starting{cursor}\n"));
                }
                Some(StreamState::Disconnected { detail }) => {
                    out.push_str(&format!("  stream: disconnected — {detail}{cursor}\n"));
                }
                Some(StreamState::TokenInvalid) => {
                    out.push_str(&format!(
                        "  stream: token invalid — the Stream Token was revoked or replaced; \
                         re-subscribe this Subscription (`subscribe add`){cursor}\n"
                    ));
                }
                Some(StreamState::Failed { detail }) => {
                    out.push_str(&format!("  stream: stopped — {detail}{cursor}\n"));
                }
                None => {
                    out.push_str(&format!("  stream: unknown{cursor}\n"));
                }
            }
            match &subscription.info {
                Ok(info) => out.push_str(&format!(
                    "  server: active_streams {}, pending_messages {}\n",
                    info.active_streams, info.pending_messages
                )),
                Err(err) => out.push_str(&format!("  server: unreachable ({err})\n")),
            }
        }
        out
    }
}
