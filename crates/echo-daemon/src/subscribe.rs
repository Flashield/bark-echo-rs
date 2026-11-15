//! The `subscribe` subcommands' logic (ticket #3): adding, listing, and
//! removing Subscriptions as one orchestration over the Subscription-list
//! config (ADR-0001), the secret store, and the server's Device/Token
//! plane. Files and process output stay with the caller (the CLI); this
//! module owns the *what*, not the *where*.
//!
//! When a daemon holds the single-instance lock, add and remove ride
//! the IPC Channel instead (ticket #14, ADR-0004): the daemon is the
//! single writer and applies the change hot. Without a daemon, the
//! caller keeps today's direct-write path.

use echo_core::server::{ServerClient, ServerError};

use crate::channel;
use crate::config::{Config, Subscription};
use crate::paths::Paths;
use crate::secrets::{SecretError, SecretKey, SecretKind, SecretStore};

/// Where a new Subscription's Device comes from (GLOSSARY.md: one
/// Subscription mirrors exactly one Device).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddSource {
    /// Register a fresh Stream Device and push from this desktop
    /// (spec story 2) — the minted Device Key is the outcome.
    Own,
    /// Mirror an existing Device by its Device Key (spec story 4) —
    /// e.g. a family iPad; the key is the user's own.
    DeviceKey(String),
}

/// What one successful add produced: the persisted Subscription and the
/// Device Key — the push target, shown to the user only here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Added {
    /// The Subscription as persisted into the config.
    pub subscription: Subscription,
    /// The Subscription's Device Key (fresh for [`AddSource::Own`], the
    /// caller's for [`AddSource::DeviceKey`]).
    pub device_key: String,
}

/// Failures of the subscribe operations. Every message is for the user:
/// the server face's typed errors and the store's actionable errors pass
/// through unchanged.
#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    /// The server refused (unknown Device Key, transport failure, ...).
    #[error(transparent)]
    Server(#[from] ServerError),
    /// The secret store failed.
    #[error(transparent)]
    Secret(#[from] SecretError),
    /// No Subscription carries this id — actionable, points at `list`.
    #[error("no Subscription with id `{0}` (see `subscribe list`)")]
    UnknownSubscription(String),
}

/// Register (or reuse) a Device, mint a labelled Stream Token, and
/// persist everything: both secrets under the fresh Subscription id, the
/// operator Basic Auth credentials (§1.4, story 18) when given, the
/// non-secret Subscription into the config. Secrets are stored before the
/// config is touched, so a failed store never leaves an entry pointing at
/// nothing. The `client` must already carry the Basic Auth credentials
/// when the server demands them for minting.
pub async fn add(
    config: &mut Config,
    store: &dyn SecretStore,
    client: &ServerClient,
    source: AddSource,
    label: &str,
    auth: Option<echo_core::stream::BasicAuth>,
) -> Result<Added, SubscribeError> {
    let device_key = match source {
        AddSource::Own => client.register_stream_device().await?.device_key,
        AddSource::DeviceKey(key) => {
            // Minting rejects unknown or mismatched keys with the typed
            // server errors — nothing local to validate first.
            key
        }
    };
    let minted = client.mint_stream_token(&device_key, label).await?;

    let server = client.base_url().to_owned();
    let key = SecretKey::for_subscription(server.clone(), minted.token_id.clone());
    store.set(&key, SecretKind::DeviceKey, &device_key)?;
    store.set(&key, SecretKind::StreamToken, &minted.secret)?;
    // The operator credentials ride in once here and every later
    // connection rides them out of the store (story 18).
    if let Some(auth) = &auth {
        crate::auth::store(config, store, &server, auth)?;
    }

    let subscription = Subscription {
        id: minted.token_id,
        server,
        label: label.to_owned(),
        token_hint: minted.token_hint,
        device_hint: device_hint(&device_key),
    };
    config.subscriptions.push(subscription.clone());
    Ok(Added {
        subscription,
        device_key,
    })
}

/// The Subscription list, in add order.
pub fn list(config: &Config) -> &[Subscription] {
    &config.subscriptions
}

/// Remove one Subscription by id: out of the config, and both of its
/// secrets out of the store (idempotent on leftovers).
pub fn remove(
    config: &mut Config,
    store: &dyn SecretStore,
    id: &str,
) -> Result<Subscription, SubscribeError> {
    let position = config
        .subscriptions
        .iter()
        .position(|subscription| subscription.id == id)
        .ok_or_else(|| SubscribeError::UnknownSubscription(id.to_owned()))?;
    let subscription = config.subscriptions.remove(position);

    let key = SecretKey::for_subscription(&subscription.server, &subscription.id);
    store.delete(&key, SecretKind::DeviceKey)?;
    store.delete(&key, SecretKind::StreamToken)?;
    Ok(subscription)
}

/// First 8 characters + `...` — matches the config's `device_hint` shape.
fn device_hint(device_key: &str) -> String {
    format!("{}...", &device_key[..device_key.len().min(8)])
}

/// Why a channel ride failed (ticket #14). The daemon's own refusals
/// come through as [`IpcError::Rejected`] carrying its message — the
/// actionable part, printed as-is.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    /// A daemon holds the lock, but the channel did not answer within
    /// the budget. Falling through to the direct write would race the
    /// single writer, so the command fails instead — stop that daemon
    /// or retry.
    #[error(
        "a daemon holds the lock but its IPC channel did not answer — \
         stop that instance or retry"
    )]
    Unreachable,
    /// The exchange itself failed (a refusal, a protocol break, a
    /// transport error).
    #[error(transparent)]
    Ipc(#[from] echo_ipc::IpcError),
}

/// `subscribe add` when a daemon holds the lock (ticket #14): the
/// request rides the IPC Channel and the daemon — the single writer —
/// applies it hot, answering once the supervisor is up. The wire shape
/// is the CLI flags' exact face (`--own` / `--device-key`, the label,
/// the optional Basic Auth credentials).
pub async fn add_via_channel(
    paths: &Paths,
    server: &str,
    source: AddSource,
    label: &str,
    auth: Option<echo_core::stream::BasicAuth>,
) -> Result<echo_ipc::message::AddedSubscription, ChannelError> {
    let wire_source = match source {
        AddSource::Own => echo_ipc::message::AddSource::Own,
        AddSource::DeviceKey(device_key) => echo_ipc::message::AddSource::DeviceKey { device_key },
    };
    let wire_auth = auth.map(|credentials| echo_ipc::message::BasicAuthCredentials {
        user: credentials.user,
        password: credentials.password,
    });
    // The ride's budget, attempt cap, and retry shape are the shared
    // loop's (crate::channel::ride); what is left here is the request's
    // wire shape and the answer's fate.
    let answer = channel::ride(paths, |mut client| {
        let source = wire_source.clone();
        let credentials = wire_auth.clone();
        async move {
            client
                .subscribe_add(server, source, label, credentials)
                .await
        }
    })
    .await;
    match answer {
        Ok(added) => Ok(added),
        // The daemon refused and said why (a bad Device Key, a refused
        // label, ...): retrying cannot change the answer.
        Err(err @ echo_ipc::IpcError::Rejected(_)) => Err(err.into()),
        // The budget was spent without an answer: never fall through
        // to the direct write (it would race the single writer) —
        // Unreachable, the caller's actionable failure.
        Err(_) => Err(ChannelError::Unreachable),
    }
}

/// `subscribe rm` when a daemon holds the lock (ticket #14): the
/// request rides the IPC Channel; the daemon stops that stream, erases
/// the secrets, and updates the config before answering.
pub async fn remove_via_channel(
    paths: &Paths,
    id: &str,
) -> Result<echo_ipc::message::RemovedSubscription, ChannelError> {
    let answer = channel::ride(paths, |mut client| async move {
        client.subscribe_remove(id).await
    })
    .await;
    match answer {
        Ok(removed) => Ok(removed),
        Err(err @ echo_ipc::IpcError::Rejected(_)) => Err(err.into()),
        Err(_) => Err(ChannelError::Unreachable),
    }
}
