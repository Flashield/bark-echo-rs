//! The `subscribe` subcommands' logic (ticket #3): adding, listing, and
//! removing Subscriptions as one orchestration over the Subscription-list
//! config (ADR-0001), the secret store, and the server's Device/Token
//! plane. Files and process output stay with the caller (the CLI); this
//! module owns the *what*, not the *where*.

use echo_core::server::{ServerClient, ServerError};

use crate::config::{Config, Subscription};
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
