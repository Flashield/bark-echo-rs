//! The operator Basic Auth credentials (protocol §1.4): one resolution
//! seam for every server connection — `push`, `pull`, `token`, the
//! daemon's streams, and `status`'s /info observation. The user and the
//! stored-password marker are config.toml's business; the password is a
//! secret and lives in the secret store keyed by server (spec story 18).
//! Explicit flags win field-by-field; what a flag leaves unset comes from
//! the stored credentials.

use echo_core::stream::BasicAuth;

use crate::config::{Config, ServerEntry};
use crate::secrets::{SecretError, SecretKey, SecretKind, SecretStore};

/// Resolve the operator credentials for `server` (§1.4): the flags win
/// field-by-field, the stored credentials fill what a flag leaves unset.
/// `None` when neither source holds anything — an open server.
pub fn resolve(
    config: &Config,
    store: &dyn SecretStore,
    server: &str,
    flag_user: Option<&str>,
    flag_password: Option<&str>,
) -> Result<Option<BasicAuth>, SecretError> {
    let user = flag_user
        .map(str::to_owned)
        .or_else(|| config.basic_auth_user(server));
    let password = match flag_password {
        Some(password) => Some(password.to_owned()),
        None => match store.get(
            &SecretKey::for_server(server),
            SecretKind::BasicAuthPassword,
        ) {
            Ok(password) => Some(password),
            // Nothing stored: no credentials for this server at all, or
            // user-only ones (§1.4 lets servers enable the check on
            // user and password independently).
            Err(SecretError::NotFound(_)) => None,
            Err(err) => return Err(err),
        },
    };
    match (user, password) {
        (None, None) => Ok(None),
        (user, password) => Ok(Some(BasicAuth {
            user: user.unwrap_or_default(),
            password: password.unwrap_or_default(),
        })),
    }
}

/// Persist the credentials `subscribe add` was given: the password into
/// the secret store under the server's key (story 18 — config.toml never
/// carries it), the user and the stored-password marker into the config's
/// per-server entry. Called only after the Subscription itself succeeded,
/// secrets before config, like every store write in `add`.
pub fn store(
    config: &mut Config,
    store: &dyn SecretStore,
    server: &str,
    auth: &BasicAuth,
) -> Result<(), SecretError> {
    if !auth.password.is_empty() {
        store.set(
            &SecretKey::for_server(server),
            SecretKind::BasicAuthPassword,
            &auth.password,
        )?;
    }
    let entry = config.servers.iter_mut().find(|entry| entry.url == server);
    match entry {
        Some(entry) => {
            entry.basic_auth_user = auth.user.clone();
            entry.basic_auth_password_stored = !auth.password.is_empty();
        }
        None => config.servers.push(ServerEntry {
            url: server.to_owned(),
            basic_auth_user: auth.user.clone(),
            basic_auth_password_stored: !auth.password.is_empty(),
        }),
    }
    Ok(())
}
