//! The History Log-facing subcommands' shared logic (ticket #7): which
//! Subscription a `pull` or `log` run addresses, its History Log location,
//! the per-server Basic Auth lookup (protocol §1.4), and the
//! human-readable entry format the `log` command prints. Files and process
//! output stay with the caller (the CLI); this module owns the *what*,
//! not the *where*.

use echo_core::history::HistoryLog;
use echo_core::intent::{ClickAction, NotificationIntent};

use crate::config::{Config, Subscription};
use crate::paths::Paths;
use crate::secrets::{SecretError, SecretKey, SecretKind, SecretStore};

/// How long [`follow_history`] waits between History Log checks — fast
/// enough to feel live, slow enough to stay invisible.
pub const FOLLOW_POLL: std::time::Duration = std::time::Duration::from_millis(200);

/// Failures of resolving what a `pull` or `log` run addresses. Every
/// message is actionable, pointing at the subcommand that fixes it.
#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    /// Nothing is held at all.
    #[error("no Subscriptions held (see `subscribe add`)")]
    NoSubscriptions,
    /// Several are held and no `--sub` picked one.
    #[error("several Subscriptions are held; choose one with `--sub LABEL` (see `subscribe list`)")]
    AmbiguousSubscription,
    /// No Subscription carries this label.
    #[error("no Subscription labelled `{0}` (see `subscribe list`)")]
    UnknownLabel(String),
    /// The secret store failed (a pull needs the Stream Token).
    #[error(transparent)]
    Secret(#[from] SecretError),
}

/// The Subscription a `pull` or `log` run addresses: the `--sub` label
/// when given (first match), else the only Subscription held.
pub fn resolve_subscription(
    config: &Config,
    label: Option<&str>,
) -> Result<Subscription, HistoryError> {
    match label {
        Some(label) => config
            .subscriptions
            .iter()
            .find(|subscription| subscription.label == label)
            .cloned()
            .ok_or_else(|| HistoryError::UnknownLabel(label.to_owned())),
        None => match config.subscriptions.as_slice() {
            [] => Err(HistoryError::NoSubscriptions),
            [only] => Ok(only.clone()),
            _ => Err(HistoryError::AmbiguousSubscription),
        },
    }
}

/// The Subscription's History Log at the daemon's file layout.
pub fn history_log(paths: &Paths, subscription: &Subscription) -> HistoryLog {
    HistoryLog::new(paths.history_log(&subscription.id))
}

/// The Stream Token secret a pull presents as `X-Stream-Token`, out of the
/// secret store under the Subscription's identity.
pub fn stream_token(
    store: &dyn SecretStore,
    subscription: &Subscription,
) -> Result<String, HistoryError> {
    let key = SecretKey {
        server: subscription.server.clone(),
        token_id: subscription.id.clone(),
    };
    Ok(store.get(&key, SecretKind::StreamToken)?)
}

/// One History Log entry, human-readable for the `log` command: the
/// per-Device message id, the server-assigned time (RFC 3339, UTC), the
/// alert text, and the resolved click action.
pub fn format_entry(entry: &NotificationIntent) -> String {
    let when = match time::OffsetDateTime::from_unix_timestamp(entry.created_at) {
        Ok(stamp) => stamp
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| entry.created_at.to_string()),
        Err(_) => entry.created_at.to_string(),
    };
    let alert: Vec<&str> = [
        entry.title.as_str(),
        entry.subtitle.as_str(),
        entry.body.as_str(),
    ]
    .into_iter()
    .filter(|part| !part.is_empty())
    .collect();
    let action = match &entry.click_action {
        Some(ClickAction::OpenUrl { url }) => format!(" [open {url}]"),
        Some(ClickAction::Copy { .. }) => " [copy]".to_owned(),
        None => String::new(),
    };
    if alert.is_empty() {
        format!("#{} {}{}", entry.id, when, action)
    } else {
        format!("#{} {} {}{}", entry.id, when, alert.join(" / "), action)
    }
}

/// Print every History Log entry appended after `seen` (the count already
/// printed) and answer the new count. The tail of `log --follow`: whatever
/// appends to the file — the daemon's receive loop, a pull — shows up live.
pub fn print_new_entries(log: &HistoryLog, seen: usize) -> std::io::Result<usize> {
    let entries = log.entries()?;
    for entry in &entries[seen.min(entries.len())..] {
        println!("{}", format_entry(entry));
    }
    Ok(entries.len())
}
