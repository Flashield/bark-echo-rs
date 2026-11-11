//! Notification intents: the pure data a Push resolves to at the core seam.
//! `echo-daemon` renders intents as desktop notifications (spec: rendering is
//! out of test; the boundary is the intent). Everything here is data — no
//! platform I/O.

use serde::{Deserialize, Serialize};

/// What clicking a rendered notification does, resolved from a Push's Ext
/// Params (spec Implementation Decisions): `url` wins, `copy` applies only
/// when there is no `url`, every other Ext Param is ignored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClickAction {
    /// Ext Param `url`: open this URL in the default browser.
    OpenUrl {
        /// The URL to open.
        url: String,
    },
    /// Ext Param `copy` (with no `url`): place this text on the clipboard.
    Copy {
        /// The text to write to the clipboard.
        text: String,
    },
}

/// One processed Push, as handed to the renderer: title/subtitle/body plus
/// the resolved click action. Carries the per-Device monotonic message id —
/// the History Log line key and the dedup cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationIntent {
    /// The per-Device monotonic message id (protocol §3.4): dedup key and
    /// reconnect cursor.
    pub id: i64,
    /// Notification title (empty when the Push carried none).
    pub title: String,
    /// Notification subtitle (empty when the Push carried none).
    pub subtitle: String,
    /// Notification body — the content.
    pub body: String,
    /// The resolved click action, `None` when the Push carried neither `url`
    /// nor `copy`.
    pub click_action: Option<ClickAction>,
    /// Unix seconds, assigned by the server at persistence (protocol §3.4).
    pub created_at: i64,
}

impl NotificationIntent {
    /// The encrypted-placeholder intent for a Push carrying `ciphertext`
    /// (spec: end-to-end decryption is out of scope; the placeholder keeps
    /// the History Log readable instead of storing ciphertext noise). The
    /// message id rides along so dedup and the cursor stay exact.
    pub fn encrypted_placeholder(id: i64, created_at: i64) -> Self {
        Self {
            id,
            title: "Encrypted notification".to_owned(),
            subtitle: String::new(),
            body: "This notification is end-to-end encrypted; decryption is not supported yet."
                .to_owned(),
            click_action: None,
            created_at,
        }
    }
}
