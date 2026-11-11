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
    /// Resolve a Push into the intent the renderer gets — the Ext Params
    /// mapping the spec pins at the core seam:
    ///
    /// - a Push carrying `ciphertext` renders the encrypted placeholder;
    /// - otherwise `url` (non-empty string) → open the default browser;
    /// - otherwise `copy` (non-empty string) → write the clipboard;
    /// - every other Ext Param is ignored.
    pub fn from_push_fields(
        id: i64,
        title: String,
        subtitle: String,
        body: String,
        ext_params: &serde_json::Map<String, serde_json::Value>,
        created_at: i64,
    ) -> Self {
        if ext_params.contains_key("ciphertext") {
            return Self::encrypted_placeholder(id, created_at);
        }
        let click_action = non_empty_string(ext_params, "url")
            .map(|url| ClickAction::OpenUrl { url })
            .or_else(|| {
                non_empty_string(ext_params, "copy").map(|text| ClickAction::Copy { text })
            });
        Self {
            id,
            title,
            subtitle,
            body,
            click_action,
            created_at,
        }
    }

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

/// An Ext Param's value when it is a non-empty string (numbers and booleans
/// ride Ext Params as JSON values, protocol §3.4 — only strings act).
fn non_empty_string(
    ext_params: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<String> {
    ext_params
        .get(key)
        .and_then(|value| value.as_str())
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}
