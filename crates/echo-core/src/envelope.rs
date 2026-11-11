//! The server's uniform response envelope (bark protocol §1.1): every
//! JSON answer except `/info` is `{code, message, timestamp[, data]}`,
//! with `code` mirroring the HTTP status. Success is `code == 200`;
//! anything else carries the server's own message. Shared by every
//! client face that decodes one — the Device/Token plane
//! ([`crate::server`]) and the Push sender ([`crate::push`]).

use serde::Deserialize;

/// The upstream response envelope, decode side only (bark protocol §1.1).
/// Routes that answer with data carry it in `data`; the push route and
/// the errors leave it absent.
#[derive(Deserialize)]
pub(crate) struct Envelope {
    pub(crate) code: u16,
    pub(crate) message: String,
    #[serde(default)]
    pub(crate) data: Option<serde_json::Value>,
}
