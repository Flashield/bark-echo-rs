//! The Push sender — echo-core's own send module (spec: "a core module
//! owning the server's V2 JSON push request, consumed first by the `push`
//! CLI subcommand; it is the future scheduler's entry point"). It speaks
//! one server face: `POST /push` with the V2 JSON body (protocol §2.2),
//! sending one Push to one Device by its Device Key. Credentials are
//! passed in by the caller (storage is the daemon's job, ADR-0001).
//!
//! Responses arrive in the server's uniform envelope `{code, message,
//! timestamp}` (§1.1): success is `code == 200`; anything else surfaces
//! the server's `code` and `message` verbatim on [`PushError::Server`].

use std::collections::BTreeMap;

use serde::Deserialize;

/// The upstream response envelope (bark protocol §1.1), decode side only.
#[derive(Deserialize)]
struct Envelope {
    code: u16,
    message: String,
}

/// Failures of the Push sender: transport problems, the Basic Auth
/// refusal (§1.4), or a non-200 envelope answer carrying the server's
/// own `code` and `message`.
#[derive(Debug, thiserror::Error)]
pub enum PushError {
    /// The request never completed (connection refused, DNS, timeout).
    #[error("transport failure talking to the bark server: {0}")]
    Transport(#[from] reqwest::Error),
    /// `418 I'm a teapot` — Basic Auth credentials missing or wrong for
    /// an operator-protected server (§1.4; the answer is plain text, not
    /// the JSON envelope).
    #[error("bark server refused the Basic Auth credentials (418): missing or wrong user/password")]
    Teapot,
    /// A non-200 envelope answer: the server's status code and message
    /// verbatim — e.g. an unknown Device Key answers `400 failed to get
    /// device token: failed to get [key] device token from database`.
    #[error("push rejected by the bark server (code {code}): {message}")]
    Server { code: u16, message: String },
}

/// One Push request (GLOSSARY: a Push is one notification request for one
/// Device): the alert's text plus every other parameter, passed through
/// as Ext Params exactly like the server's V2 JSON route. An Ext Param
/// sharing a name with a typed field (`title`, `subtitle`, `body`,
/// `device_key`) overwrites it — the body is built as if the caller had
/// written the JSON by hand.
#[derive(Debug, Clone, Default)]
pub struct Push {
    device_key: String,
    title: Option<String>,
    subtitle: Option<String>,
    body: Option<String>,
    ext_params: BTreeMap<String, String>,
}

impl Push {
    /// Address a Push to a Device by its Device Key.
    pub fn to(device_key: impl Into<String>) -> Self {
        Self {
            device_key: device_key.into(),
            ..Self::default()
        }
    }

    /// The notification's title.
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// The notification's subtitle.
    pub fn subtitle(mut self, subtitle: impl Into<String>) -> Self {
        self.subtitle = Some(subtitle.into());
        self
    }

    /// The notification's content.
    pub fn body(mut self, body: impl Into<String>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Add one Ext Param (`url`, `group`, `sound`, `level`, …): sent as a
    /// string parameter, classified server-side per §2.2.
    pub fn ext_param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.ext_params.insert(key.into(), value.into());
        self
    }

    /// The V2 JSON `POST /push` body (protocol §2.2): the Device Key, the
    /// alert fields when set, then the Ext Params.
    fn body_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("device_key".into(), self.device_key.clone().into());
        if let Some(title) = &self.title {
            map.insert("title".into(), title.clone().into());
        }
        if let Some(subtitle) = &self.subtitle {
            map.insert("subtitle".into(), subtitle.clone().into());
        }
        if let Some(body) = &self.body {
            map.insert("body".into(), body.clone().into());
        }
        for (key, value) in &self.ext_params {
            map.insert(key.clone(), value.clone().into());
        }
        serde_json::Value::Object(map)
    }
}

/// The sender face of one bark server: pushes go out through it, alone —
/// the send module owns no Device/Token state (that plane is
/// [`crate::server::ServerClient`]).
#[derive(Debug, Clone)]
pub struct PushClient {
    base_url: String,
    http: reqwest::Client,
    basic_auth: Option<(String, String)>,
}

impl PushClient {
    /// Build a sender for `base_url` (e.g. `http://127.0.0.1:8080`).
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            http: reqwest::Client::new(),
            basic_auth: None,
        }
    }

    /// Send every push with these Basic Auth credentials (protocol §1.4)
    /// for operator-protected servers. Either field alone is enough —
    /// servers enable the check on `--user`/`--password` independently.
    pub fn with_basic_auth(self, user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            basic_auth: Some((user.into(), password.into())),
            ..self
        }
    }

    /// Send one Push (protocol §2.2): `POST /push`, `Content-Type:
    /// application/json`. Ok(()) means the server accepted and delivered
    /// the Push — the Device's Realtime Streams fan out from there.
    pub async fn send(&self, push: &Push) -> Result<(), PushError> {
        let mut request = self
            .http
            .post(format!("{}/push", self.base_url))
            .json(&push.body_json());
        if let Some((user, password)) = &self.basic_auth {
            request = request.basic_auth(user, Some(password));
        }
        let response = request.send().await?;
        if response.status().as_u16() == 418 {
            return Err(PushError::Teapot);
        }
        let envelope: Envelope = response.json().await?;
        if envelope.code == 200 {
            Ok(())
        } else {
            Err(PushError::Server {
                code: envelope.code,
                message: envelope.message,
            })
        }
    }
}
