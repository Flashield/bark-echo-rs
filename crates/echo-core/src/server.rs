//! The bark-server client face of echo-core: registering Stream Devices
//! (protocol §3.1) and the Stream Token lifecycle (§3.2), later the push
//! sender. Everything here speaks HTTP to one server base URL; credentials
//! are passed in by the caller (storage is the daemon's job, ADR-0001).
//!
//! Responses arrive in the server's uniform envelope `{code, message,
//! timestamp[, data]}` with `code` mirroring the HTTP status — success is
//! `code == 200`, anything else is mapped onto [`ServerError`].

use serde::Deserialize;

/// The upstream response envelope (bark protocol §1).
#[derive(Deserialize)]
struct Envelope {
    code: u16,
    message: String,
    #[serde(default)]
    data: Option<serde_json::Value>,
}

/// Failures of the server client face: transport problems, or a response
/// the protocol's error table does not (yet) model for this call.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// The request never completed (connection refused, DNS, timeout).
    #[error("transport failure talking to the bark server: {0}")]
    Transport(#[from] reqwest::Error),
    /// A non-200 envelope answer without a typed mapping.
    #[error("bark server answered {status}: {message}")]
    Unexpected { status: u16, message: String },
}

/// A Stream Device registration (GLOSSARY.md: a Device without a Device
/// Token, addressable by its Device Key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDevice {
    /// The 22-char Device Key: authorizes pushing and token minting.
    pub device_key: String,
}

/// A freshly minted Stream Token. The full `secret` appears in a server
/// response exactly once — here (protocol §3.2) — and must be stored
/// immediately; later responses identify the token by `token_id` and
/// `token_hint` only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MintedStreamToken {
    /// `st_` + 43 unpadded base64url chars. The read credential.
    pub secret: String,
    /// 16 lowercase hex chars — the public handle for list/revoke.
    pub token_id: String,
    /// First 12 characters of the secret + `...` — matches a stored
    /// secret to a list entry without re-exposing it.
    pub token_hint: String,
    /// The free-form label naming the Realtime Client this token is for.
    pub client_label: String,
}

/// A client of one bark server's Device/Token plane.
#[derive(Debug, Clone)]
pub struct ServerClient {
    base_url: String,
    http: reqwest::Client,
}

impl ServerClient {
    /// Build a client for `base_url` (e.g. `http://127.0.0.1:8080`).
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            http: reqwest::Client::new(),
        }
    }

    /// Register a fresh Stream Device (protocol §3.1): `device_type:
    /// "stream"`, no Device Token by invariant. Returns the minted
    /// Device Key.
    pub async fn register_stream_device(&self) -> Result<StreamDevice, ServerError> {
        self.register(serde_json::json!({ "device_type": "stream" }))
            .await
    }

    /// Re-register a Stream Device under a known Device Key — the
    /// protocol's refresh-in-place: the same key comes back, updated.
    pub async fn refresh_stream_device(
        &self,
        device_key: &str,
    ) -> Result<StreamDevice, ServerError> {
        self.register(serde_json::json!({
            "device_type": "stream",
            "device_key": device_key,
        }))
        .await
    }

    /// Mint a Stream Token for a Device (protocol §3.2): one revocable
    /// read credential per Realtime Client, labelled `client_label`.
    pub async fn mint_stream_token(
        &self,
        device_key: &str,
        client_label: &str,
    ) -> Result<MintedStreamToken, ServerError> {
        let url = format!("{}/realtime/v1/devices/{device_key}/streams", self.base_url);
        let envelope = self
            .post_envelope(
                url,
                Some(device_key),
                &serde_json::json!({ "client_label": client_label }),
            )
            .await?;
        let data = envelope.data.expect("mint success carries data");
        Ok(MintedStreamToken {
            secret: data["token"]
                .as_str()
                .expect("token is a string")
                .to_owned(),
            token_id: data["token_id"]
                .as_str()
                .expect("token_id is a string")
                .to_owned(),
            token_hint: data["token_hint"]
                .as_str()
                .expect("token_hint is a string")
                .to_owned(),
            client_label: data["client_label"]
                .as_str()
                .expect("client_label is a string")
                .to_owned(),
        })
    }

    /// Shared `POST /register` flow for both registration flavors.
    async fn register(&self, body: serde_json::Value) -> Result<StreamDevice, ServerError> {
        let envelope = self
            .post_envelope(format!("{}/register", self.base_url), None, &body)
            .await?;
        let data = envelope.data.expect("register success carries data");
        Ok(StreamDevice {
            device_key: data["device_key"]
                .as_str()
                .expect("device_key is a string")
                .to_owned(),
        })
    }

    /// POST JSON to `url` (optionally presenting the `X-Device-Key`
    /// header) and map the envelope onto a typed result.
    async fn post_envelope(
        &self,
        url: String,
        device_key: Option<&str>,
        body: &serde_json::Value,
    ) -> Result<Envelope, ServerError> {
        let mut request = self.http.post(&url).json(body);
        if let Some(key) = device_key {
            request = request.header("X-Device-Key", key);
        }
        let response = request.send().await?;
        let status = response.status().as_u16();
        let envelope: Envelope = response.json().await?;
        if envelope.code == 200 {
            Ok(envelope)
        } else {
            Err(Self::map_error(status, envelope.message))
        }
    }

    /// Map a non-200 answer onto the error surface. Protocol §3.2's typed
    /// errors grow here as the faces that surface them land.
    fn map_error(status: u16, message: String) -> ServerError {
        ServerError::Unexpected { status, message }
    }
}
