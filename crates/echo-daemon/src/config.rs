//! The Subscription-list configuration (ADR-0001): every Subscription is
//! one server URL + one Device + one Stream Token. The TOML file holds
//! **non-secret** data only — secrets live in the secret store
//! ([`crate::secrets`]); the config carries their public handles
//! (`token_hint`, `device_hint`) and the Stream Token's `token_id`, which
//! doubles as the Subscription's unique id.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The whole persisted configuration: the Subscription list plus
/// per-server entries (Basic Auth, protocol §1.4).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// One entry per held Realtime Stream credential, in add order.
    #[serde(default)]
    pub subscriptions: Vec<Subscription>,
    /// Per-server operator-level settings, keyed by URL.
    #[serde(default)]
    pub servers: Vec<ServerEntry>,
}

/// One Subscription (GLOSSARY.md): one server URL, one Device, one Stream
/// Token. Non-secret fields only — the Device Key and Stream Token
/// secrets are stored under this Subscription's id in the secret store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    /// The Stream Token's `token_id` (protocol §3.2): public, unique per
    /// token, and minted fresh on every add — the Subscription's id.
    pub id: String,
    /// The server base URL, e.g. `http://127.0.0.1:8080` (no trailing
    /// slash).
    pub server: String,
    /// The Stream Token's client label — names the machine this
    /// Subscription mirrors.
    pub label: String,
    /// First 12 characters of the Stream Token secret + `...`: matches
    /// the stored secret to a server-side list entry without
    /// re-exposing it (protocol §3.2).
    pub token_hint: String,
    /// First 8 characters of the Device Key + `...`: same idea for the
    /// Device Key.
    pub device_hint: String,
}

/// Per-server settings (protocol §1.4): Basic Auth credentials an
/// operator may put in front of a bark server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerEntry {
    /// The server base URL this entry applies to.
    pub url: String,
    /// Basic Auth user, empty when the server is open.
    #[serde(default)]
    pub basic_auth_user: String,
    /// Basic Auth password, empty when the server is open.
    #[serde(default)]
    pub basic_auth_password: String,
}

/// Failures of loading or saving the config file — every variant names
/// the file so the user knows what to fix.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The file exists but is not valid TOML for our schema (includes
    /// unknown fields — typos must not silently drop data).
    #[error("config at {path} is malformed: {source}")]
    Malformed {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    /// Reading or writing failed at the OS level.
    #[error("cannot {doing} config at {path}: {source}")]
    Io {
        doing: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl Config {
    /// The operator Basic Auth (protocol §1.4) `server` demands, taken
    /// from the per-server entries — the credentials stream connects and
    /// `/info` observations present alongside the Stream Token.
    pub fn basic_auth(&self, server: &str) -> Option<echo_core::stream::BasicAuth> {
        self.servers
            .iter()
            .find(|entry| entry.url == server && !entry.basic_auth_user.is_empty())
            .map(|entry| echo_core::stream::BasicAuth {
                user: entry.basic_auth_user.clone(),
                password: entry.basic_auth_password.clone(),
            })
    }

    /// Load the config at `path`. A missing file is a fresh install and
    /// yields an empty [`Config`]; anything unreadable or unparseable is
    /// an error naming the file and the spot.
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let raw = match std::fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Config::default());
            }
            Err(err) => {
                return Err(ConfigError::Io {
                    doing: "read",
                    path: path.to_path_buf(),
                    source: err,
                });
            }
        };
        toml::from_str(&raw).map_err(|source| ConfigError::Malformed {
            path: path.to_path_buf(),
            source,
        })
    }

    /// Save the config at `path`, atomically: written to a sibling temp
    /// file first, renamed into place, so a crash never leaves a
    /// half-written config. Parent directories are created as needed.
    pub fn save(&self, path: &Path) -> Result<(), ConfigError> {
        let raw = toml::to_string_pretty(self).map_err(|source| ConfigError::Io {
            doing: "serialize",
            path: path.to_path_buf(),
            source: std::io::Error::other(source),
        })?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ConfigError::Io {
                doing: "create directory for",
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let tmp = path.with_extension("toml.tmp");
        std::fs::write(&tmp, raw).map_err(|source| ConfigError::Io {
            doing: "write",
            path: tmp.clone(),
            source,
        })?;
        std::fs::rename(&tmp, path).map_err(|source| ConfigError::Io {
            doing: "rename",
            path: path.to_path_buf(),
            source,
        })
    }
}
