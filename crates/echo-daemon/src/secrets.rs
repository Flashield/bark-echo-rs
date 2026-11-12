//! The secret store: Stream Token and Device Key secrets (ADR-0001 —
//! keyed by server + token_id, one of each per Subscription). The OS
//! keyring is the normal home (spec story 18); the plain-file fallback is
//! the explicit opt-in for headless machines (story 19). The trait is the
//! seam; every CLI test runs against [`PlainFileStore`].

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Which secret is being addressed: a Subscription's two (Device Key,
/// Stream Token) or the operator's per-server Basic Auth password.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretKind {
    /// The Device Key — authorizes pushing and token minting (§3.2).
    DeviceKey,
    /// The Stream Token — authorizes reading the Realtime Stream (§3.2).
    StreamToken,
    /// The operator Basic Auth password for one server (§1.4, story 18)
    /// — keyed by server alone, shared by every Subscription on it.
    BasicAuthPassword,
}

impl SecretKind {
    /// The wire name used in store keys and error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            SecretKind::DeviceKey => "device-key",
            SecretKind::StreamToken => "stream-token",
            SecretKind::BasicAuthPassword => "basic-auth-password",
        }
    }
}

/// Locates a Subscription's secrets: the server URL plus the Stream
/// Token's `token_id` (the Subscription's unique id — minted fresh on
/// every add, so two Subscriptions never share a key even when they
/// mirror the same Device).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretKey {
    /// The server base URL.
    pub server: String,
    /// The Subscription's id (its Stream Token's `token_id`).
    pub token_id: String,
}

impl SecretKey {
    /// The key for one Subscription's secrets: the server plus its Stream
    /// Token's `token_id` (the Subscription's id, ADR-0001).
    pub fn for_subscription(server: impl Into<String>, token_id: impl Into<String>) -> Self {
        Self {
            server: server.into(),
            token_id: token_id.into(),
        }
    }

    /// The per-server key the operator's Basic Auth password lives under
    /// (§1.4, story 18): one credential per server, shared by every
    /// Subscription on it.
    pub fn for_server(server: impl Into<String>) -> Self {
        Self {
            server: server.into(),
            token_id: String::new(),
        }
    }

    /// The human-readable address of one secret — what NotFound errors
    /// quote and what the keyring account line looks like.
    fn account(&self, kind: SecretKind) -> String {
        format!("{}::{}::{}", kind.as_str(), self.server, self.token_id)
    }
}

/// Failures of the secret store. The unavailable variant carries the
/// `--plain-secrets` hint: a headless machine without a secret service
/// must learn the fallback from the error alone.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    /// The OS credential store is unreachable (no secret service, locked
    /// keychain, ...).
    #[error(
        "secret store unavailable: {0} (hint: pass --plain-secrets to keep secrets in a 0600 file instead)"
    )]
    Unavailable(String),
    /// No secret stored under this account.
    #[error("secret `{0}` not found")]
    NotFound(String),
    /// The plain-file fallback's TOML is damaged.
    #[error("secrets file at {path} is corrupted: {source}")]
    Corrupted {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    /// Reading or writing the fallback file failed at the OS level.
    #[error("cannot {doing} secrets file at {path}: {source}")]
    Io {
        doing: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Where a Subscription's secrets live. Implementations: the OS keyring
/// (normal mode) and the 0600 plain file (`--plain-secrets`). The
/// runtime holds the store behind an `Arc` across await points, so the
/// trait carries `Send + Sync`.
pub trait SecretStore: Send + Sync {
    /// Store `secret` under `key`/`kind`, replacing any previous value.
    fn set(&self, key: &SecretKey, kind: SecretKind, secret: &str) -> Result<(), SecretError>;
    /// Fetch the secret stored under `key`/`kind`.
    fn get(&self, key: &SecretKey, kind: SecretKind) -> Result<String, SecretError>;
    /// Remove the secret under `key`/`kind`; removing an absent secret is
    /// fine (a Subscription's rm must not strand on leftovers).
    fn delete(&self, key: &SecretKey, kind: SecretKind) -> Result<(), SecretError>;
}

/// One section of the plain fallback file: `[ "<server>::<token_id>" ]`.
type FileContents = BTreeMap<String, BTreeMap<String, String>>;

/// The headless fallback (spec story 19): secrets in a TOML file with
/// owner-only permissions. One section per Subscription, one field per
/// secret kind. Writes are atomic (temp file + rename).
#[derive(Debug, Clone)]
pub struct PlainFileStore {
    path: PathBuf,
}

impl PlainFileStore {
    /// A store backed by the file at `path` (usually
    /// `<config_dir>/secrets.toml`).
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The backing file's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn section(key: &SecretKey) -> String {
        format!("{}::{}", key.server, key.token_id)
    }

    fn read(&self) -> Result<FileContents, SecretError> {
        let raw = match std::fs::read_to_string(&self.path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(err) => {
                return Err(SecretError::Io {
                    doing: "read",
                    path: self.path.clone(),
                    source: err,
                });
            }
        };
        toml::from_str(&raw).map_err(|source| SecretError::Corrupted {
            path: self.path.clone(),
            source,
        })
    }

    fn write(&self, contents: &FileContents) -> Result<(), SecretError> {
        let raw = toml::to_string_pretty(contents).map_err(|source| SecretError::Io {
            doing: "serialize",
            path: self.path.clone(),
            source: std::io::Error::other(source),
        })?;
        // Atomic, owner-only: the helper restricts the temp file before
        // the rename, so no plaintext-bearing file ever has wider perms.
        crate::atomic::write(&self.path, &raw, true).map_err(|source| SecretError::Io {
            doing: "write",
            path: self.path.clone(),
            source,
        })
    }
}

impl SecretStore for PlainFileStore {
    fn set(&self, key: &SecretKey, kind: SecretKind, secret: &str) -> Result<(), SecretError> {
        let mut contents = self.read()?;
        contents
            .entry(Self::section(key))
            .or_default()
            .insert(kind.as_str().to_owned(), secret.to_owned());
        self.write(&contents)
    }

    fn get(&self, key: &SecretKey, kind: SecretKind) -> Result<String, SecretError> {
        self.read()?
            .get(&Self::section(key))
            .and_then(|section| section.get(kind.as_str()))
            .cloned()
            .ok_or_else(|| SecretError::NotFound(key.account(kind)))
    }

    fn delete(&self, key: &SecretKey, kind: SecretKind) -> Result<(), SecretError> {
        let mut contents = self.read()?;
        let section = Self::section(key);
        let now_empty = contents
            .get_mut(&section)
            .map(|fields| {
                fields.remove(kind.as_str());
                fields.is_empty()
            })
            .unwrap_or(false);
        if now_empty {
            // The Subscription's last secret is gone: drop its section.
            contents.remove(&section);
        }
        self.write(&contents)
    }
}

/// The normal secret home (spec story 18): the OS credential store —
/// keychain on macOS, Credential Manager on Windows, Secret Service on
/// Linux. One keyring entry per secret, addressed
/// `service = "bark-echo"`, `account = "<kind>::<server>::<token_id>"`
/// (ADR-0001: keyed by server + token_id — the fresh-per-add `token_id`
/// keeps two Subscriptions on one Device from sharing a key).
#[derive(Debug, Clone, Default)]
pub struct KeyringStore {
    service: &'static str,
}

impl KeyringStore {
    /// A store in the OS keyring under this application's service name.
    pub fn new() -> Self {
        Self {
            service: "bark-echo",
        }
    }

    fn entry(&self, key: &SecretKey, kind: SecretKind) -> Result<keyring::Entry, SecretError> {
        keyring::Entry::new(self.service, &key.account(kind)).map_err(map_keyring_error)
    }
}

impl SecretStore for KeyringStore {
    fn set(&self, key: &SecretKey, kind: SecretKind, secret: &str) -> Result<(), SecretError> {
        self.entry(key, kind)?
            .set_password(secret)
            .map_err(map_keyring_error)
    }

    fn get(&self, key: &SecretKey, kind: SecretKind) -> Result<String, SecretError> {
        self.entry(key, kind)?
            .get_password()
            .map_err(|err| match err {
                keyring::Error::NoEntry => SecretError::NotFound(key.account(kind)),
                other => map_keyring_error(other),
            })
    }

    fn delete(&self, key: &SecretKey, kind: SecretKind) -> Result<(), SecretError> {
        self.entry(key, kind)?
            .delete_credential()
            .map(|_| ())
            .or_else(|err| match err {
                // Absent counts as deleted: rm must not strand on leftovers.
                keyring::Error::NoEntry => Ok(()),
                other => Err(map_keyring_error(other)),
            })
    }
}

/// Fold every keyring failure into the actionable surface: unreachable
/// stores point at the `--plain-secrets` fallback (see [`SecretError::Unavailable`]).
fn map_keyring_error(err: keyring::Error) -> SecretError {
    SecretError::Unavailable(err.to_string())
}
