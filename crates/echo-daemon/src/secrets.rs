//! The secret store: Stream Token and Device Key secrets (ADR-0001 —
//! keyed by server + device, one of each per Subscription). The OS
//! keyring is the normal home (spec story 18); the plain-file fallback is
//! the explicit opt-in for headless machines (story 19). The trait is the
//! seam; every CLI test runs against [`PlainFileStore`].

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Which of a Subscription's two secrets is being addressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretKind {
    /// The Device Key — authorizes pushing and token minting (§3.2).
    DeviceKey,
    /// The Stream Token — authorizes reading the Realtime Stream (§3.2).
    StreamToken,
}

impl SecretKind {
    /// The wire name used in store keys and error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            SecretKind::DeviceKey => "device-key",
            SecretKind::StreamToken => "stream-token",
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
/// (normal mode) and the 0600 plain file (`--plain-secrets`).
pub trait SecretStore {
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
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| SecretError::Io {
                doing: "create directory for",
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let tmp = self.path.with_extension("toml.tmp");
        std::fs::write(&tmp, raw).map_err(|source| SecretError::Io {
            doing: "write",
            path: tmp.clone(),
            source,
        })?;
        restrict_to_owner(&tmp)?;
        std::fs::rename(&tmp, &self.path).map_err(|source| SecretError::Io {
            doing: "rename",
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

/// Set owner-only (0600) permissions where the concept exists; other
/// platforms keep their default per-user ACLs.
fn restrict_to_owner(path: &Path) -> Result<(), SecretError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(
            |source| SecretError::Io {
                doing: "restrict",
                path: path.to_path_buf(),
                source,
            },
        )?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
