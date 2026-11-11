//! Where the daemon's files live: the Subscription list config and the
//! plain-file secret fallback under the standard per-OS config dir
//! (ADR-0001, spec story 19). Resolution reads the environment at call
//! time, so a temp `HOME` hermetically relocates every CLI test.

use std::env;
use std::path::{Path, PathBuf};

/// Why the config dir could not be located — the message names the
/// environment variable to set, so headless users know the fix.
#[derive(Debug, thiserror::Error)]
pub enum PathsError {
    /// No standard config root was found (no `$HOME`/`$XDG_CONFIG_HOME`
    /// on unix, no `%APPDATA%` on Windows).
    #[error(
        "cannot locate the config directory: no standard config root is set{}",
        hint()
    )]
    NoConfigRoot,
}

/// The platform's list of variables a user can set to fix the above.
fn hint() -> &'static str {
    #[cfg(windows)]
    {
        " (set %APPDATA%)"
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        " (set $XDG_CONFIG_HOME or $HOME)"
    }
    #[cfg(target_os = "macos")]
    {
        " (set $HOME)"
    }
}

/// The daemon's two file locations, derived from one config dir.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paths {
    /// The app's own directory inside the per-OS config root.
    pub config_dir: PathBuf,
}

impl Paths {
    /// Locate the config dir from the process environment.
    pub fn resolve() -> Result<Paths, PathsError> {
        #[cfg(unix)]
        {
            let home = env::var_os("HOME").map(PathBuf::from);
            let xdg = env::var_os("XDG_CONFIG_HOME").map(PathBuf::from);
            Self::from_unix_roots(home.as_deref(), xdg.as_deref())
                .map(|config_dir| Paths { config_dir })
        }
        #[cfg(windows)]
        {
            let appdata = env::var_os("APPDATA").map(PathBuf::from);
            Self::from_windows_root(appdata.as_deref()).map(|config_dir| Paths { config_dir })
        }
    }

    /// Wrap an already-resolved config dir (tests, future IPC seam).
    pub fn from_dir(config_dir: impl Into<PathBuf>) -> Paths {
        Paths {
            config_dir: config_dir.into(),
        }
    }

    /// The Subscription-list config file (ADR-0001).
    pub fn config_file(&self) -> PathBuf {
        self.config_dir.join("config.toml")
    }

    /// The plain-file secret fallback's file (spec story 19), beside the
    /// config so one directory describes one daemon install.
    pub fn secrets_file(&self) -> PathBuf {
        self.config_dir.join("secrets.toml")
    }

    /// The single-instance lock file (ADR-0002): an advisory lock the
    /// runtime holds for its lifetime.
    pub fn lock_file(&self) -> PathBuf {
        self.config_dir.join("daemon.lock")
    }

    /// The runtime state file (ADR-0002 MVP): what `status` reads while
    /// the daemon runs.
    pub fn state_file(&self) -> PathBuf {
        self.config_dir.join("daemon-state.json")
    }

    /// The History Log directory (GLOSSARY): one append-only JSONL file
    /// per Subscription, named by the Subscription's id. One file for
    /// every writer — the daemon's receive loop appends live, `pull`
    /// backfills into it, `log` reads it back.
    pub fn history_dir(&self) -> PathBuf {
        self.config_dir.join("history")
    }

    /// One Subscription's History Log path inside [`Paths::history_dir`].
    pub fn history_log(&self, subscription_id: &str) -> PathBuf {
        self.history_dir().join(format!("{subscription_id}.jsonl"))
    }

    #[cfg(unix)]
    fn from_unix_roots(home: Option<&Path>, xdg: Option<&Path>) -> Result<PathBuf, PathsError> {
        #[cfg(target_os = "macos")]
        let root = home.map(|home| home.join("Library/Application Support"));
        #[cfg(not(target_os = "macos"))]
        let root = xdg
            .map(PathBuf::from)
            .or_else(|| home.map(|home| home.join(".config")));
        root.map(|root| root.join("echo-daemon"))
            .ok_or(PathsError::NoConfigRoot)
    }

    #[cfg(windows)]
    fn from_windows_root(appdata: Option<&Path>) -> Result<PathBuf, PathsError> {
        appdata
            .map(|root| root.join("echo-daemon"))
            .ok_or(PathsError::NoConfigRoot)
    }
}

/// Resolve the app config dir on unix from explicit roots: `$XDG_CONFIG_HOME`
/// (Linux's XDG standard) wins, else `$HOME/.config`; on macOS the XDG
/// variable is not the standard and only `$HOME/Library/Application
/// Support` is consulted. Used directly by tests to avoid env races.
#[cfg(all(unix, not(target_os = "macos")))]
pub fn config_dir_from(
    home: Option<&Path>,
    xdg_config_home: Option<&Path>,
) -> Result<PathBuf, PathsError> {
    Paths::from_unix_roots(home, xdg_config_home)
}

/// Resolve the app config dir on macOS from explicit roots: the standard
/// `~/Library/Application Support`; the XDG variable is accepted for
/// signature parity but not consulted.
#[cfg(target_os = "macos")]
pub fn config_dir_from(
    home: Option<&Path>,
    _xdg_config_home: Option<&Path>,
) -> Result<PathBuf, PathsError> {
    Paths::from_unix_roots(home, None)
}

/// Resolve the app config dir on Windows from the explicit `%APPDATA%` root.
#[cfg(windows)]
pub fn config_dir_from(appdata: Option<&Path>) -> Result<PathBuf, PathsError> {
    Paths::from_windows_root(appdata)
}
