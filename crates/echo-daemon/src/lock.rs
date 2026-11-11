//! The single-instance lock (ADR-0002, spec story 23): the daemon owns
//! every Realtime Stream and every desktop notification, so two `run`
//! processes must never coexist — the loser of the lock exits with a
//! clear error instead of double-displaying every Push.
//!
//! The lock is an advisory whole-file lock on `<config_dir>/daemon.lock`,
//! held for the daemon's lifetime. The OS releases it when the holding
//! process dies — crash and `kill -9` included — so no stale-lock cleanup
//! ever has to happen, and a lock file left on disk is never an obstacle.

use std::fs::File;
use std::path::{Path, PathBuf};

/// Why the daemon lock could not be taken.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    /// Another live process holds the lock — the single-instance case.
    /// Starting anyway would double-display every Push, so the caller
    /// must stop that instance (or point this run at another config).
    #[error(
        "another echo-daemon instance is already running (lock held at {path}) — \
         notifications would double-display; stop that instance first"
    )]
    Held { path: PathBuf },
    /// The lock file could not be opened or locked at the OS level.
    #[error("cannot {doing} the daemon lock at {path}: {source}")]
    Io {
        doing: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// The daemon's single-instance lock. Hold the value for the daemon's
/// lifetime; dropping it releases the lock (as does process death).
#[derive(Debug)]
pub struct DaemonLock {
    /// The open lock file — the lock dies with this handle.
    _file: File,
    path: PathBuf,
}

impl DaemonLock {
    /// Take the daemon lock at `path`. Refuses with [`LockError::Held`]
    /// when another live process holds it; creating the parent directory
    /// is part of taking it.
    pub fn acquire(path: impl Into<PathBuf>) -> Result<DaemonLock, LockError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| LockError::Io {
                doing: "create the directory for",
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            // Never truncate on open: the file may belong to a live
            // holder; content is replaced only after the lock is ours.
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|source| LockError::Io {
                doing: "open",
                path: path.clone(),
                source,
            })?;
        match try_lock(&file) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(LockError::Held { path });
            }
            Err(source) => {
                return Err(LockError::Io {
                    doing: "lock",
                    path: path.clone(),
                    source,
                });
            }
        }
        // Diagnostics, not identity: the flock decides ownership. A best-
        // effort write keeps `ps` lookups one command away for an operator
        // staring at the refusal above.
        use std::io::Write;
        let mut file = file;
        let _ = file.set_len(0);
        let _ = writeln!(file, "pid {}", std::process::id());
        Ok(DaemonLock { _file: file, path })
    }

    /// The lock file's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Liveness probe: is a daemon running (its lock held) at `path`?
    /// Never creates the file — an absent lock file means nothing has
    /// ever run here. Transient collisions with a starting daemon are
    /// the caller's to absorb (`run` retries; a probe answer that flips
    /// a heartbeat later is harmless).
    pub fn probe(path: impl AsRef<Path>) -> Result<bool, LockError> {
        let path = path.as_ref().to_path_buf();
        let file = std::fs::OpenOptions::new()
            .create(false)
            .write(true)
            .open(&path)
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::NotFound {
                    // Nothing has ever run here.
                    std::io::Error::from(std::io::ErrorKind::Other)
                } else {
                    err
                }
            });
        let file = match file {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::Other => return Ok(false),
            Err(source) => {
                return Err(LockError::Io {
                    doing: "probe",
                    path,
                    source,
                });
            }
        };
        match try_lock(&file) {
            // Acquired: no daemon is running; drop the transient hold.
            Ok(()) => Ok(false),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
            Err(source) => Err(LockError::Io {
                doing: "probe",
                path,
                source,
            }),
        }
    }
}

/// Try to lock the whole file exclusively, without waiting.
#[cfg(unix)]
fn try_lock(file: &File) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // flock(2): BSD advisory lock, released by the OS on close/process
    // death. LOCK_EX | LOCK_NB = 2 | 4 on Linux and macOS alike; libc is
    // linked by std, so no crate is needed for the one call.
    unsafe extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    let result = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Try to lock the whole file exclusively, without waiting (Windows):
/// a byte-range lock over the whole file with `LockFileEx`, released by
/// the OS when the handle closes.
#[cfg(windows)]
fn try_lock(file: &File) -> std::io::Result<()> {
    use std::os::windows::io::AsRawHandle;

    const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x0000_0002;
    const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x0000_0001;
    // ERROR_LOCK_VIOLATION — the "someone else holds it" case.
    const ERROR_LOCK_VIOLATION: i32 = 33;

    #[repr(C)]
    struct Overlapped {
        internal: usize,
        internal_high: usize,
        offset: u32,
        offset_high: u32,
        event: *mut core::ffi::c_void,
    }

    unsafe extern "system" {
        fn LockFileEx(
            file: *mut core::ffi::c_void,
            flags: u32,
            reserved: u32,
            bytes_low: u32,
            bytes_high: u32,
            overlapped: *mut Overlapped,
        ) -> i32;
    }

    let mut overlapped = Overlapped {
        internal: 0,
        internal_high: 0,
        offset: 0,
        offset_high: 0,
        event: std::ptr::null_mut(),
    };
    let ok = unsafe {
        LockFileEx(
            file.as_raw_handle() as *mut core::ffi::c_void,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            1, // one byte at offset 0: the whole-file convention
            0,
            &mut overlapped,
        )
    };
    if ok != 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(ERROR_LOCK_VIOLATION) {
        Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
    } else {
        Err(err)
    }
}
