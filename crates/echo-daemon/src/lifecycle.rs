//! The daemon's lifecycle from the desktop user's point of view (ticket
//! #12, ADR-0004): launching the GUI brings the daemon up when the
//! single-instance lock is idle (spawn-if-absent) and attaches when it
//! is held — never a second daemon attempt. The child is detached so it
//! outlives the GUI process: a GUI crash or kill leaves the daemon
//! mirroring, because mirroring is the daemon's job, not the GUI's.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use crate::lock::{DaemonLock, LockError};
use crate::paths::Paths;

/// What launching the GUI found.
#[derive(Debug)]
pub enum SpawnOutcome {
    /// The lock was held: a daemon is running (or starting) — attach to
    /// it; no second daemon is attempted (two would double-display
    /// every Push, ADR-0002).
    AlreadyRunning,
    /// The lock was idle: `bark-echo run` was spawned detached. The
    /// handle rides along for diagnostics (its pid) and reaping.
    Spawned { child: Child },
}

/// Why the launch could not spawn the daemon it owed the user.
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    /// The lock probe failed — the lock file exists but cannot be
    /// opened or locked for probing.
    #[error(transparent)]
    Probe(#[from] LockError),
    /// This process's own executable path could not be resolved.
    #[error("cannot resolve this executable's path: {0}")]
    Executable(#[from] std::io::Error),
    /// The child process could not be spawned.
    #[error("cannot spawn the daemon at {program}: {source}")]
    Spawn {
        /// The program that was to become the daemon.
        program: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Spawn the daemon beside this process's own executable if the lock is
/// idle; answer [`SpawnOutcome::AlreadyRunning`] if it is held. This is
/// the GUI launch's entry (`bark-echo gui`): GUI and daemon are one
/// binary, so `current_exe` is the daemon to spawn.
pub fn spawn_if_absent(paths: &Paths, plain_secrets: bool) -> Result<SpawnOutcome, SpawnError> {
    let program = std::env::current_exe()?;
    spawn_detached_daemon(&program, paths, plain_secrets)
}

/// [`spawn_if_absent`] with the daemon program named — the test seam
/// (the launch shape driven against the real `bark-echo` binary) and
/// the shape any caller that knows its daemon binary uses.
pub fn spawn_detached_daemon(
    program: &Path,
    paths: &Paths,
    plain_secrets: bool,
) -> Result<SpawnOutcome, SpawnError> {
    // The lock is the arbiter (ADR-0002): held means a daemon is
    // running or starting — attach, never spawn a rival. Idle means the
    // launch owes the user a daemon.
    if DaemonLock::probe(paths.lock_file())? {
        return Ok(SpawnOutcome::AlreadyRunning);
    }

    let mut command = Command::new(program);
    command.arg("run");
    // The child resolves its files from its environment (paths.rs), so
    // it rides the parent's environment verbatim — on a desktop, that
    // is the user's real config dir.
    if plain_secrets {
        command.arg("--plain-secrets");
    }
    // The spawned daemon has no terminal of its own: its logs go to
    // null rather than ride pipes the GUI would have to hold — and a
    // dead pipe would turn the daemon's next log line into a panic.
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;

        // Platform fact (CODING_STANDARDS), verified against
        // Microsoft's CreateProcess flag table
        // (learn.microsoft.com/windows/win32/procthread/process-creation-flags):
        // DETACHED_PROCESS (0x00000008) — the child inherits no console,
        // so closing the GUI's terminal cannot drag the daemon down
        // with it; CREATE_NEW_PROCESS_GROUP (0x00000200) — the child
        // roots a new process group with Ctrl-C disabled, so console
        // events addressed to the GUI's group never land in the daemon.
        // (CREATE_NO_WINDOW is ignored under DETACHED_PROCESS, and
        // CREATE_BREAKAWAY_FROM_JOB is deliberately absent: it fails
        // the spawn outright when the job forbids breakaway. What
        // Windows users still get: the child survives the GUI as long
        // as no kill-on-close job object holds the GUI — a launch from
        // Explorer or the Start menu does not.)
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }
    #[cfg(unix)]
    {
        // Platform fact: a Unix child outlives its parent by default (no
        // PDEATHSIG is set) — but it would share the launcher's terminal
        // process group, so a Ctrl-C addressed to the foreground group
        // would land in the daemon too. `process_group(0)` gives the
        // child its own group: terminal Ctrl-C stops the GUI, never the
        // daemon; only an addressed signal (`kill -INT <pid>`) does, and
        // that one is graceful (exit 0 — no supervisor resurrects it).
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    // The probe-then-spawn gap is arbitrated by the child's own lock
    // take: if a daemon claims the lock in between, the spawned run
    // loses and exits with the standard lock refusal (non-zero, no
    // double display) while the launcher attaches to the winner.
    // Holding the lock across the spawn is not expressible with std —
    // and the loser's exit is already harmless by design.
    let child = command.spawn().map_err(|source| SpawnError::Spawn {
        program: program.to_path_buf(),
        source,
    })?;
    Ok(SpawnOutcome::Spawned { child })
}
