# The daemon is the sole runtime; GUIs attach over local IPC

The echo-daemon process owns every Realtime Stream, the desktop
notifications, and the History Log. The future tray menu and GUI app are
clients that attach over a local IPC channel (Unix domain socket / named
pipe), not second runtimes — two processes holding streams would
double-display every Push. The CLI's live query (`status`) already
speaks the same protocol: when a daemon holds the lock, its snapshot
rides the channel (ADR-0004); with no daemon running, the CLI reads the
runtime state file as before.

## Consequences

- Single instance enforced by an advisory lock file (`daemon.lock`);
  the IPC endpoint hangs beside it, derived from the same config dir
  (ADR-0004).
- Shipped without IPC first: CLI subcommands other than `status` still
  read the History Log and config files directly, and the runtime state
  file remains the source `status` falls back to when no daemon answers
  — the file is the crash-recovery record, the channel the live view.
- Workspace layout (`echo-core` + `echo-daemon` + `echo-ipc`, GUI crate
  slot later — now chosen, see ADR-0003)
  exists to make this attachment cheap (ADR-0001's config model is
  unaffected).
