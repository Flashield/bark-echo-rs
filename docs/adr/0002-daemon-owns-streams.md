# The daemon is the sole runtime; GUIs attach over local IPC

The echo-daemon process owns every Realtime Stream, the desktop
notifications, and the History Log. The future tray menu and Tauri app are
clients that attach over a local IPC channel (Unix domain socket / named
pipe), not second runtimes — two processes holding streams would
double-display every Push. The CLI's live queries (`status`) will speak
the same protocol once it exists.

## Consequences

- Single instance enforced by lockfile/lock on the IPC socket.
- MVP ships without IPC: CLI subcommands that need state read the History
  Log and config files directly; the seam is the `echo-core` interface,
  so IPC lands with the tray without a refactor.
- Workspace layout (`echo-core` + `echo-daemon`, Tauri crate slot later)
  exists to make this attachment cheap (ADR-0001's config model is
  unaffected).
