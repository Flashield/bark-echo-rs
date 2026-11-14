# The IPC Channel is JSON Lines over a UDS/named pipe; the daemon is the single writer

The GUI stage's local channel (ADR-0002) carries one JSON Lines
connection per client — the GUI, and the CLI's live queries. Requests
and responses carry an id; daemon-pushed events carry none. Openings are
snapshot requests; changes (a stream state flip, a History Log append)
arrive unsolicited. Since ticket #13 the status snapshot's
per-Subscription lines also carry the Subscription list's public
identity (label, server URL, token hint — the config's non-secret
fields), so the GUI's opening is the one `status` request; a separate
list snapshot never became a message type. The daemon is the only
writer of bark-echo state:
while it runs, `subscribe add/rm` from the GUI and the CLI ride the IPC
Channel and take effect immediately (it mutates the Subscription list,
its secrets, and starts/stops that stream's supervisor); with no daemon
running, the CLI keeps writing the files directly. The GUI touches no
bark-echo files or secrets — status, CRUD, History Log tailing
(`log --follow` GUI-ified), and Push Composer sends all ride the
channel. HTTP and gRPC were rejected: a same-user local channel does not
justify a server stack or codegen in the daemon, and localhost TCP
introduces port squatting where UDS file permissions / named pipe ACLs
are already the access control.

Lifecycle: the GUI spawns the daemon when the single-instance lock is
idle — one probe, then a detached `bark-echo run` beside the same
binary (its own process group on Unix;
`DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP` on Windows) — so the
child outlives the GUI: a crash or kill leaves the daemon mirroring.
When the lock is held, the GUI attaches and never attempts a second
daemon; the probe-then-spawn gap is arbitrated by the child's own lock
take, and a loser exits with the standard lock refusal. The Tray Menu's
quit sends the channel's `shutdown` request, which
the daemon answers (`shutting_down`) before it begins stopping — the
request rides the same graceful stop as ctrl-C (streams fenced, state
persisted, lock released, clean exit code), and the daemon closes every
connection as the stop proceeds: that close is the confirmation the GUI
waits for, under a bounded budget after which it exits anyway and
reports the daemon unconfirmed — a stuck daemon must not hold the tray
hostage. A clean exit code composes with autostart supervisors
(`Restart=on-failure`, `KeepAlive SuccessfulExit=false` do not resurrect
a clean stop). Headless autostart keeps running `bark-echo run`
directly.

## Consequences

- New crates: `echo-ipc` (serde-only message types shared by daemon,
  GUI, CLI) and `echo-gui` (egui/eframe per ADR-0003).
- Subscription changes no longer need a daemon restart — autostart.md's
  "restart the service after `subscribe add`" note dies.
- Secrets never enter the GUI process; connection policy stays
  single-sourced in the daemon.
