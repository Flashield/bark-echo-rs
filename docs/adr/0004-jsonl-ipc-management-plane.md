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

Since ticket #14 the CRUD face is its own vocabulary: `subscribe_add`
(servers, Device source — own register or mirror by Device Key — the
required label, the optional operator Basic Auth credentials) and
`subscribe_remove` (the Subscription id) are requests with typed
outcomes; a refusal is an error response whose message is the reason —
the server's own words, shown in the window and printed by the CLI. The
add answers once that stream's supervisor is up; the remove answers
once the stream is stopped, its secrets erased, and the config
persisted. A removal is also announced unsolicited as a
`subscription_removed` event so watching clients drop the line at once;
the add needs no event of its own — the new stream's `starting` flip is
that announcement, and it grows the line in place. Every apply
serializes in the daemon: file before memory, so a failed save refuses
the apply whole and the daemon's memory never disagrees with the file.

Hot-apply also reshapes the daemon's own floor: zero Subscriptions is a
healthy, actionable state — the daemon runs, serves the empty snapshot,
and CRUD fills it — not a startup refusal ("nothing to mirror"). The
GUI's spawn-if-absent starts the daemon before any Subscription exists,
and the window's add form works from there.

Lifecycle: the GUI spawns the daemon when the single-instance lock is
free; the Tray Menu's quit asks the daemon for a graceful shutdown and
then exits — a clean exit code composes with autostart supervisors
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
- The CLI's `subscribe add/rm` probes the lock: held, the change rides
  the channel and a channel that never answers is a failure, never a
  silent fall through to the direct write — racing the single writer is
  worse than failing. Lock free, the direct write stands byte for byte.
