# The IPC Channel is JSON Lines over a UDS/named pipe; the daemon is the single writer

The GUI stage's local channel (ADR-0002) carries one JSON Lines
connection per client — the GUI, and the CLI's live queries. Requests
and responses carry an id; daemon-pushed events carry none. Openings are
snapshot requests; changes (a stream state flip, a History Log append)
arrive unsolicited. Since ticket #13 the status snapshot's
per-Subscription lines also carry the Subscription list's public
identity (label, server URL, token hint — the config's non-secret
fields), so the GUI's opening is the one `status` request; a separate
list snapshot never became a message type. The Push Composer's send is
one request-response (ticket #16): its context is one Subscription the
user picks — the daemon sends through that Subscription's server with
its own stored Basic Auth credentials — plus the Composer's explicit
target Device Key field; an any-server-with-temporary-credentials send
stays the `push` command's territory, so the wire never carries a
credential. The daemon is the only
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
