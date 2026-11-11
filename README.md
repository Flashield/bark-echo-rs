# bark-echo-rs

The desktop Realtime Client for
[bark-server-rs](https://github.com/Flashield/bark-server-rs): holds
Subscriptions — one server URL, one Device, one Stream Token each — mirrors
every Push they receive as a desktop notification, and sends Pushes of its
own. One binary, no installer: your iPhone keeps using the official Bark App
while your Windows, macOS, and Linux machines receive the same reminders in
real time.

Everything it consumes is the server repo's client contract,
[`docs/protocol.md`](https://github.com/Flashield/bark-server-rs/blob/main/docs/protocol.md);
the shared vocabulary (Subscription, Push, Stream Token, History Log, Pull)
is [`GLOSSARY.md`](GLOSSARY.md).

## Features

- **Realtime mirroring over SSE** (`/realtime/v1`, protocol §6): the
  `: connected` handshake, keep-alive handling, per-Device message-id
  dedup, jittered exponential backoff, and `Last-Event-ID` resume on every
  disconnect — a flaky network or a server restart never loses a Push.
- **Desktop notifications from notification intents**: title / subtitle /
  body rendered natively; a `url` Ext Param opens the default browser on
  click and a `copy` Ext Param writes the clipboard — click actions are
  supported on macOS and Linux; on Windows the toast fires but the action
  is dropped until the tray stage (ADR-0002); an end-to-end-encrypted
  Push renders a readable placeholder instead of ciphertext noise.
- **Pull** (GLOSSARY): `pull` replays and lands everything the server
  still retains — for a laptop that slept through a day — into the
  History Log, silently, with replay-overlap duplicates suppressed by
  dedup.
- **History Log** (GLOSSARY): one append-only JSONL file per Subscription;
  it doubles as the reconnect cursor, and `log` tails and filters it.
- **Push from scripts**: `push` sends through the server's V2 JSON route;
  any Ext Param (`--url`, `--group`, `--sound`, …) rides through, so
  anything scriptable against the server is scriptable from the desktop.
- **Several Subscriptions per machine**: mirror your own Stream Device and
  any number of other Devices (a family iPad, …); each holds its own
  labelled Stream Token, revocable independently.
- **Secrets in the OS keyring** — macOS Keychain, Windows Credential
  Manager, Linux Secret Service — with an explicit 0600 plain-file
  fallback (`--plain-secrets`) for headless machines.
- **Single-instance lock** (ADR-0002): a second `run` refuses to start, so
  notifications never double-display; autostart samples live in
  [`docs/autostart.md`](docs/autostart.md).
- **Basic Auth per server** (protocol §1.4) sent alongside the Stream
  Token on streams, pushes, pulls, `/info` observations, and the token
  commands — laid in once with `subscribe add --auth-user U
  --auth-password P`: the password lands in the secret store, and every
  later connection rides it without flags (explicit flags still win).

## Quickstart

Download a binary from
[Releases](https://github.com/Flashield/bark-echo-rs/releases/latest)
(`bark-echo-<version>-<target>.tar.gz` / `.zip`), put `bark-echo` on your
`PATH`, and point it at a running bark-server-rs:

```sh
# 1. Register a Stream Device for this desktop, mint its labelled Stream
#    Token, persist the Subscription. Prints the Device Key.
$ bark-echo subscribe add --own --server http://192.168.1.10:8080
device key: htXq7Fm9SPTkYw3hZbLc2A
subscription e9351ee91c837988 added (label: workstation, token hint: st_31gpMA4IqFoX...)

# 2. Hold the Subscription's Realtime Stream open and render each Push as
#    a desktop notification. Stops on Ctrl-C.
$ bark-echo run
```

Then push from anywhere that reaches the server — another desktop, a
script, or the iPhone through the ordinary Bark App — and the notification
appears on this machine:

```sh
# 3. Send one Push to this desktop's Device (from any machine):
$ bark-echo push --server http://192.168.1.10:8080 \
      --device-key htXq7Fm9SPTkYw3hZbLc2A \
      --title "Stand-up" --body "in five minutes"
```

Missed time (the laptop was closed, the server restarted) is recoverable,
and the History Log answers "did it reach my desktop?":

```sh
# 4. Pull (replay) everything the server still retains, silently:
$ bark-echo pull

# 5. Audit recent reminders, or stream them live while the daemon runs:
$ bark-echo log --tail 20
$ bark-echo log --follow
```

### Command cheat sheet

| Command | Effect |
|---|---|
| `bark-echo subscribe add --own --server URL [--auth-user U --auth-password P]` | Register a fresh Stream Device for this desktop and persist the Subscription; prints the Device Key. The auth flags lay in a protected server's Basic Auth credentials |
| `bark-echo subscribe add --device-key KEY --server URL [--auth-user U --auth-password P]` | Mirror an existing Device (an iPhone, another machine) instead |
| `bark-echo subscribe list` | List Subscriptions (id, server, label, token hint) |
| `bark-echo subscribe rm ID` | Remove one Subscription and delete its secrets |
| `bark-echo run` | The daemon: hold every stream open, render notifications (Ctrl-C stops; a second run refuses on the single-instance lock) |
| `bark-echo status` | Per-Subscription stream state plus the server's live `/info` statistics |
| `bark-echo log [--tail N] [--follow] [--sub LABEL]` | Read a Subscription's History Log; `--follow` streams new lines live |
| `bark-echo pull [--cursor 0] [--sub LABEL] [--user U --password P]` | One-shot replay of retained Messages into the History Log, silently; `--cursor 0` replays everything retained |
| `bark-echo push --server URL --device-key KEY --title T [--subtitle S] [--body B] [--key value …]` | Send one Push; unknown `--key value` pairs ride through as Ext Params, exactly like the server's V2 JSON route |
| `bark-echo token list --server URL --device-key KEY [--user U --password P]` | List a Device's Stream Tokens (id, hint, label, revoked state) — never the secrets |
| `bark-echo token revoke --server URL --device-key KEY --token-id ID [--user U --password P]` | Revoke one Stream Token by its public id; takes effect immediately |
| `bark-echo --plain-secrets <command>` | Run with secrets in a 0600 plain file instead of the OS keyring |

With several Subscriptions held, `--sub LABEL` picks one on `pull` and
`log`. More Ext Params for `push`:

```sh
$ bark-echo push --server http://192.168.1.10:8080 \
      --device-key htXq7Fm9SPTkYw3hZbLc2A \
      --title "Build finished" --body "branch main is green" \
      --url "https://ci.example.org/runs/42" \
      --group "ci" --sound bell
```

## Configuration and credentials

Everything lives under one per-OS config directory:

| Platform | Config directory |
|---|---|
| Linux | `$XDG_CONFIG_HOME/echo-daemon`, else `~/.config/echo-daemon` |
| macOS | `~/Library/Application Support/echo-daemon` |
| Windows | `%APPDATA%\echo-daemon` |

Inside it:

- `config.toml` — the Subscription list (ADR-0001): per Subscription the
  server URL, label, token id, and the non-secret token/device hints, plus
  optional per-server Basic Auth entries holding the user and a
  stored-password marker only. No secrets here.
- Stream Tokens, Device Keys, and Basic Auth passwords — the OS keyring
  under the service name `bark-echo` (macOS Keychain / Windows Credential
  Manager / Linux Secret Service); Basic Auth passwords are keyed by
  server, the Subscription secrets by server + token id. With
  `--plain-secrets` they live in `secrets.toml` instead, written 0600 —
  the explicit opt-in for headless machines without a secret service.
- `history/<subscription-id>.jsonl` — the Subscription's History Log
  (GLOSSARY), appended by both the daemon and `pull`.
- `daemon.lock` — the single-instance lock (ADR-0002); the OS releases it
  when the holding process dies, so a crash never leaves a stale lock.
- `daemon-state.json` — the per-Subscription stream state `status` reads
  while the daemon runs.

On a headless Linux box without a secret service, run every command with
`--plain-secrets` — the keyring error names the fallback when it is needed.

## Development

```sh
cargo test                       # the whole suite: a real bark-server-rs
                                 # embedded in-process is the one seam
cargo fmt
cargo clippy --all-targets
```

CI runs the same three checks plus a build on Linux, macOS, and Windows on
every push and PR (the embedded server means no external services); a
`v*` tag publishes release binaries for all three platforms.

Contributors: start with [`GLOSSARY.md`](GLOSSARY.md) (the vocabulary
authority) and the ADRs in [`docs/adr/`](docs/adr/) — they explain the why
behind the Subscription-list config model (ADR-0001) and the daemon-owns-
streams runtime (ADR-0002). The client contract in the server repo's
[`docs/protocol.md`](https://github.com/Flashield/bark-server-rs/blob/main/docs/protocol.md)
is consumed, never changed.

## Attribution

bark-echo-rs is a client for the bark push ecosystem: the server contract
it speaks is [bark-server-rs](https://github.com/Flashield/bark-server-rs)'s
`docs/protocol.md`, a Rust rewrite of
[Finb/bark-server](https://github.com/Finb/bark-server) (MIT, Copyright
(c) 2018 Feng) — this project depends on that server's design and owes its
shape to it.

## License

[MIT](LICENSE) — Copyright (c) 2026 bark-echo-rs contributors.
