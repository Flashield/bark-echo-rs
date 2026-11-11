# Autostart samples

The daemon (`bark-echo run`) is what holds your Subscriptions' Realtime
Streams open and renders desktop notifications. Without an installer, these
samples make it survive reboots on each of the three desktop platforms
(spec Out of Scope: no installers, no code signing, no auto-update — a
documented sample per platform is the whole story).

All three samples rely on the same facts about the daemon:

- **The single-instance lock makes double starts harmless** (ADR-0002):
  a second `bark-echo run` reports
  `another bark-echo instance is already running (lock held at …)` and
  exits non-zero — it never double-displays a Push. You can start the
  daemon by hand while the service runs, re-run an installer script, or
  leave a stale unit enabled; the loser just exits.
- **The lock dies with the process.** The OS releases it on exit — crash
  and `kill -9` included — so there are no stale locks to clean up and an
  automatic restart by the service manager always succeeds.
- **The daemon rides out server restarts itself** (reconnect with jittered
  backoff and `Last-Event-ID` resume), so the service supervisor only
  needs to cover process crashes; a bark-server-rs maintenance window is
  invisible.
- Configuration changes (ADR-0001's Subscription list) are read at
  startup: restart the service after `subscribe add` / `subscribe rm`.

## Linux: systemd user unit

Save as `~/.config/systemd/user/bark-echo.service` (adjust the binary
path — the release archive's `bark-echo` wherever you put it):

```ini
[Unit]
Description=bark-echo — desktop Realtime Client for bark-server-rs

[Service]
# No network ordering needed: the daemon reconnects with jittered
# backoff, so starting before the network is up is harmless.
ExecStart=%h/.local/bin/bark-echo run
# On a headless machine without a secret service, run with the plain
# 0600-secrets fallback instead:
# ExecStart=%h/.local/bin/bark-echo --plain-secrets run
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
```

Enable and start:

```sh
systemctl --user daemon-reload
systemctl --user enable --now bark-echo.service
# For a start at boot without an active login session (headless boxes):
sudo loginctl enable-linger "$USER"
```

Follow the daemon's output with `journalctl --user -u bark-echo -f`; the
History Log (`bark-echo log --follow`) shows what actually landed. Note
that desktop notifications need a notification daemon on the user's D-Bus
session — on a headless server the daemon still records every Push in the
History Log, it just has nowhere to display them.

## macOS: LaunchAgent

Save as
`~/Library/LaunchAgents/com.bark-echo-rs.bark-echo.plist` (adjust the
binary path):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.bark-echo-rs.bark-echo</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/bin/bark-echo</string>
    <string>run</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <!-- Restart on non-zero exit (crash, lock refused) but never on a
       clean stop; the daemon manages its own reconnects. -->
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ThrottleInterval</key>
  <integer>5</integer>
  <key>StandardOutPath</key>
  <string>/tmp/bark-echo.log</string>
  <key>StandardErrorPath</key>
  <string>/tmp/bark-echo.err.log</string>
</dict>
</plist>
```

Load it (runs now and at every login):

```sh
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.bark-echo-rs.bark-echo.plist
# Older macOS: launchctl load ~/Library/LaunchAgents/com.bark-echo-rs.bark-echo.plist
```

Stop and unload with `launchctl bootout gui/$(id -u)/com.bark-echo-rs.bark-echo`
(or `launchctl unload …`).

## Windows: Startup folder

Drop a `bark-echo.cmd` into your Startup folder — open it with
<kbd>Win</kbd>+<kbd>R</kbd> → `shell:startup`
(usually `%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup`;
adjust the binary path):

```bat
@echo off
start "bark-echo" /min "%LOCALAPPDATA%\Programs\bark-echo\bark-echo.exe" run
```

`start /min` keeps the console window minimized while the daemon runs.
Starting it twice — a double-click of the `.cmd` while the daemon is
already up, or a manual `bark-echo run` — is safe: the second instance
reports the lock error and exits immediately. Task Scheduler with an
"at log on" trigger running the same command is an equivalent
alternative.

## Verifying

On any platform: `bark-echo status` answers whether the daemon is running
(the lock is held), how each Subscription's stream is doing, and what the
server's `/info` reports — after a reboot it should say
`daemon: running` with every stream `running`.
