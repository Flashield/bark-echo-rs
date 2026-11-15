# Autostart samples

Without an installer (spec Out of Scope: no installers, no code signing,
no auto-update — a documented sample per platform is the whole story),
these samples make bark-echo survive reboots on each of the three
desktop platforms. What to autostart depends on the setup:

- **A desktop session** autostarts the Tray Menu's host — `bark-echo
  tray` (ADR-0005). The host is the desktop presence: a tray icon per
  Subscription's stream state, Manage opening the management face in
  the browser, Quit stopping everything. It inherits spawn-if-absent
  from the old GUI (ADR-0004): when the daemon is not running, the host
  spawns a detached `bark-echo run` beside the same binary before it
  serves — one launch covers both processes, so the autostart entry
  names `tray`, and the daemon never needs an entry of its own.
- **A headless box** autostarts the daemon directly — `bark-echo run`,
  exactly as before this stage. There is no tray on a headless machine;
  the daemon is the whole story, and the management face it serves is
  reachable over an SSH tunnel, a tailnet, or an explicit LAN bind
  (README: Management face).

All samples rely on the same facts about the daemon:

- **The single-instance lock makes double starts harmless** (ADR-0002):
  a second `bark-echo run` reports
  `another bark-echo instance is already running (lock held at …)` and
  exits non-zero — it never double-displays a Push. You can start the
  daemon by hand while an autostart entry exists, run the tray host
  beside a service-managed daemon (it probes the lock, finds it held,
  and attaches instead of spawning), or leave a stale unit enabled; the
  loser just exits.
- **The lock dies with the process.** The OS releases it on exit — crash
  and `kill -9` included — so there are no stale locks to clean up and an
  automatic restart by the service manager always succeeds.
- **The daemon rides out server restarts itself** (reconnect with jittered
  backoff and `Last-Event-ID` resume), so the service supervisor only
  needs to cover process crashes; a bark-server-rs maintenance window is
  invisible.
- Configuration changes (ADR-0001's Subscription list) apply hot while
  a daemon runs: `subscribe add` / `subscribe rm` ride the IPC Channel
  and take effect immediately (ADR-0004) — no service restart. Only the
  headless direct write (no daemon up, the CLI writing the files
  directly) takes effect at the daemon's next start.

## Quit composes with the service manager

The daemon's graceful stop — the ADR-0004 Shutdown exchange, or ctrl-C
on a foreground run — exits with code 0, and that is the composition
contract with every sample below: `Restart=on-failure` and
`KeepAlive SuccessfulExit=false` leave a clean stop alone, so nothing
resurrects the daemon behind Quit's back.

The Tray Menu's Quit does the whole exchange (ADR-0004, resurrected
intact): it asks the daemon for a graceful shutdown over the IPC
Channel, waits up to 10 s for the daemon's confirmation — the channel
closing underneath the answer — and then exits itself. A daemon already
gone reports absent; one silent past the budget is reported
unconfirmed while the host quits anyway: the user's Quit is not
negotiable, and a stuck daemon never holds the tray hostage.

Two boundaries worth naming:

- **Quit stops the processes, not the autostart.** The entry (unit,
  agent, shortcut) survives Quit untouched — the next login or manual
  `bark-echo tray` brings the pair back up.
- **The service manager's own stop is not the Quit exchange.**
  `systemctl --user stop` / `launchctl bootout` terminate their unit's
  processes directly (the tray host and, on Linux, the whole cgroup
  with it). That is safe — the lock dies with the process and the
  state file is a crash-recovery record by design — but the fenced,
  confirm-first stop is Quit's or ctrl-C's job. Stopping a service is
  still the right way to take a headless daemon down for maintenance;
  the streams simply re-establish with resume on the next start.

## Linux: systemd user unit

Save as `~/.config/systemd/user/bark-echo.service` (adjust the binary
path — the release archive's `bark-echo` wherever you put it):

```ini
[Unit]
Description=bark-echo — desktop Realtime Client for bark-server-rs

[Service]
# No network ordering needed: the daemon reconnects with jittered
# backoff, so starting before the network is up is harmless.
# Desktop: the tray host — it brings the daemon up when absent.
ExecStart=%h/.local/bin/bark-echo tray
# Headless box instead? Autostart the daemon directly and drop the
# tray (there is no tray to host without a graphical session):
# ExecStart=%h/.local/bin/bark-echo run
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

The tray icon needs a desktop session with a StatusNotifier host —
every mainstream desktop environment provides one. Follow the daemon's
output with `journalctl --user -u bark-echo -f`; the History Log
(`bark-echo log --follow`) shows what actually landed. Note that
desktop notifications need a notification daemon on the user's D-Bus
session — on a headless server the daemon still records every Push in
the History Log, it just has nowhere to display them.

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
    <string>tray</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <!-- Restart on non-zero exit (crash, lock refused) but never on a
       clean stop; the daemon manages its own reconnects, and Quit's
       graceful stop exits 0. -->
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

The tray host runs as a menu-bar-only app (the Accessory activation
policy): the icon lives in the menu bar and no Dock icon appears. For
a headless Mac, swap `tray` for `run` in `ProgramArguments` — the
daemon autostarts exactly as before.

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
start "bark-echo" /min "%LOCALAPPDATA%\Programs\bark-echo\bark-echo.exe" tray
```

`start /min` keeps the host's console window minimized while the tray
icon lives in the notification area. Starting it twice — a double-click
of the `.cmd` while the host is already up, or a manual `bark-echo
tray` — is safe: the daemon's single-instance lock absorbs the second
attempt (a second host finds the lock held and attaches; a spawned
daemon that loses the lock reports the refusal and exits). A headless
Windows box swaps `tray` for `run`; Task Scheduler with an "at log on"
trigger running the same command is an equivalent alternative.

## Verifying

On any platform: `bark-echo status` answers whether the daemon is running
(the lock is held), how each Subscription's stream is doing, and what the
server's `/info` reports — after a reboot it should say
`daemon: running` with every stream `running`. On a desktop, the tray
icon is the at-a-glance version of the same answer; Manage opens the
management face (`http://127.0.0.1:8081` by default) where the face
itself confirms the daemon is serving.
