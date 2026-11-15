# Research: inventory of the built gui-stage — what exists, what survives a B/S flip

Ticket #20 (wayfinder:research). Branch inventoried: `gui-stage`, tip
`8aade3e` (2026-11-15). Range vs `main`: 66 files changed, 15669
insertions, 405 deletions. Facts with pointers only; no recommendation.

Sources: spec #9, ADR-0003, ADR-0004, tickets #10–#17 (bodies + closing
comments), `git log --oneline main..gui-stage`, the branch's source and
tests, CI run 43672246549 (logs), local reproduction attempts (noted
inline).

---

## 0. Correction to the ticket's premise

Spec #9 contains **24 user stories**, not 18 (numbered 1–24 in the issue
body). All 24 are mapped below. Five milestones (T1–T8, tickets #10–#17)
were all resolved on `gui-stage`; each ticket's closing comment names its
merge commit:

| Ticket | Milestone | Merge commit |
|---|---|---|
| #10 T1 IPC foundation + CLI status | 1 | `cb3674d` (Windows-CI slice `8542b65`) |
| #11 T2 daemon events + read-only Tray | 1 | `c43e81e` |
| #12 T3 lifecycle (spawn-if-absent, Quit) | 2 | `b3b5b6f` |
| #13 T4 Management Window (read-only) | 2 | `7864654` |
| #14 T5 Subscription CRUD + hot-apply | 2 | `1857c2e` |
| #15 T6 History Log viewer | 3 | `cdc3a2f` |
| #16 T7 Push Composer | 4 | `e885306` |
| #17 T8 Windows toast click actions | 5 | `41c0ebc` |

Test inventory at tip: the #9 closing comment records "284 tests across
47 suites" (its green count at the 2026-11-15 review); a plain grep of
`#[test]`/`#[tokio::test]` functions on the working tree today counts
287 test functions across 43 `tests/` files plus in-src unit tests.

---

## 1. Spec #9's 24 user stories: implemented / partial / missing

Evidence classes: **S** = automated seam/transport test (real daemon,
real echo-ipc client, real UDS/named pipe, embedded bark-server),
**C** = automated CLI-subprocess test, **U** = pure-model unit test,
**M** = manual acceptance (per the spec's Testing Decisions the egui
paint layer has no automated tests by design).

| # | Story (abridged) | Status | Evidence |
|---|---|---|---|
| 1 | Tray shows per-Subscription stream state | Implemented (S, U; paint M) | commits `2182cc0`, `c43e81e`, `40d123c`, `0bc6204`; `crates/echo-gui/src/model.rs` unit tests (`the_opening_snapshot_paints_the_lines_and_connects`, `an_event_flips_the_visible_state_without_any_user_action`); `crates/echo-daemon/tests/ipc_seam.rs:690` `a_push_lands_as_a_history_event_and_a_server_death_as_a_state_flip`; `crates/echo-ipc/tests/events.rs` |
| 2 | Open Management Window from tray | Implemented (U, M for the click/paint) | commits `7381ec2`, `cf1719b`; `app.rs` menu dispatch; `crates/echo-gui/src/model.rs` (`MenuItem`); #13 closing: verified on real X11 + KSNI tray |
| 3 | Tray Quit stops the daemon | Implemented (S, C) | commits `84ac05b`, `fae40d3`, `9ed8eb4`, `deb71ca`; `crates/echo-gui/tests/shutdown.rs` (4 tests: confirmed / daemon-absent / refused / never-closes); `crates/echo-daemon/tests/lifecycle.rs:64` `a_launch_spawns_attaches_quits_and_takes_over` (exit 0, lock released, re-spawn takeover); `ipc_seam.rs:484` `a_shutdown_request_rides_the_same_graceful_stop_as_the_fence` |
| 4 | GUI spawn-if-absent | Implemented (C, S) | commit `289de0c`; `lifecycle.rs:64` (subprocess spawn → attach → clean exit → re-spawn; runs on all three CI platforms per #12 closing); `ipc_seam.rs:537` `a_launch_never_attempts_a_second_daemon_while_the_lock_is_held` |
| 5 | Add Subscription via form (own / mirror) | Implemented (S, U; form paint M) | commits `df508cc`, `286731a`, `2607165`; `crates/echo-gui/src/crud.rs` 7 unit tests (own/mirror/refusal/confirm); `ipc_seam.rs:820` `a_hot_own_add_starts_the_stream_and_its_pushes_reach_the_history_log`, `:928` `a_hot_mirror_add_mints_a_labelled_token_for_the_existing_device`, `:970` `a_refused_add_is_an_error_and_leaves_nothing_behind` |
| 6 | New Subscription's stream starts immediately | Implemented (S, C) | same commits; `ipc_seam.rs:820`; `crates/echo-daemon/tests/subscribe_channel_cli.rs:58` observes `active_streams` 0→2→1 through `/info` without a daemon restart |
| 7 | Remove stops stream, deletes secrets | Implemented (S, U) | `ipc_seam.rs:1040` `a_hot_remove_stops_the_stream_erases_secrets_and_updates_the_config`; commit `ffee0d6`; window model test `a_removal_event_drops_the_row_at_once` |
| 8 | Window lists label / server / token hint / state | Implemented (S, U; paint M) | commits `6466a95`, `8a3c9fc`; `crates/echo-gui/src/window.rs` tests (`the_opening_snapshot_paints_every_row_as_configured`, `rows_sort_by_label_for_a_stable_window`, …) |
| 9 | Browse History Log in window | Implemented (S, C, U) | commits `9608e51`, `851a88d`, `0cd13c0`, `1d580e2`; `ipc_seam.rs:214` `the_history_snapshot_answers_with_the_daemons_own_log_tail`, `:291` `the_viewers_initial_content_is_what_log_tail_prints` (matches a real `bark-echo log --sub … --tail 2` CLI subprocess line-for-line) |
| 10 | History Log updates live | Implemented (S, U) | #11 events; `crates/echo-gui/src/history_view.rs` (`appends_land_in_place_while_following`, `an_append_that_races_the_snapshot_shows_exactly_once_whichever_way_it_lands`); `ipc_seam.rs:690` |
| 11 | Compose and send Push from form | Implemented (S, C, U) | commits `1b44ffb`, `ba8ffaa`, `2b7326b`; `crates/echo-daemon/tests/composer.rs` (4 seam tests incl. `the_composer_send_goes_out_with_the_daemons_own_credentials`, `a_bad_device_key_surfaces_the_servers_verdict_verbatim`); `crates/echo-gui/tests/connection.rs:466` `a_composer_send_rides_the_loop_and_the_verdict_comes_back` |
| 12 | Chinese (any IME) input renders correctly | **Partial by design** | Automatable layer done: `crates/echo-gui/src/fonts.rs` (CJK startup font load, 3 tests), commits `9ceccc1`, `b90d40d` (eframe `system_fonts` feature). The red line itself — real IME input rendering on Linux/macOS/Windows — is on the manual list (ADR-0003; #16 AC; #13 noted its dev box has no CJK fonts — tofu seen, render path proven separately via the project's font loader) |
| 13 | Secrets stay inside the daemon | Implemented (S) | `crates/echo-ipc/tests/messages.rs:517-521` asserts the send request's JSON carries no credential material; #14 contract (in-form credentials pass through to be stored — #9 closing clarification); `composer.rs:79` (daemon sends with its own stored credentials) |
| 14 | Same single binary, no installer | Implemented (structural) | `bark-echo gui` subcommand (commit `a1f2e97`); one workspace binary; Linux tray via tray-icon's `ksni` feature (no GTK dep, #11 closing) |
| 15 | Quit composes with service managers | Implemented (C, contract level) | `lifecycle.rs:180` asserts the clean exit code that `Restart=on-failure` / `KeepAlive SuccessfulExit=false` require (per #12 closing); actual systemd/LaunchAgent/Startup-folder behavior is by-contract (docs/autostart.md rewritten by `a234c35`), not exercised against real supervisors in CI |
| 16 | Windows toast click actions (`url`/`copy`) | **Partial by design** | Automatable layer done: `crates/echo-daemon/tests/toast_activation.rs` — 11 cross-platform tests on the decision layer (launch-argument codec, toast XML, clipboard BOM), commits `d6aadb7`, `bf88d96`, `41c0ebc`. Windows-side linking + real-machine activation (browser opens, clipboard writes, graceful degradation) is manual (#17 closing) |
| 17 | macOS tray like menu-bar app (no Dock icon) | Implemented (code; M for visuals) | `crates/echo-gui/src/app.rs:106` sets `ActivationPolicy::Accessory` (winit macOS); main-thread tray creation per `tray.rs` module doc. No automated test possible in CI (no GUI session) |
| 18 | Absence / reconnecting shown, never stale silence | Implemented (S, U) | `LinkState` {Connecting, Connected, Reconnecting, Quitting} in `model.rs`; tests `losing_the_connection_shows_reconnecting_and_dethrones_stale_states`, `losing_the_link_names_the_absence_and_dethrones_stale_states`; `crates/echo-gui/tests/connection.rs:93` `the_loop_snapshots_hears_events_and_self_heals_after_a_daemon_restart` |
| 19 | CLI `subscribe add/rm` hot on a running daemon | Implemented (C) | commit `6a44613` + `8a801b7`; `subscribe_channel_cli.rs:58` (rides the channel while the lock is held), `:153` (a refusal is actionable on stderr, never a silent direct write) |
| 20 | `status` reports the same facts with/without GUI | Implemented (C, S) | commit `05c22ee`; `crates/echo-daemon/tests/status_cli.rs:161` `status_takes_the_live_state_over_the_channel_while_the_lock_is_held`, `:115` `status_follows_a_running_daemons_stream_state_and_shuts_down_clean` |
| 21 | CLI unchanged headless | Implemented (C) | `status_cli.rs:74` `status_without_a_daemon_reports_not_running_with_the_server_stats`, `:213`; the no-daemon direct-write path is today's byte-identical path (ADR-0004; #14 closing) |
| 22 | `bark-echo run` autostart unchanged | Implemented (C) | `crates/echo-daemon/tests/run.rs` (626 lines, pre-existing; gui-stage changes to `run.rs` are additive: event relay `0bc6204`, spawn-if-absent helper `289de0c`) |
| 23 | Daemon is the only writer | Implemented (S, C) | `Manager` single-writer (`crates/echo-daemon/src/run.rs:89`, one `start_stream()` at `:121` shared by startup and hot-add); `ipc_seam.rs` hot-apply tests assert config/secrets/snapshot consistency from outside; `subscribe_channel_cli.rs` pins no-silent-fallback |
| 24 | Message types shared from one crate | Implemented (S) | `crates/echo-ipc/src/message.rs` (428 lines) — sole wire vocabulary; consumers echo-daemon/echo-gui/CLI all import `echo_ipc`; `crates/echo-ipc/tests/` protocol 689 + messages 606 + events 300 lines |

**Tally**: 24/24 have code on the branch. 21/24 have automated,
externally observable evidence for their substance. No story is
missing. Partial-by-design (decisive acceptance is manual per the
spec's own Testing Decisions): **12** (Chinese IME render on real
desktops), **16** (Windows toast activation on a real machine), plus
**17** (macOS visuals) and the paint half of 1/2/8/9/10/11/18, which
the spec explicitly excludes from automation ("Never egui internals").
Story 15 is verified at the clean-exit-code contract, not against real
service managers.

---

## 2. echo-gui's ~4045 LOC decomposed, and what survives a B/S flip

`crates/echo-gui/src` = **4045 lines** (matches the ticket figure).
The crate's architecture doc (lib.rs, every module doc) enforces one
split: models are pure state machines, rendering is thin. The split is
visible in the files — render functions and `#[cfg(test)]` modules
start at the line numbers below (verified by grep):

| File | Total | IPC-client logic | egui render | unit tests |
|---|---|---|---|---|
| `model.rs` (TrayModel) | 480 | 188 (l.1–188) | — | 292 (l.189–480) |
| `connection.rs` (channel loop: snapshot, events, asks, reconnect) | 320 | 320 | — | — (covered by `tests/connection.rs`, 583 lines) |
| `shutdown.rs` (Quit exchange) | 121 | 121 | — | — (covered by `tests/shutdown.rs`, 140 lines) |
| `composer.rs` | 648 | 261 (l.1–261) | 102 (l.262–363) | 285 (l.364–648) |
| `crud.rs` | 606 | 242 (l.1–242) | 134 (l.243–376) | 230 (l.377–606) |
| `window.rs` | 518 | 218 (l.1–218) | 66 (l.219–284) | 234 (l.285–518) |
| `history_view.rs` | 574 | 307 (l.1–307) | 34 (l.308–341) | 233 (l.342–574) |
| `app.rs` (eframe shell: drains UiMsgs, dispatches, draws) | 456 | — | 456 | — |
| `fonts.rs` (CJK font plumbing) | 149 | — | 92 | 57 (l.93–149) |
| `tray.rs` (native tray-icon + muda) | 146 | — | 146 (native, not egui) | — |
| `lib.rs` | 27 | — | — | — |
| **Total** | **4045** | **1657 (41%)** | **884 (22%)** | **1331 (33%)** |
| plus `tray.rs` native | | | **146 (3.6%)** | |
| plus `lib.rs` | | | | **27 (0.7%)** |

Footnote: the only egui type inside a "logic" region is
`ComposerModel::state_line` returning `egui::Color32`
(`composer.rs:232-240`); every model otherwise imports only
`echo_ipc::message` and std. The models are framework-free plain Rust.

Integration tests outside src: `tests/connection.rs` 583 +
`tests/shutdown.rs` 140 = 723 lines (crate total incl. tests dir:
4768).

### What a B/S flip would discard

The egui rendering/window layer: **884 lines** (`app.rs` 456 shell,
`fonts.rs` 92, the four render functions 336) — the only code with
egui/eframe/glow dependencies. A web face re-expresses this in
HTML/JS. `fonts.rs` exists solely because egui ships no CJK glyphs
(ADR-0003) — the problem it solves does not exist in a browser.

### What survives untouched (GUI-independent, daemon-side or wire-side)

- **echo-ipc, entire crate**: src 1756 lines (`message.rs` 428 wire
  vocabulary, `transport.rs` 411 UDS/named-pipe, `client.rs` 258,
  `framing.rs` 225, `server.rs` 131, `connection.rs` 124,
  `handshake.rs` 109, `error.rs` 37, `lib.rs` 33) + tests 1595. The
  message vocabulary and framing are UI-agnostic by construction.
- **echo-daemon's channel/CRUD/hot-apply**: `run.rs` 809 (`Manager`
  single-writer at :89, `start_stream()` :121, `IpcServer::bind` :414),
  `subscribe.rs` 220 (subscribe_add/subscribe_remove apply),
  `composer.rs` 90 (SendPush execution with daemon-owned secrets),
  `status.rs` 192, `state.rs` 212, `channel.rs` 47 (CLI ride loop).
  All pre-existing daemon floors (lock, secrets, notify, history) as
  well.
- **Windows AUMID + COM toast work**: `crates/echo-daemon/src/toast_activation/`
  (`mod.rs` 163 + `windows.rs` 667) + `tests/toast_activation.rs` 227
  (11 cross-platform tests). Daemon-side, notification-layer work —
  #17's own frame — independent of any GUI framework.
- **Tray**: `tray.rs` 146 + `TrayModel` 188 are egui-free (native
  muda/tray-icon; CJK labels are the OS's business, per tray.rs doc).
  Whether a tray survives a B/S flip is an architecture decision, but
  the fact is: the tray code binds to no egui and would keep working
  beside a web face only in a process that still runs a desktop event
  loop.
- **GUI logic that is *not* bound to egui** (would need re-expressing
  or could be reused as-is server-side): the 1657 logic lines above.
  They are plain state machines over `echo_ipc::message` types; a
  browser face would re-express their presentation half, and their
  transport rules (reconnect, ask fates, dedup) would move server-side.

---

## 3. Tip CI failure: diagnosis

Run **43672246549** (workflow "CI", `.github/workflows/ci.yml`), head
`8aade3e`, 2026-11-15T03:24Z — the **only** CI run the branch has ever
had (pushed once with its full history; the workflow's concurrency
group cancels same-ref runs, and no earlier gui-stage push exists).
Jobs: ubuntu-latest **success** (132684642849), macos-latest
**failure** (132684642936), windows-latest **failure** (132684643094).

### 3a. windows-latest: real regression — echo-ipc's Windows transport does not compile

`cargo build --workspace` fails compiling the **first-party** `echo-ipc`
lib with 4 E0599 errors, all in the `#[cfg(windows)]` block of
`crates/echo-ipc/src/transport.rs`:

1. `:178` — `io::ErrorKind::Cancelled` does not exist on stable rustc.
   Reproduced locally on rustc 1.99.1 (2026-10-13): the variant is
   still unstable (`io_error_more`), so the code compiles on no stable
   toolchain.
2. `:147`, `:179`, `:189` — `ServerOptions::new().open(...)` does not
   exist. In tokio **1.54.1** (pinned by Cargo.lock) the server-side
   method is `create` (registry source
   `tokio-1.54.1/src/net/windows/named_pipe.rs:2281`); `open` is
   `ClientOptions`' method (`:2525`), correctly used at
   `transport.rs:229`.

**Breaking commit: `8028397`** ("echo-ipc: the channel over the real
transport — UDS beside the lock, named pipe on Windows", 2026-11-15
00:27 +0800). `git log -S` shows both strings enter and are touched by
no other commit. Because the code is `cfg(windows)` inside a
Linux-developed crate, it was never compiled on Windows until this run:
#17's `x86_64-pc-windows-msvc` type-check covered echo-daemon's
toast_activation only, and #10's Windows slice (`8542b65`) had never
actually executed when claimed (first execution = this run).

**Classification: real regression** (deterministic compile defect in
first-party code, not environment). It **exceeds the documented
limitation**: the ci.yml header excuses only the *dev-dependency*
bark-server-rs (upstream Flashield/bark-server-rs#13) from Windows; the
failing code is the first-party lib that plain `cargo build --workspace`
must compile — the header's own premise ("the released Windows binary
is unaffected") fails at tip. It also breaks the `cargo test -p
echo-ipc` step #10's closing promised for Windows (never ran green).

### 3b. macos-latest: one seam test failed — evidence leans environment/timing flake; cannot be fully attributed

Failure: `crates/echo-daemon/tests/basic_auth_store.rs:247`
`the_daemons_stream_and_status_ride_the_stored_credentials` panicked at
`:274` — "the stream never connected on the stored credentials":
`/info` reported `streams:1, active_streams:0` when the test's 10 s
deadline expired (the binary finished in 10.27 s, i.e. the full budget
burned).

Facts gathered:

- The test file is **untouched on gui-stage** (`git log
  main..gui-stage -- crates/echo-daemon/tests/basic_auth_store.rs` is
  empty); it was **green on macOS with main code** (run 43604041873,
  2026-11-14, commit 4c81666).
- The same suite **passes on ubuntu** in the very same failing run —
  identical daemon logic.
- Local reproduction (Linux, this repo): 3/3 green, each full-binary
  run ~1.5 s against the 10 s deadline.
- gui-stage did refactor the daemon's stream start path (`a5b1a78`
  dynamic supervisor set, `286731a` hot-apply, `0bc6204` event relay,
  `8a801b7` shared ride loop), but inspection finds no macOS-specific
  difference (the connect path — loopback to the embedded server — is
  unchanged; `0bc6204`'s echo-core change is an additive appends
  channel).
- CI history cannot attribute a commit: there is exactly one gui-stage
  run. The Nov 12 integration-branch macOS failures (runs
  43268486490/43269888674/43273250597/43278221643/43279986036) checked
  out as deterministic dev-iteration errors (a clippy `-D warnings`
  lint, mid-development test failures) — **not** flake precedent.

**Classification**: evidence leans **environment flake** (runner
timing; untouched test; ubuntu-green on identical code; 15× headroom
locally), but a deterministic macOS-only regression introduced by the
gui-stage daemon refactor cannot be excluded without a macOS run on an
earlier gui-stage commit — no such run exists. It does **not** match
the documented limitation: that limitation (ci.yml header, #10 closing)
excuses *Windows* from bark-server-dependent seam tests; this test is
supposed to run on macOS, and did so on main the day before.

---

## 4. Outstanding manual-acceptance list (as it stands today)

Recorded in #9's closing comment (2026-11-15T01:53Z) as the
"three-platform first-week pass per ADR-0003", with per-ticket entries
folded in:

1. **Chinese IME into the Push Composer** renders correctly on all
   three platforms (ADR-0003 red line; #16 AC; #13's note: its dev box
   had no CJK fonts — on-device label rendering confirmed there too;
   Linux Wayland IME is a known egui regression, egui#5544 — dev boxes
   run X11/XWayland).
2. **glow's GL fallback on old GPUs/VMs/remote desktops** (ADR-0003;
   #16 AC).
3. **Tray visuals** on real desktops (#9 closing; covers #11's
   macOS no-Dock-icon Accessory behavior and main-thread tray clicks,
   and #13's window/tray appearance — all untestable in CI).
4. **GUI-crash-survives-daemon** — the GUI parent must really die and
   the daemon keep mirroring (#12 closing: "parent must really die";
   the subprocess lifecycle test covers spawn/attach/exit, not a real
   crash/kill of the parent).
5. **Windows toast activation on a real machine** — clicking a toast
   with `url` opens the browser, `copy` writes the clipboard, and
   AUMID/COM registration degrades gracefully where it fails (#17 AC
   + closing: linking and real-machine behavior guarded by Windows CI
   build only).

Nothing else is recorded as pending manual acceptance on #10–#17 or
#9.

---

## Appendix: branch composition

- New crates on gui-stage: `echo-ipc` (3351 lines incl. tests; src
  1756) and `echo-gui` (4768 incl. tests; src 4045).
- echo-daemon grew the channel face: `channel.rs` 47, `run.rs` 809
  (Manager + serve loop), `subscribe.rs` 220, `composer.rs` 90,
  `toast_activation/` 830 (163 mod + 667 windows).
- Autostart doc rewritten to the hot-apply reality (`a234c35`); ADR-0004
  extended three times (`f865d8e`, `7864654`, `fdf21fa`).
- Key doc commits: ADR pair `56208ab`; glossary terms in the same
  commit range (GLOSSARY.md).
