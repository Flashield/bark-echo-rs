# The management face is a B/S web UI served by the daemon; the egui face is dormant

The management face re-seats onto a web UI the daemon serves over HTTP:
API-first JSON endpoints with a thin vanilla-JS page as their consumer —
no node build chain, assets embedded in the binary, server-rendered
HTML. ADR-0003 (egui/eframe, on the dormant `gui-stage` branch) is
superseded; ADR-0004's IPC Channel stands unchanged as the CLI's face.

The requirements changed under ADR-0003, and that re-ranked its
constraints. Managing from a phone — and from headless boxes — is a real
requirement for this project, and no native window can serve it: the
web face must exist regardless. Once it does, a native face is a second
UI over the same state. The research re-priced both sides with the egui
implementation already built: a B/S flip discards only the ~884-line
egui render layer (22% of echo-gui) while the 1657-line IPC-client
layer, the tray, the Windows toast activation, and echo-daemon's
channel/CRUD/hot-apply survive untouched; the face costs +0.8 MB where
egui costs 18.9 MB (52% of today's 36 MB binary), so replacement halves
the binary; browser IME and accessibility are native, and egui's known
risks there (the Wayland IME regression, manual font loading, the
three-platform manual acceptance pass) die with it. axum resolves to
0.8.9 — exactly the version bark-server-rs pins — with +8 crates mostly
pre-paid by reqwest, and live updates ride the broadcast hub `run.rs`
already holds. Dual-face (egui desktop + web remote) was rejected as a
permanent double-maintenance tax; "ship egui first, web next" was
rejected because the egui branch still owed the echo-ipc Windows fix
(#26) and a five-item manual acceptance pass before it could ship at
all, and the phone need waits the whole time. Research (2026-11-15):
`.notes/2026-11-15-gui-stage-inventory.md` (branch
`research/gui-stage-inventory`) and
`docs/research/issue-21-bs-face-cost.md` (branch
`research/bs-face-cost`).

## Consequences

- The built egui face is archived, not deleted: branch `gui-stage`
  stays unmerged and dormant, tagged `egui-face-dormant` (tip `3c0344a`
  carries superseded/amended headers on ADR-0003/0004; the tag marks
  `8aade3e`, the reviewed implementation state). Its 1657-line
  IPC-client logic informs the web face's state handling; the render
  layer is dead code. If the browser flow proves worse than expected,
  the native window layer is resurrectable from the tag.
- Stack: axum 0.8 + tower + `include_bytes`-embedded assets; JSON
  endpoints are the product (the API-first baseline that keeps a future
  fleet layer — a bark-server-rs directory/credential broker or an SSE
  control plane — un-foreclosed, per the 2026-11-15 scoping); the page
  is a thin vanilla-JS consumer (~2.7 KB measured), not htmx's fragment
  contract; no templating engine for a one-page face; `/v1` prefix and
  a stable JSON error shape.
- Security posture (design pass: ticket #24): bind `127.0.0.1` by
  default with LAN bind as an explicit opt-in; a management token
  printed once at first enable, kept in the keyring, sent as a custom
  header; Host header validation on by default; Origin/Referer checks
  covering SSE (EventSource cannot send custom headers); state changes
  are POST/JSON only; fixed default port, fail-loud on squatting.
- The tray stays native (tray-icon + muda) but its host becomes a bare
  winit/tao event loop with no windows; it inherits ADR-0004's
  spawn-if-absent duty from the GUI, and "open the Management Window"
  becomes "open the management face" — the default browser at the
  face's address.
- ADR-0004 stands: the JSON Lines IPC Channel remains the CLI's face
  and the daemon stays the single writer; only the presentation layer
  changed. The web face reads daemon state in-process, so the channel's
  client count drops from two (GUI, CLI) to one (CLI).
- The echo-ipc Windows regression (never-compiled `cfg(windows)`
  transport: unstable `io::ErrorKind::Cancelled`; tokio
  `ServerOptions::open` → `create`) is independent of this decision and
  must be fixed regardless (#26) — Windows named-pipe transport is the
  CLI's substrate under either architecture.
- The egui face's five-item manual acceptance pass dies with it; the
  web face's acceptance list (responsive layout on a phone, SSE
  liveness, the token flow, tray→browser handoff) is defined by the
  revised spec (#25), which also owns the GLOSSARY revision — the
  Management Window becomes the web management face.
