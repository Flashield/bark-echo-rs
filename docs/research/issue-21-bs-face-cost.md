# Research #21 — what a B/S management face costs this binary

Research ticket: [Flashield/bark-echo-rs#21](https://github.com/Flashield/bark-echo-rs/issues/21)
(label `wayfinder:research`). Measured 2026-11-15 on the scratch worktree
`research/bs-face-cost` (branch off `gui-stage` @ `8aade3e`). Facts only —
no recommendation is made anywhere in this document.

**Method summary**: every number below is a real `cargo build --release
-p echo-daemon` in the scratch worktree, measured with `stat -c %s` on
`target/release/bark-echo` ("raw"), then again after `strip --strip-all`
on a copy ("stripped"). Scratch edits (a hidden `face` subcommand, two
face modules, three embedded assets) live only in the worktree working
tree — nothing but this document is committed. Toolchain: cargo/rustc
1.99.1 (2026-08-27/2026-10-13), Linux 6.12 x86-64, 8 cores, edition
2024. The repo defines **no `[profile.release]`**, so "default" means
cargo's release defaults (opt-level 3, lto off, codegen-units 16).

## 1. Binary-size delta (measured)

### 1.1 Variant matrix

The product binary is `bark-echo` (echo-daemon). The egui GUI is **not a
separate binary** — echo-gui is a library linked into `bark-echo` via the
`gui` subcommand, so "the egui binary" is the daemon binary itself. All
sizes in bytes.

| id | variant | raw | stripped | Δ raw vs V1 |
|---|---|---:|---:|---:|
| V0 | gui-stage HEAD as-is (egui, no HTTP face) | 36,238,424 | 26,890,712 | — |
| V1 | V0 with echo-gui removed (daemon without egui) | 17,321,176 | 13,042,768 | 0 |
| V2 | V1 + axum 0.8.9 default features + futures-util + face module + 6,164 B embedded assets | 18,158,248 | 13,625,168 | +837,072 |
| V5 | V1 + axum 0.8.9 minimal features (`http1`,`json`,`tokio`,`query`) + same face | 18,019,784 | 13,517,272 | +698,608 |
| V4 | V1 + hyper 1.12 only (`http1`,`server`), hand-rolled router, same face | 17,698,696 | 13,313,488 | +377,520 |
| V5h | V5 with htmx.min.js (51,250 B) in place of app.js (2,730 B) | 18,068,904 | 13,566,424 | +747,728 |
| V3 | V0 + the axum face (both faces in one binary; assets 6,265 B) | 37,159,568 | 27,535,960 | — |
| V0t | HEAD config + tuned profile (lto=fat, codegen-units=1, strip=symbols) | 20,561,832 | 20,561,824 | — |
| V2t | V2 config + tuned profile (assets 6,265 B) | 10,301,584 | 10,301,584 | — |

Notes on the tuned rows: cargo's `strip = "symbols"` did the work — a
follow-up `strip --strip-all` removed only 8 B from V0t and 0 B from
V2t. A first V2t build ran with echo-gui still declared (but
unreferenced — the `Gui` arm was gone): 10,303,248 B, within 1,664 B of
the properly-removed build, confirming that an unreferenced dependency
contributes no linked code. With lto=fat, the egui binary drops 36.2 →
20.6 MB and the B/S-face daemon 18.2 → 10.3 MB — the replacement story
(egui → B/S face, −49.9 %) holds at both profile settings.

### 1.2 Derived facts (same measurements, arithmetic only)

- **The egui stack costs 18,917,248 B raw (52.2 % of the binary) / 13,847,944 B stripped.** V0 − V1.
- **The full axum B/S face costs +837,072 B raw (+582,400 stripped) — 2.3 % of the egui binary's raw size, 4.8 % of the no-gui daemon's.** V2 − V1. egui costs ~22.6× what the face costs.
- **Replacing egui with the B/S face: 36,238,424 → 18,158,248 raw (−49.9 %) / 26,890,712 → 13,625,168 stripped (−49.3 %).**
- **axum's premium over a hand-rolled hyper router: +459,552 B raw (V2 − V4, default features) or +321,088 B raw (V5 − V4, minimal features).** The hand-rolled variant also costs more code: 91 lines (axum face) vs 172 lines (hyper face, incl. the Host check and a hand-written SSE body) — see §2.4.
- **axum default features vs minimal features: 138,464 B raw saved** by turning off `form`, `matched-path`, `original-uri`, `tower-log`, `tracing` (V2 − V5). SSE needs no feature flag (see §2.5).
- **Embedded assets ride at byte cost.** Swapping the 2,730 B app.js for the 51,250 B htmx.min.js moved the binary by +49,120 B raw (expected +48,520 from the byte delta; the ~600 B residue is section padding/alignment). V5h − V5.
- **Package-graph cost is small because the daemon already ships an HTTP stack.** The daemon's prod tree (echo-core → reqwest 0.13.5) already contains hyper 1.12.0, hyper-util 0.1.21, http 1.5.0, http-body(-util), httparse, tower 0.5.3, tower-layer, tower-service, tower-http, and futures-util 0.3.34. Adding the axum face added exactly **8 new crates** (axum, axum-core, httpdate, matchit, mime, ryu, serde_path_to_error, serde_urlencoded; unique-package count 201 → 209). The hyper-only variant added **1** (httpdate; 201 → 202). `Cargo.lock` total package count was unchanged (607 → 607) because the axum tree was already resolved for the test seam's dev-dependencies (`bark-server-rs` pulls axum 0.8).
- **Version unification with the server is exact**: axum resolves to **0.8.9** in this workspace and bark-server-rs pins `axum = "0.8.9"` in its own `Cargo.toml`; tower 0.5.3, hyper 1.12.0 and tokio 1.54.1 also match the server's pins. Adding the face forks no version anywhere.
- **Non-additivity caveat**: V3 − V0 = +921,144 raw is ~10 % larger than V2 − V1 = +837,072 for the same face code (assets differ by 101 B). Codegen-unit partitioning and feature-unification shifts make per-variant numbers the primary data; cross-subtractions carry layout noise.
- Tuned profile pair (lto=fat, codegen-units=1, strip=symbols) — see V0t/V2t in §1.1 for the "shipped" counterfactual: **20,561,832 → 10,301,584 B (−49.9 %)**, the same proportional gap as at default settings (V0t − V2t = 10,260,248 B).

### 1.3 What was measured (scratch build description)

All scratch code sits in the echo-daemon crate of the worktree, wired
through a hidden subcommand so the face is genuinely linked and
exercised, not dead-stripped:

- `src/face.rs` (91 lines): axum 0.8 router — `GET /` (embedded HTML),
  `GET /assets/app.js`, `GET /assets/style.css` (include_bytes, manual
  CONTENT_TYPE), `GET /api/v1/status` (serde_json), `GET /api/v1/events`
  (`axum::response::sse::Sse` over `tokio::sync::broadcast`, with
  `KeepAlive::default()`).
- `src/face_hyper.rs` (172 lines): the same five endpoints on bare hyper
  1.x — `http1::Builder::serve_connection` + `service_fn` +
  `hyper_util::rt::TokioIo`, manual routing on `(method, path)`, manual
  content-type headers, a hand-written `http_body::Body` impl for SSE
  (tokio's `broadcast::Receiver` has **no public poll method usable in a
  Body** — see §2.5 — so each SSE connection spawns a forwarding task
  into an `mpsc::Receiver`, whose `poll_recv` is public), and a
  hardcoded localhost Host check (returns 403 on a foreign Host).
- `src/face_assets/`: `index.html` (1,718 B), `app.js` (2,730 B vanilla
  JS; swapped to htmx.min.js 51,250 B for V5h), `style.css` (1,716 B) —
  hand-authored, no build chain, embedded with `include_bytes!`.
- `Cargo.toml` deltas per variant as listed in §1.1; tokio features were
  NOT touched (the workspace's existing `sync`,`net`,`macros`,… set
  already covers broadcast + TcpListener).

Smoke tests run against the built binary: JSON endpoint returns the
versioned envelope; HTML serves with `content-length` exactly equal to
the embedded file's byte size (1,718); SSE endpoint opens and stays
open; the hand-rolled Host check answered 403 to a request with
`Host: evil.example.com`.

## 2. Stack options for server-rendered + live updates

### 2.1 htmx vs vanilla JS + JSON endpoints (measured asset sizes)

- **htmx 2.0.8** (cdnjs.cloudflare.com copy, downloaded 2026-11-15):
  **51,250 B raw / 16,536 B gzip -9**; license BSD-2-Clause (cdnjs
  library listing). It is a single JS file — no node build chain needed.
  Contract note (fact from htmx's model): htmx swaps **HTML fragments**
  into the DOM (`hx-swap innerHTML` default) — server responses it
  consumes are markup, not JSON. Using it does not remove the need for
  JSON endpoints for a fleet layer; it adds HTML-fragment endpoint
  shapes beside them.
- **Vanilla JS + JSON**: the scratch face's complete client (fetch JSON
  from `/api/v1/*` + EventSource to `/api/v1/events` + DOM rendering of
  a status section, a subscription table and a capped 200-line live
  history log) is **2,730 B** of one hand-written file with zero
  dependencies. The JSON it consumes is exactly the API-first endpoint
  set (§4), so the same endpoints serve both the browser face and
  non-browser clients.

### 2.2 Templating: compile-time (askama/rinja) vs hand-rolled strings

- rinja began as a fork of dormant Askama and the projects **merged
  back** under the askama-rs org (askama.rs ships an "Upgrading" guide;
  `rinja_*` integration crates were removed). Askama/rinja derive
  `#[derive(Template)]` and compile templates to Rust at build time —
  type-checked, **no runtime template parsing**, opt-in features to cut
  dependencies (askama.rs "Opt-in features").
- The scratch face needed **no templating at all**: one page is served
  verbatim from an embedded file, and dynamic rows are rendered
  client-side from JSON. Hand-rolled strings/`format!` would enter with
  server-side fragment rendering (the htmx shape); askama/rinja enters
  at the same point, moving the fragment templates into compile-checked
  files. No measurement was made of askama's binary delta (not built in
  the scratch matrix).

### 2.3 Static embedding

- **`include_bytes!`**: zero dependencies, per-file; MIME types must be
  written by hand (the scratch face does; `mime_guess` is the usual
  helper). This is what the measurement used.
- **rust-embed 8.13.0** (crates.io): embeds a folder via
  `#[derive(RustEmbed)]`, lists/retrieves files at runtime, debug builds
  load from the filesystem for iteration, release builds embed. Helper
  methods for MIME/hashes depending on features. `rust-embed-for-web`
  (fork) adds pre-compressed gzip/br and ETag/cache headers. For a
  handful of files include_bytes is sufficient; a folder of many assets
  is the case rust-embed addresses.

### 2.4 axum vs minimal hyper (measured in §1.2)

- axum 0.8.9 (default features) face: **+837,072 B raw** over V1; hand
  code 91 lines.
- hyper 1.12 (`http1`,`server`) hand-rolled face: **+377,520 B raw**
  over V1; hand code 172 lines; +1 crate (httpdate).
- What axum buys for the extra ~0.3–0.5 MB raw: matchit routing, typed
  extractors (`State`, `Json`, `Query`), `Sse`/`Event`/`KeepAlive`
  abstractions, graceful `axum::serve` loop, tower middleware
  composability (auth/Host-check as middleware instead of inline
  checks). What hyper-only buys: smallest delta, no tower layers to
  learn, but every cross-cutting concern is hand-written (the scratch
  hyper face hand-wrote the Host check; the axum face would express it
  as middleware).
- **tower is already in the tree**: `tower` 0.5.3, `tower-layer`,
  `tower-service`, `tower-http` arrive with reqwest — adding an explicit
  `tower` dependency adds no new package. `tower-http`'s middleware
  (e.g. compression, trace) are per-feature opt-ins.
- In-family precedent: bark-server-rs is an axum 0.8.9 + tower 0.5.3 +
  hyper 1.12.0 app; the daemon consuming its `/realtime/v1` SSE streams
  today would serve the same wire shapes with the same stack versions.

### 2.5 Serving SSE — what is actually needed (verified)

- **axum**: `axum::response::sse::{Sse, Event, KeepAlive}` is **not
  feature-gated** — it ships in core axum 0.8 (docs.rs feature list:
  the 9 default features are `form`, `http1`, `json`, `matched-path`,
  `original-uri`, `query`, `tokio`, `tower-log`, `tracing`; opt-ins are
  `http2`, `macros`, `multipart`, `ws`). `Sse::new(stream).keep_alive(KeepAlive::default())`
  over a `tokio::sync::broadcast::Receiver` converted with
  `futures_util::stream::unfold` is the exact pattern bark-server-rs's
  `src/api/sse.rs` uses for the streams this daemon already consumes.
  `futures-util` is already in the prod tree via reqwest (no new
  crate), and `tokio/sync` (broadcast) is already a workspace feature.
- **tokio fact verified against source**: tokio 1.54.1's
  `broadcast::Receiver` exposes **no `poll_recv`** — grep of the
  vendored source finds none — so a hand-rolled hyper SSE body cannot
  poll the bus directly; `mpsc::Receiver::poll_recv` IS public, hence
  the forwarding-task shape in `face_hyper.rs` (one extra task per SSE
  connection, or a `tokio-stream`/`BroadcastStream` dependency).
- **Client side interacts with token auth**: the browser `EventSource`
  API **cannot set custom headers** (MDN documents only `withCredentials`
  for credentials). So a header-token face serves SSE to its own page
  via (a) same-origin + Origin/Host check (no auth header needed —
  EventSource sends no cross-site credentials unless cookies are used),
  (b) a short-lived ticket query parameter, or (c) a fetch-based reader
  (`@microsoft/fetch-event-source`, `extended-eventsource` polyfills)
  that can set headers. Cookie-based auth for SSE would reintroduce the
  CSRF surface that header-token auth avoids.

## 3. Localhost admin-face security precedents (CSRF / DNS-rebinding)

### 3.1 The patterns, each with precedent citations

1. **Token in a custom header (no cookies at all).** Syncthing's REST
   API: `X-API-Key` header (docs.syncthing.net/dev/rest.html); HashiCorp
   Vault: `X-Vault-Token` (also `Authorization`); Consul: `X-Consul-Token`
   (or Bearer); Nomad: `X-Nomad-Token` (or Bearer); Transmission:
   `X-Transmission-Session-Id`. Mechanism per OWASP CSRF Prevention
   Cheat Sheet: a custom header requirement is "a user-friendly defense
   that is particularly well suited for AJAX or API endpoints… No token
   is needed for this approach" — because browsers cannot send custom
   headers in cross-site "simple" requests, the request is forced into a
   CORS preflight. A no-cookie design also makes SameSite moot and
   cannot be CSRF'd through ambient credentials. (Cross-origin *reads*
   of responses are still CORS-gated; the threat left is DNS rebinding +
   CSRF via simple requests, addressed by 2.)
2. **Host header validation (DNS-rebinding defense).** Transmission
   ships it default-on: `rpc_host_whitelist_enabled` default true, the
   server strips the port and matches `Host` against a whitelist where
   "`localhost`, `localhost.` and any IP address are always implicitly
   allowed" (docs/rpc-spec.md §2.2.2). qBittorrent WebUI ships "Enable
   Host header validation" default-on alongside its CSRF, clickjacking
   and cookie-Secure toggles (WebUI API docs document all four keys —
   `web_ui_host_header_validation_enabled`, `web_ui_csrf_protection_enabled`,
   `web_ui_clickjacking_protection_enabled`, `web_ui_secure_cookie_enabled` —
   and the shipped example configuration shows all four `true`). The
   canonical defense statement is
   Jackson et al., *Protecting Browsers from DNS Rebinding Attacks*
   (CCS 2007 / TISSEC 2009): "Individual servers can defend themselves
   against DNS rebinding attacks by validating the HTTP Host header and
   rejecting requests that contain an unexpected value" — the attacking
   page cannot forge the Host header. The scratch face implements this
   (403 on foreign Host, smoke-tested).
3. **Bind localhost by default; opt-in LAN.** Syncthing binds
   `127.0.0.1:8384` by default (docs/forum; binding to localhost is
   described as the security default). Docker binds the daemon to a
   **unix socket** by default and documents that plain TCP
   (`tcp://0.0.0.0:2375`) is unauthenticated and root-equivalent
   (docs.docker.com "Protect the Docker daemon socket"). Counter-precedents
   (they bind all interfaces by default and lean on the operator):
   Prometheus (0.0.0.0:9090; its security model page states components
   provide no server-side auth/encryption by default and assume a
   trusted network), and the Rust single-binary file servers miniserve
   and dufs (both default `0.0.0.0`, both offer `-b 127.0.0.1` and
   auth flags). The ticket's "localhost default, opt-in LAN bind" matches
   the Syncthing/Docker end of the precedent spectrum.
4. **SameSite cookies (defense-in-depth, only if cookies exist).** OWASP
   CSRF cheat sheet: `SameSite=Lax` "balances security and usability";
   it "only blocks unsafe methods", is scoped to the registrable
   domain, and browser defaults differ (Chromium defaults Lax; Firefox
   and Safari do not). With a header-token design (pattern 1) no session
   cookie exists, so this layer is simply absent — the precedents in 1
   (Vault/Consul/Nomad/Syncthing/Transmission) all authenticate by
   header, not cookie, for their API surfaces.
5. **Origin/Referer verification for browser requests.** OWASP's
   "Verifying Origin with Standard Headers": `Origin`/`Referer` are
   forbidden headers only browsers can set; compare source vs target
   origin; treat missing headers and `Origin: null` as *not* same-origin;
   beware suffix spoofing (`example.org.attacker.com`). This is the
   layer that covers SSE specifically (see §2.5): same-origin
   EventSource passes an Origin check; a rebound-host page is rejected
   by the Host check of pattern 2.
6. **Browser-side guardrails (context, not a server control).** Chromium
   Private Network Access (preflight requiring
   `Access-Control-Allow-Private-Network: true` for public-site →
   localhost requests) has been evolving through 2025–2026 into a
   user-facing **Local Network Access permission prompt**; it reduces
   but does not eliminate attacker reachability of localhost faces, and
   other browsers are behind — hence server-side patterns 1–2 still
   carry the weight.
7. **State-changing requests are POST/JSON, never GET.** OWASP: state
   changes reachable by GET defeat SameSite=Lax; the API-first shape
   (JSON body + custom header) forces preflight, which doubles as the
   CSRF boundary for cross-site JS.

### 3.2 What the token-auth face needs, concretely (facts only)

- Serve on 127.0.0.1 by default (pattern 3 precedent set).
- Reject requests whose `Host` is not localhost/127.0.0.1/[::1]/
  configured LAN name (pattern 2; Transmission and qBittorrent default).
- Accept the token from a custom header (and/or `Authorization: Bearer`);
  no cookies, no CSRF token state (pattern 1 + OWASP custom-header
  paragraph). Hand-rolled cost demonstrated: the Host check is ~15 lines
  (§1.3).
- SSE endpoints answer same-origin EventSource with the Origin check of
  pattern 5; cross-origin machine clients (the fleet layer) use the
  header token, which browsers' EventSource cannot send — that asymmetry
  is why §2.5(c) alternatives exist.

## 4. What "API-first with token auth" minimally requires (precedent facts)

The embedded-UI tools where an external programmatic layer rides the
same endpoints the UI uses — the exact fleet-layer shape the ticket
describes:

- **Vault** (single binary, UI built-in, `ui = true`): the UI is served
  from the same listener at `/ui/`, and "the UI uses the Vault API as
  the basis for all operations: anything done in the UI has a
  corresponding API call" (developer.hashicorp.com Vault UI docs +
  UI tutorial); the UI's own API service authenticates with the
  `X-Vault-Token` header against `/v1/...` endpoints (vault/ui source,
  app services). Token via `X-Vault-Token` or `Authorization` header.
- **Consul** (single binary, agent + UI + API on one port): versioned
  `/v1/...` paths; ACL tokens via `X-Consul-Token` (or Bearer); the web
  UI is served beside the API by the same agent.
- **Nomad** (same shape): `/v1/...` REST paths, `X-Nomad-Token` (or
  Bearer), UI served by the agent on the same port.
- **Syncthing**: GUI and REST API share `127.0.0.1:8384`; API auth is
  the `X-API-Key` header; the GUI's own JS calls the same REST API with
  CSRF tokens while external tools use the key (docs + forum).
- **Versioning precedents**: path-versioned `/v1` is the common shape of
  Kubernetes (`/api/v1`, `/apis/GROUP/VERSION`, with a published
  deprecation policy), Vault, Consul, Nomad (all `/v1/...`). Header
  versioning is the other documented pattern: GitHub
  (`X-GitHub-Api-Version: 2022-11-28`, calendar versions, requests
  without the header default to the launch version) and Stripe
  (`Stripe-Version` header, account-pinned default; the `/v2` namespace
  *requires* the header).

Minimal contract elements, each traceable to the precedents above:

1. **JSON endpoints beside the HTML routes** (Vault: UI = API; Consul/
   Nomad: UI beside `/v1`); no endpoint exists that only the UI uses.
2. **Token from a header, not a cookie** (`X-Vault-Token` /
   `X-Consul-Token` / `X-Nomad-Token` / `X-API-Key` family, or standard
   `Authorization: Bearer` which Consul, Nomad and Vault also accept) —
   this is simultaneously the CSRF posture (§3) and what lets a
   non-browser fleet client authenticate with one static credential.
3. **A version prefix on the JSON surface** — `/v1/...` is the dominant
   precedent; header-versioning is the documented alternative when the
   URL surface must stay stable.
4. **A stable, machine-readable response/error shape** (all cited
   services return JSON errors on their APIs; the daemon's scratch face
   already returns a versioned envelope `"api": {"version": "v1"}`).
5. **Live updates over SSE** are additive: the server already speaks the
   SSE wire format toward the server's `/realtime/v1/stream`
   (Last-Event-ID replay, keepalive comments), and serving it back out
   (status/History Log events) needs only the broadcast channel the
   daemon already holds (§2.5) — bark-server-rs's `api/sse.rs` is the
   in-family serving reference.
6. No CORS emission is needed for the same-origin face; cross-origin
   fleet clients authenticate by header, and CORS only matters if the
   fleet plane is browser-hosted (Vault/Consul/Nomad ship no permissive
   CORS on their agent APIs).

## 5. Reproducibility notes

- Worktree: `/home/zhuangbin/SyncProjects/20261111/.worktrees/research-bs-face-cost`,
  branch `research/bs-face-cost` off `gui-stage` @ `8aade3e`. Only this
  document is committed; the scratch face code and Cargo.toml variants
  are uncommitted working-tree state (described precisely in §1.3).
- Builds: `cargo build --release -p echo-daemon` (binary `bark-echo`).
  Build times on the 8-core host: daemon-only rebuild ~5 s (V1→V2 class),
  axum-tree first compile ~52 s, echo-gui rebuild ~1 m 07 s (V3), full
  cold egui build ~5 min, tuned-profile builds: V0t 4 m 03 s, V2t
  1 m 33 s–1 m 51 s (warm dependency cache; LTO dominates). The V2t
  asset set is the V3 one (app.js 2,831 B vs V2's 2,730 B — 101 B noise
  inside the 10.3 MB figure).
- The `status.rs` module already exposes the exact state a face would
  serve (`StatusSnapshot` over the IPC Channel plus the server's `/info`),
  and `run.rs` already holds a `tokio::sync::broadcast::Sender<echo_ipc::Event>`
  event hub — the face's SSE stream would subscribe to that hub (the
  scratch face used a stand-alone bus).

## Sources

Measured in this workspace: all §1 numbers (worktree builds, stat
byte sizes), asset/htmx byte sizes (cdnjs 2.0.8 file + gzip -9),
tokio 1.54.1 broadcast source grep, Cargo.lock resolutions, axum
feature list (docs.rs/crate/axum/0.8.4/features), face smoke tests.

Cited:

- OWASP CSRF Prevention Cheat Sheet — https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html
- Transmission RPC spec (CSRF §2.2.1, DNS-rebinding whitelist §2.2.2) — https://github.com/transmission/transmission/blob/main/docs/rpc-spec.md
- Jackson, Barth, Boneh, Mitchell, *Protecting Browsers from DNS Rebinding Attacks* — https://web.eecs.umich.edu/~aprakash/eecs588/handouts/dns-rebinding.pdf
- qBittorrent WebUI API security keys — https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-(qBittorrent-5.0)
- Syncthing REST API (`X-API-Key`) — https://docs.syncthing.net/dev/rest.html
- Vault UI docs (UI = API, same listener) — https://developer.hashicorp.com/vault/docs/ui and https://developer.hashicorp.com/vault/tutorials/get-started/learn-ui ; HTTP API (`X-Vault-Token`) — https://developer.hashicorp.com/vault/api-docs
- Consul HTTP API structure (`/v1`, `X-Consul-Token`) — https://developer.hashicorp.com/consul/api-docs/api-structure
- Nomad HTTP API (`/v1`, `X-Nomad-Token`) — https://developer.hashicorp.com/nomad/api-docs
- Docker daemon socket protection — https://docs.docker.com/engine/security/protect-access
- Prometheus security model — https://prometheus.io/docs/operating/security/
- miniserve — https://github.com/svenstaro/miniserve ; dufs — https://github.com/sigoden/dufs
- MDN EventSource (`withCredentials`; no custom headers) — https://developer.mozilla.org/en-US/docs/Web/API/EventSource/withCredentials ; header-limitation discussion — https://stackoverflow.com/questions/28176933/
- Chrome Private Network Access → Local Network Access prompt — https://developer.chrome.com/blog/private-network-access-update (and 2026 LNA coverage)
- Kubernetes API versioning — https://kubernetes.io/docs/reference/using-api and deprecation policy https://kubernetes.io/docs/reference/using-api/deprecation-guide/
- GitHub API versioning — https://docs.github.com/rest/overview/api-versions
- Stripe versioning — https://docs.stripe.com/development (Stripe-Version header; /v2 requires it)
- askama/rinja merge — https://askama.rs/ (upgrading guide), https://github.com/askama-rs/askama
- rust-embed — https://crates.io/crates/rust-embed (8.13.0)
- htmx — https://htmx.org/docs ; 2.0.8 file — https://cdnjs.cloudflare.com/ajax/libs/htmx/2.0.8/htmx.min.js
- bark-server-rs (in-family axum precedent, SSE serving reference) — local checkout `/home/zhuangbin/SyncProjects/20261110/bark-server-rs` (`Cargo.toml`, `src/api/sse.rs`)
