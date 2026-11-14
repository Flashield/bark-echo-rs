# Coding Standards

The review pass reads this file. Mechanical violations (fmt, clippy
`-D warnings`, tracked build artifacts — CI's "No build artifacts tracked"
step) belong to the tooling; this file carries the judgement calls no
guardrail substitutes for. Each rule names the failure that paid for it.

## Wire fakes close gracefully

A test TCP fake writes its response, drains the peer's request bytes, then
`shutdown()` — FIN. A bare `drop` on a socket with unread receive-buffer
data sends RST on BSD/macOS, destroying the peer's buffered response: the
401 envelope arrives empty and the classification under test flips.
See `stream_events.rs`'s `close_gracefully`. _(Paid by CI round 6.)_

## Platform-conditional tests name the platform reality

A test whose behavior depends on the OS — keyring availability, path
conventions, how `close` treats a socket — is cfg-gated with a comment
stating the platform fact that forces the gate, and what desktop users
still get. An unexplained `#[cfg]` invites the next reader to "fix" it.
_(Paid by CI rounds 5–8: macOS keychain, XDG paths, Windows casts.)_

## CLI tests: temp HOME + `--plain-secrets`, always

Every CLI-subprocess test runs in the harness's temp HOME in plain-secrets
mode. Keyring mode is exercised only behind a reachability probe, never
assumed — a probe against a headless secret store turns an assertion
failure into a store error and the test reports the wrong problem.
_(Paid by CI rounds 1–2.)_

## User-facing text speaks GLOSSARY

Docs, README prose, and CLI strings use the defined terms (`GLOSSARY.md`)
and never its _Avoid_ words — `backfill`/`sync`/`fetch` for Pull,
`cache`/`archive` for History Log. The glossary is the vocabulary
authority; drift in strings is drift in the domain. _(Paid by the review
pass: "backfill" survived in the README and a doc comment.)_

## Cite ADRs as written

A code comment citing an ADR must match the ADR's actual content. Where
the implementation has outgrown the ADR, update the ADR in the same
commit; the code never misquotes its decisions — that is how a deliberate
choice gets "fixed" by a later reader. _(Paid by the review pass:
secrets.rs cited "keyed by server + device" while keying by token_id.)_
