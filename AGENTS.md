# bark-echo-rs

Desktop Realtime Client for bark-server-rs: receives pushes over SSE
(`/realtime/v1`), delivers them as desktop notifications, and can send
pushes to any Device. Rust.

- **Client contract** (consumed, never modified): the server checkout at
  `~/SyncProjects/20261110/bark-server-rs/docs/protocol.md`. The test seam
  pins that server as a git dev-dependency — the rev lives in
  `crates/echo-core/Cargo.toml` under `[dev-dependencies]`; bump it only
  when the server's protocol-facing behavior is verified against
  `protocol.md` at that rev.
- **Orchestration notes** from implement-spec runs (embedded-server API,
  TestServer patterns, crate versions): `~/SyncProjects/20261111/.notes/`.
- **Review standards**: `CODING_STANDARDS.md` (the review pass reads it;
  implementation reads it to avoid the flagged shapes).

## Agent skills

### Issue tracker

Issues live in GitHub Issues (`Flashield/bark-echo-rs`), managed with the
`gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-role vocabulary: `needs-triage`, `needs-info`,
`ready-for-agent`, `ready-for-human`, `wontfix`. See
`docs/agents/triage-labels.md`.

### Domain docs

Single-context layout: `GLOSSARY.md` at the repo root, ADRs in
`docs/adr/`. See `docs/agents/domain.md`.
