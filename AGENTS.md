# bark-echo-rs

Desktop Realtime Client for bark-server-rs: receives pushes over SSE
(`/realtime/v1`), delivers them as desktop notifications, and can send
pushes to any Device. Rust. The server-side client contract lives in the
server repo: `bark-server-rs/docs/protocol.md`.

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
