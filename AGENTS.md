# AGENTS.md

Landing Zones is a public-safe app repository for the operator CLI that builds,
validates, reports, and deploys transfer runtimes.

## Commands

This project is managed with Pixi. Prefer the commands already defined in
`pixi.toml`.

## Agent skills

### Task tracking

Read [shared workflow](docs/agents/shared-workflow.md) first, then
[repository tracker settings](docs/agents/issue-tracker.md). Both wiki and
GitHub are supported. Triage labels apply to GitHub-owned tasks only; see
`docs/agents/triage-labels.md` when using that tracker.

### Domain docs

Single-context repo with `CONTEXT.md` and `docs/adr/` at the repo root. See
`docs/agents/domain.md`.

## Shared workflow

Before routing work, read [Shared Repository Workflow](docs/agents/shared-workflow.md)
for preferences, wiki/GitHub tracking, authorship, and execution boundaries.
Read [repository tracker settings](docs/agents/issue-tracker.md) for local mappings.
The shared file is wiki-owned and hardlinked here; see [the link contract](docs/HARDLINKS.md).
