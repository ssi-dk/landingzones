# Version Detected GitHub Release Action

Tracking backend: local fallback
Status: Ready to Merge
Target repo: app
Visibility: public-safe
Created: 2026-05-13
Last updated: 2026-05-13
GitHub issue: not created
Branch: feature/archive-entry-end-transfers
PR: not opened

## Summary

Add a GitHub Actions workflow that detects the app package version and creates a
GitHub release named `v<version>` when that release does not already exist.

## Type

release / automation

## Repository routing

Target repo: app

Related repos: deploy

Routing rationale: release automation for the public app repository belongs in
`app/.github/workflows`. Deployment manifests that consume release tags are
tracked separately in `deploy`.

Public/private sensitivity: public-safe.

## Problem

The release process currently depends on `v*` tags, but there is no app workflow
that automatically creates a GitHub release when the package version is bumped.

## Expected behavior

On a push to `main`, the workflow reads `src/landingzones/__init__.py`, derives
tag/release name `v<version>`, and creates the release if it does not already
exist.

## Actual behavior or current limitation

Version bumps are local code/config changes until a tag or release is created
manually.

## Acceptance criteria

- [x] Add a workflow that runs on push to `main` and manual dispatch.
- [x] Read the version from `src/landingzones/__init__.py`.
- [x] Validate the version matches `pixi.toml`.
- [x] Create release/tag `v<version>` only when the release does not already exist.
- [x] Use GitHub-provided credentials and avoid third-party release actions.
- [x] Add tests that inspect the workflow behavior.

## Test plan

Test requirement: workflow structure test.

Test type: YAML/static test in `tests/test_python_standalone_packaging.py`.

Failing-test expectation: the new test fails before the workflow file exists or
contains the expected release-creation behavior.

Existing tests to run:

```text
./.pixi/envs/default/bin/pytest tests/test_python_standalone_packaging.py
env PATH="$PWD/.pixi/envs/default/bin:$PATH" ./.pixi/envs/default/bin/pytest
```

Manual validation, if any: actual release creation requires pushing to `main`.

## Implementation notes

Creating the release tag will trigger existing `v*` tag workflows, including
standalone bundle and package publishing workflows.

## Dependencies / blockers

- Blocks: automated GitHub release creation for version bumps
- Blocked by: none
- Needs decision: whether to promote this local fallback record to GitHub
- Needs more info: none

## Tasks

- [x] Create local fallback issue record.
- [x] Add failing workflow test.
- [x] Add release-on-version workflow.
- [x] Run targeted and full tests.
- [x] Update issue with verification.

## Updates

### 2026-05-13 08:42 CEST - In Progress

- Work done: routed the task to `app` and created this local fallback issue.
- Files changed: `.github/llm_issues/2026-05-13-version-release-action.md`.
- Verification: pending.
- Blockers or decisions: no GitHub issue created; using local fallback tracking.

### 2026-05-13 08:44 CEST - Ready to Merge

- Work done: added `.github/workflows/release-on-version.yml`, a workflow that
  detects the app version, validates `pixi.toml`, checks for an existing
  release, and creates `v<version>` with `gh release create` when missing.
- Files changed: `.github/workflows/release-on-version.yml`, `README.md`,
  `tests/test_python_standalone_packaging.py`,
  `.github/llm_issues/2026-05-13-version-release-action.md`.
- Verification: the new workflow test failed before implementation because the
  workflow file was missing; after implementation, the focused test passed, the
  packaging module passed with the pixi environment on `PATH`, and the full app
  suite passed with 231 tests.
- Blockers or decisions: actual release creation still requires this workflow to
  be merged and pushed to `main`.

## Links

Parent issue:

Child issues:

Related PRs:

Docs:

## Labels

Suggested labels:

```text
area:release
type:infra
priority:medium
risk:medium
status:ready-to-merge
test:required
repo:app
```
