# Archive Entry And End Point Transfers

Tracking backend: local fallback
Status: Ready to Merge
Target repo: app
Visibility: public-safe
Created: 2026-05-12
Last updated: 2026-05-12
GitHub issue: not created
Branch: feature/archive-entry-end-transfers
PR: not opened

## Summary

Add transfer rows that can mark the first and final points of a multi-hop flow so
large file sets are archived once at entry, transferred as one bundle through
intermediate hops, and extracted only at the final destination.

## Type

feature

## Repository routing

Target repo: app

Related repos: none for the public-safe implementation

Routing rationale: generated transfer behavior, tests, and README updates live
in the public application package.

Public/private sensitivity: public-safe; no internal runtime names, endpoints,
or credentials included.

## Problem

Multi-hop transfers with many small files spend too much time walking and
transferring individual payload files at every hop.

## Expected behavior

When a transfer row has `is_entry_point=TRUE`, each top-level run directory is
converted to a portable archive before transfer. Intermediate hops move the
archive and `.landing_zones` metadata. When a later transfer row has
`is_end_point=TRUE`, the archive is extracted after staging promotion and
removed from the final destination.

## Actual behavior or current limitation

Before this change, all hops transferred unpacked payload files.

## Acceptance criteria

- [x] Entry-point transfer creates a portable archive and removes unpacked source contents.
- [x] Intermediate destination contains the archive and metadata, not unpacked payload files.
- [x] End-point transfer extracts payload files at the final destination and removes the archive.
- [x] Archive implementation uses `tar` for Linux-to-Linux deployments.
- [x] README documents the transfer columns, behavior, and system dependency.

## Test plan

Test requirement: add failing regression test first.

Test type: generated shell integration-style unit test using local temporary
directories.

Failing-test expectation: the new test should fail while the generator does not
create the expected archive file.

Existing tests to run:

```text
./.pixi/envs/default/bin/pytest tests/test_generate_cron_files.py::TestGenerateRsyncCommand::test_entry_and_end_points_archive_across_intermediate_hops
./.pixi/envs/default/bin/pytest tests/test_generate_cron_files.py
./.pixi/envs/default/bin/pytest tests/test_check_deployment_readiness.py
env PATH="$PWD/.pixi/envs/default/bin:$PATH" ./.pixi/envs/default/bin/pytest
```

Manual validation, if any: none.

## Implementation notes

Use an uncompressed `.tar` archive rather than `.zip` because the target
deployments are Linux machines and the goal is reducing file-count overhead
without spending avoidable CPU on compression.

## Dependencies / blockers

- Blocks: none
- Blocked by: none
- Needs decision: whether to promote this local fallback record to GitHub
- Needs more info: none

## Tasks

- [x] Add regression test for entry and end point archive behavior.
- [x] Confirm the test fails before implementation.
- [x] Implement tar archive creation at entry points.
- [x] Implement tar extraction at end points.
- [x] Document transfer columns and tar dependency.
- [x] Run targeted and full test suites.

## Updates

### 2026-05-12 12:16 CEST - Tests Passing

- Work done: added entry/end-point archive support and switched the archive
  implementation from zip/unzip to uncompressed tar.
- Files changed: `README.md`, `src/landingzones/generate_cron_files.py`,
  `tests/test_generate_cron_files.py`.
- Verification: focused archive regression passed; `tests/test_generate_cron_files.py`
  passed; `tests/test_check_deployment_readiness.py` passed; full app suite passed
  with 230 tests; `git diff --check` passed.
- Blockers or decisions: GitHub issue was not created during the task; this local
  fallback issue records the work until a GitHub issue is created or explicitly skipped.

### 2026-05-12 12:25 CEST - In Review

- Work done: ran the completed app changes through the `dev_flow` dispatcher,
  issue tracking, readiness, test-selection, and review checklist steps.
- Files changed: `README.md`, `src/landingzones/generate_cron_files.py`,
  `tests/test_generate_cron_files.py`,
  `.github/llm_issues/2026-05-12-archive-entry-end-transfers.md`.
- Verification: routing is `app` and public-safe; acceptance criteria are
  testable and checked; regression test was added first and observed failing
  before implementation; targeted modules and full app suite were already run
  successfully; `git -C app diff --check` remains clean.
- Blockers or decisions: no PR is open yet; GitHub issue promotion still needs
  an explicit decision.

### 2026-05-13 08:32 CEST - Ready to Merge

- Work done: bumped the app hotfix version to `1.1.8` for release prep.
- Files changed: `src/landingzones/__init__.py`, `pixi.toml`.
- Verification: full app suite passed with 230 tests after the version bump.
- Blockers or decisions: release tag and artifact publication are still separate
  follow-up operations tracked in the deploy release-prep issue.

## Links

Parent issue:

Child issues:

Related PRs:

Docs: `README.md`

Release prep: `deploy/llm_issues/2026-05-13-hotfix-release-1.1.8.md`

## Labels

Suggested labels:

```text
area:artifact
type:feature
priority:medium
risk:medium
status:ready-to-merge
test:required
repo:app
```
