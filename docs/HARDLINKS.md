# Documentation Hardlinks

This manifest declares the repo-owned documentation that should be mirrored
into the private wiki reading surface with hardlinks.

Wiki reference root: `wiki/20_Products/Landingzones/Reference`

Status: active. These wiki paths are intended to be hardlinks. Recreate them
after a fresh clone or if inode verification fails. In the multi-repo
workspace, see `wiki/99_Meta/Documentation Hardlinks.md`.

## Mirrors

| Repo path | Wiki path | Required |
| --- | --- | --- |
| `README.md` | `wiki/20_Products/Landingzones/Reference/apps-landingzones-README.md` | yes |
| `CONTEXT.md` | `wiki/20_Products/Landingzones/Reference/apps-landingzones-CONTEXT.md` | if present |
| `CHANGELOG.md` | `wiki/20_Products/Landingzones/Reference/apps-landingzones-CHANGELOG.md` | if present |
| `CONTRIBUTING.md` | `wiki/20_Products/Landingzones/Reference/apps-landingzones-CONTRIBUTING.md` | if present |
| `CODE_OF_CONDUCT.md` | `wiki/20_Products/Landingzones/Reference/apps-landingzones-CODE_OF_CONDUCT.md` | if present |
| `SECURITY.md` | `wiki/20_Products/Landingzones/Reference/apps-landingzones-SECURITY.md` | if present |
| `SUPPORT.md` | `wiki/20_Products/Landingzones/Reference/apps-landingzones-SUPPORT.md` | if present |
| `GOVERNANCE.md` | `wiki/20_Products/Landingzones/Reference/apps-landingzones-GOVERNANCE.md` | if present |
| `docs/**/*.md` except `docs/agents/**` | `wiki/20_Products/Landingzones/Reference/apps-landingzones-docs/` | if present |

## Excluded

- `AGENTS.md`
- `PLAN.md`
- `docs/agents/`

## Notes

- The wiki path must contain ordinary Markdown file content, not a symlink.
- This file is included in the mirrored `docs/` tree.
- Product-level wiki notes remain wiki-owned and should link to these mirrors.

## Incoming shared agent guidance

Canonical owner: team wiki. This is an incoming public-safe policy file, not
an outgoing product documentation mirror. Keep it out of product Reference folders.

| Wiki source (workspace-relative) | Repo destination | Required |
| --- | --- | --- |
| `wiki/99_Meta/Agents/Shared/repository-workflow.md` | `docs/agents/shared-workflow.md` | yes |

Commit the destination as ordinary Markdown so standalone clones retain it.
Git does not preserve hardlinks. Reconcile any content differences before
relinking; verify matching device and inode. The wiki source owns shared edits.
Setup and restoration: `wiki/99_Meta/Agents/Shared Agent Hardlinks.md`.
