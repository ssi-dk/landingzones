# Documentation Hardlinks

This manifest declares the repo-owned documentation that should be mirrored
into the private wiki reading surface with hardlinks.

Wiki reference root: `Wiki/Products/Landingzones/Reference`

Status: active. These wiki paths are intended to be hardlinks. Recreate them
after a fresh clone or if inode verification fails. In the multi-repo
workspace, see `docs/documentation-hardlinks.md`.

## Mirrors

| Repo path | Wiki path | Required |
| --- | --- | --- |
| `README.md` | `Wiki/Products/Landingzones/Reference/apps-landingzones-README.md` | yes |
| `CONTEXT.md` | `Wiki/Products/Landingzones/Reference/apps-landingzones-CONTEXT.md` | if present |
| `CHANGELOG.md` | `Wiki/Products/Landingzones/Reference/apps-landingzones-CHANGELOG.md` | if present |
| `CONTRIBUTING.md` | `Wiki/Products/Landingzones/Reference/apps-landingzones-CONTRIBUTING.md` | if present |
| `CODE_OF_CONDUCT.md` | `Wiki/Products/Landingzones/Reference/apps-landingzones-CODE_OF_CONDUCT.md` | if present |
| `SECURITY.md` | `Wiki/Products/Landingzones/Reference/apps-landingzones-SECURITY.md` | if present |
| `SUPPORT.md` | `Wiki/Products/Landingzones/Reference/apps-landingzones-SUPPORT.md` | if present |
| `GOVERNANCE.md` | `Wiki/Products/Landingzones/Reference/apps-landingzones-GOVERNANCE.md` | if present |
| `docs/**/*.md` except `docs/agents/**` | `Wiki/Products/Landingzones/Reference/apps-landingzones-docs/` | if present |

## Excluded

- `AGENTS.md`
- `PLAN.md`
- `docs/agents/`

## Notes

- The wiki path must contain ordinary Markdown file content, not a symlink.
- This file is included in the mirrored `docs/` tree.
- Product-level wiki notes remain wiki-owned and should link to these mirrors.
