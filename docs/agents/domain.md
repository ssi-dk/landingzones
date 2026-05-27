# Domain Docs

How the engineering skills should consume this repo's domain documentation when
exploring the codebase.

## Layout

This is a single-context repo.

## Before exploring, read these

- `CONTEXT.md` at the repo root.
- `docs/adr/` for ADRs that touch the area being changed.

If a specific doc does not exist, proceed silently. The producer skill
`grill-with-docs` creates domain docs lazily when terms or decisions are
resolved.

## Use the glossary's vocabulary

When output names a domain concept, use the term as defined in `CONTEXT.md`.
Do not drift to synonyms the glossary explicitly avoids.

If the concept is missing, either reconsider whether the project uses that
language or note the gap for `grill-with-docs`.

## Flag ADR conflicts

If output contradicts an existing ADR, surface it explicitly rather than
silently overriding the decision.
