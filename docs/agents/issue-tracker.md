# Issue tracker: GitHub

Issues and PRDs for this repo live as GitHub issues in `ssi-dk/landingzones`.
Use the `gh` CLI for issue operations.

This app repo should be treated as public. Do not include private deployment
details such as hostnames, user accounts, private paths, tokens, private deploy
manifests, payload contents, or operational incidents. Put those details in the
relevant private deploy repo issue and link to it from a sanitized public issue
when needed.

## Conventions

- Create an issue: `gh issue create --title "..." --body "..."`
- Read an issue: `gh issue view <number> --comments`
- List issues: `gh issue list --state open`
- Comment on an issue: `gh issue comment <number> --body "..."`
- Apply or remove labels with `gh issue edit`.
- Close an issue with `gh issue close`.

Infer the repo from `git remote -v`; `gh` does this automatically when run
inside the clone.

## Cross-repo work

For work spanning app, automation, and deploy repos, create or update a
repo-local issue for the public Landing Zones change, link related issues, and
associate them with the GitHub Project named `researchit todo`.

## When a skill says "publish to the issue tracker"

Create a GitHub issue in this repo unless the requested content contains
private deployment detail. If it does, create the detailed issue in the relevant
private deploy repo and keep this repo's issue public-safe.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number> --comments` inside this repo.
