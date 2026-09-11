# Independent connection examples

These files describe the revised feature branch, not deployed configuration.
Replace illustrative paths, hostname, account and credentials with a permitted
local execution context before use. Each `flow_group` selects one connection;
`step_order` remains a compatibility field, not a cross-server dispatcher.

## Invoking work

`request.example.json` chooses one configured connection and optionally supplies
an expected itinerary. `transfer preflight --request FILE` validates admission;
`transfer run --request FILE` executes it. `transfer resume ID` retries unfinished
phases; `--retry-parked` grants a new bounded retry budget after repair.
`transfer status ID` shows local state, timestamps, total bytes and elapsed time.

`transfer discover --connection analysis` scans the source and resumes unfinished
work. Schedule that command with cron if needed. `.ready` is required under
`producer_marker`; `managed_ready` means the input is exclusively published by a
trusted producer after completion. This is a configured trust boundary, not an
inference from inactivity or the existence of a label. Waiting input is reported
without starting an attempt.

Configuration is snapshotted in accepted local work. Changing the catalog does
not silently reroute an existing transfer. A request's idempotency key must not
be reused for different content. Local receipts suppress repeated copies for the
same connection, source/destination, package identity/version and payload name,
including requests with another key. Do not share or discard state directories.
One executor owns each configured connection; independent centers use separate
state stores and exchange packages rather than shared requests.

## Label format and admission

The portable `.landingzones-package.json` contains:

- `schema_version: 1`, `package_id`, stable `transfer_run_id`, `content_version`, and `manifest` (relative
  paths mapped to kind, file size and SHA-256).
- Optional `itinerary`: descriptive connection identifiers. They never authorize
  paths or commands. Local state reports `on_plan`, `discrepancy` or `unspecified`.
- `history`: prepared handoffs with connection and local transfer identity.
  A prepared handoff is not itself proof of publication; local completion events
  supply that evidence. Consumers can take the package before events are ingested.
- Optional `sequencing_run_id`, `parent_package_ids` and preserved `source_metadata`.

The content hash excludes this label, `.ready` and legacy `.landing_zones`
metadata. Symlinks and special files are rejected. Legacy archive bundles stored
inside `.landing_zones` are explicitly rejected rather than silently omitting data;
keep archive transport on legacy routes or unpack it before admission.

Schema 0 with the same identity, manifest and content-version fields can be
translated while preserving verified identity. This is a supported compatibility
shape, not a claim that old TSV labels contained hashes. Legacy TSV labels have
no matching inventory evidence: strict admission rejects them. `admission_policy`
`relabel` assigns a fresh identity and preserves the original label as source
metadata. Unknown/malformed or changed labels follow the same explicit policy.
A retained label alone cannot establish unchanged data. No label is accepted only
after readiness and inventory validation, and gets a new identity.

The executor does not modify the source label when copying. For independent copies
from one unlabelled input to share identity, provide a current label at the managed
admission point first (for example, an intake connection publishing a managed
folder). Successful downstream labels preserve that identity between centers.

Processing creates a new package. Pass explicit sequencing-run and parent identities
in a valid new label to preserve lineage; old identifiers are not inferred from
filenames or automatically attached to changed data. No processing status or
whole-run completion is invented from transfer events.

## Handoff and limits

Moves stage privately in a sibling directory outside the watched destination,
verify, complete source cleanup, then atomically publish. Copy retains its source.
The private stage and state belong exclusively to the executor; no consumer or
maintenance process may delete them. Publication intent plus disappearance of
that private stage reconciles a process interruption after rename, even if the
consumer has already removed the output. See ADR 0004 for the durability scope.

Local staging and publication require filesystem fsync and non-overwriting atomic
rename (macOS or Linux). Remote destinations support copy only: SFTP does not
provide the durable staging guarantee required here to delete the source first.
Remote source pulls through SFTP can move to a durable local destination. Pinned
host keys and configured credential references are required. Both ends remote use
the selected credential reference; use separate connections for distinct accounts.

Size verification is available for copies but weaker than checksums. Moves must
use checksum verification. Legacy packaging and access-mode tuning, coordinated
fan-out, automatic Python cron generation, dynamic routing, subscriptions and
orchestration integrations are not supplied by this slice. Remote network tests
require an explicit user-run environment; filesystem-backed tests do not prove
server behavior.

The old multi-recipient request-state mockup has been replaced with a schema-2
shape summary. Existing schema-1 executor state requires explicit migration and
is rejected, not silently reused.
