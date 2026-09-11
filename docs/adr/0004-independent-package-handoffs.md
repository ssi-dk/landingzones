# Independent package handoffs

Status: implemented on the request-driven-transfers feature branch; local
validation only. This revises the branch's earlier local journey coordinator,
not the legacy generated-script execution contract.

## Decision

Each Python connection has one executor and a durable local receipt store.
Requests and scheduled discovery use the same executor. One selected flow group
contains one connection; coordinated multi-recipient requests are rejected.
Independent centers share portable package and Transfer Run identities, not mutable
request state. Each connection keeps its own receipt and phase attempts.
No shared execution database is required. Configuration schema 1 remains in use;
local state schema 2 rejects old schema-1 request snapshots pending explicit migration.

`.landingzones-package.json` schema 1 records package identity, accepted content
inventory/version, optional descriptive itinerary, prepared handoff history and
optional supplied provenance. Only an understood matching inventory justifies
preserving identity during label translation. Unknown, changed or legacy TSV
labels require explicit relabel admission and are retained as source evidence.
A legacy TSV run identifier without content evidence is not promoted to a verified
package identity. An equivalent schema-0 JSON inventory is supported. Relabelling
does not establish readiness or processing lineage.

## Publication and recovery

For local destinations, transfer into a private sibling staging directory,
verify, durably record staging, finish required source cleanup, persist publication
intent, and atomically rename without overwriting into the consumer's input.
Flush files and directories before cleanup. A failed rename retains staging even
when the original has been removed. Consumers can immediately take the output;
subsequent events and local bookkeeping do not inspect that published package.

Publication intent plus disappearance of the uniquely owned stage is the local
reconciliation evidence after process interruption. This depends on exclusive
executor ownership of staging and state: consumers, other executors and cleanup
jobs must never remove stages. Manual deletion, state loss, filesystem rollback
or violated ownership invalidate that inference and require operator recovery.
This is not an exactly-once guarantee across arbitrary host/storage failure.
Local storage must implement fsync and atomic non-overwriting rename; network
filesystem durability must be established before using these paths for moves.
The macOS and Linux rename implementations explicitly refuse existing targets.

SFTP copies use the same stage/intent reconciliation for process interruptions,
but SFTP persistence through server power loss is unverified. Remote-destination
moves are rejected before transfer because durable remote staging is not guaranteed.
Configured remote-source pulls may move into a durable local destination; source
cleanup is resumable and checks remaining content. Producers must stop writing.

Copy receipts are keyed by connection/configured endpoints, package identity when
available, accepted version and payload name. Another request key or scan reuses
existing work. Unlabelled admissions across separate connections may assign distinct
identities; use a managed labelled input when copies need shared identity.

## Consequences and migration boundary

ADR 0003 remains applicable: completion follows both delivery and required cleanup,
and best-effort events are not execution state. The order is now cleanup before
consumer publication. Transfer Attempts represent actual phase execution; timestamps
allow duration inspection. Portable prepared history is not proof of publication.
Actual completion is recorded in local state and emitted as immutable events.

Legacy archive preparation, extraction and access-mode tuning remain on the legacy
executor. Discovery is callable from cron; automatic Python cron generation remains
out of scope. Each configured connection must have one authoritative local store.
Remote accounts require SFTP access, and rsync push additionally requires rsync/SSH.
The sibling staging root must be writable exclusively by the executor and on the
same filesystem as the destination; provision it without exposing it to consumers.

Local fault-injection tests cover partial cleanup, failed publication and a crash
after rename followed by immediate consumer removal. Filesystem-backed SFTP tests
exercise pull/copy APIs without server connections; they do not establish server
or remote filesystem behavior. Server rollout and evidence remain user-owned.
