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
intent, and publish the staged directory with a rename into the consumer's input.
Flush files and directories before cleanup. A failed rename retains staging even
when the original has been removed. Consumers can immediately take the output;
subsequent events and local bookkeeping do not inspect that published package.

Publication intent plus disappearance of the uniquely owned stage is the local
reconciliation evidence after process interruption. This depends on exclusive
executor ownership of staging and state: consumers, other executors and cleanup
jobs must never remove stages. Manual deletion, state loss, filesystem rollback
or violated ownership invalidate that inference and require operator recovery.
This is not an exactly-once guarantee across arbitrary host/storage failure.
Local storage must support `fsync`. Establish network filesystem durability
before using it for moves. The default publication path uses atomic
non-overwriting rename on macOS and Linux. A local move checks publication
support in private staging before removing the source.

An execution configuration may list `exclusive_local_destinations` for storage
that rejects atomic no-overwrite rename. For those destinations only, the
executor can check that the final name is free and use a plain directory rename
while holding its state-directory lock. All publishers must share that lock.
An independent writer can still race the check, including one on another host
or in another runtime. An existing destination leaves the package staged for
operator recovery. Retained requests use the current policy for their stored
physical destination and can resume publication after cleanup.

**Future database coordination.** A shared database could let participating
publishers claim the destination and final package name with a unique constraint.
Retain the request and publication phase with the claim so another worker can
recover an interrupted publication. Direct filesystem writers would still need
to follow the same coordination rules.

An SFTP move flushes the staged files, label and directories using the receiver's
OpenSSH fsync extension. For group-editable delivery the receiving directory must
have the configured group. The executor verifies its name with the receiver,
sets the group and modes on the private package and checks the results. It then
renames the package, flushes both parent directories and persists the publication
receipt before cleaning up the source. If a rename reply is lost, recovery
requires the matching published package and label. An ambiguous delivery leaves
the source available for reconciliation. Remote rsync and ENA destinations support copies.
Configured remote-source pulls may move into a durable local destination; source
cleanup is resumable and checks remaining content. Producers must stop writing.

Copy receipts are keyed by connection/configured endpoints, package identity when
available, accepted version and payload name. Another request key or scan reuses
existing work. Unlabelled admissions across separate connections may assign distinct
identities; use a managed labelled input when copies need shared identity.

### Verification policy

The default `checksum` policy compares file SHA-256 digests throughout admission,
staging, publication and cleanup. A native local or SFTP connection can select
`size` for a copy or a move. It compares relative paths, entry types and byte lengths.
It still checks staging before source removal, refuses destination collisions and
preserves the same publication and recovery ordering. Changes that preserve file
size can pass size verification. ENA still requires a checksum-verified copy.

Existing requests retain their accepted policy, including size-copy receipts that
previously captured source checksums. A size-verified handoff can carry the previous
package identity and historical checksum manifest when the size inventory matches.
The label records `verification: size` for that handoff. A later checksum route
can verify the carried checksum manifest or record checksums for a size-only label.

## Configured local endpoint mappings

The executor supports an optional `local_path_base`, resolved relative to the
execution configuration's directory. Relative local transfer roots resolve from
that explicit base; absolute roots remain supported. Only configured endpoint
roots are normalized through symlinks. Payload paths retain canonical-path and
symlink checks. Remote endpoint URLs retain their existing contract.

Normalize roots before validating overlap or conflicting shared-source moves.
Accepted state, receipt identity and event source/destination paths use resolved
physical endpoints for mapped roots. Recovery uses the accepted snapshot rather
than resolving a changed alias again. Already-physical absolute spellings are
retained to preserve existing receipt keys. Each environment owns separate state;
promoting configuration does not promote test receipts.

Deployment owns the runtime, link targets, account access and private staging
beside the physical destination. The application only interprets configured
paths and applies the connection logic; it does not install a runtime or create
its endpoint aliases.

## Consequences and migration boundary

ADR 0003 remains applicable: completion follows both delivery and required cleanup,
and best-effort events are not execution state. The order is now cleanup before
consumer publication. Transfer Attempts represent actual phase execution; timestamps
allow duration inspection. Portable prepared history is not proof of publication.
Actual completion is recorded in local state and emitted as immutable events.

Legacy archive preparation and extraction remain on the legacy executor. A Python
connection with a local destination can set `publication_group` in its transfer
table. The executor applies the group and modes `0770` for directories and
`0660` for files, including the package label, inside private staging. A failure
blocks source cleanup. The execution account must be able to assign the group.
A local or SFTP handoff with a configured group can set the optional TSV
column `publication_directory_mode` to `2770`. The executor verifies setgid on
the package root and nested directories before publication. Regular files,
including the transfer label, use `0660`; the sibling staging root stays private.
Discovery is callable from cron; automatic Python cron generation remains
out of scope. Each configured connection must have one authoritative local store.
Remote accounts require SFTP access, and rsync push additionally requires rsync/SSH.
The sibling staging root must be writable exclusively by the executor and on the
same filesystem as the destination; provision it without exposing it to consumers.

Local fault-injection tests cover partial cleanup, failed publication and a crash
after rename followed by immediate consumer removal. Filesystem-backed SFTP tests
exercise pull/copy APIs without server connections; they do not establish server
or remote filesystem behavior. Server rollout and evidence remain user-owned.

## Bulk pulls into local staging

Select `adapter=rsync` with an `ssh://` source and a local destination to use
one rsync invocation per accepted package. SFTP still provides discovery,
inventory and verified source cleanup. The receiver writes admitted paths
directly into the receipt's private stage and flushes it before cleanup.
A retry can reuse complete staged files. The rsync inactivity timeout allows
a continuously progressing large package to run for longer than an hour.
An existing receipt resumes with its recorded adapter.
