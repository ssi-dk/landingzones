# Spool-Decoupled Transfer Event Monitoring

Transfer runtimes emit immutable, schema-versioned Transfer Events to a local Event Spool and never write to the monitoring database directly, so monitoring availability cannot affect data movement. A monitoring-side SQLAlchemy adapter ingests schema-version-1 spools idempotently into a host-local SQLite database, while current Transfer Definitions are synchronized into a separate table so expected routes with no history remain visible; other database backends require their own future integration coverage.

**Consequences**

- Runtime and portable histories use one fixed-width schema and preserve the same event identity for the same operational fact.
- A completed event is emitted only after destination delivery and required source cleanup; cleanup recovery creates a new attempt for the same run without retransferring data.
- Ingestion owns database credentials, complete-row checkpointing, and replay handling; the runtime owns only best-effort spool appends.
- Transfer Definitions and Transfer Events remain separate so disabled or never-observed routes are visible without inventing operational history.
- The live monitoring service queries the database for every request. The existing static report remains a legacy schema-0 surface.
- Schema version 1 supports synchronous host-local SQLite only. Additional backends require an explicit adapter and integration coverage.

Monitoring also reads schema-1 Python execution configurations. It resolves the
transfer table from the configuration directory and local endpoint aliases from
`local_path_base`, preserving remote URLs and disabled definitions. Reporting
does not authenticate credentials or require the report process to share the
executor's hostname/account; route ownership must still match the configured
execution context. Transfer execution retains its actual-context check.

The monitoring HTTP process may own periodic ingestion of explicitly configured
schema-v1 spools. This keeps one service responsible for ingestion and reporting
without moving database access into transfer execution. Separate `monitor ingest`
invocations remain supported; reader-only serving remains the default when no
spools are configured.

Native executors report logical payload bytes in the existing JSON message of a
`started` transfer event. A measurement can include accepted bytes and file
count, observed bytes, bytes per second, and an estimated number of seconds left
in the payload transfer. Any value may be unknown. A separate phase heartbeat
records the worker's last observation. The index suppresses rates and estimates
after two minutes without a byte sample, even when heartbeats continue. Elapsed
time for an unconfirmed or stale attempt ends at its last observation.

The JSON run index exposes current phase attempts in `runs[].activity`. Run
detail exposes all phase attempts in `attempts` and the original events in
`timeline`. Measurements belong to their runtime, route, run, request, and
attempt. A retry starts its own counters. A subsequent publication or cleanup
phase may report the previous payload's measured bytes and sample time. Those
phases have their own elapsed time and receive no payload rate or ETA.

A native runtime can also expose an atomic version-1 route-discovery snapshot
in `scheduler/status.json` under its configured `state_dir`. The report reads
this optional file on each request. The Route discovery section and
`GET /api/scheduler` expose the dispatcher observation, slot limit, discovery
interval and each route's last finished scan, next due time, current scan start
and reported worker PID. The scheduler's stated wait reason distinguishes a
slot wait from an overlapping queue or contention outside the dispatcher.
A successful scan can find zero packages. Package delivery and byte counts
continue to come from the Transfer Event Spool.

The API returns `scheduler: null` when no snapshot is present for the selected
runtime. With an enabled dispatcher, an observation more than two minutes old
is stale; route states and PIDs are historical at that observation. A persisted
hold with no dispatcher or running workers remains held. New writer fields
are ignored. A malformed or inaccessible snapshot is reported as unavailable.

When an intermediate route finishes and the catalog identifies an enabled
downstream route, a run with no downstream observation is marked
`awaiting downstream observation`. Its completed phase appears in history.
The `current_phase` is null, `last_event_phase` preserves the last recorded
phase, and `run_age_seconds` (also available as `age_seconds`) measures the
unfinished run from its creation or earliest observation. The dashboard counts
active phase attempts separately. If a downstream attempt is already open when
upstream cleanup finishes, the history reports the upstream route as completed
and the downstream attempt continues to appear under its own phase.

For a native execution catalog, a downstream handoff requires an enabled
consumer on the same runtime and system with the exact local destination queue
as its source. Native flow groups identify individual connections. A disabled
consumer or a similarly named directory on another host establishes no handoff.
