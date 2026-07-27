# Spool-Decoupled Transfer Event Monitoring

Transfer runtimes emit immutable, schema-versioned Transfer Events to a local Event Spool and never write to the monitoring database directly, so monitoring availability cannot affect data movement. A separately invoked SQLAlchemy adapter ingests schema-version-1 spools idempotently into a host-local SQLite database, while current Transfer Definitions are synchronized into a separate table so expected routes with no history remain visible; other database backends require their own future integration coverage.

**Consequences**

- Runtime and portable histories use one fixed-width schema and preserve the same event identity for the same operational fact.
- A completed event is emitted only after destination delivery and required source cleanup; cleanup recovery creates a new attempt for the same run without retransferring data.
- Ingestion owns database credentials, complete-row checkpointing, and replay handling; the runtime owns only best-effort spool appends.
- Transfer Definitions and Transfer Events remain separate so disabled or never-observed routes are visible without inventing operational history.
- The live monitoring service queries the database for every request. The existing static report remains a legacy schema-0 surface.
- Schema version 1 supports synchronous host-local SQLite only. Additional backends require an explicit adapter and integration coverage.
