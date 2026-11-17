# Optional ENA delivery and submission receipts

Status: implemented on the request-driven-transfers feature branch; local
mock-backed validation. Live service acceptance remains unverified.

## Decision

Keep one SFTP implementation using Paramiko. ENA is a separate, lazily loaded
adapter using standard-library FTPS for file upload and HTTPS for optional
caller-prepared Webin V2 metadata. ENA configuration and credentials are unnecessary
for other methods. Source data is retained; ENA moves are rejected.

ENA uses a unique upload directory with verified content and a durable local
receipt. It does not implement ADR 0004's consumer-directory rename. The executor
distinguishes rename publication from upload finalization: a missing ENA directory
cannot prove delivery. No SFTP or FTP rename guarantee is assumed for ENA.

An optional adapter request snapshot adds a separate submission phase after
delivery. Returned receipts, accessions and external identifiers belong to the
local work record, while monitoring receives operational facts. A successful
receipt means submission accepted, not subsequent archival validation completed.

Persist submission intent before POST. An unknown outcome never automatically
reposts, including after an explicit retry-budget reset. An independently obtained,
matching receipt can reconcile the record without another network operation.
Recover an already durable successful receipt before enforcing retry budgets.
Metadata submission defaults to the test service; production is explicit.

## Compatibility

Existing request JSON, configured routes, copy receipts and schema-2 execution
state remain valid. New fields are additive. Existing schema-1 execution state
still needs the migration described by ADR 0004. Upload-only discovery remains
available; it does not infer or submit metadata.

Transfer Event schema 2 adds the submission phase without changing TSV columns
or database tables. Existing phases emit schema 1. New ingestors accept both and
stop without checkpoint advancement on unknown future versions. Upgrade readers
before submission producers: already deployed old readers skip version 2, so
recovering skipped events requires a retained-spool replay with a new spool ID.

## Limits

Webin upload areas are temporary, and the adapter never deletes local sources.
Verified FTPS, directory listing and file readback capabilities must be checked
on the actual account before rollout. The adapter does not claim atomic ENA
publication, byte-range upload resume, metadata generation, release management,
or automatic resolution of ambiguous submission outcomes.
