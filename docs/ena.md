# ENA uploads and optional metadata submission

The feature branch supports `adapter=ena` for local packages copied into a
configured ENA Webin upload area. Supply prepared Webin V2 XML to also submit
metadata and retain returned accessions. Without metadata, completion means
verified upload only. Neither outcome establishes later archival validation.

ENA uses Python's standard-library FTPS and HTTPS clients, loaded only for an
ENA request. It adds no package dependency to local, rsync or SFTP transfers.
SFTP has one implementation, using Paramiko; install `landingzones[sftp]` when
using SFTP sources/destinations or remote rsync destinations. The Pixi development
environment and standalone bundle include that extra.

## Connection configuration

Use the ordinary Python transfer table fields with:

| Field | Value |
| --- | --- |
| `executor` | `python` |
| `adapter` | `ena` |
| `operation` | `copy` |
| `verification` | `checksum` |
| `source` | An absolute local input directory |
| `destination` | `ena://webin2.ebi.ac.uk/incoming` |
| `credential_ref` | `ena_webin` |
| `flow_group` | A unique connection name, for example `ena_upload` |

`webin.ebi.ac.uk` is also accepted. The URL path is relative to the Webin
account's upload area; it must already exist. Omit it to use the account home.
Credentials are separate from the URL:

```yaml
credentials:
  ena_webin:
    username: Webin-12345
    password_env: ENA_WEBIN_PASSWORD
ena:
  environment: test
```

The account above is illustrative. Provision the password through the named
environment variable in the execution context; never put it in the transfer
table, request, command arguments or a committed file. ENA credentials are
validated when that adapter is selected. Unused ENA credentials do not require
SSH key files or a populated password environment variable.

`ena.environment` selects the **metadata submission service**, defaulting to
`test`; set `production` explicitly for production submissions. Both use the
configured Webin upload area. Test submission is not a dry run of file upload:
files are uploaded to that area. Test accessions must not be treated as durable
production identifiers.

## Requests and results

A request uses the existing CLI interface, `transfer run --config CONFIG
--request REQUEST`. For one package already in the configured input:

```json
{
  "idempotency_key": "example-run-123-ena",
  "connection": "ena_upload",
  "payload_name": "run-123",
  "adapter_options": {
    "submission_file": "/absolute/path/to/prepared-webin.xml"
  }
}
```

Omit `adapter_options` for upload only. Discovery also performs upload only;
metadata submission is an explicit request. `preflight` snapshots prepared XML
and checks its file references against the accepted package. `run` records that
snapshot with accepted work, so later edits to the XML file do not change a
retry. Supply a fresh idempotency key for a different intended submission.

The XML must be one UTF-8 Webin V2 `WEBIN` document, at most 15 MiB, containing
one `SUBMISSION` with an explicit alias, one `ADD` action and optionally one
`HOLD` action. Object aliases must be unique within their object type. `MODIFY`,
`RELEASE`, arbitrary API calls, metadata generation and sample selection are
outside this adapter. Local validation checks this bounded contract; ENA performs
its own full schema and content validation.

Each `FILE` filename must name an accepted data file relative to the package,
such as `reads/sample_R1.fastq.gz`. At submission, the adapter substitutes the
verified upload path and its MD5. A supplied MD5 must match. Metadata files can
be kept outside the package; internal Landing Zones labels and readiness markers
are never uploaded.

The normal `transfer status --config CONFIG REQUEST_ID` result retains:

- `deliveries[0].steps[0].delivery_receipt`: exact upload directory and each
  file's remote path, byte count, SHA-256 and MD5.
- `deliveries[0].steps[0].submission`: durable submission state and raw XML receipt.
- `deliveries[0].steps[0].result.accessions`: returned accession, object type,
  alias, status and any external accession identifiers.
- `result.environment` and `result.archive_validated`: service provenance and
  the explicit fact that archive validation has not been established.

Accessions remain in the local receipt store; no separate spreadsheet or remote
database is required. Back up that store through the deployment's own workflow.
Submission metadata and receipts may contain scientific metadata and need the
same access controls as their source data. They are not included in event messages.

## Verification and recovery

Files are uploaded under a unique `landingzones-<UUID>` directory owned by the
local receipt. This remains the final upload location; there is no remote rename
or atomic consumer-publication claim. A retry rewrites files in its own directory
and refuses unexpected contents. Upload retries restart files rather than using
byte-range resume. The source is retained throughout.

The transport requires verified explicit TLS on port 21, protected passive data
connections, `MLSD` type/size listings and `RETR` readback. It computes SHA-256 and
MD5 while reading uploaded files back; this costs additional transfer bandwidth.
Unsupported capabilities fail explicitly. Their availability on a live ENA
account still requires user-run verification. No plain FTP fallback is attempted.

Delivery is saved before metadata submission starts. A metadata failure leaves
that upload receipt intact. A successful ENA XML receipt is required for requested
submission to complete; HTTP success by itself is insufficient.

Submission intent is persisted before the HTTPS POST. A timeout, disconnect or
unusable response leaves `submitting` or `uncertain` state, which blocks another
POST even with `resume --retry-parked`. Obtain the receipt independently from the
same ENA service/account, then use the local reconciliation interface:

```text
landingzones transfer reconcile --config CONFIG REQUEST_ID \
  --receipt RECEIPT.xml --environment test
```

Reconciliation checks the service selected by the caller and the saved submission
alias; it imports successful receipt evidence without uploading or submitting.
An XML receipt does not authenticate its origin: the operator must obtain it
from the correct service/account. A definitive rejection remains recorded; use a
corrected request with appropriate unique aliases after resolving its errors.
Do not discard state to bypass uncertain outcomes.

This slice requires metadata at initial request acceptance. Adding metadata to a
previous upload-only request is not supported; a new request gets a new upload
directory. Retries of the original request reuse its directory and receipt.

## Monitoring and validation boundary

Submission events use event schema 2; existing transfer phases still emit schema
1 with the same TSV columns. Upgrade monitoring ingestors before enabling ENA
submission. Older ingestors skip schema-2 rows and advance their checkpoints;
recover skipped evidence by replaying retained spools under a new spool ID after
upgrade. New ingestors read both versions and stop at unsupported future versions.
The database table layout is unchanged.

Local tests use synthetic files and mocked FTPS/HTTPS. They cover upload and
submission failures, retries, receipt persistence, isolation and reconciliation.
They do not prove live ENA acceptance, account permissions or network behavior.
Deployment and live validation remain user-run; this document defines the app
interface and is not a deployment handoff.

Sources: [ENA upload and retention contract](https://ena-docs.readthedocs.io/en/latest/submit/fileprep/upload.html)
and [Webin programmatic submission and receipts](https://ena-docs.readthedocs.io/en/latest/submit/general-guide/programmatic.html).
