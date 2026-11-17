# Local Landing Zones transfer lab

One Linux image represents four independent machines, including an SFTP-only target. Real generated transfer
scripts run under non-root accounts; the host driver only coordinates them.
No production server, credentials, shared data directory, or host SSH port is used.

Prerequisites: Python 3.9+ on a macOS/Linux host, a running Docker engine with Compose v2,
and access to image/package registries for the initial build. Both ARM64 and
x86_64 use the native architecture of the base image.

For independent checks, follow the [testing guide](../../docs/testing-transfers.md).
From this directory, initialize once and run each layer separately:

```sh
python3 lab.py setup
python3 lab.py sftp-smoke
python3 lab.py sftp-adapter
python3 lab.py python-transfers --connection sftp_copy
python3 lab.py python-transfers --connection local_copy
python3 lab.py python-transfers --connection local_move
python3 lab.py permissions
python3 lab.py python-transfers --connection rsync_copy
```

`setup` builds the current checkout, initializes groups and directories, builds
the runtime scripts, generates disposable keys, and verifies the two internal SSH connections and the SFTP subsystem.
It refuses to overwrite existing lab containers. `python3 lab.py run` first checks account isolation,
SFTP transport, the product adapter and all Python routes, then seeds two projects and
executes the full scenario, including an intentional Cluster B outage and retry.
Generated remote scripts retain their normal random startup delay of up to
59 seconds, so allow several minutes. No cron daemon is involved.

`inspect` prints container state, SSH logs, transfer events, and accessible file
trees with ownership and modes. Files, keys, and logs live in each container's
writable Linux filesystem and survive stop/start. `reset` removes only resources
in the fixed `landingzones-container-lab` Compose project. It leaves the reusable
image cached. Run reset/setup for a fresh scenario, or after changing code.
Do not use the reserved Compose project name for other work.

### Review and validate the monitoring TSV

Each transfer account writes real schema-version-1 events to
`/home/<user>/runtime/log/monitor.transfers.tsv` inside its container. This is
the generated event history; `runtime/transfers.tsv` remains the input route
definition. No monitoring events are fabricated by the lab.

After a successful scenario, `run` automatically validates these files and
exports review copies to `tests/container_lab/output/` on the host:

- `cluster-a.a_transfer.transfers.tsv`
- `cluster-a.a_distributor.transfers.tsv`
- `cluster-b.b_distributor.transfers.tsv`
- `validation.json` — written only when all checks pass.

Run `python3 lab.py validate` to repeat the check without rerunning transfers.
Validation checks the exact header and row schema, unique event IDs, expected
runtime/user/flow identities, exactly one completion per route, the deliberate
`push_alpha` outage followed by a successful retry, and run identity across
both hops. Unexpected failures and duplicate completions fail validation.
An unfinished scenario will fail these final-history checks; use `inspect`
to investigate it. A TSV is exported before validation so malformed rows can
be reviewed too. Errors identify the file and, for malformed events, its row.

The host output folder is ignored by Git and remains after `reset`; validation
refreshes these files and removes any previous success summary first. Existing
containers built before this feature need `reset` followed by `setup` to receive
the explicit spool path and validator. Reset removes the lab's container data,
so inspect or copy any old results you want to retain first.

## Single transfer file and Python execution

[`transfers.tsv`](transfers.tsv) is the authoritative lab route fixture. It now
contains eight legacy move steps and four enabled Python steps: local copy,
remote rsync copy, SFTP copy, and local move. `executor` selects exactly one
execution owner. Generated legacy runtime tables contain only legacy rows; the
Python executor reads the same authoritative table and selects its runtime IDs.
An explicit copy operation on a legacy row is rejected rather than executed as
a move. The legacy generator also excludes Python rows when given the full table.

`python3 lab.py python-transfers --connection NAME` invokes real `landingzones transfer`
commands under A's transfer account for just the selected connection:

| Connection | Check |
| --- | --- |
| `local_copy` | Local copy and source retention. |
| `local_move` | Same-server intake move followed by a local delivery move and source cleanup. |
| `rsync_copy` | Rsync/SSH delivery, with SFTP inspection and publication. |
| `sftp_copy` | Deliberate SFTP outage, restart, resume and repeated-request idempotency. |

Omit `--connection` to run all four. Only the SFTP scenario stops/restarts an
endpoint; it restores the target even when the expected outage check fails.
Payloads include
nested data, an empty file, and a filename with spaces.
Every selected scenario reads the published files as the recipient account and
compares their full manifest and portable package identity with the accepted
input. A completed executor response alone cannot pass the scenario.

The generated `/home/a_transfer/runtime/execution.yaml` contains local state and
event-spool locations, runtime selection, the actual execution context, and key
references. It points to `/opt/lab/transfers.tsv`. Request IDs, run IDs, payload
IDs and actual phase attempt IDs are generated by the
product. Selected scenarios export reports to `output/python-transfers-NAME.json`
and events to `output/python-NAME.events.tsv`. The combined run writes
`output/python-transfers.json` and `output/python.events.tsv`. Event exports are
snapshots of the runtime's accumulated schema-v1 spool, including earlier checks;
use the report's request IDs to identify the selected run. These are separate from legacy
scenario history. The scenario is also included in `lab.py run`. Each `python-transfers` invocation
uses unique payload names, request filenames, and idempotency keys, so it can be
repeated without reset/setup. Within each invocation the same request is still
repeated to test product idempotency. Previous data and state remain available.
Per-run evidence lives under `output/python-transfers-NAME/<scenario-id>/`
(or `output/python-transfers/<scenario-id>/` for the combined run). Each check
removes its previous top-level success report before starting. A started report without
passed status indicates an incomplete run. The full legacy `lab.py run` scenario
still requires reset/setup before repeating.

Credential references include the transfer method: `rsync_cluster_writer` and
`sftp_partner_upload`. Both point to the same disposable lab key and pinned-host
file; production connections can reference separate key files. Names are a
readability convention, while each route's `adapter` selects its behavior.

`python3 lab.py sftp-smoke` is an independent OpenSSH transport
probe. It uploads/downloads a unique folder, compares file checksums, checks
source retention and shell denial, and writes `output/sftp-smoke.json`. It does
not fabricate product events. OpenSSH is a diagnostic client; Paramiko remains
the application's only SFTP implementation.

`python3 lab.py sftp-adapter` exercises that actual product adapter directly,
without the executor, readiness processing, request state or events. It verifies
private staging, checksums, publication, destination-conflict refusal and source
retention, writing `output/sftp-adapter.json`. The source, final copy and rejected
conflict stage remain for inspection. Unique fixture names make both SFTP checks
repeatable without resetting the lab.

`python3 lab.py permissions` independently checks that transfer accounts and
project users cannot list or write unrelated directories. It does not run a
transfer or establish final output ownership/modes; those assertions are in
the full `python3 lab.py run` scenario.

The SFTP target forces `internal-sftp` inside a
root-owned chroot with writable `/incoming`. Internal SSH endpoints also expose
the SFTP subsystem for the Python rsync adapter's verification and promotion;
rsync itself still carries that adapter's payload bytes over SSH.

## Python request CLI

The following commands run inside the configured local lab execution account;
use the host driver above to run the automated scenario:

```sh
landingzones --config /home/a_transfer/runtime/execution.yaml transfer preflight --request /home/a_transfer/copy-request-RUN_ID.json
landingzones --config /home/a_transfer/runtime/execution.yaml transfer run --request /home/a_transfer/copy-request-RUN_ID.json
landingzones --config /home/a_transfer/runtime/execution.yaml transfer status REQUEST_ID
landingzones --config /home/a_transfer/runtime/execution.yaml transfer resume REQUEST_ID
landingzones --config /home/a_transfer/runtime/execution.yaml transfer resume REQUEST_ID --retry-parked
```

Exit status 0 means successful preflight/status read or completed execution;
1 means accepted work remains blocked; 2 means rejection or a configuration/IO
error before a structured execution outcome. An idempotency key must be reused
with exactly the same request content. Each invocation makes at most one new
transfer attempt per unfinished step and, when eligible, a cleanup attempt.
Permanent validation/collision failures park immediately; other failures use a
bounded attempt budget per phase. `--retry-parked` explicitly grants another
budget after repair. No background retry loop is created. Cron may invoke an
explicit saved request or `transfer discover --connection NAME`. Discovery resumes
unfinished local work even when source cleanup removed the original. Python cron
artifact generation is not implemented yet.

## Implemented migration boundary

The executor now handles independent connections. See the
[connection contract](../../examples/request-driven-transfers/README.md) and
[handoff ADR](../../docs/adr/0004-independent-package-handoffs.md) for authoritative
state, label, publication and recovery requirements. Use a fresh state directory
for revised schema-2 work; older request snapshots are explicitly rejected.

The local driver submits separate copy requests, rather than one multi-recipient
request, and exercises an independently failed SFTP connection. A source intended
for several independent copies should be labelled at its managed admission point
if those copies must share a portable identity; unlabelled admissions can otherwise
receive separate identities. Existing legacy archive scenarios remain unchanged.

The Python path does not migrate legacy archive packaging, owner/group/mode tuning,
notifications or automatic cron generation. Configured remote sources use SFTP
inspection and download, including sources named with an `ssh://` URL. Remote
copy targets require SFTP inspection/rename; rsync pushes additionally require
rsync and SSH. Remote moves remain a capability error. Server power-loss durability
of SFTP operations has not been established.

## Route and accounts

| Machine | Account | Data access |
| --- | --- | --- |
| SFTP target | `upload` | Chrooted `/incoming` via SFTP only; shell denied |
| Lab | `producer`, `lab_transfer` | Lab export only, via `export` group |
| A | `a_transfer` | Intake and outbound; holds the only client SSH key |
| A | `a_distributor` | Intake and both project workspaces |
| A | `processor` | Both project workspaces and outbound |
| B | `b_receive` | Intake only; accepts A's key |
| B | `b_distributor` | Intake and both project workspaces |
| A and B | `alpha_user`, `beta_user` | Their own project only |

The raw flow pulls lab exports into A, then distributes and extracts them into
A's project directories. The preprocessing stand-in validates the original files
and creates a checksum result in a new outbound run. The processed flow pushes
that run into B's intake, then B distributes and extracts it. Each project has
its own raw and processed flow identities. Portable run IDs must survive each
flow's handoff; preprocessing starts a new run identity.

The lab transfer account has write access to its export because the real scripts
archive and remove source data. These are move semantics, not a read-only pull.
Groups and setgid directories enforce data boundaries, not a chroot: ordinary
system files remain readable. Internal SSH permits only `lab_transfer` and `b_receive`; the SFTP target permits only `upload`,
with password authentication and forwarding disabled. There is no B-to-A key.
Root initializes the containers; transfers and assertions run as named accounts.

## Assertions and diagnostics

The driver checks nested files, an empty file, a filename with spaces, expected
preprocessing checksums, source cleanup, final owner/group/modes, metadata and
run-ID continuity, and schema-v1 completion events. It verifies denied directory
listing and file creation through SSH and across project boundaries. Stopping B
must produce a failure event and preserve A's unsent content; restoring B must
allow delivery. Repeating completed push/distribution scripts must not create
another completion or alter the final metadata and payload.

A failed test retains everything for inspection. The run marker prevents mixing
fixtures from separate attempts. Individual runtime files are under
`/home/<account>/runtime/`: `config.yaml`, `transfers.tsv`, `output/scripts/`,
`log/`, and `flock/`. For example:

```sh
docker compose -p landingzones-container-lab -f compose.yaml exec --user a_transfer cluster-a sh
```

Host-side configuration and generated shell syntax can be checked without Docker:

```sh
cd ../..
pixi run pytest tests/test_container_lab.py
```

The container scenario checks Linux permission behavior and rsync/SSH execution.
It does not emulate shared cluster filesystems, production networking, scheduling,
or real preprocessing. Docker execution is an explicit opt-in, not part of the
default pytest suite. No public Landing Zones CLI changes are required.
