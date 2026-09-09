# Local Landing Zones transfer lab

One Linux image represents three independent machines. Real generated transfer
scripts run under non-root accounts; the host driver only coordinates them.
No production server, credentials, shared data directory, or host SSH port is used.

Prerequisites: Python 3.9+ on a macOS/Linux host, a running Docker engine with Compose v2,
and access to image/package registries for the initial build. Both ARM64 and
x86_64 use the native architecture of the base image.

From this directory:

```sh
python3 lab.py setup
python3 lab.py run
python3 lab.py inspect
python3 lab.py validate
python3 lab.py reset
```

`setup` builds the current checkout, initializes groups and directories, builds
the runtime scripts, generates disposable keys, and verifies both SSH connections.
It refuses to overwrite existing lab containers. `run` seeds two projects and
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

## Route and accounts

| Machine | Account | Data access |
| --- | --- | --- |
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
system files remain readable. SSH permits only `lab_transfer` and `b_receive`,
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
