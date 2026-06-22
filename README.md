# Landing Zones

Automated data transfer system using rsync with cron job generation.

## Quick Start

```bash
# Install
pip install -e .

# Generate cron files, transfer scripts, and validation wrappers
landingzones --help
landingzones --config config/config.yaml build
landingzones build

# Check deployment readiness
landingzones validate deployment

# Run a hop-local validation
landingzones validate hop <flow_group> preflight
landingzones validate hop <flow_group>

# Run toy data through the configured flows
landingzones validate integration
```

## Project Structure

```
landingzones/
├── src/landingzones/           # Main package
│   ├── cli.py                  # Top-level operator CLI
│   ├── generate_cron_files.py  # Cron generation tool
│   ├── check_deployment_readiness.py
│   ├── plot_transfer_status.py
│   └── config/transfers.tsv    # Default config
├── input/                      # Default input directory
├── output/                     # Default output directory
│   ├── crontab.d/              # Generated cron files
│   ├── scripts/                # Generated transfer scripts
│   └── validation_scripts/     # Generated validation wrappers
├── log/                        # Default log directory
├── tests/                      # Test suite
├── pyproject.toml              # Package config
└── README.md
```

## Configuration

The system is configured via a tab-separated `transfers.tsv` file:

| Column | Description | Example |
|--------|-------------|---------|
| `identifiers` | Unique transfer ID used for generated shell script names | `transfer_001`, `server1_to_server2` |
| `runtime_id` | Required deploy/artifact identity used for cron grouping and filtering | `server1_prod.user1` |
| `system` | Configured system key used for managed paths and flock settings | `server1`, `localhost` |
| `users` | Optional user/account context for review and generated headers | `user1`, `local` |
| `source` | Source directory path | `/srv/data/src/` |
| `source_port` | SSH port for remote sources (optional) | `2222` |
| `destination` | Destination (local or remote) | `user@host:/dest/` |
| `destination_port` | SSH port (optional) | `225` |
| `rsync_options` | Additional rsync flags | `--chown=:group` |
| `io_nice` | Optional `ionice` settings for `rsync` | `-c2 -n7` |
| `log_file` | Log file name resolved under the system log folder | `transfers.log` |
| `flock_file` | Lock file name resolved under the system flock folder | `transfer.lock` |
| `flow_group` | Optional logical flow label shared by multi-hop transfers | `labnet_to_seqdata` |
| `is_entry_point` | Optional `TRUE` marker for the first hop of a logical flow | `TRUE` |
| `is_end_point` | Optional `TRUE` marker for the final hop of a logical flow | `TRUE` |
| `readiness_policy` | Entry-point readiness policy; defaults to current direct behavior | `direct`, `stable_snapshot` |
| `readiness_stable_observations` | Number of identical source inventories required before `stable_snapshot` is eligible | `2` |
| `readiness_quiet_seconds` | Minimum seconds since the last observed source change before `stable_snapshot` is eligible | `300` |
| `readiness_fingerprint_mode` | Source inventory fingerprint mode | `path_size_mtime` |

Future todo: add an optional second, per-remote-host lock for cross-server
transfers. The existing `flock_file` prevents one transfer from overlapping
with itself; a host-level lock would limit concurrent SSH/rsync handshakes
against the same remote server when many transfer rows run on the same cron
schedule.

### Example

```tsv
identifiers	runtime_id	system	users	source	source_port	destination	destination_port	rsync_options	io_nice	log_file	flock_file
local_copy	localhost_test.testuser	localhost	testuser	input/*		output/				transfers.log	landingzones.lock
```

## CLI Commands

```bash
# Generate cron files with defaults
landingzones build

# Generate only selected runtime IDs from a shared transfers.tsv
landingzones build --runtime-id server1_prod.user1 --runtime-id server2_prod.user2

# Check deployment readiness
landingzones validate deployment

# Run a hop-local validation wrapper through the CLI
landingzones validate hop <flow_group>

# Seed toy data and run the real scripts/logs/locks
landingzones validate integration

# Generate an HTML health dashboard from a shared transfer TSV log
landingzones report transfers output/log/Landing_Zone_server1_prod.user1.transfers.tsv
```

### Generated Cron Format

```bash
*/15 * * * * /bin/sh output/scripts/local_copy.sh
```

### Transfer Catalog Loading Modes

The transfer catalog is the owner of transfer loading invariants. Future
transfer-column, filtering, endpoint-expansion, validation, artifact-naming,
boolean, tag, and warning behavior should land at the catalog seam before
command code consumes the rows.

`load_runtime_transfer_catalog` is the build/runtime loading mode. It keeps
runtime validation enabled, including required runtime file columns such as
`log_file` and `flock_file`, disabled-row filtering, exact `runtime_id`
filtering, path-variable endpoint expansion, generated artifact names, boolean
normalization, tag normalization, and shared lock/log warning metadata.

`load_reporting_transfer_catalog` is the reporting/analysis loading mode. It
uses the same normalized transfer facts, filtering, endpoint expansion,
booleans, tags, and artifact naming, but reporting analysis can omit
runtime-only `log_file` and `flock_file` columns because dashboards inspect
transfer metadata and status logs instead of generating runnable scripts.

Command boundaries:

- `landingzones build` uses the runtime catalog.
- `landingzones validate deployment` uses the runtime catalog.
- `landingzones validate integration` uses the runtime catalog.
- `landingzones validate separation` uses the reporting catalog.
- `landingzones report transfers` uses the reporting catalog.

### Entry-Point Readiness Policies

Entry-point readiness checks protect flows where the upstream producer writes
directly into a visible source tree. They are a mitigation, not a guarantee:
only producer-controlled atomic publish or a producer completion marker can
prove that a run is complete.

`readiness_policy=direct` keeps the existing behavior. When a row has
`is_entry_point=TRUE`, the generated script mints portable metadata, archives
the visible run directory, and removes the unpacked source contents from that
hop before transfer. Use this for producers that publish atomically, or where
the operator accepts the current direct-archive contract.

`readiness_policy=stable_snapshot` is the consumer-side mitigation for local
entry-point sources. The generated script observes each top-level run without
modifying producer-visible contents, records durable state under the
entry-root `.landing_zones_readiness` sidecar directory, and waits until the
same path/size/mtime fingerprint has been seen for
`readiness_stable_observations` observations and the configured
`readiness_quiet_seconds` has elapsed. Once eligible, it copies the run into a
Landing Zones-owned private snapshot, confirms that the source still matches
the snapshot, and archives from that private copy. The original producer-visible
run is left in place for separate retention or explicit operator cleanup.

### Shared Main Transfer Locks

`flock_file` is the top-level transfer lock for one generated script. Remote
transfers run startup jitter before acquiring the main `flock_file`, then use
non-blocking `flock -n` for the main lock. This spreads same-minute cron starts
before the lock race, but a transfer can still skip a run if another process
keeps the same main lock busy after the jitter. This behavior does not change
the cron cadence in `transfers.tsv`; for example, a `*/2 * * * *` row remains a
two-minute schedule.

`landingzones build` and `landingzones validate deployment` warn with `Shared
main transfer locks detected` when multiple transfer rows in the same runtime
resolve to the same main `flock_file`. Treat that warning as an operator review
item: either the shared lock is intentional serialization, or one of the rows
should get a distinct `flock_file`.

The shared main-lock warning is different from the generated
transfer-status and notification-status locks. Files such as
`Landing_Zone_<system>.transfers.lock` and
`Landing_Zone_<system>.notifications.lock` protect short TSV append sections and
are expected to be shared by all scripts for a runtime.

To verify a generated script manually, hold its resolved `flock_file`, run the
script with debug output, and check the message order:

```bash
script=/path/to/generated_transfer.sh
lock=$(sed -n 's/^flock_file="\([^"]*\)"/\1/p' "$script" | sed "s|\$HOME|$HOME|")
mkdir -p "$(dirname "$lock")"
(
  exec 200>"$lock"
  flock -n 200
  LZ_DEBUG_CLI=1 /bin/sh "$script"
) 2>&1
```

For remote transfers, debug output should show `startup delay ...` before
`using lock file ...`; if the held lock wins, the script then reports
`lock busy, exiting`.

## Installation

```bash
# Development mode
pip install -e ".[report]"

# With test dependencies
pip install -e ".[test]"

# Production
pip install .
```

### Lab Sequencer Bundle

For lab machines where a managed Python environment is awkward, build a
relocatable bundle using a `python-build-standalone` runtime. Build it on a
machine that matches the lab sequencer OS, architecture, and libc family.

Download or provide a python-build-standalone `install_only` archive, then run:

```bash
cd app
python scripts/build_python_standalone_bundle.py --python-archive /path/to/cpython-*-install_only.tar.*
```

If you already extracted the runtime, point at its Python executable instead:

```bash
python scripts/build_python_standalone_bundle.py --python-bin /path/to/python/install/bin/python3
```

With Pixi, the app includes a packaging task that downloads a matching
python-build-standalone runtime using `getpybs`:

```bash
cd app
pixi run build-standalone
```

Build the lab Linux artifact on Linux. A bundle built on macOS contains a macOS
Python runtime and will fail on the sequencer with `cannot execute binary file`.
Before copying a tarball to the lab host, verify the bundled runtime:

```bash
file packaging/dist/landingzones-standalone/python/bin/python3
packaging/dist/landingzones-standalone/python/bin/python3 -c "import platform; print(platform.system(), platform.machine())"
```

Expected for the current lab machines is Linux/x86_64.

The standalone bundle installs the core operator CLI without pandas, so it is
intended for `build`, `validate`, and `deploy` on locked-down lab machines.
`landingzones report transfers` remains a reporting extra and should run from
an environment with `landingzones[report]` installed.

The same bundle can be produced by the GitHub Actions workflow
`Build Standalone Bundle`. Run it manually from Actions, or push a `v*` tag.
It uploads `landingzones-standalone-linux-x86_64` containing:

```text
landingzones-standalone-linux-x86_64.tar.gz
```

For `v*` tags, the workflow also creates or updates the matching GitHub Release
and uploads `landingzones-standalone-linux-x86_64.tar.gz` as a release asset.

The GitHub Actions workflow `Create Release From Version` runs on pushes to
`main` when the app version files change. It reads
`src/landingzones/__init__.py`, validates `pixi.toml` has the same version, and
creates the missing `v<version>` GitHub Release. That tag then triggers the
existing `v*` release and publishing workflows.

The bundle is written to:

```text
app/packaging/dist/landingzones-standalone/
app/packaging/dist/landingzones-standalone.tar.gz
```

Copy the tarball to the lab machine, extract it, and run it like the normal CLI:

```bash
./landingzones --config config/config.yaml build
./landingzones --config config/config.yaml validate deployment
```

For offline builds, pass `--wheelhouse /path/to/wheels` so dependencies are
installed from local wheels. The legacy shell wrapper still works:

```bash
./scripts/build_python_standalone_bundle.sh --python-archive /path/to/cpython-*-install_only.tar.*
```

The bundle carries Python and Python packages only; the target machine still
needs system tools such as `rsync`, `ssh`, `flock`, `curl`, and `cron`.

## Testing

```bash
# Run all tests
pytest

# Verbose
pytest -v

# With coverage
pytest --cov=landingzones --cov-report=html

# Specific test
pytest tests/test_generate_cron_files.py::TestClassName::test_method
```

### Validation Modes

The operator-facing validation surface has three modes:

- `landingzones validate deployment`
- `landingzones validate hop <flow_group> [preflight|run]`
- `landingzones validate integration`

`landingzones validate integration` is the heavier integration-style test mode. It copies toy data into the configured starting locations, generates the real shell scripts, and runs the transfers using the normal log and flock paths.
Use `landingzones validate integration --slow` when you want the harness to print the result of each completed step and wait for Enter before running the next one.

Generated transfer scripts create portable `.landing_zones` sidecars for every enabled transfer. `flow_group` is optional sidecar metadata: when a transfer mints a new sidecar the value may be blank, and downstream transfers preserve the value already stored in the sidecar.

When a row has `is_entry_point=TRUE`, the generated script archives top-level
run directories into `.landing_zones/landingzone-run-archive.tar` before
transfer. The default `readiness_policy=direct` archives from the visible source
and removes unpacked contents from that hop. The `stable_snapshot` policy
archives from a Landing Zones-owned private snapshot and leaves the original
producer-visible run in place. Intermediate hops then move the archive plus the
`.landing_zones` metadata instead of thousands of individual payload files. When a later row has
`is_end_point=TRUE`, the generated script extracts the archive after staging
promotion and removes the archive from the final destination. Archive extraction
validates that tar entries are relative paths before unpacking.

### Generated Validation Wrappers

Each `flow_group` with exactly one `is_entry_point=TRUE` row gets a generated wrapper in the configured validation-scripts directory:

```text
output/validation_scripts/lz_run_validation_<flow_group>.sh
```

Use `landingzones validate hop <flow_group>` as the main interface. The generated wrapper remains available directly and bakes in:

- the entry directory for that flow
- the immediate next hop for preflight checks
- the default fixture directory under `test_data`
- the `flow_group` and producer labels used in the `LZTEST_...` folder name

Typical usage:

```bash
# Regenerate scripts after changing config/transfers
landingzones --config config/config.yaml build

# Check only the current hop structure and immediate next-hop access
landingzones validate hop local_labnet_to_server1_data preflight

# Inject a validation run with the baked-in defaults
landingzones validate hop local_labnet_to_server1_data

# Inject a validation run with an explicit token suffix
landingzones validate hop local_labnet_to_server1_data --token ABCD

# Direct wrapper execution still works if needed
./output/validation_scripts/lz_run_validation_local_labnet_to_server1_data.sh
```

Wrapper/CLI behavior:

- no action defaults to `run`
- `preflight` checks only the current hop plus its immediate next hop
- options-only invocation such as `--token ABCD` also defaults to `run`

Use `landingzones validate hop` for lightweight producer-side validation. Use `landingzones validate integration` when you want the heavier integration test that seeds toy data and executes the full generated transfer chain.

Required config in your deployment `config.yaml`:

```yaml
transfers_file: input/transfers.tsv
test_data: tests/toy_data/
validation_scripts_dir: output/validation_scripts/
rit_managed_locations:
  test_local: tests/test_local
flock_paths:
  test_local: /opt/homebrew/bin/flock
rit_managed_folder_structure:
  log: output/log/
  flock: output/flock/
  sh_output: output/scripts/
  crontabs: output/crontab.d/
```

Typical local fixture layout:

```text
deploy/local/
├── config/config.yaml
├── input/transfers.tsv
├── tests/toy_data/
└── tests/test_local/
```

How to run it:

```bash
# Run from the deployment root that owns config/, input/, and tests/
cd deploy/local

# Generate deployment artifacts
landingzones build --config config/config.yaml

# Run the heavier integration test
landingzones --config config/config.yaml validate integration
```

What it does:

- Filters `transfers.tsv` to the current `system` and `user`
- Seeds each initial source root from `test_data`
- Generates scripts into the configured `sh_output` directory
- Uses the configured `log` and `flock` directories
- Executes the scripts in transfer order
- Validates that the seeded top-level directories reached the terminal destinations

Before seeding, integration validation summarizes pre-existing endpoint entries.
Expected toy-data directories and visible source entries are blockers because a
new run would mix old and new test data. Unrelated destination leftovers are
reported as extras. An empty `.staging` directory is reported as managed
persistent staging state and does not block reruns, because successful Landing
Zone Runtime transfers preserve that group-writable staging root. A `.staging`
path still blocks when it is non-empty, not a usable directory, or cannot be
inspected.

After a successful run it asks whether you want cleanup. Answer `y` to remove the propagated test directories plus generated log and lock artifacts so the next run starts from the initial state. Answer `n` to inspect the final tree and logs.

## Deployment

1. Configure `transfers.tsv` with your routes
2. Generate cron files: `landingzones build`
3. Deploy:
   ```bash
   cp output/crontab.d/*.cron ~/crontab.d/
   cat ~/crontab.d/*.cron | crontab -
   ```

Or use automated deployment:
```bash
landingzones deploy cron
```

`landingzones deploy cron` defaults to `--cron-scope execution-context`. This
scope replaces the active crontab with a previewed **Cron Activation Plan** for
the current system/user **Execution Context**: selected runtime cron fragments
are activated, same-context staged runtime cron fragments are preserved,
unidentified staged `.cron` files are preserved, and foreign or unresolved
runtime fragments are excluded unless you choose a broader scope.

Cron scopes:

- `execution-context`: safe default for shared managed hosts.
- `expected`: uses Generated Runtime Metadata, while staying bounded by the
  current Execution Context.
- `replace-selected`: activates only the selected Runtime Selection plus
  unidentified staged `.cron` files; this is the explicit replacement path for
  older selected-runtime behavior.
- `staged`: activates every staged `*.cron` file after preview.

Exact staged filenames can be omitted with repeated CLI flags or config:

```bash
landingzones deploy cron \
  --exclude-cron-fragment old-runtime.Landing_Zone.cron
```

```yaml
cron_fragment_exclusions:
  - old-runtime.Landing_Zone.cron
```

Missing exclusion filenames are shown in the preview and do not block
activation. Non-interactive cron activation still requires
`--confirm-cron-activation`.

Upgrade note: the older selected-runtime default is now the explicit
`replace-selected` scope. The compatibility name `selected` still works, but it
prints a warning and maps to `replace-selected`.

## Development

```bash
# Make changes in src/landingzones/
# Run tests
pytest

# Test CLI
landingzones --help
```

## Requirements

- Python >= 3.8
- PyYAML >= 5.0.0
- pandas >= 1.0.0 only for `landingzones report transfers` / `landingzones[report]`
- System: rsync, ssh, flock
- System for archived entry/end-point flows: tar
