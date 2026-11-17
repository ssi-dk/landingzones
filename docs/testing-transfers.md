# Test transports, processing and permissions separately

Start with independent checks, then combine the parts. A passed transport check
establishes that the tested client, account and endpoint can perform the tested
operations. It does not establish production permissions or every later transfer.

Run commands below from the Landing Zones repository root. Local unit tests need
Pixi; the container checks need Python 3 and a running local Docker engine with
Compose v2. Initial setup downloads the image and packages. The lab generates
its own disposable keys and synthetic data; it needs no SSI or ENA credentials.

## Offline checks available now

| Command | What it isolates |
| --- | --- |
| `pixi run test-processing` | Readiness, package admission, local copy/move, staging/publication, receipts and recovery using local files and test doubles. |
| `pixi run test-sftp-unit` | Our SFTP adapter's client configuration, host-key handling, resource cleanup, verification and error handling against test doubles. |
| `pixi run test-ena-unit` | ENA upload behavior and metadata/receipt handling, tested separately with mocked FTPS/HTTPS. |
| `pixi run test-ena-execution` | ENA joined to the executor: optional submission, saved accessions, retry guards and reconciliation. Network calls remain mocked. |

These checks contact no endpoint and cannot establish that a real SFTP or ENA
service works. ENA's live check can wait for credentials without blocking the
other layers.

If Pixi cannot refresh its environment but the existing environment is usable,
run the same selected test files with `.pixi/envs/default/bin/python -m pytest`
and `PYTHONPATH=src`. Use `PATH="$PWD/.pixi/envs/default/bin:$PATH"` for tests that
launch Python subprocesses. Do not interpret dependency-installation failures as
transfer failures.

## Real SFTP against a disposable local endpoint

The deployment-time [credential check](credential-validation.md) verifies the
installed account's endpoint access before activation. The tests below then
exercise actual file movement and request processing as separate acceptance steps.

These commands create or operate only the reserved `landingzones-container-lab`
Docker project. Ensure Docker is using a **local** engine, not a remote context.
There are no host data mounts or published SSH ports. No production account is
used. Run each check separately and stop at the first failure.

```sh
python3 tests/container_lab/lab.py setup
python3 tests/container_lab/lab.py sftp-smoke
python3 tests/container_lab/lab.py sftp-adapter
python3 tests/container_lab/lab.py python-transfers --connection sftp_copy
```

1. `setup` builds the current checkout, including the SFTP dependency, creates
   Linux users/directories and pins disposable host keys. It refuses an existing
   lab so old state is not silently overwritten.
2. `sftp-smoke` uses the OpenSSH SFTP client. It checks upload/download bytes,
   source retention and denied shell access. Failure here points first to the
   lab endpoint, connectivity, account, key or directory setup.
3. `sftp-adapter` calls the actual Paramiko-based Landing Zones adapter directly,
   without the request executor, readiness discovery, state store or event spool.
   It checks private staging, content verification, publication and retained source.
4. `python-transfers --connection sftp_copy` adds the request executor and tests
   a deliberate endpoint outage, resume, completed-request deduplication and source
   retention. The target is restarted after the outage. It does not run the local
   or rsync transfer scenarios.

OpenSSH is only the independent diagnostic client. Paramiko remains the sole
SFTP implementation in Landing Zones.

If step 2 passes and step 3 fails, investigate the adapter's configuration and
operations. If step 3 passes and step 4 fails, investigate the request/executor
integration first. Compare the error and affected phase rather than assigning
every failure at that layer to one cause.

The fixture includes nested files, empty content and spaces in filenames.
Results are written under `tests/container_lab/output/`; a successful report is
evidence only for the named check. Failed checks retain diagnostic state.
Request scenarios independently read the delivered files as the recipient
account and check their complete manifest and package identity before reporting
success.

## Processing and permissions

Run these independently after setup:

```sh
python3 tests/container_lab/lab.py python-transfers --connection local_copy
python3 tests/container_lab/lab.py python-transfers --connection local_move
python3 tests/container_lab/lab.py permissions
python3 tests/container_lab/lab.py python-transfers --connection rsync_copy
```

The local copy/move cases test the executor against a real Linux filesystem
without a remote transport. `permissions` checks that accounts cannot list or
write unrelated directories. The rsync case tests the application's rsync adapter in that prepared
environment; it is not a test of rsync's protocol implementation. This adapter
also uses SFTP for destination inspection and publication.

The existing `python3 tests/container_lab/lab.py run` remains the combined
legacy sequencing scenario, including archive preparation/extraction, expected
ownership/modes, project isolation and failure recovery. Use it after the
individual checks. This is where successful transfer access and final ownership,
group and mode requirements are checked together. It is a broader integration
check, not the first diagnostic.

Later, repeat endpoint and account checks against the actual deployment.
Production ACLs, groups, privileges, filesystem behavior and routing are not
established by this local lab. ENA additionally needs an authenticated upload
and test-service submission with a real returned receipt before claiming live
compatibility; keep that gate pending until credentials are available.

## Inspection and cleanup

```sh
python3 tests/container_lab/lab.py inspect
```

Inspect reports before cleanup. `python3 tests/container_lab/lab.py reset`
removes the lab containers and their writable data, leaving exported host reports
and the image cache. Use it only when those disposable container contents are no
longer needed. Rebuild with `setup` after changing code; existing containers do
not automatically use new files. See the [lab reference](../tests/container_lab/README.md)
for the accounts and scenario details.
