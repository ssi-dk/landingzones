#!/usr/bin/env python3
"""Host driver. Requires only Python 3 and Docker Compose v2."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import uuid

from node import PROJECTS

COMPOSE = ["docker", "compose", "--project-name", "landingzones-container-lab", "--file", str(Path(__file__).with_name("compose.yaml"))]
RUNTIMES = (("cluster-a", "a_transfer"), ("cluster-a", "a_distributor"), ("cluster-b", "b_distributor"))
PYTHON_CONNECTIONS = ("local_copy", "rsync_copy", "sftp_copy", "local_move")
PYTHON_RECIPIENTS = {
    "local_copy": ("cluster-a", "a_transfer", "/data/copy-output"),
    "rsync_copy": ("cluster-b", "b_receive", "/data/intake/python"),
    "sftp_copy": ("sftp-target", "upload", "/srv/sftp/incoming"),
    "local_move": ("cluster-a", "a_transfer", "/data/move-output"),
}


def compose(*args, check=True, capture=False, input=None):
    return subprocess.run(COMPOSE + list(args), check=check, text=True,
                          capture_output=capture, input=input)


def execute(service, user, *args, **kwargs):
    return compose("exec", "-T", "--user", user, "-e", f"HOME=/home/{user}",
                   service, *args, **kwargs)


def node(service, user, *args, **kwargs):
    return execute(service, user, "python", "/opt/lab/node.py", *args, **kwargs)


def python(service, user, code):
    return execute(service, user, "python", "-c", code, capture=True).stdout.strip()


def setup():
    existing = compose("ps", "--all", "--quiet", capture=True).stdout.strip()
    if existing:
        raise RuntimeError("Lab containers already exist. Inspect them or run reset before setup.")
    compose("build", "lab")
    compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "120")
    node("cluster-a", "a_transfer", "credentials", "a_transfer")
    public = execute("cluster-a", "a_transfer", "cat", "/home/a_transfer/.ssh/id_ed25519.pub", capture=True).stdout
    known_hosts = []
    for service, user in (("lab", "lab_transfer"), ("cluster-b", "b_receive"), ("sftp-target", "upload")):
        node(service, user, "credentials", user, input=public)
        # Read the actual public host key through the local Docker control plane.
        key = execute(service, user, "cat", "/etc/ssh/ssh_host_ed25519_key.pub", capture=True).stdout.split()
        known_hosts.append(f"{service} {key[0]} {key[1]}\n")
    execute("cluster-a", "a_transfer", "sh", "-c", "cat > ~/.ssh/known_hosts", input="".join(known_hosts))
    for host in ("lab_transfer@lab", "b_receive@cluster-b"):
        execute("cluster-a", "a_transfer", "ssh", host, "true")
    execute("cluster-a", "a_transfer", "sftp", "-b", "-", "upload@sftp-target", input="ls /incoming\n")
    print("Setup complete. Run: python3 lab.py run", flush=True)


def events(service, user):
    return json.loads(node(service, user, "events", capture=True).stdout)


def transfer(service, user, identifier, failure=False, empty=False):
    print(f"{service}/{user}: {identifier}" + (" (expected outage)" if failure else ""), flush=True)
    before = {event["event_id"] for event in events(service, user)}
    result = execute(service, user, "sh", f"/home/{user}/runtime/output/scripts/{identifier}.sh", check=False)
    added = [event for event in events(service, user) if event["event_id"] not in before]
    if failure:
        assert any(e["status"] == "failed" and e["exit_code"] not in ("", "0") for e in added), added
        assert not any(e["status"] in ("completed", "delivered") for e in added), added
    else:
        assert result.returncode == 0, result.returncode
        assert not any(e["status"] == "failed" for e in added), added
        completed = [e for e in added if e["status"] == "completed"]
        assert len(completed) == (0 if empty else 1), added
        for event in completed:
            assert event["runtime_id"] == f"{service}_test.{user}"
            assert event["execution_user"] == user
            assert event["run_id"] and event["attempt_id"]
            assert event["schema_version"] == "1"
    return added


def absent(service, user, path):
    python(service, user, f"from pathlib import Path; assert not Path({path!r}).exists(), {path!r}")


def denied(service, user, path, remote=None):
    # Test real directory enumeration and file creation, rather than mode bits alone.
    code = (
        "from pathlib import Path\n"
        f"p=Path({path!r})\n"
        "for operation in (lambda: list(p.iterdir()), lambda: (p/'forbidden-probe').write_text('bad')):\n"
        " try:\n  operation()\n"
        " except PermissionError:\n  pass\n"
        " else:\n  raise AssertionError('Unexpected access: '+str(p))\n"
    )
    if remote:
        execute(service, user, "ssh", remote, "python -", input=code)
    else:
        python(service, user, code)


def isolation():
    print("Checking SSH and project access boundaries", flush=True)
    denied("cluster-a", "a_transfer", "/data/unrelated", "lab_transfer@lab")
    for project in PROJECTS:
        denied("cluster-a", "a_transfer", f"/data/projects/{project}")
        denied("cluster-a", "a_transfer", f"/data/projects/{project}", "b_receive@cluster-b")
    denied("cluster-a", "a_transfer", "/data/unrelated", "b_receive@cluster-b")
    for service in ("cluster-a", "cluster-b"):
        for project, other in (("alpha", "beta"), ("beta", "alpha")):
            denied(service, f"{project}_user", f"/data/projects/{other}")
            denied(service, f"{project}_user", "/data/intake")


def retained():
    # An entry-point attempt may archive the visible source before failing.
    python("cluster-a", "a_transfer", """
from pathlib import Path
import hashlib, tarfile
run = Path('/data/outbound/alpha/processed_alpha')
expected = ('alpha\\t' + hashlib.sha256(b'alpha: synthetic sequencing input\\n').hexdigest() + '\\n').encode()
if (run/'result.txt').exists():
    actual = (run/'result.txt').read_bytes()
else:
    with tarfile.open(run/'.landing_zones/landingzone-run-archive.tar') as archive:
        actual = archive.extractfile('./result.txt').read()
assert actual == expected
"""
    )


def sftp_smoke():
    output = Path(__file__).parent / "output"
    output.mkdir(exist_ok=True)
    report = output / "sftp-smoke.json"
    report.unlink(missing_ok=True)
    result = json.loads(node("cluster-a", "a_transfer", "sftp-smoke", capture=True).stdout)
    report.write_text(json.dumps(result, indent=2) + "\n")
    print("PASS: SFTP transport smoke, source retention, round-trip bytes and shell denial. "
          "This does not validate a Landing Zones SFTP delivery request.", flush=True)


def sftp_adapter():
    output = Path(__file__).parent / "output"
    output.mkdir(exist_ok=True)
    report = output / "sftp-adapter.json"
    report.unlink(missing_ok=True)
    result = json.loads(node("cluster-a", "a_transfer", "sftp-adapter", capture=True).stdout)
    report.write_text(json.dumps(result, indent=2) + "\n")
    print("PASS: product SFTP adapter, private staging, checksums, publication, "
          "destination conflict refusal and source retention. Executor not involved.", flush=True)


def permissions():
    isolation()
    print("PASS: account access boundaries. Ownership and mode changes during transfer "
          "are checked by the full legacy scenario.", flush=True)


def payload_snapshot(service, user, path, require_label=False):
    """Read actual bytes as the recipient account, independently of CLI status."""
    return json.loads(python(service, user, f"""
import json
from pathlib import Path
from landingzones.execution.adapters import manifest
from landingzones.execution.package import LABEL
root = Path({path!r})
result = {{'manifest': manifest(root)}}
if {require_label!r}:
    result['label'] = json.loads((root / LABEL).read_text())
print(json.dumps(result))
"""))


def verify_recipient(connection, payload_name, accepted, state):
    service, user, root = PYTHON_RECIPIENTS[connection]
    path = root + '/' + payload_name
    observed = payload_snapshot(service, user, path, require_label=True)
    assert observed['manifest'] == accepted, connection + ": recipient content mismatch"
    label = observed['label']
    assert label['schema_version'] == 1, connection + ": unsupported recipient label"
    assert label['manifest'] == accepted, connection + ": recipient label manifest mismatch"
    for field, state_field in (('package_id', 'payload_id'), ('transfer_run_id', 'run_id'),
                               ('content_version', 'payload_version')):
        assert label[field] == state[state_field], connection + ": recipient label identity mismatch"
    return dict(service=service, user=user, path=path, **observed)


def python_transfers(connection=None):
    selected = (connection,) if connection else PYTHON_CONNECTIONS
    if any(item not in PYTHON_CONNECTIONS for item in selected):
        raise ValueError("Unknown Python lab connection")
    scenario_id = uuid.uuid4().hex
    payload_name = "python-run-" + scenario_id
    service, user = "cluster-a", "a_transfer"
    output = Path(__file__).parent / "output"
    output.mkdir(exist_ok=True)
    report_name = "python-transfers" + ("-" + connection if connection else "")
    report = output / (report_name + ".json")
    report.unlink(missing_ok=True)
    archive = output / report_name / scenario_id
    archive.mkdir(parents=True)
    evidence = {'status': 'started', 'scenario_id': scenario_id, 'payload_name': payload_name,
                'connections': list(selected)}
    (archive / 'report.json').write_text(json.dumps(evidence, indent=2) + "\n")
    print(f"Python scenario {scenario_id}; evidence: {archive}", flush=True)

    def write_request(kind, value):
        path = f"/home/a_transfer/{kind}-request-{scenario_id}.json"
        execute(service, user, "python", "-c",
                f"import sys; from pathlib import Path; Path({path!r}).write_text(sys.stdin.read())",
                input=json.dumps(value))
        return path

    roots = []
    if any(item != "local_move" for item in selected):
        roots.append('/data/copy-input')
    if "local_move" in selected:
        roots.append('/data/user-output')
    python(service, user, f"""
from pathlib import Path
for root in {roots!r}:
    p = Path(root) / {payload_name!r}
    p.mkdir()
    (p / 'nested').mkdir()
    (p / 'nested/data.txt').write_text('Python executor fixture')
    (p / 'empty.txt').touch()
    (p / 'file with spaces.txt').write_text('spaces')
    (p / '.ready').touch()
""")
    # Capture accepted content before any move or executor metadata mutation.
    accepted = payload_snapshot(service, user, roots[0] + '/' + payload_name)['manifest']
    base = ["landingzones", "--config", "/home/a_transfer/runtime/execution.yaml", "transfer"]
    completed_connections = []
    recipient_checks = {}
    request_paths = {}
    for selected_connection in selected:
        if selected_connection not in ("local_copy", "rsync_copy"):
            continue
        request_path = write_request(selected_connection, dict(idempotency_key=selected_connection + scenario_id,
                                                     payload_name=payload_name, connection=selected_connection))
        value = json.loads(execute(service, user, *base, "run", "--request", request_path, capture=True).stdout)
        assert value['status'] == 'completed'
        recipient_checks[selected_connection] = verify_recipient(selected_connection, payload_name, accepted, value)
        completed_connections.append(value)
        request_paths[selected_connection] = request_path
    if "sftp_copy" in selected:
        request_path = write_request("copy", dict(idempotency_key="sftp-" + scenario_id,
                                              payload_name=payload_name, connection="sftp_copy"))
        compose("stop", "sftp-target")
        try:
            result = execute(service, user, *base, "run", "--request", request_path, capture=True, check=False)
            assert result.returncode == 1, result.stderr + result.stdout
            blocked = json.loads(result.stdout)
            assert blocked['status'] == 'blocked'
        finally:
            compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "120", "sftp-target")
        result = execute(service, user, *base, "resume", blocked['request_id'], capture=True)
        complete = json.loads(result.stdout)
        assert complete['status'] == 'completed'
        assert [a['phase'] for a in complete['deliveries'][0]['steps'][0]['attempts']] == ['transfer', 'transfer', 'promotion']
        repeated = json.loads(execute(service, user, *base, "run", "--request", request_path, capture=True).stdout)
        assert repeated == complete
        recipient_checks['sftp_copy'] = verify_recipient('sftp_copy', payload_name, accepted, complete)
        completed_connections.append(complete)
        evidence['copy'] = complete
        request_paths['copy'] = request_path
    if any(item != "local_move" for item in selected):
        retained = payload_snapshot(service, user, '/data/copy-input/' + payload_name)
        assert retained['manifest'] == accepted, "Copy changed source content"
    if "local_move" in selected:
        move = dict(idempotency_key="python-move-" + scenario_id, payload_name=payload_name,
                intake=dict(source_path=f"/data/user-output/{payload_name}", input_root="/data/move-input", operation="move"),
                deliveries=[{"flow_group": "local_move"}])
        move_path = write_request("move", move)
        moved = json.loads(execute(service, user, *base, "run", "--request", move_path, capture=True).stdout)
        assert moved['status'] == 'completed'
        absent(service, user, f"/data/user-output/{payload_name}")
        absent(service, user, f"/data/move-input/{payload_name}")
        recipient_checks['local_move'] = verify_recipient('local_move', payload_name, accepted, moved)
        completed_connections.append(moved)
        evidence['move'] = moved
        request_paths['move'] = move_path
    raw = execute(service, user, "cat", "/home/a_transfer/runtime/log/python.events.tsv", capture=True).stdout
    event_name = "python" + ("-" + connection if connection else "") + ".events.tsv"
    (output / event_name).write_text(raw)
    (archive / "events.tsv").write_text(raw)
    evidence.update(status='passed', completed_connections=completed_connections,
                    request_paths=request_paths, accepted_manifest=accepted,
                    recipient_checks=recipient_checks)
    result = json.dumps(evidence, indent=2) + "\n"
    (archive / 'report.json').write_text(result)
    report.write_text(result)
    print("PASS: Python CLI " + ", ".join(selected) + "; evidence: " + str(report), flush=True)


def run():
    # A failed or completed run is preserved; never silently mix fixture generations.
    python("lab", "producer", "from pathlib import Path; p=Path.home()/'.lab-run-started'; assert not p.exists(), 'Run reset and setup before another run'; p.touch()")
    isolation()
    sftp_smoke()
    sftp_adapter()
    python_transfers()
    node("lab", "producer", "seed")
    raw_ids = {}
    for project in PROJECTS:
        pulled = transfer("cluster-a", "a_transfer", f"pull_{project}")
        absent("lab", "producer", f"/data/export/{project}/raw_{project}")
        distributed = transfer("cluster-a", "a_distributor", f"distribute_{project}")
        raw_ids[project] = next(e["run_id"] for e in pulled if e["status"] == "completed")
        assert next(e["run_id"] for e in distributed if e["status"] == "completed") == raw_ids[project]
        absent("cluster-a", "a_distributor", f"/data/intake/{project}/raw_{project}")
    node("cluster-a", "processor", "preprocess")
    compose("stop", "cluster-b")
    try:
        transfer("cluster-a", "a_transfer", "push_alpha", failure=True)
        retained()
    finally:
        compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "120", "cluster-b")
    for project in PROJECTS:
        pushed = transfer("cluster-a", "a_transfer", f"push_{project}")
        processed_id = next(e["run_id"] for e in pushed if e["status"] == "completed")
        assert processed_id != raw_ids[project]
        absent("cluster-a", "a_transfer", f"/data/outbound/{project}/processed_{project}")
        distributed = transfer("cluster-b", "b_distributor", f"distribute_{project}")
        assert next(e["run_id"] for e in distributed if e["status"] == "completed") == processed_id
        absent("cluster-b", "b_distributor", f"/data/intake/{project}/processed_{project}")
        metadata = json.loads(node("cluster-b", f"{project}_user", "verify-project", project, capture=True).stdout)
        assert metadata["run_id"] == processed_id
        transfer("cluster-a", "a_transfer", f"push_{project}", empty=True)
        transfer("cluster-b", "b_distributor", f"distribute_{project}", empty=True)
        assert json.loads(node("cluster-b", f"{project}_user", "verify-project", project, capture=True).stdout) == metadata
    isolation()
    validate()
    print("PASS: full route, checksums, permissions, metadata, events, outage/retry, and duplicate checks.")
    print("Containers and logs retained. Use inspect, or reset to remove this lab.")


def validate():
    """Export real TSVs for review and check their final scenario history."""
    output = Path(__file__).parent / "output"
    output.mkdir(exist_ok=True)
    # Remove an earlier success summary before attempting a new validation.
    (output / "validation.json").unlink(missing_ok=True)
    summaries = {}
    event_ids = set()
    for service, user in RUNTIMES:
        spool = f"/home/{user}/runtime/log/monitor.transfers.tsv"
        raw = execute(service, user, "cat", spool, capture=True).stdout
        destination = output / f"{service}.{user}.transfers.tsv"
        destination.write_text(raw)
        summary = json.loads(node(service, user, "validate-events", user, capture=True).stdout)
        ids = set(summary.pop("event_ids"))
        if event_ids & ids:
            raise RuntimeError("Duplicate event IDs across runtime spools")
        event_ids.update(ids)
        summaries[user] = summary
        print(f"Validated {destination}: {summary['events']} events, {summary['failures']} expected failures", flush=True)
    for project in PROJECTS:
        raw = summaries["a_transfer"]["completed_runs"][f"pull_{project}"]
        processed = summaries["a_transfer"]["completed_runs"][f"push_{project}"]
        if raw != summaries["a_distributor"]["completed_runs"][f"distribute_{project}"]:
            raise RuntimeError(f"{project}: raw run identity changed between hops")
        if processed != summaries["b_distributor"]["completed_runs"][f"distribute_{project}"] or processed == raw:
            raise RuntimeError(f"{project}: processed run identity is inconsistent")
    (output / "validation.json").write_text(json.dumps({"status": "passed", "runtimes": summaries}, indent=2) + "\n")
    print(f"PASS: monitoring TSV validation. Review files in {output}", flush=True)


def inspect():
    compose("ps", "--all")
    compose("logs", "--tail", "30")
    for service, user in RUNTIMES:
        print(f"\n{service}/{user} transfer events:", flush=True)
        print(json.dumps(events(service, user), indent=2))
        execute(service, user, "sh", "-c", "find /data -printf '%M %u:%g %p\\n' 2>/dev/null || true")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("setup", "run", "inspect", "validate", "sftp-smoke", "sftp-adapter", "permissions", "python-transfers", "reset"))
    parser.add_argument("--connection", choices=PYTHON_CONNECTIONS,
                        help="Run one Python connection independently (python-transfers only)")
    args = parser.parse_args()
    if args.connection and args.action != "python-transfers":
        parser.error("--connection requires python-transfers")
    if not shutil.which("docker"):
        parser.exit(2, "Docker is unavailable. Install/start a Docker engine with Compose v2, then retry.\n")
    compose("version", capture=True)
    if args.action == "reset":
        compose("down", "--volumes", "--remove-orphans")
    elif args.action == "python-transfers":
        python_transfers(args.connection)
    else:
        globals()[args.action.replace("-", "_")]()


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, RuntimeError, subprocess.CalledProcessError) as exc:
        if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
            print(exc.stderr, file=sys.stderr, end="" if exc.stderr.endswith("\n") else "\n")
        print(f"FAIL: {exc}\nState retained; use inspect. See the lab README for retry and reset options.", file=sys.stderr)
        sys.exit(1)
