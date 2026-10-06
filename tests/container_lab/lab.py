#!/usr/bin/env python3
"""Host driver. Requires only Python 3 and Docker Compose v2."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys

from node import PROJECTS

COMPOSE = ["docker", "compose", "--project-name", "landingzones-container-lab", "--file", str(Path(__file__).with_name("compose.yaml"))]
RUNTIMES = (("cluster-a", "a_transfer"), ("cluster-a", "a_distributor"), ("cluster-b", "b_distributor"))


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
    for service, user in (("lab", "lab_transfer"), ("cluster-b", "b_receive")):
        node(service, user, "credentials", user, input=public)
        # Read the actual public host key through the local Docker control plane.
        key = execute(service, user, "cat", "/etc/ssh/ssh_host_ed25519_key.pub", capture=True).stdout.split()
        known_hosts.append(f"{service} {key[0]} {key[1]}\n")
    execute("cluster-a", "a_transfer", "sh", "-c", "cat > ~/.ssh/known_hosts", input="".join(known_hosts))
    for host in ("lab_transfer@lab", "b_receive@cluster-b"):
        execute("cluster-a", "a_transfer", "ssh", host, "true")
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


def run():
    # A failed or completed run is preserved; never silently mix fixture generations.
    python("lab", "producer", "from pathlib import Path; p=Path.home()/'.lab-run-started'; assert not p.exists(), 'Run reset and setup before another run'; p.touch()")
    isolation()
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
    parser.add_argument("action", choices=("setup", "run", "inspect", "validate", "reset"))
    args = parser.parse_args()
    if not shutil.which("docker"):
        parser.exit(2, "Docker is unavailable. Install/start a Docker engine with Compose v2, then retry.\n")
    compose("version", capture=True)
    if args.action == "reset":
        compose("down", "--volumes", "--remove-orphans")
    else:
        globals()[args.action]()


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, RuntimeError, subprocess.CalledProcessError) as exc:
        if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
            print(exc.stderr, file=sys.stderr, end="" if exc.stderr.endswith("\n") else "\n")
        print(f"FAIL: {exc}\nState retained; use inspect. Reset and setup before a fresh run.", file=sys.stderr)
        sys.exit(1)
