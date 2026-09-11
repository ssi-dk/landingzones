"""Container initialization and non-root fixture/assertion operations."""
import csv
import grp
import hashlib
import json
import os
from pathlib import Path
import pwd
import socket
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit

PROJECTS = ("alpha", "beta")
ROLES = {
    "sftp-target": {"upload": ["upload"]},
    "lab": {"producer": ["export"], "lab_transfer": ["export"]},
    "cluster-a": {
        "a_transfer": ["intake", "outbound"],
        "a_distributor": ["intake", "alpha", "beta"],
        "processor": ["alpha", "beta", "outbound"],
        "alpha_user": ["alpha"], "beta_user": ["beta"],
    },
    "cluster-b": {
        "b_receive": ["intake"],
        "b_distributor": ["intake", "alpha", "beta"],
        "alpha_user": ["alpha"], "beta_user": ["beta"],
    },
}


def command(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def directory(path, owner, group, mode=0o2770):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    os.chown(path, pwd.getpwnam(owner).pw_uid, grp.getgrnam(group).gr_gid)
    path.chmod(mode)


def catalog(path=None):
    """Single maintained lab table, including explicitly disabled future cases."""
    with Path(path or Path(__file__).with_name("transfers.tsv")).open() as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows = list(reader)
    seen = set()
    for row in rows:
        if None in row or any(value is None for value in row.values()):
            raise ValueError("Malformed lab transfer row")
        identity = (row["runtime_id"], row["identifiers"])
        if identity in seen:
            raise ValueError("Duplicate lab transfer identity")
        seen.add(identity)
        if row["enabled"] not in ("TRUE", "FALSE"):
            raise ValueError("Invalid enabled value")
        if row["executor"] not in ("legacy", "python"):
            raise ValueError("Unknown lab executor")
        if row["enabled"] == "TRUE" and (
            row["executor"] == "legacy" and (row["operation"] != "move"
            or row["adapter"] not in ("local", "rsync"))
        ):
            raise ValueError("Legacy route cannot implement this adapter or operation")
    return rows


def routes(role, user):
    return [
        {key: value for key, value in row.items()
         if key not in ("adapter", "operation", "step_order", "executor")}
        for row in catalog()
        if row["system"] == role and row["users"] == user
        and row["enabled"] == "TRUE" and row["executor"] == "legacy"
    ]


def write_config(role, user, root):
    """Also usable outside Docker for catalog/build validation."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    rows = routes(role, user)
    with (root / "transfers.tsv").open("w") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    config = {
        "transfers_file": str(root / "transfers.tsv"),
        "output_dir": str(root / "output"), "log_dir": str(root / "log"),
        "report_transfer_log_file": str(root / "log/monitor.transfers.tsv"),
        "crontab_dir": str(root / "output/crontab.d"),
        "validation_scripts_dir": str(root / "output/validation_scripts"),
        "rit_managed_locations": {role: str(root)},
        "rit_managed_folder_structure": {"sh_output": "output/scripts", "crontabs": "output/crontab.d", "log": "log", "flock": "flock"},
        "flock_paths": {role: "/usr/bin/flock"},
    }
    # JSON is a YAML subset; avoid a dependency for host-side inspection.
    (root / "config.yaml").write_text(json.dumps(config, indent=2))
    if user == "a_transfer":
        settings = {
            "execution_schema_version": 1, "transfers_file": str(Path(__file__).with_name("transfers.tsv")),
            "runtime_ids": [f"{role}_test.{user}"],
            "execution_context": {"system": role, "user": user},
            "state_dir": str(root / "python-state"), "event_spool": str(root / "log/python.events.tsv"),
            "credentials": {"partner_sftp": {
                "private_key_file": f"/home/{user}/.ssh/id_ed25519",
                "known_hosts_file": f"/home/{user}/.ssh/known_hosts",
            }},
        }
        settings["credentials"]["internal_ssh"] = dict(settings["credentials"]["partner_sftp"])
        (root / "execution.yaml").write_text(json.dumps(settings, indent=2))


def boot():
    role = os.environ["LAB_ROLE"]
    if not Path("/var/lib/lab-initialized").exists():
        groups = sorted({g for memberships in ROLES[role].values() for g in memberships})
        for group in groups + ["unrelated"]:
            command("groupadd", group)
        for user, memberships in ROLES[role].items():
            command("useradd", "-m", "-s", "/bin/sh", "-g", memberships[0], "-G", ",".join(memberships), user)
            # Unlock account for public-key SSH; password authentication is disabled.
            command("usermod", "-p", "x", user)
        Path("/data").mkdir()
        Path("/data").chmod(0o755)
        directory("/data/unrelated", "root", "unrelated")
        if role == "lab":
            directory("/data/export", "producer", "export")
            for project in PROJECTS:
                directory(f"/data/export/{project}", "producer", "export")
        elif role == "sftp-target":
            directory("/srv/sftp", "root", "root", mode=0o755)
            directory("/srv/sftp/incoming", "upload", "upload")
            directory("/srv/sftp/.landingzones-staging-incoming", "upload", "upload", 0o700)
        else:
            distributor = "a_distributor" if role == "cluster-a" else "b_distributor"
            receiver = "a_transfer" if role == "cluster-a" else "b_receive"
            directory("/data/intake", distributor, "intake")
            Path("/data/projects").mkdir()
            Path("/data/projects").chmod(0o755)
            for project in PROJECTS:
                directory(f"/data/intake/{project}", receiver, "intake")
                directory(f"/data/projects/{project}", distributor, project)
            if role == "cluster-b":
                directory("/data/intake/python", "b_receive", "intake")
                directory("/data/intake/.landingzones-staging-python", "b_receive", "intake", 0o700)
            if role == "cluster-a":
                directory("/data/copy-input", "a_transfer", "intake")
                directory("/data/copy-output", "a_transfer", "intake")
                for target in ("copy-output", "move-input", "move-output"):
                    directory("/data/.landingzones-staging-" + target, "a_transfer", "intake", 0o700)
                for path in ("move-input", "move-output", "rsync-output", "user-output"):
                    directory("/data/" + path, "a_transfer", "intake")
                directory("/data/outbound", "processor", "outbound")
                for project in PROJECTS:
                    directory(f"/data/outbound/{project}", "processor", "outbound")
        for user in ROLES[role]:
            if routes(role, user):
                root = Path(f"/home/{user}/runtime")
                write_config(role, user, root)
                command("chown", "-R", f"{user}:{ROLES[role][user][0]}", str(root))
                command("runuser", "-u", user, "--", "landingzones", "--config", str(root / "config.yaml"), "build")
        Path("/var/lib/lab-initialized").touch()
    # The fixture umask permits group writes; sshd requires a protected directory.
    directory("/run/sshd", "root", "root", mode=0o755)
    command("ssh-keygen", "-A")
    Path("/etc/ssh/ssh_host_ed25519_key.pub").chmod(0o644)
    Path("/etc/ssh/sshd_config").write_text(
        "Port 22\nHostKey /etc/ssh/ssh_host_ed25519_key\n"
        "PasswordAuthentication no\nKbdInteractiveAuthentication no\n"
        "PermitRootLogin no\nUsePAM no\nAllowTcpForwarding no\n"
        "X11Forwarding no\nPermitTunnel no\nSubsystem sftp internal-sftp\n"
        + ("AllowUsers upload\n"
         "Match User upload\nChrootDirectory /srv/sftp\n"
         "ForceCommand internal-sftp\nPermitTTY no\n"
         if role == "sftp-target" else "AllowUsers lab_transfer b_receive\n")
    )
    os.execv("/usr/sbin/sshd", ["/usr/sbin/sshd", "-D", "-e"])


def credentials(user):
    ssh = Path.home() / ".ssh"
    ssh.mkdir(mode=0o700, exist_ok=True)
    ssh.chmod(0o700)
    if user == "a_transfer":
        command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(ssh / "id_ed25519"))
        (ssh / "config").write_text("Host *\n  BatchMode yes\n  StrictHostKeyChecking yes\n  ConnectTimeout 3\n  ConnectionAttempts 1\n")
        (ssh / "config").chmod(0o600)
    else:
        (ssh / "authorized_keys").write_text("restrict " + sys.stdin.read().strip() + "\n")
        (ssh / "authorized_keys").chmod(0o600)


def sftp_smoke():
    """Exercise endpoint transport only; not the future product delivery API."""
    candidates = [row for row in catalog() if row["adapter"] == "sftp"]
    if len(candidates) != 1:
        raise ValueError("SFTP smoke requires exactly one configured SFTP fixture")
    row = candidates[0]
    if row["operation"] != "copy":
        raise ValueError("SFTP smoke supports copy fixtures only")
    target = urlsplit(row["destination"])
    if target.scheme != "sftp" or not target.hostname or not target.username:
        raise ValueError("Invalid SFTP fixture destination")
    if target.password or any(c in row["destination"] for c in ('"', "\n", "\r")):
        raise ValueError("Invalid SFTP fixture destination")
    source = Path(tempfile.mkdtemp(prefix="smoke_", dir=row["source"]))
    (source / "nested").mkdir()
    (source / "nested/data.txt").write_text("synthetic SFTP payload\n")
    (source / "empty.txt").touch()
    (source / "file with spaces.txt").write_text("spaces preserved\n")

    def inventory(root):
        return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in root.rglob("*") if path.is_file()}

    before = inventory(source)
    remote = target.path.rstrip("/") + "/" + source.name
    destination = f"{target.username}@{target.hostname}"
    base = ["sftp", "-P", str(target.port or 22), "-b", "-", destination]
    with tempfile.TemporaryDirectory(prefix="lz-sftp-download-") as download:
        batch = f'put -r "{source}" "{remote}"\nget -r "{remote}" "{download}/received"\n'
        subprocess.run(base, input=batch, check=True, text=True, capture_output=True, timeout=30)
        assert inventory(Path(download) / "received") == before, "SFTP round-trip mismatch"
    assert inventory(source) == before, "SFTP copy altered source"
    shell = subprocess.run(
        ["ssh", "-p", str(target.port or 22), destination, "true"],
        capture_output=True, text=True, timeout=10,
    )
    assert shell.returncode != 0, "SFTP account unexpectedly allows shell commands"
    print(json.dumps({"status": "passed", "scope": "transport-smoke-only",
                      "transfer_identifier": row["identifiers"], "source": str(source),
                      "destination": remote, "file_checksums": before,
                      "source_retained": True, "shell_denied": True}))


def seed():
    for project in PROJECTS:
        run = Path(f"/data/export/{project}/raw_{project}")
        run.mkdir()
        (run / "nested").mkdir()
        (run / "nested/input.txt").write_text(f"{project}: synthetic sequencing input\n")
        (run / "empty.txt").touch()
        (run / "file with spaces.txt").write_text("fixture\n")


def preprocess():
    for project in PROJECTS:
        source = Path(f"/data/projects/{project}/raw_{project}")
        assert (source / "nested/input.txt").read_text() == f"{project}: synthetic sequencing input\n"
        assert (source / "empty.txt").stat().st_size == 0
        assert (source / "file with spaces.txt").read_text() == "fixture\n"
        assert (source / ".landing_zones/landingzone-run-metadata.tsv").is_file()
        assert not (source / ".landing_zones/landingzone-run-archive.tar").exists()
        destination = Path(f"/data/outbound/{project}/processed_{project}")
        destination.mkdir()
        digest = hashlib.sha256((source / "nested/input.txt").read_bytes()).hexdigest()
        (destination / "result.txt").write_text(f"{project}\t{digest}\n")


def events():
    result = []
    for path in (Path.home() / "runtime/log").glob("*.transfers.tsv"):
        with path.open() as handle:
            result.extend(csv.DictReader(handle, delimiter="\t"))
    print(json.dumps(result))


def validate_events(root, role, user):
    """Validate the real spool against the completed lab scenario."""
    from landingzones.transfer_events import EVENT_COLUMNS, event_from_tsv_row

    path = Path(root) / "log/monitor.transfers.tsv"
    expected = {row["identifiers"]: row for row in routes(role, user)}
    if not expected:
        raise ValueError(f"No expected routes for {role}/{user}")
    with path.open(newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        if next(reader, None) != list(EVENT_COLUMNS):
            raise ValueError(f"{path}: invalid Transfer Event header")
        rows = []
        seen = set()
        for number, row in enumerate(reader, 2):
            try:
                event = event_from_tsv_row(row)
                if event.event_id in seen:
                    raise ValueError("duplicate event_id")
                seen.add(event.event_id)
                route = expected.get(event.transfer_identifier)
                if route is None:
                    raise ValueError("unexpected transfer_identifier")
                route_failure = (
                    event.status == "failed" and event.phase == "discovery"
                    and not event.run_id and not event.attempt_id
                )
                if (event.system, event.runtime_id, event.execution_user) != (
                    role, route["runtime_id"], user
                ) or (event.flow_group != route["flow_group"] and not (
                    route_failure and not event.flow_group
                )):
                    raise ValueError("event does not match the expected runtime/user/flow")
                rows.append(event)
            except ValueError as exc:
                raise ValueError(f"{path}:{number}: {exc}") from exc
    completed = {}
    for identifier in expected:
        matches = [event for event in rows if event.transfer_identifier == identifier and event.status == "completed"]
        if len(matches) != 1 or matches[0].exit_code not in (None, 0):
            raise ValueError(f"{path}: expected exactly one successful completion for {identifier}")
        completed[identifier] = matches[0]
    failures = [event for event in rows if event.status == "failed"]
    if user == "a_transfer":
        if not failures or any(event.transfer_identifier != "push_alpha" or event.exit_code in (None, 0) for event in failures):
            raise ValueError(f"{path}: expected only the push_alpha outage failure")
        retry = completed["push_alpha"]
        for failure in failures:
            if rows.index(failure) >= rows.index(retry):
                raise ValueError(f"{path}: outage must precede successful retry")
            if failure.run_id and failure.run_id != retry.run_id:
                raise ValueError(f"{path}: retry changed run_id")
            if failure.attempt_id and failure.attempt_id == retry.attempt_id:
                raise ValueError(f"{path}: retry reused attempt_id")
    elif failures:
        raise ValueError(f"{path}: unexpected failure")
    return {
        "path": str(path), "events": len(rows), "failures": len(failures),
        "event_ids": sorted(seen),
        "completed_runs": {name: event.run_id for name, event in completed.items()},
    }


def verify_project(project):
    root = Path(f"/data/projects/{project}")
    assert sorted(p.name for p in root.iterdir() if not p.name.startswith(".")) == [f"processed_{project}"]
    run = root / f"processed_{project}"
    digest = hashlib.sha256((project + ": synthetic sequencing input\n").encode()).hexdigest()
    expected = f"{project}\t{digest}\n"
    assert (run / "result.txt").read_text() == expected
    metadata = dict(line.split("\t", 1) for line in (run / ".landing_zones/landingzone-run-metadata.tsv").read_text().splitlines())
    assert metadata["flow_group"] == f"processed_{project}"
    assert metadata["run_id"]
    assert not (run / ".landing_zones/landingzone-run-archive.tar").exists()
    for path in [run, run / "result.txt"]:
        stat = path.stat()
        assert stat.st_uid == pwd.getpwnam("b_distributor").pw_uid
        assert stat.st_gid == grp.getgrnam(project).gr_gid
        assert stat.st_mode & 0o007 == 0
    print(json.dumps(metadata))


if __name__ == "__main__":
    os.umask(0o007)
    action = sys.argv[1]
    if action == "boot":
        boot()
    elif action == "health":
        with socket.create_connection(("127.0.0.1", 22), timeout=2) as connection:
            assert connection.recv(100).startswith(b"SSH-")
    elif action == "credentials":
        credentials(sys.argv[2])
    elif action == "sftp-smoke":
        sftp_smoke()
    elif action == "seed":
        seed()
    elif action == "preprocess":
        preprocess()
    elif action == "events":
        events()
    elif action == "validate-events":
        print(json.dumps(validate_events(Path.home() / "runtime", os.environ["LAB_ROLE"], sys.argv[2])))
    elif action == "verify-project":
        verify_project(sys.argv[2])
    else:
        raise SystemExit(f"Unknown node action: {action}")
