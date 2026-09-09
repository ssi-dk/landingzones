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

PROJECTS = ("alpha", "beta")
ROLES = {
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


def routes(role, user):
    rows = []
    for project in PROJECTS:
        if user == "a_transfer":
            specs = [
                ("pull", f"lab_transfer@lab:/data/export/{project}/*", f"/data/intake/{project}/", True, False, "raw"),
                ("push", f"/data/outbound/{project}/*", f"b_receive@cluster-b:/data/intake/{project}/", True, False, "processed"),
            ]
        elif user == "a_distributor":
            specs = [("distribute", f"/data/intake/{project}/*", f"/data/projects/{project}/", False, True, "raw")]
        elif user == "b_distributor":
            specs = [("distribute", f"/data/intake/{project}/*", f"/data/projects/{project}/", False, True, "processed")]
        else:
            continue
        for step, source, destination, entry, end, flow in specs:
            rows.append(dict(
                identifiers=f"{step}_{project}", runtime_id=f"{role}_test.{user}",
                system=role, users=user, enabled="TRUE", source=source,
                destination=destination, rsync_options="--no-owner --no-group --chmod=D2770,F660",
                log_file=f"{step}_{project}.log", flock_file=f"{step}_{project}.lock",
                frequency="0 * * * *", flow_group=f"{flow}_{project}",
                is_entry_point=str(entry).upper(), is_end_point=str(end).upper(),
                notify_on_success="FALSE", notify_on_error="FALSE",
            ))
    return rows


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
        else:
            distributor = "a_distributor" if role == "cluster-a" else "b_distributor"
            receiver = "a_transfer" if role == "cluster-a" else "b_receive"
            directory("/data/intake", distributor, "intake")
            Path("/data/projects").mkdir()
            Path("/data/projects").chmod(0o755)
            for project in PROJECTS:
                directory(f"/data/intake/{project}", receiver, "intake")
                directory(f"/data/projects/{project}", distributor, project)
            if role == "cluster-a":
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
        "X11Forwarding no\nPermitTunnel no\n"
        "AllowUsers lab_transfer b_receive\n"
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
