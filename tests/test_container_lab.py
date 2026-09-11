"""Check the disposable lab's real CLI configuration without requiring Docker."""
import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest
import yaml


LAB = Path(__file__).parent / "container_lab"
spec = importlib.util.spec_from_file_location("container_lab_node", LAB / "node.py")
node = importlib.util.module_from_spec(spec)
spec.loader.exec_module(node)


@pytest.mark.parametrize("role,user,count", [
    ("cluster-a", "a_transfer", 4),
    ("cluster-a", "a_distributor", 2),
    ("cluster-b", "b_distributor", 2),
])
def test_real_cli_builds_lab_runtimes(tmp_path, role, user, count):
    node.write_config(role, user, tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "landingzones.cli", "--config", str(tmp_path / "config.yaml"), "build"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    scripts = list((tmp_path / "output/scripts").glob("*.sh"))
    assert len(scripts) == count, result.stdout
    for row in node.routes(role, user):
        path = tmp_path / "output/scripts" / (row["identifiers"] + ".sh")
        content = path.read_text()
        assert str(tmp_path / "log") in content
        assert str(tmp_path / "flock") in content
        assert row["runtime_id"] in content
        subprocess.run(["sh", "-n", str(path)], check=True)


def test_compose_uses_one_image_and_separate_filesystems():
    compose = yaml.safe_load((LAB / "compose.yaml").read_text())
    services = compose["services"]
    assert set(services) == {"lab", "cluster-a", "cluster-b", "sftp-target"}
    assert len({s["image"] for s in services.values()}) == 1
    assert compose["networks"]["lab"]["internal"] is True
    for service in services.values():
        assert "volumes" not in service
        assert "ports" not in service
        assert not service.get("privileged", False)


def test_lab_driver_parses():
    result = subprocess.run([sys.executable, str(LAB / "lab.py"), "--help"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "setup,run,inspect,validate,sftp-smoke,python-transfers,reset" in result.stdout


def write_event_fixture(root, role="cluster-a", user="a_transfer"):
    from landingzones.transfer_events import (
        EVENT_HEADER, create_transfer_event, event_to_tsv_row, new_identifier,
    )
    rows = []
    for route in node.routes(role, user):
        run_id = new_identifier()
        common = dict(
            transfer_identifier=route["identifiers"], system=role,
            runtime_id=route["runtime_id"], execution_user=user,
            flow_group=route["flow_group"], run_id=run_id,
        )
        if route["identifiers"] == "push_alpha":
            rows.append(create_transfer_event(
                **common, status="failed", phase="transfer", exit_code=255,
                reason_code="ssh_failed", attempt_id=new_identifier(),
            ))
        rows.append(create_transfer_event(
            **common, status="completed", phase="cleanup", attempt_id=new_identifier(),
        ))
    path = root / "log/monitor.transfers.tsv"
    path.parent.mkdir()
    path.write_text(EVENT_HEADER + "\n" + "\n".join(event_to_tsv_row(row) for row in rows) + "\n")
    return path


@pytest.mark.parametrize("role,user", [
    ("cluster-a", "a_transfer"), ("cluster-a", "a_distributor"),
    ("cluster-b", "b_distributor"),
])
def test_monitoring_tsv_validates_completed_scenario(tmp_path, role, user):
    write_event_fixture(tmp_path, role, user)
    result = node.validate_events(tmp_path, role, user)
    assert result["failures"] == (1 if user == "a_transfer" else 0)
    assert len(result["completed_runs"]) == len(node.routes(role, user))


@pytest.mark.parametrize("damage,message", [
    ("header", "header"), ("truncated", "columns"),
    ("duplicate", "duplicate event_id"), ("missing", "completion"),
    ("identity", "runtime/user/flow"), ("outage", "outage failure"),
    ("retry", "retry reused attempt_id"),
])
def test_monitoring_tsv_rejects_broken_history(tmp_path, damage, message):
    from landingzones.transfer_events import EVENT_COLUMNS
    path = write_event_fixture(tmp_path)
    lines = path.read_text().splitlines()
    if damage == "header":
        lines[0] = "wrong\theader"
    elif damage == "truncated":
        lines[-1] = lines[-1].rsplit("\t", 1)[0]
    elif damage == "duplicate":
        lines.append(lines[-1])
    elif damage == "missing":
        lines.pop()
    elif damage == "identity":
        lines[-1] = lines[-1].replace("cluster-a_test.a_transfer", "wrong-runtime")
    elif damage == "outage":
        lines = [line for line in lines if "\tfailed\t" not in line]
    elif damage == "retry":
        failure = next(line.split("\t") for line in lines if "\tfailed\t" in line)
        for index, line in enumerate(lines):
            if "\tpush_alpha\t" in line and "\tcompleted\t" in line:
                row = line.split("\t")
                row[EVENT_COLUMNS.index("attempt_id")] = failure[EVENT_COLUMNS.index("attempt_id")]
                lines[index] = "\t".join(row)
    path.write_text("\n".join(lines) + "\n")
    with pytest.raises(ValueError, match=message):
        node.validate_events(tmp_path, "cluster-a", "a_transfer")


@pytest.mark.parametrize("phase,run_id,flow,valid", [
    ("discovery", "", "", True),
    ("discovery", "", "wrong-flow", False),
    ("transfer", "", "", False),
])
def test_route_outage_flow_validation(tmp_path, phase, run_id, flow, valid):
    from landingzones.transfer_events import EVENT_COLUMNS
    path = write_event_fixture(tmp_path)
    lines = path.read_text().splitlines()
    for index, line in enumerate(lines):
        if "\tfailed\t" in line:
            row = line.split("\t")
            for field, value in dict(phase=phase, run_id=run_id, attempt_id="", flow_group=flow).items():
                row[EVENT_COLUMNS.index(field)] = value
            lines[index] = "\t".join(row)
    path.write_text("\n".join(lines) + "\n")
    if valid:
        assert node.validate_events(tmp_path, "cluster-a", "a_transfer")["failures"] == 1
    else:
        with pytest.raises(ValueError):
            node.validate_events(tmp_path, "cluster-a", "a_transfer")


def test_catalog_separates_legacy_and_python_owners():
    rows = node.catalog()
    legacy = [row for row in rows if row["executor"] == "legacy"]
    assert len(legacy) == 8
    assert all(row["operation"] == "move" for row in legacy)
    python_rows = [row for row in rows if row["executor"] == "python"]
    assert len(python_rows) == 4
    assert {row["adapter"] for row in python_rows} == {"local", "sftp", "rsync"}
    assert all(row["enabled"] == "TRUE" for row in python_rows)


def test_catalog_never_silently_executes_legacy_copy_as_move(tmp_path):
    import csv
    rows = node.catalog()
    rows[0]["operation"] = "copy"
    path = tmp_path / "transfers.tsv"
    with path.open("w") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    with pytest.raises(ValueError, match="Legacy route"):
        node.catalog(path)


def test_legacy_build_from_shared_table_excludes_python_owned_routes(tmp_path):
    node.write_config('cluster-a', 'a_transfer', tmp_path)
    result = subprocess.run(
        [sys.executable, '-m', 'landingzones.cli', '--config', str(tmp_path / 'config.yaml'),
         'build', '--transfers', str(LAB / 'transfers.tsv'), '--runtime-id', 'cluster-a_test.a_transfer'],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert {p.stem for p in (tmp_path / 'output/scripts').glob('*.sh')} == {
        'pull_alpha', 'pull_beta', 'push_alpha', 'push_beta'}
