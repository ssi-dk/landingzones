"""Check the disposable lab's real CLI configuration without requiring Docker."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

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
    assert "sftp-adapter" in result.stdout
    assert "permissions" in result.stdout
    assert "--connection" in result.stdout


@pytest.fixture
def driver(monkeypatch, tmp_path):
    monkeypatch.setitem(sys.modules, "node", node)
    spec = importlib.util.spec_from_file_location("container_lab_driver", LAB / "lab.py")
    driver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)
    monkeypatch.setattr(driver, "__file__", str(tmp_path / "lab.py"))
    return driver


def test_connection_option_rejected_outside_python_scenario():
    result = subprocess.run(
        [sys.executable, str(LAB / "lab.py"), "run", "--connection", "sftp_copy"],
        capture_output=True, text=True,
    )
    assert result.returncode == 2
    assert "--connection requires python-transfers" in result.stderr


@pytest.mark.parametrize("connection", [None, "local_copy", "rsync_copy", "sftp_copy", "local_move"])
@pytest.mark.parametrize("corrupt_recipient", [False, True])
def test_python_scenarios_select_only_requested_connection(driver, monkeypatch, tmp_path, connection, corrupt_recipient):
    """No Docker: assert the host driver selects the intended product CLI calls."""
    selected = [connection] if connection else list(driver.PYTHON_CONNECTIONS)
    requested, runs, changes, code = [], [], [], []
    snapshots = []
    stopped = False

    def compose(*args, **kwargs):
        nonlocal stopped
        changes.append(args)
        stopped = args[0] == "stop"

    def execute(service, user, *args, **kwargs):
        if "input" in kwargs:
            requested.append(json.loads(kwargs['input']))
            return SimpleNamespace(stdout='', returncode=0)
        if args[0] == "cat":
            return SimpleNamespace(stdout="events\n", returncode=0)
        assert args[0] == "landingzones"
        request = requested[-1]
        kind = request.get('connection') or request['deliveries'][0]['flow_group']
        runs.append(kind)
        status = "blocked" if stopped else "completed"
        value = {'status': status, 'request_id': kind + '-request',
                 'payload_id': 'package-id', 'run_id': 'run-id', 'payload_version': 'version',
                 'deliveries': [{'steps': [{'attempts': [
                     {'phase': phase} for phase in ('transfer', 'transfer', 'promotion')]}]}]}
        return SimpleNamespace(stdout=json.dumps(value), stderr='', returncode=int(stopped))

    def snapshot(service, user, path, require_label=False):
        snapshots.append((service, user, path, require_label))
        contents = {'nested': {'kind': 'directory'}}
        result = {'manifest': contents}
        if require_label:
            result['label'] = dict(schema_version=1, package_id='package-id', transfer_run_id='run-id',
                                   content_version='version', manifest=contents)
            if corrupt_recipient:
                result['manifest'] = {}
        return result

    monkeypatch.setattr(driver, "compose", compose)
    monkeypatch.setattr(driver, "execute", execute)
    monkeypatch.setattr(driver, "python", lambda service, user, source: code.append(source))
    monkeypatch.setattr(driver, "payload_snapshot", snapshot)
    report_name = "python-transfers" + ("-" + connection if connection else "")
    report_path = tmp_path / 'output' / (report_name + '.json')
    if corrupt_recipient:
        with pytest.raises(AssertionError, match="recipient content mismatch"):
            driver.python_transfers(connection)
        assert not report_path.exists()
        records = list((tmp_path / 'output' / report_name).glob('*/report.json'))
        assert len(records) == 1
        assert json.loads(records[0].read_text())['status'] == 'started'
        return
    driver.python_transfers(connection)
    assert [item.get('connection') or item['deliveries'][0]['flow_group'] for item in requested] == selected
    assert set(runs) == set(selected)
    assert bool(changes) == ("sftp_copy" in selected)
    if changes:
        assert changes[0] == ("stop", "sftp-target")
        assert changes[-1][-1] == "sftp-target"
        assert changes[-1][0] == "up"
    if connection == 'local_move':
        assert '/data/copy-input' not in code[0]
    elif connection:
        assert '/data/user-output' not in code[0]
    report = json.loads(report_path.read_text())
    assert report['status'] == 'passed'
    assert report['connections'] == selected
    assert len(report['completed_connections']) == len(selected)
    assert set(report['recipient_checks']) == set(selected)
    recipient_reads = [item for item in snapshots if item[3]]
    assert len(recipient_reads) == len(selected)
    for kind, (service, user, path, _) in zip(selected, recipient_reads):
        target_service, target_user, target_root = driver.PYTHON_RECIPIENTS[kind]
        assert (service, user) == (target_service, target_user)
        assert path == target_root + '/' + report['payload_name']
    # Initial acceptance precedes both executor calls and recipient verification.
    assert snapshots[0][0:2] == ('cluster-a', 'a_transfer')
    assert snapshots[0][3] is False


def test_python_scenario_failure_removes_earlier_pass(driver, monkeypatch, tmp_path):
    output = tmp_path / 'output'
    output.mkdir()
    report = output / 'python-transfers-local_copy.json'
    report.write_text('{"status":"passed"}')
    monkeypatch.setattr(driver, "python", lambda *args: None)
    monkeypatch.setattr(driver, "payload_snapshot", lambda *args: {'manifest': {}})

    def failure(*args, **kwargs):
        raise RuntimeError("fixture transfer failed")

    monkeypatch.setattr(driver, "execute", failure)
    with pytest.raises(RuntimeError, match="fixture transfer failed"):
        driver.python_transfers('local_copy')
    assert not report.exists()
    started = list((output / 'python-transfers-local_copy').glob('*/report.json'))
    assert len(started) == 1
    assert json.loads(started[0].read_text())['status'] == 'started'


def test_recipient_snapshot_reads_complete_manifest_and_portable_label(driver, monkeypatch, tmp_path):
    import contextlib
    import hashlib
    import io
    from landingzones.execution.adapters import manifest
    from landingzones.execution.package import LABEL, admit

    payload = tmp_path / 'payload'
    (payload / 'nested').mkdir(parents=True)
    (payload / 'nested/data.txt').write_bytes(b'recipient bytes')
    (payload / 'empty.txt').touch()
    (payload / 'file with spaces.txt').write_bytes(b'spaces')
    (payload / '.ready').touch()
    label = admit(manifest(payload))
    (payload / LABEL).write_text(json.dumps(label))

    def local_python(service, user, source):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exec(source, {})
        return output.getvalue()

    monkeypatch.setattr(driver, 'python', local_python)
    snapshot = driver.payload_snapshot('unused', 'unused', str(payload), require_label=True)
    assert snapshot['label'] == label
    assert set(snapshot['manifest']) == {'nested', 'nested/data.txt', 'empty.txt', 'file with spaces.txt'}
    assert snapshot['manifest']['nested/data.txt']['sha256'] == hashlib.sha256(b'recipient bytes').hexdigest()
    assert snapshot['manifest']['empty.txt']['size'] == 0


@pytest.mark.parametrize('field', ['schema_version', 'manifest', 'package_id', 'transfer_run_id', 'content_version'])
def test_recipient_verification_rejects_wrong_portable_label(driver, monkeypatch, field):
    accepted = {'empty.txt': {'kind': 'file', 'size': 0, 'sha256': 'checksum'}}
    label = dict(schema_version=1, manifest=accepted, package_id='package',
                 transfer_run_id='run', content_version='version')
    label[field] = 'wrong'
    monkeypatch.setattr(driver, 'payload_snapshot', lambda *args, **kwargs: {'manifest': accepted, 'label': label})
    with pytest.raises(AssertionError, match='recipient label'):
        driver.verify_recipient('sftp_copy', 'payload', accepted,
                                dict(payload_id='package', run_id='run', payload_version='version'))


def test_sftp_adapter_probe_uses_adapter_without_executor(monkeypatch, tmp_path, capsys):
    """Exercise probe assertions against a filesystem-backed adapter double."""
    from landingzones.execution import adapters, model

    source = tmp_path / 'source'
    destination = tmp_path / 'incoming'
    source.mkdir()
    destination.mkdir()
    step = SimpleNamespace(source=str(source), destination=str(destination),
                           adapter='sftp', operation='copy', identifiers='sftp_copy')
    calls = []

    class FilesystemTransport(adapters.LocalAdapter):
        def path(self, relative):
            return str(self.root / relative)

        def check_path(self, path):
            return Path(path).stat()

        def promote(self, relative, final):
            calls.append((relative, final))
            super().promote(relative, final)

    monkeypatch.setattr(adapters, 'SFTPAdapter', FilesystemTransport)
    monkeypatch.setattr(model, 'load_settings', lambda path: {})
    monkeypatch.setattr(model, 'load_steps', lambda settings: {'sftp_copy': [step]})
    node.sftp_adapter()
    report = json.loads(capsys.readouterr().out)
    assert report['scope'] == 'product-sftp-adapter-only'
    assert report['source_retained'] and report['staging_private']
    assert report['destination_conflict_refused']
    assert len(calls) == 2
    assert (Path(report['source']) / 'empty.txt').stat().st_size == 0
    assert 'nested/data.txt' in report['manifest']
    assert 'file with spaces.txt' in report['manifest']
    assert (destination / report['retained_conflict_stage']).is_dir()


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
