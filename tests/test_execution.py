"""Independent-center identity, retention and consumer handoff contracts."""
import csv
import json
import os
from pathlib import Path
import pwd
import shutil
import socket

import pytest
import yaml

from landingzones.execution.engine import Executor
from landingzones.execution.adapters import LocalAdapter
from landingzones.execution import source as source_module
from landingzones.execution.package import LABEL
from landingzones.execution.storage import StateStore


@pytest.fixture
def setup(tmp_path):
    root = tmp_path.resolve()
    source = root / 'input'
    source.mkdir()
    payload = source / 'run-123'
    payload.mkdir()
    (payload / '.ready').touch()
    (payload / 'data.txt').write_text('accepted data')
    (payload / 'empty.txt').touch()
    (payload / 'empty-directory').mkdir()
    (root / 'a').mkdir()
    (root / 'b').mkdir()
    rows = [dict(identifiers='a', enabled='TRUE', executor='python', runtime_id='test',
                 system=socket.gethostname(), users=pwd.getpwuid(os.geteuid()).pw_name,
                 flow_group='a', step_order='1', source=str(source), destination=str(root / 'a'),
                 adapter='local', operation='copy', verification='checksum', max_attempts='3',
                 readiness_policy='producer_marker', admission_policy='strict')]
    table = root / 'transfers.tsv'
    def write():
        with table.open('w') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]), delimiter='\t')
            writer.writeheader()
            writer.writerows(rows)
    write()
    config = root / 'config.yaml'
    config.write_text(yaml.safe_dump(dict(execution_schema_version=1, transfers_file=str(table),
        state_dir=str(root / 'state'), event_spool=str(root / 'events.tsv'), runtime_ids=['test'],
        execution_context=dict(system=socket.gethostname(), user=pwd.getpwuid(os.geteuid()).pw_name))))
    request = dict(idempotency_key='example', payload_name='run-123', connection='a')
    return root, payload, rows, write, config, request


def step(state):
    return state['deliveries'][0]['steps'][0]


def test_copy_receipt_retains_source_and_deduplicates_other_callers(setup):
    root, payload, rows, write, config, request = setup
    executor = Executor(config)
    state = executor.submit(request)
    assert state['status'] == 'completed'
    assert payload.exists()
    assert (root / 'a/run-123/empty-directory').is_dir()
    assert not (root / 'a/run-123/.ready').exists()
    shutil.rmtree(root / 'a/run-123')  # consumer takes it
    assert executor.submit(request) == state
    assert executor.submit(dict(request, idempotency_key='another')) == state
    assert not (root / 'a/run-123').exists()
    assert executor.discover('a')['results'][0]['request_id'] == state['request_id']


def test_move_cleanup_failure_keeps_output_private_and_resumes(setup, monkeypatch):
    root, payload, rows, write, config, request = setup
    rows[0]['operation'] = 'move'
    write()
    original = source_module.cleanup
    def interrupt(source, accepted):
        (source / 'empty.txt').unlink()
        raise PermissionError('cleanup interrupted')
    monkeypatch.setattr(source_module, 'cleanup', interrupt)
    executor = Executor(config)
    state = executor.submit(request)
    assert state['status'] == 'blocked'
    assert not (root / 'a/run-123').exists()
    assert (root / '.landingzones-staging-a' / step(state)['step_id'] / 'data.txt').exists()
    monkeypatch.setattr(source_module, 'cleanup', original)
    state = executor.resume(state['request_id'])
    assert state['status'] == 'completed' and not payload.exists()
    assert [a['phase'] for a in step(state)['attempts']] == ['transfer', 'cleanup', 'cleanup', 'promotion']


@pytest.mark.parametrize('operation', ['copy', 'move'])
def test_crash_after_rename_and_immediate_consumption_never_resends(setup, monkeypatch, operation):
    root, payload, rows, write, config, request = setup
    rows[0]['operation'] = operation
    write()
    original = LocalAdapter.promote
    def crash(self, relative, final):
        if operation == 'move':
            assert not payload.exists()  # cleanup must precede publication
        original(self, relative, final)
        shutil.rmtree(self.root / final)
        raise SystemExit('crash before success record')
    monkeypatch.setattr(LocalAdapter, 'promote', crash)
    executor = Executor(config)
    with pytest.raises(SystemExit):
        executor.submit(request)
    monkeypatch.setattr(LocalAdapter, 'promote', original)
    state = executor.submit(request)
    assert state['status'] == 'completed'
    assert not (root / 'a/run-123').exists()
    assert sum(a['phase'] == 'transfer' for a in step(state)['attempts']) == 1


def test_publication_failure_after_cleanup_recovers_on_discovery(setup, monkeypatch):
    root, payload, rows, write, config, request = setup
    rows[0]['operation'] = 'move'
    write()
    original = LocalAdapter.promote
    def fail(*args):
        raise PermissionError('rename failed')
    monkeypatch.setattr(LocalAdapter, 'promote', fail)
    executor = Executor(config)
    state = executor.submit(request)
    assert state['status'] == 'blocked' and not payload.exists()
    monkeypatch.setattr(LocalAdapter, 'promote', original)
    result = executor.discover('a')
    assert result['results'][0]['status'] == 'completed'
    assert (root / 'a/run-123/data.txt').read_text() == 'accepted data'


def test_independent_centers_preserve_identity_and_itinerary(setup):
    root, payload, rows, write, config, request = setup
    request['itinerary'] = ['a', 'b']
    first = Executor(config).submit(request)
    label = json.loads((root / 'a/run-123' / LABEL).read_text())
    rows[0].update(identifiers='b', flow_group='b', source=str(root / 'a'), destination=str(root / 'b'),
                   readiness_policy='managed_ready', operation='move')
    write()
    settings = yaml.safe_load(config.read_text())
    settings['state_dir'] = str(root / 'other-state')
    config.write_text(yaml.safe_dump(settings))
    second = Executor(config).discover('b')['results'][0]
    assert second['payload_id'] == first['payload_id'] == label['package_id']
    assert second['payload_version'] == first['payload_version']
    assert second['run_id'] == first['run_id']
    assert step(second)['itinerary_status'] == 'on_plan'
    assert not (root / 'a/run-123').exists()
    assert (root / 'b/run-123' / LABEL).exists()


def test_known_old_label_preserves_identity_changed_label_requires_policy(setup):
    root, payload, rows, write, config, request = setup
    executor = Executor(config)
    label = executor.preflight(request)['label']
    label['schema_version'] = 0
    (payload / LABEL).write_text(json.dumps(label))
    plan = executor.preflight(request)
    assert plan['label']['package_id'] == label['package_id']
    assert plan['label']['schema_version'] == 1
    (payload / 'data.txt').write_text('trimmed data')
    with pytest.raises(ValueError, match='relabel'):
        executor.preflight(request)
    rows[0]['admission_policy'] = 'relabel'
    write()
    new = executor.preflight(request)['label']
    assert new['package_id'] != label['package_id']
    assert new['source_metadata'] == label
    assert 'parent_package_ids' not in new  # no invented lineage


def test_legacy_tsv_is_preserved_but_not_claimed_as_verified_identity(setup):
    root, payload, rows, write, config, request = setup
    (payload / '.landing_zones').mkdir()
    (payload / '.landing_zones/landingzone-run-metadata.tsv').write_text('run_id\told-id\n')
    with pytest.raises(ValueError, match='relabel'):
        Executor(config).submit(request)
    rows[0]['admission_policy'] = 'relabel'
    write()
    state = Executor(config).submit(request)
    assert state['payload_id'] != 'old-id'
    assert 'old-id' in state['label']['source_metadata']['legacy_metadata_tsv']


def test_readiness_is_not_created_by_relabelling(setup):
    root, payload, rows, write, config, request = setup
    (payload / '.ready').unlink()
    rows[0]['admission_policy'] = 'relabel'
    write()
    assert Executor(config).discover('a')['results'][0]['status'] == 'waiting'
    assert not (root / 'a/run-123').exists()


def test_destination_conflict_is_parked_before_cleanup(setup):
    root, payload, rows, write, config, request = setup
    rows[0]['operation'] = 'move'
    write()
    (root / 'a/run-123').mkdir()
    executor = Executor(config)
    state = executor.submit(request)
    assert step(state)['status'] == 'parked' and payload.exists()
    assert executor.resume(state['request_id']) == state
    (root / 'a/run-123').rmdir()
    assert executor.resume(state['request_id'], retry_parked=True)['status'] == 'completed'


def test_monitoring_failure_does_not_block_execution(setup):
    root, payload, rows, write, config, request = setup
    (root / 'events.tsv').mkdir()
    with pytest.warns(UserWarning, match='Event append failed'):
        assert Executor(config).submit(request)['status'] == 'completed'


def test_untrusted_symlinks_and_fanout_are_rejected(setup):
    root, payload, rows, write, config, request = setup
    (payload / 'escape').symlink_to(root / 'a', target_is_directory=True)
    with pytest.raises(ValueError, match='Symlinks'):
        Executor(config).preflight(request)
    (payload / 'escape').unlink()
    request.pop('connection')
    request['deliveries'] = [{'flow_group': 'a'}, {'flow_group': 'b'}]
    with pytest.raises(ValueError, match='exactly one'):
        Executor(config).preflight(request)


def test_intake_and_downstream_copy(setup):
    root, payload, rows, write, config, request = setup
    user = root / 'user-output'
    user.mkdir()
    payload.rename(user / payload.name)
    request['intake'] = dict(source_path=str(user / payload.name), input_root=str(payload.parent), operation='move')
    state = Executor(config).submit(request)
    assert state['status'] == 'completed'
    assert payload.exists() and not (user / payload.name).exists()


def test_cli_discovery(setup):
    from landingzones.cli import main
    root, payload, rows, write, config, request = setup
    assert main(['--config', str(config), 'transfer', 'discover', '--connection', 'a']) == 0


def test_runtime_lock_excludes_concurrent_execution(tmp_path):
    store = StateStore(tmp_path)
    with store.locked():
        with pytest.raises(BlockingIOError):
            with store.locked():
                pytest.fail('concurrent executor admitted')


@pytest.fixture
def filesystem_sftp(monkeypatch):
    """Exercise remote-source/destination APIs locally; never connect to a server."""
    from types import SimpleNamespace
    from landingzones.execution.adapters import SFTPAdapter
    class FilesystemSFTP:
        def lstat(self, path):
            return Path(path).lstat()
        def listdir_attr(self, path):
            return [SimpleNamespace(filename=p.name, st_mode=p.lstat().st_mode, st_size=p.lstat().st_size)
                    for p in Path(path).iterdir()]
        def open(self, path, mode):
            return open(path, mode)
        def get(self, source, target):
            shutil.copyfile(source, target)
        def put(self, source, target, confirm=True):
            shutil.copyfile(source, target)
        def mkdir(self, path, mode=0o700):
            Path(path).mkdir(mode=mode)
        def rmdir(self, path):
            Path(path).rmdir()
        def remove(self, path):
            Path(path).unlink()
        def rename(self, source, target):
            if Path(target).exists():
                raise FileExistsError(target)
            Path(source).rename(target)
    def enter(self):
        self.sftp = FilesystemSFTP()
        self.check_path(self.root)
        return self
    monkeypatch.setattr(SFTPAdapter, '__enter__', enter)
    return FilesystemSFTP


def credentials(config):
    settings = yaml.safe_load(config.read_text())
    settings['credentials'] = {'test': {'private_key_file': '/unused/key', 'known_hosts_file': '/unused/hosts'}}
    config.write_text(yaml.safe_dump(settings))


def test_configured_remote_pull_and_cleanup_use_local_handoff(setup, filesystem_sftp):
    root, payload, rows, write, config, request = setup
    rows[0].update(source='sftp://producer@example.invalid' + str(payload.parent), operation='move')
    rows[0]['credential_ref'] = 'test'
    write()
    credentials(config)
    state = Executor(config).discover('a')['results'][0]
    assert state['status'] == 'completed'
    assert not payload.exists()
    assert (root / 'a/run-123/data.txt').read_text() == 'accepted data'


def test_sftp_copy_receipt_and_remote_move_capability_gate(setup, filesystem_sftp):
    root, payload, rows, write, config, request = setup
    rows[0].update(destination='sftp://receiver@example.invalid' + str(root / 'a'), adapter='sftp')
    rows[0]['credential_ref'] = 'test'
    write()
    credentials(config)
    executor = Executor(config)
    state = executor.submit(request)
    assert state['status'] == 'completed' and payload.exists()
    assert json.loads((root / 'a/run-123' / LABEL).read_text())['package_id'] == state['payload_id']
    rows[0]['operation'] = 'move'
    write()
    with pytest.raises(ValueError, match='durable publication support'):
        executor.preflight(dict(request, idempotency_key='move'))
    assert payload.exists()


def test_archive_transport_is_not_silently_excluded_as_metadata(setup):
    root, payload, rows, write, config, request = setup
    (payload / '.landing_zones').mkdir()
    (payload / '.landing_zones/landingzone-run-archive.tar').write_bytes(b'archive contents')
    rows[0]['admission_policy'] = 'relabel'
    write()
    with pytest.raises(ValueError, match='Legacy archive'):
        Executor(config).submit(request)


@pytest.mark.parametrize('raw', ['{unknown format', 'null'])
def test_unknown_label_relabel_keeps_evidence_and_itinerary_discrepancy(setup, raw):
    root, payload, rows, write, config, request = setup
    (payload / LABEL).write_text(raw)
    rows[0]['admission_policy'] = 'relabel'
    write()
    request['itinerary'] = ['other-connection']
    state = Executor(config).submit(request)
    assert state['label']['source_metadata']['unparsed_label'] == raw
    assert step(state)['itinerary_status'] == 'discrepancy'


def test_destination_race_does_not_overwrite_other_publisher(setup, monkeypatch):
    root, payload, rows, write, config, request = setup
    original = LocalAdapter.promote
    def raced(self, relative, final):
        (self.root / final).mkdir()
        return original(self, relative, final)
    monkeypatch.setattr(LocalAdapter, 'promote', raced)
    state = Executor(config).submit(request)
    assert step(state)['status'] == 'parked'
    assert list((root / 'a/run-123').iterdir()) == []
    assert payload.exists()
