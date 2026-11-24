"""Independent-center identity, retention and consumer handoff contracts."""
import csv
import errno
import grp
import json
import os
from pathlib import Path
import pwd
import shutil
import socket
import stat

import pytest
import yaml

from landingzones.execution.engine import Executor
from landingzones.execution.adapters import LocalAdapter
from landingzones.execution import adapters, source as source_module
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


def test_ready_on_discovery_publishes_the_configured_group_before_handoff(setup):
    root, payload, rows, write, config, _ = setup
    (payload / '.ready').unlink()
    group = grp.getgrgid(os.getegid())
    rows[0].update(operation='move', readiness_policy='managed_ready', publication_group=group.gr_name)
    write()
    executor = Executor(config)
    result, = executor.discover('a')['results']
    assert result['status'] == 'completed'
    assert not payload.exists()
    published = root / 'a/run-123'
    assert (published / 'data.txt').read_text() == 'accepted data'
    for path in (published, *published.rglob('*')):
        assert path.stat().st_gid == group.gr_gid
        assert stat.S_IMODE(path.stat().st_mode) == (0o770 if path.is_dir() else 0o660)
    assert (published / LABEL).is_file()
    assert executor.discover('a')['results'] == []


def test_move_keeps_source_when_publication_permissions_cannot_be_applied(setup, monkeypatch):
    root, payload, rows, write, config, request = setup
    rows[0].update(operation='move', publication_group=grp.getgrgid(os.getegid()).gr_name)
    write()

    def denied(fd, mode):
        raise PermissionError('group publication denied')

    monkeypatch.setattr(adapters.os, 'fchmod', denied)
    state = Executor(config).submit(request)
    assert state['status'] != 'completed'
    assert 'group publication denied' in step(state)['attempts'][-1]['error']
    assert payload.exists()
    assert not (root / 'a/run-123').exists()


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


@pytest.mark.parametrize('previously_cleaned', [False, True])
def test_exclusive_local_rename_recovers_unsupported_atomic_move(setup, monkeypatch, previously_cleaned):
    root, payload, rows, write, config, request = setup
    rows[0]['operation'] = 'move'
    write()

    def unsupported(source, destination):
        raise OSError(errno.EINVAL, 'Invalid argument', str(destination))

    monkeypatch.setattr(adapters, '_rename_noreplace', unsupported)
    with monkeypatch.context() as previous_version:
        if previously_cleaned:
            previous_version.setattr(LocalAdapter, 'check_move_publication', lambda *args: None)
        state = Executor(config).submit(request)

    assert state['status'] == 'blocked'
    assert payload.exists() == (not previously_cleaned)
    assert step(state)['attempts'][-1]['phase'] == ('promotion' if previously_cleaned else 'cleanup')
    stage = root / '.landingzones-staging-a' / step(state)['step_id']
    assert (stage / 'data.txt').read_text() == 'accepted data'

    settings = yaml.safe_load(config.read_text())
    settings.update(local_path_base='.', exclusive_local_destinations=['a'])
    config.write_text(yaml.safe_dump(settings))
    executor = Executor(config)
    completed = executor.submit(request)
    assert completed['request_id'] == state['request_id']
    assert completed['status'] == 'completed'
    assert (root / 'a/run-123/data.txt').read_text() == 'accepted data'
    assert not stage.exists() and not payload.exists()
    assert sum(attempt['phase'] == 'transfer' for attempt in step(completed)['attempts']) == 1
    assert executor.submit(request) == completed


def test_checked_rename_preserves_a_destination_found_at_fallback(setup, monkeypatch):
    root, payload, rows, write, config, request = setup
    settings = yaml.safe_load(config.read_text())
    settings['exclusive_local_destinations'] = [str(root / 'a')]
    config.write_text(yaml.safe_dump(settings))

    def unsupported(source, destination):
        destination.mkdir()
        raise OSError(errno.EINVAL, 'Invalid argument', str(destination))

    monkeypatch.setattr(adapters, '_rename_noreplace', unsupported)
    state = Executor(config).submit(request)
    assert step(state)['status'] == 'parked'
    assert 'Destination already exists' in step(state)['attempts'][-1]['error']
    assert list((root / 'a/run-123').iterdir()) == []
    assert (root / '.landingzones-staging-a' / step(state)['step_id'] / 'data.txt').is_file()
    assert payload.is_dir()


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
    rows[0].update(operation='move', adapter='rsync', destination='ssh://receiver@example.invalid' + str(root / 'a'))
    write()
    with pytest.raises(ValueError, match='durable publication support'):
        executor.preflight(dict(request, idempotency_key='move'))
    assert payload.exists()



@pytest.fixture
def openssh_pipe(monkeypatch):
    """Speak SFTP to a child process over a Unix socketpair on the test machine."""
    from contextlib import ExitStack
    import subprocess
    from landingzones.execution.adapters import SFTPAdapter
    paramiko = pytest.importorskip('paramiko')
    binary = next((p for p in ('/usr/libexec/sftp-server', '/usr/lib/openssh/sftp-server')
                   if Path(p).is_file()), None)
    if not binary:
        pytest.skip('OpenSSH SFTP test program unavailable')

    class Channel(socket.socket):
        def get_name(self):
            return 'local-test'

    def enter(self):
        self.stack = ExitStack()
        child, client = socket.socketpair()
        with child:
            self.stack.enter_context(subprocess.Popen([binary], stdin=child, stdout=child))
        channel = self.stack.enter_context(Channel(fileno=client.detach()))
        self.sftp = self.stack.enter_context(paramiko.SFTPClient(channel))
        return self

    monkeypatch.setattr(SFTPAdapter, '__enter__', enter)
    monkeypatch.setattr(SFTPAdapter, '__exit__', lambda self, *args: self.stack.close())


@pytest.mark.parametrize('fault', ['none', 'wrong_group', 'sync_failed', 'lost_rename_reply', 'lost_and_collected'])
def test_remote_move_receipt_precedes_source_cleanup(setup, openssh_pipe, monkeypatch, fault):
    root, payload, rows, write, config, request = setup
    gid = (root / 'a').stat().st_gid
    group = grp.getgrgid(gid).gr_name
    rows[0].update(destination='sftp://receiver@example.invalid' + str(root / 'a'),
                   adapter='sftp', operation='move', credential_ref='test',
                   publication_group='missing-group' if fault == 'wrong_group' else group)
    write()
    credentials(config)
    published = root / 'a' / payload.name

    with monkeypatch.context() as patch:
        if fault == 'sync_failed':
            def unavailable(*args):
                raise OSError('receiver cannot flush the stage')
            patch.setattr(adapters.SFTPAdapter, 'sync_directory', unavailable)
        if fault in ('lost_rename_reply', 'lost_and_collected'):
            promote = adapters.SFTPAdapter.promote
            def interrupted(transport, stage, final):
                promote(transport, stage, final)
                if fault == 'lost_and_collected':
                    shutil.rmtree(published)
                raise OSError('lost rename reply')
            patch.setattr(adapters.SFTPAdapter, 'promote', interrupted)
        result = Executor(config).submit(request)

    if fault != 'none':
        assert result['status'] == 'blocked' and payload.is_dir()
    if fault == 'wrong_group':
        assert not published.exists()
        return
    result = Executor(config).submit(request)
    if fault == 'lost_and_collected':
        assert result['status'] == 'blocked' and payload.is_dir()
        return
    assert result['status'] == 'completed' and not payload.exists()
    assert (published / 'data.txt').read_text() == 'accepted data'
    assert all(p.stat().st_gid == gid and stat.S_IMODE(p.stat().st_mode) == (0o770 if p.is_dir() else 0o660)
               for p in (published, *published.rglob('*')))
    assert step(result)['published'] and step(result)['cleaned']

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
