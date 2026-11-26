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
import subprocess

import pytest
import yaml

from landingzones.execution.engine import Executor
from landingzones.execution.adapters import LocalAdapter
from landingzones.execution import adapters, source as source_module
from landingzones.execution.package import LABEL, admit
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


@pytest.mark.parametrize('directory_mode', ['', '2770'])
def test_ready_on_discovery_publishes_the_configured_group_before_handoff(setup, directory_mode):
    root, payload, rows, write, config, _ = setup
    (payload / '.ready').unlink()
    group = grp.getgrgid(os.getegid())
    rows[0].update(operation='move', readiness_policy='managed_ready', publication_group=group.gr_name,
                   publication_directory_mode=directory_mode)
    write()
    executor = Executor(config)
    result, = executor.discover('a')['results']
    assert result['status'] == 'completed'
    assert not payload.exists()
    published = root / 'a/run-123'
    assert (published / 'data.txt').read_text() == 'accepted data'
    for path in (published, *published.rglob('*')):
        assert path.stat().st_gid == group.gr_gid
        assert stat.S_IMODE(path.stat().st_mode) == (int(directory_mode or '0770', 8) if path.is_dir() else 0o660)
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


def configure_rsync_pull(setup, source_root=None):
    root, payload, rows, write, config, request = setup
    if source_root:
        payload.parent.rename(source_root)
        payload = source_root / payload.name
    rows[0].update(source='ssh://producer@example.invalid:2222' + str(payload.parent),
                   adapter='rsync', operation='move', verification='size', credential_ref='test')
    write()
    credentials(config)
    return root, payload, Executor(config), request


def test_rsync_pull_uses_private_stage_and_preserves_source_on_failure(setup, filesystem_sftp, monkeypatch):
    root, payload, executor, request = configure_rsync_pull(setup)
    commands = []

    def transfer(argv, *, input, pass_fds, **kwargs):
        import fcntl
        stage = Path(argv[-1])
        assert stage.parent == root / '.landingzones-staging-a'
        assert set(input.rstrip(b'\0').split(b'\0')) == {b'data.txt', b'empty.txt', b'empty-directory'}
        assert argv[0] == 'rsync' and '--protect-args' in argv
        assert argv[-2] == 'producer@example.invalid:' + str(payload) + '/'
        assert '--checksum' not in argv and '--remove-source-files' not in argv
        lock = root / 'state/execution.lock'
        assert len(pass_fds) == 1 and os.fstat(pass_fds[0]).st_ino == lock.stat().st_ino
        fd = os.open(lock, os.O_RDWR)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(fd)
        assert kwargs.get('timeout') is None
        scratch = stage.with_name('.rsync-' + stage.name)
        assert '--temp-dir=' + str(scratch) in argv
        commands.append(argv)
        if len(commands) == 1:
            (scratch / '.interrupted-file').write_text('incomplete')
            (stage / 'data.txt').write_text('part')
            raise subprocess.CalledProcessError(23, argv)
        for path in payload.iterdir():
            if path.is_dir():
                (stage / path.name).mkdir()
            elif path.name != '.ready':
                shutil.copyfile(path, stage / path.name)

    def no_get(*args):
        pytest.fail('Direct rsync pulls must bypass individual SFTP downloads')

    monkeypatch.setattr(filesystem_sftp, 'get', no_get)
    monkeypatch.setattr(adapters.subprocess, 'run', transfer)
    state = executor.submit(request)
    assert state['status'] == 'blocked' and step(state)['attempts'][-1]['phase'] == 'transfer'
    assert payload.is_dir() and not (root / 'a' / payload.name).exists()
    assert not (root / 'state/source-cache').exists()
    retry = executor.resume(state['request_id'])
    assert retry['status'] == 'completed' and retry['request_id'] == state['request_id']
    assert len(commands) == 2 and commands[0] == commands[1]
    assert not list((root / '.landingzones-staging-a').iterdir())
    assert not payload.exists() and (root / 'a/run-123/data.txt').read_text() == 'accepted data'


def local_rsync_shell(root, monkeypatch):
    # Simulate only SSH transport: both sides execute the installed rsync locally.
    rsync = shutil.which('rsync')
    help_text = subprocess.check_output([rsync, '--help'], text=True) if rsync else ''
    if '--protect-args' not in help_text and '--secluded-args' not in help_text:
        pytest.skip('Requires rsync with protected arguments')
    bin_dir = root / 'bin'
    bin_dir.mkdir()
    ssh = bin_dir / 'ssh'
    ssh.write_text('#!/bin/sh\nwhile [ "$#" -gt 0 ] && [ "$1" != rsync ]; do shift; done\n'
                   '[ "$1" = rsync ] && [ "$2" = --server ] || exit 2\n'
                   'exec "$@"\n')
    ssh.chmod(0o700)
    monkeypatch.setenv('PATH', str(bin_dir) + os.pathsep + os.environ['PATH'])


def test_real_rsync_pull_over_local_shell_fixture(setup, filesystem_sftp, monkeypatch):
    root, payload, executor, request = configure_rsync_pull(setup, setup[0] / 'input [ab]')
    # A generator using an unescaped source glob would read this other queue.
    shadow = root / 'input a' / payload.name
    shadow.mkdir(parents=True)
    (shadow / 'data.txt').write_text('wrong queue')
    local_rsync_shell(root, monkeypatch)
    (payload / 'nested files').mkdir()
    special = Path('nested files') / 'data [part 1]; a.fastq'
    (payload / special).write_bytes(b'sequence')
    (payload / '.dotfile').write_bytes(b'hidden')
    (payload / '.landing_zones').mkdir()
    (payload / '.landing_zones/metadata').touch()
    state = executor.submit(request)
    assert state['status'] == 'completed', step(state)['attempts']
    published = root / 'a' / payload.name
    assert (published / special).read_bytes() == b'sequence'
    assert (published / '.dotfile').read_bytes() == b'hidden'
    assert (published / 'empty-directory').is_dir()
    assert not (published / '.ready').exists() and not (published / '.landing_zones').exists()
    assert not payload.exists() and not (root / 'state/source-cache').exists()


@pytest.mark.parametrize('remote_source', [False, True])
def test_size_only_move_does_not_hash_payload_and_preserves_known_label(setup, filesystem_sftp, monkeypatch, remote_source):
    root, payload, rows, write, config, _ = setup
    previous = admit(adapters.manifest(payload))
    (payload / LABEL).write_text(json.dumps(previous))
    rows[0].update(operation='move', verification='size')
    if remote_source:
        rows[0].update(source='sftp://producer@example.invalid' + str(payload.parent), credential_ref='test')
        credentials(config)
    write()

    def unexpected_digest(*args):
        pytest.fail('Size-only verification hashed payload bytes')

    monkeypatch.setattr(adapters, 'digest', unexpected_digest)
    monkeypatch.setattr(source_module, 'digest', unexpected_digest)
    state, = Executor(config).discover('a')['results']
    assert state['status'] == 'completed'
    assert state['manifest']['data.txt'] == {'kind': 'file', 'size': len('accepted data')}
    assert not payload.exists()
    published = root / 'a' / payload.name
    assert (published / 'data.txt').read_text() == 'accepted data'
    label = json.loads((published / LABEL).read_text())
    assert label['package_id'] == previous['package_id']
    assert label['content_version'] == previous['content_version']
    assert label['verification'] == 'size'


def test_size_only_move_keeps_source_after_staging_size_changes(setup, monkeypatch):
    root, payload, rows, write, config, request = setup
    rows[0].update(operation='move', verification='size')
    write()
    copy = LocalAdapter.copy

    def truncate_stage(transport, source, relative, accepted):
        copy(transport, source, relative, accepted)
        (transport.root / relative / 'data.txt').write_text('short')

    monkeypatch.setattr(LocalAdapter, 'copy', truncate_stage)
    state = Executor(config).submit(request)
    assert state['status'] == 'blocked'
    assert step(state)['attempts'][-1]['error'] == 'Staging verification failed'
    assert (payload / 'data.txt').read_text() == 'accepted data'
    assert not (root / 'a' / payload.name).exists()


def test_size_only_copy_reuses_a_completed_checksum_receipt(setup):
    root, payload, rows, write, config, request = setup
    first = Executor(config).submit(request)
    assert first['status'] == 'completed'
    shutil.rmtree(root / 'a' / payload.name)
    rows[0]['verification'] = 'size'
    write()
    repeat, = Executor(config).discover('a')['results']
    assert repeat['request_id'] == first['request_id']
    assert payload.exists()
    assert not (root / 'a' / payload.name).exists()


def test_legacy_size_copy_receipt_can_resume_with_its_checksum_manifest(setup, monkeypatch):
    root, payload, rows, write, config, request = setup
    rows[0]['verification'] = 'size'
    write()

    def inspect_with_checksum(source, name):
        return adapters.manifest(source.root / name)

    def unavailable(*args):
        raise OSError('copy unavailable')

    with monkeypatch.context() as patch:
        patch.setattr(source_module.Source, 'inspect', inspect_with_checksum)
        patch.setattr(LocalAdapter, 'copy', unavailable)
        saved = Executor(config).submit(request)
    assert saved['status'] == 'blocked'
    assert 'sha256' in saved['manifest']['data.txt']

    resumed = Executor(config).resume(saved['request_id'])
    assert resumed['status'] == 'completed'
    assert payload.exists()
    assert (root / 'a' / payload.name / 'data.txt').read_text() == 'accepted data'


def test_size_only_remote_move_keeps_source_until_publication(setup, openssh_pipe, monkeypatch):
    root, payload, rows, write, config, request = setup
    rows[0].update(destination='sftp://receiver@example.invalid' + str(root / 'a'),
                   adapter='sftp', operation='move', verification='size', credential_ref='test')
    write()
    credentials(config)

    def unexpected_digest(*args):
        pytest.fail('Size-only verification hashed payload bytes')

    def unavailable(*args):
        raise OSError('publication unavailable')

    monkeypatch.setattr(adapters, 'digest', unexpected_digest)
    monkeypatch.setattr(source_module, 'digest', unexpected_digest)
    with monkeypatch.context() as patch:
        patch.setattr(adapters.SFTPAdapter, 'promote', unavailable)
        blocked = Executor(config).submit(request)
    assert blocked['status'] == 'blocked'
    assert payload.is_dir()
    assert not (root / 'a' / payload.name).exists()
    completed = Executor(config).resume(blocked['request_id'])
    assert completed['status'] == 'completed'
    assert step(completed)['published'] and step(completed)['cleaned']
    assert (root / 'a' / payload.name / 'data.txt').read_text() == 'accepted data'
    assert not payload.exists()


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


def test_sftp_copy_publishes_inheritable_directories_and_keeps_source(setup, openssh_pipe):
    root, payload, rows, write, config, request = setup
    gid = (root / 'a').stat().st_gid
    (payload / 'empty-directory' / 'nested.txt').write_text('nested')
    rows[0].update(destination='sftp://receiver@example.invalid' + str(root / 'a'),
                   adapter='sftp', credential_ref='test', publication_group=grp.getgrgid(gid).gr_name,
                   publication_directory_mode='2770')
    write()
    credentials(config)
    assert Executor(config).submit(request)['status'] == 'completed'
    published = root / 'a' / payload.name
    for path in (published, *published.rglob('*')):
        assert path.stat().st_gid == gid
        assert stat.S_IMODE(path.stat().st_mode) == (0o2770 if path.is_dir() else 0o660)
    assert payload.is_dir()
    assert stat.S_IMODE((root / '.landingzones-staging-a').stat().st_mode) & 0o077 == 0


def test_inheritance_upgrade_preserves_receipts_for_collected_copies(setup):
    root, payload, rows, write, config, _ = setup
    first, = Executor(config).discover('a')['results']
    assert first['status'] == 'completed'
    shutil.rmtree(root / 'a' / payload.name)
    rows[0].update(publication_group=grp.getgrgid(os.getegid()).gr_name, publication_directory_mode='2770')
    write()
    repeat, = Executor(config).discover('a')['results']
    assert repeat['request_id'] == first['request_id']
    assert not (root / 'a' / payload.name).exists()


@pytest.mark.parametrize('transfer_adapter', ['sftp', 'rsync'])
@pytest.mark.parametrize('fault', ['none', 'wrong_group', 'sync_failed', 'lost_rename_reply', 'lost_and_collected'])
def test_remote_move_receipt_precedes_source_cleanup(setup, openssh_pipe, monkeypatch, fault, transfer_adapter):
    root, payload, rows, write, config, request = setup
    if transfer_adapter == 'rsync':
        local_rsync_shell(root, monkeypatch)
    gid = (root / 'a').stat().st_gid
    group = grp.getgrgid(gid).gr_name
    rows[0].update(destination='sftp://receiver@example.invalid' + str(root / 'a'),
                   adapter=transfer_adapter, operation='move', credential_ref='test',
                   verification='size' if transfer_adapter == 'rsync' else 'checksum',
                   publication_group='missing-group' if fault == 'wrong_group' else group,
                   publication_directory_mode='2770')
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
    assert all(p.stat().st_gid == gid and stat.S_IMODE(p.stat().st_mode) == (0o2770 if p.is_dir() else 0o660)
               for p in (published, *published.rglob('*')))
    assert step(result)['published'] and step(result)['cleaned']

@pytest.mark.parametrize('fault', ['partial', 'wrong_size'])
def test_rsync_push_keeps_source_private_until_stage_is_verified(setup, filesystem_sftp, monkeypatch, fault):
    root, payload, rows, write, config, request = setup
    rows[0].update(destination='sftp://receiver@example.invalid' + str(root / 'a'),
                   adapter='rsync', operation='copy', credential_ref='test', verification='size')
    write()
    credentials(config)
    (payload / '.landing_zones').mkdir()
    (payload / '.landing_zones' / 'user-metadata').touch()
    local_rsync_shell(root, monkeypatch)
    executor = Executor(config)
    published = root / 'a' / payload.name
    real_copy = adapters.RemoteRsyncAdapter.copy_manifest

    def broken(self, source, destination, scratch, accepted, execution_lock):
        assert set(accepted) == {'empty-directory', 'empty.txt', 'data.txt'}
        assert execution_lock.fileno() >= 0
        if fault == 'partial':
            (Path(scratch) / '.interrupted').touch()
            raise OSError('interrupted rsync')
        real_copy(self, source, destination, scratch, accepted, execution_lock)
        (Path(destination.split(':', 1)[1]) / 'data.txt').write_bytes(b'short')

    with monkeypatch.context() as patch:
        patch.setattr(adapters.RemoteRsyncAdapter, 'copy_manifest', broken)
        state = executor.submit(request)
    assert state['status'] == 'blocked' and not published.exists() and payload.exists()
    work = step(state)
    assert work['attempts'][-1]['phase'] == 'transfer' and not work.get('staged')
    if fault == 'wrong_size':
        assert work['attempts'][-1]['error'] == 'Staging verification failed'
    result = executor.resume(state['request_id'], retry_parked=True)
    assert result['status'] == 'completed' and payload.is_dir()
    assert (published / 'data.txt').read_text() == 'accepted data'
    assert (published / 'empty-directory').is_dir()
    assert not (published / '.ready').exists() and not (published / '.landing_zones').exists()
    assert not list((root / '.landingzones-staging-a').iterdir())


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
