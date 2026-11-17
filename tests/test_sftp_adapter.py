"""SFTP transport contracts using synthetic local files and clients, never a server."""
from pathlib import Path
import shutil
import stat
import sys
from types import SimpleNamespace

import pytest

from landingzones.execution.adapters import SFTPAdapter, manifest, matches
from landingzones.execution.model import Step
from landingzones.execution.package import LABEL


@pytest.fixture
def transport(tmp_path):
    root = tmp_path.resolve() / 'destination'
    root.mkdir()
    step = Step(identifiers='test', runtime_id='test', system='example', users='operator',
                flow_group='test', step_order=1, source='/example/input',
                destination='sftp://upload@example.invalid:2222' + str(root),
                adapter='sftp', operation='copy', credential_ref='test')
    settings = {'credentials': {'test': {'private_key_file': '/unused/key',
                                        'known_hosts_file': '/unused/known_hosts'}}}
    return SFTPAdapter(step, settings)


class FilesystemSFTP:
    """The SFTP file operations are backed only by the test's temporary directory."""
    def __init__(self):
        self.closed = False
        self.uploads = []

    def get_channel(self):
        return SimpleNamespace(settimeout=lambda timeout: None)

    def close(self):
        self.closed = True

    def lstat(self, path):
        return Path(path).lstat()

    def listdir_attr(self, path):
        return [SimpleNamespace(filename=p.name, st_mode=p.lstat().st_mode,
                                st_size=p.lstat().st_size) for p in Path(path).iterdir()]

    def open(self, path, mode):
        return open(path, mode)

    def mkdir(self, path, mode=0o700):
        Path(path).mkdir(mode=mode)

    def put(self, source, target, confirm=True):
        self.uploads.append(target)
        shutil.copyfile(source, target)

    def rename(self, source, target):
        if Path(target).exists():
            raise FileExistsError(target)
        Path(source).rename(target)


@pytest.fixture
def paramiko_client(monkeypatch):
    sftp = FilesystemSFTP()
    client = SimpleNamespace(closed=False, connected=False, sftp=sftp)

    def load_host_keys(path):
        client.known_hosts = path

    def set_policy(policy):
        client.policy = policy

    def connect(hostname, **kwargs):
        client.connected = True
        client.hostname = hostname
        client.options = kwargs

    def close():
        client.closed = True

    class RejectPolicy:
        pass

    class SSHException(Exception):
        pass

    client.load_host_keys = load_host_keys
    client.set_missing_host_key_policy = set_policy
    client.connect = connect
    client.open_sftp = lambda: sftp
    client.close = close
    module = SimpleNamespace(SSHClient=lambda: client, RejectPolicy=RejectPolicy,
                             SSHException=SSHException)
    monkeypatch.setitem(sys.modules, 'paramiko', module)
    return client, module


def test_optional_dependency_has_an_actionable_error(transport, monkeypatch):
    monkeypatch.setitem(sys.modules, 'paramiko', None)
    with pytest.raises(ValueError, match=r'landingzones\[sftp\]'):
        transport.__enter__()


def test_explicit_credentials_and_pinned_hosts_close_both_resources(transport, paramiko_client):
    client, module = paramiko_client
    with transport:
        assert client.hostname == 'example.invalid'
        assert isinstance(client.policy, module.RejectPolicy)
        assert client.known_hosts == '/unused/known_hosts'
        assert client.options['port'] == 2222
        assert client.options['username'] == 'upload'
        assert client.options['key_filename'] == '/unused/key'
        assert client.options['allow_agent'] is False
        assert client.options['look_for_keys'] is False
    assert client.sftp.closed and client.closed


@pytest.mark.parametrize('failure', ['host_keys', 'connect', 'open_sftp', 'timeout', 'root'])
def test_failed_open_closes_every_created_resource(transport, paramiko_client, failure):
    client, module = paramiko_client

    def fail(*args, **kwargs):
        raise OSError('synthetic setup failure')

    if failure == 'host_keys':
        client.load_host_keys = fail
    elif failure == 'connect':
        client.connect = fail
    elif failure == 'open_sftp':
        client.open_sftp = fail
    elif failure == 'timeout':
        client.sftp.get_channel = lambda: SimpleNamespace(settimeout=fail)
    else:
        client.sftp.lstat = fail
    with pytest.raises(OSError, match='synthetic setup failure'):
        transport.__enter__()
    assert client.closed
    if failure in ('timeout', 'root'):
        assert client.sftp.closed
    if failure == 'host_keys':
        assert not client.connected


def test_client_is_closed_even_when_channel_close_fails(transport, paramiko_client):
    client, module = paramiko_client

    def fail():
        raise OSError('synthetic close failure')

    with pytest.raises(OSError, match='synthetic close failure'):
        with transport:
            client.sftp.close = fail
    assert client.closed


def test_missing_host_pin_or_bad_pin_does_not_fall_back_to_other_auth(transport, paramiko_client):
    client, module = paramiko_client

    def reject(*args, **kwargs):
        raise module.SSHException('synthetic host-key rejection')

    client.connect = reject
    with pytest.raises(OSError, match='SFTP connection'):
        transport.__enter__()
    assert client.closed
    assert isinstance(client.policy, module.RejectPolicy)


def test_configured_remote_root_must_be_a_directory(transport, paramiko_client):
    root = Path(transport.root)
    root.rmdir()
    root.write_bytes(b'not a directory')
    with pytest.raises(ValueError, match='root must be a directory'):
        with transport:
            pass


def test_remote_filesystem_root_cannot_supply_sibling_staging(transport):
    transport.root = '/'
    with pytest.raises(ValueError, match='sibling staging'):
        transport.stage_name('test-package')


def test_existing_shared_staging_is_rejected_before_upload(transport, paramiko_client, tmp_path):
    payload = tmp_path / 'payload'
    payload.mkdir()
    (payload / 'data').write_bytes(b'accepted data')
    stage = transport.stage_name('test-package')
    staging_root = Path(transport.path(stage)).parent
    staging_root.mkdir(mode=0o755)
    staging_root.chmod(0o755)
    with transport:
        with pytest.raises(ValueError, match='Staging root must be private'):
            transport.copy(payload, stage, manifest(payload))
        assert not transport.sftp.uploads


@pytest.mark.parametrize('marker', ['.ready', LABEL])
def test_metadata_markers_must_be_regular_files(transport, paramiko_client, marker):
    root = Path(transport.root) / 'package'
    root.mkdir()
    (root / marker).mkdir()
    with transport:
        with pytest.raises(ValueError, match='marker must be a regular file'):
            transport.inspect('package')


def test_copy_retry_repairs_partial_upload_and_checks_content(transport, paramiko_client, tmp_path):
    payload = tmp_path / 'payload'
    payload.mkdir()
    (payload / 'nested').mkdir()
    (payload / 'nested/data').write_bytes(b'accepted data')
    (payload / 'empty-directory').mkdir()
    (payload / '.ready').touch()
    accepted = manifest(payload)
    stage = transport.stage_name('test-package')
    client, module = paramiko_client
    upload = client.sftp.put

    def interrupt(source, target, confirm=True):
        Path(target).write_bytes(b'partial')
        raise OSError('synthetic interrupted upload')

    with transport:
        client.sftp.put = interrupt
        with pytest.raises(OSError, match='interrupted upload'):
            transport.copy(payload, stage, accepted)
        assert not (Path(transport.root) / 'package').exists()
        client.sftp.put = upload
        transport.copy(payload, stage, accepted)
        assert matches(transport.inspect(stage), accepted)
        assert not (Path(transport.path(stage)) / '.ready').exists()
        assert stat.S_IMODE(Path(transport.path(stage)).parent.stat().st_mode) == 0o700
        (Path(transport.path(stage)) / 'nested/data').write_bytes(b'corrupt! data')
        assert not matches(transport.inspect(stage), accepted)


def test_symlink_stage_is_never_written_or_published(transport, paramiko_client, tmp_path):
    payload = tmp_path / 'payload'
    payload.mkdir()
    (payload / 'data').write_bytes(b'accepted data')
    stage = transport.stage_name('test-package')
    staging_root = Path(transport.path(stage)).parent
    elsewhere = tmp_path / 'elsewhere'
    elsewhere.mkdir()
    staging_root.symlink_to(elsewhere, target_is_directory=True)
    with transport:
        with pytest.raises(ValueError, match='symlink'):
            transport.copy(payload, stage, manifest(payload))
    assert not list(elsewhere.iterdir())
