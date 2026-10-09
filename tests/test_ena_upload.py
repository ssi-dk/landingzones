"""ENA FTPS contracts backed by memory only; no server connections."""
from dataclasses import replace
import ftplib
import hashlib
import ssl
import traceback

import pytest

from landingzones.execution.adapters import manifest, matches
from landingzones.execution.ena import ENAAdapter, parse_destination
from landingzones.execution.model import Step
from landingzones.execution.package import LABEL


TOKEN = 'fd65c9cc-075d-4edc-9b09-5bc1204f134c'


class MemoryFTP:
    def __init__(self):
        self.directories = {'', 'incoming'}
        self.files = {}
        self.calls = []
        self.closed = False

    def connect(self, host, **kwargs):
        self.calls.append(('connect', host, kwargs))

    def login(self, username, password, **kwargs):
        self.calls.append(('login', username, kwargs))
        assert password == 'synthetic-password'

    def prot_p(self):
        self.calls.append(('prot_p',))

    def set_pasv(self, enabled):
        self.calls.append(('passive', enabled))

    def close(self):
        self.closed = True

    def mlsd(self, path='', facts=()):
        if path not in self.directories:
            raise ftplib.error_perm('550 synthetic missing directory')
        prefix = path + '/' if path else ''
        yield '.', {'type': 'cdir'}
        yield '..', {'type': 'pdir'}
        for directory in sorted(self.directories - {path}):
            name = directory[len(prefix):]
            if directory.startswith(prefix) and name and '/' not in name:
                yield name, {'type': 'dir'}
        for filename, contents in sorted(self.files.items()):
            name = filename[len(prefix):]
            if filename.startswith(prefix) and name and '/' not in name:
                yield name, {'type': 'file', 'size': str(len(contents))}

    def mkd(self, path):
        assert path.rpartition('/')[0] in self.directories
        assert path not in self.files
        self.directories.add(path)

    def storbinary(self, command, handle):
        assert command.startswith('STOR ')
        path = command[5:]
        assert path.rpartition('/')[0] in self.directories
        assert path not in self.directories
        self.calls.append(('upload', path))
        self.files[path] = handle.read()

    def retrbinary(self, command, callback):
        assert command.startswith('RETR ')
        contents = self.files[command[5:]]
        for start in range(0, len(contents), 3):
            callback(contents[start:start + 3])


@pytest.fixture
def ftp(monkeypatch):
    client = MemoryFTP()

    def factory(**kwargs):
        client.options = kwargs
        return client

    monkeypatch.setattr(ftplib, 'FTP_TLS', factory)
    monkeypatch.setenv('TEST_WEBIN_PASSWORD', 'synthetic-password')
    return client


@pytest.fixture
def transport(tmp_path, ftp):
    source = tmp_path.resolve() / 'source'
    source.mkdir()
    step = Step(identifiers='ena-test', runtime_id='test', system='example', users='operator',
                flow_group='ena-test', step_order=1, source=str(source),
                destination='ena://webin2.ebi.ac.uk/incoming/', adapter='ena',
                operation='copy', credential_ref='webin')
    settings = {'credentials': {'webin': {'username': 'Webin-12345',
                                          'password_env': 'TEST_WEBIN_PASSWORD'}}}
    return ENAAdapter(step, settings)


@pytest.fixture
def payload(transport):
    from pathlib import Path
    source = Path(transport.step.source) / 'run'
    source.mkdir()
    (source / 'nested').mkdir()
    (source / 'empty').mkdir()
    (source / 'nested' / 'reads.fastq.gz').write_bytes(b'synthetic sequence data')
    (source / 'empty-file').touch()
    (source / '.ready').touch()
    (source / LABEL).write_text('{}')
    (source / '.landing_zones').mkdir()
    (source / '.landing_zones' / 'old-label').write_text('internal')
    return source


def test_verified_tls_and_private_passive_data_channel(transport, ftp):
    with transport:
        assert ftp.options['context'].check_hostname is True
        assert ftp.options['context'].verify_mode == ssl.CERT_REQUIRED
        assert ftp.options['timeout'] == 30
        assert ftp.calls[:4] == [
            ('connect', 'webin2.ebi.ac.uk', {'port': 21, 'timeout': 30}),
            ('login', 'Webin-12345', {'secure': True}),
            ('prot_p',), ('passive', True),
        ]
    assert ftp.closed


@pytest.mark.parametrize('url', [
    'ena://example.invalid/incoming', 'ftp://webin2.ebi.ac.uk/incoming',
    'ena://Webin-12345@webin2.ebi.ac.uk/incoming', 'ena://webin2.ebi.ac.uk:21/incoming',
    'ena://webin2.ebi.ac.uk/a/../b', 'ena://webin2.ebi.ac.uk/a/./b',
    'ena://webin2.ebi.ac.uk/a//b', 'ena://webin2.ebi.ac.uk/%2e%2e',
    'ena://webin2.ebi.ac.uk/a?token=x', 'ena://webin2.ebi.ac.uk/a#x',
    'ena://webin2.ebi.ac.uk/a\\b', 'ena://webin2.ebi.ac.uk/a\r\nDELE b',
    'ena://webin2.ebi.ac.uk/a\t',
])
def test_destination_rejects_unsafe_or_non_ena_urls(url):
    with pytest.raises(ValueError):
        parse_destination(url)


def test_destination_is_account_relative_and_supports_account_home():
    assert parse_destination('ena://webin2.ebi.ac.uk/incoming/') == ('webin2.ebi.ac.uk', 'incoming')
    assert parse_destination('ena://webin.ebi.ac.uk/') == ('webin.ebi.ac.uk', '')


@pytest.mark.parametrize('change', [{'operation': 'move'}, {'verification': 'size'},
                                    {'source': 'sftp://upload@example.invalid/input'}])
def test_unsupported_routes_fail_before_connect(transport, ftp, change):
    with pytest.raises(ValueError):
        ENAAdapter(replace(transport.step, **change), transport.settings)
    assert not ftp.calls


def test_password_is_resolved_only_when_entering_transport(transport, monkeypatch, ftp):
    monkeypatch.delenv('TEST_WEBIN_PASSWORD')
    adapter = ENAAdapter(transport.step, transport.settings)
    assert adapter.stage_name(TOKEN) == 'landingzones-' + TOKEN
    with pytest.raises(ValueError, match='environment variable'):
        with adapter:
            pass
    assert not ftp.calls
    assert adapter.finalize(None, {}, {}, lambda: None) == {}


@pytest.mark.parametrize('phase', ['connect', 'login', 'prot_p', 'set_pasv', 'mlsd'])
def test_failed_setup_is_redacted_and_closed_without_fallback(transport, ftp, phase):
    def fail(*args, **kwargs):
        raise ftplib.error_perm('530 synthetic-password Webin-12345 secret command')

    setattr(ftp, phase, fail)
    with pytest.raises(OSError, match=r'ENA .* failed \(FTP 530\)') as caught:
        with transport:
            pass
    rendered = ''.join(traceback.format_exception(caught.type, caught.value, caught.tb))
    assert 'synthetic-password Webin-12345' not in rendered
    assert caught.value.__suppress_context__
    assert ftp.closed
    assert transport.ftp is None


def test_late_mlsd_failure_is_redacted(transport, ftp):
    def fail(*args, **kwargs):
        yield '.', {'type': 'cdir'}
        raise ftplib.error_perm('500 unsupported MLSD: synthetic-password')

    ftp.mlsd = fail
    with pytest.raises(OSError, match='FTP 500') as caught:
        with transport:
            pass
    assert 'synthetic-password' not in str(caught.value)
    assert ftp.closed


@pytest.mark.parametrize('token', ['../other', 'run-123', '', None])
def test_staging_requires_a_state_uuid(transport, token):
    with pytest.raises(ValueError, match='UUID'):
        transport.stage_name(token)


def test_upload_verified_receipt_has_md5_and_relative_paths_without_rename(transport, payload, ftp):
    accepted = manifest(payload)
    stage = transport.stage_name(TOKEN)
    with transport:
        assert not transport.exists(stage)
        transport.copy(payload, stage, accepted)
        assert transport.exists(stage)
        assert matches(transport.inspect(stage), accepted)
        transport.write_label(stage, {'private': 'internal identity'})
        receipt = transport.promote(stage, 'run')
        assert transport.exists(stage)  # Final upload directory is never renamed.
    root = 'incoming/' + stage
    assert receipt['remote_directory'] == root
    assert receipt['archival_status'] == 'not_submitted'
    assert set(receipt['files']) == {'empty-file', 'nested/reads.fastq.gz'}
    record = receipt['files']['nested/reads.fastq.gz']
    data = (payload / 'nested' / 'reads.fastq.gz').read_bytes()
    assert record == {'remote_path': root + '/nested/reads.fastq.gz', 'size': len(data),
                      'md5': hashlib.md5(data).hexdigest(), 'sha256': hashlib.sha256(data).hexdigest()}
    assert set(ftp.files) == {root + '/empty-file', root + '/nested/reads.fastq.gz'}
    assert manifest(payload) == accepted
    assert (payload / '.ready').exists()
    assert (payload / LABEL).exists()


def test_retry_replaces_only_partial_owned_files(transport, payload, ftp):
    accepted = manifest(payload)
    stage = transport.stage_name(TOKEN)
    upload = ftp.storbinary

    def interrupted(command, handle):
        ftp.files[command[5:]] = b'partial'
        raise OSError('synthetic-password connection lost')

    with transport:
        ftp.storbinary = interrupted
        with pytest.raises(OSError, match='ENA file upload failed') as caught:
            transport.copy(payload, stage, accepted)
        assert 'synthetic-password' not in str(caught.value)
        ftp.storbinary = upload
        transport.copy(payload, stage, accepted)
        assert transport.inspect(stage) == accepted
        path = transport.path(stage) + '/nested/reads.fastq.gz'
        ftp.files[path] = b'corrupt! sequence data!'  # Same length; hashes must differ.
        assert len(ftp.files[path]) == accepted['nested/reads.fastq.gz']['size']
        assert not matches(transport.inspect(stage), accepted)


def test_unexpected_remote_file_is_not_deleted_or_overwritten(transport, payload, ftp):
    stage = transport.stage_name(TOKEN)
    root = transport.path(stage)
    ftp.directories.add(root)
    ftp.files[root + '/foreign'] = b'leave alone'
    with transport:
        with pytest.raises(ValueError, match='unexpected content'):
            transport.copy(payload, stage, manifest(payload))
    assert ftp.files == {root + '/foreign': b'leave alone'}
    assert not any(call[0] == 'upload' for call in ftp.calls)


@pytest.mark.parametrize('filename,facts', [
    ('../escape', {'type': 'file', 'size': '1'}),
    ('link', {'type': 'OS.unix=slink'}), ('unknown', {}),
    ('file', {'type': 'file'}), ('file', {'type': 'file', 'size': '-1'}),
])
def test_unsafe_or_unsupported_remote_entries_fail_closed(transport, ftp, filename, facts):
    ftp.mlsd = lambda *args, **kwargs: iter([(filename, facts)])
    with pytest.raises(ValueError):
        with transport:
            pass
    assert ftp.closed


def test_retrieval_failure_is_not_treated_as_success(transport, payload, ftp):
    stage = transport.stage_name(TOKEN)
    with transport:
        transport.copy(payload, stage, manifest(payload))

        def fail(*args):
            raise ftplib.error_perm('500 RETR unavailable synthetic-password')

        ftp.retrbinary = fail
        with pytest.raises(OSError, match='FTP 500') as caught:
            transport.inspect(stage)
        assert 'synthetic-password' not in str(caught.value)


def test_internal_markers_cannot_be_requested_as_uploads(transport, payload, ftp):
    accepted = manifest(payload)
    accepted[LABEL] = {'kind': 'file', 'size': 2, 'sha256': 'irrelevant'}
    with transport:
        with pytest.raises(ValueError, match='unsupported content'):
            transport.copy(payload, transport.stage_name(TOKEN), accepted)
    assert not ftp.files


def test_verification_detects_changes_during_retrieval_and_discards_old_receipt(transport, payload, ftp):
    stage = transport.stage_name(TOKEN)
    with transport:
        transport.copy(payload, stage, manifest(payload))
        transport.inspect(stage)
        assert stage in transport._inspected
        ftp.retrbinary = lambda command, callback: callback(b'changed-during-download')
        with pytest.raises(ValueError, match='size changed'):
            transport.inspect(stage)
        assert stage not in transport._inspected
        with pytest.raises(ValueError, match='size changed'):
            transport.promote(stage, 'run')


def test_account_home_uploads_do_not_use_absolute_server_paths(transport, payload, ftp):
    transport = ENAAdapter(replace(transport.step, destination='ena://webin2.ebi.ac.uk/'),
                           transport.settings)
    stage = transport.stage_name(TOKEN)
    with transport:
        transport.copy(payload, stage, manifest(payload))
        assert transport.inspect(stage) == manifest(payload)
        receipt = transport.promote(stage, 'run')
    assert receipt['remote_directory'] == stage
    assert all(path.startswith(stage + '/') for path in ftp.files)
