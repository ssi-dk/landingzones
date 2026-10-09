"""Credential probes use real configuration selection and mocked remote IO only."""
import builtins
import csv
from dataclasses import fields
import json
import os
from pathlib import Path
import pwd
import socket
import subprocess
import sys
from types import SimpleNamespace

import pytest

from landingzones.execution import credential_validation as validation
from landingzones.execution.model import Step


@pytest.fixture
def config(tmp_path):
    source, destination = tmp_path / 'source', tmp_path / 'destination'
    source.mkdir()
    destination.mkdir()
    context = {'system': socket.gethostname(), 'user': pwd.getpwuid(os.geteuid()).pw_name}

    def row(name, **changes):
        result = dict(identifiers=name, runtime_id='selected-runtime', system=context['system'],
                      users=context['user'], enabled='TRUE', executor='python',
                      flow_group=name, step_order='1', source=str(source),
                      destination=str(destination), adapter='local', operation='copy')
        result.update(changes)
        return result

    def write(rows, credentials=None, **changes):
        table = tmp_path / 'transfers.tsv'
        columns = [field.name for field in fields(Step)] + ['enabled', 'executor']
        with table.open('w') as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, delimiter='\t')
            writer.writeheader()
            writer.writerows(rows)
        settings = dict(execution_schema_version=1, transfers_file='transfers.tsv',
                        state_dir='state', event_spool='events.tsv',
                        execution_context=context, runtime_ids=['selected-runtime'],
                        credentials=credentials or {})
        settings.update(changes)
        path = tmp_path / 'execution.yaml'
        path.write_text(json.dumps(settings))
        return path

    return SimpleNamespace(row=row, write=write, root=tmp_path, source=source,
                           destination=destination, context=context)


@pytest.fixture
def remote(monkeypatch):
    calls, processes, failures = [], [], {}

    class SFTP:
        def __init__(self, step, settings):
            self.root = step.destination
            self.credentials = settings['credentials'][step.credential_ref]
            self.sftp = self
            self.step = step

        def __enter__(self):
            calls.append(('enter', self.step.destination, self.step.credential_ref, self.credentials))
            if self.step.destination in failures:
                raise failures[self.step.destination]
            return self

        def __exit__(self, *args):
            calls.append(('close', self.step.destination))

        def listdir_attr(self, root):
            assert root == self.root
            calls.append(('list', root))
            return []

    def run(argv, **options):
        processes.append((argv, options))
        return SimpleNamespace(stdout='rsync  version 3.2.7  protocol version 31\n')

    monkeypatch.setattr(validation, 'SFTPAdapter', SFTP)
    monkeypatch.setattr(validation.subprocess, 'run', run)
    return SimpleNamespace(calls=calls, processes=processes, failures=failures)


def credentials(*names):
    return {name: dict(private_key_file=name + '.key', known_hosts_file=name + '.hosts') for name in names}


def failed_checks(report):
    return [check for route in report['connections'] for check in route['checks'] if check['status'] == 'failed']


def test_checks_every_selected_connection_and_both_remote_endpoints(config, remote):
    rows = [config.row('first', adapter='sftp', source='ssh://reader@source.invalid/root',
                       destination='sftp://writer@first.invalid/root', credential_ref='one'),
            config.row('second', adapter='sftp', destination='sftp://writer@second.invalid/root', credential_ref='two')]
    path = config.write(rows, credentials('one', 'two'))
    report = validation.validate_credentials(path)
    assert report['status'] == 'passed'
    assert report['execution_context'] == config.context
    assert report['runtime_ids'] == ['selected-runtime']
    entered = [item for item in remote.calls if item[0] == 'enter']
    assert [(item[1], item[2]) for item in entered] == [
        ('ssh://reader@source.invalid/root', 'one'),
        ('sftp://writer@first.invalid/root', 'one'),
        ('sftp://writer@second.invalid/root', 'two')]
    assert entered[0][3]['private_key_file'] == str(config.root / 'one.key')
    assert entered[-1][3]['known_hosts_file'] == str(config.root / 'two.hosts')
    assert len([item for item in remote.calls if item[0] == 'list']) == 3
    assert not remote.processes
    assert not (config.root / 'state').exists()
    assert not (config.root / 'events.tsv').exists()


def test_failed_credentials_and_multiple_endpoint_failures_do_not_stop_other_routes(config, remote):
    denied = 'sftp://reader@denied.invalid/root'
    unavailable = 'sftp://writer@unavailable.invalid/root'
    rows = [config.row('missing', adapter='sftp', destination='sftp://writer@missing.invalid/root', credential_ref='missing'),
            config.row('broken', adapter='sftp', source=denied, destination=unavailable, credential_ref='one'),
            config.row('works', adapter='sftp', destination='sftp://writer@good.invalid/root', credential_ref='two')]
    remote.failures[denied] = PermissionError('password=SECRET source/server-private-name')
    remote.failures[unavailable] = OSError('SECRET private-key-file ssh message')
    report = validation.validate_credentials(config.write(rows, credentials('one', 'two')))
    assert report['status'] == 'failed'
    assert [item['status'] for item in report['connections']] == ['failed', 'failed', 'passed']
    assert len(failed_checks(report)) == 3
    rendered = json.dumps(report)
    for private in ('SECRET', 'server-private-name', 'private-key-file', denied, unavailable):
        assert private not in rendered
    assert {item['error']['category'] for item in failed_checks(report)} == {
        'invalid_configuration_or_endpoint', 'permission_denied', 'connection_or_io_failure'}


def test_disabled_unselected_runtime_and_unused_malformed_credentials_are_ignored(config, remote, monkeypatch):
    rows = [config.row('local'),
            config.row('disabled', adapter='ena', destination='ena://webin2.ebi.ac.uk/incoming', enabled='FALSE'),
            config.row('other', adapter='sftp', destination='sftp://x@other.invalid/root', runtime_id='other-runtime',
                       credential_ref='missing'),
            config.row('ena', adapter='ena', destination='ena://webin2.ebi.ac.uk/incoming', credential_ref='unused')]
    path = config.write(rows, {'unused': None, 'bad-path': {'private_key_file': None}})
    original = builtins.__import__

    def without_optional(name, *args, **kwargs):
        if name == 'paramiko' or name in ('ena', 'ena_submission') or name.endswith(('.ena', '.ena_submission')):
            raise AssertionError('Unused optional transport imported')
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', without_optional)
    report = validation.validate_credentials(path, connection='local')
    assert report['status'] == 'passed'
    assert [item['connection'] for item in report['connections']] == ['local']
    assert not remote.calls and not remote.processes
    assert all(item['credential_check'] == 'not_required' for item in report['connections'][0]['checks'])


def test_rsync_checks_product_openssh_options_remote_capability_and_local_binary(config, remote):
    row = config.row('rsync', adapter='rsync', destination='ssh://writer@remote.invalid:2222/root', credential_ref='one')
    report = validation.validate_credentials(config.write([row], credentials('one')))
    assert report['status'] == 'passed'
    assert len(remote.processes) == 2
    argv, options = remote.processes[0]
    assert argv[:3] == ['ssh', '-p', '2222']
    assert argv[argv.index('-i') + 1] == str(config.root / 'one.key')
    assert 'IdentitiesOnly=yes' in argv and 'StrictHostKeyChecking=yes' in argv
    assert 'UserKnownHostsFile=' + str(config.root / 'one.hosts') in argv
    assert argv[-3:] == ['--', 'writer@remote.invalid', 'rsync --version']
    assert options == dict(check=True, capture_output=True, text=True, timeout=30)
    assert remote.processes[1][0] == ['rsync', '--version']
    assert len([item for item in remote.calls if item[0] == 'enter']) == 1


def test_rsync_does_not_confuse_sftp_success_with_shell_or_binary_success(config, remote, monkeypatch):
    row = config.row('rsync', adapter='rsync', destination='ssh://writer@remote.invalid/root', credential_ref='one')

    def denied(argv, **kwargs):
        if argv[0] == 'ssh':
            raise subprocess.CalledProcessError(255, argv, output='SECRET', stderr='private reply')
        raise FileNotFoundError('/private/path/to/rsync')

    monkeypatch.setattr(validation.subprocess, 'run', denied)
    report = validation.validate_credentials(config.write([row], credentials('one')))
    assert report['status'] == 'failed'
    assert {check['phase'] for check in failed_checks(report)} == {
        'ssh_authentication_and_rsync_capability', 'local_rsync_capability'}
    assert 'SECRET' not in json.dumps(report)
    assert '/private/path' not in json.dumps(report)


def test_rsync_rejects_unrelated_successful_forced_command(config, remote, monkeypatch):
    row = config.row('rsync', adapter='rsync')
    monkeypatch.setattr(validation.subprocess, 'run', lambda *args, **kwargs: SimpleNamespace(stdout='unrelated command'))
    report = validation.validate_credentials(config.write([row]))
    assert report['status'] == 'failed'
    assert failed_checks(report)[0]['phase'] == 'local_rsync_capability'


def test_ena_only_authenticates_and_lists_configured_root(config, remote, monkeypatch):
    from landingzones.execution import ena
    calls = []

    class ENA:
        def __init__(self, step, settings):
            assert settings['credentials']['webin']['username'] == 'Webin-12345'
            self.root = 'incoming'

        def __enter__(self):
            calls.append('authenticate-and-root')
            return self

        def __exit__(self, *args):
            calls.append('close')

        def _entries(self, path):
            calls.append(('list', path))
            return {}

    monkeypatch.setattr(ena, 'ENAAdapter', ENA)
    row = config.row('ena', adapter='ena', destination='ena://webin2.ebi.ac.uk/incoming', credential_ref='webin')
    report = validation.validate_credentials(config.write([row], {'webin': {'username': 'Webin-12345', 'password_env': 'UNSET'}}))
    assert report['status'] == 'passed'
    assert calls == ['authenticate-and-root', ('list', 'incoming'), 'close']
    assert {'metadata_api_authentication', 'metadata_submission', 'accession_receipt',
            'destination_write_permissions', 'upload_delete_permissions'} <= set(report['connections'][0]['untested'])
    assert not remote.calls and not remote.processes
    assert not (config.root / 'state').exists() and not (config.root / 'events.tsv').exists()


def test_ena_missing_credentials_fail_only_ena_route(config, remote):
    rows = [config.row('ena', adapter='ena', destination='ena://webin2.ebi.ac.uk/incoming', credential_ref='missing'),
            config.row('local')]
    report = validation.validate_credentials(config.write(rows))
    assert report['status'] == 'failed'
    assert [item['status'] for item in report['connections']] == ['failed', 'passed']
    assert failed_checks(report)[0]['phase'] == 'credential_configuration'


@pytest.mark.parametrize('failure', [None, 'missing_password', 'rejected_login', 'root_listing'])
def test_real_ena_authentication_and_root_checks_are_read_only(config, remote, monkeypatch, failure):
    import ftplib
    from landingzones.execution import ena_submission
    from tests.test_ena_upload import MemoryFTP

    client = MemoryFTP()
    construction, listing = [], []

    def create(**kwargs):
        construction.append(kwargs)
        return client

    original_login, original_listing = client.login, client.mlsd

    def login(*args, **kwargs):
        original_login(*args, **kwargs)
        if failure == 'rejected_login':
            raise ftplib.error_perm('530 SECRET account rejected')

    def mlsd(path='', facts=()):
        listing.append(path)
        if failure == 'root_listing' and path == 'incoming':
            raise ftplib.error_perm('550 SECRET private-directory denied')
        return original_listing(path, facts=facts)

    def forbidden(*args, **kwargs):
        pytest.fail('Credential validation attempted payload IO or metadata HTTP')

    monkeypatch.setattr(ftplib, 'FTP_TLS', create)
    monkeypatch.setattr(client, 'login', login)
    monkeypatch.setattr(client, 'mlsd', mlsd)
    for method in ('mkd', 'storbinary', 'retrbinary'):
        monkeypatch.setattr(client, method, forbidden)
    monkeypatch.setattr(ena_submission.request, 'build_opener', forbidden)
    monkeypatch.setattr(ena_submission, '_post', forbidden)
    if failure == 'missing_password':
        monkeypatch.delenv('TEST_VALIDATION_WEBIN_PASSWORD', raising=False)
    else:
        monkeypatch.setenv('TEST_VALIDATION_WEBIN_PASSWORD', 'synthetic-password')
    rows = [config.row('ena', adapter='ena', destination='ena://webin2.ebi.ac.uk/incoming', credential_ref='webin'),
            config.row('local')]
    path = config.write(rows, {'webin': {'username': 'Webin-12345', 'password_env': 'TEST_VALIDATION_WEBIN_PASSWORD'}})
    report = validation.validate_credentials(path)
    assert [item['status'] for item in report['connections']] == ['failed' if failure else 'passed', 'passed']
    assert report['status'] == ('failed' if failure else 'passed')
    assert 'SECRET' not in json.dumps(report)
    assert 'private-directory' not in json.dumps(report)
    assert 'synthetic-password' not in json.dumps(report)
    if failure == 'missing_password':
        assert not construction and not client.calls
    else:
        assert len(construction) == 1
        assert client.closed
    if failure in (None, 'root_listing'):
        assert listing == ['', 'incoming']
    assert client.files == {}
    assert client.directories == {'', 'incoming'}
    assert not (config.root / 'state').exists() and not (config.root / 'events.tsv').exists()


def test_local_checks_directory_readability_without_creating_destination(config, remote):
    absent = config.root / 'not-created'
    report = validation.validate_credentials(config.write([config.row('local', destination=str(absent))]))
    assert report['status'] == 'failed'
    assert not absent.exists()
    assert failed_checks(report)[0]['direction'] == 'destination'


def test_actual_execution_context_is_required_and_error_is_sanitized(config, remote):
    path = config.write([config.row('local')], execution_context={'system': 'wrong', 'user': 'SECRET'})
    with pytest.raises(ValueError, match='actual execution account') as caught:
        validation.validate_credentials(path)
    assert 'SECRET' not in str(caught.value)
    assert caught.value.__suppress_context__
    assert not remote.calls and not remote.processes


def test_empty_selection_fails_and_unknown_connection_is_configuration_error(config, remote):
    path = config.write([config.row('disabled', enabled='FALSE')])
    report = validation.validate_credentials(path)
    assert report['status'] == 'failed'
    assert report['connections'] == []
    assert report['errors'][0]['category'] == 'no_enabled_connections'
    with pytest.raises(ValueError, match='Unknown connection'):
        validation.validate_credentials(path, connection='absent')


@pytest.mark.parametrize('exception_name,category', [
    ('AuthenticationException', 'authentication_failed'),
    ('BadHostKeyException', 'host_key_validation_failed'),
    ('SSHException', 'ssh_transport_or_host_key_failure'),
])
def test_wrapped_paramiko_failures_remain_actionable_and_secret_free(config, remote, monkeypatch, exception_name, category):
    ssh_error = type('SSHException', (Exception,), {})
    authentication = type('AuthenticationException', (ssh_error,), {})
    bad_host = type('BadHostKeyException', (ssh_error,), {})
    module = SimpleNamespace(SSHException=ssh_error, AuthenticationException=authentication, BadHostKeyException=bad_host)
    monkeypatch.setitem(sys.modules, 'paramiko', module)
    inner = getattr(module, exception_name)('SECRET returned by endpoint')
    outer = OSError('SECRET wrapper')
    outer.__cause__ = inner
    url = 'sftp://writer@bad.invalid/root'
    remote.failures[url] = outer
    row = config.row('sftp', adapter='sftp', destination=url, credential_ref='one')
    report = validation.validate_credentials(config.write([row], credentials('one')))
    assert failed_checks(report)[0]['error']['category'] == category
    assert 'SECRET' not in json.dumps(report)
