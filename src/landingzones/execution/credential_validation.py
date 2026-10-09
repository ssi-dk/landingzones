"""Read-only connection checks under the configured runtime execution account.

This deliberately does not construct an Executor or touch receipts/event spools.
It proves authentication and directory inspection, never payload write access.
"""
from dataclasses import replace
from datetime import datetime, timezone
import os
from pathlib import Path
import pwd
import re
import socket
import subprocess
import sys

from .adapters import RemoteRsyncAdapter, SFTPAdapter
from .model import load_settings, load_steps, local_root


def _error(exc):
    """Return only an allowlisted category; replies and exception text are private."""
    cause = exc
    while cause.__cause__ is not None:
        cause = cause.__cause__
    paramiko = sys.modules.get('paramiko')
    authentication_errors = tuple(value for value in (
        getattr(paramiko, name, None) for name in
        ('AuthenticationException', 'BadAuthenticationType', 'PasswordRequiredException'))
        if isinstance(value, type))
    host_key_error = getattr(paramiko, 'BadHostKeyException', ())
    ssh_error = getattr(paramiko, 'SSHException', ())
    if isinstance(cause, ImportError):
        category = 'optional_dependency_unavailable'
    elif isinstance(cause, authentication_errors):
        category = 'authentication_failed'
    elif isinstance(cause, host_key_error):
        category = 'host_key_validation_failed'
    elif isinstance(cause, ssh_error):
        category = 'ssh_transport_or_host_key_failure'
    elif isinstance(cause, (TimeoutError, subprocess.TimeoutExpired)):
        category = 'timeout'
    elif isinstance(exc, PermissionError):
        category = 'permission_denied'
    elif isinstance(exc, FileNotFoundError):
        category = 'missing_path_or_program'
    elif isinstance(exc, subprocess.CalledProcessError):
        category = 'command_failed'
    elif isinstance(exc, (ValueError, TypeError, KeyError)):
        category = 'invalid_configuration_or_endpoint'
    elif isinstance(exc, OSError):
        category = 'connection_or_io_failure'
    else:
        category = 'validation_failed'
    return {'category': category}


def _check(checks, direction, phase, transport, credential_ref, operation):
    item = dict(direction=direction, phase=phase, transport=transport,
                credential_ref=credential_ref, status='passed')
    checks.append(item)
    try:
        result = operation()
    except Exception as exc:
        item.update(status='failed', error=_error(exc))
        return False, None
    return True, result


def _credential_settings(settings, config_dir, reference, transport):
    credentials = settings.get('credentials', {})
    if not isinstance(credentials, dict) or not isinstance(credentials.get(reference), dict):
        raise ValueError('Selected endpoint requires a configured credential reference')
    credential = dict(credentials[reference])
    if transport == 'sftp':
        for key in ('private_key_file', 'known_hosts_file'):
            value = credential.get(key)
            if not isinstance(value, str) or not value:
                raise ValueError('SFTP requires explicit key and host-key files')
            path = Path(value)
            credential[key] = str((config_dir / path).resolve()) if not path.is_absolute() else str(path)
    return dict(settings, credentials={reference: credential})


def _local_directory(path):
    root = local_root(path)
    if not Path(root).is_dir():
        raise ValueError('Local endpoint must be an existing directory')
    options = {'effective_ids': True} if os.access in os.supports_effective_ids else {}
    if not os.access(root, os.R_OK | os.X_OK, **options):
        raise PermissionError('Local endpoint is not readable and searchable')
    # One directory entry suffices. Do not open files or retain entry names.
    with os.scandir(root) as entries:
        next(entries, None)


def _sftp_directory(step, settings, endpoint):
    probe = replace(step, adapter='sftp', destination=endpoint)
    with SFTPAdapter(probe, settings) as transport:
        # __enter__ validates authentication, pinned host key and directory type.
        transport.sftp.listdir_attr(transport.root)


def _rsync_version(command):
    result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)
    if not re.search(r'^rsync\s+version\s+\d', result.stdout, re.MULTILINE):
        raise ValueError('Endpoint did not report rsync capability')


def _remote_rsync(step, settings):
    transport = RemoteRsyncAdapter(step, settings)
    host = transport.target.username + '@' + transport.target.hostname
    # The remote command is constant. Local argv is never passed to a shell.
    _rsync_version(transport.ssh_command() + ['--', host, 'rsync --version'])


def _ena_directory(step, settings):
    # ENA and its credentials remain entirely optional until this route is selected.
    from .ena import ENAAdapter
    with ENAAdapter(step, settings) as transport:
        transport._entries(transport.root)


def _endpoint(checks, step, settings, config_dir, direction):
    endpoint = step.source if direction == 'source' else step.destination
    is_ena = direction == 'destination' and step.adapter == 'ena'
    remote = endpoint.startswith(('sftp://', 'ssh://'))
    if not remote and not is_ena:
        _check(checks, direction, 'local_directory_read', 'local', '',
               lambda: _local_directory(endpoint))
        checks[-1]['credential_check'] = 'not_required'
        return
    transport = 'ena_ftps' if is_ena else 'sftp'
    valid, resolved = _check(
        checks, direction, 'credential_configuration', transport, step.credential_ref,
        lambda: _credential_settings(settings, config_dir, step.credential_ref,
                                     'ena' if is_ena else 'sftp'),
    )
    if not valid:
        return
    if is_ena:
        _check(checks, direction, 'authentication_and_root_listing', transport,
               step.credential_ref, lambda: _ena_directory(step, resolved))
    else:
        _check(checks, direction, 'authentication_host_key_and_root_listing', transport,
               step.credential_ref, lambda: _sftp_directory(step, resolved, endpoint))
        if direction == 'destination' and step.adapter == 'rsync':
            _check(checks, direction, 'ssh_authentication_and_rsync_capability', 'rsync_ssh',
                   step.credential_ref, lambda: _remote_rsync(step, resolved))


def validate_credentials(config_path, connection=None):
    """Check enabled Python connections without starting transfers or writing state.

    ``connection`` selects one configured flow_group. All failures inside a selected
    endpoint are sanitized and collected; one route cannot prevent another probe.
    A passed report excludes the capabilities explicitly listed as ``untested``.
    """
    report = {
        'status': 'failed',
        'checked_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'execution_context': {'system': socket.gethostname(), 'user': pwd.getpwuid(os.geteuid()).pw_name},
        'runtime_ids': [], 'connections': [],
        'scope': 'credentials_and_directory_read_access',
    }
    try:
        # Normalize only credentials used by the selected routes, below.
        settings = load_settings(config_path, resolve_credentials=False)
        report['runtime_ids'] = settings['runtime_ids']
        groups = load_steps(settings, validate_credentials=False)
    except Exception:
        raise ValueError('Credential validation requires valid configuration for the actual execution account') from None
    if connection is not None:
        if connection not in groups:
            raise ValueError('Unknown connection for credential validation')
        groups = {connection: groups[connection]}
    if not groups:
        report['errors'] = [{'phase': 'configuration', 'category': 'no_enabled_connections'}]
        return report
    config_dir = Path(config_path).resolve().parent
    for group in groups.values():
        step = group[0]
        item = dict(connection=step.flow_group, identifier=step.identifiers,
                    runtime_id=step.runtime_id, transfer_method=step.adapter,
                    credential_ref=step.credential_ref, operation=step.operation,
                    status='passed', checks=[],
                    untested=['payload_readability', 'destination_write_permissions',
                              'source_delete_permissions', 'transfer_and_integrity'])
        report['connections'].append(item)
        checks = item['checks']
        _endpoint(checks, step, settings, config_dir, 'source')
        _endpoint(checks, step, settings, config_dir, 'destination')
        if step.adapter == 'rsync':
            _check(checks, 'execution_account', 'local_rsync_capability', 'rsync', '',
                   lambda: _rsync_version(['rsync', '--version']))
        if step.adapter == 'ena':
            item['untested'].extend(['metadata_api_authentication', 'metadata_submission',
                                     'accession_receipt', 'upload_delete_permissions'])
        if any(check['status'] == 'failed' for check in checks):
            item['status'] = 'failed'
    report['status'] = 'failed' if any(item['status'] == 'failed' for item in report['connections']) else 'passed'
    return report
