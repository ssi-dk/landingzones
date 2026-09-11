"""Validated configuration for the first Python-owned transfer paths."""
import csv
from dataclasses import asdict, dataclass
import os
from pathlib import Path
import pwd
import re
import socket
from urllib.parse import urlsplit

import yaml


@dataclass(frozen=True)
class Step:
    identifiers: str
    runtime_id: str
    system: str
    users: str
    flow_group: str
    step_order: int
    source: str
    destination: str
    adapter: str
    operation: str
    credential_ref: str = ''
    verification: str = 'checksum'
    readiness_policy: str = 'producer_marker'
    max_attempts: int = 3
    admission_policy: str = 'strict'

    def record(self):
        return asdict(self)


def name(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', value):
        raise ValueError('Payload name must be one safe path component')
    return value


def local_root(value):
    path = Path(value)
    if not path.is_absolute() or '..' in path.parts or any(c in str(path) for c in '\n\r\0'):
        raise ValueError('Expected an absolute local root without traversal')
    # Reject symlink roots and symlink ancestors, including paths not yet created.
    if path.resolve() != path:
        raise ValueError('Symlink or noncanonical root: ' + str(path))
    return str(path)


def remote_url(value, scheme='sftp'):
    parsed = urlsplit(value)
    if (parsed.scheme != scheme or not parsed.hostname or not parsed.username
            or parsed.password or parsed.query or parsed.fragment or not parsed.path.startswith('/')
            or '..' in Path(parsed.path).parts or any(c in value for c in '\n\r\0')):
        raise ValueError('Expected sftp://user@host:port/absolute/root')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', parsed.username) or not re.fullmatch(r'[A-Za-z0-9_.:-]+', parsed.hostname):
        raise ValueError('Invalid remote account or hostname')
    return parsed


def load_settings(config_file):
    path = Path(config_file).resolve()
    with path.open() as handle:
        settings = yaml.safe_load(handle)
    if not isinstance(settings, dict) or settings.get('execution_schema_version') != 1:
        raise ValueError('Python execution requires execution_schema_version: 1')
    for key in ('transfers_file', 'state_dir', 'event_spool'):
        value = Path(settings[key])
        settings[key] = str((path.parent / value).resolve()) if not value.is_absolute() else str(value)
    for credential in settings.get('credentials', {}).values():
        for key in ('private_key_file', 'known_hosts_file'):
            value = Path(credential[key])
            credential[key] = str((path.parent / value).resolve()) if not value.is_absolute() else str(value)
    context = settings['execution_context']
    if context != {'system': socket.gethostname(), 'user': pwd.getpwuid(os.geteuid()).pw_name}:
        raise ValueError('Execution context must match the actual local hostname and effective user')
    if not settings.get('runtime_ids'):
        raise ValueError('Explicit runtime_ids are required')
    return settings


def load_steps(settings):
    selected = []
    identities = set()
    with open(settings['transfers_file']) as handle:
        rows = csv.DictReader((line for line in handle if not line.startswith('#')), delimiter='\t')
        for row in rows:
            if None in row or any(v is None for v in row.values()):
                raise ValueError('Malformed transfer table')
            if row.get('enabled', '').upper() != 'TRUE' or row.get('executor', 'legacy') != 'python':
                continue
            if row['runtime_id'] not in settings['runtime_ids']:
                continue
            if (row['system'], row['users']) != (settings['execution_context']['system'], settings['execution_context']['user']):
                raise ValueError('Selected route is owned by another execution context')
            values = {key: row[key] for key in Step.__dataclass_fields__ if row.get(key)}
            values['step_order'] = int(values['step_order'])
            values['max_attempts'] = int(values.get('max_attempts', 3))
            step = Step(**values)
            identity = (step.runtime_id, step.identifiers)
            if identity in identities:
                raise ValueError('Duplicate step identity')
            identities.add(identity)
            if step.adapter not in ('local', 'rsync', 'sftp') or step.operation not in ('copy', 'move'):
                raise ValueError('Unsupported adapter or operation')
            if step.operation == 'move' and step.verification != 'checksum':
                raise ValueError('Moves require checksum verification before source deletion')
            if step.verification not in ('checksum', 'size') or step.max_attempts < 1:
                raise ValueError('Invalid verification or attempt policy')
            if row.get('destination_exists', 'fail') not in ('', 'fail'):
                raise ValueError('Only destination_exists=fail is supported')
            if step.admission_policy not in ('strict', 'relabel'):
                raise ValueError('Unsupported admission policy')
            if step.readiness_policy not in ('producer_marker', 'managed_ready'):
                raise ValueError('Unsupported readiness policy; archive routes must remain legacy')
            if row.get('rsync_options') or any(row.get(field, '').upper() == 'TRUE' for field in ('is_entry_point', 'is_end_point')):
                raise ValueError('Python paths do not accept legacy archive preparation or rsync options')
            if row.get('trigger', '') not in ('', 'request', 'discovery'):
                raise ValueError('Python routes currently support explicit requests only')
            if row.get('source_port') or row.get('destination_port'):
                raise ValueError('Use a port in the SFTP URL; legacy port columns are unsupported')
            if step.source.startswith(('sftp://', 'ssh://')):
                remote_url(step.source, 'sftp' if step.source.startswith('sftp://') else 'ssh')
                if step.credential_ref not in settings.get('credentials', {}):
                    raise ValueError('Missing source credential reference')
            else:
                local_root(step.source)
            if step.adapter == 'sftp' or step.destination.startswith('ssh://'):
                remote_url(step.destination, 'sftp' if step.adapter == 'sftp' else 'ssh')
                if step.adapter not in ('sftp', 'rsync'):
                    raise ValueError('Remote destination requires sftp or rsync')
            else:
                local_root(step.destination)
            if (step.adapter == 'sftp' or step.destination.startswith('ssh://')) and step.credential_ref not in settings.get('credentials', {}):
                raise ValueError('Missing SFTP credential reference')
            selected.append(step)
    groups = {}
    for step in selected:
        groups.setdefault(step.flow_group, []).append(step)
    # A flow group now selects one locally owned connection, not an itinerary.
    for group in groups.values():
        if len(group) != 1:
            raise ValueError('One connection per flow_group is required; split journey steps into independent groups')
    moves = {}
    for step in selected:
        moves.setdefault(step.source, []).append(step)
    if any(len(group) > 1 and any(s.operation == 'move' for s in group) for group in moves.values()):
        raise ValueError('Conflicting shared-source move connections; use separate input folders')
    return groups
