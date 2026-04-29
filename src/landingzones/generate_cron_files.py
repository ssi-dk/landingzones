#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate cron files from transfers.tsv configuration.

Creates a .cron file for each system-user combination with rsync commands.
"""

import os
import sys
import argparse
import csv
import re
import shlex

from landingzones.config import config
from landingzones.table import TransferTable
from landingzones.transfer_definitions import (
    definitions_from_dataframe,
    normalize_tags_text,
)


VALIDATION_HELPER_NAME = 'lz_run_validation.sh'
VALIDATION_WRAPPER_PREFIX = 'lz_run_validation_'
VALIDATION_TEMPLATE_NAME = 'lz_run_validation.sh'
OWNER_MARKER_PREFIX = '# landingzones-owner:'
PATH_VARIABLE_PATTERN = re.compile(r'\$\{([A-Za-z_][A-Za-z0-9_]*)\}')


def normalize_bool_text(value):
    """Normalize common TSV boolean spellings to TRUE/FALSE strings."""
    text = str(value).strip().upper() if value is not None else ''
    if not text or text == 'NAN':
        return 'FALSE'
    if text in ('TRUE', 'T', 'YES', 'Y', '1'):
        return 'TRUE'
    if text in ('FALSE', 'F', 'NO', 'N', '0'):
        return 'FALSE'
    raise ValueError("Unsupported boolean value: {0}".format(value))


def ensure_text_column(df, column_name, default=''):
    """Ensure a normalized text column exists on a transfer table."""
    if column_name not in df.columns:
        df[column_name] = default
    df[column_name] = [
        clean_tsv_value(value, default=default)
        for value in df[column_name]
    ]


def clean_tsv_value(value, default=''):
    """Normalize an optional TSV value to stripped text."""
    if value is None:
        return default
    text = str(value).strip()
    if text == 'nan':
        return default
    return text


def legacy_runtime_id(row):
    """Return the legacy runtime identity derived from system and user columns."""
    system = clean_tsv_value(row.get('system', ''))
    user = clean_tsv_value(row.get('users', row.get('user', '')))
    if system and user:
        return "{0}.{1}".format(system, user)
    return system or user


def ensure_runtime_id_column(rows, columns):
    """Ensure every row has a stored runtime_id identity."""
    if 'runtime_id' not in columns:
        insert_at = columns.index('identifiers') + 1 if 'identifiers' in columns else 0
        columns.insert(insert_at, 'runtime_id')
        for row in rows:
            row['runtime_id'] = legacy_runtime_id(row)
    else:
        for row in rows:
            row['runtime_id'] = clean_tsv_value(row.get('runtime_id', ''))


def validate_runtime_ids(rows):
    """Validate runtime_id values used for artifact grouping and filtering."""
    missing = [
        row.get('identifiers', '<unknown>')
        for row in rows
        if not clean_tsv_value(row.get('runtime_id', ''))
    ]
    if missing:
        raise ValueError(
            "runtime_id is required for all enabled transfers: {0}".format(
                ', '.join(missing)
            )
        )

    invalid = sorted({
        row.get('runtime_id', '')
        for row in rows
        if sanitize_identifier(row.get('runtime_id', '')) != row.get('runtime_id', '')
    })
    if invalid:
        raise ValueError(
            "runtime_id values must be filename-safe: {0}".format(
                ', '.join(invalid)
            )
        )


def parse_transfers_file(filename, require_runtime_files=True, runtime_ids=None, systems=None):
    """Parse the transfers.tsv file and return normalized transfer records.

    Args:
        filename: Path to a transfers.tsv file.
        require_runtime_files: When True, keep generator/runtime validation that
            requires fields such as log_file. When False, parse only the shared
            transfer metadata needed for reporting/analysis.
        runtime_ids: Optional exact runtime_id values to include before runtime
            path validation and artifact generation.
        systems: Optional exact system values to include before endpoint
            expansion.
    """
    with open(filename, 'r', newline='') as handle:
        reader = csv.DictReader(handle, delimiter='\t')
        columns = list(reader.fieldnames or [])
        rows = [
            {
                column: clean_tsv_value(row.get(column, ''))
                for column in columns
            }
            for row in reader
        ]

    rows = [
        row for row in rows
        if not clean_tsv_value(row.get('runtime_id', '')).startswith('#')
        and not clean_tsv_value(row.get('system', '')).startswith('#')
    ]

    if 'enabled' in columns:
        rows = [
            row for row in rows
            if clean_tsv_value(row.get('enabled', '')).upper() == 'TRUE'
        ]

    if 'identifiers' not in columns:
        columns.insert(0, 'identifiers')
        for index, row in enumerate(rows, start=1):
            row['identifiers'] = "transfer_{0:03d}".format(index)

    ensure_runtime_id_column(rows, columns)
    validate_runtime_ids(rows)

    requested_runtime_ids = normalize_runtime_id_filters(runtime_ids)
    if requested_runtime_ids:
        available = set(row.get('runtime_id', '') for row in rows)
        missing = sorted(set(requested_runtime_ids) - available)
        if missing:
            raise ValueError(
                "runtime_id filter matched no transfer rows for: {0}".format(
                    ', '.join(missing)
                )
            )
        rows = [
            row for row in rows
            if row.get('runtime_id', '') in requested_runtime_ids
        ]
        if not rows:
            raise ValueError("runtime_id filter produced no transfer rows")

    requested_systems = normalize_system_filters(systems)
    if requested_systems:
        available = set(row.get('system', '') for row in rows)
        missing = sorted(set(requested_systems) - available)
        if missing:
            raise ValueError(
                "system filter matched no transfer rows for: {0}".format(
                    ', '.join(missing)
                )
            )
        rows = [
            row for row in rows
            if row.get('system', '') in requested_systems
        ]
        if not rows:
            raise ValueError("system filter produced no transfer rows")

    text_columns = (
        'runtime_id',
        'rsync_options',
        'log_file',
        'flock_file',
        'io_nice',
        'frequency',
        'flow_group',
        'tags',
        'destination_port',
        'source_port',
    )
    for column in text_columns:
        if column not in columns:
            columns.append(column)
        for row in rows:
            row[column] = clean_tsv_value(row.get(column, ''))

    for row in rows:
        for field_name in ('source', 'destination'):
            row[field_name] = expand_transfer_endpoint(
                row.get(field_name, ''),
                config.path_variables,
                row.get('identifiers', ''),
                field_name,
            )
        row['system_user'] = row.get('runtime_id', '')
        row['tags'] = normalize_tags_text(row.get('tags', ''))
        for bool_column in (
            'is_entry_point',
            'is_end_point',
            'notify_on_success',
            'notify_on_error',
        ):
            if bool_column not in columns:
                columns.append(bool_column)
            row[bool_column] = normalize_bool_text(row.get(bool_column, 'FALSE'))
        row['destination_port'] = re.sub(r'\.0$', '', row.get('destination_port', ''))

    if 'system_user' not in columns:
        columns.append('system_user')

    identifiers = [row.get('identifiers', '') for row in rows]
    if any(identifier == '' for identifier in identifiers):
        raise ValueError("identifiers is required for all enabled transfers")
    if require_runtime_files and any(row.get('log_file', '') == '' for row in rows):
        raise ValueError("log_file is required for all enabled transfers")

    sanitized_identifiers = [sanitize_identifier(identifier) for identifier in identifiers]
    if any(identifier == '' for identifier in sanitized_identifiers):
        raise ValueError("identifiers must contain at least one filename-safe character")
    duplicate_sanitized = {
        identifier for identifier in sanitized_identifiers
        if sanitized_identifiers.count(identifier) > 1
    }
    if duplicate_sanitized:
        duplicates = sorted({
            row.get('identifiers', '')
            for row, sanitized in zip(rows, sanitized_identifiers)
            if sanitized in duplicate_sanitized
        })
        raise ValueError(
            "identifiers must be unique after filename sanitization: {0}".format(
                ', '.join(duplicates)
            )
        )

    for row, sanitized in zip(rows, sanitized_identifiers):
        row['script_name'] = "{0}.sh".format(prefixed_artifact_stem(sanitized))
    if 'script_name' not in columns:
        columns.append('script_name')

    df = TransferTable(rows, columns=columns)
    validate_transfer_endpoints(df)
    validate_flow_metadata(df)
    df = resolve_transfer_file_paths(df)
    df.attrs['shared_file_pair_warnings'] = audit_shared_file_pairs(df)
    
    return df


def normalize_runtime_id_filters(runtime_ids):
    """Normalize CLI-provided runtime_id filters."""
    if not runtime_ids:
        return []
    normalized = []
    for runtime_id in runtime_ids:
        value = clean_tsv_value(runtime_id)
        if value:
            normalized.append(value)
    return normalized


def normalize_system_filters(systems):
    """Normalize CLI-provided system filters."""
    if not systems:
        return []
    if isinstance(systems, str):
        systems = [systems]
    normalized = []
    for system in systems:
        value = clean_tsv_value(system)
        if value and value not in normalized:
            normalized.append(value)
    return normalized


def filter_transfers_by_runtime_ids(transfers_df, runtime_ids):
    """Return only transfers matching exact runtime_id values."""
    requested = normalize_runtime_id_filters(runtime_ids)
    if not requested:
        return transfers_df.copy()

    available = set(str(value) for value in transfers_df['runtime_id'].dropna())
    missing = sorted(set(requested) - available)
    if missing:
        raise ValueError(
            "runtime_id filter matched no transfer rows for: {0}".format(
                ', '.join(missing)
            )
        )

    filtered = transfers_df[transfers_df['runtime_id'].isin(requested)].copy()
    if filtered.empty:
        raise ValueError("runtime_id filter produced no transfer rows")
    return filtered


def normalize_source_path(source):
    """Normalize a source path for comparison.
    
    Strips trailing wildcards and slashes to get the base directory.
    """
    path = source.strip()
    # Remove trailing wildcard patterns
    if path.endswith('/*'):
        path = path[:-2]
    elif path.endswith('*'):
        path = path[:-1]
    # Remove trailing slash
    path = path.rstrip('/')
    return path


def normalize_io_nice(io_nice):
    """Normalize io_nice input to a shell command prefix or empty string."""
    value = str(io_nice).strip() if io_nice is not None else ''
    if not value or value == 'nan':
        return ''
    if value.startswith('ionice'):
        return value
    return "ionice {0}".format(value)


def transfer_uses_portable_metadata(transfer):
    """Return True when a transfer participates in portable run tracking."""
    flow_group = str(transfer.get('flow_group', '') or '').strip()
    return bool(flow_group and flow_group != 'nan')


def sanitize_identifier(identifier):
    """Convert a transfer identifier into a safe shell script file name stem."""
    value = str(identifier).strip() if identifier is not None else ''
    if not value or value == 'nan':
        return ''
    value = re.sub(r'[^A-Za-z0-9._-]+', '_', value)
    return value.strip('._-')


def configured_artifact_prefix():
    """Return the sanitized configured generated-artifact prefix."""
    return sanitize_identifier(config.artifact_prefix)


def prefixed_artifact_stem(stem):
    """Apply the configured artifact prefix to a filename stem."""
    prefix = configured_artifact_prefix()
    if not prefix:
        return stem
    return "{0}__{1}".format(prefix, stem)


def current_artifact_owner_id():
    """Return the configured artifact owner marker for shared output cleanup."""
    return str(config.artifact_owner_id or '').strip()


def add_owner_marker(content):
    """Insert the generated-artifact owner marker into file content."""
    owner_id = current_artifact_owner_id()
    if not owner_id:
        return content

    marker = "{0} {1}\n".format(OWNER_MARKER_PREFIX, owner_id)
    lines = content.splitlines(True)
    if lines and lines[0].startswith('#!'):
        return ''.join([lines[0], marker] + lines[1:])
    return marker + content


def file_has_current_owner_marker(path):
    """Return True when a generated file belongs to the current owner context."""
    owner_id = current_artifact_owner_id()
    if not owner_id:
        return True
    try:
        with open(path, 'r') as handle:
            for _ in range(8):
                line = handle.readline()
                if not line:
                    break
                if line.strip() == "{0} {1}".format(OWNER_MARKER_PREFIX, owner_id):
                    return True
    except OSError:
        return False
    return False


def package_root():
    """Return the package root directory for bundled templates."""
    return os.path.dirname(__file__)


def bundled_template_path(template_name):
    """Return the absolute path to a bundled shell template."""
    return os.path.join(package_root(), 'templates', template_name)


def load_bundled_template(template_name):
    """Read a bundled text template from the package."""
    with open(bundled_template_path(template_name), 'r') as handle:
        return handle.read()


def list_visible_directories(path):
    """List visible subdirectories under a local path."""
    if not os.path.isdir(path):
        return []
    return sorted(
        name for name in os.listdir(path)
        if not name.startswith('.')
        and os.path.isdir(os.path.join(path, name))
    )


def split_remote_path(path):
    """Split an rsync path into remote target and filesystem path."""
    value = str(path).strip() if path is not None else ''
    if not value or ':' not in value:
        return None, value
    remote, remote_path = value.split(':', 1)
    if not remote or not remote_path:
        return None, value
    return remote, remote_path


def unresolved_path_variables(value):
    """Return unresolved ${VAR} placeholders from a transfer path."""
    return sorted(set(PATH_VARIABLE_PATTERN.findall(str(value or ''))))


def expand_path_variables(text, variables, identifier, field_name):
    """Expand ${VAR} placeholders using config-backed path variables."""
    value = str(text).strip() if text is not None else ''
    if not value or value == 'nan':
        return value

    missing = []

    def replace(match):
        name = match.group(1)
        if name not in variables:
            missing.append(name)
            return match.group(0)
        return str(variables[name])

    expanded = PATH_VARIABLE_PATTERN.sub(replace, value)
    remaining = unresolved_path_variables(expanded)
    if missing or remaining:
        unresolved = sorted(set(missing + remaining))
        raise ValueError(
            "Transfer '{0}' has unresolved variable(s) in {1}: {2}".format(
                identifier,
                field_name,
                ', '.join(unresolved),
            )
        )
    return expanded


def expand_transfer_endpoint(endpoint, variables, identifier, field_name):
    """Expand ${VAR} placeholders in a local or remote transfer endpoint."""
    remote, path = split_remote_path(endpoint)
    if remote:
        return join_remote_path(
            remote,
            expand_path_variables(path, variables, identifier, field_name),
        )
    return expand_path_variables(endpoint, variables, identifier, field_name)


def expand_transfer_endpoint_variables(df):
    """Expand config-backed ${VAR} placeholders in transfer endpoints."""
    df = df.copy()
    variables = config.path_variables
    for field_name in ('source', 'destination'):
        df[field_name] = [
            expand_transfer_endpoint(
                row.get(field_name, ''),
                variables,
                row.get('identifiers', ''),
                field_name,
            )
            for _, row in df.iterrows()
        ]
    return df


def validate_transfer_endpoints(df):
    """Reject malformed remote endpoints before generating scripts."""
    errors = []
    for _, row in df.iterrows():
        identifier = row.get('identifiers', '')
        for field_name in ('source', 'destination'):
            value = str(row.get(field_name, '')).strip()
            if not value or value == 'nan':
                continue
            if '@' in value and ':' not in value:
                errors.append(
                    "Invalid {0} for transfer '{1}': expected host:path, got '{2}'".format(
                        field_name, identifier, value
                    )
                )
    if errors:
        raise ValueError("\n".join(errors))


def validate_flow_metadata(df):
    """Reject inconsistent flow/portable-metadata settings."""
    errors = []
    for _, row in df.iterrows():
        identifier = row.get('identifiers', '')
        uses_portable_metadata = transfer_uses_portable_metadata(row)
        is_entry_point = row.get('is_entry_point', 'FALSE') == 'TRUE'
        rsync_options = str(row.get('rsync_options', '') or '')

        if not uses_portable_metadata:
            if any(
                row.get(column, 'FALSE') == 'TRUE'
                for column in (
                    'is_entry_point',
                    'is_end_point',
                )
            ):
                errors.append(
                    "Transfer '{0}' sets flow boundary booleans but has no flow_group".format(
                        identifier
                    )
                )
            continue

        if "--exclude='/.*'" in rsync_options or '--exclude="/.*"' in rsync_options:
            errors.append(
                "Transfer '{0}' uses flow_group='{1}' but rsync_options excludes hidden "
                "root-level entries via --exclude='/.*'; portable .landing_zones metadata "
                "would not transfer".format(identifier, row.get('flow_group', ''))
            )

    if errors:
        raise ValueError("\n".join(errors))


def audit_shared_file_pairs(df):
    """Return warnings for transfers that share the same log/flock pair."""
    warnings = []
    grouped = df.groupby(['log_file', 'flock_file'], dropna=False)
    for (log_file, flock_file), group_df in grouped:
        if not str(log_file or '').strip() and not str(flock_file or '').strip():
            continue
        identifiers = sorted(group_df['identifiers'].tolist())
        if len(identifiers) < 2:
            continue
        warnings.append(
            "Shared log/flock pair: log_file='{0}', flock_file='{1}', identifiers={2}".format(
                log_file,
                flock_file,
                ', '.join(identifiers),
            )
        )
    return warnings


def join_remote_path(remote, path):
    """Join a remote target and filesystem path into rsync syntax."""
    if remote:
        return "{0}:{1}".format(remote, path)
    return path


def shell_quote(value):
    """Shell-quote a string value."""
    return shlex.quote(str(value))


def shell_assignment_value(value):
    """Shell-quote a value for a generated variable assignment."""
    return shlex.quote(str(value or ''))


def shell_path(value):
    """Quote a path for shell use while preserving env-var expansion."""
    text = str(value)
    escaped = text.replace('\\', '\\\\').replace('"', '\\"')
    return '"{0}"'.format(escaped)


def escape_local_shell_vars(value):
    """Prevent the local shell from expanding variables meant for a remote path."""
    return str(value).replace('$', '\\$')


def build_ssh_command(remote, port=''):
    """Build an ssh command targeting a remote host."""
    command = "ssh"
    port_value = str(port).strip() if port is not None else ''
    if port_value.isdigit():
        command = "{0} -p {1}".format(command, port_value)
    return "{0} {1}".format(command, shell_quote(remote))


def build_remote_shell_command(command, remote, port=''):
    """Run a shell command on a remote host through ssh."""
    ssh_cmd = build_ssh_command(remote, port)
    return "{0} {1}".format(ssh_cmd, shell_quote(command))


def build_source_exists_command(source, port=''):
    """Build a shell command that checks whether the source directory exists."""
    remote, source_path = split_remote_path(source)
    source_root = normalize_source_path(source_path if remote else source)
    test_cmd = '[ -d {0} ]'.format(shell_path(source_root))
    if remote:
        return build_remote_shell_command(test_cmd, remote, port), source_root
    return test_cmd, source_root


def build_directory_command(command, path, remote=None, port=''):
    """Build a local or remote directory-management shell command."""
    quoted_path = shell_quote(path)
    if remote:
        ssh_cmd = build_ssh_command(remote, port)
        return '{0} "{1} {2}"'.format(
            ssh_cmd,
            command,
            quoted_path,
        )
    return "{0} {1}".format(command, quoted_path)


def build_staging_paths(destination, identifier):
    """Return destination metadata for staged transfers."""
    remote, destination_path = split_remote_path(destination)
    destination_dir = destination_path.rstrip('/') or destination_path
    staging_root = "{0}/.staging".format(destination_dir.rstrip('/'))
    staging_dir = "{0}/{1}".format(staging_root, sanitize_identifier(identifier))
    staged_destination = join_remote_path(remote, staging_dir + '/')
    return {
        'destination_remote': remote,
        'destination_dir': destination_dir,
        'staging_root': staging_root,
        'staging_dir': staging_dir,
        'staged_destination': staged_destination,
    }


def build_promote_command(destination_dir, staging_dir, remote=None, port=''):
    """Move staged content into the final destination."""
    staging_root = os.path.dirname(staging_dir)
    full_cmd = (
        "if [ -d {0} ]; then "
        "find {1} -mindepth 1 -maxdepth 1 ! -name '.staging' -exec mv {{}} {0}/ \\; && "
        "rmdir {1}; "
        "else "
        "mv {1} {0}; "
        "fi && "
        "rmdir {2} 2>/dev/null || true".format(
            shell_quote(destination_dir),
            shell_quote(staging_dir),
            shell_quote(staging_root),
        )
    )
    if remote:
        ssh_cmd = build_ssh_command(remote, port)
        return '{0} "set -eu; {1}"'.format(ssh_cmd, full_cmd)
    return full_cmd


def resolve_transfer_file_paths(df):
    """Resolve per-system log and flock file names into full paths."""
    df = df.copy()
    df['log_file'] = [
        config.resolve_managed_file_path(
            row['system'], row['log_file'], 'log'
        )
        for _, row in df.iterrows()
    ]
    df['flock_file'] = [
        config.resolve_managed_file_path(
            row['system'], row['flock_file'], 'flock'
        )
        for _, row in df.iterrows()
    ]
    return df


def get_common_status_log_file(system):
    """Return the shared per-system TSV status log path."""
    safe_system = sanitize_identifier(system) or 'system'
    filename = "Landing_Zone_{0}.transfers.tsv".format(safe_system)
    return config.resolve_managed_file_path(system, filename, 'log')


def get_common_status_lock_file(system):
    """Return the shared per-system lock path used for TSV appends."""
    safe_system = sanitize_identifier(system) or 'system'
    filename = "Landing_Zone_{0}.transfers.lock".format(safe_system)
    return config.resolve_managed_file_path(system, filename, 'flock')


def get_notification_status_log_file(system):
    """Return the shared per-system TSV notification delivery log path."""
    notification_config = config.notifications
    configured_file = notification_config.get('status_file', '')
    if configured_file:
        return config.resolve_managed_file_path(system, configured_file, 'log')
    safe_system = sanitize_identifier(system) or 'system'
    filename = "Landing_Zone_{0}.notifications.tsv".format(safe_system)
    return config.resolve_managed_file_path(system, filename, 'log')


def get_notification_status_lock_file(system):
    """Return the shared per-system notification delivery log lock path."""
    notification_config = config.notifications
    configured_file = notification_config.get('status_lock_file', '')
    if configured_file:
        return config.resolve_managed_file_path(system, configured_file, 'flock')
    safe_system = sanitize_identifier(system) or 'system'
    filename = "Landing_Zone_{0}.notifications.lock".format(safe_system)
    return config.resolve_managed_file_path(system, filename, 'flock')


def check_overlapping_sources(df):
    """Check for overlapping source paths that could cause conflicts.
    
    Detects when one source path is a parent or child of another,
    which can cause files to be transferred multiple times or race conditions.
    
    Args:
        df: DataFrame with transfer configurations
        
    Returns:
        list: List of warning messages for overlapping paths
    """
    warnings = []
    
    # Group by system (overlaps only matter on the same system)
    for system in df['system'].unique():
        system_df = df[df['system'] == system]
        sources = system_df['source'].tolist()
        
        # Normalize all source paths
        normalized = [(src, normalize_source_path(src)) for src in sources]
        
        # Check each pair for overlaps
        for i, (src1, norm1) in enumerate(normalized):
            for j, (src2, norm2) in enumerate(normalized):
                if i >= j:  # Skip self-comparison and duplicate pairs
                    continue
                
                # Check if one path is a prefix of the other
                if norm1.startswith(norm2 + '/') or norm2.startswith(norm1 + '/'):
                    # Determine which is parent/child
                    if len(norm1) < len(norm2):
                        parent, child = src1, src2
                    else:
                        parent, child = src2, src1
                    
                    warnings.append(
                        "System '{0}': Overlapping source paths detected!\n"
                        "  Parent: {1}\n"
                        "  Child:  {2}\n"
                        "  Files in the child path may be transferred by both rules, "
                        "causing conflicts.".format(system, parent, child)
                    )
    
    return warnings


def generate_cron_header(system, user):
    """Generate header comments for cron file"""
    header = """# Update from github landingzones DO NOT manually adjust
# Generated cron file for {0} system, user {1}
# put me in $HOME/crontab.d/Landing_Zone.cron
# Activate cron with:
# `cat $HOME/crontab.d/*.cron | crontab -`
# All .cron files should be found or linked at `$HOME/crontab.d`
SHELL=/bin/sh
PATH=/usr/bin:/bin
""".format(system, user)
    return header

def build_transfer_commands(transfer):
    """Build the shell commands and log paths for a transfer."""
    source = transfer['source']
    destination = transfer['destination']
    rsync_options = transfer.get('rsync_options', '')
    log_file = transfer.get('log_file', '')
    destination_port = transfer.get('destination_port', '')
    source_port = transfer.get('source_port', '')
    flock_file = transfer.get('flock_file', '')
    frequency = transfer.get('frequency', '')
    io_nice = transfer.get('io_nice', '')
    identifier = transfer.get('identifiers', 'transfer')
    
    # Ensure all values are strings and handle potential NaN values
    rsync_options = str(rsync_options) if rsync_options is not None else ''
    log_file = str(log_file) if log_file is not None else ''
    destination_port = (str(destination_port)
                        if destination_port is not None else '')
    source_port = (str(source_port)
                   if source_port is not None else '')
    flock_file = str(flock_file) if flock_file is not None else ''
    frequency = str(frequency) if frequency is not None else ''
    io_nice = str(io_nice) if io_nice is not None else ''
    
    # Clean up any 'nan' strings that might have come from NaN values
    if rsync_options == 'nan':
        rsync_options = ''
    if log_file == 'nan':
        log_file = ''
    if destination_port == 'nan':
        destination_port = ''
    if source_port == 'nan':
        source_port = ''
    if flock_file == 'nan':
        flock_file = ''
    if frequency == 'nan':
        frequency = ''
    if io_nice == 'nan':
        io_nice = ''
    
    # Base rsync options
    base_options = "-av --remove-source-files"
    staging_paths = build_staging_paths(destination, identifier)
    
    # Build SSH options for ports (both source and destination may need ports)
    ssh_ports = []
    if source_port and source_port.strip().isdigit():
        ssh_ports.append("-e 'ssh -p {0}'".format(source_port.strip()))
    elif destination_port and destination_port.strip().isdigit():
        # If only destination port is specified, use it
        ssh_ports.append("-e 'ssh -p {0}'".format(destination_port.strip()))
    
    # Add SSH port option if specified
    if ssh_ports:
        port_option = ssh_ports[0]
        base_options = "{0} {1}".format(base_options, port_option)
    
    # Combine with additional options if provided
    if rsync_options:
        rsync_options_clean = rsync_options.strip()
        if rsync_options_clean:
            options = "{0} {1}".format(base_options, rsync_options_clean)
        else:
            options = base_options
    else:
        options = base_options
    
    io_nice_cmd = normalize_io_nice(io_nice)
    if io_nice_cmd:
        rsync_cmd = "{0} rsync {1} {2} {3}".format(
            io_nice_cmd, options, source, staging_paths['staged_destination'])
    else:
        rsync_cmd = "rsync {0} {1} {2}".format(
            options, source, staging_paths['staged_destination']
        )

    # For find, strip a trailing wildcard so 'find' targets the parent dir
    # This avoids passing a literal '*' to find (which won't expand inside -c quotes)
    find_target = source
    if source.endswith('/*'):
        find_target = source[:-2]
    elif source.endswith('*'):
        # Generic fallback: remove trailing '*' and any trailing '/'
        find_target = source.rstrip('*').rstrip('/')
    source_remote, source_path = split_remote_path(find_target)
    find_path = source_path if source_remote else find_target
    find_inner_cmd = "find {0} -mindepth 1 -type d -empty -delete".format(
        shell_path(find_path) if source_remote else shell_path(find_path)
    )
    if source_remote:
        find_cmd = build_remote_shell_command(
            find_inner_cmd, source_remote, source_port
        )
    else:
        find_cmd = find_inner_cmd

    prepare_cmd = build_directory_command(
        "mkdir -p",
        staging_paths['staging_dir'],
        staging_paths['destination_remote'],
        destination_port,
    )
    promote_cmd = build_promote_command(
        staging_paths['destination_dir'],
        staging_paths['staging_dir'],
        staging_paths['destination_remote'],
        destination_port,
    )

    if not flock_file or not flock_file.strip():
        raise ValueError("flock_file is required but not specified for "
                         "transfer: {0} -> {1}".format(source, destination))

    latest_log_file = "{0}.latest".format(log_file) if log_file else ''
    mini_log_file = "{0}.mini".format(log_file) if log_file else ''
    flock_command = config.get_flock_path(transfer['system'])
    common_status_log_file = get_common_status_log_file(transfer['system'])
    common_status_lock_file = get_common_status_lock_file(transfer['system'])
    notification_config = config.notifications
    notification_status_log_file = get_notification_status_log_file(transfer['system'])
    notification_status_lock_file = get_notification_status_lock_file(transfer['system'])

    return {
        'prepare_cmd': prepare_cmd,
        'rsync_cmd': rsync_cmd,
        'promote_cmd': promote_cmd,
        'find_cmd': find_cmd,
        'log_file': log_file,
        'latest_log_file': latest_log_file,
        'mini_log_file': mini_log_file,
        'flock_file': flock_file,
        'flock_command': flock_command,
        'common_status_log_file': common_status_log_file,
        'common_status_lock_file': common_status_lock_file,
        'notification_api_endpoint': notification_config.get('endpoint', ''),
        'notification_token_env': notification_config.get('token_env', ''),
        'notification_title': notification_config.get('title', ''),
        'notification_body': notification_config.get('body', ''),
        'notification_timeout_seconds': notification_config.get('timeout_seconds', '5'),
        'notification_status_log_file': notification_status_log_file,
        'notification_status_lock_file': notification_status_lock_file,
    }


def build_transfer_command(transfer):
    """Build the shell command executed by a transfer script."""
    commands = build_transfer_commands(transfer)
    log_file = commands['log_file']
    log_redirect = " >> {0} 2>&1".format(log_file) if log_file else ''
    if log_redirect:
        return "{0}{1} && {2}{3} && {4}{5} && {6}{7}".format(
            commands['prepare_cmd'],
            log_redirect,
            commands['rsync_cmd'],
            log_redirect,
            commands['promote_cmd'],
            log_redirect,
            commands['find_cmd'],
            log_redirect,
        )
    return "{0} && {1} && {2} && {3}".format(
        commands['prepare_cmd'],
        commands['rsync_cmd'],
        commands['promote_cmd'],
        commands['find_cmd'],
    )


def generate_script_content(transfer):
    """Generate shell script content for a transfer."""
    return generate_iterative_script_content(transfer)


def generate_iterative_script_content(transfer):
    """Generate shell script content that scans source dirs and stages each one."""
    source = transfer['source']
    destination = transfer['destination']
    identifier = str(transfer.get('identifiers', 'transfer') or 'transfer')
    source_remote, source_path = split_remote_path(source)
    destination_remote, destination_path = split_remote_path(destination)
    commands = build_transfer_commands(transfer)
    source_root = normalize_source_path(source_path if source_remote else source)
    source_exists_cmd, _ = build_source_exists_command(
        transfer['source'],
        transfer.get('source_port', ''),
    )
    destination_root = destination_path.rstrip('/') or destination_path

    base_options = "-av --remove-source-files"
    source_port = str(transfer.get('source_port', '') or '').strip()
    destination_port = str(transfer.get('destination_port', '') or '').strip()
    if source_port.isdigit():
        base_options = "{0} -e 'ssh -p {1}'".format(base_options, source_port)
    elif destination_port.isdigit():
        base_options = "{0} -e 'ssh -p {1}'".format(base_options, destination_port)

    rsync_options = str(transfer.get('rsync_options', '') or '').strip()
    if rsync_options and rsync_options != 'nan':
        base_options = "{0} {1}".format(base_options, rsync_options)

    io_nice_cmd = normalize_io_nice(transfer.get('io_nice', ''))
    rsync_cmd = "rsync {0}".format(base_options)
    dry_run_rsync_cmd = "rsync --dry-run {0}".format(base_options)
    if io_nice_cmd:
        rsync_cmd = "{0} {1}".format(io_nice_cmd, rsync_cmd)
        dry_run_rsync_cmd = "{0} {1}".format(io_nice_cmd, dry_run_rsync_cmd)

    if source_remote:
        remote_find_cmd = (
            'find {0} -mindepth 1 -maxdepth 1 -type d ! -name ".*" -print'.format(
                shell_path(source_root),
            )
        )
        source_loop = (
            '{0} | while IFS= read -r source_dir; do'
        ).format(
            build_remote_shell_command(remote_find_cmd, source_remote, source_port),
        )
        rsync_source = '"{0}:$source_dir/"'.format(source_remote)
        source_cleanup_preflight_cmd = (
            'remote_ssh "$source_remote_target" "$source_remote_port" sh -c '
            '\'find "$1" -type d -print | while IFS= read -r dir_path; do '
            '[ -w "$dir_path" ] && [ -x "$dir_path" ] || printf "%s\\n" "$dir_path"; '
            'done\' '
            'sh "$source_dir"'
        )
    else:
        source_loop = (
            'find {0} -mindepth 1 -maxdepth 1 -type d ! -name ".*" -print | '
            'while IFS= read -r source_dir; do'
        ).format(shell_path(source_root))
        rsync_source = '"$source_dir/"'
        source_cleanup_preflight_cmd = (
            'find "$source_dir" -type d -print | while IFS= read -r dir_path; do '
            '[ -w "$dir_path" ] && [ -x "$dir_path" ] || printf "%s\\n" "$dir_path"; '
            'done'
        )

    remote_destination_setup = ''
    runtime_destination_root = destination_root
    if destination_remote:
        if '$' in destination_root or destination_root.startswith('~'):
            runtime_destination_root = '${resolved_destination_root}'
            remote_destination_setup = (
                'resolved_destination_root="$({0} \'printf %s "{1}"\')"\n'
            ).format(
                build_ssh_command(destination_remote, destination_port),
                destination_root.replace('"', '\\"'),
            )
        if runtime_destination_root == '${resolved_destination_root}':
            escaped_destination_root = runtime_destination_root
        else:
            escaped_destination_root = escape_local_shell_vars(runtime_destination_root)
        mkdir_cmd = '{0} "mkdir -p \\"{1}\\""'.format(
            build_ssh_command(destination_remote, destination_port),
            "{0}/.staging/$dir_name".format(escaped_destination_root),
        )
        promote_cmd = (
            '{0} "set -eu; if [ -d \\"{1}/$dir_name\\" ]; then '
            'find \\"{1}/.staging/$dir_name\\" -mindepth 1 -maxdepth 1 ! -name \\".staging\\" -exec mv {{}} \\"{1}/$dir_name/\\" \\; && '
            'rmdir \\"{1}/.staging/$dir_name\\"; '
            'else '
            'mv \\"{1}/.staging/$dir_name\\" \\"{1}/$dir_name\\"; '
            'fi; '
            'rmdir \\"{1}/.staging\\" 2>/dev/null || true"'
        ).format(
            build_ssh_command(destination_remote, destination_port),
            escaped_destination_root,
        )
        rsync_destination = '"{0}:{1}/.staging/$dir_name/"'.format(
            destination_remote,
            escaped_destination_root,
        )
        cleanup_staging_cmd = (
            '{0} "rmdir \\"{1}/.staging/$dir_name\\" 2>/dev/null || true; '
            'rmdir \\"{1}/.staging\\" 2>/dev/null || true"'
        ).format(
            build_ssh_command(destination_remote, destination_port),
            escaped_destination_root,
        )
    else:
        mkdir_cmd = 'mkdir -p "{0}/.staging/$dir_name"'.format(destination_root)
        promote_cmd = (
            'if [ -d "{0}/$dir_name" ]; then '
            'find "{0}/.staging/$dir_name" -mindepth 1 -maxdepth 1 ! -name ".staging" -exec mv {{}} "{0}/$dir_name"/ \\; && '
            'rmdir "{0}/.staging/$dir_name"; '
            'else '
            'mv "{0}/.staging/$dir_name" "{0}/$dir_name"; '
            'fi; '
            'rmdir "{0}/.staging" 2>/dev/null || true'
        ).format(destination_root)
        rsync_destination = '"{0}/.staging/$dir_name/"'.format(destination_root)
        cleanup_staging_cmd = (
            'rmdir "{0}/.staging/$dir_name" 2>/dev/null || true; '
            'rmdir "{0}/.staging" 2>/dev/null || true'
        ).format(destination_root)

    if source_remote:
        run_source_expr = '"{0}:$source_dir"'.format(source_remote)
        transfer_source_label = "{0}:{1}".format(source_remote, source_root)
    else:
        run_source_expr = '"$source_dir"'
        transfer_source_label = source_root

    if destination_remote:
        run_destination_expr = '"{0}:{1}/$dir_name"'.format(
            destination_remote,
            escaped_destination_root,
        )
        transfer_destination_label = "{0}:{1}".format(
            destination_remote,
            runtime_destination_root,
        )
    else:
        run_destination_expr = '"{0}/$dir_name"'.format(destination_root)
        transfer_destination_label = destination_root

    flow_group = str(transfer.get('flow_group', '') or '').strip()
    transfer_tags = normalize_tags_text(transfer.get('tags', ''))
    is_entry_point = str(transfer.get('is_entry_point', 'FALSE') or 'FALSE').strip().upper()
    notify_on_success = str(transfer.get('notify_on_success', 'FALSE') or 'FALSE').strip().upper()
    notify_on_error = str(transfer.get('notify_on_error', 'FALSE') or 'FALSE').strip().upper()
    portable_metadata_enabled = '1' if transfer_uses_portable_metadata(transfer) else '0'
    source_remote_target = source_remote or ''
    destination_remote_target = destination_remote or ''
    destination_root_runtime = runtime_destination_root

    return """#!/bin/sh
set -eu

log_file="{log_file}"
latest_log_file="{latest_log_file}"
mini_log_file="{mini_log_file}"
flock_file="{flock_file}"
common_status_log_file="{common_status_log_file}"
common_status_lock_file="{common_status_lock_file}"
notification_api_endpoint={notification_api_endpoint}
notification_token_env={notification_token_env}
notification_title={notification_title}
notification_body={notification_body}
notification_timeout_seconds={notification_timeout_seconds}
notification_status_log_file="{notification_status_log_file}"
notification_status_lock_file="{notification_status_lock_file}"
transfer_identifier="{transfer_identifier}"
transfer_system="{transfer_system}"
flow_group="{flow_group}"
transfer_tags="{transfer_tags}"
portable_metadata_enabled="{portable_metadata_enabled}"
is_entry_point="{is_entry_point}"
notify_on_success="{notify_on_success}"
notify_on_error="{notify_on_error}"
source_remote_target="{source_remote_target}"
source_remote_port="{source_remote_port}"
destination_remote_target="{destination_remote_target}"
destination_remote_port="{destination_remote_port}"
destination_root_runtime="{destination_root_runtime}"
portable_metadata_dir_name=".landing_zones"
portable_metadata_file_name="landingzone-run-metadata.tsv"
portable_events_file_name="landingzone-transfer-events.tsv"
run_log="$(mktemp "${{TMPDIR:-/tmp}}/landingzones.{script_stem}.rsync.XXXXXX")"
cleanup_log="$(mktemp "${{TMPDIR:-/tmp}}/landingzones.{script_stem}.cleanup.XXXXXX")"
promote_log="$(mktemp "${{TMPDIR:-/tmp}}/landingzones.{script_stem}.promote.XXXXXX")"
preflight_log="$(mktemp "${{TMPDIR:-/tmp}}/landingzones.{script_stem}.preflight.XXXXXX")"
preflight_stderr_log="$(mktemp "${{TMPDIR:-/tmp}}/landingzones.{script_stem}.preflight-stderr.XXXXXX")"
current_run=""
current_run_id=""
current_run_name=""
current_origin_system=""
current_entry_transfer_identifier=""
current_created_at_utc=""
current_run_source=""
current_run_destination=""
current_run_completed=0

cleanup() {{
    rm -f "$run_log" "$cleanup_log" "$promote_log" "$preflight_log" "$preflight_stderr_log"
}}
debug_enabled() {{
    [ -t 1 ] || [ "${{LZ_DEBUG_CLI:-0}}" = "1" ]
}}

log_status() {{
    printf '%s %s\\n' "$(date '+%Y-%m-%d %H:%M:%S%z')" "$1" >> "$mini_log_file"
}}

sanitize_tsv_field() {{
    printf '%s' "$1" | tr '\\t\\r\\n' '   '
}}

portable_metadata_dir_for_run() {{
    printf '%s/%s' "$1" "$portable_metadata_dir_name"
}}

portable_metadata_file_for_run() {{
    printf '%s/%s' "$(portable_metadata_dir_for_run "$1")" "$portable_metadata_file_name"
}}

portable_events_file_for_run() {{
    printf '%s/%s' "$(portable_metadata_dir_for_run "$1")" "$portable_events_file_name"
}}

remote_ssh() {{
    remote_target="$1"
    remote_port="$2"
    shift 2
    remote_command=""
    for remote_arg in "$@"; do
        quoted_remote_arg=$(printf '%s' "$remote_arg" | sed "s/'/'\\\\''/g")
        if [ -n "$remote_command" ]; then
            remote_command="$remote_command "
        fi
        remote_command="${{remote_command}}'${{quoted_remote_arg}}'"
    done
    if [ -n "$remote_port" ]; then
        ssh -p "$remote_port" "$remote_target" "$remote_command"
    else
        ssh "$remote_target" "$remote_command"
    fi
}}

ensure_portable_events_file_local() {{
    metadata_dir="$1"
    events_file="$2"
    mkdir -p "$metadata_dir"
    if [ ! -s "$events_file" ]; then
        printf 'event_time_utc\\trun_id\\tflow_group\\ttransfer_identifier\\tsystem\\tstatus\\tsource_path\\tdestination_path\\tmessage\\n' >> "$events_file"
    fi
}}

ensure_portable_events_file_remote() {{
    remote_target="$1"
    remote_port="$2"
    metadata_dir="$3"
    events_file="$4"
    remote_ssh "$remote_target" "$remote_port" mkdir -p "$metadata_dir"
    remote_ssh "$remote_target" "$remote_port" sh -c '
        if [ ! -s "$1" ]; then
            printf "%s\\n" "$2" >> "$1"
        fi
    ' sh "$events_file" 'event_time_utc	run_id	flow_group	transfer_identifier	system	status	source_path	destination_path	message'
}}

read_run_id_local() {{
    metadata_file="$1"
    awk -F '\\t' '$1 == "run_id" {{ print $2; exit }}' "$metadata_file"
}}

read_metadata_field_local() {{
    metadata_file="$1"
    field_name="$2"
    grep "^$field_name" "$metadata_file" | head -n 1 | cut -f2-
}}

read_run_id_remote() {{
    remote_target="$1"
    remote_port="$2"
    metadata_file="$3"
    remote_ssh "$remote_target" "$remote_port" sh -c '
        grep "^run_id" "$1" | head -n 1 | cut -f2-
    ' sh "$metadata_file"
}}

read_metadata_field_remote() {{
    remote_target="$1"
    remote_port="$2"
    metadata_file="$3"
    field_name="$4"
    remote_ssh "$remote_target" "$remote_port" sh -c '
        grep "^$2" "$1" | head -n 1 | cut -f2-
    ' sh "$metadata_file" "$field_name"
}}

write_run_metadata_local() {{
    run_dir="$1"
    metadata_dir="$(portable_metadata_dir_for_run "$run_dir")"
    metadata_file="$(portable_metadata_file_for_run "$run_dir")"
    created_at_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    mkdir -p "$metadata_dir"
    {{
        printf 'schema_version\\t1\\n'
        printf 'run_id\\t%s\\n' "$(sanitize_tsv_field "$current_run_id")"
        printf 'run_name\\t%s\\n' "$(sanitize_tsv_field "$current_run")"
        printf 'flow_group\\t%s\\n' "$(sanitize_tsv_field "$flow_group")"
        printf 'origin_system\\t%s\\n' "$(sanitize_tsv_field "$transfer_system")"
        printf 'entry_transfer_identifier\\t%s\\n' "$(sanitize_tsv_field "$transfer_identifier")"
        printf 'created_at_utc\\t%s\\n' "$(sanitize_tsv_field "$created_at_utc")"
    }} > "$metadata_file"
    ensure_portable_events_file_local "$metadata_dir" "$(portable_events_file_for_run "$run_dir")"
}}

write_run_metadata_remote() {{
    remote_target="$1"
    remote_port="$2"
    run_dir="$3"
    metadata_dir="$(portable_metadata_dir_for_run "$run_dir")"
    metadata_file="$(portable_metadata_file_for_run "$run_dir")"
    created_at_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    remote_ssh "$remote_target" "$remote_port" mkdir -p "$metadata_dir"
    remote_ssh "$remote_target" "$remote_port" sh -c '
        printf "%s\\n" "$2" "$3" "$4" "$5" "$6" "$7" "$8" > "$1"
    ' sh "$metadata_file" \
        'schema_version	1' \
        "run_id	$(sanitize_tsv_field "$current_run_id")" \
        "run_name	$(sanitize_tsv_field "$current_run")" \
        "flow_group	$(sanitize_tsv_field "$flow_group")" \
        "origin_system	$(sanitize_tsv_field "$transfer_system")" \
        "entry_transfer_identifier	$(sanitize_tsv_field "$transfer_identifier")" \
        "created_at_utc	$(sanitize_tsv_field "$created_at_utc")"
    ensure_portable_events_file_remote "$remote_target" "$remote_port" "$metadata_dir" "$(portable_events_file_for_run "$run_dir")"
}}

append_portable_event_local() {{
    run_dir="$1"
    event_status="$2"
    event_message="${{3:-}}"
    metadata_dir="$(portable_metadata_dir_for_run "$run_dir")"
    events_file="$(portable_events_file_for_run "$run_dir")"
    event_time_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    ensure_portable_events_file_local "$metadata_dir" "$events_file"
    printf '%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n' \
        "$(sanitize_tsv_field "$event_time_utc")" \
        "$(sanitize_tsv_field "$current_run_id")" \
        "$(sanitize_tsv_field "$flow_group")" \
        "$(sanitize_tsv_field "$transfer_identifier")" \
        "$(sanitize_tsv_field "$transfer_system")" \
        "$(sanitize_tsv_field "$event_status")" \
        "$(sanitize_tsv_field "$current_run_source")" \
        "$(sanitize_tsv_field "$current_run_destination")" \
        "$(sanitize_tsv_field "$event_message")" >> "$events_file"
}}

append_portable_event_remote() {{
    remote_target="$1"
    remote_port="$2"
    run_dir="$3"
    event_status="$4"
    event_message="${{5:-}}"
    metadata_dir="$(portable_metadata_dir_for_run "$run_dir")"
    events_file="$(portable_events_file_for_run "$run_dir")"
    event_time_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    ensure_portable_events_file_remote "$remote_target" "$remote_port" "$metadata_dir" "$events_file"
    remote_ssh "$remote_target" "$remote_port" sh -c '
        printf "%s\\n" "$2" >> "$1"
    ' sh "$events_file" \
        "$(sanitize_tsv_field "$event_time_utc")	$(sanitize_tsv_field "$current_run_id")	$(sanitize_tsv_field "$flow_group")	$(sanitize_tsv_field "$transfer_identifier")	$(sanitize_tsv_field "$transfer_system")	$(sanitize_tsv_field "$event_status")	$(sanitize_tsv_field "$current_run_source")	$(sanitize_tsv_field "$current_run_destination")	$(sanitize_tsv_field "$event_message")"
}}

source_metadata_exists() {{
    if [ -n "$source_remote_target" ]; then
        remote_ssh "$source_remote_target" "$source_remote_port" sh -c '[ -f "$1" ]' sh "$(portable_metadata_file_for_run "$source_dir")"
    else
        [ -f "$(portable_metadata_file_for_run "$source_dir")" ]
    fi
}}

ensure_source_run_bundle() {{
    if [ "$portable_metadata_enabled" != "1" ]; then
        current_run_id=""
        current_run_name="$current_run"
        current_origin_system=""
        current_entry_transfer_identifier=""
        current_created_at_utc=""
        return 0
    fi

    if source_metadata_exists; then
        if [ -n "$source_remote_target" ]; then
            current_run_id="$(read_run_id_remote "$source_remote_target" "$source_remote_port" "$(portable_metadata_file_for_run "$source_dir")")"
            current_run_name="$(read_metadata_field_remote "$source_remote_target" "$source_remote_port" "$(portable_metadata_file_for_run "$source_dir")" "run_name")"
            current_origin_system="$(read_metadata_field_remote "$source_remote_target" "$source_remote_port" "$(portable_metadata_file_for_run "$source_dir")" "origin_system")"
            current_entry_transfer_identifier="$(read_metadata_field_remote "$source_remote_target" "$source_remote_port" "$(portable_metadata_file_for_run "$source_dir")" "entry_transfer_identifier")"
            current_created_at_utc="$(read_metadata_field_remote "$source_remote_target" "$source_remote_port" "$(portable_metadata_file_for_run "$source_dir")" "created_at_utc")"
        else
            current_run_id="$(read_run_id_local "$(portable_metadata_file_for_run "$source_dir")")"
            current_run_name="$(read_metadata_field_local "$(portable_metadata_file_for_run "$source_dir")" "run_name")"
            current_origin_system="$(read_metadata_field_local "$(portable_metadata_file_for_run "$source_dir")" "origin_system")"
            current_entry_transfer_identifier="$(read_metadata_field_local "$(portable_metadata_file_for_run "$source_dir")" "entry_transfer_identifier")"
            current_created_at_utc="$(read_metadata_field_local "$(portable_metadata_file_for_run "$source_dir")" "created_at_utc")"
        fi
        [ -n "$current_run_name" ] || current_run_name="$current_run"
        if [ -z "$current_run_id" ]; then
            log_status "$dir_name metadata missing run_id"
            append_common_status "error" "$dir_name" "$current_run_source" "$current_run_destination"
            debug "$dir_name metadata missing run_id"
            return 1
        fi
        return 0
    fi

    if [ "$is_entry_point" != "TRUE" ]; then
        log_status "$dir_name missing portable metadata"
        append_common_status "error" "$dir_name" "$current_run_source" "$current_run_destination"
        debug "$dir_name missing portable metadata"
        return 1
    fi

    if ! command -v uuidgen >/dev/null 2>&1; then
        log_status "$dir_name missing uuidgen"
        append_common_status "error" "$dir_name" "$current_run_source" "$current_run_destination"
        debug "$dir_name missing uuidgen"
        return 1
    fi

    current_run_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
    current_run_name="$current_run"
    current_origin_system="$transfer_system"
    current_entry_transfer_identifier="$transfer_identifier"
    current_created_at_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    if [ -n "$source_remote_target" ]; then
        write_run_metadata_remote "$source_remote_target" "$source_remote_port" "$source_dir"
    else
        write_run_metadata_local "$source_dir"
    fi
}}

append_source_portable_event() {{
    event_status="$1"
    event_message="${{2:-}}"
    [ "$portable_metadata_enabled" = "1" ] || return 0
    [ -n "$current_run_id" ] || return 0
    if [ -n "$source_remote_target" ]; then
        append_portable_event_remote "$source_remote_target" "$source_remote_port" "$source_dir" "$event_status" "$event_message"
    else
        append_portable_event_local "$source_dir" "$event_status" "$event_message"
    fi
}}

destination_run_exists() {{
    destination_run_dir="$destination_root_runtime/$dir_name"
    if [ -n "$destination_remote_target" ]; then
        remote_ssh "$destination_remote_target" "$destination_remote_port" sh -c '[ -d "$1" ]' sh "$destination_run_dir"
    else
        [ -d "$destination_run_dir" ]
    fi
}}

append_destination_portable_event() {{
    event_status="$1"
    event_message="${{2:-}}"
    destination_run_dir="$destination_root_runtime/$dir_name"
    [ "$portable_metadata_enabled" = "1" ] || return 0
    [ -n "$current_run_id" ] || return 0
    destination_run_exists || return 0
    if [ -n "$destination_remote_target" ]; then
        append_portable_event_remote "$destination_remote_target" "$destination_remote_port" "$destination_run_dir" "$event_status" "$event_message"
    else
        append_portable_event_local "$destination_run_dir" "$event_status" "$event_message"
    fi
}}

append_best_effort_portable_error() {{
    event_message="${{1:-script_error}}"
    [ "$portable_metadata_enabled" = "1" ] || return 0
    [ -n "$current_run_id" ] || return 0
    if source_metadata_exists; then
        append_source_portable_event "error" "$event_message"
        return 0
    fi
    append_destination_portable_event "error" "$event_message"
}}

notification_enabled_for_status() {{
    event_status="$1"
    if [ "$event_status" = "completed" ] && [ "$notify_on_success" = "TRUE" ]; then
        return 0
    fi
    if [ "$event_status" = "error" ] && [ "$notify_on_error" = "TRUE" ]; then
        return 0
    fi
    return 1
}}

notification_token_value() {{
    if [ -z "$notification_token_env" ]; then
        return 0
    fi
    env | awk -F= -v name="$notification_token_env" '$1 == name {{ print substr($0, length(name) + 2); exit }}'
}}

json_escape() {{
    printf '%s' "$1" | sed 's/\\\\/\\\\\\\\/g; s/"/\\\\"/g; s/	/\\\\t/g' | tr '\\r\\n' '  '
}}

build_notification_payload() {{
    event_status="$1"
    event_directory="$2"
    event_source="$3"
    event_destination="$4"
    event_message="$5"
    idempotency_key="$6"
    printf '{{"title":"%s","body":"%s","idempotency_key":"%s","transfer_identifier":"%s","system":"%s","run_id":"%s","run_name":"%s","flow_group":"%s","tags":"%s","directory":"%s","source_path":"%s","destination_path":"%s","status":"%s","message":"%s"}}' \\
        "$(json_escape "$notification_title")" \\
        "$(json_escape "$notification_body")" \\
        "$(json_escape "$idempotency_key")" \\
        "$(json_escape "$transfer_identifier")" \\
        "$(json_escape "$transfer_system")" \\
        "$(json_escape "$current_run_id")" \\
        "$(json_escape "$current_run_name")" \\
        "$(json_escape "$flow_group")" \\
        "$(json_escape "$transfer_tags")" \\
        "$(json_escape "$event_directory")" \\
        "$(json_escape "$event_source")" \\
        "$(json_escape "$event_destination")" \\
        "$(json_escape "$event_status")" \\
        "$(json_escape "$event_message")"
}}

notification_already_sent() {{
    idempotency_key="$1"
    [ -s "$notification_status_log_file" ] || return 1
    awk -F '\\t' -v key="$idempotency_key" '$3 == key && $14 == "sent" {{ found = 1 }} END {{ exit found ? 0 : 1 }}' "$notification_status_log_file"
}}

append_notification_status() {{
    transfer_event_time_utc="$1"
    notification_time_utc="$2"
    idempotency_key="$3"
    event_directory="$4"
    event_source="$5"
    event_destination="$6"
    event_status="$7"
    notification_status="$8"
    http_status="$9"
    attempt="${{10}}"
    notification_message="${{11:-}}"
    (
        exec 7>>"$notification_status_lock_file"
        {flock_command} 7
        if [ ! -s "$notification_status_log_file" ]; then
            printf 'event_time_utc\\tnotification_time_utc\\tidempotency_key\\ttransfer_identifier\\tsystem\\trun_id\\trun_name\\tflow_group\\ttags\\tdirectory\\tsource_path\\tdestination_path\\tstatus\\tnotification_status\\thttp_status\\tattempt\\ttitle\\tbody\\tmessage\\n' >> "$notification_status_log_file"
        fi
        printf '%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n' \\
            "$(sanitize_tsv_field "$transfer_event_time_utc")" \\
            "$(sanitize_tsv_field "$notification_time_utc")" \\
            "$(sanitize_tsv_field "$idempotency_key")" \\
            "$(sanitize_tsv_field "$transfer_identifier")" \\
            "$(sanitize_tsv_field "$transfer_system")" \\
            "$(sanitize_tsv_field "$current_run_id")" \\
            "$(sanitize_tsv_field "$current_run_name")" \\
            "$(sanitize_tsv_field "$flow_group")" \\
            "$(sanitize_tsv_field "$transfer_tags")" \\
            "$(sanitize_tsv_field "$event_directory")" \\
            "$(sanitize_tsv_field "$event_source")" \\
            "$(sanitize_tsv_field "$event_destination")" \\
            "$(sanitize_tsv_field "$event_status")" \\
            "$(sanitize_tsv_field "$notification_status")" \\
            "$(sanitize_tsv_field "$http_status")" \\
            "$(sanitize_tsv_field "$attempt")" \\
            "$(sanitize_tsv_field "$notification_title")" \\
            "$(sanitize_tsv_field "$notification_body")" \\
            "$(sanitize_tsv_field "$notification_message")" >> "$notification_status_log_file"
    ) || debug "unable to append notification status row"
}}

notify_transfer_event() {{
    transfer_event_time_utc="$1"
    event_status="$2"
    event_directory="${{3:-}}"
    event_source="${{4:-}}"
    event_destination="${{5:-}}"
    event_message="${{6:-}}"
    notification_enabled_for_status "$event_status" || return 0
    [ -n "$notification_api_endpoint" ] || return 0
    [ -n "$notification_status_log_file" ] || return 0
    idempotency_key="$(sanitize_tsv_field "${{current_run_id}}|${{transfer_identifier}}|${{event_status}}|${{event_directory}}|${{event_source}}|${{event_destination}}")"
    notification_already_sent "$idempotency_key" && return 0
    notification_time_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    curl_bin="${{LANDINGZONES_CURL:-curl}}"
    if ! command -v "$curl_bin" >/dev/null 2>&1; then
        append_notification_status "$transfer_event_time_utc" "$notification_time_utc" "$idempotency_key" "$event_directory" "$event_source" "$event_destination" "$event_status" "failed" "curl_missing" "1" "curl command not found"
        return 0
    fi
    payload="$(build_notification_payload "$event_status" "$event_directory" "$event_source" "$event_destination" "$event_message" "$idempotency_key")"
    token_value="$(notification_token_value)"
    if [ -n "$token_value" ]; then
        if http_status="$("$curl_bin" -sS -o /dev/null -w '%{{http_code}}' --max-time "$notification_timeout_seconds" -H 'Content-Type: application/json' -H "Authorization: Bearer $token_value" -H "Idempotency-Key: $idempotency_key" --data "$payload" "$notification_api_endpoint" 2>/dev/null)"; then
            :
        else
            http_status="curl_error"
        fi
    else
        if http_status="$("$curl_bin" -sS -o /dev/null -w '%{{http_code}}' --max-time "$notification_timeout_seconds" -H 'Content-Type: application/json' -H "Idempotency-Key: $idempotency_key" --data "$payload" "$notification_api_endpoint" 2>/dev/null)"; then
            :
        else
            http_status="curl_error"
        fi
    fi
    case "$http_status" in
        2*) notification_status="sent" ;;
        *) notification_status="failed" ;;
    esac
    append_notification_status "$transfer_event_time_utc" "$notification_time_utc" "$idempotency_key" "$event_directory" "$event_source" "$event_destination" "$event_status" "$notification_status" "$http_status" "1" ""
    return 0
}}

append_common_status() {{
    event_status="$1"
    event_directory="${{2:-}}"
    event_source="${{3:-}}"
    event_destination="${{4:-}}"
    event_message="${{5:-}}"
    event_timestamp="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    (
        exec 8>>"$common_status_lock_file"
        {flock_command} 8
        if [ ! -s "$common_status_log_file" ]; then
            printf 'event_time_utc\\ttransfer_identifier\\tsystem\\trun_id\\trun_name\\tflow_group\\ttags\\torigin_system\\tentry_transfer_identifier\\tcreated_at_utc\\tdirectory\\tsource_path\\tdestination_path\\tstatus\\tmessage\\n' >> "$common_status_log_file"
        fi
        printf '%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n' \\
            "$(sanitize_tsv_field "$event_timestamp")" \\
            "$(sanitize_tsv_field "$transfer_identifier")" \\
            "$(sanitize_tsv_field "$transfer_system")" \\
            "$(sanitize_tsv_field "$current_run_id")" \\
            "$(sanitize_tsv_field "$current_run_name")" \\
            "$(sanitize_tsv_field "$flow_group")" \\
            "$(sanitize_tsv_field "$transfer_tags")" \\
            "$(sanitize_tsv_field "$current_origin_system")" \\
            "$(sanitize_tsv_field "$current_entry_transfer_identifier")" \\
            "$(sanitize_tsv_field "$current_created_at_utc")" \\
            "$(sanitize_tsv_field "$event_directory")" \\
            "$(sanitize_tsv_field "$event_source")" \\
            "$(sanitize_tsv_field "$event_destination")" \\
            "$(sanitize_tsv_field "$event_status")" \\
            "$(sanitize_tsv_field "$event_message")" >> "$common_status_log_file"
    ) || debug "unable to append common status row"
    notify_transfer_event "$event_timestamp" "$event_status" "$event_directory" "$event_source" "$event_destination" "$event_message" || debug "notification delivery failed"
}}

debug() {{
    if debug_enabled; then
        printf '%s %s\\n' "$(date '+%Y-%m-%d %H:%M:%S%z')" "$1" >&2
    fi
}}

dump_debug_log() {{
    label="$1"
    path="$2"
    if debug_enabled && [ -s "$path" ]; then
        debug "$label follows"
        cat "$path" >&2
    fi
}}

reset_current_run_context() {{
    current_run=""
    current_run_id=""
    current_run_name=""
    current_origin_system=""
    current_entry_transfer_identifier=""
    current_created_at_utc=""
    current_run_source=""
    current_run_destination=""
    current_run_completed=0
}}

summarize_log() {{
    path="$1"
    if [ ! -s "$path" ]; then
        printf 'see log'
        return 0
    fi
    awk 'NF {{ gsub(/\\t/, " "); print; exit }}' "$path"
}}

on_exit() {{
    status=$?
    if [ "$status" -ne 0 ]; then
        if [ -n "$current_run" ] && [ "$current_run_completed" -eq 0 ]; then
            append_best_effort_portable_error "script_error"
            log_status "$current_run error"
            append_common_status "error" "$current_run" "$current_run_source" "$current_run_destination"
        else
            append_common_status "error" "" "{transfer_source_label}" "{transfer_destination_label}"
        fi
        debug "script failed with exit code $status"
        dump_debug_log "run log" "$run_log"
        dump_debug_log "promote log" "$promote_log"
        dump_debug_log "cleanup log" "$cleanup_log"
        dump_debug_log "preflight log" "$preflight_log"
        dump_debug_log "preflight stderr log" "$preflight_stderr_log"
    fi
    cleanup
    exit "$status"
}}
trap on_exit EXIT HUP INT TERM

mkdir -p "$(dirname "$log_file")" "$(dirname "$latest_log_file")" "$(dirname "$mini_log_file")" "$(dirname "$flock_file")" "$(dirname "$common_status_log_file")" "$(dirname "$common_status_lock_file")" "$(dirname "$notification_status_log_file")" "$(dirname "$notification_status_lock_file")"
debug "using lock file $flock_file"
{remote_destination_setup}
exec 9>"$flock_file"
if ! {flock_command} -n 9; then
    debug "lock busy, exiting"
    exit 0
fi

if ! {source_exists_cmd}; then
    log_status "{missing_source_message}"
    append_common_status "error" "" "{transfer_source_label}" "{transfer_destination_label}"
    printf '%s %s\\n' "$(date '+%Y-%m-%d %H:%M:%S%z')" "{missing_source_message}" >> "$log_file"
    debug "{missing_source_message}"
    exit 0
fi

: >"$run_log"
: >"$promote_log"
{source_loop}
    [ -n "$source_dir" ] || continue
    dir_name=$(basename "$source_dir")
    current_run="$dir_name"
    current_run_source={run_source_expr}
    current_run_destination={run_destination_expr}
    current_run_id=""
    current_run_name="$dir_name"
    current_origin_system=""
    current_entry_transfer_identifier=""
    current_created_at_utc=""
    current_run_completed=0
    ensure_source_run_bundle || continue
    log_status "$dir_name initiated"
    append_source_portable_event "initiated"
    append_common_status "initiated" "$dir_name" "$current_run_source" "$current_run_destination"
    debug "$dir_name initiated"
    : >"$preflight_log"
    : >"$preflight_stderr_log"
    if ! {source_cleanup_preflight_cmd} >"$preflight_log" 2>"$preflight_stderr_log"; then
        preflight_message="source cleanup preflight command failed: $(summarize_log "$preflight_stderr_log")"
        log_status "$dir_name error"
        append_source_portable_event "error" "$preflight_message"
        append_common_status "error" "$dir_name" "$current_run_source" "$current_run_destination" "$preflight_message"
        debug "$dir_name $preflight_message"
        reset_current_run_context
        continue
    fi
    if [ -s "$preflight_log" ]; then
        preflight_message="source cleanup preflight failed: $(summarize_log "$preflight_log")"
        log_status "$dir_name error"
        append_source_portable_event "error" "$preflight_message"
        append_common_status "error" "$dir_name" "$current_run_source" "$current_run_destination" "$preflight_message"
        debug "$dir_name $preflight_message"
        reset_current_run_context
        continue
    fi
    {mkdir_cmd} </dev/null >>"$promote_log" 2>&1
    if ! {dry_run_rsync_cmd} {rsync_source} {rsync_destination} </dev/null >>"$preflight_log" 2>&1; then
        preflight_message="rsync dry-run failed: $(summarize_log "$preflight_log")"
        {cleanup_staging_cmd} </dev/null >>"$preflight_log" 2>&1
        log_status "$dir_name error"
        append_source_portable_event "error" "$preflight_message"
        append_common_status "error" "$dir_name" "$current_run_source" "$current_run_destination" "$preflight_message"
        debug "$dir_name $preflight_message"
        reset_current_run_context
        continue
    fi
    : >"$preflight_log"
    if ! {rsync_cmd} {rsync_source} {rsync_destination} </dev/null >>"$run_log" 2>&1; then
        rsync_message="rsync failed: $(summarize_log "$run_log")"
        log_status "$dir_name error"
        append_source_portable_event "error" "$rsync_message"
        append_common_status "error" "$dir_name" "$current_run_source" "$current_run_destination" "$rsync_message"
        debug "$dir_name $rsync_message"
        reset_current_run_context
        continue
    fi
    if ! ( {promote_cmd} ) </dev/null >>"$promote_log" 2>&1; then
        promote_message="staging promote failed: see promote log"
        log_status "$dir_name error"
        append_source_portable_event "error" "$promote_message"
        append_common_status "error" "$dir_name" "$current_run_source" "$current_run_destination" "$promote_message"
        debug "$dir_name $promote_message"
        reset_current_run_context
        continue
    fi
    log_status "$dir_name completed"
    append_destination_portable_event "completed"
    append_common_status "completed" "$dir_name" "$current_run_source" "$current_run_destination"
    debug "$dir_name completed"
    current_run_completed=1
    reset_current_run_context
done
if [ -s "$run_log" ]; then
    cat "$run_log" >> "$log_file"
fi
if [ -s "$promote_log" ]; then
    cat "$promote_log" >> "$log_file"
fi
if [ -s "$preflight_log" ]; then
    cat "$preflight_log" >> "$log_file"
fi
if [ -s "$preflight_stderr_log" ]; then
    cat "$preflight_stderr_log" >> "$log_file"
fi

{find_cmd} >"$cleanup_log" 2>&1
if [ -s "$cleanup_log" ]; then
    cat "$cleanup_log" >> "$log_file"
fi

if sed '/^sending incremental file list$/d; /^sent .* bytes .*$/d; /^total size is .*$/d; /^$/d' "$run_log" | grep -q .; then
    cat "$run_log" > "$latest_log_file"
    if [ -s "$promote_log" ]; then
        cat "$promote_log" >> "$latest_log_file"
    fi
    if [ -s "$preflight_log" ]; then
        cat "$preflight_log" >> "$latest_log_file"
    fi
    if [ -s "$preflight_stderr_log" ]; then
        cat "$preflight_stderr_log" >> "$latest_log_file"
    fi
    if [ -s "$cleanup_log" ]; then
        cat "$cleanup_log" >> "$latest_log_file"
    fi
fi
    """.format(
        log_file=commands['log_file'],
        latest_log_file=commands['latest_log_file'],
        mini_log_file=commands['mini_log_file'],
        flock_file=commands['flock_file'],
        common_status_log_file=commands['common_status_log_file'],
        common_status_lock_file=commands['common_status_lock_file'],
        notification_api_endpoint=shell_assignment_value(commands['notification_api_endpoint']),
        notification_token_env=shell_assignment_value(commands['notification_token_env']),
        notification_title=shell_assignment_value(commands['notification_title']),
        notification_body=shell_assignment_value(commands['notification_body']),
        notification_timeout_seconds=shell_assignment_value(commands['notification_timeout_seconds']),
        notification_status_log_file=commands['notification_status_log_file'],
        notification_status_lock_file=commands['notification_status_lock_file'],
        transfer_identifier=identifier.replace('"', '\\"'),
        transfer_system=str(transfer.get('system', '') or '').replace('"', '\\"'),
        flow_group=flow_group.replace('"', '\\"'),
        transfer_tags=transfer_tags.replace('"', '\\"'),
        portable_metadata_enabled=portable_metadata_enabled,
        is_entry_point=is_entry_point,
        notify_on_success=notify_on_success,
        notify_on_error=notify_on_error,
        source_remote_target=source_remote_target.replace('"', '\\"'),
        source_remote_port=source_port.replace('"', '\\"'),
        destination_remote_target=destination_remote_target.replace('"', '\\"'),
        destination_remote_port=destination_port.replace('"', '\\"'),
        destination_root_runtime=escape_local_shell_vars(
            str(destination_root_runtime)
        ).replace('"', '\\"'),
        script_stem=sanitize_identifier(identifier),
        flock_command=commands['flock_command'],
        source_exists_cmd=source_exists_cmd,
        missing_source_message='source directory missing: {0}'.format(source_root),
        source_loop=source_loop,
        run_source_expr=run_source_expr,
        run_destination_expr=run_destination_expr,
        mkdir_cmd=mkdir_cmd,
        source_cleanup_preflight_cmd=source_cleanup_preflight_cmd,
        dry_run_rsync_cmd=dry_run_rsync_cmd,
        rsync_cmd=rsync_cmd,
        rsync_source=rsync_source,
        rsync_destination=rsync_destination,
        promote_cmd=promote_cmd,
        cleanup_staging_cmd=cleanup_staging_cmd,
        find_cmd=commands['find_cmd'],
        transfer_source_label=transfer_source_label.replace('"', '\\"'),
        transfer_destination_label=transfer_destination_label.replace('"', '\\"'),
        remote_destination_setup=remote_destination_setup.rstrip(),
    )


def generate_rsync_command(transfer):
    """Backward-compatible wrapper returning the underlying transfer command."""
    return build_transfer_command(transfer)


def generate_cron_entry(transfer, script_path):
    """Generate cron entry that executes a transfer shell script."""
    frequency = transfer.get('frequency', '')
    cron_schedule = str(frequency).strip() if frequency is not None else ''
    if not cron_schedule or cron_schedule == 'nan':
        cron_schedule = config.default_cron_frequency
    return "{0} /bin/sh {1}".format(cron_schedule, script_path)


def get_deployed_script_path(system, script_name):
    """Return the configured deployed script path for a system."""
    script_dir = config.get_rit_managed_path(system, 'sh_output')
    return os.path.join(script_dir, script_name)


def get_validation_fixture_container_candidates(source_path):
    """Return likely fixture container directory names for a local entry path."""
    candidates = []
    absolute_source = os.path.abspath(normalize_source_path(source_path))
    marker = "{0}tests{0}test_local{0}".format(os.sep)
    if marker in absolute_source:
        relative_tail = absolute_source.split(marker, 1)[1]
        first_segment = relative_tail.split(os.sep, 1)[0]
        if first_segment:
            candidates.append(first_segment)

    basename = os.path.basename(absolute_source.rstrip(os.sep))
    if basename:
        candidates.append(basename)

    result = []
    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        result.append(candidate)
    return result


def resolve_validation_fixture_dir(transfer):
    """Resolve the default validation fixture directory for an entry-point transfer."""
    toy_data_root = os.path.abspath(config.test_data)
    candidate_roots = []
    source = transfer.source if hasattr(transfer, 'source') else transfer['source']
    for candidate in get_validation_fixture_container_candidates(source):
        candidate_root = os.path.join(toy_data_root, candidate)
        if os.path.isdir(candidate_root):
            candidate_roots.append(candidate_root)
    candidate_roots.append(toy_data_root)

    for candidate_root in candidate_roots:
        visible_dirs = list_visible_directories(candidate_root)
        if visible_dirs:
            return os.path.join(candidate_root, visible_dirs[0])
        if os.path.isdir(candidate_root):
            return candidate_root

    return toy_data_root


def validation_wrapper_script_name(flow_group):
    """Return the generated shell wrapper filename for a flow group."""
    sanitized_flow_group = sanitize_identifier(flow_group)
    if not sanitized_flow_group:
        raise ValueError(
            "flow_group must contain at least one filename-safe character: {0}".format(
                flow_group
            )
        )
    return "{0}{1}.sh".format(
        VALIDATION_WRAPPER_PREFIX,
        prefixed_artifact_stem(sanitized_flow_group),
    )


def validation_wrapper_file_prefix():
    """Return the filename prefix used by generated validation wrappers."""
    prefix = configured_artifact_prefix()
    if not prefix:
        return VALIDATION_WRAPPER_PREFIX
    return "{0}{1}__".format(VALIDATION_WRAPPER_PREFIX, prefix)


def validation_helper_name():
    """Return the generated validation helper filename."""
    prefix = configured_artifact_prefix()
    if not prefix:
        return VALIDATION_HELPER_NAME
    return "lz_run_validation_{0}.sh".format(prefix)


def build_validation_wrapper_specs(transfers_df):
    """Build per-flow-group validation wrapper definitions."""
    if transfers_df is None or transfers_df.empty:
        return []

    flow_groups = {}
    for transfer in definitions_from_dataframe(transfers_df):
        flow_group = transfer.flow_group.strip()
        if not flow_group or flow_group == 'nan':
            continue
        if not transfer.is_entry_point:
            continue
        flow_groups.setdefault(flow_group, []).append(transfer)

    specs = []
    script_names = set()
    for flow_group, entries in sorted(flow_groups.items()):
        if len(entries) != 1:
            identifiers = ', '.join(sorted(entry.identifier for entry in entries))
            raise ValueError(
                "flow_group '{0}' must have exactly one entry-point transfer to generate "
                "a validation wrapper; found {1}: {2}".format(
                    flow_group, len(entries), identifiers
                )
            )
        transfer = entries[0]
        script_name = validation_wrapper_script_name(flow_group)
        if script_name in script_names:
            raise ValueError(
                "validation wrapper script names must be unique after sanitization: {0}".format(
                    script_name
                )
            )
        script_names.add(script_name)
        specs.append({
            'script_name': script_name,
            'flow_group': flow_group,
            'entry_dir': os.path.abspath(normalize_source_path(transfer.source)),
            'next_hop': transfer.destination.strip(),
            'next_hop_port': transfer.destination_port.strip(),
            'producer': transfer.system.strip(),
            'fixture_dir': resolve_validation_fixture_dir(transfer),
        })
    return specs


def generate_validation_wrapper_content(spec):
    """Generate a self-contained flow-group wrapper around the shared helper."""
    next_hop_default = spec['next_hop']
    next_hop_port_default = spec['next_hop_port']
    return """#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
HELPER_SCRIPT="$SCRIPT_DIR/{helper_name}"

FIXTURE_DIR_DEFAULT={fixture_dir}
ENTRY_DIR_DEFAULT={entry_dir}
NEXT_HOP_DEFAULT={next_hop}
NEXT_HOP_PORT_DEFAULT={next_hop_port}
FLOW_GROUP_DEFAULT={flow_group}
PRODUCER_DEFAULT={producer}

if [ "$#" -eq 0 ]; then
    set -- run
fi

COMMAND="$1"

case "$COMMAND" in
    --help|-h|help)
        exec "$HELPER_SCRIPT" --help
        ;;
    -*)
        set -- run "$@"
        COMMAND="$1"
        ;;
esac

shift

FIXTURE_DIR="${{LZ_VALIDATION_FIXTURE_DIR:-$FIXTURE_DIR_DEFAULT}}"
ENTRY_DIR="${{LZ_VALIDATION_ENTRY_DIR:-$ENTRY_DIR_DEFAULT}}"
NEXT_HOP="${{LZ_VALIDATION_NEXT_HOP:-$NEXT_HOP_DEFAULT}}"
NEXT_HOP_PORT="${{LZ_VALIDATION_NEXT_HOP_PORT:-$NEXT_HOP_PORT_DEFAULT}}"
FLOW_GROUP="${{LZ_VALIDATION_FLOW_GROUP:-$FLOW_GROUP_DEFAULT}}"
PRODUCER="${{LZ_VALIDATION_PRODUCER:-$PRODUCER_DEFAULT}}"

set -- "$COMMAND" \
    --fixture-dir "$FIXTURE_DIR" \
    --entry-dir "$ENTRY_DIR" \
    --flow-group "$FLOW_GROUP" \
    --producer "$PRODUCER" \
    "$@"

if [ -n "$NEXT_HOP" ]; then
    set -- "$@" --next-hop "$NEXT_HOP"
fi
if [ -n "$NEXT_HOP_PORT" ]; then
    set -- "$@" --next-hop-port "$NEXT_HOP_PORT"
fi

exec "$HELPER_SCRIPT" "$@"
""".format(
        helper_name=validation_helper_name(),
        fixture_dir=shlex.quote(spec['fixture_dir']),
        entry_dir=shlex.quote(spec['entry_dir']),
        next_hop=shlex.quote(next_hop_default),
        next_hop_port=shlex.quote(next_hop_port_default),
        flow_group=shlex.quote(spec['flow_group']),
        producer=shlex.quote(spec['producer']),
    )


def generate_validation_script_content():
    """Return the shared hop-local validation helper shell script."""
    return load_bundled_template(VALIDATION_TEMPLATE_NAME)


def validation_script_names(transfers_df=None):
    """Return generated validation helper and wrapper filenames."""
    names = {validation_helper_name()}
    names.update(
        spec['script_name'] for spec in build_validation_wrapper_specs(transfers_df)
    )
    return sorted(names)


def write_validation_scripts(validation_scripts_dir, transfers_df=None):
    """Write shared helper shell scripts into the validation scripts directory."""
    if not os.path.isdir(validation_scripts_dir):
        os.makedirs(validation_scripts_dir)
    validation_script_path = os.path.join(validation_scripts_dir, validation_helper_name())
    with open(validation_script_path, 'w') as handle:
        handle.write(add_owner_marker(generate_validation_script_content()))
    os.chmod(validation_script_path, 0o755)

    for spec in build_validation_wrapper_specs(transfers_df):
        wrapper_path = os.path.join(validation_scripts_dir, spec['script_name'])
        with open(wrapper_path, 'w') as handle:
            handle.write(add_owner_marker(generate_validation_wrapper_content(spec)))
        os.chmod(wrapper_path, 0o755)


def remove_stale_generated_scripts(scripts_dir, expected_script_names):
    """Delete orphaned generated shell scripts from the output directory."""
    if not os.path.isdir(scripts_dir):
        return

    expected = set(expected_script_names)
    for entry in os.listdir(scripts_dir):
        if not entry.endswith('.sh'):
            continue
        if entry in expected:
            continue
        path = os.path.join(scripts_dir, entry)
        if not file_has_current_owner_marker(path):
            continue
        os.remove(path)


def cron_file_name(runtime_id):
    """Return the generated cron filename for a runtime_id group."""
    prefix = configured_artifact_prefix()
    if prefix:
        return "{0}.{1}.Landing_Zone.cron".format(prefix, runtime_id)
    return "{0}.Landing_Zone.cron".format(runtime_id)


def remove_stale_cron_files(crontab_dir, expected_cron_names):
    """Delete stale owned cron files from the output directory."""
    if not current_artifact_owner_id() or not os.path.isdir(crontab_dir):
        return

    expected = set(expected_cron_names)
    for entry in os.listdir(crontab_dir):
        if not entry.endswith('.cron'):
            continue
        if entry in expected:
            continue
        path = os.path.join(crontab_dir, entry)
        if not file_has_current_owner_marker(path):
            continue
        os.remove(path)


def remove_stale_validation_scripts(validation_scripts_dir, transfers_df=None):
    """Delete orphaned validation helper/wrapper scripts from the output directory."""
    if not os.path.isdir(validation_scripts_dir):
        return

    expected = set(validation_script_names(transfers_df))
    for entry in os.listdir(validation_scripts_dir):
        if not entry.endswith('.sh'):
            continue
        if entry in expected:
            continue
        path = os.path.join(validation_scripts_dir, entry)
        if not file_has_current_owner_marker(path):
            continue
        os.remove(path)


def generate_cron_file(runtime_id, transfers_df, scripts_dir):
    """Generate complete cron file content from DataFrame subset"""
    # Get the first row to extract system and user info
    first_transfer = transfers_df.iloc[0]
    system = first_transfer['system']
    user = first_transfer.get('users', first_transfer.get('user', ''))
    
    content = generate_cron_header(system, user)
    
    for i, (_, transfer) in enumerate(transfers_df.iterrows()):
        if i > 0:
            content += "\n"
        
        # Add comment describing the transfer
        source = transfer['source']
        dest = transfer['destination']
        identifier = transfer['identifiers']
        content += "# [{0}] Transfer from {1} to {2}\n".format(identifier, source, dest)
        
        script_path = get_deployed_script_path(system, transfer['script_name'])
        content += generate_cron_entry(transfer, script_path) + "\n"
    
    return content


def main(argv=None):
    """Main function to generate all cron files."""
    parser = argparse.ArgumentParser(
        description='Generate cron files from transfers.tsv configuration'
    )
    parser.add_argument(
        '--config', '-c',
        default=None,
        help='Path to config.yaml file (default: auto-detect in current directory)'
    )
    parser.add_argument(
        '--transfers', '-t',
        default=None,
        help='Path to transfers.tsv file (default: config/transfers.tsv)'
    )
    parser.add_argument(
        '--output-dir', '-o',
        default=None,
        help='Output directory for generated cron files (default: output/crontab.d)'
    )
    parser.add_argument(
        '--log-dir', '-l',
        default=None,
        help='Default log directory for transfer logs (default: log/)'
    )
    parser.add_argument(
        '--scripts-dir', '-s',
        default=None,
        help='Output directory for generated shell scripts (default: output/scripts)'
    )
    parser.add_argument(
        '--validation-scripts-dir',
        default=None,
        help='Output directory for generated validation wrapper scripts (default: output/validation_scripts)'
    )
    parser.add_argument(
        '--runtime-id',
        action='append',
        default=[],
        help='Exact runtime_id to include. May be passed multiple times.'
    )
    args = parser.parse_args(argv)
    
    # Load configuration from file and/or command line arguments
    config.load_config(
        config_file=args.config,
        transfers_file=args.transfers,
        crontab_dir=args.output_dir,
        log_dir=args.log_dir,
        validation_scripts_dir=args.validation_scripts_dir,
    )
    
    transfers_file = config.transfers_file
    output_dir = config.crontab_dir
    log_dir = config.log_dir
    scripts_dir = args.scripts_dir or os.path.join(
        os.path.dirname(output_dir), 'scripts'
    )
    validation_scripts_dir = config.validation_scripts_dir
    
    if not os.path.exists(transfers_file):
        print("Error: {0} not found".format(transfers_file))
        return 1
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Create log directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    if not os.path.exists(scripts_dir):
        os.makedirs(scripts_dir)
    if not os.path.exists(validation_scripts_dir):
        os.makedirs(validation_scripts_dir)
    
    # Parse transfers into DataFrame using pandas
    try:
        transfers_df = parse_transfers_file(transfers_file, runtime_ids=args.runtime_id)
    except ValueError as exc:
        print("Error: {0}".format(exc))
        return 1
    
    if transfers_df.empty:
        print("No transfers found in the file")
        return 1
    
    # Check for overlapping source paths
    overlap_warnings = check_overlapping_sources(transfers_df)
    if overlap_warnings:
        print("\n\033[93m⚠ WARNING: Overlapping source paths detected!\033[0m")
        print("=" * 60)
        for warning in overlap_warnings:
            print("\n" + warning)
        print("\n" + "=" * 60)
        print("Consider adjusting your transfers.tsv to avoid conflicts.")
        print("Continuing with generation...\n")

    shared_file_pair_warnings = transfers_df.attrs.get('shared_file_pair_warnings', [])
    if shared_file_pair_warnings:
        print("\n\033[93m⚠ WARNING: Shared log/flock pairs detected!\033[0m")
        print("=" * 60)
        for warning in shared_file_pair_warnings:
            print("\n" + warning)
        print("\n" + "=" * 60)
        print("Review transfer definitions for accidental log/lock reuse.")
        print("Continuing with generation...\n")
    
    remove_stale_generated_scripts(
        scripts_dir,
        transfers_df['script_name'].dropna().tolist(),
    )
    remove_stale_validation_scripts(validation_scripts_dir, transfers_df)
    expected_cron_names = [
        cron_file_name(runtime_id)
        for runtime_id in transfers_df['runtime_id'].dropna().unique()
    ]
    remove_stale_cron_files(output_dir, expected_cron_names)
    write_validation_scripts(validation_scripts_dir, transfers_df)

    # Group by runtime_id and generate cron files
    grouped = transfers_df.groupby('runtime_id')

    for runtime_id, group_df in grouped:
        for _, transfer in group_df.iterrows():
            script_path = os.path.join(scripts_dir, transfer['script_name'])
            script_content = generate_script_content(transfer)
            with open(script_path, 'w') as file:
                file.write(add_owner_marker(script_content))
            os.chmod(script_path, 0o755)

        filename = cron_file_name(runtime_id)
        content = generate_cron_file(runtime_id, group_df, scripts_dir)
        
        # Write the cron file
        output_path = os.path.join(output_dir, filename)
        with open(output_path, 'w') as file:
            file.write(add_owner_marker(content))
        
        # Get unique log files for this group
        log_files = group_df['log_file'].dropna().unique()
        # Filter out empty strings
        log_files = [lf for lf in log_files if lf.strip()]
        if len(log_files) > 0:
            log_info = " (logs to: {0})".format(', '.join(log_files))
        else:
            log_info = ""
        
        transfer_count = len(group_df)
        print("Generated {0} with {1} transfer(s)".format(
            filename, transfer_count))
        if log_info:
            print("  {0}".format(log_info.strip()))
    
    # Print summary statistics
    print("\nSummary:")
    print("Total transfers: {0}".format(len(transfers_df)))
    unique_combinations = transfers_df['runtime_id'].nunique()
    print("Unique runtime IDs: {0}".format(unique_combinations))
    if 'system' in transfers_df.columns:
        print("Systems: {0}".format(', '.join(transfers_df['system'].unique())))
    if 'users' in transfers_df.columns:
        print("Users: {0}".format(', '.join(transfers_df['users'].unique())))
    print("Validation scripts: {0}".format(validation_scripts_dir))
    
    # Show log file information
    if 'log_file' in transfers_df.columns:
        unique_logs = transfers_df['log_file'].dropna().unique()
        # Filter out empty strings
        unique_logs = [lf for lf in unique_logs if lf.strip()]
        if len(unique_logs) > 0:
            print("Log files: {0}".format(', '.join(unique_logs)))
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
