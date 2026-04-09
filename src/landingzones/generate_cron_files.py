#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate cron files from transfers.tsv configuration.

Creates a .cron file for each system-user combination with rsync commands.
"""

import os
import sys
import argparse
import re
import shlex
import pandas as pd

from landingzones.config import config


def parse_transfers_file(filename):
    """Parse the transfers.tsv file and return a pandas DataFrame"""
    # Read the TSV file with pandas - now with proper header line
    df = pd.read_csv(filename, sep='\t')
    
    # Filter out commented lines (rows where system starts with #)
    df = df[~df['system'].astype(str).str.startswith('#')]
    
    # Filter by enabled column if present - only process rows where enabled is TRUE
    if 'enabled' in df.columns:
        df['enabled'] = df['enabled'].astype(str).str.strip().str.upper()
        df = df[df['enabled'] == 'TRUE']

    if 'identifiers' not in df.columns:
        df.insert(0, 'identifiers', [
            "transfer_{0:03d}".format(i) for i in range(1, len(df) + 1)
        ])
    
    # Clean up any extra whitespace in string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    
    # Create system_user combination column
    df['system_user'] = df['system'] + '.' + df['users']
    
    # Handle empty columns and convert destination_port to string
    df['rsync_options'] = df['rsync_options'].fillna('').astype(str)
    df['log_file'] = df['log_file'].fillna('').astype(str)
    df['flock_file'] = df['flock_file'].fillna('').astype(str)
    if 'io_nice' not in df.columns:
        df['io_nice'] = ''
    df['io_nice'] = df['io_nice'].fillna('').astype(str)
    
    # Handle frequency column - use default if not present or empty
    if 'frequency' not in df.columns:
        df['frequency'] = ''
    df['frequency'] = df['frequency'].fillna('').astype(str)
    
    # Handle destination_port and source_port specially (may be numeric)
    df['destination_port'] = df['destination_port'].fillna('').astype(str)
    
    # Handle source_port if it exists, otherwise create empty column
    if 'source_port' not in df.columns:
        df['source_port'] = ''
    df['source_port'] = df['source_port'].fillna('').astype(str)
    
    # Remove rows where columns contain 'nan' (from NaN values)
    df['rsync_options'] = df['rsync_options'].replace('nan', '')
    df['log_file'] = df['log_file'].replace('nan', '')
    df['destination_port'] = df['destination_port'].replace('nan', '')
    df['source_port'] = df['source_port'].replace('nan', '')
    df['flock_file'] = df['flock_file'].replace('nan', '')
    df['frequency'] = df['frequency'].replace('nan', '')
    df['io_nice'] = df['io_nice'].replace('nan', '')
    df['identifiers'] = df['identifiers'].replace('nan', '')
    
    # Clean up destination_port - remove .0 if it's a whole number
    df['destination_port'] = df['destination_port'].str.replace(
        r'\.0$', '', regex=True)

    if (df['identifiers'] == '').any():
        raise ValueError("identifiers is required for all enabled transfers")
    if (df['log_file'] == '').any():
        raise ValueError("log_file is required for all enabled transfers")

    sanitized_identifiers = df['identifiers'].apply(sanitize_identifier)
    if (sanitized_identifiers == '').any():
        raise ValueError("identifiers must contain at least one filename-safe character")
    if sanitized_identifiers.duplicated().any():
        duplicates = sorted(df.loc[sanitized_identifiers.duplicated(keep=False), 'identifiers'].unique())
        raise ValueError(
            "identifiers must be unique after filename sanitization: {0}".format(
                ', '.join(duplicates)
            )
        )

    validate_transfer_endpoints(df)
    df['script_name'] = sanitized_identifiers + '.sh'
    df = resolve_transfer_file_paths(df)
    df.attrs['shared_file_pair_warnings'] = audit_shared_file_pairs(df)
    
    return df


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


def sanitize_identifier(identifier):
    """Convert a transfer identifier into a safe shell script file name stem."""
    value = str(identifier).strip() if identifier is not None else ''
    if not value or value == 'nan':
        return ''
    value = re.sub(r'[^A-Za-z0-9._-]+', '_', value)
    return value.strip('._-')


def split_remote_path(path):
    """Split an rsync path into remote target and filesystem path."""
    value = str(path).strip() if path is not None else ''
    if not value or ':' not in value:
        return None, value
    remote, remote_path = value.split(':', 1)
    if not remote or not remote_path:
        return None, value
    return remote, remote_path


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


def audit_shared_file_pairs(df):
    """Return warnings for transfers that share the same log/flock pair."""
    warnings = []
    grouped = df.groupby(['log_file', 'flock_file'], dropna=False)
    for (log_file, flock_file), group_df in grouped:
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
    move_cmd = (
        "find {0} -mindepth 1 -maxdepth 1 ! -name '.staging' -exec mv {{}} {1}/ \\;".format(
            shell_quote(staging_dir),
            shell_quote(destination_dir),
        )
    )
    cleanup_cmd = (
        "{{ rmdir {0} 2>/dev/null || true; }} && "
        "{{ rmdir {1} 2>/dev/null || true; }}".format(
            shell_quote(staging_dir),
            shell_quote(os.path.dirname(staging_dir)),
        )
    )
    full_cmd = "{0} && {1}".format(move_cmd, cleanup_cmd)
    if remote:
        ssh_cmd = build_ssh_command(remote, port)
        return '{0} "{1}"'.format(ssh_cmd, full_cmd)
    return full_cmd


def resolve_transfer_file_paths(df):
    """Resolve per-system log and flock file names into full paths."""
    df = df.copy()
    df['log_file'] = df.apply(
        lambda row: config.resolve_managed_file_path(
            row['system'], row['log_file'], 'log'
        ),
        axis=1
    )
    df['flock_file'] = df.apply(
        lambda row: config.resolve_managed_file_path(
            row['system'], row['flock_file'], 'flock'
        ),
        axis=1
    )
    return df


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
    if io_nice_cmd:
        rsync_cmd = "{0} {1}".format(io_nice_cmd, rsync_cmd)

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
    else:
        source_loop = (
            'find {0} -mindepth 1 -maxdepth 1 -type d ! -name ".*" -print | '
            'while IFS= read -r source_dir; do'
        ).format(shell_path(source_root))
        rsync_source = '"$source_dir/"'

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
            '{0} "if [ -d \\"{1}/$dir_name\\" ]; then '
            'find \\"{1}/.staging/$dir_name\\" -mindepth 1 -maxdepth 1 ! -name \\".staging\\" -exec mv {{}} \\"{1}/$dir_name/\\" \\; && '
            'rmdir \\"{1}/.staging/$dir_name\\" 2>/dev/null || true; '
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
    else:
        mkdir_cmd = 'mkdir -p "{0}/.staging/$dir_name"'.format(destination_root)
        promote_cmd = (
            'if [ -d "{0}/$dir_name" ]; then '
            'find "{0}/.staging/$dir_name" -mindepth 1 -maxdepth 1 ! -name ".staging" -exec mv {{}} "{0}/$dir_name"/ \\; && '
            'rmdir "{0}/.staging/$dir_name" 2>/dev/null || true; '
            'else '
            'mv "{0}/.staging/$dir_name" "{0}/$dir_name"; '
            'fi; '
            'rmdir "{0}/.staging" 2>/dev/null || true'
        ).format(destination_root)
        rsync_destination = '"{0}/.staging/$dir_name/"'.format(destination_root)

    return """#!/bin/sh
set -eu

log_file="{0}"
latest_log_file="{1}"
mini_log_file="{2}"
flock_file="{3}"
run_log="$(mktemp "${{TMPDIR:-/tmp}}/landingzones.{4}.rsync.XXXXXX")"
cleanup_log="$(mktemp "${{TMPDIR:-/tmp}}/landingzones.{4}.cleanup.XXXXXX")"
promote_log="$(mktemp "${{TMPDIR:-/tmp}}/landingzones.{4}.promote.XXXXXX")"

cleanup() {{
    rm -f "$run_log" "$cleanup_log" "$promote_log"
}}
debug_enabled() {{
    [ -t 1 ] || [ "${{LZ_DEBUG_CLI:-0}}" = "1" ]
}}

log_status() {{
    printf '%s %s\\n' "$(date '+%Y-%m-%d %H:%M:%S%z')" "$1" >> "$mini_log_file"
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

on_exit() {{
    status=$?
    if [ "$status" -ne 0 ]; then
        debug "script failed with exit code $status"
        dump_debug_log "run log" "$run_log"
        dump_debug_log "promote log" "$promote_log"
        dump_debug_log "cleanup log" "$cleanup_log"
    fi
    cleanup
    exit "$status"
}}
trap on_exit EXIT HUP INT TERM

mkdir -p "$(dirname "$log_file")" "$(dirname "$latest_log_file")" "$(dirname "$mini_log_file")" "$(dirname "$flock_file")"
debug "using lock file $flock_file"
{15} 
exec 9>"$flock_file"
if ! {5} -n 9; then
    debug "lock busy, exiting"
    exit 0
fi

if ! {6}; then
    log_status "{7}"
    printf '%s %s\\n' "$(date '+%Y-%m-%d %H:%M:%S%z')" "{7}" >> "$log_file"
    debug "{7}"
    exit 0
fi

: >"$run_log"
: >"$promote_log"
{8}
    [ -n "$source_dir" ] || continue
    dir_name=$(basename "$source_dir")
    log_status "$dir_name initiated"
    debug "$dir_name initiated"
    {9} >>"$promote_log" 2>&1
    {10} {11} {12} >>"$run_log" 2>&1
    {13} >>"$promote_log" 2>&1
    log_status "$dir_name completed"
    debug "$dir_name completed"
done
if [ -s "$run_log" ]; then
    cat "$run_log" >> "$log_file"
fi
if [ -s "$promote_log" ]; then
    cat "$promote_log" >> "$log_file"
fi

{14} >"$cleanup_log" 2>&1
if [ -s "$cleanup_log" ]; then
    cat "$cleanup_log" >> "$log_file"
fi

if sed '/^sending incremental file list$/d; /^sent .* bytes .*$/d; /^total size is .*$/d; /^$/d' "$run_log" | grep -q .; then
    cat "$run_log" > "$latest_log_file"
    if [ -s "$promote_log" ]; then
        cat "$promote_log" >> "$latest_log_file"
    fi
    if [ -s "$cleanup_log" ]; then
        cat "$cleanup_log" >> "$latest_log_file"
    fi
fi
""".format(
        commands['log_file'],
        commands['latest_log_file'],
        commands['mini_log_file'],
        commands['flock_file'],
        sanitize_identifier(transfer.get('identifiers', 'transfer')),
        commands['flock_command'],
        source_exists_cmd,
        'source directory missing: {0}'.format(source_root),
        source_loop,
        mkdir_cmd,
        rsync_cmd,
        rsync_source,
        rsync_destination,
        promote_cmd,
        commands['find_cmd'],
        remote_destination_setup.rstrip(),
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
        os.remove(os.path.join(scripts_dir, entry))


def generate_cron_file(system_user, transfers_df, scripts_dir):
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


def main():
    """Main function to generate all cron files"""
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
    args = parser.parse_args()
    
    # Load configuration from file and/or command line arguments
    config.load_config(
        config_file=args.config,
        transfers_file=args.transfers,
        crontab_dir=args.output_dir,
        log_dir=args.log_dir
    )
    
    transfers_file = config.transfers_file
    output_dir = config.crontab_dir
    log_dir = config.log_dir
    scripts_dir = args.scripts_dir or os.path.join(
        os.path.dirname(output_dir), 'scripts'
    )
    
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
    
    # Parse transfers into DataFrame using pandas
    transfers_df = parse_transfers_file(transfers_file)
    
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

    # Group by system_user and generate cron files
    grouped = transfers_df.groupby('system_user')

    for system_user, group_df in grouped:
        for _, transfer in group_df.iterrows():
            script_path = os.path.join(scripts_dir, transfer['script_name'])
            script_content = generate_script_content(transfer)
            with open(script_path, 'w') as file:
                file.write(script_content)
            os.chmod(script_path, 0o755)

        filename = "{0}.Landing_Zone.cron".format(system_user)
        content = generate_cron_file(system_user, group_df, scripts_dir)
        
        # Write the cron file
        output_path = os.path.join(output_dir, filename)
        with open(output_path, 'w') as file:
            file.write(content)
        
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
    unique_combinations = transfers_df['system_user'].nunique()
    print("Unique system-user combinations: {0}".format(
        unique_combinations))
    print("Systems: {0}".format(', '.join(transfers_df['system'].unique())))
    print("Users: {0}".format(', '.join(transfers_df['users'].unique())))
    
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
