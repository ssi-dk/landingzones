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
    df['script_name'] = sanitized_identifiers + '.sh'
    df = resolve_transfer_file_paths(df)
    
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
            io_nice_cmd, options, source, destination)
    else:
        rsync_cmd = "rsync {0} {1} {2}".format(options, source, destination)

    # For find, strip a trailing wildcard so 'find' targets the parent dir
    # This avoids passing a literal '*' to find (which won't expand inside -c quotes)
    find_target = source
    if source.endswith('/*'):
        find_target = source[:-2]
    elif source.endswith('*'):
        # Generic fallback: remove trailing '*' and any trailing '/'
        find_target = source.rstrip('*').rstrip('/')
    find_cmd = "find {0} -mindepth 1 -type d -empty -delete".format(find_target)

    if not flock_file or not flock_file.strip():
        raise ValueError("flock_file is required but not specified for "
                         "transfer: {0} -> {1}".format(source, destination))

    latest_log_file = "{0}.latest".format(log_file) if log_file else ''
    flock_command = config.get_flock_path(transfer['system'])

    return {
        'rsync_cmd': rsync_cmd,
        'find_cmd': find_cmd,
        'log_file': log_file,
        'latest_log_file': latest_log_file,
        'flock_file': flock_file,
        'flock_command': flock_command,
    }


def build_transfer_command(transfer):
    """Build the shell command executed by a transfer script."""
    commands = build_transfer_commands(transfer)
    log_file = commands['log_file']
    log_redirect = " >> {0} 2>&1".format(log_file) if log_file else ''
    if log_redirect:
        return "{0}{1} && {2}{3}".format(
            commands['rsync_cmd'], log_redirect, commands['find_cmd'], log_redirect
        )
    return "{0} && {1}".format(commands['rsync_cmd'], commands['find_cmd'])


def generate_script_content(transfer):
    """Generate shell script content for a transfer."""
    commands = build_transfer_commands(transfer)
    return """#!/bin/sh
set -eu

log_file="{0}"
latest_log_file="{1}"
flock_file="{2}"
run_log="$(mktemp "${{TMPDIR:-/tmp}}/landingzones.{3}.rsync.XXXXXX")"
cleanup_log="$(mktemp "${{TMPDIR:-/tmp}}/landingzones.{3}.cleanup.XXXXXX")"

cleanup() {{
    rm -f "$run_log" "$cleanup_log"
}}
trap cleanup EXIT HUP INT TERM

exec 9>"$flock_file"
if ! {4} -n 9; then
    exit 0
fi

if {5} >"$run_log" 2>&1; then
    rsync_status=0
else
    rsync_status=$?
fi

cat "$run_log" >> "$log_file"

if [ "$rsync_status" -ne 0 ]; then
    printf '%s\n' "rsync failed with exit code $rsync_status" >> "$log_file"
    cat "$run_log" > "$latest_log_file"
    exit "$rsync_status"
fi

{6} >"$cleanup_log" 2>&1
if [ -s "$cleanup_log" ]; then
    cat "$cleanup_log" >> "$log_file"
fi

if sed '/^sending incremental file list$/d; /^sent .* bytes .*$/d; /^total size is .*$/d; /^$/d' "$run_log" | grep -q .; then
    cat "$run_log" > "$latest_log_file"
    if [ -s "$cleanup_log" ]; then
        cat "$cleanup_log" >> "$latest_log_file"
    fi
fi
""".format(
        commands['log_file'],
        commands['latest_log_file'],
        commands['flock_file'],
        sanitize_identifier(transfer.get('identifiers', 'transfer')),
        commands['flock_command'],
        commands['rsync_cmd'],
        commands['find_cmd'],
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
