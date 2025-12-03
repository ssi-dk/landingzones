#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate cron files from transfers.tsv configuration.

Creates a .cron file for each system-user combination with rsync commands.
"""

import os
import sys
import argparse
import pandas as pd

from landingzones.config import config

def parse_transfers_file(filename):
    """Parse the transfers.tsv file and return a pandas DataFrame"""
    # Read the TSV file with pandas - now with proper header line
    df = pd.read_csv(filename, sep='\t')
    
    # Filter out commented lines (rows where system starts with #)
    df = df[~df['system'].astype(str).str.startswith('#')]
    
    # Clean up any extra whitespace in string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    
    # Create system_user combination column
    df['system_user'] = df['system'] + '.' + df['users']
    
    # Handle empty columns and convert destination_port to string
    df['rsync_options'] = df['rsync_options'].fillna('').astype(str)
    df['log_file'] = df['log_file'].fillna('').astype(str)
    df['flock_file'] = df['flock_file'].fillna('').astype(str)
    
    # Handle frequency column - use default if not present or empty
    if 'frequency' not in df.columns:
        df['frequency'] = ''
    df['frequency'] = df['frequency'].fillna('').astype(str)
    
    # Handle destination_port specially (may be numeric)
    df['destination_port'] = df['destination_port'].fillna('').astype(str)
    
    # Remove rows where columns contain 'nan' (from NaN values)
    df['rsync_options'] = df['rsync_options'].replace('nan', '')
    df['log_file'] = df['log_file'].replace('nan', '')
    df['destination_port'] = df['destination_port'].replace('nan', '')
    df['flock_file'] = df['flock_file'].replace('nan', '')
    df['frequency'] = df['frequency'].replace('nan', '')
    
    # Clean up destination_port - remove .0 if it's a whole number
    df['destination_port'] = df['destination_port'].str.replace(
        r'\.0$', '', regex=True)
    
    return df

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

def generate_rsync_command(transfer):
    """Generate rsync command for a transfer"""
    source = transfer['source']
    destination = transfer['destination']
    rsync_options = transfer.get('rsync_options', '')
    log_file = transfer.get('log_file', '')
    destination_port = transfer.get('destination_port', '')
    flock_file = transfer.get('flock_file', '')
    frequency = transfer.get('frequency', '')
    
    # Ensure all values are strings and handle potential NaN values
    rsync_options = str(rsync_options) if rsync_options is not None else ''
    log_file = str(log_file) if log_file is not None else ''
    destination_port = (str(destination_port)
                        if destination_port is not None else '')
    flock_file = str(flock_file) if flock_file is not None else ''
    frequency = str(frequency) if frequency is not None else ''
    
    # Clean up any 'nan' strings that might have come from NaN values
    if rsync_options == 'nan':
        rsync_options = ''
    if log_file == 'nan':
        log_file = ''
    if destination_port == 'nan':
        destination_port = ''
    if flock_file == 'nan':
        flock_file = ''
    if frequency == 'nan':
        frequency = ''
    
    # Base rsync options
    base_options = "-av --remove-source-files"
    
    # Add SSH port option if destination_port is specified
    if destination_port and destination_port.strip().isdigit():
        port_option = '-e "ssh -p {0}"'.format(destination_port.strip())
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
    
    # Add logging if log_file is specified
    log_redirect = ""
    if log_file:
        log_redirect = " >> {0} 2>&1".format(log_file)
    
    # Generate the rsync command with cleanup and logging as a single line
    # Create the complete command as a single line
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
    
    if log_redirect:
        full_cmd = "{0}{1} && {2}{3}".format(
            rsync_cmd, log_redirect, find_cmd, log_redirect)
    else:
        full_cmd = "{0} && {1}".format(rsync_cmd, find_cmd)
    
    # Flock is now required - validate that flock_file is specified
    if not flock_file or not flock_file.strip():
        raise ValueError("flock_file is required but not specified for "
                         "transfer: {0} -> {1}".format(source, destination))
    
    # Always use flock with the specified lock file
    flock_path = flock_file.strip()
    
    # Use frequency from transfer config, or fall back to default
    cron_schedule = frequency.strip() if frequency.strip() else config.default_cron_frequency
    
    command = "{0} /usr/bin/flock -n {1} -c '{2}'".format(
        cron_schedule, flock_path, full_cmd)
    
    return command


def generate_cron_file(system_user, transfers_df):
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
        content += "# Transfer from {0} to {1}\n".format(source, dest)
        
        # Add the rsync command
        content += generate_rsync_command(transfer) + "\n"
    
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
    
    if not os.path.exists(transfers_file):
        print("Error: {0} not found".format(transfers_file))
        return 1
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Create log directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Parse transfers into DataFrame using pandas
    transfers_df = parse_transfers_file(transfers_file)
    
    if transfers_df.empty:
        print("No transfers found in the file")
        return 1
    
    # Apply default log file path for entries without one
    default_log_file = os.path.join(log_dir, 'transfers.log')
    transfers_df['log_file'] = transfers_df['log_file'].apply(
        lambda x: x if x and x.strip() else default_log_file
    )
    
    # Group by system_user and generate cron files
    grouped = transfers_df.groupby('system_user')
    
    for system_user, group_df in grouped:
        filename = "{0}.Landing_Zone.cron".format(system_user)
        content = generate_cron_file(system_user, group_df)
        
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
