#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Pre-deployment verification script for Landing Zone.

Checks all prerequisites before deploying cron files:
- Local directories exist and are writable
- Remote SSH connections work
- Log directories exist
- Required tools are available
"""

import os
import sys
import subprocess
import socket
import shutil
import glob
import errno
import argparse
import tempfile
import shlex
from datetime import datetime
from io import StringIO

import pandas as pd

from landingzones.config import config
from landingzones.generate_cron_files import (
    parse_transfers_file,
    normalize_source_path,
    split_remote_path,
    shell_path,
)


class Colors:
    """ANSI color codes for console output"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'


RUN_TEST_FILE_NAME = 'lz-check-deployment.txt'


def get_test_flock_path():
    """Prefer a real flock binary, but allow a no-op lock command for local tests."""
    flock_path = shutil.which('flock')
    if flock_path:
        return flock_path
    return '/usr/bin/true'


def print_status(message, status, details=None):
    """Print formatted status message"""
    if status == "OK":
        icon = "{0}✓{1}".format(Colors.GREEN, Colors.END)
        status_text = "{0}OK{1}".format(Colors.GREEN, Colors.END)
    elif status == "WARN":
        icon = "{0}⚠{1}".format(Colors.YELLOW, Colors.END)
        status_text = "{0}WARNING{1}".format(Colors.YELLOW, Colors.END)
    elif status == "INFO" or status == "...":
        icon = "{0}ℹ{1}".format(Colors.BLUE, Colors.END)
        status_text = "{0}INFO{1}".format(Colors.BLUE, Colors.END)
    else:  # ERROR
        icon = "{0}✗{1}".format(Colors.RED, Colors.END)
        status_text = "{0}ERROR{1}".format(Colors.RED, Colors.END)
    
    print("{0} {1}: {2}".format(icon, message, status_text))
    if details:
        print("   {0}".format(details))


def print_header(title):
    """Print section header"""
    print("\n{0}{1}=== {2} ==={3}".format(Colors.BOLD, Colors.BLUE, title, Colors.END))


def _snapshot_config_state():
    """Capture config state so temporary overrides can be restored."""
    return {
        'yaml_config': dict(config._yaml_config),
        'runtime_config': dict(config._runtime_config),
        'config_file': config._config_file,
    }


def _restore_config_state(snapshot):
    """Restore config state after temporary overrides."""
    config._yaml_config = snapshot['yaml_config']
    config._runtime_config = snapshot['runtime_config']
    config._config_file = snapshot['config_file']


def get_repo_root():
    """Return the repository root from the installed package path."""
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', '..', '..')
    )


def create_test_root():
    """Create a timestamp-prefixed temporary root for run-tests artifacts."""
    prefix = "{0}_lz_check_".format(datetime.now().strftime('%Y%m%dT%H%M%S'))
    return tempfile.mkdtemp(prefix=prefix)


def list_visible_entries(path):
    """List non-hidden entries under a directory."""
    if not os.path.isdir(path):
        return []
    return sorted(
        name for name in os.listdir(path)
        if not name.startswith('.')
    )


def endpoint_key(value):
    """Return a normalized endpoint key for comparing transfer roots."""
    remote, path = split_remote_path(value)
    root = normalize_source_path(path if remote else value)
    return remote or '', root


def absolutize_local_endpoint(value, base_dir):
    """Resolve a local endpoint against the command working directory."""
    text = str(value).strip()
    remote, _ = split_remote_path(text)
    if remote or not text or os.path.isabs(text):
        return text

    suffix = ''
    if text.endswith('/*'):
        suffix = '/*'
        text = text[:-2]
    elif text.endswith('*'):
        suffix = '*'
        text = text[:-1]
    elif text.endswith('/'):
        suffix = '/'
        text = text[:-1]

    resolved = os.path.normpath(os.path.join(base_dir, text))
    return resolved + suffix


def unique_transfer_endpoints(transfers_df, column, port_column):
    """Return unique endpoints preserving the first seen port value."""
    endpoints = {}
    for _, transfer in transfers_df.iterrows():
        value = transfer[column]
        key = endpoint_key(value)
        if key not in endpoints:
            port = transfer.get(port_column, '')
            port = str(port).strip() if port is not None else ''
            endpoints[key] = {
                'value': value,
                'port': port if port and port != 'nan' else '',
            }
    return endpoints


def build_run_test_plan(transfers_df):
    """Build source/destination relationships for the real-transfer smoke test."""
    source_keys = {
        endpoint_key(transfer['source']) for _, transfer in transfers_df.iterrows()
    }
    destination_keys = {
        endpoint_key(transfer['destination']) for _, transfer in transfers_df.iterrows()
    }

    initial_sources = []
    terminal_destinations = []
    for _, transfer in transfers_df.iterrows():
        source_info = {
            'value': transfer['source'],
            'port': str(transfer.get('source_port', '') or '').strip(),
        }
        destination_info = {
            'value': transfer['destination'],
            'port': str(transfer.get('destination_port', '') or '').strip(),
        }
        if endpoint_key(transfer['source']) not in destination_keys:
            initial_sources.append(source_info)
        if endpoint_key(transfer['destination']) not in source_keys:
            terminal_destinations.append(destination_info)

    return {
        'initial_sources': dedupe_test_endpoints(initial_sources),
        'all_sources': dedupe_test_endpoints([
            {
                'value': transfer['source'],
                'port': str(transfer.get('source_port', '') or '').strip(),
            }
            for _, transfer in transfers_df.iterrows()
        ]),
        'all_destinations': dedupe_test_endpoints([
            {
                'value': transfer['destination'],
                'port': str(transfer.get('destination_port', '') or '').strip(),
            }
            for _, transfer in transfers_df.iterrows()
        ]),
        'terminal_destinations': dedupe_test_endpoints(terminal_destinations),
    }


def dedupe_test_endpoints(endpoints):
    """Remove duplicate endpoints while keeping order."""
    seen = set()
    result = []
    for endpoint in endpoints:
        key = endpoint_key(endpoint['value'])
        if key in seen:
            continue
        seen.add(key)
        result.append(endpoint)
    return result


def get_endpoint_root(endpoint):
    """Return the normalized root path for an endpoint."""
    _, root = endpoint_key(endpoint['value'])
    return root


def join_test_path(root, folder_name):
    """Join a normalized root with a test folder name."""
    return os.path.join(root, folder_name)


def shell_target(user, host):
    """Format a target suitable for ssh."""
    if user:
        return '{0}@{1}'.format(user, host)
    return host


def run_remote_shell(user, host, command, port=''):
    """Run a shell command over ssh."""
    cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10']
    if port:
        cmd.extend(['-p', str(port)])
    cmd.extend([shell_target(user, host), command])
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    return proc.returncode, stdout.decode('utf-8'), stderr.decode('utf-8')


def create_test_folder(endpoint, folder_name):
    """Create a test folder with a marker file under a source root."""
    root = get_endpoint_root(endpoint)
    test_dir = join_test_path(root, folder_name)
    marker_path = os.path.join(test_dir, RUN_TEST_FILE_NAME)
    marker_text = "landingzones run-tests marker for {0}\n".format(folder_name)
    user, host, _ = parse_remote_destination(endpoint['value'])
    port = endpoint.get('port', '')

    if host:
        command = "mkdir -p {0} && printf %s {1} > {2}".format(
            shell_path(test_dir),
            shlex.quote(marker_text),
            shell_path(marker_path),
        )
        rc, _, stderr = run_remote_shell(user, host, command, port)
        if rc != 0:
            raise ValueError(
                "Cannot create test folder on {0}: {1}".format(
                    shell_target(user, host), stderr.strip()
                )
            )
        return

    os.makedirs(test_dir)
    with open(marker_path, 'w') as handle:
        handle.write(marker_text)


def cleanup_test_folder(endpoint, folder_name):
    """Remove a test folder from a source or destination root."""
    root = get_endpoint_root(endpoint)
    test_dir = join_test_path(root, folder_name)
    user, host, _ = parse_remote_destination(endpoint['value'])
    port = endpoint.get('port', '')

    if host:
        command = "rm -rf {0}".format(shell_path(test_dir))
        rc, _, stderr = run_remote_shell(user, host, command, port)
        if rc != 0:
            raise ValueError(
                "Cannot clean test folder on {0}: {1}".format(
                    shell_target(user, host), stderr.strip()
                )
            )
        return

    if os.path.isdir(test_dir):
        shutil.rmtree(test_dir)


def test_folder_exists(endpoint, folder_name):
    """Check whether a test folder exists under an endpoint root."""
    root = get_endpoint_root(endpoint)
    test_dir = join_test_path(root, folder_name)
    marker_path = os.path.join(test_dir, RUN_TEST_FILE_NAME)
    user, host, _ = parse_remote_destination(endpoint['value'])
    port = endpoint.get('port', '')

    if host:
        command = '[ -f {0} ] && echo EXISTS || echo MISSING'.format(
            shell_path(marker_path)
        )
        rc, stdout, stderr = run_remote_shell(user, host, command, port)
        if rc != 0:
            return False, "Remote check failed: {0}".format(stderr.strip())
        return 'EXISTS' in stdout, marker_path

    return os.path.isfile(marker_path), marker_path


def prepare_run_test_environment(test_plan, folder_name):
    """Create one unique test folder in each true source root."""
    cleanup_errors = []
    for endpoint in test_plan['all_destinations'] + test_plan['all_sources']:
        try:
            cleanup_test_folder(endpoint, folder_name)
        except ValueError as exc:
            cleanup_errors.append(str(exc))
    if cleanup_errors:
        raise ValueError('\n'.join(cleanup_errors))

    for endpoint in test_plan['initial_sources']:
        create_test_folder(endpoint, folder_name)


def build_test_runtime_overrides(config_file, transfers_file, test_root):
    """Build runtime overrides for generated script output and test locks/logs."""
    config.load_config(config_file=config_file)
    return {
        'config_file': config_file,
        'transfers_file': transfers_file,
        'crontab_dir': os.path.join(test_root, 'generated', 'crontab.d'),
        'log_dir': os.path.join(test_root, 'generated', 'log'),
    }


def load_run_test_transfers(
    transfers_file, current_system, current_user, output_file, base_dir
):
    """Write the current system/user transfer subset for run-tests."""
    df = parse_transfers_file(transfers_file)
    df = df[(df['system'] == current_system) & (df['users'] == current_user)].copy()
    if df.empty:
        raise ValueError(
            "No transfers found for user '{0}' on system '{1}'".format(
                current_user, current_system
            )
        )
    placeholder_mask = (
        df['source'].astype(str).str.contains(r'\$LZ_TEST_ROOT', regex=True)
        | df['destination'].astype(str).str.contains(r'\$LZ_TEST_ROOT', regex=True)
    )
    skipped = int(placeholder_mask.sum())
    df = df[~placeholder_mask].copy()
    if df.empty:
        raise ValueError(
            "No real transfers found for user '{0}' on system '{1}'. "
            "run-tests ignores placeholder test rows containing $LZ_TEST_ROOT.".format(
                current_user, current_system
            )
        )
    for column in ('source', 'destination'):
        df[column] = df[column].apply(
            lambda value: absolutize_local_endpoint(value, base_dir)
        )
    df.to_csv(output_file, sep='\t', index=False)
    df.attrs['skipped_placeholder_rows'] = skipped
    return df


def build_runtime_test_dataframe(transfers_df, test_root):
    """Redirect run-test logging and locking into the temporary workspace."""
    transfers_df = transfers_df.copy()
    log_dir = os.path.join(test_root, 'generated', 'log')
    flock_dir = os.path.join(test_root, 'generated', 'flock')

    transfers_df['log_file'] = transfers_df['identifiers'].apply(
        lambda ident: os.path.join(log_dir, '{0}.log'.format(ident))
    )
    transfers_df['flock_file'] = transfers_df['identifiers'].apply(
        lambda ident: os.path.join(flock_dir, '{0}.lock'.format(ident))
    )
    return transfers_df


def generate_test_scripts(runtime_overrides, scripts_dir, test_root):
    """Generate shell scripts and cron files for the current transfer subset."""
    from landingzones import generate_cron_files as gcf

    config.load_config(**runtime_overrides)
    transfers_df = gcf.parse_transfers_file(config.transfers_file)
    transfers_df = build_runtime_test_dataframe(transfers_df, test_root)
    os.makedirs(config.crontab_dir)
    os.makedirs(config.log_dir)
    os.makedirs(scripts_dir)

    grouped = transfers_df.groupby('system_user')
    for system_user, group_df in grouped:
        for _, transfer in group_df.iterrows():
            script_path = os.path.join(scripts_dir, transfer['script_name'])
            script_content = gcf.generate_script_content(transfer)
            with open(script_path, 'w') as handle:
                handle.write(script_content)
            os.chmod(script_path, 0o755)

        cron_path = os.path.join(
            config.crontab_dir, "{0}.Landing_Zone.cron".format(system_user)
        )
        with open(cron_path, 'w') as handle:
            handle.write(gcf.generate_cron_file(system_user, group_df, scripts_dir))

    return transfers_df


def run_generated_scripts(transfers_df, scripts_dir, test_root):
    """Execute generated scripts in transfer order."""
    env = os.environ.copy()
    env['LZ_TEST_ROOT'] = test_root
    env['LZ_DEBUG_CLI'] = '1'
    env.setdefault('TMPDIR', test_root)
    results = []

    for _, transfer in transfers_df.iterrows():
        script_path = os.path.join(scripts_dir, transfer['script_name'])
        proc = subprocess.Popen(
            ['/bin/sh', script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=test_root,
        )
        stdout, stderr = proc.communicate()
        results.append({
            'identifier': transfer['identifiers'],
            'script_path': script_path,
            'log_file': transfer.get('log_file', ''),
            'returncode': proc.returncode,
            'stdout': stdout.decode('utf-8'),
            'stderr': stderr.decode('utf-8'),
        })
    return results


def validate_script_test_results(test_plan, folder_name):
    """Validate that the test folder reached each terminal destination."""
    errors = []
    for endpoint in test_plan['terminal_destinations']:
        exists, marker_path = test_folder_exists(endpoint, folder_name)
        if not exists:
            errors.append("Test folder missing at terminal destination: {0}".format(marker_path))
    return errors


def run_local_script_tests(config_file=None, transfers_file=None, keep_test_env=False):
    """Run generated scripts against the real transfer roots using a marker folder."""
    print_header("Transfer Script Tests")
    snapshot = _snapshot_config_state()
    test_root = None
    folder_name = None
    current_system = None
    current_user = None

    try:
        work_root = os.getcwd()
        config.load_config(
            config_file=config_file,
            transfers_file=transfers_file,
        )
        transfers_path = transfers_file or config.transfers_file
        current_system = get_current_system()
        current_user = get_current_user()
        test_root = create_test_root()
        print_status("Test workspace", "INFO", test_root)
        os.makedirs(test_root, exist_ok=True)

        subset_file = os.path.join(test_root, 'run_tests.transfers.tsv')
        subset_df = load_run_test_transfers(
            transfers_path, current_system, current_user, subset_file, work_root
        )
        skipped_placeholder_rows = subset_df.attrs.get('skipped_placeholder_rows', 0)
        if skipped_placeholder_rows:
            print_status(
                "Skipped placeholder transfers",
                "INFO",
                "Ignored {0} row(s) containing $LZ_TEST_ROOT".format(
                    skipped_placeholder_rows
                ),
            )
        test_plan = build_run_test_plan(subset_df)
        folder_name = "lz_check_{0}".format(datetime.now().strftime('%Y%m%dT%H%M%S'))
        prepare_run_test_environment(test_plan, folder_name)
        print_status(
            "Seeded test folders",
            "OK",
            "Prepared {0} starting location(s)".format(
                len(test_plan['initial_sources'])
            ),
        )
        runtime_overrides = build_test_runtime_overrides(
            config_file, subset_file, test_root
        )
        if get_test_flock_path() == '/usr/bin/true':
            print_status(
                "Flock fallback",
                "WARN",
                "No flock binary found on PATH; run-tests are running without lock enforcement",
            )
        scripts_dir = os.path.join(test_root, 'generated', 'scripts')
        transfers_df = generate_test_scripts(runtime_overrides, scripts_dir, test_root)

        print_status(
            "Generated test scripts",
            "OK",
            "Prepared {0} scripts".format(len(transfers_df)),
        )

        run_results = run_generated_scripts(transfers_df, scripts_dir, test_root)
        failed_runs = [result for result in run_results if result['returncode'] != 0]
        if failed_runs:
            first_failure = failed_runs[0]
            log_excerpt = ''
            log_file = first_failure.get('log_file', '')
            if log_file and os.path.exists(log_file):
                with open(log_file, 'r') as handle:
                    log_excerpt = handle.read().strip()
            details = (
                "{0} failed with exit code {1}\nstdout:\n{2}\nstderr:\n{3}".format(
                    first_failure['identifier'],
                    first_failure['returncode'],
                    first_failure['stdout'].strip(),
                    first_failure['stderr'].strip(),
                )
            )
            if log_excerpt:
                details = "{0}\nlog:\n{1}".format(details, log_excerpt)
            print_status("Script execution", "ERROR", details)
            return False

        print_status(
            "Script execution",
            "OK",
            "Executed {0} generated scripts".format(len(run_results)),
        )

        validation_errors = validate_script_test_results(test_plan, folder_name)
        if validation_errors:
            print_status(
                "Script validation",
                "ERROR",
                "\n".join(validation_errors),
            )
            return False

        print_status(
            "Script validation",
            "OK",
            "Test folder reached all terminal destinations",
        )
        return True
    finally:
        _restore_config_state(snapshot)
        if folder_name:
            cleanup_errors = []
            try:
                cleanup_config_path = transfers_file or config.transfers_file
                if cleanup_config_path:
                    config.load_config(
                        config_file=config_file,
                        transfers_file=cleanup_config_path,
                    )
                if current_system is None:
                    current_system = get_current_system()
                if current_user is None:
                    current_user = get_current_user()
                cleanup_df = parse_transfers_file(transfers_file or config.transfers_file)
                cleanup_df = cleanup_df[
                    (cleanup_df['system'] == current_system)
                    & (cleanup_df['users'] == current_user)
                ].copy()
                if not cleanup_df.empty:
                    cleanup_plan = build_run_test_plan(cleanup_df)
                    for endpoint in cleanup_plan['all_destinations'] + cleanup_plan['all_sources']:
                        try:
                            cleanup_test_folder(endpoint, folder_name)
                        except ValueError as exc:
                            cleanup_errors.append(str(exc))
            except Exception as exc:
                cleanup_errors.append(str(exc))
            if cleanup_errors:
                print_status(
                    "Run-tests cleanup",
                    "WARN",
                    "\n".join(cleanup_errors),
                )
            else:
                print_status(
                    "Run-tests cleanup",
                    "OK",
                    "Removed test folders from source and destination roots",
                )
        if test_root and os.path.isdir(test_root):
            if keep_test_env:
                print_status("Test workspace retained", "INFO", test_root)
            else:
                shutil.rmtree(test_root)
                print_status("Test workspace cleanup", "OK", "Removed temporary test data")


def check_required_tools():
    """Check if required system tools are available"""
    print_header("Checking Required Tools")
    
    tools = ['rsync', 'ssh', 'find']
    
    all_good = True
    
    for tool in tools:
        try:
            proc = subprocess.Popen(['which', tool], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()
            result_returncode = proc.returncode
            result_stdout = stdout.decode('utf-8')
            if result_returncode == 0:
                location = result_stdout.strip()
                print_status("{0} available".format(tool), "OK", "Location: {0}".format(location))
            else:
                print_status("{0} missing".format(tool), "ERROR", 
                           "Please install {0}".format(tool))
                all_good = False
        except Exception as e:
            print_status("{0} check failed".format(tool), "ERROR", str(e))
            all_good = False
    
    return all_good


def check_flock_command(system):
    """Check whether the configured flock binary exists and is executable."""
    flock_path = config.get_flock_path(system)
    expanded_path = os.path.expandvars(os.path.expanduser(flock_path))

    if not os.path.exists(expanded_path):
        print_status("Flock binary", "ERROR",
                     "Configured path does not exist: {0}".format(flock_path))
        return False

    if not os.access(expanded_path, os.X_OK):
        print_status("Flock binary", "ERROR",
                     "Configured path is not executable: {0}".format(flock_path))
        return False

    print_status("Flock binary", "OK",
                 "Using: {0}".format(flock_path))
    return True


def check_local_directory(path, description, check_writable=True):
    """Check if a local directory exists and is writable
    
    If the path ends with a wildcard (e.g., /path/to/dir/*), 
    it will check the parent directory instead.
    """
    # Expand both environment variables ($HOME, $USER, etc.) and user home (~)
    expanded_path = os.path.expandvars(os.path.expanduser(path))
    
    # Handle wildcards at the end of the path
    # If path ends with /* or /*, check the parent directory instead
    check_path = expanded_path
    is_wildcard = False
    if expanded_path.endswith('/*') or expanded_path.endswith('*'):
        # Strip the wildcard and trailing slash
        check_path = expanded_path.rstrip('*').rstrip('/')
        is_wildcard = True
        if description == "Source directory":
            description = "Source directory (wildcard pattern)"
    
    if not os.path.exists(check_path):
        print_status("{0}".format(description), "ERROR", "Directory does not exist: {0}".format(path))
        return False
    
    if not os.path.isdir(check_path):
        print_status("{0}".format(description), "ERROR", "Path exists but is not a directory: {0}".format(path))
        return False
    
    if check_writable and not os.access(check_path, os.W_OK):
        print_status("{0}".format(description), "ERROR", "Directory is not writable: {0}".format(path))
        return False
    
    if is_wildcard:
        print_status("{0}".format(description), "OK", "Parent path: {0}".format(check_path))
    else:
        print_status("{0}".format(description), "OK", "Path: {0}".format(check_path))
    return True


def parse_remote_destination(destination):
    """Parse a transfer endpoint into remote target components."""
    value = str(destination).strip() if destination is not None else ''
    if ':' not in value:
        return None, None, value

    remote, path = value.split(':', 1)
    if not remote or not path or '/' in remote:
        return None, None, value

    if '@' in remote:
        user, host = remote.split('@', 1)
        if user and host:
            return user, host, path
        return None, None, value

    return None, remote, path


def build_ssh_target(user, host):
    """Build an ssh target string from parsed endpoint parts."""
    if user:
        return '{0}@{1}'.format(user, host)
    return host


def check_ssh_connection(user, host, port=None):
    """Check SSH connection to remote host"""
    try:
        cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10']
        if port:
            cmd.extend(['-p', str(port)])
        cmd.extend([build_ssh_target(user, host), 'echo', 'SSH_TEST_OK'])
        
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        result_returncode = proc.returncode
        result_stdout = stdout.decode('utf-8')
        result_stderr = stderr.decode('utf-8')
        
        if result_returncode == 0 and 'SSH_TEST_OK' in result_stdout:
            return True, "Connection successful"
        else:
            return False, "SSH failed: {0}".format(result_stderr.strip() if result_stderr else 'Unknown error')
    
    except Exception as e:
        return False, "SSH test error: {0}".format(str(e))


def check_remote_directory(
    user, host, path, port=None, description="Remote directory", check_writable=True
):
    """Check if a remote directory exists and is optionally writable."""
    try:
        cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10']
        if port:
            cmd.extend(['-p', str(port)])
        
        if check_writable:
            test_cmd = (
                '[ -d "{0}" ] && [ -w "{0}" ] && echo "DIR_OK" || echo "DIR_FAIL"'
            ).format(path)
        else:
            test_cmd = '[ -d "{0}" ] && echo "DIR_OK" || echo "DIR_FAIL"'.format(path)
        cmd.extend([build_ssh_target(user, host), test_cmd])
        
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        result_returncode = proc.returncode
        result_stdout = stdout.decode('utf-8')
        result_stderr = stderr.decode('utf-8')
        
        if result_returncode == 0:
            if 'DIR_OK' in result_stdout:
                if check_writable:
                    return True, "Directory exists and is writable"
                return True, "Directory exists"
            if check_writable:
                return False, "Directory does not exist or is not writable"
            return False, "Directory does not exist"
        else:
            return False, "Remote check failed: {0}".format(result_stderr.strip())
    
    except Exception as e:
        return False, "Remote directory check error: {0}".format(str(e))


def check_log_directory(log_file_path):
    """Check if log directory exists and create if necessary"""
    if not log_file_path or log_file_path == 'nan':
        return True, "No log file specified"
    
    # Expand both environment variables ($HOME, $USER, etc.) and user home (~)
    expanded_path = os.path.expandvars(os.path.expanduser(log_file_path))
    log_dir = os.path.dirname(expanded_path)
    
    if not log_dir:  # Relative path in current directory
        return True, "Log file in current directory"
    
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
            return True, "Created log directory: {0}".format(log_dir)
        except OSError as e:
            # Handle race condition where directory was created by another process
            if e.errno == errno.EEXIST and os.path.isdir(log_dir):
                pass
            else:
                return False, "Cannot create log directory {0}: {1}".format(log_dir, str(e))
    
    if not os.access(log_dir, os.W_OK):
        return False, "Log directory not writable: {0}".format(log_dir)
    
    return True, "Log directory OK: {0}".format(log_dir)


def check_lock_file_directory(lock_file=None):
    """Check if lock file directory exists and is writable
    
    Args:
        lock_file: Path to lock file. If None, uses config.default_lock_file
    """
    if lock_file is None:
        lock_file = config.default_lock_file
    
    lock_dir = os.path.dirname(lock_file)
    
    # Handle case where lock file is in current directory
    if not lock_dir:
        lock_dir = '.'
    
    if not os.path.exists(lock_dir):
        print_status("Lock file directory", "ERROR", "Directory does not exist: {0}".format(lock_dir))
        return False
    
    if not os.access(lock_dir, os.W_OK):
        print_status("Lock file directory", "ERROR", "Directory not writable: {0}".format(lock_dir))
        return False
    
    print_status("Lock file directory", "OK", "Path: {0} (lock: {1})".format(lock_dir, lock_file))
    return True


def get_current_user():
    """Get current user and allow selection if multiple users exist in transfers"""
    current_user = os.environ.get('USER', os.environ.get('USERNAME', ''))
    transfers_file = config.transfers_file
    
    try:
        df = parse_transfers_file(transfers_file)
        users = df['users'].unique()
        
        # If current user is in the list, use it
        if current_user in users:
            return current_user
        
        # If only one user, use that
        if len(users) == 1:
            return users[0]
            
    except Exception:
        pass
    
    # Ask user if we can't determine
    print("\n{0}Current user: {1}{2}".format(Colors.YELLOW, current_user, Colors.END))
    print("Available users in {0}:".format(transfers_file))
    
    try:
        df = parse_transfers_file(transfers_file)
        users = df['users'].unique()
        for i, user in enumerate(users, 1):
            marker = " (current)" if user == current_user else ""
            print("  {0}. {1}{2}".format(i, user, marker))
        
        while True:
            try:
                choice = input("\nSelect user (1-{0}) or enter username: ".format(len(users))).strip()
                if choice.isdigit():
                    idx = int(choice) - 1
                    if 0 <= idx < len(users):
                        return users[idx]
                elif choice in users:
                    return choice
                elif not choice and current_user in users:
                    return current_user
                print("Invalid choice. Please try again.")
            except KeyboardInterrupt:
                print("\nOperation cancelled.")
                sys.exit(1)
    
    except Exception as e:
        print("Could not read {0}: {1}".format(transfers_file, e))
        return current_user if current_user else input("Please enter your username: ").strip()


def get_current_system():
    """Determine current system based on hostname or user input"""
    hostname = socket.gethostname().lower()
    transfers_file = config.transfers_file
    
    # Try to match hostname against systems defined in transfers.tsv
    try:
        df = parse_transfers_file(transfers_file)
        systems = df['system'].unique()
        
        # Check if hostname contains any known system name
        for system in systems:
            if system.lower() in hostname:
                return system
    except Exception:
        pass
    
    # Ask user if we can't determine
    print("\n{0}Current hostname: {1}{2}".format(Colors.YELLOW, hostname, Colors.END))
    print("Available systems in {0}:".format(transfers_file))
    
    try:
        df = parse_transfers_file(transfers_file)
        systems = df['system'].unique()
        for i, system in enumerate(systems, 1):
            print("  {0}. {1}".format(i, system))
        
        while True:
            try:
                choice = input("\nSelect your system (1-{0}) or enter system name: ".format(len(systems))).strip()
                if choice.isdigit():
                    idx = int(choice) - 1
                    if 0 <= idx < len(systems):
                        return systems[idx]
                elif choice in systems:
                    return choice
                print("Invalid choice. Please try again.")
            except KeyboardInterrupt:
                print("\nOperation cancelled.")
                sys.exit(1)
    
    except Exception as e:
        print("Could not read {0}: {1}".format(transfers_file, e))
        return input("Please enter your system name: ").strip()


def generate_cron_files():
    """Generate cron files using the installed module.

    Prefer calling landingzones.generate_cron_files.main() directly.
    Falls back to `python -m landingzones.generate_cron_files` if import fails.
    """
    # First, try to import and call the module directly
    try:
        from landingzones import generate_cron_files as gcf
        # Capture stdout so we can show a concise message to the user
        output_capture = None
        old_stdout = None
        if StringIO is not None:
            output_capture = StringIO()
            old_stdout = sys.stdout
            sys.stdout = output_capture
        try:
            gcf.main()
        finally:
            if old_stdout is not None:
                sys.stdout = old_stdout
        details = output_capture.getvalue() if output_capture is not None else "Generated cron files"
        return True, details
    except Exception as import_err:
        # Fallback to module execution via python -m
        try:
            proc = subprocess.Popen([sys.executable, '-m', 'landingzones.generate_cron_files'],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()
            rc = proc.returncode
            out = stdout.decode('utf-8') if stdout else ''
            err = stderr.decode('utf-8') if stderr else ''
            if rc == 0:
                return True, out or 'Generated cron files'
            return False, "Generation failed: {0}".format(err or out)
        except Exception as e:
            return False, "Error running generator: {0}".format(str(e))


def setup_crontab_directory():
    """Ensure ~/crontab.d directory exists"""
    crontab_dir = os.path.expandvars(os.path.expanduser("~/crontab.d"))
    try:
        os.makedirs(crontab_dir)
        return True, "Directory ready: {0}".format(crontab_dir)
    except OSError as e:
        # Handle race condition where directory already exists
        if e.errno == errno.EEXIST and os.path.isdir(crontab_dir):
            return True, "Directory ready: {0}".format(crontab_dir)
        else:
            return False, "Cannot create directory {0}: {1}".format(crontab_dir, str(e))


def deploy_cron_files(current_system, current_user=None):
    """Deploy cron files for the current system and user
    
    Args:
        current_system: The system name to deploy for
        current_user: The user to deploy for (defaults to current OS user)
    """
    if current_user is None:
        current_user = os.environ.get('USER', os.environ.get('USERNAME', ''))
    
    print_header("Automatic Cron Deployment")
    print_status("Deploying for", "INFO", "{0}@{1}".format(current_user, current_system))

    # Step 1: Ensure local crontab.d exists before generation
    dir_ok, dir_msg = setup_crontab_directory()
    print_status("Crontab directory setup", "OK" if dir_ok else "ERROR", dir_msg)
    if not dir_ok:
        return False

    # Step 2: Generate cron files via installed module
    print_status("Generating cron files", "INFO", "Using landingzones.generate_cron_files")
    gen_ok, gen_msg = generate_cron_files()
    if not gen_ok:
        print_status("Cron file generation", "WARN", gen_msg)
        print_status("Continuing deployment", "INFO", "Will use existing files in crontab.d/")
    else:
        print_status("Cron file generation", "OK", gen_msg)
    
    # Step 3: Find and copy relevant cron files
    cron_files = []
    try:
        # Look for cron files that match the current system and user in crontab.d/
        # File format: system.user.Landing_Zone.cron
        pattern = "crontab.d/{0}.{1}.Landing_Zone.cron".format(current_system, current_user)
        matches = glob.glob(pattern)
        
        if not matches:
            msg = (
                "No new files for '{0}@{1}'. Using existing crontab.d/"
            ).format(current_user, current_system)
            print_status("Cron file discovery", "WARN", msg)
            return True  # Not an error, just no files to deploy

        cron_files = matches
        found_msg = "Found {0} files".format(len(cron_files))
        print_status("Cron file discovery", "OK", found_msg)
    except Exception as e:
        print_status("Cron file discovery", "ERROR", "Search failed: {0}".format(str(e)))
        return False
    
    # Step 4: Copy files to ~/crontab.d/
    crontab_dir = os.path.expandvars(os.path.expanduser("~/crontab.d"))
    copied_files = []
    
    for cron_file in cron_files:
        try:
            # Extract just the filename from the path (remove crontab.d/ prefix)
            filename = os.path.basename(cron_file)
            dest_path = os.path.join(crontab_dir, filename)
            shutil.copy2(cron_file, dest_path)
            copied_files.append(filename)
            print_status("Copy {0}".format(filename), "OK", "Copied to {0}".format(dest_path))
        except Exception as e:
            print_status("Copy {0}".format(cron_file), "ERROR", "Failed: {0}".format(str(e)))
            return False
    
    if not copied_files:
        return True  # Nothing to deploy
    
    # Step 5: Activate cron jobs
    try:
        # Run: cat ~/crontab.d/*.cron | crontab -
        crontab_pattern = os.path.join(crontab_dir, "*.cron")
        cmd = "cat {0} | crontab -".format(crontab_pattern)
        
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        result_returncode = proc.returncode
        result_stderr = stderr.decode('utf-8')

        if result_returncode == 0:
            print_status("Crontab activation", "OK",
                         "Activated {0} cron files".format(len(copied_files)))

            # Show current crontab for verification
            verify_proc = subprocess.Popen(['crontab', '-l'],
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            verify_stdout, verify_stderr = verify_proc.communicate()
            verify_returncode = verify_proc.returncode
            verify_result_stdout = verify_stdout.decode('utf-8')
            if verify_returncode == 0:
                lines = verify_result_stdout.split('\n')
                active_jobs = len([line for line in lines
                                   if (line.strip() and
                                       not line.startswith('#'))])
                print_status("Crontab verification", "OK",
                             "Total active cron jobs: {0}".format(active_jobs))

            return True
        else:
            print_status("Crontab activation", "ERROR",
                         "Failed: {0}".format(result_stderr.strip()))
            return False
    
    except Exception as e:
        print_status("Crontab activation", "ERROR", "Error: {0}".format(str(e)))
        return False


def ask_user_permission():
    """Ask user if they want to proceed with automatic deployment"""
    prompt = ("\n{0}Do you want to automatically deploy "
              "the cron files now? (y/N): {1}").format(Colors.YELLOW, Colors.END)
    print(prompt, end="")
    try:
        response = input().strip().lower()
        return response in ['y', 'yes']
    except KeyboardInterrupt:
        print("\n{0}Operation cancelled.{1}".format(Colors.YELLOW, Colors.END))
        return False


def main():
    """Main verification function"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Check deployment readiness for Landing Zone cron jobs'
    )
    parser.add_argument(
        '--config', '-c',
        default=None,
        help='Path to config.yaml file (default: auto-detect in current directory)'
    )
    parser.add_argument(
        '--transfers', '-t',
        default=None,
        help='Path to transfers.tsv file (overrides config file)'
    )
    parser.add_argument(
        '--run-tests',
        action='store_true',
        help='Generate and execute local script tests from prod toy data without deploying cron'
    )
    parser.add_argument(
        '--keep-test-env',
        action='store_true',
        help='Keep the timestamped local script-test workspace instead of removing it'
    )
    args = parser.parse_args()
    
    # Load configuration from file and/or command line arguments
    config.load_config(
        config_file=args.config,
        transfers_file=args.transfers
    )

    if args.run_tests:
        return run_local_script_tests(
            config_file=args.config,
            transfers_file=args.transfers,
            keep_test_env=args.keep_test_env,
        )
    
    print("{0}{1}".format(Colors.BOLD, Colors.BLUE))
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║            Landing Zone Pre-Deployment Check                 ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print("{0}".format(Colors.END))
    
    transfers_file = config.transfers_file
    
    if not os.path.exists(transfers_file):
        print_status("Configuration file", "ERROR", 
                    "{0} not found".format(transfers_file))
        return False
    
    # Check required tools
    tools_ok = check_required_tools()
    
    # Load and filter transfers
    try:
        df = parse_transfers_file(transfers_file)
        
        print_status("Configuration file", "OK", "Loaded {0} active transfers".format(len(df)))
    except Exception as e:
        print_status("Configuration file", "ERROR", "Cannot read transfers.tsv: {0}".format(e))
        return False
    
    # Determine current system and user
    current_system = get_current_system()
    current_user = get_current_user()
    print("\n{0}Checking transfers for system: {1}, user: {2}{3}".format(
        Colors.BOLD, current_system, current_user, Colors.END))
    
    # Filter transfers for current system and user
    system_transfers = df[(df['system'] == current_system) & (df['users'] == current_user)]
    if len(system_transfers) == 0:
        # Check if there are transfers for the system but different user
        system_only = df[df['system'] == current_system]
        if len(system_only) > 0:
            other_users = system_only['users'].unique()
            print_status("User transfers", "WARN", 
                        "No transfers for user '{0}' on system '{1}'. Available users: {2}".format(
                            current_user, current_system, ', '.join(other_users)))
        else:
            print_status("System transfers", "WARN", 
                        "No transfers found for system '{0}'".format(current_system))
        return True
    
    print_status("User transfers", "OK", 
                "Found {0} transfers for {1}@{2}".format(
                    len(system_transfers), current_user, current_system))
    
    # Check system prerequisites
    print_header("System Prerequisites")
    flock_ok = check_flock_command(current_system)
    
    # Check each transfer
    all_transfers_ok = True
    
    for _, transfer in system_transfers.iterrows():
        print_header("Transfer: {0} → {1}".format(transfer['source'], transfer['destination']))
        
        transfer_ok = True
        
        # Check source directory
        source_user, source_host, source_path = parse_remote_destination(
            transfer['source']
        )
        source_port = transfer.get('source_port', '')
        source_port = str(source_port) if source_port is not None else ''
        source_port = (
            source_port if source_port and source_port != 'nan' and source_port.strip()
            else None
        )

        if source_host:
            ssh_ok, ssh_msg = check_ssh_connection(source_user, source_host, source_port)
            print_status(
                "SSH connection to source {0}".format(source_host),
                "OK" if ssh_ok else "ERROR",
                ssh_msg,
            )
            if ssh_ok:
                dir_ok, dir_msg = check_remote_directory(
                    source_user,
                    source_host,
                    source_path,
                    source_port,
                    "Source directory",
                    check_writable=False,
                )
                print_status("Source directory", "OK" if dir_ok else "ERROR", dir_msg)
                if not dir_ok:
                    transfer_ok = False
            else:
                transfer_ok = False
        else:
            if not check_local_directory(transfer['source'], "Source directory"):
                transfer_ok = False
        
        # Check destination
        user, host, dest_path = parse_remote_destination(transfer['destination'])
        port = transfer.get('destination_port', '')
        
        # Convert port to string and handle NaN values safely
        port = str(port) if port is not None else ''
        port = port if port and port != 'nan' and port.strip() else None
        
        if host:
            # Remote destination
            remote_label = build_ssh_target(user, host)
            print("\n  Remote destination: {0}:{1}".format(remote_label, dest_path))
            if port:
                print("  Using port: {0}".format(port))
            
            # Check SSH connection
            ssh_ok, ssh_msg = check_ssh_connection(user, host, port)
            print_status("SSH connection to {0}".format(host), "OK" if ssh_ok else "ERROR", ssh_msg)
            
            if ssh_ok:
                # Check remote directory
                dir_ok, dir_msg = check_remote_directory(user, host, dest_path, port, "Remote destination")
                print_status("Remote destination directory", "OK" if dir_ok else "ERROR", dir_msg)
                if not dir_ok:
                    transfer_ok = False
            else:
                transfer_ok = False
        else:
            # Local destination
            if not check_local_directory(dest_path, "Destination directory"):
                transfer_ok = False
        
        # Check log file directory
        log_file = transfer.get('log_file', '')
        if log_file and log_file != 'nan':
            log_ok, log_msg = check_log_directory(log_file)
            print_status("Log directory", "OK" if log_ok else "ERROR", log_msg)
            if not log_ok:
                transfer_ok = False
        
        # Check flock file directory
        flock_file = transfer.get('flock_file', '')
        if flock_file and flock_file != 'nan':
            flock_ok, flock_msg = check_log_directory(flock_file)
            print_status("Flock directory", "OK" if flock_ok else "ERROR",
                         flock_msg)
            if not flock_ok:
                transfer_ok = False
        
        if not transfer_ok:
            all_transfers_ok = False
        
        print()  # Blank line between transfers
    
    # Final summary
    print_header("Deployment Readiness Summary")
    
    overall_ok = tools_ok and flock_ok and all_transfers_ok
    
    if overall_ok:
        print_status("System ready for cron deployment", "OK", 
                    "All checks passed! You can safely deploy the cron files.")
        print("\n{0}{1}✓ READY TO DEPLOY{2}".format(Colors.GREEN, Colors.BOLD, Colors.END))
        
        # Ask user if they want automatic deployment
        if ask_user_permission():
            print_header("Automatic Deployment")
            deploy_ok = deploy_cron_files(current_system, current_user)
            
            if deploy_ok:
                success_msg = ("\n{0}🚀 Deployment completed "
                               "successfully!{1}").format(Colors.GREEN, Colors.END)
                print(success_msg)
                print("Your cron jobs are now active and will run "
                      "according to schedule.")
                print("Use 'crontab -l' to view active jobs.")
            else:
                print("\n{0}❌ Deployment failed.{1}".format(Colors.RED, Colors.END))
                print("Please check the errors above and deploy manually.")
        else:
            print("\nManual deployment steps:")
            print("1. Run: python generate_cron_files.py")
            print("2. Copy relevant .cron files to ~/crontab.d/")
            print("3. Activate: cat ~/crontab.d/*.cron | crontab -")
    else:
        print_status("System deployment readiness", "ERROR",
                     "Please fix the issues above before deploying "
                     "cron files.")
        fail_msg = ("\n{0}{1}✗ NOT READY FOR "
                    "DEPLOYMENT{2}").format(Colors.RED, Colors.BOLD, Colors.END)
        print(fail_msg)
        print("\nPlease address the errors listed above and "
              "run this check again.")
    
    return overall_ok


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n{0}Operation cancelled by user.{1}".format(Colors.YELLOW, Colors.END))
        sys.exit(1)
    except Exception as e:
        print("\n{0}Unexpected error: {1}{2}".format(Colors.RED, e, Colors.END))
        sys.exit(1)
