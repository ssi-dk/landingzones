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


def list_visible_entries(path):
    """List non-hidden entries under a directory."""
    if not os.path.isdir(path):
        return []
    return sorted(
        name for name in os.listdir(path)
        if not name.startswith('.')
    )


def list_visible_directories(path):
    """List non-hidden directories under a directory."""
    return [
        name for name in list_visible_entries(path)
        if os.path.isdir(os.path.join(path, name))
    ]


def endpoint_key(value):
    """Return a normalized endpoint key for comparing transfer roots."""
    remote, path = split_remote_path(value)
    root = normalize_source_path(path if remote else value)
    return remote or '', root


def absolutize_local_endpoint(value, base_dir):
    """Resolve a local endpoint against the command working directory."""
    text = str(value).strip()
    remote, _ = split_remote_path(text)
    if remote or not text or os.path.isabs(text) or text.startswith('$') or text.startswith('~'):
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


def parse_test_fixture_names(value):
    """Parse an optional comma-separated fixture-name list."""
    text = str(value).strip() if value is not None else ''
    if not text or text == 'nan':
        return []
    return [
        item.strip()
        for item in text.split(',')
        if item.strip()
    ]


def build_run_test_plan(transfers_df):
    """Build source/destination relationships for the real-transfer smoke test."""
    source_keys = {
        endpoint_key(transfer['source']) for _, transfer in transfers_df.iterrows()
    }
    destination_keys = {
        endpoint_key(transfer['destination']) for _, transfer in transfers_df.iterrows()
    }

    initial_source_map = {}
    terminal_destinations = []
    for _, transfer in transfers_df.iterrows():
        source_info = {
            'value': transfer['source'],
            'port': str(transfer.get('source_port', '') or '').strip(),
            'test_fixture_names': parse_test_fixture_names(
                transfer.get('test_fixture_names', '')
            ),
        }
        destination_info = {
            'value': transfer['destination'],
            'port': str(transfer.get('destination_port', '') or '').strip(),
        }
        if endpoint_key(transfer['source']) not in destination_keys:
            source_key = endpoint_key(transfer['source'])
            existing = initial_source_map.get(source_key)
            if existing is None:
                initial_source_map[source_key] = source_info
            else:
                merged_fixtures = []
                seen_fixtures = set()
                for fixture_name in (
                    existing.get('test_fixture_names', []) +
                    source_info.get('test_fixture_names', [])
                ):
                    if fixture_name in seen_fixtures:
                        continue
                    seen_fixtures.add(fixture_name)
                    merged_fixtures.append(fixture_name)
                existing['test_fixture_names'] = merged_fixtures
        if endpoint_key(transfer['destination']) not in source_keys:
            terminal_destinations.append(destination_info)

    return {
        'initial_sources': list(initial_source_map.values()),
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


def get_test_data_toy_data_candidates(endpoint_root, test_tree_root):
    """Return candidate toy-data subdirectories for an initial source root."""
    root = os.path.abspath(endpoint_root)
    deployment_tests_root = os.path.join(
        os.path.abspath(test_tree_root), 'tests'
    )
    candidates = []

    if root.startswith(deployment_tests_root + os.sep):
        relative_parts = os.path.relpath(root, deployment_tests_root).split(os.sep)
        if len(relative_parts) >= 2 and relative_parts[0] == 'test_local':
            candidates.append(relative_parts[1])
        elif relative_parts:
            candidates.append(relative_parts[0])

    basename = os.path.basename(root.rstrip(os.sep))
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


def resolve_test_data_dir(endpoint, toy_data_root, test_tree_root):
    """Resolve which toy-data directory seeds a given initial source root."""
    if not os.path.isdir(toy_data_root):
        raise ValueError(
            "Toy data directory not found: {0}".format(toy_data_root)
        )

    endpoint_root = get_endpoint_root(endpoint)
    checked_paths = []
    for candidate in get_test_data_toy_data_candidates(endpoint_root, test_tree_root):
        toy_dir = os.path.join(toy_data_root, candidate)
        checked_paths.append(toy_dir)
        if os.path.isdir(toy_dir):
            return toy_dir

    available = list_visible_directories(toy_data_root)
    if len(available) == 1:
        return os.path.join(toy_data_root, available[0])

    raise ValueError(
        "No toy data found for source root '{0}'. Checked: {1}".format(
            endpoint_root,
            ', '.join(checked_paths) if checked_paths else toy_data_root,
        )
    )


def remove_local_path(path):
    """Remove a local file or directory if it exists."""
    if os.path.islink(path) or os.path.isfile(path):
        os.remove(path)
        return
    if os.path.isdir(path):
        shutil.rmtree(path)


def remove_endpoint_entries(endpoint, entry_names):
    """Remove specific entry names from an endpoint root."""
    root = get_endpoint_root(endpoint)
    user, host, _ = parse_remote_destination(endpoint['value'])
    port = endpoint.get('port', '')

    if host:
        if not entry_names:
            return
        command = "rm -rf {0}".format(
            ' '.join(
                shell_path(os.path.join(root, entry_name))
                for entry_name in entry_names
            )
        )
        rc, _, stderr = run_remote_shell(user, host, command, port)
        if rc != 0:
            raise ValueError(
                "Cannot clean test entries on {0}: {1}".format(
                    shell_target(user, host), stderr.strip()
                )
            )
        return

    os.makedirs(root, exist_ok=True)
    for entry_name in entry_names:
        remove_local_path(os.path.join(root, entry_name))


def build_test_with_data_seed_plan(test_plan, toy_data_root, test_tree_root):
    """Resolve toy-data sources and expected top-level directories for testing with data."""
    seed_plan = []
    for endpoint in test_plan['initial_sources']:
        user, host, _ = parse_remote_destination(endpoint['value'])
        if host:
            raise ValueError(
                "test-with-data toy-data seeding does not support remote initial source: {0}".format(
                    endpoint['value']
                )
            )
        toy_data_dir = resolve_test_data_dir(
            endpoint, toy_data_root, test_tree_root
        )
        fixture_names = endpoint.get('test_fixture_names', [])
        if fixture_names:
            entry_names = []
            missing_fixtures = []
            available_dirs = set(list_visible_directories(toy_data_dir))
            for fixture_name in fixture_names:
                if fixture_name in available_dirs:
                    entry_names.append(fixture_name)
                else:
                    missing_fixtures.append(fixture_name)
            if missing_fixtures:
                raise ValueError(
                    "Toy data directory missing configured fixture(s) for source root '{0}': {1}".format(
                        get_endpoint_root(endpoint), ', '.join(missing_fixtures)
                    )
                )
        else:
            entry_names = list_visible_directories(toy_data_dir)
        if not entry_names:
            raise ValueError(
                "Toy data directory contains no visible directories: {0}".format(
                    toy_data_dir
                )
            )
        seed_plan.append({
            'endpoint': endpoint,
            'toy_data_dir': toy_data_dir,
            'entry_names': entry_names,
        })
    return seed_plan


def cleanup_test_with_data_entries(test_plan, entry_names):
    """Remove seeded test directories from all test-with-data roots."""
    cleanup_errors = []
    all_entries = sorted(set(entry_names) | {'.staging'})
    for endpoint in test_plan['all_destinations'] + test_plan['all_sources']:
        try:
            remove_endpoint_entries(endpoint, all_entries)
        except ValueError as exc:
            cleanup_errors.append(str(exc))
    if cleanup_errors:
        raise ValueError('\n'.join(cleanup_errors))


def cleanup_test_with_data_runtime_artifacts(transfers_df):
    """Remove old lock and log artifacts for the test-with-data transfer subset."""
    artifact_paths = set()
    for _, transfer in transfers_df.iterrows():
        log_file = str(transfer.get('log_file', '') or '').strip()
        flock_file = str(transfer.get('flock_file', '') or '').strip()
        if log_file and log_file != 'nan':
            artifact_paths.update([
                log_file,
                "{0}.latest".format(log_file),
                "{0}.mini".format(log_file),
            ])
        if flock_file and flock_file != 'nan':
            artifact_paths.add(flock_file)

    for path in artifact_paths:
        if os.path.exists(path) or os.path.islink(path):
            remove_local_path(path)


def seed_test_data_sources(seed_plan):
    """Copy toy-data directories into the initial source roots."""
    seeded_count = 0
    for seed in seed_plan:
        root = get_endpoint_root(seed['endpoint'])
        os.makedirs(root, exist_ok=True)
        for entry_name in seed['entry_names']:
            shutil.copytree(
                os.path.join(seed['toy_data_dir'], entry_name),
                os.path.join(root, entry_name),
            )
            seeded_count += 1
    return seeded_count


def build_test_with_data_expectations(transfers_df, seed_plan):
    """Predict which seeded directories should arrive at terminal destinations."""
    root_contents = {}
    for seed in seed_plan:
        root_contents[endpoint_key(seed['endpoint']['value'])] = set(seed['entry_names'])

    for _, transfer in transfers_df.iterrows():
        source_key = endpoint_key(transfer['source'])
        destination_key = endpoint_key(transfer['destination'])
        moved_entries = set(root_contents.get(source_key, set()))
        if not moved_entries:
            continue
        root_contents.setdefault(destination_key, set()).update(moved_entries)
        root_contents[source_key] = set()

    return root_contents


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


def load_test_with_data_transfers(
    transfers_file, current_system, current_user, base_dir
):
    """Load the current system/user transfer subset for test-with-data."""
    df = parse_transfers_file(transfers_file)
    df = df[(df['system'] == current_system) & (df['users'] == current_user)].copy()
    if df.empty:
        raise ValueError(
            "No transfers found for user '{0}' on system '{1}'".format(
                current_user, current_system
            )
        )
    for column in ('source', 'destination'):
        df[column] = df[column].apply(
            lambda value: absolutize_local_endpoint(value, base_dir)
        )
    return df


def generate_test_scripts(transfers_df, scripts_dir, crontab_dir, validation_scripts_dir):
    """Generate shell scripts and cron files for the current transfer subset."""
    from landingzones import generate_cron_files as gcf

    os.makedirs(crontab_dir, exist_ok=True)
    os.makedirs(scripts_dir, exist_ok=True)
    os.makedirs(validation_scripts_dir, exist_ok=True)
    gcf.remove_stale_generated_scripts(
        scripts_dir, transfers_df['script_name'].dropna().tolist()
    )
    gcf.remove_stale_validation_scripts(validation_scripts_dir, transfers_df)
    gcf.write_validation_scripts(validation_scripts_dir, transfers_df)

    grouped = transfers_df.groupby('system_user')
    for system_user, group_df in grouped:
        for _, transfer in group_df.iterrows():
            script_path = os.path.join(scripts_dir, transfer['script_name'])
            script_content = gcf.generate_script_content(transfer)
            with open(script_path, 'w') as handle:
                handle.write(script_content)
            os.chmod(script_path, 0o755)

        cron_path = os.path.join(
            crontab_dir, "{0}.Landing_Zone.cron".format(system_user)
        )
        with open(cron_path, 'w') as handle:
            handle.write(gcf.generate_cron_file(system_user, group_df, scripts_dir))

    return transfers_df


def read_log_excerpt(log_file, max_lines=10):
    """Return a short trailing excerpt from a log file when it exists."""
    if not log_file or log_file == 'nan' or not os.path.exists(log_file):
        return ''

    with open(log_file, 'r') as handle:
        lines = handle.read().splitlines()

    if not lines:
        return ''
    return '\n'.join(lines[-max_lines:])


def format_script_result_summary(result):
    """Format a compact execution summary for a generated test script."""
    details = [
        "Script: {0}".format(result['script_path']),
        "Exit code: {0}".format(result['returncode']),
    ]

    stdout = result['stdout'].strip()
    stderr = result['stderr'].strip()
    log_excerpt = read_log_excerpt(result.get('log_file', ''))

    if stdout:
        details.append("stdout:\n{0}".format(stdout))
    if stderr:
        details.append("stderr:\n{0}".format(stderr))
    if log_excerpt:
        details.append("log tail:\n{0}".format(log_excerpt))

    return '\n'.join(details)


def prompt_to_continue(prompt_text):
    """Pause until the operator confirms the next slow-mode step should run."""
    prompt = "\n{0}{1}{2}".format(Colors.YELLOW, prompt_text, Colors.END)
    print(prompt, end="")
    try:
        input()
    except EOFError:
        return
    except KeyboardInterrupt:
        print("\n{0}Slow integration run cancelled.{1}".format(
            Colors.YELLOW, Colors.END
        ))
        raise SystemExit(130)


def print_slow_step_summary(result, step_number, total_steps):
    """Print a human-readable summary of the most recent integration step."""
    print_header(
        "Integration Step {0}/{1}: {2}".format(
            step_number, total_steps, result['identifier']
        )
    )
    print_status(
        "Last step",
        "OK" if result['returncode'] == 0 else "ERROR",
        format_script_result_summary(result),
    )


def run_generated_scripts(transfers_df, scripts_dir, runtime_root=None, slow=False):
    """Execute generated scripts in transfer order."""
    env = os.environ.copy()
    env['LZ_DEBUG_CLI'] = '1'
    if runtime_root:
        env['LZ_TEST_ROOT'] = runtime_root
        env.setdefault('TMPDIR', runtime_root)
    else:
        env.pop('LZ_TEST_ROOT', None)
    results = []

    total_steps = len(transfers_df)
    for step_number, (_, transfer) in enumerate(transfers_df.iterrows(), start=1):
        script_path = os.path.join(scripts_dir, transfer['script_name'])
        proc = subprocess.Popen(
            ['/bin/sh', script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=runtime_root or os.getcwd(),
        )
        stdout, stderr = proc.communicate()
        result = {
            'identifier': transfer['identifiers'],
            'script_path': script_path,
            'log_file': transfer.get('log_file', ''),
            'returncode': proc.returncode,
            'stdout': stdout.decode('utf-8'),
            'stderr': stderr.decode('utf-8'),
        }
        results.append(result)

        if slow:
            print_slow_step_summary(result, step_number, total_steps)
            if step_number < total_steps:
                next_identifier = transfers_df.iloc[step_number]['identifiers']
                prompt_to_continue(
                    "Press Enter to continue to the next step ({0})... ".format(
                        next_identifier
                    )
                )
    return results


def endpoint_directory_exists(endpoint, directory_name):
    """Check whether a named directory exists directly under an endpoint root."""
    root = get_endpoint_root(endpoint)
    directory_path = os.path.join(root, directory_name)
    user, host, _ = parse_remote_destination(endpoint['value'])
    port = endpoint.get('port', '')

    if host:
        command = '[ -d {0} ] && echo EXISTS || echo MISSING'.format(
            shell_path(directory_path)
        )
        rc, stdout, stderr = run_remote_shell(user, host, command, port)
        if rc != 0:
            return False, "Remote check failed: {0}".format(stderr.strip())
        return 'EXISTS' in stdout, directory_path

    return os.path.isdir(directory_path), directory_path


def validate_script_test_results(test_plan, expected_contents):
    """Validate that seeded toy-data directories reached each terminal destination."""
    errors = []
    for endpoint in test_plan['terminal_destinations']:
        expected_entries = sorted(
            expected_contents.get(endpoint_key(endpoint['value']), set())
        )
        for entry_name in expected_entries:
            exists, path = endpoint_directory_exists(endpoint, entry_name)
            if not exists:
                errors.append(
                    "Expected test directory missing at terminal destination: {0}".format(
                        path
                    )
                )
    return errors


def cleanup_test_with_data_outputs(test_plan, transfers_df, seed_plan):
    """Remove seeded test directories and runtime artifacts after a test-with-data run."""
    entry_names = sorted({
        entry_name
        for seed in seed_plan
        for entry_name in seed['entry_names']
    })
    cleanup_test_with_data_entries(test_plan, entry_names)
    cleanup_test_with_data_runtime_artifacts(transfers_df)


def ask_yes_no(prompt_text):
    """Ask a yes/no question and return True for yes."""
    prompt = "\n{0}{1} (y/N): {2}".format(
        Colors.YELLOW, prompt_text, Colors.END
    )
    print(prompt, end="")
    try:
        response = input().strip().lower()
        return response in ['y', 'yes']
    except KeyboardInterrupt:
        print("\n{0}Operation cancelled.{1}".format(Colors.YELLOW, Colors.END))
        return False


def run_test_with_data(config_file=None, transfers_file=None, slow=False):
    """Run generated scripts against seeded toy-data in the real test tree."""
    print_header("Transfer Test With Data")
    snapshot = _snapshot_config_state()
    current_system = None
    current_user = None
    transfers_df = None
    test_plan = None
    seed_plan = None

    try:
        work_root = os.getcwd()
        config.load_config(
            config_file=config_file,
            transfers_file=transfers_file,
        )
        transfers_path = transfers_file or config.transfers_file
        current_system = get_current_system()
        current_user = get_current_user()
        test_tree_root = os.path.dirname(
            os.path.abspath(
                transfers_path if os.path.isabs(transfers_path)
                else os.path.join(work_root, transfers_path)
            )
        )
        if os.path.basename(test_tree_root) == 'input':
            test_tree_root = os.path.dirname(test_tree_root)
        toy_data_root = config.test_data
        if not os.path.isabs(toy_data_root):
            toy_data_root = os.path.abspath(os.path.join(work_root, toy_data_root))
        transfers_df = load_test_with_data_transfers(
            transfers_path, current_system, current_user, work_root
        )
        test_plan = build_run_test_plan(transfers_df)
        seed_plan = build_test_with_data_seed_plan(
            test_plan, toy_data_root, test_tree_root
        )
        expected_entry_names = sorted({
            entry_name
            for seed in seed_plan
            for entry_name in seed['entry_names']
        })
        cleanup_test_with_data_entries(test_plan, expected_entry_names)
        cleanup_test_with_data_runtime_artifacts(transfers_df)
        seeded_count = seed_test_data_sources(seed_plan)
        print_status(
            "Seeded toy data",
            "OK",
            "Copied {0} directory tree(s) into {1} starting location(s)".format(
                seeded_count, len(seed_plan)
            ),
        )
        if get_test_flock_path() == '/usr/bin/true':
            print_status(
                "Flock fallback",
                "WARN",
                "No flock binary found on PATH; test-with-data is running without lock enforcement",
            )
        scripts_dir = config.get_rit_managed_path(current_system, 'sh_output')
        crontab_dir = config.get_rit_managed_path(current_system, 'crontabs')
        validation_scripts_dir = config.validation_scripts_dir
        generate_test_scripts(
            transfers_df, scripts_dir, crontab_dir, validation_scripts_dir
        )

        print_status(
            "Generated test scripts",
            "OK",
            "Prepared {0} scripts in {1}".format(len(transfers_df), scripts_dir),
        )

        expected_contents = build_test_with_data_expectations(
            transfers_df, seed_plan
        )
        run_results = run_generated_scripts(
            transfers_df, scripts_dir, slow=slow
        )
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

        validation_errors = validate_script_test_results(
            test_plan, expected_contents
        )
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
            "Seeded toy-data directories reached all terminal destinations",
        )
        if ask_yes_no(
            "Do you want to clean up the propagated output locations so test-with-data can be rerun from the initial state?"
        ):
            cleanup_test_with_data_outputs(test_plan, transfers_df, seed_plan)
            print_status(
                "Test-with-data cleanup",
                "OK",
                "Removed propagated test data and runtime artifacts from the test tree",
            )
        else:
            print_status(
                "Test-with-data final state",
                "INFO",
                "Seeded data and generated artifacts were left in the real test locations",
            )
        return True
    finally:
        _restore_config_state(snapshot)


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


def main(argv=None):
    """Main verification function."""
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
        '--test-with-data',
        action='store_true',
        help='Seed toy data from config.test_data into the real test tree and execute the generated transfer scripts'
    )
    parser.add_argument(
        '--slow',
        action='store_true',
        help='With --test-with-data, print each completed step and wait for Enter before continuing'
    )
    parser.add_argument(
        '--validation-scripts-dir',
        default=None,
        help='Directory containing generated validation wrappers (overrides config)'
    )
    args = parser.parse_args(argv)
    
    # Load configuration from file and/or command line arguments
    config.load_config(
        config_file=args.config,
        transfers_file=args.transfers,
        validation_scripts_dir=args.validation_scripts_dir,
    )

    if args.test_with_data:
        return run_test_with_data(
            config_file=args.config,
            transfers_file=args.transfers,
            slow=args.slow,
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
        if ask_yes_no("Do you want to automatically deploy the cron files now?"):
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
