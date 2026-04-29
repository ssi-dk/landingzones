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
import shutil
import argparse

from landingzones.config import config
from landingzones.generate_cron_files import (
    parse_transfers_file,
    normalize_source_path,
    sanitize_identifier,
    split_remote_path,
    shell_path,
)
from landingzones.readiness_ops import (
    Colors,
    build_ssh_target,
    check_flock_command,
    check_local_directory,
    check_log_directory,
    check_remote_directory,
    check_required_tools,
    check_ssh_connection,
    deploy_cron_files,
    generate_cron_files,
    get_current_system,
    get_current_user,
    inspect_local_directory,
    inspect_remote_directory,
    normalize_directory_path,
    parse_remote_destination,
    print_header,
    print_status,
    setup_crontab_directory,
)
from landingzones.transfer_loading import (
    filter_transfers_by_system_user,
    load_runtime_transfers,
    normalize_runtime_id_args,
    resolve_runtime_ids,
)


def get_test_flock_path():
    """Prefer a real flock binary, but allow a no-op lock command for local tests."""
    flock_path = shutil.which('flock')
    if flock_path:
        return flock_path
    return '/usr/bin/true'


def _snapshot_config_state():
    """Capture config state so temporary overrides can be restored."""
    return config.snapshot_state()


def _restore_config_state(snapshot):
    """Restore config state after temporary overrides."""
    config.restore_state(snapshot)


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
    entry_point_flags = {
        str(transfer.get('is_entry_point', '') or '').strip().upper()
        for _, transfer in transfers_df.iterrows()
    }
    prefer_explicit_entry_points = 'TRUE' in entry_point_flags

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
        is_entry_point = (
            str(transfer.get('is_entry_point', '') or '').strip().upper() == 'TRUE'
        )
        is_initial_source = False
        if prefer_explicit_entry_points:
            is_initial_source = is_entry_point
        else:
            is_initial_source = endpoint_key(transfer['source']) not in destination_keys

        if is_initial_source:
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

    fixture_names = endpoint.get('test_fixture_names', [])
    if fixture_names:
        available = set(list_visible_directories(toy_data_root))
        if all(fixture_name in available for fixture_name in fixture_names):
            return toy_data_root

    available = list_visible_directories(toy_data_root)
    if len(available) == 1:
        # Allow fixture bundles to be wrapped in one or more single-directory
        # containers such as test_data/data/lab_machine_1/<runs>.
        current = os.path.join(toy_data_root, available[0])
        while True:
            nested = list_visible_directories(current)
            if len(nested) != 1:
                return current
            child = os.path.join(current, nested[0])
            if not list_visible_directories(child):
                return current
            current = child

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


def list_endpoint_entries(endpoint):
    """List top-level entries under an endpoint root, including hidden names."""
    root = get_endpoint_root(endpoint)
    user, host, _ = parse_remote_destination(endpoint['value'])
    port = endpoint.get('port', '')

    if host:
        command = (
            "if [ -d {0} ]; then find {0} -mindepth 1 -maxdepth 1 "
            "-exec basename {{}} \\; | sort; fi"
        ).format(shell_path(root))
        rc, stdout, stderr = run_remote_shell(user, host, command, port)
        if rc != 0:
            raise ValueError(
                "Cannot inspect test entries on {0}: {1}".format(
                    shell_target(user, host), stderr.strip() or 'unknown error'
                )
            )
        return [
            line.strip()
            for line in stdout.splitlines()
            if line.strip() not in ('.', '..')
        ]

    if not os.path.isdir(root):
        return []
    return sorted(
        name for name in os.listdir(root)
        if name not in ('.', '..')
    )


def build_test_with_data_existing_state(test_plan, expected_entry_names):
    """Classify existing endpoint contents before seeding toy data."""
    source_keys = {
        endpoint_key(endpoint['value']) for endpoint in test_plan['all_sources']
    }
    endpoint_state = []

    for endpoint in dedupe_test_endpoints(
        test_plan['all_sources'] + test_plan['all_destinations']
    ):
        entries = list_endpoint_entries(endpoint)
        if not entries:
            continue

        endpoint_is_source = endpoint_key(endpoint['value']) in source_keys
        blockers = []
        extras = []
        for entry_name in entries:
            is_hidden = entry_name.startswith('.')
            is_expected = entry_name in expected_entry_names
            is_blocker = False
            if entry_name == '.staging':
                is_blocker = True
            elif endpoint_is_source and not is_hidden:
                is_blocker = True
            elif is_expected:
                is_blocker = True

            if is_blocker:
                blockers.append(entry_name)
            else:
                extras.append(entry_name)

        endpoint_state.append({
            'endpoint': endpoint,
            'display': normalize_endpoint_display(endpoint['value']),
            'is_source': endpoint_is_source,
            'entries': entries,
            'blockers': blockers,
            'extras': extras,
        })

    return endpoint_state


def summarize_test_with_data_existing_state(existing_state):
    """Render a concise summary of pre-existing test data state."""
    lines = []
    for item in existing_state:
        lines.append(item['display'])
        if item['blockers']:
            lines.append(
                "blockers: {0}".format(', '.join(item['blockers']))
            )
        if item['extras']:
            lines.append(
                "extras: {0}".format(', '.join(item['extras']))
            )
    return '\n'.join(lines)


def ask_test_with_data_existing_state_action(existing_state):
    """Ask how pre-existing integration entries should be handled."""
    if not existing_state:
        return 'blockers'

    print_header("Existing Test Data State")
    print_status(
        "Pre-existing entries",
        "WARN",
        summarize_test_with_data_existing_state(existing_state),
    )
    prompt = (
        "\n{0}Choose cleanup scope before seeding "
        "[a]ll/[b]lockers/[l]eave as-is (default: b): {1}"
    ).format(Colors.YELLOW, Colors.END)
    print(prompt, end="")
    try:
        response = input().strip().lower()
    except KeyboardInterrupt:
        print("\n{0}Operation cancelled.{1}".format(Colors.YELLOW, Colors.END))
        return 'leave'

    if response in ('', 'b', 'blockers'):
        return 'blockers'
    if response in ('a', 'all'):
        return 'all'
    if response in ('l', 'leave', 'leave as-is', 'leave as is'):
        return 'leave'
    print_status(
        "Pre-existing state choice",
        "WARN",
        "Unrecognized response '{0}', defaulting to blocker cleanup".format(
            response
        ),
    )
    return 'blockers'


def build_test_with_data_cleanup_map(existing_state, mode):
    """Build per-endpoint cleanup entries for the requested preflight mode."""
    cleanup_map = []
    for item in existing_state:
        if mode == 'all':
            entry_names = item['entries']
        elif mode == 'blockers':
            entry_names = item['blockers']
        else:
            entry_names = []
        if not entry_names:
            continue
        cleanup_map.append({
            'endpoint': item['endpoint'],
            'entry_names': sorted(set(entry_names)),
        })
    return cleanup_map


def cleanup_test_with_data_endpoint_entries(cleanup_map):
    """Remove the requested entry names from each endpoint."""
    cleanup_errors = []
    for item in cleanup_map:
        try:
            remove_endpoint_entries(item['endpoint'], item['entry_names'])
        except ValueError as exc:
            cleanup_errors.append(str(exc))
    if cleanup_errors:
        raise ValueError('\n'.join(cleanup_errors))


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
    all_entries = sorted(set(entry_names) | {'.staging'})
    cleanup_test_with_data_endpoint_entries([
        {
            'endpoint': endpoint,
            'entry_names': all_entries,
        }
        for endpoint in test_plan['all_destinations'] + test_plan['all_sources']
    ])


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


def cleanup_test_with_data_generated_scripts(runtime_dirs):
    """Remove generated integration script artifacts from a previous run."""
    if not runtime_dirs:
        return
    runtime_root = runtime_dirs.get('root', '')
    if runtime_root and (os.path.exists(runtime_root) or os.path.islink(runtime_root)):
        remove_local_path(runtime_root)


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
    cmd = [
        'ssh',
        '-o', 'BatchMode=yes',
        '-o', 'ConnectTimeout=10',
        '-o', 'LogLevel=ERROR',
    ]
    if port:
        cmd.extend(['-p', str(port)])
    cmd.extend([shell_target(user, host), command])
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    return proc.returncode, stdout.decode('utf-8'), stderr.decode('utf-8')


def load_test_with_data_transfers(
    transfers_file, current_system, current_user, base_dir, runtime_ids=None
):
    """Load the current system/user transfer subset for test-with-data."""
    df = load_test_with_data_transfer_graph(
        transfers_file,
        base_dir,
        runtime_ids=runtime_ids,
    )
    df = filter_transfers_by_system_user(df, current_system, current_user)
    if df.empty:
        raise ValueError(
            "No transfers found for user '{0}' on system '{1}'".format(
                current_user, current_system
            )
        )
    return df


def load_test_with_data_transfer_graph(transfers_file, base_dir, runtime_ids=None):
    """Load all test-with-data transfers with local endpoints absolutized."""
    df = parse_transfers_file(transfers_file, runtime_ids=runtime_ids)
    for column in ('source', 'destination'):
        df[column] = df[column].apply(
            lambda value: absolutize_local_endpoint(value, base_dir)
        )
    return df


def get_test_with_data_runtime_dirs(current_system, current_user):
    """Return writable directories used to generate integration-only artifacts."""
    base_root = config.get_rit_managed_location(current_system)
    runtime_name = "{0}.{1}".format(
        sanitize_identifier(current_system) or 'system',
        sanitize_identifier(current_user) or 'user',
    )
    runtime_root = os.path.join(base_root, 'test_with_data_runtime', runtime_name)
    return {
        'root': runtime_root,
        'scripts_dir': os.path.join(runtime_root, 'scripts'),
        'crontab_dir': os.path.join(runtime_root, 'crontab.d'),
        'validation_scripts_dir': os.path.join(runtime_root, 'validation_scripts'),
    }


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
    expected_cron_names = [
        gcf.cron_file_name(system_user)
        for system_user in transfers_df['system_user'].dropna().unique()
    ]
    gcf.remove_stale_cron_files(crontab_dir, expected_cron_names)
    gcf.write_validation_scripts(validation_scripts_dir, transfers_df)

    grouped = transfers_df.groupby('system_user')
    for system_user, group_df in grouped:
        for _, transfer in group_df.iterrows():
            script_path = os.path.join(scripts_dir, transfer['script_name'])
            script_content = gcf.generate_script_content(transfer)
            with open(script_path, 'w') as handle:
                handle.write(gcf.add_owner_marker(script_content))
            os.chmod(script_path, 0o755)

        cron_path = os.path.join(crontab_dir, gcf.cron_file_name(system_user))
        with open(cron_path, 'w') as handle:
            handle.write(
                gcf.add_owner_marker(
                    gcf.generate_cron_file(system_user, group_df, scripts_dir)
                )
            )

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


def build_test_with_data_handoffs(
    all_transfers_df,
    current_transfers_df,
    slow=False,
):
    """Identify downstream system/user handoffs after the current subset finishes."""
    if all_transfers_df is None or all_transfers_df.empty:
        return []
    if current_transfers_df is None or current_transfers_df.empty:
        return []

    current_pairs = {
        (str(row['system']).strip(), str(row['users']).strip())
        for _, row in current_transfers_df.iterrows()
    }
    destination_keys = {
        endpoint_key(row['destination'])
        for _, row in current_transfers_df.iterrows()
    }

    grouped = []
    grouped_index = {}
    for _, transfer in all_transfers_df.iterrows():
        system = str(transfer['system']).strip()
        user = str(transfer['users']).strip()
        if (system, user) in current_pairs:
            continue
        if endpoint_key(transfer['source']) not in destination_keys:
            continue

        group_key = (system, user)
        if group_key not in grouped_index:
            grouped_index[group_key] = len(grouped)
            grouped.append({
                'system': system,
                'user': user,
                'command': 'landingzones validate integration{0}'.format(
                    ' --slow' if slow else ''
                ),
                'transfers': [],
            })

        grouped[grouped_index[group_key]]['transfers'].append({
            'identifier': str(transfer['identifiers']).strip(),
            'flow_group': str(transfer.get('flow_group', '') or '').strip(),
            'source': transfer['source'],
            'destination': transfer['destination'],
        })

    return grouped


def format_test_with_data_handoff(handoff):
    """Render a downstream handoff for the operator."""
    lines = [
        "Switch to {0}@{1}".format(handoff['user'], handoff['system']),
        "Run from that deployment root: `{0}`".format(handoff['command']),
    ]
    for transfer in handoff['transfers']:
        flow_group = transfer['flow_group']
        if flow_group and flow_group != 'nan':
            lines.append(
                "Next transfer: {0} (flow: {1})".format(
                    transfer['identifier'], flow_group
                )
            )
        else:
            lines.append("Next transfer: {0}".format(transfer['identifier']))
        lines.append(
            "Source to verify there: {0}".format(
                normalize_endpoint_display(transfer['source'])
            )
        )
    return '\n'.join(lines)


def print_test_with_data_handoffs(handoffs):
    """Print downstream handoff guidance for multi-system integration runs."""
    if not handoffs:
        return

    print_header("Next System Handoff")
    for handoff in handoffs:
        print_status(
            "Continue flow",
            "INFO",
            format_test_with_data_handoff(handoff),
        )


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
            remote_path = "{0}:{1}".format(
                build_ssh_target(user, host),
                directory_path,
            )
            return False, "Remote check failed for {0}: {1}".format(
                remote_path,
                stderr.strip() or 'unknown error',
            )
        remote_path = "{0}:{1}".format(
            build_ssh_target(user, host),
            directory_path,
        )
        return 'EXISTS' in stdout, remote_path

    return os.path.isdir(directory_path), directory_path


def endpoint_root_ready(endpoint):
    """Check whether a terminal endpoint root is reachable and exists."""
    root = get_endpoint_root(endpoint)
    user, host, _ = parse_remote_destination(endpoint['value'])
    port = endpoint.get('port', '')

    if host:
        command = '[ -d {0} ] && echo EXISTS || echo MISSING'.format(
            shell_path(root)
        )
        rc, stdout, stderr = run_remote_shell(user, host, command, port)
        remote_path = "{0}:{1}".format(
            build_ssh_target(user, host),
            root,
        )
        if rc != 0:
            return False, "Remote check failed for {0}: {1}".format(
                remote_path,
                stderr.strip() or 'unknown error',
            )
        if 'EXISTS' in stdout:
            return True, remote_path
        return False, remote_path

    return os.path.isdir(root), root


def validate_script_test_results(test_plan, expected_contents):
    """Validate that seeded toy-data directories reached each terminal destination."""
    errors = []
    for endpoint in test_plan['terminal_destinations']:
        expected_entries = sorted(
            expected_contents.get(endpoint_key(endpoint['value']), set())
        )
        root_ready, root_status = endpoint_root_ready(endpoint)
        if not root_ready:
            errors.append(
                "Terminal destination root unavailable: {0}".format(
                    root_status
                )
            )
            continue
        for entry_name in expected_entries:
            exists, path = endpoint_directory_exists(endpoint, entry_name)
            if not exists:
                if str(path).startswith("Remote check failed for "):
                    errors.append(
                        "Terminal destination entry check failed: {0}".format(
                            path
                        )
                    )
                else:
                    errors.append(
                        "Expected test directory missing under terminal destination root: {0}".format(
                            path
                        )
                    )
    return errors


def cleanup_test_with_data_outputs(test_plan, transfers_df, seed_plan, runtime_dirs=None):
    """Remove seeded test directories and runtime artifacts after a test-with-data run."""
    entry_names = sorted({
        entry_name
        for seed in seed_plan
        for entry_name in seed['entry_names']
    })
    cleanup_test_with_data_entries(test_plan, entry_names)
    cleanup_test_with_data_runtime_artifacts(transfers_df)
    cleanup_test_with_data_generated_scripts(runtime_dirs)


def ask_yes_no(prompt_text):
    """Ask a yes/no question and return True for yes."""
    prompt = "\n{0}{1} (y/N): {2}".format(
        Colors.YELLOW, prompt_text, Colors.END
    )
    print(prompt, end="")
    try:
        response = input().strip().lower()
        return response in ['y', 'yes']
    except EOFError:
        print()
        return False
    except KeyboardInterrupt:
        print("\n{0}Operation cancelled.{1}".format(Colors.YELLOW, Colors.END))
        return False


def print_runtime_filter_status(runtime_ids, runtime_filter_source):
    """Print the active runtime filter when one is resolved."""
    if not runtime_ids:
        return
    print_status(
        "Runtime filter",
        "OK",
        "Using runtime_id {0} from {1}".format(
            ', '.join(runtime_ids),
            runtime_filter_source,
        ),
    )


def run_cron_deployment_prompt(runtime_ids=None, runtime_filter_source=None):
    """Offer an interactive cron deployment for the current system/user."""
    print_runtime_filter_status(runtime_ids, runtime_filter_source)

    transfers_df = None
    if runtime_ids:
        try:
            transfers_df = load_runtime_transfers(
                transfers_file=config.transfers_file,
                runtime_ids=runtime_ids,
            )
        except Exception as exc:
            print_status(
                "Runtime filter",
                "ERROR",
                "Cannot read filtered transfers: {0}".format(exc),
            )
            return False

    current_system = get_current_system(
        transfers_df=transfers_df,
        runtime_ids=runtime_ids,
    )
    current_user = get_current_user(
        transfers_df=transfers_df,
        runtime_ids=runtime_ids,
    )

    print_header("Cron Deployment")
    print_status(
        "Deployment target",
        "INFO",
        "{0}@{1}".format(current_user, current_system),
    )

    if not ask_yes_no("Do you want to deploy the cron files now?"):
        print_status(
            "Cron deployment",
            "INFO",
            "Skipped. Re-run `landingzones deploy cron` when ready.",
        )
        return False

    deploy_ok = deploy_cron_files(
        current_system,
        current_user,
        runtime_ids=runtime_ids,
    )
    if deploy_ok:
        print_status(
            "Cron deployment",
            "OK",
            "Cron files deployed and activated. Use `crontab -l` to review active jobs.",
        )
    else:
        print_status(
            "Cron deployment",
            "ERROR",
            "Deployment failed. Review the errors above and retry when ready.",
        )
    return deploy_ok


def normalize_endpoint_display(value):
    """Render a local or remote endpoint with redundant slashes collapsed."""
    user, host, path = parse_remote_destination(value)
    normalized_path = normalize_directory_path(path)
    if host:
        return "{0}:{1}".format(build_ssh_target(user, host), normalized_path)
    return normalized_path


def missing_directory_key(entry):
    """Return a stable dedupe key for a missing directory record."""
    if entry['scope'] == 'remote':
        return (
            entry['scope'],
            entry['user'] or '',
            entry['host'],
            str(entry['port'] or ''),
            entry['path'],
        )
    return (entry['scope'], entry['path'])


def add_missing_directory(missing_directories, entry):
    """Append a missing directory record if it is not already present."""
    key = missing_directory_key(entry)
    if key in {missing_directory_key(item) for item in missing_directories}:
        return
    missing_directories.append(entry)


def format_missing_directory(entry):
    """Return a user-facing display string for a missing directory."""
    if entry['scope'] == 'remote':
        return "{0}:{1}".format(
            build_ssh_target(entry['user'], entry['host']),
            entry['path'],
        )
    return entry['path']


def create_missing_directories(missing_directories):
    """Attempt to create the collected missing directories."""
    if not missing_directories:
        return True

    print_header("Create Missing Directories")
    all_ok = True

    for entry in missing_directories:
        display_path = format_missing_directory(entry)
        if entry['scope'] == 'local':
            try:
                os.makedirs(entry['path'], exist_ok=True)
                print_status(
                    "Create local directory",
                    "OK",
                    display_path,
                )
            except OSError as exc:
                print_status(
                    "Create local directory",
                    "ERROR",
                    "{0}: {1}".format(display_path, exc),
                )
                all_ok = False
            continue

        command = 'mkdir -p {0}'.format(shell_path(entry['path']))
        rc, _, stderr = run_remote_shell(
            entry['user'],
            entry['host'],
            command,
            entry['port'],
        )
        if rc == 0:
            print_status(
                "Create remote directory",
                "OK",
                display_path,
            )
        else:
            print_status(
                "Create remote directory",
                "ERROR",
                "{0}: {1}".format(display_path, stderr.strip() or 'unknown error'),
            )
            all_ok = False

    return all_ok


def run_test_with_data(
    config_file=None,
    transfers_file=None,
    slow=False,
    runtime_ids=None,
    runtime_filter_source=None,
):
    """Run generated scripts against seeded toy-data in the real test tree."""
    print_header("Transfer Test With Data")
    snapshot = _snapshot_config_state()
    current_system = None
    current_user = None
    all_transfers_df = None
    transfers_df = None
    test_plan = None
    seed_plan = None
    runtime_dirs = None

    try:
        work_root = os.getcwd()
        config.load_config(
            config_file=config_file,
            transfers_file=transfers_file,
            runtime_ids=runtime_ids,
        )
        if runtime_ids is None:
            runtime_ids = config.runtime_ids
            if runtime_ids and runtime_filter_source is None:
                runtime_filter_source = 'config'
        else:
            runtime_ids = normalize_runtime_id_args(runtime_ids)
        print_runtime_filter_status(runtime_ids, runtime_filter_source)
        transfers_path = transfers_file or config.transfers_file
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
        all_transfers_df = load_test_with_data_transfer_graph(
            transfers_path,
            work_root,
            runtime_ids=runtime_ids,
        )
        if runtime_ids:
            current_system = get_current_system(
                transfers_df=all_transfers_df,
                runtime_ids=runtime_ids,
            )
            current_user = get_current_user(
                transfers_df=all_transfers_df,
                runtime_ids=runtime_ids,
            )
        else:
            current_system = get_current_system()
            current_user = get_current_user()
        transfers_df = filter_transfers_by_system_user(
            all_transfers_df,
            current_system,
            current_user,
        )
        if transfers_df.empty:
            raise ValueError(
                "No transfers found for user '{0}' on system '{1}'".format(
                    current_user,
                    current_system,
                )
            )
        runtime_dirs = get_test_with_data_runtime_dirs(
            current_system, current_user
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
        existing_state = build_test_with_data_existing_state(
            test_plan, expected_entry_names
        )
        cleanup_mode = ask_test_with_data_existing_state_action(existing_state)
        if cleanup_mode != 'leave':
            cleanup_test_with_data_endpoint_entries(
                build_test_with_data_cleanup_map(existing_state, cleanup_mode)
            )
            print_status(
                "Pre-existing test data cleanup",
                "OK",
                "Removed {0} entries before seeding".format(cleanup_mode),
            )
        else:
            remaining_blockers = [
                item for item in existing_state
                if item['blockers']
            ]
            if remaining_blockers:
                print_status(
                    "Pre-existing test data cleanup",
                    "ERROR",
                    "Blocking entries left in place:\n{0}".format(
                        summarize_test_with_data_existing_state(
                            remaining_blockers
                        )
                    ),
                )
                return False
        cleanup_test_with_data_runtime_artifacts(transfers_df)
        cleanup_test_with_data_generated_scripts(runtime_dirs)
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
        scripts_dir = runtime_dirs['scripts_dir']
        crontab_dir = runtime_dirs['crontab_dir']
        validation_scripts_dir = runtime_dirs['validation_scripts_dir']
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
        downstream_handoffs = build_test_with_data_handoffs(
            all_transfers_df,
            transfers_df,
            slow=slow,
        )
        if downstream_handoffs:
            print_test_with_data_handoffs(downstream_handoffs)
            print_status(
                "Test-with-data handoff",
                "INFO",
                "Left propagated data and runtime artifacts in place so the downstream system can continue the flow",
            )
            return True

        if ask_yes_no(
            "Do you want to clean up the propagated output locations so test-with-data can be rerun from the initial state?"
        ):
            cleanup_test_with_data_outputs(
                test_plan, transfers_df, seed_plan, runtime_dirs
            )
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
    parser.add_argument(
        '--runtime-id',
        action='append',
        default=None,
        help='Exact runtime_id to validate. Defaults to the runtime IDs generated by the most recent build when available.'
    )
    parser.add_argument(
        '--deploy-cron',
        action='store_true',
        help='Prompt to deploy the generated cron files for the current system/user'
    )
    args = parser.parse_args(argv)
    
    # Load configuration from file and/or command line arguments
    config.load_config(
        config_file=args.config,
        transfers_file=args.transfers,
        validation_scripts_dir=args.validation_scripts_dir,
        runtime_ids=args.runtime_id,
    )

    runtime_ids, runtime_filter_source = resolve_runtime_ids(args.runtime_id)

    if args.test_with_data:
        return run_test_with_data(
            config_file=args.config,
            transfers_file=args.transfers,
            slow=args.slow,
            runtime_ids=runtime_ids,
            runtime_filter_source=runtime_filter_source,
        )

    if args.deploy_cron:
        return run_cron_deployment_prompt(
            runtime_ids=runtime_ids,
            runtime_filter_source=runtime_filter_source,
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

    print_runtime_filter_status(runtime_ids, runtime_filter_source)
    
    # Check required tools
    tools_ok = check_required_tools()
    
    # Load and filter transfers
    try:
        df = load_runtime_transfers(
            transfers_file=transfers_file,
            runtime_ids=runtime_ids,
        )
        
        print_status("Configuration file", "OK", "Loaded {0} active transfers".format(len(df)))
    except Exception as e:
        print_status("Configuration file", "ERROR", "Cannot read transfers.tsv: {0}".format(e))
        return False
    
    # Determine current system and user
    current_system = get_current_system(transfers_df=df, runtime_ids=runtime_ids)
    current_user = get_current_user(transfers_df=df, runtime_ids=runtime_ids)
    print("\n{0}Checking transfers for system: {1}, user: {2}{3}".format(
        Colors.BOLD, current_system, current_user, Colors.END))
    
    # Filter transfers for current system and user
    system_transfers = filter_transfers_by_system_user(df, current_system, current_user)
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
    missing_directories = []
    
    for _, transfer in system_transfers.iterrows():
        print_header(
            "Transfer: {0} → {1}".format(
                normalize_endpoint_display(transfer['source']),
                normalize_endpoint_display(transfer['destination']),
            )
        )
        
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
                remote_source_info = inspect_remote_directory(
                    source_user,
                    source_host,
                    source_path,
                    port=source_port,
                    check_writable=False,
                )
                print_status(
                    "Source directory",
                    "OK" if remote_source_info['ok'] else "ERROR",
                    remote_source_info['message'],
                )
                if not remote_source_info['ok']:
                    transfer_ok = False
                    if remote_source_info['missing']:
                        add_missing_directory(
                            missing_directories,
                            {
                                'scope': 'remote',
                                'user': source_user,
                                'host': source_host,
                                'port': source_port,
                                'path': remote_source_info['path'],
                            },
                        )
            else:
                transfer_ok = False
        else:
            local_source_info = inspect_local_directory(
                transfer['source'],
                check_writable=False,
            )
            print_status(
                "Source directory",
                "OK" if local_source_info['ok'] else "ERROR",
                local_source_info['message'],
            )
            if not local_source_info['ok']:
                transfer_ok = False
                if local_source_info['missing']:
                    add_missing_directory(
                        missing_directories,
                        {
                            'scope': 'local',
                            'path': local_source_info['path'],
                        },
                    )
        
        # Check destination
        user, host, dest_path = parse_remote_destination(transfer['destination'])
        port = transfer.get('destination_port', '')
        
        # Convert port to string and handle NaN values safely
        port = str(port) if port is not None else ''
        port = port if port and port != 'nan' and port.strip() else None
        
        if host:
            # Remote destination
            remote_label = build_ssh_target(user, host)
            print(
                "\n  Remote destination: {0}:{1}".format(
                    remote_label,
                    normalize_directory_path(dest_path),
                )
            )
            if port:
                print("  Using port: {0}".format(port))
            
            # Check SSH connection
            ssh_ok, ssh_msg = check_ssh_connection(user, host, port)
            print_status("SSH connection to {0}".format(host), "OK" if ssh_ok else "ERROR", ssh_msg)
            
            if ssh_ok:
                remote_dest_info = inspect_remote_directory(
                    user,
                    host,
                    dest_path,
                    port=port,
                    check_writable=True,
                )
                print_status(
                    "Remote destination directory",
                    "OK" if remote_dest_info['ok'] else "ERROR",
                    remote_dest_info['message'],
                )
                if not remote_dest_info['ok']:
                    transfer_ok = False
                    if remote_dest_info['missing']:
                        add_missing_directory(
                            missing_directories,
                            {
                                'scope': 'remote',
                                'user': user,
                                'host': host,
                                'port': port,
                                'path': remote_dest_info['path'],
                            },
                        )
            else:
                transfer_ok = False
        else:
            # Local destination
            local_dest_info = inspect_local_directory(
                dest_path,
                check_writable=True,
            )
            print_status(
                "Destination directory",
                "OK" if local_dest_info['ok'] else "ERROR",
                local_dest_info['message'],
            )
            if not local_dest_info['ok']:
                transfer_ok = False
                if local_dest_info['missing']:
                    add_missing_directory(
                        missing_directories,
                        {
                            'scope': 'local',
                            'path': local_dest_info['path'],
                        },
                    )
        
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

    if missing_directories:
        print_header("Missing Directories")
        print_status(
            "Missing directory count",
            "INFO",
            "Found {0} missing directories.".format(len(missing_directories)),
        )
        for entry in missing_directories:
            print("  - {0}".format(format_missing_directory(entry)))
        if ask_yes_no("Do you want to attempt to create these missing directories now?"):
            create_ok = create_missing_directories(missing_directories)
            if create_ok:
                print_status(
                    "Directory creation",
                    "OK",
                    "Created all listed directories. Re-run validate deployment to confirm readiness.",
                )
            else:
                print_status(
                    "Directory creation",
                    "ERROR",
                    "Some directories could not be created. Review the errors above.",
                )
    
    # Final summary
    print_header("Deployment Readiness Summary")
    
    overall_ok = tools_ok and flock_ok and all_transfers_ok
    
    if overall_ok:
        print_status("System ready for cron deployment", "OK", 
                    "All checks passed! You can safely deploy the cron files.")
        print("\n{0}{1}✓ READY TO DEPLOY{2}".format(Colors.GREEN, Colors.BOLD, Colors.END))
        print("\nNext steps after all validations pass:")
        print("1. Run: landingzones validate integration")
        print("2. Deploy crons when ready: landingzones deploy cron")
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
