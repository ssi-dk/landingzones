#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Shared readiness, preflight, and deployment helpers."""

import errno
from io import StringIO
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys

from landingzones.config import config
from landingzones.generate_cron_files import (
    configured_artifact_prefix,
    cron_file_name,
    runtime_filter_metadata_path,
    shell_path,
)
from landingzones.transfer_loading import (
    discover_runtime_ids_from_crontabs,
    load_runtime_transfers,
    normalize_runtime_id_args,
    read_runtime_filter_metadata,
)


class Colors:
    """ANSI color codes for console output."""

    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'


def _load_identity_transfers(transfers_file, runtime_ids=None):
    """Load transfer rows for current-system/user detection."""
    if runtime_ids:
        return load_runtime_transfers(
            transfers_file=transfers_file,
            runtime_ids=runtime_ids,
        )
    return load_runtime_transfers(transfers_file=transfers_file)


def normalize_directory_path(path):
    """Collapse redundant slashes in a filesystem path string."""
    value = str(path).strip() if path is not None else ''
    if not value:
        return value
    return re.sub(r'/+', '/', value)


def print_status(message, status, details=None):
    """Print formatted status message."""
    if status == "OK":
        icon = "{0}✓{1}".format(Colors.GREEN, Colors.END)
        status_text = "{0}OK{1}".format(Colors.GREEN, Colors.END)
    elif status == "WARN":
        icon = "{0}⚠{1}".format(Colors.YELLOW, Colors.END)
        status_text = "{0}WARNING{1}".format(Colors.YELLOW, Colors.END)
    elif status == "INFO" or status == "...":
        icon = "{0}ℹ{1}".format(Colors.BLUE, Colors.END)
        status_text = "{0}INFO{1}".format(Colors.BLUE, Colors.END)
    else:
        icon = "{0}✗{1}".format(Colors.RED, Colors.END)
        status_text = "{0}ERROR{1}".format(Colors.RED, Colors.END)

    print("{0} {1}: {2}".format(icon, message, status_text))
    if details:
        print("   {0}".format(details))


def print_header(title):
    """Print section header."""
    print("\n{0}{1}=== {2} ==={3}".format(Colors.BOLD, Colors.BLUE, title, Colors.END))


def check_required_tools():
    """Check if required system tools are available."""
    print_header("Checking Required Tools")

    tools = ['rsync', 'ssh', 'find']
    all_good = True

    for tool in tools:
        try:
            proc = subprocess.Popen(['which', tool], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, _ = proc.communicate()
            if proc.returncode == 0:
                location = stdout.decode('utf-8').strip()
                print_status("{0} available".format(tool), "OK", "Location: {0}".format(location))
            else:
                print_status("{0} missing".format(tool), "ERROR", "Please install {0}".format(tool))
                all_good = False
        except Exception as exc:
            print_status("{0} check failed".format(tool), "ERROR", str(exc))
            all_good = False

    return all_good


def check_flock_command(system):
    """Check whether the configured flock binary exists and is executable."""
    flock_path = config.get_flock_path(system)
    expanded_path = os.path.expandvars(os.path.expanduser(flock_path))

    if not os.path.exists(expanded_path):
        print_status("Flock binary", "ERROR", "Configured path does not exist: {0}".format(flock_path))
        return False

    if not os.access(expanded_path, os.X_OK):
        print_status("Flock binary", "ERROR", "Configured path is not executable: {0}".format(flock_path))
        return False

    print_status("Flock binary", "OK", "Using: {0}".format(flock_path))
    return True


def inspect_local_directory(path, check_writable=True):
    """Return structured status for a local directory check."""
    expanded_path = os.path.expandvars(os.path.expanduser(str(path)))
    normalized_path = normalize_directory_path(expanded_path)
    check_path = normalized_path.rstrip('/') or '/'
    is_wildcard = False
    if normalized_path.endswith('/*') or normalized_path.endswith('*'):
        check_path = normalized_path.rstrip('*').rstrip('/') or '/'
        is_wildcard = True

    result = {
        'ok': False,
        'status': 'missing',
        'path': check_path,
        'message': "Directory does not exist: {0}".format(check_path),
        'is_wildcard': is_wildcard,
        'missing': False,
    }

    if not os.path.exists(check_path):
        result['missing'] = True
        return result

    if not os.path.isdir(check_path):
        result['status'] = 'not_directory'
        result['message'] = "Path exists but is not a directory: {0}".format(
            check_path
        )
        return result

    if check_writable and not os.access(check_path, os.W_OK):
        result['status'] = 'not_writable'
        result['message'] = "Directory is not writable: {0}".format(check_path)
        return result

    result['ok'] = True
    result['status'] = 'ok'
    if is_wildcard:
        result['message'] = "Parent path: {0}".format(check_path)
    else:
        result['message'] = "Path: {0}".format(check_path)
    return result


def check_local_directory(path, description, check_writable=True):
    """Check if a local directory exists and is writable."""
    info = inspect_local_directory(path, check_writable=check_writable)
    if info['is_wildcard'] and description == "Source directory":
        description = "Source directory (wildcard pattern)"

    print_status(
        "{0}".format(description),
        "OK" if info['ok'] else "ERROR",
        info['message'],
    )
    return info['ok']


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


def inspect_remote_directory(
    user,
    host,
    path,
    port=None,
    check_writable=True,
):
    """Return structured status for a remote directory check."""
    normalized_path = normalize_directory_path(path).rstrip('/') or '/'
    try:
        cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10']
        if port:
            cmd.extend(['-p', str(port)])
        remote_path_expr = shell_path(normalized_path)
        writable_flag = '1' if check_writable else '0'
        remote_script = (
            'target_path={0}; '
            'if [ ! -e "$target_path" ]; then '
            'echo "DIR_MISSING"; '
            'elif [ ! -d "$target_path" ]; then '
            'echo "DIR_NOT_DIRECTORY"; '
            'elif [ "{1}" = "1" ] && [ ! -w "$target_path" ]; then '
            'echo "DIR_NOT_WRITABLE"; '
            'else '
            'echo "DIR_OK"; '
            'fi'
        ).format(
            remote_path_expr,
            writable_flag,
        )
        remote_command = "sh -c {0}".format(shlex.quote(remote_script))
        cmd.extend([build_ssh_target(user, host), remote_command])

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        result_stdout = stdout.decode('utf-8').strip()
        result_stderr = stderr.decode('utf-8').strip()

        result = {
            'ok': False,
            'status': 'remote_error',
            'path': normalized_path,
            'message': "Remote check failed: {0}".format(result_stderr),
            'missing': False,
        }

        if proc.returncode != 0:
            return result

        if 'DIR_OK' in result_stdout:
            result['ok'] = True
            result['status'] = 'ok'
            result['message'] = (
                "Directory exists and is writable"
                if check_writable else
                "Directory exists"
            )
            return result

        if 'DIR_MISSING' in result_stdout:
            result['status'] = 'missing'
            result['missing'] = True
            result['message'] = "Directory does not exist: {0}".format(
                normalized_path
            )
            return result

        if 'DIR_NOT_DIRECTORY' in result_stdout:
            result['status'] = 'not_directory'
            result['message'] = "Path exists but is not a directory: {0}".format(
                normalized_path
            )
            return result

        if 'DIR_NOT_WRITABLE' in result_stdout:
            result['status'] = 'not_writable'
            result['message'] = "Directory is not writable: {0}".format(
                normalized_path
            )
            return result

        result['message'] = "Remote check returned unexpected output: {0}".format(
            result_stdout or '(empty)'
        )
        return result
    except Exception as exc:
        return {
            'ok': False,
            'status': 'remote_error',
            'path': normalized_path,
            'message': "Remote directory check error: {0}".format(str(exc)),
            'missing': False,
        }


def check_ssh_connection(user, host, port=None):
    """Check SSH connection to remote host."""
    try:
        cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10']
        if port:
            cmd.extend(['-p', str(port)])
        cmd.extend([build_ssh_target(user, host), 'echo', 'SSH_TEST_OK'])

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        result_stdout = stdout.decode('utf-8')
        result_stderr = stderr.decode('utf-8')

        if proc.returncode == 0 and 'SSH_TEST_OK' in result_stdout:
            return True, "Connection successful"
        return False, "SSH failed: {0}".format(result_stderr.strip() if result_stderr else 'Unknown error')
    except Exception as exc:
        return False, "SSH test error: {0}".format(str(exc))


def check_remote_directory(user, host, path, port=None, description="Remote directory", check_writable=True):
    """Check if a remote directory exists and is optionally writable."""
    info = inspect_remote_directory(
        user,
        host,
        path,
        port=port,
        check_writable=check_writable,
    )
    return info['ok'], info['message']


def check_log_directory(log_file_path):
    """Check if log directory exists and create if necessary."""
    if not log_file_path or log_file_path == 'nan':
        return True, "No log file specified"

    expanded_path = os.path.expandvars(os.path.expanduser(log_file_path))
    log_dir = normalize_directory_path(os.path.dirname(expanded_path))

    if not log_dir:
        return True, "Log file in current directory"

    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
            return True, "Created log directory: {0}".format(log_dir)
        except OSError as exc:
            if exc.errno != errno.EEXIST or not os.path.isdir(log_dir):
                return False, "Cannot create log directory {0}: {1}".format(log_dir, str(exc))

    if not os.access(log_dir, os.W_OK):
        return False, "Log directory not writable: {0}".format(log_dir)

    return True, "Log directory OK: {0}".format(log_dir)


def check_lock_file_directory(lock_file=None):
    """Check if lock file directory exists and is writable."""
    if lock_file is None:
        lock_file = config.default_lock_file

    lock_dir = os.path.dirname(lock_file) or '.'
    if not os.path.exists(lock_dir):
        print_status("Lock file directory", "ERROR", "Directory does not exist: {0}".format(lock_dir))
        return False
    if not os.access(lock_dir, os.W_OK):
        print_status("Lock file directory", "ERROR", "Directory not writable: {0}".format(lock_dir))
        return False

    print_status("Lock file directory", "OK", "Path: {0} (lock: {1})".format(lock_dir, lock_file))
    return True


def _select_from_transfer_values(prompt_label, values, current_value=''):
    """Prompt for one value from a parsed transfer-definition column."""
    if len(values) == 1:
        return values[0]

    print("\n{0}{1}: {2}{3}".format(Colors.YELLOW, prompt_label, current_value, Colors.END))
    for index, value in enumerate(values, 1):
        marker = " (current)" if value == current_value else ""
        print("  {0}. {1}{2}".format(index, value, marker))

    while True:
        try:
            choice = input("\nSelect {0} (1-{1}) or enter value: ".format(prompt_label.lower(), len(values))).strip()
            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(values):
                    return values[idx]
            elif choice in values:
                return choice
            elif not choice and current_value in values:
                return current_value
            print("Invalid choice. Please try again.")
        except KeyboardInterrupt:
            print("\nOperation cancelled.")
            sys.exit(1)


def get_current_user(transfers_df=None, runtime_ids=None):
    """Get current user and allow selection if multiple users exist in transfers."""
    current_user = os.environ.get('USER', os.environ.get('USERNAME', ''))
    transfers_file = config.transfers_file

    try:
        df = (
            transfers_df if transfers_df is not None
            else _load_identity_transfers(transfers_file, runtime_ids)
        )
        users = df['users'].unique()
        if current_user in users:
            return current_user
        if len(users) == 1:
            return users[0]
    except Exception:
        pass

    try:
        df = (
            transfers_df if transfers_df is not None
            else _load_identity_transfers(transfers_file, runtime_ids)
        )
        users = df['users'].unique()
        return _select_from_transfer_values("Current user", users, current_user)
    except Exception as exc:
        print("Could not read {0}: {1}".format(transfers_file, exc))
        return current_user if current_user else input("Please enter your username: ").strip()


def get_current_system(transfers_df=None, runtime_ids=None):
    """Determine current system based on hostname or user input."""
    hostname = socket.gethostname().lower()
    transfers_file = config.transfers_file

    try:
        df = (
            transfers_df if transfers_df is not None
            else _load_identity_transfers(transfers_file, runtime_ids)
        )
        systems = df['system'].unique()
        for system in systems:
            if system.lower() in hostname:
                return system
        if len(systems) == 1:
            return systems[0]
    except Exception:
        pass

    try:
        df = (
            transfers_df if transfers_df is not None
            else _load_identity_transfers(transfers_file, runtime_ids)
        )
        systems = df['system'].unique()
        return _select_from_transfer_values("Current hostname", systems, hostname)
    except Exception as exc:
        print("Could not read {0}: {1}".format(transfers_file, exc))
        return input("Please enter your system name: ").strip()


def generate_cron_files(runtime_ids=None):
    """Generate cron files using the installed module."""
    if runtime_ids is None:
        runtime_ids = config.runtime_ids
    argv = []
    for runtime_id in runtime_ids or []:
        argv.extend(['--runtime-id', runtime_id])
    try:
        from landingzones import generate_cron_files as gcf
        output_capture = None
        old_stdout = None
        if StringIO is not None:
            output_capture = StringIO()
            old_stdout = sys.stdout
            sys.stdout = output_capture
        try:
            gcf.main(argv)
        finally:
            if old_stdout is not None:
                sys.stdout = old_stdout
        details = output_capture.getvalue() if output_capture is not None else "Generated cron files"
        return True, details
    except Exception:
        try:
            proc = subprocess.Popen(
                [sys.executable, '-m', 'landingzones.generate_cron_files'] + argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = proc.communicate()
            out = stdout.decode('utf-8') if stdout else ''
            err = stderr.decode('utf-8') if stderr else ''
            if proc.returncode == 0:
                return True, out or 'Generated cron files'
            return False, "Generation failed: {0}".format(err or out)
        except Exception as exc:
            return False, "Error running generator: {0}".format(str(exc))


def setup_crontab_directory():
    """Ensure ~/crontab.d directory exists."""
    crontab_dir = os.path.expandvars(os.path.expanduser("~/crontab.d"))
    try:
        os.makedirs(crontab_dir)
        return True, "Directory ready: {0}".format(crontab_dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(crontab_dir):
            return True, "Directory ready: {0}".format(crontab_dir)
        return False, "Cannot create directory {0}: {1}".format(crontab_dir, str(exc))


def staged_crontab_directory():
    """Return the operator's staged cron fragment directory."""
    return os.path.expandvars(os.path.expanduser("~/crontab.d"))


def cron_runtime_id_from_filename(filename):
    """Return a runtime_id for generated Landing Zone cron filenames."""
    suffix = '.Landing_Zone.cron'
    if not filename.endswith(suffix):
        return None
    stem = filename[:-len(suffix)]
    prefix = configured_artifact_prefix()
    if prefix:
        prefix_text = "{0}.".format(prefix)
        if not stem.startswith(prefix_text):
            return None
        stem = stem[len(prefix_text):]
    return stem or None


def staged_cron_fragments(crontab_dir):
    """Return sorted staged .cron fragment paths."""
    if not os.path.isdir(crontab_dir):
        return []
    return [
        os.path.join(crontab_dir, entry)
        for entry in sorted(os.listdir(crontab_dir))
        if entry.endswith('.cron') and os.path.isfile(os.path.join(crontab_dir, entry))
    ]


def classify_cron_fragments(cron_files):
    """Classify staged cron fragments as generated runtime or unidentified."""
    identified = []
    unidentified = []
    for path in cron_files:
        filename = os.path.basename(path)
        runtime_id = cron_runtime_id_from_filename(filename)
        if runtime_id:
            identified.append({
                'filename': filename,
                'path': path,
                'runtime_id': runtime_id,
            })
        else:
            unidentified.append({
                'filename': filename,
                'path': path,
            })
    return identified, unidentified


def print_cron_fragment_list(title, fragments, status="INFO"):
    """Print a deterministic cron fragment preview line."""
    filenames = [fragment['filename'] for fragment in fragments]
    details = ', '.join(filenames) if filenames else '(none)'
    print_status(title, status, details)


def print_cron_activation_preview(
    cron_scope,
    activated_runtime_fragments,
    preserved_unidentified_fragments,
    excluded_runtime_fragments,
):
    """Show the operator exactly which staged cron fragments will be activated."""
    print_header("Cron Activation Preview")
    print_status("Cron activation scope", "INFO", cron_scope)
    if cron_scope == 'staged':
        print_status(
            "Staged cron activation",
            "WARN",
            "Every staged .cron file will be activated.",
        )
    print_cron_fragment_list(
        "Activated runtime fragments",
        activated_runtime_fragments,
        "OK" if activated_runtime_fragments else "WARN",
    )
    print_cron_fragment_list(
        "Preserved Unidentified Cron Fragments",
        preserved_unidentified_fragments,
    )
    print_cron_fragment_list(
        "Excluded runtime cron fragments",
        excluded_runtime_fragments,
    )


def is_interactive_terminal():
    """Return whether stdin can receive an operator confirmation prompt."""
    return sys.stdin.isatty()


def cron_activation_confirmed(
    cron_scope,
    confirm_activation=False,
    prompt_confirmation=None,
):
    """Require explicit approval before replacing the active crontab."""
    if confirm_activation:
        return True
    if not is_interactive_terminal():
        print_status(
            "Crontab activation",
            "ERROR",
            (
                "Non-interactive cron activation for scope '{0}' requires "
                "--confirm-cron-activation."
            ).format(cron_scope),
        )
        return False
    if prompt_confirmation is None:
        response = input(
            "Replace active crontab using cron scope '{0}'? (y/N): ".format(
                cron_scope
            )
        ).strip().lower()
        return response in ('y', 'yes')
    return prompt_confirmation(
        "Replace active crontab using cron scope '{0}'?".format(cron_scope)
    )


def activate_cron_fragments(cron_files):
    """Replace the active crontab with the provided cron fragment contents."""
    content_parts = []
    for cron_file in cron_files:
        with open(cron_file, 'r') as handle:
            content = handle.read()
        content_parts.append(content)
        if content and not content.endswith('\n'):
            content_parts.append('\n')
    active_content = ''.join(content_parts).encode('utf-8')

    proc = subprocess.Popen(
        ['crontab', '-'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    _, stderr = proc.communicate(input=active_content)
    result_stderr = stderr.decode('utf-8')

    if proc.returncode != 0:
        print_status(
            "Crontab activation",
            "ERROR",
            "Failed: {0}".format(result_stderr.strip()),
        )
        return False

    print_status(
        "Crontab activation",
        "OK",
        "Activated {0} cron files".format(len(cron_files)),
    )
    verify_proc = subprocess.Popen(
        ['crontab', '-l'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    verify_stdout, _ = verify_proc.communicate()
    if verify_proc.returncode == 0:
        lines = verify_stdout.decode('utf-8').split('\n')
        active_jobs = len([
            line for line in lines
            if line.strip() and not line.startswith('#')
        ])
        print_status(
            "Crontab verification",
            "OK",
            "Total active cron jobs: {0}".format(active_jobs),
        )
    return True


def copy_generated_runtime_crons(
    runtime_ids,
    crontab_dir,
    repair_missing=False,
    confirm_activation=False,
    prompt_confirmation=None,
):
    """Copy generated selected runtime cron fragments into the staged directory."""
    copied_files = []
    missing_runtime_ids = []
    for runtime_id in runtime_ids:
        filename = cron_file_name(runtime_id)
        generated_path = os.path.join(config.crontab_dir, filename)
        staged_path = os.path.join(crontab_dir, filename)
        if os.path.exists(generated_path):
            if repair_missing and os.path.exists(staged_path):
                continue
            if repair_missing and not confirm_activation:
                if not is_interactive_terminal():
                    print_status(
                        "Cron staged-file repair",
                        "ERROR",
                        (
                            "Missing expected runtime cron fragment {0} cannot "
                            "be repaired non-interactively without "
                            "--confirm-cron-activation."
                        ).format(filename),
                    )
                    return None, None
                if prompt_confirmation is None:
                    response = input(
                        "Copy missing expected runtime cron fragment {0} "
                        "into ~/crontab.d before activation? (y/N): ".format(
                            filename
                        )
                    ).strip().lower()
                    should_copy = response in ('y', 'yes')
                else:
                    should_copy = prompt_confirmation(
                        (
                            "Copy missing expected runtime cron fragment {0} "
                            "into ~/crontab.d before activation?"
                        ).format(filename)
                    )
                if not should_copy:
                    print_status(
                        "Cron staged-file repair",
                        "ERROR",
                        "Missing expected runtime cron fragment was not copied: {0}".format(
                            filename
                        ),
                    )
                    return None, None
            try:
                shutil.copy2(generated_path, staged_path)
                copied_files.append(filename)
                print_status(
                    "Copy {0}".format(filename),
                    "OK",
                    "Copied to {0}".format(staged_path),
                )
            except Exception as exc:
                print_status(
                    "Copy {0}".format(generated_path),
                    "ERROR",
                    "Failed: {0}".format(str(exc)),
                )
                return None, None
        elif not os.path.exists(staged_path):
            missing_runtime_ids.append(runtime_id)
    return copied_files, missing_runtime_ids


def resolve_expected_runtime_ids():
    """Resolve runtime IDs represented by the latest generated cron output."""
    metadata_path = runtime_filter_metadata_path(config.crontab_dir)
    if os.path.exists(metadata_path):
        runtime_ids = read_runtime_filter_metadata(config.crontab_dir)
        if runtime_ids:
            print_status(
                "Expected runtime metadata",
                "OK",
                "Using {0}".format(metadata_path),
            )
        return runtime_ids

    runtime_ids = discover_runtime_ids_from_crontabs(config.crontab_dir)
    if runtime_ids:
        print_status(
            "Expected runtime metadata",
            "WARN",
            (
                "Missing {0}; discovered expected runtime IDs from generated "
                "cron filenames."
            ).format(metadata_path),
        )
    return runtime_ids


def select_cron_fragments_for_activation(cron_scope, runtime_ids, crontab_dir):
    """Select staged cron fragments according to the requested activation scope."""
    cron_files = staged_cron_fragments(crontab_dir)
    identified, unidentified = classify_cron_fragments(cron_files)
    runtime_id_set = set(runtime_ids or [])
    if cron_scope in ('selected', 'expected'):
        activated_runtime = [
            fragment for fragment in identified
            if fragment['runtime_id'] in runtime_id_set
        ]
        excluded_runtime = [
            fragment for fragment in identified
            if fragment['runtime_id'] not in runtime_id_set
        ]
        active_files = [
            fragment['path']
            for fragment in activated_runtime + unidentified
        ]
        return active_files, activated_runtime, unidentified, excluded_runtime
    return cron_files, identified, unidentified, []


def deploy_cron_files(
    current_system,
    current_user=None,
    runtime_ids=None,
    cron_scope='selected',
    confirm_activation=False,
    prompt_confirmation=None,
):
    """Deploy cron files for the current system and user."""
    if runtime_ids is None:
        runtime_ids = config.runtime_ids
    runtime_ids = normalize_runtime_id_args(runtime_ids)
    if current_user is None:
        current_user = os.environ.get('USER', os.environ.get('USERNAME', ''))

    print_header("Automatic Cron Deployment")
    print_status("Deploying for", "INFO", "{0}@{1}".format(current_user, current_system))

    dir_ok, dir_msg = setup_crontab_directory()
    print_status("Crontab directory setup", "OK" if dir_ok else "ERROR", dir_msg)
    if not dir_ok:
        return False

    if cron_scope == 'expected':
        runtime_ids = resolve_expected_runtime_ids()

    print_status("Generating cron files", "INFO", "Using landingzones.generate_cron_files")
    gen_ok, gen_msg = generate_cron_files(runtime_ids=runtime_ids)
    if not gen_ok:
        print_status("Cron file generation", "WARN", gen_msg)
        print_status("Continuing deployment", "INFO", "Will use existing files in crontab.d/")
    else:
        print_status("Cron file generation", "OK", gen_msg)

    crontab_dir = staged_crontab_directory()
    if cron_scope in ('selected', 'expected') and not runtime_ids:
        scope_label = "selected" if cron_scope == 'selected' else "expected"
        print_status(
            "Cron file discovery",
            "ERROR",
            "No {0} runtime IDs available for cron activation.".format(
                scope_label
            ),
        )
        return False

    copied_files, missing_runtime_ids = copy_generated_runtime_crons(
        runtime_ids,
        crontab_dir,
        repair_missing=cron_scope == 'expected',
        confirm_activation=confirm_activation,
        prompt_confirmation=prompt_confirmation,
    )
    if copied_files is None:
        return False
    if missing_runtime_ids:
        print_status(
            "Cron file discovery",
            "ERROR",
            "Missing generated or staged cron fragments for runtime_id: {0}. Rebuild generated cron output before retrying.".format(
                ', '.join(missing_runtime_ids)
            ),
        )
        return False

    active_files, activated_runtime, unidentified, excluded_runtime = (
        select_cron_fragments_for_activation(cron_scope, runtime_ids, crontab_dir)
    )
    if not active_files:
        print_status(
            "Cron file discovery",
            "ERROR",
            "No staged cron fragments selected for activation.",
        )
        return False
    print_cron_activation_preview(
        cron_scope,
        activated_runtime,
        unidentified,
        excluded_runtime,
    )
    if not cron_activation_confirmed(
        cron_scope,
        confirm_activation=confirm_activation,
        prompt_confirmation=prompt_confirmation,
    ):
        print_status(
            "Crontab activation",
            "INFO",
            "Skipped activation for cron scope '{0}'.".format(cron_scope),
        )
        return False

    try:
        return activate_cron_fragments(active_files)
    except Exception as exc:
        print_status("Crontab activation", "ERROR", "Error: {0}".format(str(exc)))
        return False
