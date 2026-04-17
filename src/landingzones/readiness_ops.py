#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Shared readiness, preflight, and deployment helpers."""

import errno
import glob
from io import StringIO
import os
import shutil
import socket
import subprocess
import sys

from landingzones.config import config
from landingzones.transfer_loading import load_runtime_transfers


class Colors:
    """ANSI color codes for console output."""

    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'


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


def check_local_directory(path, description, check_writable=True):
    """Check if a local directory exists and is writable."""
    expanded_path = os.path.expandvars(os.path.expanduser(path))
    check_path = expanded_path
    is_wildcard = False
    if expanded_path.endswith('/*') or expanded_path.endswith('*'):
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
    try:
        cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10']
        if port:
            cmd.extend(['-p', str(port)])

        if check_writable:
            test_cmd = '[ -d "{0}" ] && [ -w "{0}" ] && echo "DIR_OK" || echo "DIR_FAIL"'.format(path)
        else:
            test_cmd = '[ -d "{0}" ] && echo "DIR_OK" || echo "DIR_FAIL"'.format(path)
        cmd.extend([build_ssh_target(user, host), test_cmd])

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        result_stdout = stdout.decode('utf-8')
        result_stderr = stderr.decode('utf-8')

        if proc.returncode == 0:
            if 'DIR_OK' in result_stdout:
                return True, "Directory exists and is writable" if check_writable else "Directory exists"
            return False, "Directory does not exist or is not writable" if check_writable else "Directory does not exist"
        return False, "Remote check failed: {0}".format(result_stderr.strip())
    except Exception as exc:
        return False, "Remote directory check error: {0}".format(str(exc))


def check_log_directory(log_file_path):
    """Check if log directory exists and create if necessary."""
    if not log_file_path or log_file_path == 'nan':
        return True, "No log file specified"

    expanded_path = os.path.expandvars(os.path.expanduser(log_file_path))
    log_dir = os.path.dirname(expanded_path)

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


def get_current_user():
    """Get current user and allow selection if multiple users exist in transfers."""
    current_user = os.environ.get('USER', os.environ.get('USERNAME', ''))
    transfers_file = config.transfers_file

    try:
        df = load_runtime_transfers(transfers_file=transfers_file)
        users = df['users'].unique()
        if current_user in users:
            return current_user
        if len(users) == 1:
            return users[0]
    except Exception:
        pass

    try:
        df = load_runtime_transfers(transfers_file=transfers_file)
        users = df['users'].unique()
        return _select_from_transfer_values("Current user", users, current_user)
    except Exception as exc:
        print("Could not read {0}: {1}".format(transfers_file, exc))
        return current_user if current_user else input("Please enter your username: ").strip()


def get_current_system():
    """Determine current system based on hostname or user input."""
    hostname = socket.gethostname().lower()
    transfers_file = config.transfers_file

    try:
        df = load_runtime_transfers(transfers_file=transfers_file)
        systems = df['system'].unique()
        for system in systems:
            if system.lower() in hostname:
                return system
    except Exception:
        pass

    try:
        df = load_runtime_transfers(transfers_file=transfers_file)
        systems = df['system'].unique()
        return _select_from_transfer_values("Current hostname", systems, hostname)
    except Exception as exc:
        print("Could not read {0}: {1}".format(transfers_file, exc))
        return input("Please enter your system name: ").strip()


def generate_cron_files():
    """Generate cron files using the installed module."""
    try:
        from landingzones import generate_cron_files as gcf
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
    except Exception:
        try:
            proc = subprocess.Popen(
                [sys.executable, '-m', 'landingzones.generate_cron_files'],
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


def deploy_cron_files(current_system, current_user=None):
    """Deploy cron files for the current system and user."""
    if current_user is None:
        current_user = os.environ.get('USER', os.environ.get('USERNAME', ''))

    print_header("Automatic Cron Deployment")
    print_status("Deploying for", "INFO", "{0}@{1}".format(current_user, current_system))

    dir_ok, dir_msg = setup_crontab_directory()
    print_status("Crontab directory setup", "OK" if dir_ok else "ERROR", dir_msg)
    if not dir_ok:
        return False

    print_status("Generating cron files", "INFO", "Using landingzones.generate_cron_files")
    gen_ok, gen_msg = generate_cron_files()
    if not gen_ok:
        print_status("Cron file generation", "WARN", gen_msg)
        print_status("Continuing deployment", "INFO", "Will use existing files in crontab.d/")
    else:
        print_status("Cron file generation", "OK", gen_msg)

    try:
        pattern = "crontab.d/{0}.{1}.Landing_Zone.cron".format(current_system, current_user)
        cron_files = glob.glob(pattern)
        if not cron_files:
            msg = "No new files for '{0}@{1}'. Using existing crontab.d/".format(current_user, current_system)
            print_status("Cron file discovery", "WARN", msg)
            return True
        print_status("Cron file discovery", "OK", "Found {0} files".format(len(cron_files)))
    except Exception as exc:
        print_status("Cron file discovery", "ERROR", "Search failed: {0}".format(str(exc)))
        return False

    crontab_dir = os.path.expandvars(os.path.expanduser("~/crontab.d"))
    copied_files = []
    for cron_file in cron_files:
        try:
            filename = os.path.basename(cron_file)
            dest_path = os.path.join(crontab_dir, filename)
            shutil.copy2(cron_file, dest_path)
            copied_files.append(filename)
            print_status("Copy {0}".format(filename), "OK", "Copied to {0}".format(dest_path))
        except Exception as exc:
            print_status("Copy {0}".format(cron_file), "ERROR", "Failed: {0}".format(str(exc)))
            return False

    if not copied_files:
        return True

    try:
        cmd = "cat {0} | crontab -".format(os.path.join(crontab_dir, "*.cron"))
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, stderr = proc.communicate()
        result_stderr = stderr.decode('utf-8')

        if proc.returncode != 0:
            print_status("Crontab activation", "ERROR", "Failed: {0}".format(result_stderr.strip()))
            return False

        print_status("Crontab activation", "OK", "Activated {0} cron files".format(len(copied_files)))
        verify_proc = subprocess.Popen(['crontab', '-l'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        verify_stdout, _ = verify_proc.communicate()
        if verify_proc.returncode == 0:
            lines = verify_stdout.decode('utf-8').split('\n')
            active_jobs = len([line for line in lines if line.strip() and not line.startswith('#')])
            print_status("Crontab verification", "OK", "Total active cron jobs: {0}".format(active_jobs))
        return True
    except Exception as exc:
        print_status("Crontab activation", "ERROR", "Error: {0}".format(str(exc)))
        return False
