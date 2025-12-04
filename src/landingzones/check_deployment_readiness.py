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


class Colors:
    """ANSI color codes for console output"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'


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


def check_required_tools():
    """Check if required system tools are available"""
    print_header("Checking Required Tools")
    
    tools = ['rsync', 'ssh', 'find']
    
    # Check for flock (Linux) or shlock (some Unix systems)
    flock_tools = ['flock', 'shlock']
    flock_found = False
    
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
    
    # Check for file locking tools
    for flock_tool in flock_tools:
        try:
            proc = subprocess.Popen(['which', flock_tool], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()
            result_returncode = proc.returncode
            result_stdout = stdout.decode('utf-8')
            if result_returncode == 0:
                location = result_stdout.strip()
                print_status("{0} available".format(flock_tool), "OK", 
                           "Location: {0}".format(location))
                flock_found = True
                break
        except Exception:
            continue
    
    if not flock_found:
        print_status("File locking tool", "WARN", 
                    "No flock/shlock found - cron may need manual locking")
    
    return all_good


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
    """Parse remote destination into components"""
    if '@' in destination and ':' in destination:
        # Format: user@host:/path
        user_host, path = destination.split(':', 1)
        user, host = user_host.split('@', 1)
        return user, host, path
    return None, None, destination  # Local path


def check_ssh_connection(user, host, port=None):
    """Check SSH connection to remote host"""
    try:
        cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10']
        if port:
            cmd.extend(['-p', str(port)])
        cmd.extend(['{0}@{1}'.format(user, host), 'echo', 'SSH_TEST_OK'])
        
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


def check_remote_directory(user, host, path, port=None, description="Remote directory"):
    """Check if remote directory exists and is writable"""
    try:
        cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10']
        if port:
            cmd.extend(['-p', str(port)])
        
        # Check if directory exists and is writable
        test_cmd = '[ -d "{0}" ] && [ -w "{0}" ] && echo "DIR_OK" || echo "DIR_FAIL"'.format(path)
        cmd.extend(['{0}@{1}'.format(user, host), test_cmd])
        
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        result_returncode = proc.returncode
        result_stdout = stdout.decode('utf-8')
        result_stderr = stderr.decode('utf-8')
        
        if result_returncode == 0:
            if 'DIR_OK' in result_stdout:
                return True, "Directory exists and is writable"
            else:
                return False, "Directory does not exist or is not writable"
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
        df = pd.read_csv(transfers_file, sep='\t')
        df = df[~df['system'].astype(str).str.startswith('#')]
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
        df = pd.read_csv(transfers_file, sep='\t')
        df = df[~df['system'].astype(str).str.startswith('#')]
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
        df = pd.read_csv(transfers_file, sep='\t')
        df = df[~df['system'].astype(str).str.startswith('#')]
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
        df = pd.read_csv(transfers_file, sep='\t')
        df = df[~df['system'].astype(str).str.startswith('#')]
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
    args = parser.parse_args()
    
    # Load configuration from file and/or command line arguments
    config.load_config(
        config_file=args.config,
        transfers_file=args.transfers
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
        df = pd.read_csv(transfers_file, sep='\t')
        df = df[~df['system'].astype(str).str.startswith('#')]
        
        # Clean up any extra whitespace in string columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
        
        # Handle empty columns and convert destination_port to string
        df['rsync_options'] = df['rsync_options'].fillna('').astype(str)
        df['log_file'] = df['log_file'].fillna('').astype(str)
        df['destination_port'] = df['destination_port'].fillna('').astype(str)
        
        # Remove rows where columns contain 'nan' (from NaN values)
        df['rsync_options'] = df['rsync_options'].replace('nan', '')
        df['log_file'] = df['log_file'].replace('nan', '')
        df['destination_port'] = df['destination_port'].replace('nan', '')
        
        # Clean up destination_port - remove .0 if it's a whole number
        df['destination_port'] = df['destination_port'].str.replace(
            r'\.0$', '', regex=True)
        
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
    lock_ok = check_lock_file_directory()
    
    # Check each transfer
    all_transfers_ok = True
    
    for _, transfer in system_transfers.iterrows():
        print_header("Transfer: {0} → {1}".format(transfer['source'], transfer['destination']))
        
        transfer_ok = True
        
        # Check source directory
        if not check_local_directory(transfer['source'], "Source directory"):
            transfer_ok = False
        
        # Check destination
        user, host, dest_path = parse_remote_destination(transfer['destination'])
        port = transfer.get('destination_port', '')
        
        # Convert port to string and handle NaN values safely
        port = str(port) if port is not None else ''
        port = port if port and port != 'nan' and port.strip() else None
        
        if user and host:
            # Remote destination
            print("\n  Remote destination: {0}@{1}:{2}".format(user, host, dest_path))
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
    
    overall_ok = tools_ok and lock_ok and all_transfers_ok
    
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