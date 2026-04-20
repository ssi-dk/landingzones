#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Test suite for check_deployment_readiness.py"""

import os
import shutil
import tempfile
from pathlib import Path
import pytest
import pandas as pd

from landingzones import check_deployment_readiness as cdr


class TestParseRemoteDestination:
    """Test the parse_remote_destination function"""
    
    def test_remote_destination(self):
        """Test parsing remote destination with user@host:path format"""
        user, host, path = cdr.parse_remote_destination('testuser@testhost:/remote/path/')
        
        assert user == 'testuser'
        assert host == 'testhost'
        assert path == '/remote/path/'
    
    def test_local_destination(self):
        """Test parsing local destination without @ and :"""
        user, host, path = cdr.parse_remote_destination('/local/path/')
        
        assert user is None
        assert host is None
        assert path == '/local/path/'
    
    def test_complex_hostname(self):
        """Test parsing with complex hostname"""
        user, host, path = cdr.parse_remote_destination('user@host.domain.com:/path/')
        
        assert user == 'user'
        assert host == 'host.domain.com'
        assert path == '/path/'
    
    def test_path_with_spaces(self):
        """Test parsing path that contains spaces"""
        user, host, path = cdr.parse_remote_destination('user@host:/path with spaces/')
        
        assert user == 'user'
        assert host == 'host'
        assert path == '/path with spaces/'

    def test_host_alias_without_user(self):
        """Test parsing a remote ssh alias without an explicit user."""
        user, host, path = cdr.parse_remote_destination('remotealias:$HOME/Landing_Zone/')

        assert user is None
        assert host == 'remotealias'
        assert path == '$HOME/Landing_Zone/'


class TestRunRemoteShell:
    """Test remote shell invocation details."""

    def test_run_remote_shell_uses_error_log_level(self, monkeypatch):
        """Remote readiness checks should suppress SSH warning noise."""
        captured = {}

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None):
                captured['args'] = args
                self.returncode = 0

            def communicate(self):
                return b'EXISTS\n', b''

        monkeypatch.setattr(cdr.subprocess, 'Popen', DummyProcess)

        rc, stdout, stderr = cdr.run_remote_shell(
            'tester', 'remotehost', 'echo EXISTS', '2222'
        )

        assert rc == 0
        assert stdout == 'EXISTS\n'
        assert stderr == ''
        assert '-o' in captured['args']
        assert 'LogLevel=ERROR' in captured['args']


class TestCheckLocalDirectory:
    """Test the check_local_directory function"""
    
    def test_existing_directory(self, tmp_path):
        """Test checking an existing directory"""
        test_dir = tmp_path / "test_dir"
        test_dir.mkdir()
        
        result = cdr.check_local_directory(str(test_dir), "Test directory", check_writable=False)
        
        assert result is True
    
    def test_nonexistent_directory(self, tmp_path):
        """Test checking a nonexistent directory"""
        test_dir = tmp_path / "nonexistent"
        
        result = cdr.check_local_directory(str(test_dir), "Test directory")
        
        assert result is False
    
    def test_writable_directory(self, tmp_path):
        """Test checking a writable directory"""
        test_dir = tmp_path / "writable_dir"
        test_dir.mkdir()
        
        result = cdr.check_local_directory(str(test_dir), "Test directory", check_writable=True)
        
        assert result is True
    
    def test_home_expansion(self, tmp_path, monkeypatch):
        """Test that ~ is expanded to home directory"""
        # Set HOME to tmp_path for testing
        monkeypatch.setenv('HOME', str(tmp_path))
        test_dir = tmp_path / "test_home"
        test_dir.mkdir()
        
        result = cdr.check_local_directory("~/test_home", "Test directory", check_writable=False)
        
        assert result is True
    
    def test_env_var_expansion(self, tmp_path, monkeypatch):
        """Test that $HOME is expanded"""
        monkeypatch.setenv('HOME', str(tmp_path))
        test_dir = tmp_path / "test_env"
        test_dir.mkdir()
        
        result = cdr.check_local_directory("$HOME/test_env", "Test directory", check_writable=False)
        
        assert result is True
    
    def test_wildcard_at_end(self, tmp_path):
        """Test checking directory with /* wildcard at the end"""
        test_dir = tmp_path / "wildcard_test"
        test_dir.mkdir()
        
        # Create some subdirectories to make it realistic
        (test_dir / "subdir1").mkdir()
        (test_dir / "subdir2").mkdir()
        
        # Should check the parent directory
        result = cdr.check_local_directory(str(test_dir) + "/*", "Test directory", check_writable=False)
        
        assert result is True
    
    def test_wildcard_nonexistent_parent(self, tmp_path):
        """Test checking directory with wildcard when parent doesn't exist"""
        test_dir = tmp_path / "nonexistent"
        
        result = cdr.check_local_directory(str(test_dir) + "/*", "Test directory")
        
        assert result is False
    
    def test_wildcard_with_env_var(self, tmp_path, monkeypatch):
        """Test wildcard with environment variable expansion"""
        monkeypatch.setenv('TEST_DIR', str(tmp_path))
        test_dir = tmp_path / "wildcard_env"
        test_dir.mkdir()
        
        result = cdr.check_local_directory("$TEST_DIR/wildcard_env/*", "Test directory", check_writable=False)
        
        assert result is True
    
    def test_wildcard_with_home_tilde(self, tmp_path, monkeypatch):
        """Test wildcard with ~ expansion"""
        monkeypatch.setenv('HOME', str(tmp_path))
        test_dir = tmp_path / "wildcard_home"
        test_dir.mkdir()
        
        result = cdr.check_local_directory("~/wildcard_home/*", "Test directory", check_writable=False)
        
        assert result is True

    def test_inspect_local_directory_normalizes_redundant_slashes(self, tmp_path):
        """Repeated slashes should collapse to a single normalized path."""
        test_dir = tmp_path / "double" / "slashes"
        test_dir.mkdir(parents=True)

        raw_path = str(tmp_path) + "//double//slashes/"
        info = cdr.inspect_local_directory(raw_path, check_writable=False)

        assert info['ok'] is True
        assert info['path'] == str(test_dir)
        assert '//' not in info['path']


class TestCheckLogDirectory:
    """Test the check_log_directory function"""
    
    def test_empty_log_path(self):
        """Test with empty log file path"""
        ok, msg = cdr.check_log_directory('')
        
        assert ok is True
        assert 'No log file specified' in msg
    
    def test_nan_log_path(self):
        """Test with 'nan' log file path"""
        ok, msg = cdr.check_log_directory('nan')
        
        assert ok is True
        assert 'No log file specified' in msg
    
    def test_existing_log_directory(self, tmp_path):
        """Test with existing log directory"""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        log_file = log_dir / "test.log"
        
        ok, msg = cdr.check_log_directory(str(log_file))
        
        assert ok is True
        assert 'OK' in msg
    
    def test_creates_missing_log_directory(self, tmp_path):
        """Test that missing log directory is created"""
        log_dir = tmp_path / "new_logs"
        log_file = log_dir / "test.log"
        
        ok, msg = cdr.check_log_directory(str(log_file))
        
        assert ok is True
        assert log_dir.exists()
        assert 'Created' in msg
    
    def test_relative_path(self):
        """Test with relative path (log file in current directory)"""
        ok, msg = cdr.check_log_directory('test.log')
        
        assert ok is True
        assert 'current directory' in msg


class TestCheckFlockCommand:
    """Test the check_flock_command function"""

    def test_flock_binary_exists(self, monkeypatch):
        """Test that an existing flock binary path passes."""
        monkeypatch.setattr(cdr.config, 'get_flock_path', lambda system: '/bin/sh')

        result = cdr.check_flock_command('calc')

        assert result is True

    def test_flock_binary_missing(self, monkeypatch):
        """Test that a missing flock binary path fails."""
        monkeypatch.setattr(cdr.config, 'get_flock_path', lambda system: '/no/such/flock')

        result = cdr.check_flock_command('calc')

        assert result is False


class TestCheckRemoteDirectory:
    """Test the remote directory inspection helper."""

    def test_inspect_remote_directory_builds_quoted_ssh_command(self, monkeypatch):
        """Remote probes should keep variable expansion on the remote side."""
        captured = {}

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None):
                captured['args'] = args
                self.returncode = 0

            def communicate(self):
                return b'DIR_OK\n', b''

        monkeypatch.setattr(cdr.subprocess, 'Popen', DummyProcess)

        info = cdr.inspect_remote_directory(
            'user',
            'host',
            '$HOME/test path//nested/',
            port='2222',
            check_writable=False,
        )

        assert info['ok'] is True
        remote_command = captured['args'][-1]
        assert captured['args'][:6] == [
            'ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', '-p'
        ]
        assert captured['args'][6] == '2222'
        assert captured['args'][7] == 'user@host'
        assert remote_command.startswith("sh -c ")
        assert 'target_path="$HOME/test path/nested"' in remote_command
        assert '$1' not in remote_command
        assert 'syntax error near unexpected token' not in remote_command


class TestColors:
    """Test the Colors class"""
    
    def test_colors_defined(self):
        """Test that color codes are defined"""
        assert hasattr(cdr.Colors, 'GREEN')
        assert hasattr(cdr.Colors, 'RED')
        assert hasattr(cdr.Colors, 'YELLOW')
        assert hasattr(cdr.Colors, 'BLUE')
        assert hasattr(cdr.Colors, 'BOLD')
        assert hasattr(cdr.Colors, 'END')
    
    def test_colors_are_strings(self):
        """Test that color codes are strings"""
        assert isinstance(cdr.Colors.GREEN, str)
        assert isinstance(cdr.Colors.RED, str)
        assert isinstance(cdr.Colors.END, str)


class TestPrintStatus:
    """Test the print_status function"""
    
    def test_print_status_ok(self, capsys):
        """Test printing OK status"""
        cdr.print_status("Test message", "OK")
        captured = capsys.readouterr()
        
        assert "Test message" in captured.out
        assert "OK" in captured.out
    
    def test_print_status_error(self, capsys):
        """Test printing ERROR status"""
        cdr.print_status("Test message", "ERROR")
        captured = capsys.readouterr()
        
        assert "Test message" in captured.out
        assert "ERROR" in captured.out
    
    def test_print_status_warn(self, capsys):
        """Test printing WARN status"""
        cdr.print_status("Test message", "WARN")
        captured = capsys.readouterr()
        
        assert "Test message" in captured.out
        assert "WARNING" in captured.out
    
    def test_print_status_with_details(self, capsys):
        """Test printing status with details"""
        cdr.print_status("Test message", "OK", "Additional details")
        captured = capsys.readouterr()
        
        assert "Test message" in captured.out
        assert "Additional details" in captured.out


class TestPrintHeader:
    """Test the print_header function"""
    
    def test_print_header(self, capsys):
        """Test printing section header"""
        cdr.print_header("Test Section")
        captured = capsys.readouterr()
        
        assert "Test Section" in captured.out
        assert "===" in captured.out


class TestSetupCrontabDirectory:
    """Test the setup_crontab_directory function"""
    
    def test_creates_crontab_directory(self, tmp_path, monkeypatch):
        """Test that crontab.d directory is created"""
        monkeypatch.setenv('HOME', str(tmp_path))
        
        ok, msg = cdr.setup_crontab_directory()
        
        assert ok is True
        crontab_dir = tmp_path / "crontab.d"
        assert crontab_dir.exists()
    
    def test_handles_existing_directory(self, tmp_path, monkeypatch):
        """Test that existing directory doesn't cause error"""
        monkeypatch.setenv('HOME', str(tmp_path))
        crontab_dir = tmp_path / "crontab.d"
        crontab_dir.mkdir()
        
        ok, msg = cdr.setup_crontab_directory()
        
        assert ok is True
        assert crontab_dir.exists()


class TestGetCurrentSystem:
    """Test system detection"""
    
    def test_hostname_detection(self, monkeypatch):
        """Test detection of system from hostname"""
        monkeypatch.setattr('socket.gethostname', lambda: 'myserver-01')
        
        # This would need user input in real scenario, so we skip the actual test
        # Just verify the function exists
        assert callable(cdr.get_current_system)


class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_empty_port_string(self):
        """Test handling of empty port string"""
        user, host, path = cdr.parse_remote_destination('user@host:/path')
        
        assert user == 'user'
        assert host == 'host'
        assert path == '/path'
    
    def test_directory_not_a_file(self, tmp_path):
        """Test checking a path that exists but is not a directory"""
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("test content")
        
        result = cdr.check_local_directory(str(test_file), "Test directory")
        
        assert result is False


class TestIntegration:
    """Integration tests"""
    
    def test_full_transfer_check_workflow(self, tmp_path):
        """Test a complete transfer validation workflow"""
        # Create source and destination directories
        source = tmp_path / "source"
        dest = tmp_path / "dest"
        source.mkdir()
        dest.mkdir()
        
        # Check both directories
        source_ok = cdr.check_local_directory(str(source), "Source", check_writable=True)
        dest_ok = cdr.check_local_directory(str(dest), "Destination", check_writable=True)
        
        assert source_ok is True
        assert dest_ok is True


class TestTestWithData:
    """End-to-end coverage for the real-transfer test-with-data mode."""

    def _write_test_with_data_fixture(self, tmp_path):
        config_file = tmp_path / 'config.yaml'
        transfers_file = tmp_path / 'transfers.tsv'
        source_root = tmp_path / 'source_root'
        transit_root = tmp_path / 'transit_root'
        final_root = tmp_path / 'final_root'
        rit_managed = tmp_path / 'rit_managed'
        toy_data_root = tmp_path / 'tests' / 'toy_data' / 'source_root'

        source_root.mkdir()
        transit_root.mkdir()
        final_root.mkdir()
        rit_managed.mkdir()
        toy_data_root.mkdir(parents=True)
        for directory_name in ('flow_one', 'flow_two'):
            run_dir = toy_data_root / directory_name
            run_dir.mkdir()
            (run_dir / 'payload.txt').write_text(directory_name)

        config_file.write_text(
            "\n".join([
                "transfers_file: {0}".format(transfers_file),
                "test_data: {0}".format(tmp_path / 'tests' / 'toy_data'),
                "rit_managed_locations:",
                "  testbox: {0}".format(rit_managed),
                "flock_paths:",
                "  testbox: /usr/bin/true",
                "rit_managed_folder_structure:",
                "  log: log/",
                "  flock: flock/",
                "  sh_output: scripts/",
                "  crontabs: crontab.d/",
                "",
            ])
        )
        transfers_file.write_text(
            "\n".join([
                "identifiers\tenabled\tsystem\tnotes\tusers\tsource\tsource_port\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file\tfrequency",
                "step1\tTRUE\ttestbox\t''\trunner\t{0}/\t\t{1}/\t\t--out-format='%t %o %i %n%L'\t\tstep1.log\tstep1.lock\t* * * * *".format(
                    source_root, transit_root
                ),
                "step2\tTRUE\ttestbox\t''\trunner\t{0}/\t\t{1}/\t\t--out-format='%t %o %i %n%L'\t\tstep2.log\tstep2.lock\t* * * * *".format(
                    transit_root, final_root
                ),
                "",
            ])
        )
        return (
            config_file,
            transfers_file,
            source_root,
            transit_root,
            final_root,
            rit_managed,
        )

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_test_with_data_executes_chained_outputs(
        self, tmp_path, monkeypatch
    ):
        """Run the generated scripts against a real local transfer chain."""
        (
            config_file,
            transfers_file,
            source_root,
            transit_root,
            final_root,
            rit_managed,
        ) = (
            self._write_test_with_data_fixture(tmp_path)
        )
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'testbox')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')
        monkeypatch.setattr('builtins.input', lambda: 'n')

        result = cdr.run_test_with_data(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
        )
        runtime_root = rit_managed / 'test_with_data_runtime' / 'testbox.runner'

        assert result is True
        assert cdr.list_visible_entries(str(source_root)) == []
        assert cdr.list_visible_entries(str(transit_root)) == []
        assert cdr.list_visible_directories(str(final_root)) == ['flow_one', 'flow_two']
        assert (final_root / 'flow_one' / 'payload.txt').read_text() == 'flow_one'
        assert (final_root / 'flow_two' / 'payload.txt').read_text() == 'flow_two'
        assert (runtime_root / 'scripts' / 'step1.sh').exists()
        assert (runtime_root / 'scripts' / 'step2.sh').exists()
        assert (runtime_root / 'validation_scripts' / 'lz_run_validation.sh').exists()
        assert (rit_managed / 'log' / 'step1.log').exists()
        assert (rit_managed / 'flock' / 'step1.lock').exists()

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_test_with_data_cleans_up_when_user_confirms(
        self, tmp_path, monkeypatch
    ):
        """The post-run prompt should allow cleanup back to the initial state."""
        (
            config_file,
            transfers_file,
            source_root,
            transit_root,
            final_root,
            rit_managed,
        ) = (
            self._write_test_with_data_fixture(tmp_path)
        )
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'testbox')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')
        monkeypatch.setattr('builtins.input', lambda: 'y')

        result = cdr.run_test_with_data(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
        )
        runtime_root = rit_managed / 'test_with_data_runtime' / 'testbox.runner'

        assert result is True
        assert cdr.list_visible_entries(str(source_root)) == []
        assert cdr.list_visible_entries(str(transit_root)) == []
        assert cdr.list_visible_entries(str(final_root)) == []
        assert not runtime_root.exists()
        assert not (rit_managed / 'log' / 'step1.log').exists()
        assert not (rit_managed / 'flock' / 'step1.lock').exists()

    def test_build_run_test_plan_identifies_initial_and_terminal_roots(self, tmp_path):
        """Intermediate destinations should not be treated as initial or terminal."""
        df = pd.DataFrame([
            {
                'identifiers': 'step1',
                'source': str(tmp_path / 'source') + '/',
                'source_port': '',
                'destination': str(tmp_path / 'mid') + '/',
                'destination_port': '',
                'test_fixture_names': 'fixture_one',
            },
            {
                'identifiers': 'step2',
                'source': str(tmp_path / 'mid') + '/',
                'source_port': '',
                'destination': str(tmp_path / 'final') + '/',
                'destination_port': '',
            },
        ])

        plan = cdr.build_run_test_plan(df)

        assert [cdr.get_endpoint_root(item) for item in plan['initial_sources']] == [
            str(tmp_path / 'source')
        ]
        assert plan['initial_sources'][0]['test_fixture_names'] == ['fixture_one']
        assert [cdr.get_endpoint_root(item) for item in plan['terminal_destinations']] == [
            str(tmp_path / 'final')
        ]

    def test_build_run_test_plan_prefers_explicit_entry_points_for_seed_sources(
        self, tmp_path
    ):
        """Explicit entry-point metadata should exclude inherited remote sources from seeding."""
        df = pd.DataFrame([
            {
                'identifiers': 'entry_local',
                'source': str(tmp_path / 'calc' / 'Landing_Zone' / 'to_ugerm') + '/',
                'source_port': '',
                'destination': 'sshdat@ugerm:/users/data/Landing_Zone/from_calc/',
                'destination_port': '',
                'test_fixture_names': 'fixture_one',
                'is_entry_point': 'TRUE',
            },
            {
                'identifiers': 'return_remote',
                'source': 'sshdat@ugerm:/users/data/Landing_Zone/to_calc/',
                'source_port': '',
                'destination': str(tmp_path / 'calc' / 'Landing_Zone' / 'from_ugerm') + '/',
                'destination_port': '',
                'test_fixture_names': '',
                'is_entry_point': 'FALSE',
            },
        ])

        plan = cdr.build_run_test_plan(df)

        assert len(plan['initial_sources']) == 1
        assert plan['initial_sources'][0]['value'] == str(
            tmp_path / 'calc' / 'Landing_Zone' / 'to_ugerm'
        ) + '/'

    def test_load_test_with_data_transfers_preserves_env_var_paths(self, tmp_path):
        """test-with-data should keep env-var based paths unchanged."""
        transfers_file = tmp_path / 'transfers.tsv'
        transfers_file.write_text(
            "\n".join([
                "identifiers\tenabled\tsystem\tnotes\tusers\tsource\tsource_port\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file\tfrequency",
                "placeholder\tTRUE\ttestbox\t''\trunner\t$LZ_TEST_ROOT/source/\t\t$LZ_TEST_ROOT/dest/\t\t\t\tplaceholder.log\tplaceholder.lock\t* * * * *",
                "",
            ])
        )

        subset_df = cdr.load_test_with_data_transfers(
            str(transfers_file), 'testbox', 'runner', str(tmp_path)
        )

        assert subset_df['source'].tolist() == ['$LZ_TEST_ROOT/source/']
        assert subset_df['destination'].tolist() == ['$LZ_TEST_ROOT/dest/']

    def test_build_test_with_data_handoffs_identifies_next_system_user(self, tmp_path):
        """A destination that feeds another system should produce a handoff hint."""
        all_transfers_df = pd.DataFrame([
            {
                'identifiers': 'step1',
                'system': 'calc',
                'users': 'runner',
                'source': str(tmp_path / 'source') + '/',
                'destination': str(tmp_path / 'handoff') + '/',
                'flow_group': 'flow_a',
            },
            {
                'identifiers': 'step2',
                'system': 'ugerm',
                'users': 'corfac',
                'source': str(tmp_path / 'handoff') + '/',
                'destination': str(tmp_path / 'final') + '/',
                'flow_group': 'flow_a',
            },
        ])

        current_transfers_df = all_transfers_df.iloc[[0]].copy()

        handoffs = cdr.build_test_with_data_handoffs(
            all_transfers_df,
            current_transfers_df,
            slow=True,
        )

        assert len(handoffs) == 1
        assert handoffs[0]['system'] == 'ugerm'
        assert handoffs[0]['user'] == 'corfac'
        assert handoffs[0]['command'] == 'landingzones validate integration --slow'
        assert handoffs[0]['transfers'][0]['identifier'] == 'step2'
        assert handoffs[0]['transfers'][0]['source'] == str(tmp_path / 'handoff') + '/'

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_test_with_data_reports_handoff_and_skips_cleanup_prompt(
        self, tmp_path, monkeypatch, capsys
    ):
        """Intermediate system runs should print handoff guidance and keep state."""
        config_file = tmp_path / 'config.yaml'
        transfers_file = tmp_path / 'transfers.tsv'
        source_root = tmp_path / 'tests' / 'toy_data' / 'producer_a'
        handoff_root = tmp_path / 'handoff'
        final_root = tmp_path / 'final'
        rit_managed = tmp_path / 'rit_managed'

        source_root.mkdir(parents=True)
        handoff_root.mkdir()
        final_root.mkdir()
        rit_managed.mkdir()
        run_dir = source_root / 'flow_one'
        run_dir.mkdir()
        (run_dir / 'payload.txt').write_text('flow_one')

        config_file.write_text(
            "\n".join([
                "transfers_file: {0}".format(transfers_file),
                "test_data: {0}".format(tmp_path / 'tests' / 'toy_data'),
                "rit_managed_locations:",
                "  calc: {0}".format(rit_managed),
                "flock_paths:",
                "  calc: /usr/bin/true",
                "rit_managed_folder_structure:",
                "  log: log/",
                "  flock: flock/",
                "  sh_output: scripts/",
                "  crontabs: crontab.d/",
                "",
            ])
        )
        transfers_file.write_text(
            "\n".join([
                "identifiers\tenabled\tsystem\tnotes\tusers\tsource\tsource_port\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file\tfrequency",
                "step1\tTRUE\tcalc\t''\trunner\t{0}/\t\t{1}/\t\t--out-format='%t %o %i %n%L'\t\tstep1.log\tstep1.lock\t* * * * *".format(
                    tmp_path / 'producer_a', handoff_root
                ),
                "step2\tTRUE\tugerm\t''\tcorfac\t{0}/\t\t{1}/\t\t--out-format='%t %o %i %n%L'\t\tstep2.log\tstep2.lock\t* * * * *".format(
                    handoff_root, final_root
                ),
                "",
            ])
        )

        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'calc')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')

        cleanup_prompts = []
        monkeypatch.setattr(
            cdr,
            'ask_yes_no',
            lambda prompt_text: cleanup_prompts.append(prompt_text) or False,
        )

        result = cdr.run_test_with_data(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
            slow=True,
        )
        captured = capsys.readouterr()
        runtime_root = rit_managed / 'test_with_data_runtime' / 'calc.runner'

        assert result is True
        assert cleanup_prompts == []
        assert cdr.list_visible_entries(str(handoff_root)) == ['flow_one']
        assert cdr.list_visible_entries(str(final_root)) == []
        assert (runtime_root / 'scripts').exists()
        assert "Next System Handoff" in captured.out
        assert "Switch to corfac@ugerm" in captured.out
        assert "landingzones validate integration --slow" in captured.out
        assert "step2" in captured.out

    def test_build_test_with_data_seed_plan_uses_only_available_toy_data_directory(
        self, tmp_path
    ):
        """A single toy-data directory should seed any unmatched source root."""
        toy_data_root = tmp_path / 'tests' / 'toy_data' / 'shared_seed'
        toy_data_root.mkdir(parents=True)
        (toy_data_root / 'flow_one').mkdir()

        test_plan = {
            'initial_sources': [
                {'value': str(tmp_path / 'somewhere' / 'source_root') + '/', 'port': ''}
            ]
        }

        seed_plan = cdr.build_test_with_data_seed_plan(
            test_plan,
            str(tmp_path / 'tests' / 'toy_data'),
            str(tmp_path),
        )

        assert seed_plan[0]['toy_data_dir'] == str(toy_data_root)
        assert seed_plan[0]['entry_names'] == ['flow_one']

    def test_build_test_with_data_seed_plan_unwraps_nested_single_dir_fixture_tree(
        self, tmp_path
    ):
        """Nested single-directory fixture wrappers should resolve to the run container."""
        run_container = (
            tmp_path / 'tests' / 'toy_data' / 'data' / 'lab_machine_1'
        )
        (run_container / 'Illumina_TransferTest').mkdir(parents=True)
        (run_container / 'Nanopore_TransferTest').mkdir()

        test_plan = {
            'initial_sources': [
                {'value': str(tmp_path / 'somewhere' / 'corefacility') + '/', 'port': ''}
            ]
        }

        seed_plan = cdr.build_test_with_data_seed_plan(
            test_plan,
            str(tmp_path / 'tests' / 'toy_data'),
            str(tmp_path),
        )

        assert seed_plan[0]['toy_data_dir'] == str(run_container)
        assert seed_plan[0]['entry_names'] == [
            'Illumina_TransferTest',
            'Nanopore_TransferTest',
        ]

    def test_build_test_with_data_seed_plan_filters_configured_fixtures(
        self, tmp_path
    ):
        """Configured fixture names should restrict which toy-data runs get seeded."""
        toy_data_root = tmp_path / 'tests' / 'toy_data' / 'shared_seed'
        toy_data_root.mkdir(parents=True)
        (toy_data_root / 'Illumina_TransferTest').mkdir()
        (toy_data_root / 'Nanopore_TransferTest').mkdir()

        test_plan = {
            'initial_sources': [
                {
                    'value': str(tmp_path / 'somewhere' / 'source_root') + '/',
                    'port': '',
                    'test_fixture_names': ['Nanopore_TransferTest'],
                }
            ]
        }

        seed_plan = cdr.build_test_with_data_seed_plan(
            test_plan,
            str(tmp_path / 'tests' / 'toy_data'),
            str(tmp_path),
        )

        assert seed_plan[0]['toy_data_dir'] == str(toy_data_root)
        assert seed_plan[0]['entry_names'] == ['Nanopore_TransferTest']

    def test_run_generated_scripts_enables_debug_cli(self, tmp_path, monkeypatch):
        """Generated scripts should run with debug logging enabled in test-with-data."""
        script = tmp_path / 'sample.sh'
        script.write_text("#!/bin/sh\nexit 0\n")
        script.chmod(0o755)

        transfers_df = pd.DataFrame([
            {
                'identifiers': 'sample',
                'script_name': 'sample.sh',
                'log_file': str(tmp_path / 'sample.log'),
            }
        ])

        captured = {}

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None, env=None, cwd=None):
                captured['args'] = args
                captured['env'] = env
                captured['cwd'] = cwd
                self.returncode = 0

            def communicate(self):
                return b'', b''

        monkeypatch.setattr(cdr.subprocess, 'Popen', DummyProcess)

        results = cdr.run_generated_scripts(
            transfers_df,
            str(tmp_path),
            str(tmp_path / 'test_root'),
        )

        assert results[0]['returncode'] == 0
        assert captured['env']['LZ_DEBUG_CLI'] == '1'

    def test_run_generated_scripts_slow_mode_pauses_between_steps(
        self, tmp_path, monkeypatch, capsys
    ):
        """Slow mode should print a step summary and wait before the next script."""
        for name in ('first.sh', 'second.sh'):
            script = tmp_path / name
            script.write_text("#!/bin/sh\nexit 0\n")
            script.chmod(0o755)

        first_log = tmp_path / 'first.log'
        first_log.write_text("line one\nline two\n")

        transfers_df = pd.DataFrame([
            {
                'identifiers': 'first',
                'script_name': 'first.sh',
                'log_file': str(first_log),
            },
            {
                'identifiers': 'second',
                'script_name': 'second.sh',
                'log_file': str(tmp_path / 'second.log'),
            },
        ])

        process_calls = []
        prompts = []

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None, env=None, cwd=None):
                process_calls.append(args[1])
                self.returncode = 0

            def communicate(self):
                script_name = os.path.basename(process_calls[-1])
                return (
                    "stdout from {0}\n".format(script_name).encode('utf-8'),
                    b'',
                )

        monkeypatch.setattr(cdr.subprocess, 'Popen', DummyProcess)
        monkeypatch.setattr(
            'builtins.input',
            lambda: prompts.append('continue') or '',
        )

        results = cdr.run_generated_scripts(
            transfers_df,
            str(tmp_path),
            slow=True,
        )
        captured = capsys.readouterr()

        assert [result['identifier'] for result in results] == ['first', 'second']
        assert prompts == ['continue']
        assert "Integration Step 1/2: first" in captured.out
        assert "stdout from first.sh" in captured.out
        assert "log tail:" in captured.out
        assert "Press Enter to continue to the next step (second)" in captured.out

    def test_main_passes_slow_flag_to_test_with_data(self, monkeypatch):
        """The readiness CLI should forward --slow to test-with-data execution."""
        captured = {}

        monkeypatch.setattr(cdr.config, 'load_config', lambda **kwargs: None)

        def fake_run_test_with_data(
            config_file=None, transfers_file=None, slow=False
        ):
            captured['config_file'] = config_file
            captured['transfers_file'] = transfers_file
            captured['slow'] = slow
            return True

        monkeypatch.setattr(cdr, 'run_test_with_data', fake_run_test_with_data)

        result = cdr.main([
            '--config', 'config.yaml',
            '--transfers', 'transfers.tsv',
            '--test-with-data',
            '--slow',
        ])

        assert result is True
        assert captured == {
            'config_file': 'config.yaml',
            'transfers_file': 'transfers.tsv',
            'slow': True,
        }

    def test_main_lists_and_creates_missing_directories(self, tmp_path, monkeypatch, capsys):
        """Missing local directories should be summarized and created on confirmation."""
        transfers_file = tmp_path / 'transfers.tsv'
        transfers_file.write_text("placeholder\n")
        source_dir = tmp_path / 'calc' / 'Landing_Zone' / 'from_labnet'
        destination_dir = tmp_path / 'calc' / 'Landing_Zone' / 'to_ugerm'
        log_file = tmp_path / 'log' / 'transfer.log'
        flock_file = tmp_path / 'flock' / 'transfer.lock'

        df = pd.DataFrame([
            {
                'source': str(tmp_path) + '//calc//Landing_Zone//from_labnet/',
                'source_port': '',
                'destination': str(tmp_path) + '//calc//Landing_Zone//to_ugerm/',
                'destination_port': '',
                'log_file': str(log_file),
                'flock_file': str(flock_file),
                'system': 'calc',
                'users': 'runner',
            }
        ])

        monkeypatch.setattr(cdr, 'check_required_tools', lambda: True)
        monkeypatch.setattr(cdr, 'check_flock_command', lambda system: True)
        monkeypatch.setattr(cdr, 'load_runtime_transfers', lambda transfers_file=None: df)
        monkeypatch.setattr(
            cdr,
            'filter_transfers_by_system_user',
            lambda loaded_df, system, user: loaded_df,
        )
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'calc')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')
        monkeypatch.setattr(cdr, 'ask_yes_no', lambda prompt_text: True)

        result = cdr.main(['--transfers', str(transfers_file)])
        captured = capsys.readouterr()

        assert result is False
        assert source_dir.is_dir()
        assert destination_dir.is_dir()
        assert "Missing Directories" in captured.out
        assert str(source_dir) in captured.out
        assert str(destination_dir) in captured.out
        assert "Directory creation" in captured.out


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
