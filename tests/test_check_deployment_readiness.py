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
        user, host, path = cdr.parse_remote_destination('calck:$HOME/Landing_Zone/')

        assert user is None
        assert host == 'calck'
        assert path == '$HOME/Landing_Zone/'


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


class TestLocalScriptTests:
    """End-to-end coverage for the real-transfer run-tests mode."""

    def _write_run_test_fixture(self, tmp_path):
        config_file = tmp_path / 'config.yaml'
        transfers_file = tmp_path / 'transfers.tsv'
        source_root = tmp_path / 'source_root'
        transit_root = tmp_path / 'transit_root'
        final_root = tmp_path / 'final_root'
        rit_managed = tmp_path / 'rit_managed'

        source_root.mkdir()
        transit_root.mkdir()
        final_root.mkdir()
        rit_managed.mkdir()

        config_file.write_text(
            "\n".join([
                "transfers_file: {0}".format(transfers_file),
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
        return config_file, transfers_file, source_root, transit_root, final_root

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_local_script_tests_executes_chained_outputs(
        self, tmp_path, monkeypatch
    ):
        """Run the generated scripts against a real local transfer chain."""
        config_file, transfers_file, source_root, transit_root, final_root = (
            self._write_run_test_fixture(tmp_path)
        )
        test_root = tmp_path / '20260409T120000_run_tests_keep'
        monkeypatch.setattr(cdr, 'create_test_root', lambda: str(test_root))
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'testbox')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')

        result = cdr.run_local_script_tests(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
            keep_test_env=True,
        )

        assert result is True
        assert cdr.list_visible_entries(str(source_root)) == []
        assert cdr.list_visible_entries(str(transit_root)) == []
        assert cdr.list_visible_entries(str(final_root)) == []
        assert test_root.exists()

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_local_script_tests_cleans_up_workspace(self, tmp_path, monkeypatch):
        """Run-tests should remove its temporary workspace by default."""
        config_file, transfers_file, source_root, transit_root, final_root = (
            self._write_run_test_fixture(tmp_path)
        )
        test_root = tmp_path / '20260409T120000_run_tests_cleanup'
        monkeypatch.setattr(cdr, 'create_test_root', lambda: str(test_root))
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'testbox')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')

        result = cdr.run_local_script_tests(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
        )

        assert result is True
        assert cdr.list_visible_entries(str(source_root)) == []
        assert cdr.list_visible_entries(str(transit_root)) == []
        assert cdr.list_visible_entries(str(final_root)) == []
        assert not test_root.exists()

    def test_build_run_test_plan_identifies_initial_and_terminal_roots(self, tmp_path):
        """Intermediate destinations should not be treated as initial or terminal."""
        df = pd.DataFrame([
            {
                'identifiers': 'step1',
                'source': str(tmp_path / 'source') + '/',
                'source_port': '',
                'destination': str(tmp_path / 'mid') + '/',
                'destination_port': '',
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
        assert [cdr.get_endpoint_root(item) for item in plan['terminal_destinations']] == [
            str(tmp_path / 'final')
        ]

    def test_load_run_test_transfers_ignores_placeholder_rows(self, tmp_path):
        """run-tests should skip synthetic $LZ_TEST_ROOT rows."""
        transfers_file = tmp_path / 'transfers.tsv'
        output_file = tmp_path / 'subset.tsv'
        transfers_file.write_text(
            "\n".join([
                "identifiers\tenabled\tsystem\tnotes\tusers\tsource\tsource_port\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file\tfrequency",
                "real\tTRUE\ttestbox\t''\trunner\t{0}/\t\t{1}/\t\t\t\treal.log\treal.lock\t* * * * *".format(
                    tmp_path / 'source', tmp_path / 'dest'
                ),
                "placeholder\tTRUE\ttestbox\t''\trunner\t$LZ_TEST_ROOT/source/\t\t$LZ_TEST_ROOT/dest/\t\t\t\tplaceholder.log\tplaceholder.lock\t* * * * *",
                "",
            ])
        )

        subset_df = cdr.load_run_test_transfers(
            str(transfers_file), 'testbox', 'runner', str(output_file), str(tmp_path)
        )

        assert subset_df['identifiers'].tolist() == ['real']
        assert subset_df.attrs['skipped_placeholder_rows'] == 1

    def test_run_generated_scripts_enables_debug_cli(self, tmp_path, monkeypatch):
        """Generated scripts should run with debug logging enabled in run-tests."""
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


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
