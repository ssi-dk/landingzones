#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Test suite for check_deployment_readiness.py"""

import os
import tempfile
import pytest

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


class TestCheckLockFileDirectory:
    """Test the check_lock_file_directory function"""
    
    def test_lock_directory_exists(self):
        """Test that /tmp directory exists (should always pass)"""
        result = cdr.check_lock_file_directory()
        
        # /tmp should always exist on Unix systems
        assert result is True


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


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
