#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Test suite for generate_cron_files.py"""

import os
import tempfile
import shutil
import pytest

from landingzones import generate_cron_files as gcf


class TestParseTransfersFile:
    """Test the parse_transfers_file function"""
    
    def test_parse_valid_tsv(self, tmp_path):
        """Test parsing a valid TSV file"""
        tsv_content = """system\tusers\tsource\tdestination\tdestination_port\trsync_options\tlog_file\tflock_file
server1\tuser1\t/srv/data/src/\tuser@host:/dest/\t22\t-av\t/tmp/log.txt\t/tmp/lock.txt
localhost\ttest\t/src/\t/dest/\t\t\t/tmp/test.log\t/tmp/test.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)
        
        df = gcf.parse_transfers_file(str(test_file))
        
        assert len(df) == 2
        assert df.iloc[0]['system'] == 'server1'
        assert df.iloc[0]['users'] == 'user1'
        assert df.iloc[1]['system'] == 'localhost'
    
    def test_parse_filters_comments(self, tmp_path):
        """Test that lines starting with # are filtered out"""
        tsv_content = """system\tusers\tsource\tdestination\tdestination_port\trsync_options\tlog_file\tflock_file
server1\tuser1\t/srv/data/src/\tuser@host:/dest/\t\t\t/tmp/log.txt\t/tmp/lock.txt
#commented\tuser\t/src/\t/dest/\t\t\t/tmp/log.txt\t/tmp/log.txt
localhost\ttest\t/src/\t/dest/\t\t\t/tmp/test.log\t/tmp/test.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)
        
        df = gcf.parse_transfers_file(str(test_file))
        
        assert len(df) == 2
        assert 'commented' not in df['system'].values
    
    def test_parse_filters_disabled_rows(self, tmp_path):
        """Test that rows with enabled != TRUE are filtered out"""
        tsv_content = """enabled\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tlog_file\tflock_file
TRUE\tserver1\tuser1\t/srv/data/src/\tuser@host:/dest/\t\t\t/tmp/log.txt\t/tmp/lock.txt
FALSE\tserver2\tuser2\t/srv/data/src2/\tuser@host:/dest2/\t\t\t/tmp/log2.txt\t/tmp/lock2.txt
TRUE\tlocalhost\ttest\t/src/\t/dest/\t\t\t/tmp/test.log\t/tmp/test.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)
        
        df = gcf.parse_transfers_file(str(test_file))
        
        assert len(df) == 2
        assert 'server2' not in df['system'].values
        assert 'server1' in df['system'].values
        assert 'localhost' in df['system'].values
    
    def test_parse_enabled_case_insensitive(self, tmp_path):
        """Test that enabled column is case insensitive"""
        tsv_content = """enabled\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tlog_file\tflock_file
true\tserver1\tuser1\t/srv/data/src/\tuser@host:/dest/\t\t\t/tmp/log.txt\t/tmp/lock.txt
True\tserver2\tuser2\t/srv/data/src2/\tuser@host:/dest2/\t\t\t/tmp/log2.txt\t/tmp/lock2.txt
FALSE\tserver3\tuser3\t/src/\t/dest/\t\t\t/tmp/test.log\t/tmp/test.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)
        
        df = gcf.parse_transfers_file(str(test_file))
        
        assert len(df) == 2
        assert 'server1' in df['system'].values
        assert 'server2' in df['system'].values
        assert 'server3' not in df['system'].values
    
    def test_parse_without_enabled_column(self, tmp_path):
        """Test that parsing works when enabled column is absent (backward compatibility)"""
        tsv_content = """system\tusers\tsource\tdestination\tdestination_port\trsync_options\tlog_file\tflock_file
server1\tuser1\t/srv/data/src/\tuser@host:/dest/\t\t\t/tmp/log.txt\t/tmp/lock.txt
localhost\ttest\t/src/\t/dest/\t\t\t/tmp/test.log\t/tmp/test.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)
        
        df = gcf.parse_transfers_file(str(test_file))
        
        # All rows should be included when enabled column is not present
        assert len(df) == 2
        assert 'server1' in df['system'].values
        assert 'localhost' in df['system'].values


class TestGenerateRsyncCommand:
    """Test the generate_rsync_command function"""
    
    def test_basic_rsync_command(self):
        """Test basic rsync command generation"""
        transfer = {
            'source': '/source/path/',
            'source_port': '',
            'destination': '/dest/path/',
            'destination_port': '',
            'rsync_options': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        assert '/usr/bin/flock' in cmd
        assert '/tmp/test.lock' in cmd
        assert 'rsync' in cmd
        assert '-av' in cmd
        assert '--remove-source-files' in cmd
        assert '/source/path/' in cmd
        assert '/dest/path/' in cmd
        assert '/tmp/test.log' in cmd
        # Default frequency should be used
        assert '*/15 * * * *' in cmd
    
    def test_rsync_with_ssh_port(self):
        """Test rsync command with SSH port"""
        transfer = {
            'source': '/source/',
            'source_port': '',
            'destination': 'user@host:/dest/',
            'destination_port': '2222',
            'rsync_options': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '*/5 * * * *'
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        assert '-e "ssh -p 2222"' in cmd or "-e 'ssh -p 2222'" in cmd
        assert 'user@host:/dest/' in cmd
        # Custom frequency should be used
        assert '*/5 * * * *' in cmd
    
    def test_rsync_with_source_ssh_port(self):
        """Test rsync command with SSH port on source"""
        transfer = {
            'source': 'user@remote:/source/',
            'source_port': '2222',
            'destination': '/local/dest/',
            'destination_port': '',
            'rsync_options': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '*/5 * * * *'
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        # Should use source port when pulling from remote
        assert '-e "ssh -p 2222"' in cmd or "-e 'ssh -p 2222'" in cmd
        assert 'user@remote:/source/' in cmd
        assert '/local/dest/' in cmd
    
    def test_rsync_with_custom_options(self):
        """Test rsync command with custom options"""
        transfer = {
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '--chown=:group --chmod=Du=rwx',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '0 * * * *'
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        assert '--chown=:group' in cmd
        assert '--chmod=Du=rwx' in cmd
        # Hourly frequency should be used
        assert '0 * * * *' in cmd
    
    def test_rsync_validates_flock_file(self):
        """Test that rsync command requires flock_file"""
        transfer = {
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'log_file': '/tmp/test.log',
            'flock_file': '',
            'frequency': ''
        }
        
        with pytest.raises(ValueError) as exc_info:
            gcf.generate_rsync_command(transfer)
        
        assert 'flock_file' in str(exc_info.value).lower()
    
    def test_rsync_with_custom_frequency(self):
        """Test rsync command with custom cron frequency"""
        transfer = {
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '0 0 * * *'  # Daily at midnight
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        # Should use the custom frequency
        assert cmd.startswith('0 0 * * *')
        assert '/usr/bin/flock' in cmd
    
    def test_rsync_default_frequency_when_empty(self):
        """Test that default frequency is used when frequency is empty"""
        transfer = {
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''  # Empty, should use default
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        # Should use the default frequency (*/15 * * * *)
        assert cmd.startswith('*/15 * * * *')
    
    def test_rsync_default_frequency_when_nan(self):
        """Test that default frequency is used when frequency is 'nan'"""
        transfer = {
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': 'nan'  # NaN string, should use default
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        # Should use the default frequency (*/15 * * * *)
        assert cmd.startswith('*/15 * * * *')


class TestGenerateCronHeader:
    """Test the generate_cron_header function"""
    
    def test_basic_header(self):
        """Test basic cron header generation"""
        header = gcf.generate_cron_header('testsystem', 'testuser')
        
        assert 'testsystem' in header
        assert 'testuser' in header
        assert 'SHELL=/bin/sh' in header
        assert 'PATH=' in header
    
    def test_header_contains_instructions(self):
        """Test that header contains usage instructions"""
        header = gcf.generate_cron_header('system', 'user')
        
        # Should contain instructions for activating cron
        assert 'crontab' in header
        assert 'Landing_Zone.cron' in header


class TestGroupTransfersBySystemUser:
    """Test grouping transfers by system and user"""
    
    def test_groups_correctly(self):
        """Test that transfers are grouped by system-user combination"""
        import pandas as pd
        
        data = {
            'system': ['server1', 'server1', 'server2'],
            'users': ['user1', 'user1', 'user2'],
            'source': ['/src1/', '/src2/', '/src3/'],
            'source_port': ['', '', ''],
            'destination': ['/dst1/', '/dst2/', '/dst3/'],
            'destination_port': ['', '', ''],
            'rsync_options': ['', '', ''],
            'log_file': ['/tmp/1.log', '/tmp/2.log', '/tmp/3.log'],
            'flock_file': ['/tmp/1.lock', '/tmp/2.lock', '/tmp/3.lock'],
            'frequency': ['*/15 * * * *', '*/5 * * * *', '0 * * * *']
        }
        df = pd.DataFrame(data)
        
        # Group by system and users
        grouped = df.groupby(['system', 'users'])
        
        assert len(grouped) == 2  # Two unique system-user combinations
        assert ('server1', 'user1') in grouped.groups
        assert ('server2', 'user2') in grouped.groups


class TestCronFileGeneration:
    """Integration tests for full cron file generation"""
    
    def test_generates_cron_file(self, tmp_path):
        """Test that cron files are generated correctly"""
        # Create test TSV
        tsv_content = """system\tusers\tsource\tsource_port\tdestination\tdestination_port\trsync_options\tlog_file\tflock_file
localhost\ttestuser\t/tmp/src/\t\t/tmp/dest/\t\t\t/tmp/test.log\t/tmp/test.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)
        
        # Parse the file
        df = gcf.parse_transfers_file(str(test_file))
        
        # Generate cron content
        system = 'localhost'
        user = 'testuser'
        transfers = df[(df['system'] == system) & (df['users'] == user)]
        
        header = gcf.generate_cron_header(system, user)
        commands = []
        for _, transfer in transfers.iterrows():
            cmd = gcf.generate_rsync_command(transfer.to_dict())
            commands.append(cmd)
        
        cron_content = header + '\n'.join(commands)
        
        assert 'localhost' in cron_content
        assert 'testuser' in cron_content
        assert '/tmp/src/' in cron_content
        assert '/tmp/dest/' in cron_content
        assert 'rsync' in cron_content
        assert 'flock' in cron_content


class TestEnvironmentVariableExpansion:
    """Test that environment variables are handled correctly"""
    
    def test_home_variable_not_expanded_in_output(self):
        """Test that $HOME is preserved in the cron output"""
        transfer = {
            'source': '$HOME/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'log_file': '$HOME/test.log',
            'flock_file': '$HOME/test.lock',
            'frequency': ''
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        # $HOME should be preserved for cron to expand at runtime
        assert '$HOME' in cmd


class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_empty_port_handled(self):
        """Test that empty port is handled correctly"""
        transfer = {
            'source': '/source/',
            'source_port': '',
            'destination': 'user@host:/dest/',
            'destination_port': '',
            'rsync_options': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        # Should not have port specification
        assert '-p ' not in cmd.split('ssh')[1].split('"')[0] if 'ssh' in cmd else True
    
    def test_nan_port_handled(self):
        """Test that NaN port values are handled"""
        transfer = {
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': 'nan',
            'rsync_options': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        # Should not crash and should not include port
        assert cmd is not None
        assert 'nan' not in cmd.lower()


class TestOverlappingSourceDetection:
    """Test detection of overlapping source paths"""
    
    def test_detects_parent_child_overlap(self):
        """Test that overlapping parent/child paths are detected"""
        import pandas as pd
        
        data = {
            'system': ['server1', 'server1'],
            'users': ['user1', 'user1'],
            'source': ['/data/landing/*', '/data/landing/subdir/*'],
            'destination': ['/dest1/', '/dest2/'],
        }
        df = pd.DataFrame(data)
        
        warnings = gcf.check_overlapping_sources(df)
        
        assert len(warnings) == 1
        assert 'Overlapping' in warnings[0]
        assert '/data/landing/*' in warnings[0]
        assert '/data/landing/subdir/*' in warnings[0]
    
    def test_no_warning_for_different_systems(self):
        """Test that overlapping paths on different systems don't warn"""
        import pandas as pd
        
        data = {
            'system': ['server1', 'server2'],
            'users': ['user1', 'user1'],
            'source': ['/data/landing/*', '/data/landing/subdir/*'],
            'destination': ['/dest1/', '/dest2/'],
        }
        df = pd.DataFrame(data)
        
        warnings = gcf.check_overlapping_sources(df)
        
        assert len(warnings) == 0
    
    def test_no_warning_for_non_overlapping_paths(self):
        """Test that non-overlapping paths don't trigger warnings"""
        import pandas as pd
        
        data = {
            'system': ['server1', 'server1'],
            'users': ['user1', 'user1'],
            'source': ['/data/dir1/*', '/data/dir2/*'],
            'destination': ['/dest1/', '/dest2/'],
        }
        df = pd.DataFrame(data)
        
        warnings = gcf.check_overlapping_sources(df)
        
        assert len(warnings) == 0
    
    def test_normalize_source_path(self):
        """Test path normalization for comparison"""
        assert gcf.normalize_source_path('/path/to/dir/*') == '/path/to/dir'
        assert gcf.normalize_source_path('/path/to/dir/') == '/path/to/dir'
        assert gcf.normalize_source_path('/path/to/dir') == '/path/to/dir'
        assert gcf.normalize_source_path('/path/to/dir* ') == '/path/to/dir'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
