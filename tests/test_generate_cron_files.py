#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Test suite for generate_cron_files.py"""

import os
import sys
import tempfile
import shutil
import subprocess
import pytest

from landingzones import generate_cron_files as gcf


class TestParseTransfersFile:
    """Test the parse_transfers_file function"""
    
    def test_parse_valid_tsv(self, tmp_path):
        """Test parsing a valid TSV file"""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\tserver1\tuser1\t/srv/data/src/\tuser@host:/dest/\t22\t-av\t\t/tmp/log.txt\t/tmp/lock.txt
localhost_main\tlocalhost\ttest\t/src/\t/dest/\t\t\t\t/tmp/test.log\t/tmp/test.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)
        
        df = gcf.parse_transfers_file(str(test_file))
        
        assert len(df) == 2
        assert df.iloc[0]['system'] == 'server1'
        assert df.iloc[0]['users'] == 'user1'
        assert df.iloc[0]['identifiers'] == 'server1_main'
        assert df.iloc[1]['system'] == 'localhost'
    
    def test_parse_filters_comments(self, tmp_path):
        """Test that lines starting with # are filtered out"""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\tserver1\tuser1\t/srv/data/src/\tuser@host:/dest/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt
commented\t#commented\tuser\t/src/\t/dest/\t\t\t\t/tmp/log.txt\t/tmp/log.txt
localhost_main\tlocalhost\ttest\t/src/\t/dest/\t\t\t\t/tmp/test.log\t/tmp/test.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)
        
        df = gcf.parse_transfers_file(str(test_file))
        
        assert len(df) == 2
        assert 'commented' not in df['system'].values
    
    def test_parse_filters_disabled_rows(self, tmp_path):
        """Test that rows with enabled != TRUE are filtered out"""
        tsv_content = """identifiers\tenabled\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\tTRUE\tserver1\tuser1\t/srv/data/src/\tuser@host:/dest/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt
server2_main\tFALSE\tserver2\tuser2\t/srv/data/src2/\tuser@host:/dest2/\t\t\t\t/tmp/log2.txt\t/tmp/lock2.txt
localhost_main\tTRUE\tlocalhost\ttest\t/src/\t/dest/\t\t\t\t/tmp/test.log\t/tmp/test.lock
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
        tsv_content = """identifiers\tenabled\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\ttrue\tserver1\tuser1\t/srv/data/src/\tuser@host:/dest/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt
server2_main\tTrue\tserver2\tuser2\t/srv/data/src2/\tuser@host:/dest2/\t\t\t\t/tmp/log2.txt\t/tmp/lock2.txt
server3_main\tFALSE\tserver3\tuser3\t/src/\t/dest/\t\t\t\t/tmp/test.log\t/tmp/test.lock
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
        tsv_content = """system\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1\tuser1\t/srv/data/src/\tuser@host:/dest/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt
localhost\ttest\t/src/\t/dest/\t\t\t\t/tmp/test.log\t/tmp/test.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)
        
        df = gcf.parse_transfers_file(str(test_file))
        
        # All rows should be included when enabled column is not present
        assert len(df) == 2
        assert 'server1' in df['system'].values
        assert 'localhost' in df['system'].values
        assert df.iloc[0]['identifiers'] == 'transfer_001'

    def test_parse_requires_identifiers_for_enabled_rows(self, tmp_path):
        """Test that enabled rows must define identifiers when column exists."""
        tsv_content = """identifiers\tenabled\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
\tTRUE\tserver1\tuser1\t/srv/data/src/\tuser@host:/dest/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        with pytest.raises(ValueError):
            gcf.parse_transfers_file(str(test_file))

    def test_parse_requires_log_file_for_enabled_rows(self, tmp_path):
        """Test that enabled rows must define log_file."""
        tsv_content = """identifiers\tenabled\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\tTRUE\tserver1\tuser1\t/srv/data/src/\tuser@host:/dest/\t\t\t\t\t/tmp/lock.txt
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        with pytest.raises(ValueError):
            gcf.parse_transfers_file(str(test_file))

    def test_parse_rejects_duplicate_sanitized_identifiers(self, tmp_path):
        """Test that identifiers remain unique after script filename sanitization."""
        tsv_content = """identifiers\tenabled\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
sample name\tTRUE\tserver1\tuser1\t/srv/data/src/\tuser@host:/dest/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt
sample_name\tTRUE\tserver1\tuser1\t/srv/data/src2/\tuser@host:/dest2/\t\t\t\t/tmp/log2.txt\t/tmp/lock2.txt
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        with pytest.raises(ValueError):
            gcf.parse_transfers_file(str(test_file))

    def test_parse_resolves_log_and_flock_filenames(self, tmp_path):
        """Test that filename-only log and flock values resolve via config."""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\tserver1\tuser1\t/srv/data/src/\tuser@host:/dest/\t22\t-av\t\ttransfer.log\ttransfer.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['rit_managed_locations'] = {
            'server1': '/srv/rit_managed'
        }
        gcf.config._runtime_config['rit_managed_folder_structure'] = {
            'log': 'log',
            'flock': 'flock',
        }
        try:
            df = gcf.parse_transfers_file(str(test_file))
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert df.iloc[0]['log_file'] == '/srv/rit_managed/log/transfer.log'
        assert df.iloc[0]['flock_file'] == '/srv/rit_managed/flock/transfer.lock'

    def test_parse_flow_metadata_columns(self, tmp_path):
        """Flow metadata columns should normalize to predictable values."""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file\tflow_group\tis_entry_point\tis_end_point\tnotify_on_success\tnotify_on_error
server1_main\tserver1\tuser1\t/src/\t/dest/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt\tflow_a\tyes\t0\t1\tfalse
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        df = gcf.parse_transfers_file(str(test_file))

        row = df.iloc[0]
        assert row['flow_group'] == 'flow_a'
        assert row['is_entry_point'] == 'TRUE'
        assert row['is_end_point'] == 'FALSE'
        assert row['notify_on_success'] == 'TRUE'
        assert row['notify_on_error'] == 'FALSE'

    def test_parse_rejects_hidden_root_exclude_for_portable_metadata(self, tmp_path):
        """Portable metadata cannot coexist with --exclude='/.*'."""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file\tflow_group\tis_entry_point
server1_main\tserver1\tuser1\t/src/\t/dest/\t\t--exclude='/.*'\t\t/tmp/log.txt\t/tmp/lock.txt\tflow_a\tTRUE
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        with pytest.raises(ValueError) as exc_info:
            gcf.parse_transfers_file(str(test_file))

        assert "portable .landing_zones metadata would not transfer" in str(exc_info.value)

    def test_parse_rejects_malformed_remote_endpoint(self, tmp_path):
        """Remote-looking endpoints must include the host:path separator."""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\tserver1\tuser1\t/src/\tuser@host/dest/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        with pytest.raises(ValueError) as exc_info:
            gcf.parse_transfers_file(str(test_file))

        message = str(exc_info.value)
        assert "Invalid destination for transfer 'server1_main'" in message
        assert "user@host/dest/" in message

    def test_parse_accepts_host_alias_remote_endpoint(self, tmp_path):
        """SSH aliases without an explicit user should still parse."""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\tserver1\tuser1\t/src/\tremotealias:$HOME/Landing_Zone/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        df = gcf.parse_transfers_file(str(test_file))

        assert len(df) == 1
        assert df.iloc[0]['destination'] == 'remotealias:$HOME/Landing_Zone/'

    def test_parse_records_shared_file_pair_warnings(self, tmp_path):
        """Duplicate log/flock pairs should be surfaced as warnings."""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
first\tserver1\tuser1\t/src1/\t/dest1/\t\t\t\t/tmp/shared.log\t/tmp/shared.lock
second\tserver1\tuser1\t/src2/\t/dest2/\t\t\t\t/tmp/shared.log\t/tmp/shared.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        df = gcf.parse_transfers_file(str(test_file))

        warnings = df.attrs['shared_file_pair_warnings']
        assert len(warnings) == 1
        assert "shared.log" in warnings[0]
        assert "shared.lock" in warnings[0]
        assert "first" in warnings[0]
        assert "second" in warnings[0]


class TestGenerateRsyncCommand:
    """Test the generate_rsync_command function"""
    
    def test_basic_rsync_command(self):
        """Test basic rsync command generation"""
        transfer = {
            'system': 'server1',
            'source': '/source/path/',
            'source_port': '',
            'destination': '/dest/path/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        assert 'rsync' in cmd
        assert 'ionice' not in cmd
        assert '-av' in cmd
        assert '--remove-source-files' in cmd
        assert '/source/path/' in cmd
        assert '/dest/path/.staging/transfer/' in cmd
        assert '/tmp/test.log' in cmd
    
    def test_rsync_with_ssh_port(self):
        """Test rsync command with SSH port"""
        transfer = {
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': 'user@host:/dest/',
            'destination_port': '2222',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '*/5 * * * *'
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        assert '-e "ssh -p 2222"' in cmd or "-e 'ssh -p 2222'" in cmd
        assert 'user@host:/dest/.staging/transfer/' in cmd
    
    def test_rsync_with_source_ssh_port(self):
        """Test rsync command with SSH port on source"""
        transfer = {
            'system': 'server1',
            'source': 'user@remote:/source/',
            'source_port': '2222',
            'destination': '/local/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '*/5 * * * *'
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        # Should use source port when pulling from remote
        assert '-e "ssh -p 2222"' in cmd or "-e 'ssh -p 2222'" in cmd
        assert 'user@remote:/source/' in cmd
        assert '/local/dest/.staging/transfer/' in cmd

    def test_remote_source_cleanup_preserves_home_expansion(self):
        """Remote cleanup commands should keep $HOME for the remote shell."""
        transfer = {
            'system': 'server1',
            'identifiers': 'remote_home_cleanup',
            'source': 'remotealias:$HOME/Landing_Zone/',
            'source_port': '',
            'destination': './dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '* * * * *'
        }

        script = gcf.generate_script_content(transfer)

        assert 'ssh remotealias' in script
        assert 'find "$HOME/Landing_Zone/" -mindepth 1 -type d -empty -delete' in script

    def test_remote_destination_preserves_home_expansion(self):
        """Remote rsync destinations should resolve $HOME to an absolute path first."""
        transfer = {
            'system': 'server1',
            'identifiers': 'remote_home_destination',
            'source': './source/',
            'source_port': '',
            'destination': 'remotealias:$HOME/Landing_Zone/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '* * * * *'
        }

        script = gcf.generate_script_content(transfer)

        assert 'destination_root_runtime="\\${resolved_destination_root}"' in script
        assert 'resolved_destination_root="$(ssh remotealias \'printf %s "$HOME/Landing_Zone"\')"' in script
        assert 'ssh remotealias "mkdir -p \\"${resolved_destination_root}/.staging/$dir_name\\"" </dev/null' in script
        assert 'rsync -av --remove-source-files "$source_dir/" "remotealias:${resolved_destination_root}/.staging/$dir_name/" </dev/null' in script

    def test_loop_commands_detach_stdin_for_remote_transfers(self):
        """Remote loop bodies should not consume the remaining find output."""
        transfer = {
            'system': 'server1',
            'identifiers': 'remote_stdin_guard',
            'source': './source/',
            'source_port': '',
            'destination': 'remotealias:$HOME/Landing_Zone/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '* * * * *'
        }

        script = gcf.generate_script_content(transfer)

        assert ' </dev/null >>"$promote_log" 2>&1' in script
        assert ' </dev/null >>"$run_log" 2>&1' in script

    def test_remove_stale_generated_scripts_keeps_only_expected_shell_files(self, tmp_path):
        """Old generated scripts should be removed on regeneration."""
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        keep = scripts_dir / "keep.sh"
        stale = scripts_dir / "stale.sh"
        note = scripts_dir / "README.txt"
        keep.write_text("#!/bin/sh\n")
        stale.write_text("#!/bin/sh\n")
        note.write_text("keep me\n")

        gcf.remove_stale_generated_scripts(str(scripts_dir), ["keep.sh"])

        assert keep.exists()
        assert not stale.exists()
        assert note.exists()

    def test_remove_stale_validation_scripts_preserves_validation_outputs(self, tmp_path):
        """Validation helper scripts should be preserved in the validation output dir."""
        test_data_root = tmp_path / "toy_data"
        fixture_dir = test_data_root / "lab_machine_1" / "FixtureRun"
        fixture_dir.mkdir(parents=True)
        (fixture_dir / "payload.txt").write_text("payload")
        entry_dir = tmp_path / "tests" / "test_local" / "lab_machine_1" / "Landing_Zone" / "to_calc"
        entry_dir.mkdir(parents=True)
        next_hop = tmp_path / "next_hop"
        next_hop.mkdir()
        transfers_file = tmp_path / "transfers.tsv"
        transfers_file.write_text(
            "identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file\tflow_group\tis_entry_point\n"
            "stage\tserver1\tuser1\t{0}/\t{1}/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt\tflow_a\tTRUE\n".format(
                entry_dir, next_hop
            )
        )
        transfers_df = gcf.parse_transfers_file(str(transfers_file))

        validation_dir = tmp_path / "validation_scripts"
        validation_dir.mkdir()
        helper = validation_dir / "lz_run_validation.sh"
        wrapper = validation_dir / "lz_run_validation_flow_a.sh"
        stale = validation_dir / "stale.sh"
        helper.write_text("#!/bin/sh\n")
        wrapper.write_text("#!/bin/sh\n")
        stale.write_text("#!/bin/sh\n")

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['test_data'] = str(test_data_root)
        try:
            gcf.remove_stale_validation_scripts(str(validation_dir), transfers_df)
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert helper.exists()
        assert wrapper.exists()
        assert not stale.exists()
    
    def test_rsync_with_custom_options(self):
        """Test rsync command with custom options"""
        transfer = {
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '--chown=:group --chmod=Du=rwx',
            'io_nice': 'ionice -c2 -n4',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '0 * * * *'
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        assert '--chown=:group' in cmd
        assert '--chmod=Du=rwx' in cmd
        assert 'ionice -c2 -n4 rsync' in cmd
        assert 'find /dest/.staging/transfer -mindepth 1 -maxdepth 1 ! -name \'.staging\' -exec mv {} /dest/ \\;' in cmd

    def test_rsync_with_io_nice_arguments_only(self):
        """Test that bare io_nice arguments are prefixed with ionice."""
        transfer = {
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '-c2 -n7',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }

        cmd = gcf.generate_rsync_command(transfer)

        assert 'ionice -c2 -n7 rsync' in cmd

    def test_rsync_without_io_nice_when_blank(self):
        """Test that blank io_nice does not prefix rsync"""
        transfer = {
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '   ',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }

        cmd = gcf.generate_rsync_command(transfer)

        assert 'ionice' not in cmd
        assert 'rsync -av --remove-source-files /source/ /dest/.staging/transfer/' in cmd

    def test_rsync_uses_remote_staging_and_remote_promote(self):
        """Test that remote destinations are staged and promoted remotely."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': 'user@host:/dest/',
            'destination_port': '2222',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }

        cmd = gcf.generate_rsync_command(transfer)

        assert 'ssh -p 2222 user@host "mkdir -p /dest/.staging/sample"' in cmd
        assert 'rsync -av --remove-source-files -e \'ssh -p 2222\' /source/ user@host:/dest/.staging/sample/' in cmd
        assert 'ssh -p 2222 user@host "find /dest/.staging/sample -mindepth 1 -maxdepth 1 ! -name \'.staging\' -exec mv {} /dest/ \\; && { rmdir /dest/.staging/sample 2>/dev/null || true; } && { rmdir /dest/.staging 2>/dev/null || true; }"' in cmd

    def test_rsync_cleans_remote_sources_via_ssh(self):
        """Test that remote source cleanup runs on the remote host."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': 'user@remote:/source/*',
            'source_port': '2200',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }

        cmd = gcf.generate_rsync_command(transfer)

        assert "ssh -p 2200 user@remote 'find \"\\\"/source\\\"\" -mindepth 1 -type d -empty -delete'" not in cmd
        assert 'ssh -p 2200 user@remote \'find "/source" -mindepth 1 -type d -empty -delete\'' in cmd
    
    def test_rsync_validates_flock_file(self):
        """Test that rsync command requires flock_file"""
        transfer = {
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '',
            'frequency': ''
        }
        
        with pytest.raises(ValueError) as exc_info:
            gcf.generate_rsync_command(transfer)
        
        assert 'flock_file' in str(exc_info.value).lower()
    
    def test_generate_cron_entry_with_custom_frequency(self):
        """Test cron entry generation with custom cron frequency."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '0 0 * * *'  # Daily at midnight
        }

        cmd = gcf.generate_cron_entry(transfer, '/tmp/scripts/sample.sh')

        assert cmd == '0 0 * * * /bin/sh /tmp/scripts/sample.sh'

    def test_generate_cron_entry_default_frequency_when_empty(self):
        """Test that default frequency is used when frequency is empty."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''  # Empty, should use default
        }

        cmd = gcf.generate_cron_entry(transfer, '/tmp/scripts/sample.sh')

        assert cmd == '*/15 * * * * /bin/sh /tmp/scripts/sample.sh'

    def test_generate_cron_entry_default_frequency_when_nan(self):
        """Test that default frequency is used when frequency is 'nan'."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': 'nan'  # NaN string, should use default
        }

        cmd = gcf.generate_cron_entry(transfer, '/tmp/scripts/sample.sh')

        assert cmd == '*/15 * * * * /bin/sh /tmp/scripts/sample.sh'

    def test_generate_script_content(self):
        """Test shell script content generation now uses iterative transfers."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['flock_paths'] = {'server1': '/opt/bin/flock'}
        try:
            script = gcf.generate_script_content(transfer)
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert script.startswith('#!/bin/sh\n')
        assert 'set -eu' in script
        assert 'find "/source" -mindepth 1 -maxdepth 1 -type d ! -name ".*" -print | while IFS= read -r source_dir; do' in script
        assert 'rsync -av --remove-source-files "$source_dir/" "/dest/.staging/$dir_name/" </dev/null >>"$run_log" 2>&1' in script
        assert 'exec 9>"$flock_file"' in script
        assert '/opt/bin/flock -n 9' in script
        assert 'flock_file="/tmp/test.lock"' in script
        assert 'cat "$run_log" >> "$log_file"' in script
        assert 'mini_log_file="/tmp/test.log.mini"' in script
        assert "printf '%s %s\\n'" in script
        assert 'common_status_log_file="output/log/Landing_Zone_server1.transfers.tsv"' in script
        assert 'common_status_lock_file="output/flock/Landing_Zone_server1.transfers.lock"' in script
        assert "printf 'event_time_utc\\ttransfer_identifier\\tsystem\\trun_id\\trun_name\\tflow_group\\torigin_system\\tentry_transfer_identifier\\tcreated_at_utc\\tdirectory\\tsource_path\\tdestination_path\\tstatus\\tmessage\\n'" in script
        assert 'append_common_status "initiated" "$dir_name" "$current_run_source" "$current_run_destination"' in script
        assert 'append_common_status "completed" "$dir_name" "$current_run_source" "$current_run_destination"' in script
        assert 'append_common_status "error" "$current_run" "$current_run_source" "$current_run_destination"' in script
        assert 'mkdir -p "$(dirname "$log_file")" "$(dirname "$latest_log_file")" "$(dirname "$mini_log_file")" "$(dirname "$flock_file")"' in script
        assert 'dump_debug_log "run log" "$run_log"' in script
        assert 'dump_debug_log "promote log" "$promote_log"' in script
        assert 'dump_debug_log "cleanup log" "$cleanup_log"' in script
        assert 'debug "script failed with exit code $status"' in script
        assert 'debug "$dir_name initiated"' in script
        assert 'debug "$dir_name completed"' in script
        assert 'log_status "$dir_name initiated"' in script
        assert 'log_status "$dir_name completed"' in script
        assert 'if ! [ -d "/source" ]; then' in script
        assert 'source directory missing: /source' in script
        assert 'append_common_status "error" "" "/source" "/dest"' in script
        assert 'latest_log_file="/tmp/test.log.latest"' in script
        assert 'cat "$run_log" > "$latest_log_file"' in script

    def test_generate_script_content_with_portable_metadata(self):
        """Portable metadata should be initialized and appended in the right order."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '',
            'flow_group': 'flow_a',
            'is_entry_point': 'TRUE',
        }

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['flock_paths'] = {'server1': '/opt/bin/flock'}
        try:
            script = gcf.generate_script_content(transfer)
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert 'portable_metadata_enabled="1"' in script
        assert 'is_entry_point="TRUE"' in script
        assert 'portable_metadata_dir_name=".landing_zones"' in script
        assert 'portable_metadata_file_name="landingzone-run-metadata.tsv"' in script
        assert 'portable_events_file_name="landingzone-transfer-events.tsv"' in script
        assert 'current_run_id="$(uuidgen | tr ' in script
        assert 'current_run_name="$dir_name"' in script
        assert 'current_origin_system="$transfer_system"' in script
        assert 'current_entry_transfer_identifier="$transfer_identifier"' in script
        assert 'ensure_source_run_bundle || continue' in script
        assert 'append_source_portable_event "initiated"' in script
        assert 'append_destination_portable_event "completed"' in script

        ensure_index = script.index('ensure_source_run_bundle || continue')
        initiated_portable_index = script.index('append_source_portable_event "initiated"')
        initiated_common_index = script.index('append_common_status "initiated" "$dir_name" "$current_run_source" "$current_run_destination"')
        rsync_index = script.index('rsync -av --remove-source-files "$source_dir/" "/dest/.staging/$dir_name/" </dev/null >>"$run_log" 2>&1')
        promote_index = script.index('if [ -d "/dest/$dir_name" ]; then ')
        completed_portable_index = script.index('append_destination_portable_event "completed"')
        completed_common_index = script.index('append_common_status "completed" "$dir_name" "$current_run_source" "$current_run_destination"')

        assert ensure_index < initiated_portable_index < initiated_common_index < rsync_index
        assert promote_index < completed_portable_index < completed_common_index

    def test_generate_script_content_with_portable_metadata_remote_destination(self):
        """Completed portable events should support remote destination appends."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': 'user@host:/dest/',
            'destination_port': '2222',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': '',
            'flow_group': 'flow_a',
            'is_entry_point': 'TRUE',
        }

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['flock_paths'] = {'server1': '/opt/bin/flock'}
        try:
            script = gcf.generate_script_content(transfer)
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert 'destination_remote_target="user@host"' in script
        assert 'destination_remote_port="2222"' in script
        assert 'quoted_remote_arg=$(printf \'%s\' "$remote_arg" | sed "s/\'/\'\\\\\'\'/g")' in script
        assert 'ssh -p "$remote_port" "$remote_target" "$remote_command"' in script
        assert 'grep "^run_id" "$1" | head -n 1 | cut -f2-' in script
        assert 'cut -f2-' in script
        assert 'append_portable_event_remote "$destination_remote_target" "$destination_remote_port" "$destination_run_dir" "$event_status" "$event_message"' in script

    def test_generate_validation_script_content(self):
        """The shared validation helper should provide preflight and run modes."""
        script = gcf.generate_validation_script_content()
        template_path = (
            os.path.dirname(gcf.__file__)
            + "/templates/lz_run_validation.sh"
        )
        with open(template_path, 'r') as handle:
            template_content = handle.read()

        assert script.startswith('#!/bin/sh\n')
        assert script == template_content
        assert 'lz_run_validation.sh preflight' in script
        assert 'lz_run_validation.sh run' in script
        assert 'build_validation_name()' in script
        assert 'LZTEST_' in script
        assert 'remote_ssh()' in script
        assert 'check_remote_dir()' in script
        assert 'sh -c "$ssh_cmd' not in script
        assert 'remote_ssh "$remote_target" "$remote_port" sh -c' in script
        assert 'run_preflight()' in script
        assert 'run_validation()' in script

    def test_write_validation_scripts_creates_validation_helper_and_wrapper(self, tmp_path):
        """Validation script writer should emit the helper and flow-group wrapper."""
        test_data_root = tmp_path / "toy_data"
        fixture_dir = test_data_root / "lab_machine_1" / "FixtureRun"
        fixture_dir.mkdir(parents=True)
        (fixture_dir / "payload.txt").write_text("payload")
        entry_dir = tmp_path / "tests" / "test_local" / "lab_machine_1" / "Landing_Zone" / "to_calc"
        entry_dir.mkdir(parents=True)
        next_hop = tmp_path / "next_hop"
        next_hop.mkdir()
        transfers_file = tmp_path / "transfers.tsv"
        transfers_file.write_text(
            "identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file\tflow_group\tis_entry_point\n"
            "stage\tserver1\tuser1\t{0}/\t{1}/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt\tflow_a\tTRUE\n".format(
                entry_dir, next_hop
            )
        )
        transfers_df = gcf.parse_transfers_file(str(transfers_file))
        validation_dir = tmp_path / "validation_scripts"

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['test_data'] = str(test_data_root)
        try:
            gcf.write_validation_scripts(str(validation_dir), transfers_df)
        finally:
            gcf.config._runtime_config = original_runtime_config

        helper = validation_dir / "lz_run_validation.sh"
        wrapper = validation_dir / "lz_run_validation_flow_a.sh"
        assert helper.exists()
        assert wrapper.exists()
        assert os.access(helper, os.X_OK)
        assert os.access(wrapper, os.X_OK)
        assert 'lz_run_validation.sh preflight' in helper.read_text()
        wrapper_text = wrapper.read_text()
        assert 'HELPER_SCRIPT="$SCRIPT_DIR/lz_run_validation.sh"' in wrapper_text
        assert str(entry_dir) in wrapper_text
        assert str(next_hop) in wrapper_text
        assert str(fixture_dir) in wrapper_text
        assert "FLOW_GROUP_DEFAULT=flow_a" in wrapper_text
        assert "PRODUCER_DEFAULT=server1" in wrapper_text

    def test_validation_helper_preflight_local(self, tmp_path):
        """The helper should validate local fixture and entry directories."""
        validation_dir = tmp_path / "validation_scripts"
        fixture_dir = tmp_path / "fixture"
        entry_dir = tmp_path / "entry"
        fixture_dir.mkdir()
        entry_dir.mkdir()
        (fixture_dir / "payload.txt").write_text("payload")

        gcf.write_validation_scripts(str(validation_dir))
        helper = validation_dir / "lz_run_validation.sh"

        proc = subprocess.run(
            [
                str(helper),
                "preflight",
                "--fixture-dir", str(fixture_dir),
                "--entry-dir", str(entry_dir),
                "--next-hop", str(entry_dir),
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        assert proc.returncode == 0
        assert "OK Preflight passed" in proc.stdout

    def test_validation_helper_help_flag(self, tmp_path):
        """The helper should accept --help without treating it as a command."""
        validation_dir = tmp_path / "validation_scripts"

        gcf.write_validation_scripts(str(validation_dir))
        helper = validation_dir / "lz_run_validation.sh"

        proc = subprocess.run(
            [str(helper), "--help"],
            capture_output=True,
            text=True,
            check=False,
        )

        assert proc.returncode == 0
        assert "Usage:" in proc.stdout
        assert "Unknown command" not in proc.stdout

    def test_validation_helper_run_creates_named_validation_folder(self, tmp_path):
        """The helper should inject a visible LZTEST folder with marker and payload."""
        validation_dir = tmp_path / "validation_scripts"
        fixture_dir = tmp_path / "fixture"
        entry_dir = tmp_path / "entry"
        fixture_dir.mkdir()
        entry_dir.mkdir()
        (fixture_dir / "payload.txt").write_text("payload")

        gcf.write_validation_scripts(str(validation_dir))
        helper = validation_dir / "lz_run_validation.sh"

        proc = subprocess.run(
            [
                str(helper),
                "run",
                "--fixture-dir", str(fixture_dir),
                "--entry-dir", str(entry_dir),
                "--flow-group", "flow-a",
                "--producer", "gridion-1",
                "--token", "ABCD",
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        assert proc.returncode == 0
        created = [child for child in entry_dir.iterdir() if child.is_dir()]
        assert len(created) == 1
        assert created[0].name.startswith("LZTEST_FLOW-A_GRIDION-1_")
        assert created[0].name.endswith("_ABCD")
        assert (created[0] / "payload.txt").read_text() == "payload"
        marker = (created[0] / "lz_validation.marker").read_text()
        assert "validation_name\t{0}".format(created[0].name) in marker
        assert "flow_group\tflow-a" in marker
        assert "producer\tgridion-1" in marker

    def test_validation_wrapper_run_uses_flow_defaults(self, tmp_path):
        """Generated flow wrappers should run the helper with baked-in defaults."""
        test_data_root = tmp_path / "toy_data"
        fixture_dir = test_data_root / "lab_machine_1" / "FixtureRun"
        fixture_dir.mkdir(parents=True)
        (fixture_dir / "payload.txt").write_text("payload")
        entry_dir = tmp_path / "tests" / "test_local" / "lab_machine_1" / "Landing_Zone" / "to_calc"
        entry_dir.mkdir(parents=True)
        next_hop = tmp_path / "next_hop"
        next_hop.mkdir()
        transfers_file = tmp_path / "transfers.tsv"
        transfers_file.write_text(
            "identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file\tflow_group\tis_entry_point\n"
            "stage\tserver1\tuser1\t{0}/\t{1}/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt\tflow_a\tTRUE\n".format(
                entry_dir, next_hop
            )
        )
        transfers_df = gcf.parse_transfers_file(str(transfers_file))
        validation_dir = tmp_path / "validation_scripts"

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['test_data'] = str(test_data_root)
        try:
            gcf.write_validation_scripts(str(validation_dir), transfers_df)
        finally:
            gcf.config._runtime_config = original_runtime_config

        wrapper = validation_dir / "lz_run_validation_flow_a.sh"
        proc = subprocess.run(
            [str(wrapper), "run", "--token", "ABCD"],
            capture_output=True,
            text=True,
            check=False,
        )

        assert proc.returncode == 0
        created = [child for child in entry_dir.iterdir() if child.is_dir()]
        assert len(created) == 1
        assert created[0].name.startswith("LZTEST_FLOW_A_SERVER1_")
        assert created[0].name.endswith("_ABCD")
        assert (created[0] / "payload.txt").read_text() == "payload"
        marker = (created[0] / "lz_validation.marker").read_text()
        assert "flow_group\tflow_a" in marker
        assert "producer\tserver1" in marker

    def test_validation_wrapper_runs_by_default_without_command(self, tmp_path):
        """Generated flow wrappers should default to run when invoked without args."""
        test_data_root = tmp_path / "toy_data"
        fixture_dir = test_data_root / "lab_machine_1" / "FixtureRun"
        fixture_dir.mkdir(parents=True)
        (fixture_dir / "payload.txt").write_text("payload")
        entry_dir = tmp_path / "tests" / "test_local" / "lab_machine_1" / "Landing_Zone" / "to_calc"
        entry_dir.mkdir(parents=True)
        next_hop = tmp_path / "next_hop"
        next_hop.mkdir()
        transfers_file = tmp_path / "transfers.tsv"
        transfers_file.write_text(
            "identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file\tflow_group\tis_entry_point\n"
            "stage\tserver1\tuser1\t{0}/\t{1}/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt\tflow_a\tTRUE\n".format(
                entry_dir, next_hop
            )
        )
        transfers_df = gcf.parse_transfers_file(str(transfers_file))
        validation_dir = tmp_path / "validation_scripts"

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['test_data'] = str(test_data_root)
        try:
            gcf.write_validation_scripts(str(validation_dir), transfers_df)
        finally:
            gcf.config._runtime_config = original_runtime_config

        wrapper = validation_dir / "lz_run_validation_flow_a.sh"
        proc = subprocess.run(
            [str(wrapper), "--token", "ABCD"],
            capture_output=True,
            text=True,
            check=False,
        )

        assert proc.returncode == 0
        created = [child for child in entry_dir.iterdir() if child.is_dir()]
        assert len(created) == 1
        assert created[0].name.startswith("LZTEST_FLOW_A_SERVER1_")
        assert created[0].name.endswith("_ABCD")

    def test_generate_script_content_handles_rsync_failure(self):
        """Test shell script content generation for the iterative rsync failure path."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['flock_paths'] = {'server1': '/opt/bin/flock'}
        try:
            script = gcf.generate_script_content(transfer)
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert ': >"$run_log"' in script
        assert 'rsync -av --remove-source-files "$source_dir/" "/dest/.staging/$dir_name/" </dev/null >>"$run_log" 2>&1' in script
        assert 'dump_debug_log "run log" "$run_log"' in script
        assert 'debug "script failed with exit code $status"' in script

    def test_generate_script_content_iterates_wildcard_source_dirs(self):
        """Test shell script content for wildcard local sources."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': 'input/*',
            'source_port': '',
            'destination': 'output/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['flock_paths'] = {'server1': '/opt/bin/flock'}
        try:
            script = gcf.generate_script_content(transfer)
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert 'find "input" -mindepth 1 -maxdepth 1 -type d ! -name ".*" -print | while IFS= read -r source_dir; do' in script
        assert 'mkdir -p "output/.staging/$dir_name"' in script
        assert 'rsync -av --remove-source-files "$source_dir/" "output/.staging/$dir_name/" </dev/null >>"$run_log" 2>&1' in script
        assert 'mv "output/.staging/$dir_name" "output/$dir_name"' in script
        assert 'find "output/.staging/$dir_name" -mindepth 1 -maxdepth 1 ! -name ".staging" -exec mv {} "output/$dir_name"/ \\;' in script
        assert 'current_run_source="$source_dir"' in script
        assert 'current_run_destination="output/$dir_name"' in script
        assert 'log_status "$dir_name initiated"' in script
        assert 'log_status "$dir_name completed"' in script
        assert 'find "input" -mindepth 1 -type d -empty -delete >"$cleanup_log" 2>&1' in script
        assert 'if ! [ -d "input" ]; then' in script
        assert 'source directory missing: input' in script

    def test_generate_script_content_iterates_into_remote_staging_dirs(self):
        """Test shell script content for local source to remote destination."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': '/source/*',
            'source_port': '',
            'destination': 'user@host:/dest/',
            'destination_port': '2222',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['flock_paths'] = {'server1': '/opt/bin/flock'}
        try:
            script = gcf.generate_script_content(transfer)
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert 'find "/source" -mindepth 1 -maxdepth 1 -type d ! -name ".*" -print | while IFS= read -r source_dir; do' in script
        assert 'ssh -p 2222 user@host "mkdir -p \\"/dest/.staging/$dir_name\\""' in script
        assert 'rsync -av --remove-source-files -e \'ssh -p 2222\' "$source_dir/" "user@host:/dest/.staging/$dir_name/" </dev/null >>"$run_log" 2>&1' in script
        assert 'ssh -p 2222 user@host "if [ -d \\"/dest/$dir_name\\" ]; then ' in script
        assert 'find \\"/dest/.staging/$dir_name\\" -mindepth 1 -maxdepth 1 ! -name \\".staging\\" -exec mv {} \\"/dest/$dir_name/\\" \\;' in script
        assert 'current_run_destination="user@host:/dest/$dir_name"' in script
        assert 'if ! [ -d "/source" ]; then' in script
        assert 'source directory missing: /source' in script

    def test_generate_script_content_iterates_remote_source_dirs(self):
        """Test shell script content for remote source to local destination."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': 'user@remote:/source/*',
            'source_port': '2200',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['flock_paths'] = {'server1': '/opt/bin/flock'}
        try:
            script = gcf.generate_script_content(transfer)
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert "ssh -p 2200 user@remote 'find \"/source\" -mindepth 1 -maxdepth 1 -type d ! -name \".*\" -print' | while IFS= read -r source_dir; do" in script
        assert 'rsync -av --remove-source-files -e \'ssh -p 2200\' "user@remote:$source_dir/" "/dest/.staging/$dir_name/" </dev/null >>"$run_log" 2>&1' in script
        assert 'current_run_source="user@remote:$source_dir"' in script
        assert 'current_run_destination="/dest/$dir_name"' in script
        assert 'if ! ssh -p 2200 user@remote \'[ -d "/source" ]\'; then' in script
        assert 'source directory missing: /source' in script

    def test_generate_script_content_skips_hidden_top_level_dirs(self):
        """Test iterative scripts exclude hidden directories such as .ssh."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': '/source/*',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['flock_paths'] = {'server1': '/opt/bin/flock'}
        try:
            script = gcf.generate_script_content(transfer)
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert '! -name ".*"' in script


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
            'identifiers': ['first', 'second', 'third'],
            'system': ['server1', 'server1', 'server2'],
            'users': ['user1', 'user1', 'user2'],
            'source': ['/src1/', '/src2/', '/src3/'],
            'source_port': ['', '', ''],
            'destination': ['/dst1/', '/dst2/', '/dst3/'],
            'destination_port': ['', '', ''],
            'rsync_options': ['', '', ''],
            'io_nice': ['', '', ''],
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
        tsv_content = """identifiers\tsystem\tusers\tsource\tsource_port\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
local_copy\tlocalhost\ttestuser\t/tmp/src/\t\t/tmp/dest/\t\t\t\t/tmp/test.log\t/tmp/test.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)
        
        # Parse the file
        df = gcf.parse_transfers_file(str(test_file))
        
        # Generate cron content
        system = 'localhost'
        user = 'testuser'
        transfers = df[(df['system'] == system) & (df['users'] == user)]
        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['rit_managed_locations'] = {
            'localhost': '/srv/deployed'
        }
        gcf.config._runtime_config['rit_managed_folder_structure'] = {
            'sh_output': 'output/scripts'
        }
        try:
            cron_content = gcf.generate_cron_file(
                '{0}.{1}'.format(system, user), transfers, '/tmp/scripts'
            )
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert 'localhost' in cron_content
        assert 'testuser' in cron_content
        assert '/tmp/src/' in cron_content
        assert '/tmp/dest/' in cron_content
        assert '/bin/sh /srv/deployed/output/scripts/local_copy.sh' in cron_content

    def test_main_prints_shared_file_pair_warning(self, tmp_path, monkeypatch, capsys):
        """Generation should emit warnings for duplicate log/flock pairs."""
        transfers_file = tmp_path / "test_transfers.tsv"
        output_dir = tmp_path / "crontab.d"
        log_dir = tmp_path / "log"
        scripts_dir = tmp_path / "scripts"
        validation_scripts_dir = tmp_path / "validation_scripts"
        transfers_file.write_text(
            """identifiers\tsystem\tusers\tsource\tsource_port\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
first\tlocalhost\ttestuser\t/tmp/src1/\t\t/tmp/dest1/\t\t\t\t/tmp/shared.log\t/tmp/shared.lock
second\tlocalhost\ttestuser\t/tmp/src2/\t\t/tmp/dest2/\t\t\t\t/tmp/shared.log\t/tmp/shared.lock
"""
        )

        monkeypatch.setattr(
            sys,
            'argv',
            [
                'generate_cron_files.py',
                '--transfers', str(transfers_file),
                '--output-dir', str(output_dir),
                '--log-dir', str(log_dir),
                '--scripts-dir', str(scripts_dir),
                '--validation-scripts-dir', str(validation_scripts_dir),
            ],
        )

        rc = gcf.main()
        captured = capsys.readouterr()

        assert rc == 0
        assert 'Shared log/flock pairs detected' in captured.out
        assert 'first' in captured.out
        assert 'second' in captured.out
        assert (validation_scripts_dir / "lz_run_validation.sh").exists()


class TestEnvironmentVariableExpansion:
    """Test that environment variables are handled correctly"""
    
    def test_home_variable_not_expanded_in_output(self):
        """Test that $HOME is preserved in the cron output"""
        transfer = {
            'system': 'server1',
            'source': '$HOME/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '$HOME/test.log',
            'flock_file': '$HOME/test.lock',
            'frequency': ''
        }
        
        cmd = gcf.generate_rsync_command(transfer)
        
        # $HOME should be preserved for cron to expand at runtime
        assert '$HOME' in cmd

    def test_shell_path_preserves_home_expansion(self):
        """Test that shell path quoting does not suppress $HOME expansion."""
        assert gcf.shell_path('$HOME/source dir') == '"$HOME/source dir"'


class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_empty_port_handled(self):
        """Test that empty port is handled correctly"""
        transfer = {
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': 'user@host:/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
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
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': 'nan',
            'rsync_options': '',
            'io_nice': '',
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

    def test_detects_directory_iteration_sources(self):
        """Test wildcard source detection for per-directory transfer scripts."""
        script = gcf.generate_script_content({
            'identifiers': 'sample',
            'system': 'server1',
            'source': '/path/to/input/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        })
        assert 'while IFS= read -r source_dir; do' in script

    def test_generate_script_content_keeps_plain_directory_sources_as_single_rsync(self):
        """Test shell script content for a normal directory source."""
        transfer = {
            'identifiers': 'sample',
            'system': 'server1',
            'source': '/source/',
            'source_port': '',
            'destination': '/dest/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': '/tmp/test.log',
            'flock_file': '/tmp/test.lock',
            'frequency': ''
        }

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['flock_paths'] = {'server1': '/opt/bin/flock'}
        try:
            script = gcf.generate_script_content(transfer)
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert 'find "/source" -mindepth 1 -maxdepth 1 -type d ! -name ".*" -print | while IFS= read -r source_dir; do' in script
        assert 'if ! [ -d "/source" ]; then' in script
        assert 'source directory missing: /source' in script
        assert 'rsync -av --remove-source-files "$source_dir/" "/dest/.staging/$dir_name/" </dev/null >>"$run_log" 2>&1' in script


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
