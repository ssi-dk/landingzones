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


HAS_RSYNC = shutil.which("rsync") is not None
HAS_FLOCK = shutil.which("flock") is not None


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
        assert df.iloc[0]['runtime_id'] == 'server1.user1'
        assert df.iloc[0]['system_user'] == 'server1.user1'
        assert df.iloc[0]['identifiers'] == 'server1_main'
        assert df.iloc[1]['system'] == 'localhost'

    def test_parse_uses_stored_runtime_id(self, tmp_path):
        """runtime_id is the stored runtime/artifact identity."""
        tsv_content = """identifiers\truntime_id\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\tserver1_prod.user1\tserver1\tuser1\t/srv/data/src/\tuser@host:/dest/\t22\t-av\t\t/tmp/log.txt\t/tmp/lock.txt
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        df = gcf.parse_transfers_file(str(test_file))

        assert df.iloc[0]['runtime_id'] == 'server1_prod.user1'
        assert df.iloc[0]['system_user'] == 'server1_prod.user1'

    def test_parse_rejects_missing_runtime_id_when_column_exists(self, tmp_path):
        """Blank runtime_id values are invalid once the column exists."""
        tsv_content = """identifiers\truntime_id\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\t\tserver1\tuser1\t/srv/data/src/\tuser@host:/dest/\t22\t-av\t\t/tmp/log.txt\t/tmp/lock.txt
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        with pytest.raises(ValueError):
            gcf.parse_transfers_file(str(test_file))

    def test_filter_transfers_by_runtime_ids_requires_exact_matches(self, tmp_path):
        """runtime filters select exact runtime_id values and reject typos."""
        tsv_content = """identifiers\truntime_id\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
one\tserver1_prod.user1\tserver1\tuser1\t/src1/\t/dest1/\t\t\t\t/tmp/log1.txt\t/tmp/lock1.txt
two\tserver2_prod.user2\tserver2\tuser2\t/src2/\t/dest2/\t\t\t\t/tmp/log2.txt\t/tmp/lock2.txt
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        df = gcf.parse_transfers_file(str(test_file))
        filtered = gcf.filter_transfers_by_runtime_ids(df, ['server1_prod.user1'])

        assert filtered['identifiers'].tolist() == ['one']
        with pytest.raises(ValueError):
            gcf.filter_transfers_by_runtime_ids(df, ['missing_prod.user'])

    def test_parse_filters_system_before_endpoint_expansion(self, tmp_path):
        """System filters should ignore unrelated rows with unavailable variables."""
        snapshot = gcf.config.snapshot_state()
        gcf.config.load_config(path_variables={"LOCAL_ROOT": "/tmp/local"})
        tsv_content = """identifiers\truntime_id\tenabled\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
local_stage\tlocal_dev.local\tTRUE\tlocal_dev\tlocal\t${LOCAL_ROOT}/in/\t${LOCAL_ROOT}/out/\t\t\t\t/tmp/local.log\t/tmp/local.lock
remote_stage\tserver2_dev.user\tTRUE\tserver2\tuser\t${MISSING_REMOTE_ROOT}/in/\t/dest/\t\t\t\t/tmp/remote.log\t/tmp/remote.lock
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        try:
            df = gcf.parse_transfers_file(str(test_file), systems=["local_dev"])
        finally:
            gcf.config.restore_state(snapshot)

        assert df["identifiers"].tolist() == ["local_stage"]
        assert df.iloc[0]["source"] == "/tmp/local/in/"

        try:
            gcf.config.load_config(path_variables={"LOCAL_ROOT": "/tmp/local"})
            with pytest.raises(ValueError, match="system filter matched no transfer rows"):
                gcf.parse_transfers_file(str(test_file), systems=["missing"])
        finally:
            gcf.config.restore_state(snapshot)

    def test_write_runtime_filter_metadata_records_unique_runtime_ids(self, tmp_path):
        """Build metadata should capture the runtime IDs represented by artifacts."""
        crontab_dir = tmp_path / "output" / "crontab.d"

        gcf.write_runtime_filter_metadata(
            str(crontab_dir),
            ["local_dev.local", "local_dev.local", "local_test.local"],
        )

        metadata_path = tmp_path / "output" / "runtime_ids.txt"
        lines = [
            line.strip()
            for line in metadata_path.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]
        assert lines == ["local_dev.local", "local_test.local"]

    def test_main_uses_yaml_runtime_ids_when_cli_filter_is_omitted(self, tmp_path):
        """Build should scope artifacts from config runtime_ids without CLI flags."""
        transfers_file = tmp_path / "transfers.tsv"
        crontab_dir = tmp_path / "output" / "crontab.d"
        validation_dir = tmp_path / "output" / "validation_scripts"
        config_file = tmp_path / "config.yaml"
        transfers_file.write_text(
            "\n".join(
                [
                    "identifiers\truntime_id\tenabled\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file",
                    "local_stage\tlocal_dev.local\tTRUE\tlocal_dev\tlocal\t/src/local/\t/dst/local/\t\t\t\t{0}\t{1}".format(
                        tmp_path / "local.log",
                        tmp_path / "local.lock",
                    ),
                    "other_stage\tother.local\tTRUE\tother\tlocal\t/src/other/\t/dst/other/\t\t\t\t{0}\t{1}".format(
                        tmp_path / "other.log",
                        tmp_path / "other.lock",
                    ),
                ]
            )
        )
        config_file.write_text(
            "transfers_file: {0}\n"
            "crontab_dir: {1}\n"
            "validation_scripts_dir: {2}\n"
            "runtime_ids:\n"
            "  - local_dev.local\n".format(
                transfers_file,
                crontab_dir,
                validation_dir,
            )
        )

        snapshot = gcf.config.snapshot_state()
        try:
            rc = gcf.main(["--config", str(config_file)])
        finally:
            gcf.config.restore_state(snapshot)

        assert rc == 0
        assert (crontab_dir / "local_dev.local.Landing_Zone.cron").exists()
        assert not (crontab_dir / "other.local.Landing_Zone.cron").exists()
        metadata_path = tmp_path / "output" / "runtime_ids.txt"
        assert "local_dev.local" in metadata_path.read_text()
        assert "other.local" not in metadata_path.read_text()
    
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

    def test_parse_applies_artifact_prefix_to_script_names(self, tmp_path):
        """Configured artifact prefixes should isolate generated script names."""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
sample\tserver1\tuser1\t/src/\t/dest/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        snapshot = gcf.config.snapshot_state()
        gcf.config.load_config(artifact_prefix="prod server1")
        try:
            df = gcf.parse_transfers_file(str(test_file))
        finally:
            gcf.config.restore_state(snapshot)

        assert df.iloc[0]["script_name"] == "prod_server1__sample.sh"

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

    def test_parse_normalizes_tags_column(self, tmp_path):
        """Tags should normalize to lowercase deduplicated comma-separated text."""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file\ttags
server1_main\tserver1\tuser1\t/src/\t/dest/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt\tHeartbeat, lab , heartbeat
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        df = gcf.parse_transfers_file(str(test_file))

        assert df.iloc[0]['tags'] == 'heartbeat,lab'

    def test_parse_allows_notifications_without_flow_group(self, tmp_path):
        """Notification flags should not require portable flow metadata."""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file\tnotify_on_success\tnotify_on_error
server1_main\tserver1\tuser1\t/src/\t/dest/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt\tTRUE\tTRUE
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        df = gcf.parse_transfers_file(str(test_file))

        assert df.iloc[0]['notify_on_success'] == 'TRUE'
        assert df.iloc[0]['notify_on_error'] == 'TRUE'

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

    def test_parse_expands_local_path_variables_from_config(self, tmp_path):
        """Local source and destination paths should expand ${VAR} placeholders."""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\tserver1\tuser1\t${SRC_ROOT}/input/\t${DST_ROOT}/output/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        snapshot = gcf.config.snapshot_state()
        gcf.config.load_config(path_variables={
            'SRC_ROOT': '/srv/dev/source',
            'DST_ROOT': '/srv/dev/destination',
        })
        try:
            df = gcf.parse_transfers_file(str(test_file))
        finally:
            gcf.config.restore_state(snapshot)

        assert df.iloc[0]['source'] == '/srv/dev/source/input/'
        assert df.iloc[0]['destination'] == '/srv/dev/destination/output/'

    def test_parse_expands_remote_path_variables_without_touching_remote_shell_vars(self, tmp_path):
        """Only the filesystem path segment of remote endpoints should expand."""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\tserver1\tuser1\t${SRC_ROOT}/input/\tremotealias:${REMOTE_ROOT}/output/$HOME/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        snapshot = gcf.config.snapshot_state()
        gcf.config.load_config(path_variables={
            'SRC_ROOT': '/srv/dev/source',
            'REMOTE_ROOT': '/srv/remote/root',
        })
        try:
            df = gcf.parse_transfers_file(str(test_file))
        finally:
            gcf.config.restore_state(snapshot)

        assert df.iloc[0]['source'] == '/srv/dev/source/input/'
        assert df.iloc[0]['destination'] == 'remotealias:/srv/remote/root/output/$HOME/'

    def test_parse_rejects_unresolved_path_variables(self, tmp_path):
        """Unresolved ${VAR} placeholders should fail fast."""
        tsv_content = """identifiers\tsystem\tusers\tsource\tdestination\tdestination_port\trsync_options\tio_nice\tlog_file\tflock_file
server1_main\tserver1\tuser1\t${SRC_ROOT}/input/\t${MISSING_ROOT}/output/\t\t\t\t/tmp/log.txt\t/tmp/lock.txt
"""
        test_file = tmp_path / "test_transfers.tsv"
        test_file.write_text(tsv_content)

        snapshot = gcf.config.snapshot_state()
        gcf.config.load_config(path_variables={
            'SRC_ROOT': '/srv/dev/source',
        })
        try:
            with pytest.raises(ValueError) as exc_info:
                gcf.parse_transfers_file(str(test_file))
        finally:
            gcf.config.restore_state(snapshot)

        message = str(exc_info.value)
        assert "server1_main" in message
        assert "destination" in message
        assert "MISSING_ROOT" in message

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

    def test_parse_reporting_mode_allows_minimal_transfer_metadata(self, tmp_path):
        """Reporting/analysis mode should not require runtime log fields."""
        tsv_content = """identifiers\tenabled\tsystem\tusers\tsource\tdestination
stage_lab\tTRUE\ttest_local\tlocal\t/source/inbox/*\t/flow/stage/
promote_server1\tTRUE\ttest_local\tlocal\t/flow/stage/\t/flow/final/
"""
        test_file = tmp_path / "minimal_transfers.tsv"
        test_file.write_text(tsv_content)

        df = gcf.parse_transfers_file(
            str(test_file),
            require_runtime_files=False,
        )

        assert len(df) == 2
        assert list(df["identifiers"]) == ["stage_lab", "promote_server1"]
        assert (df["log_file"] == "").all()
        assert (df["flock_file"] == "").all()


class TestGenerateRsyncCommand:
    """Test the generate_rsync_command function"""

    def _run_generated_transfer_script(self, tmp_path, transfer, notifications=None):
        """Write and execute a generated transfer script under test-local config."""
        managed_root = tmp_path / "managed"
        snapshot = gcf.config.snapshot_state()
        config_kwargs = {
            'output_dir': str(tmp_path / "output"),
            'rit_managed_locations': {'server1': str(managed_root)},
            'rit_managed_folder_structure': {
                'sh_output': 'scripts',
                'crontabs': 'crontab.d',
                'log': 'log',
                'flock': 'flock',
            },
            'flock_paths': {'server1': shutil.which("flock") or '/usr/bin/flock'},
        }
        if notifications is not None:
            config_kwargs['notifications'] = notifications
        gcf.config.load_config(**config_kwargs)
        try:
            script = gcf.generate_script_content(transfer)
        finally:
            gcf.config.restore_state(snapshot)

        script_path = tmp_path / "{0}.sh".format(transfer['identifiers'])
        script_path.write_text(script)
        script_path.chmod(0o755)

        env = dict(os.environ)
        if HAS_RSYNC or HAS_FLOCK:
            path_parts = []
            rsync_path = shutil.which("rsync")
            flock_path = shutil.which("flock")
            if rsync_path:
                path_parts.append(os.path.dirname(rsync_path))
            if flock_path:
                path_parts.append(os.path.dirname(flock_path))
            path_parts.append(env.get("PATH", ""))
            env["PATH"] = os.pathsep.join([part for part in path_parts if part])

        proc = subprocess.run(
            [str(script_path)],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(tmp_path),
            env=env,
        )
        return proc, managed_root
    
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

    def test_remove_stale_generated_scripts_respects_owner_markers(self, tmp_path):
        """Shared cleanup should only remove files owned by the current app."""
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        owned_stale = scripts_dir / "owned.sh"
        other_stale = scripts_dir / "other.sh"
        unowned_stale = scripts_dir / "unowned.sh"
        owned_stale.write_text("#!/bin/sh\n# landingzones-owner: deploy:app\n")
        other_stale.write_text("#!/bin/sh\n# landingzones-owner: other:app\n")
        unowned_stale.write_text("#!/bin/sh\n")

        snapshot = gcf.config.snapshot_state()
        gcf.config.load_config(artifact_owner_id="deploy:app")
        try:
            gcf.remove_stale_generated_scripts(str(scripts_dir), [])
        finally:
            gcf.config.restore_state(snapshot)

        assert not owned_stale.exists()
        assert other_stale.exists()
        assert unowned_stale.exists()

    def test_prefixed_validation_wrappers_are_discoverable_by_flow(self, tmp_path):
        """Artifact prefixes should not change the operator-facing flow key."""
        test_data_root = tmp_path / "toy_data"
        fixture_dir = test_data_root / "lab_machine_1" / "FixtureRun"
        fixture_dir.mkdir(parents=True)
        (fixture_dir / "payload.txt").write_text("payload")
        entry_dir = tmp_path / "tests" / "test_local" / "lab_machine_1" / "Landing_Zone" / "to_server1"
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

        snapshot = gcf.config.snapshot_state()
        gcf.config.load_config(
            artifact_prefix="prod-server1",
            test_data=str(test_data_root),
        )
        try:
            transfers_df = gcf.parse_transfers_file(str(transfers_file))
            names = gcf.validation_script_names(transfers_df)
        finally:
            gcf.config.restore_state(snapshot)

        assert "lz_run_validation_prod-server1.sh" in names
        assert "lz_run_validation_prod-server1__flow_a.sh" in names

    def test_remove_stale_validation_scripts_preserves_validation_outputs(self, tmp_path):
        """Validation helper scripts should be preserved in the validation output dir."""
        test_data_root = tmp_path / "toy_data"
        fixture_dir = test_data_root / "lab_machine_1" / "FixtureRun"
        fixture_dir.mkdir(parents=True)
        (fixture_dir / "payload.txt").write_text("payload")
        entry_dir = tmp_path / "tests" / "test_local" / "lab_machine_1" / "Landing_Zone" / "to_server1"
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
        assert 'if [ -d /dest ]; then find /dest/.staging/transfer -mindepth 1 -maxdepth 1 ! -name \'.staging\' -exec mv {} /dest/ \\; && rmdir /dest/.staging/transfer; else mv /dest/.staging/transfer /dest; fi' in cmd

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
        assert 'ssh -p 2222 user@host "set -eu; if [ -d /dest ]; then find /dest/.staging/sample -mindepth 1 -maxdepth 1 ! -name \'.staging\' -exec mv {} /dest/ \\; && rmdir /dest/.staging/sample; else mv /dest/.staging/sample /dest; fi && rmdir /dest/.staging 2>/dev/null || true"' in cmd

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
        assert 'preflight_log="$(mktemp "${TMPDIR:-/tmp}/landingzones.sample.preflight.XXXXXX")"' in script
        assert 'preflight_stderr_log="$(mktemp "${TMPDIR:-/tmp}/landingzones.sample.preflight-stderr.XXXXXX")"' in script
        assert 'if ! find "$source_dir" -type d -print | while IFS= read -r dir_path; do [ -w "$dir_path" ] && [ -x "$dir_path" ] || printf "%s\\n" "$dir_path"; done >"$preflight_log" 2>"$preflight_stderr_log"; then' in script
        assert 'rsync --dry-run -av --remove-source-files "$source_dir/" "/dest/.staging/$dir_name/" </dev/null >>"$preflight_log" 2>&1' in script
        assert 'if ! rsync -av --remove-source-files "$source_dir/" "/dest/.staging/$dir_name/" </dev/null >>"$run_log" 2>&1; then' in script
        assert 'rsync_message="rsync failed: $(summarize_log "$run_log")"' in script
        assert 'if ! ( if [ -d "/dest/$dir_name" ]; then find "/dest/.staging/$dir_name" -mindepth 1 -maxdepth 1 ! -name ".staging" -exec mv {} "/dest/$dir_name"/ \\; && rmdir "/dest/.staging/$dir_name"; else mv "/dest/.staging/$dir_name" "/dest/$dir_name"; fi; rmdir "/dest/.staging" 2>/dev/null || true ) </dev/null >>"$promote_log" 2>&1; then' in script
        assert 'preflight_message="source cleanup preflight command failed: $(summarize_log "$preflight_stderr_log")"' in script
        assert 'preflight_message="source cleanup preflight failed: $(summarize_log "$preflight_log")"' in script
        assert 'preflight_message="rsync dry-run failed: $(summarize_log "$preflight_log")"' in script
        assert 'promote_message="staging promote failed: see promote log"' in script
        assert 'reset_current_run_context' in script
        assert 'exec 9>"$flock_file"' in script
        assert '/opt/bin/flock -n 9' in script
        assert 'flock_file="/tmp/test.lock"' in script
        assert 'cat "$run_log" >> "$log_file"' in script
        assert 'cat "$preflight_log" >> "$log_file"' in script
        assert 'cat "$preflight_stderr_log" >> "$log_file"' in script
        assert 'mini_log_file="/tmp/test.log.mini"' in script
        assert "printf '%s %s\\n'" in script
        assert 'common_status_log_file="output/log/Landing_Zone_server1.transfers.tsv"' in script
        assert 'common_status_lock_file="output/flock/Landing_Zone_server1.transfers.lock"' in script
        assert 'transfer_tags=""' in script
        assert "printf 'event_time_utc\\ttransfer_identifier\\tsystem\\trun_id\\trun_name\\tflow_group\\ttags\\torigin_system\\tentry_transfer_identifier\\tcreated_at_utc\\tdirectory\\tsource_path\\tdestination_path\\tstatus\\tmessage\\n'" in script
        assert 'append_common_status "initiated" "$dir_name" "$current_run_source" "$current_run_destination"' in script
        assert 'append_common_status "completed" "$dir_name" "$current_run_source" "$current_run_destination"' in script
        assert 'append_common_status "error" "$current_run" "$current_run_source" "$current_run_destination"' in script
        assert 'mkdir -p "$(dirname "$log_file")" "$(dirname "$latest_log_file")" "$(dirname "$mini_log_file")" "$(dirname "$flock_file")"' in script
        assert 'dump_debug_log "run log" "$run_log"' in script
        assert 'dump_debug_log "promote log" "$promote_log"' in script
        assert 'dump_debug_log "cleanup log" "$cleanup_log"' in script
        assert 'dump_debug_log "preflight log" "$preflight_log"' in script
        assert 'dump_debug_log "preflight stderr log" "$preflight_stderr_log"' in script
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
            'tags': 'heartbeat,lab',
        }

        original_runtime_config = dict(gcf.config._runtime_config)
        gcf.config._runtime_config['flock_paths'] = {'server1': '/opt/bin/flock'}
        try:
            script = gcf.generate_script_content(transfer)
        finally:
            gcf.config._runtime_config = original_runtime_config

        assert 'portable_metadata_enabled="1"' in script
        assert 'is_entry_point="TRUE"' in script
        assert 'transfer_tags="heartbeat,lab"' in script
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

    def test_generate_script_content_includes_notification_delivery(self):
        """Generated scripts should derive notification attempts from status events."""
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
            'notify_on_success': 'TRUE',
            'notify_on_error': 'TRUE',
            'tags': 'heartbeat',
        }

        snapshot = gcf.config.snapshot_state()
        gcf.config.load_config(
            rit_managed_locations={'server1': '/srv/rit'},
            rit_managed_folder_structure={
                'log': 'log',
                'flock': 'flock',
            },
            flock_paths={'server1': '/opt/bin/flock'},
            notifications={
                'endpoint': 'https://notify.example/events',
                'token_env': 'NOTIFY_TOKEN',
                'title': 'Transfer event',
                'body': 'A transfer changed state',
                'timeout_seconds': '3',
            },
        )
        try:
            script = gcf.generate_script_content(transfer)
        finally:
            gcf.config.restore_state(snapshot)

        assert "notification_api_endpoint=https://notify.example/events" in script
        assert "notification_token_env=NOTIFY_TOKEN" in script
        assert "notification_title='Transfer event'" in script
        assert "notification_body='A transfer changed state'" in script
        assert "notification_timeout_seconds=3" in script
        assert 'notification_status_log_file="/srv/rit/log/Landing_Zone_server1.notifications.tsv"' in script
        assert 'notification_status_lock_file="/srv/rit/flock/Landing_Zone_server1.notifications.lock"' in script
        assert 'notify_on_success="TRUE"' in script
        assert 'notify_on_error="TRUE"' in script
        assert 'notification_enabled_for_status()' in script
        assert 'notification_already_sent "$idempotency_key" && return 0' in script
        assert "-H \"Idempotency-Key: $idempotency_key\"" in script
        assert 'append_notification_status "$transfer_event_time_utc"' in script
        assert 'notify_transfer_event "$event_timestamp" "$event_status" "$event_directory" "$event_source" "$event_destination" "$event_message"' in script

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
        entry_dir = tmp_path / "tests" / "test_local" / "lab_machine_1" / "Landing_Zone" / "to_server1"
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
        entry_dir = tmp_path / "tests" / "test_local" / "lab_machine_1" / "Landing_Zone" / "to_server1"
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
        entry_dir = tmp_path / "tests" / "test_local" / "lab_machine_1" / "Landing_Zone" / "to_server1"
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
        assert 'if ! rsync -av --remove-source-files "$source_dir/" "/dest/.staging/$dir_name/" </dev/null >>"$run_log" 2>&1; then' in script
        assert 'append_common_status "error" "$dir_name" "$current_run_source" "$current_run_destination" "$rsync_message"' in script
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
        assert 'ssh -p 2222 user@host "set -eu; if [ -d \\"/dest/$dir_name\\" ]; then ' in script
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
        assert 'if ! remote_ssh "$source_remote_target" "$source_remote_port" sh -c \'find "$1" -type d -print | while IFS= read -r dir_path; do [ -w "$dir_path" ] && [ -x "$dir_path" ] || printf "%s\\n" "$dir_path"; done\' sh "$source_dir" >"$preflight_log" 2>"$preflight_stderr_log"; then' in script
        assert 'rsync --dry-run -av --remove-source-files -e \'ssh -p 2200\' "user@remote:$source_dir/" "/dest/.staging/$dir_name/" </dev/null >>"$preflight_log" 2>&1' in script
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

    @pytest.mark.skipif(
        not (HAS_RSYNC and HAS_FLOCK),
        reason="requires rsync and flock",
    )
    def test_generated_script_skips_run_when_hidden_directory_permissions_fail(self, tmp_path):
        """A hidden directory with bad permissions should block that run before transfer."""
        source_root = tmp_path / "source"
        destination_root = tmp_path / "destination"
        run_dir = source_root / "Run1"
        hidden_dir = run_dir / ".cache"
        hidden_file = hidden_dir / ".hidden_payload"
        source_root.mkdir()
        destination_root.mkdir()
        hidden_dir.mkdir(parents=True)
        hidden_file.write_text("hidden")
        hidden_dir.chmod(0o600)

        transfer = {
            'identifiers': 'hidden_permission_failure',
            'system': 'server1',
            'source': str(source_root / '*'),
            'source_port': '',
            'destination': str(destination_root) + '/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': str(tmp_path / 'hidden_permission_failure.log'),
            'flock_file': str(tmp_path / 'hidden_permission_failure.lock'),
            'frequency': '',
        }

        try:
            proc, managed_root = self._run_generated_transfer_script(tmp_path, transfer)
        finally:
            hidden_dir.chmod(0o700)

        common_status_log = managed_root / "log" / "Landing_Zone_server1.transfers.tsv"

        assert proc.returncode == 0
        assert run_dir.exists()
        assert not (destination_root / "Run1").exists()
        assert common_status_log.exists()
        status_text = common_status_log.read_text()
        assert "initiated" in status_text
        assert "error" in status_text
        assert "source cleanup preflight failed" in status_text
        assert ".cache" in status_text

    @pytest.mark.skipif(
        not (HAS_RSYNC and HAS_FLOCK),
        reason="requires rsync and flock",
    )
    def test_generated_script_transfers_hidden_files_when_permissions_are_valid(self, tmp_path):
        """Hidden files inside a selected run should transfer when permissions are fine."""
        source_root = tmp_path / "source"
        destination_root = tmp_path / "destination"
        run_dir = source_root / "RunGood"
        source_root.mkdir()
        destination_root.mkdir()
        run_dir.mkdir()
        (run_dir / "payload.txt").write_text("visible")
        (run_dir / ".hidden_payload").write_text("hidden")

        transfer = {
            'identifiers': 'hidden_permission_success',
            'system': 'server1',
            'source': str(source_root / '*'),
            'source_port': '',
            'destination': str(destination_root) + '/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': str(tmp_path / 'hidden_permission_success.log'),
            'flock_file': str(tmp_path / 'hidden_permission_success.lock'),
            'frequency': '',
        }

        proc, managed_root = self._run_generated_transfer_script(tmp_path, transfer)
        destination_run = destination_root / "RunGood"
        common_status_log = managed_root / "log" / "Landing_Zone_server1.transfers.tsv"

        assert proc.returncode == 0
        assert not run_dir.exists()
        assert destination_run.exists()
        assert (destination_run / "payload.txt").read_text() == "visible"
        assert (destination_run / ".hidden_payload").read_text() == "hidden"
        assert common_status_log.exists()
        status_text = common_status_log.read_text()
        assert "RunGood" in status_text
        assert "completed" in status_text
        assert "source cleanup preflight failed" not in status_text

    @pytest.mark.skipif(
        not (HAS_RSYNC and HAS_FLOCK),
        reason="requires rsync and flock",
    )
    def test_generated_script_writes_notification_log_and_suppresses_duplicate_success(self, tmp_path, monkeypatch):
        """Notification delivery should be tracked separately and deduplicated."""
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        fake_curl = fake_bin / "curl"
        fake_curl.write_text("#!/bin/sh\nprintf '200'\n")
        fake_curl.chmod(0o755)
        monkeypatch.setenv("LANDINGZONES_CURL", str(fake_curl))
        monkeypatch.setenv(
            "PATH",
            "{0}{1}{2}".format(fake_bin, os.pathsep, os.environ.get("PATH", "")),
        )

        source_root = tmp_path / "source"
        destination_root = tmp_path / "destination"
        source_root.mkdir()
        destination_root.mkdir()

        transfer = {
            'identifiers': 'notify_success',
            'system': 'server1',
            'source': str(source_root / '*'),
            'source_port': '',
            'destination': str(destination_root) + '/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'log_file': str(tmp_path / 'notify_success.log'),
            'flock_file': str(tmp_path / 'notify_success.lock'),
            'frequency': '',
            'notify_on_success': 'TRUE',
            'tags': 'heartbeat',
        }
        notifications = {
            'endpoint': 'https://notify.example/events',
            'title': 'Transfer complete',
            'body': 'A transfer completed.',
            'timeout_seconds': '3',
        }

        run_dir = source_root / "RunGood"
        run_dir.mkdir()
        (run_dir / "payload.txt").write_text("first")
        proc, managed_root = self._run_generated_transfer_script(
            tmp_path,
            transfer,
            notifications=notifications,
        )

        assert proc.returncode == 0

        run_dir.mkdir()
        (run_dir / "payload.txt").write_text("second")
        proc, managed_root = self._run_generated_transfer_script(
            tmp_path,
            transfer,
            notifications=notifications,
        )

        notification_log = managed_root / "log" / "Landing_Zone_server1.notifications.tsv"
        rows = notification_log.read_text().splitlines()

        assert proc.returncode == 0
        assert len(rows) == 2
        assert rows[0].startswith("event_time_utc\tnotification_time_utc\tidempotency_key")
        assert "\tnotify_success\tserver1\t" in rows[1]
        assert "\theartbeat\t" in rows[1]
        assert "\tcompleted\tsent\t200\t1\t" in rows[1]


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
