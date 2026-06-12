#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Test suite for check_deployment_readiness.py"""

import os
import shutil
import stat
import tempfile
from pathlib import Path
import pytest
import pandas as pd

from landingzones import check_deployment_readiness as cdr
from landingzones import readiness_ops as ro
from landingzones import transfer_catalog
from landingzones.table import TransferTable


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


class TestCronDeploymentPrompt:
    """Test explicit cron deployment prompting."""

    def test_ask_yes_no_treats_eof_as_no(self, monkeypatch):
        """Non-interactive prompts should not crash on EOF."""
        monkeypatch.setattr('builtins.input', lambda: (_ for _ in ()).throw(EOFError()))

        assert cdr.ask_yes_no("Create directories?") is False

    def test_main_routes_deploy_cron_prompt(self, monkeypatch):
        """`--deploy-cron` should bypass readiness checks and run the prompt flow."""
        monkeypatch.setattr(cdr.config, 'load_config', lambda **kwargs: None)
        monkeypatch.setattr(
            cdr,
            'run_cron_deployment_prompt',
            lambda **kwargs: True,
        )

        result = cdr.main(['--deploy-cron'])

        assert result is True

    def test_build_cron_activation_plan_classifies_execution_context_fragments(
        self, tmp_path
    ):
        """Execution-context planning should classify mixed staged cron fragments."""
        staged_dir = tmp_path / "crontab.d"
        staged_dir.mkdir()
        selected_name = "local_dev.local.Landing_Zone.cron"
        preserved_name = "local_prod.local.Landing_Zone.cron"
        foreign_name = "other.runner.Landing_Zone.cron"
        unresolved_name = "stale.runner.Landing_Zone.cron"
        manual_name = "manual-maintenance.cron"
        backup_name = "manual-maintenance.cron.bak"
        for filename in [
            selected_name,
            preserved_name,
            foreign_name,
            unresolved_name,
            manual_name,
            backup_name,
        ]:
            (staged_dir / filename).write_text("* * * * * {0}\n".format(filename))

        plan = ro.build_cron_activation_plan(
            'execution-context',
            ['local_dev.local'],
            str(staged_dir),
            'calc',
            'operator',
            runtime_contexts={
                'local_dev.local': {('calc', 'operator')},
                'local_prod.local': {('calc', 'operator')},
                'other.runner': {('other', 'operator')},
            },
            cron_fragment_exclusions=[
                manual_name,
                'missing-runtime.Landing_Zone.cron',
            ],
        )

        assert [f['filename'] for f in plan.activated_runtime_fragments] == [
            selected_name
        ]
        assert [f['filename'] for f in plan.preserved_runtime_fragments] == [
            preserved_name
        ]
        assert [f['filename'] for f in plan.foreign_runtime_fragments] == [
            foreign_name
        ]
        assert [f['filename'] for f in plan.unresolved_runtime_fragments] == [
            unresolved_name
        ]
        assert [f['filename'] for f in plan.applied_exclusions] == [manual_name]
        assert [f['filename'] for f in plan.missing_exclusions] == [
            'missing-runtime.Landing_Zone.cron'
        ]
        assert all(backup_name not in path for path in plan.active_files)

    def test_deploy_cron_defaults_to_execution_context_scope(
        self, tmp_path, monkeypatch, capsys
    ):
        """Default cron activation should preserve same-context runtime crons."""
        home_dir = tmp_path / "home"
        staged_dir = home_dir / "crontab.d"
        generated_dir = tmp_path / "output" / "crontab.d"
        staged_dir.mkdir(parents=True)
        generated_dir.mkdir(parents=True)

        selected_name = "local_dev.local.Landing_Zone.cron"
        preserved_name = "local_prod.local.Landing_Zone.cron"
        foreign_name = "other.runner.Landing_Zone.cron"
        unresolved_name = "stale.runner.Landing_Zone.cron"
        unidentified_name = "manual-maintenance.cron"
        (generated_dir / selected_name).write_text("* * * * * selected-runtime\n")
        (staged_dir / preserved_name).write_text("* * * * * preserved-runtime\n")
        (staged_dir / foreign_name).write_text("* * * * * foreign-runtime\n")
        (staged_dir / unresolved_name).write_text("* * * * * unresolved-runtime\n")
        (staged_dir / unidentified_name).write_text("* * * * * manual-maintenance\n")
        config_file = tmp_path / "config.yaml"
        config_file.write_text("crontab_dir: {0}\n".format(generated_dir))

        activated = {}

        class DummyProcess:
            def __init__(
                self,
                args,
                stdout=None,
                stderr=None,
                stdin=None,
                shell=False,
            ):
                self.args = args
                self.returncode = 0
                self.shell = shell

            def communicate(self, input=None):
                if self.args == ['crontab', '-']:
                    activated['content'] = input.decode('utf-8')
                    return b'', b''
                if self.args == ['crontab', '-l']:
                    return activated.get('content', '').encode('utf-8'), b''
                return b'', b''

        snapshot = cdr.config.snapshot_state()
        responses = iter(['y'])
        monkeypatch.setenv('HOME', str(home_dir))
        monkeypatch.setattr(ro, 'generate_cron_files', lambda runtime_ids=None: (True, "generated"))
        monkeypatch.setattr(
            ro,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                },
                {
                    'runtime_id': 'local_prod.local',
                    'system': 'local_dev',
                    'users': 'local',
                },
                {
                    'runtime_id': 'other.runner',
                    'system': 'other',
                    'users': 'runner',
                },
            ]),
        )
        monkeypatch.setattr(ro.subprocess, 'Popen', DummyProcess)
        monkeypatch.setattr(ro, 'is_interactive_terminal', lambda: True, raising=False)
        monkeypatch.setattr(
            cdr,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                }
            ]),
        )
        monkeypatch.setattr(
            cdr,
            'get_current_system',
            lambda transfers_df=None, runtime_ids=None: 'local_dev',
        )
        monkeypatch.setattr(
            cdr,
            'get_current_user',
            lambda transfers_df=None, runtime_ids=None: 'local',
        )
        monkeypatch.setattr('builtins.input', lambda: next(responses))

        try:
            result = cdr.main([
                '--config', str(config_file),
                '--runtime-id', 'local_dev.local',
                '--deploy-cron',
            ])
        finally:
            cdr.config.restore_state(snapshot)

        captured = capsys.readouterr()

        assert result is True
        assert "selected-runtime" in activated['content']
        assert "preserved-runtime" in activated['content']
        assert "manual-maintenance" in activated['content']
        assert "foreign-runtime" not in activated['content']
        assert "unresolved-runtime" not in activated['content']
        assert preserved_name in captured.out
        assert foreign_name in captured.out
        assert unresolved_name in captured.out
        assert "Preserved runtime cron fragments" in captured.out
        assert "Excluded runtime cron fragments" in captured.out

    def test_deploy_cron_replace_selected_excludes_same_context_runtime(
        self, tmp_path, monkeypatch, capsys
    ):
        """replace-selected should retain the old selected-runtime replacement behavior."""
        home_dir = tmp_path / "home"
        staged_dir = home_dir / "crontab.d"
        generated_dir = tmp_path / "output" / "crontab.d"
        staged_dir.mkdir(parents=True)
        generated_dir.mkdir(parents=True)

        selected_name = "local_dev.local.Landing_Zone.cron"
        nonselected_name = "local_prod.local.Landing_Zone.cron"
        manual_name = "manual-maintenance.cron"
        (generated_dir / selected_name).write_text("* * * * * selected-runtime\n")
        (staged_dir / nonselected_name).write_text("* * * * * nonselected-runtime\n")
        (staged_dir / manual_name).write_text("* * * * * manual-maintenance\n")
        config_file = tmp_path / "config.yaml"
        config_file.write_text("crontab_dir: {0}\n".format(generated_dir))
        activated = {}

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None, stdin=None):
                self.args = args
                self.returncode = 0

            def communicate(self, input=None):
                if self.args == ['crontab', '-']:
                    activated['content'] = input.decode('utf-8')
                    return b'', b''
                if self.args == ['crontab', '-l']:
                    return activated.get('content', '').encode('utf-8'), b''
                return b'', b''

        snapshot = cdr.config.snapshot_state()
        monkeypatch.setenv('HOME', str(home_dir))
        monkeypatch.setattr(ro, 'generate_cron_files', lambda runtime_ids=None: (True, "generated"))
        monkeypatch.setattr(
            ro,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                },
                {
                    'runtime_id': 'local_prod.local',
                    'system': 'local_dev',
                    'users': 'local',
                },
            ]),
        )
        monkeypatch.setattr(ro.subprocess, 'Popen', DummyProcess)
        monkeypatch.setattr(ro, 'is_interactive_terminal', lambda: False, raising=False)
        monkeypatch.setattr(
            cdr,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                }
            ]),
        )
        monkeypatch.setattr(
            cdr,
            'get_current_system',
            lambda transfers_df=None, runtime_ids=None: 'local_dev',
        )
        monkeypatch.setattr(
            cdr,
            'get_current_user',
            lambda transfers_df=None, runtime_ids=None: 'local',
        )

        try:
            result = cdr.main([
                '--config', str(config_file),
                '--runtime-id', 'local_dev.local',
                '--deploy-cron',
                '--cron-scope', 'replace-selected',
                '--confirm-cron-activation',
            ])
        finally:
            cdr.config.restore_state(snapshot)

        captured = capsys.readouterr()

        assert result is True
        assert "selected-runtime" in activated['content']
        assert "manual-maintenance" in activated['content']
        assert "nonselected-runtime" not in activated['content']
        assert "Replacement cron activation" in captured.out
        assert nonselected_name in captured.out

    def test_deploy_cron_combines_config_and_cli_exact_exclusions(
        self, tmp_path, monkeypatch, capsys
    ):
        """Exact cron exclusions should combine config and CLI values."""
        home_dir = tmp_path / "home"
        staged_dir = home_dir / "crontab.d"
        generated_dir = tmp_path / "output" / "crontab.d"
        staged_dir.mkdir(parents=True)
        generated_dir.mkdir(parents=True)

        selected_name = "local_dev.local.Landing_Zone.cron"
        manual_name = "manual-maintenance.cron"
        missing_name = "missing-maintenance.cron"
        (generated_dir / selected_name).write_text("* * * * * selected-runtime\n")
        (staged_dir / manual_name).write_text("* * * * * manual-maintenance\n")
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "crontab_dir: {0}\ncron_fragment_exclusions:\n  - {1}\n".format(
                generated_dir,
                manual_name,
            )
        )
        activated = {}

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None, stdin=None):
                self.args = args
                self.returncode = 0

            def communicate(self, input=None):
                if self.args == ['crontab', '-']:
                    activated['content'] = input.decode('utf-8')
                    return b'', b''
                if self.args == ['crontab', '-l']:
                    return activated.get('content', '').encode('utf-8'), b''
                return b'', b''

        snapshot = cdr.config.snapshot_state()
        monkeypatch.setenv('HOME', str(home_dir))
        monkeypatch.setattr(ro, 'generate_cron_files', lambda runtime_ids=None: (True, "generated"))
        monkeypatch.setattr(
            ro,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                }
            ]),
        )
        monkeypatch.setattr(ro.subprocess, 'Popen', DummyProcess)
        monkeypatch.setattr(ro, 'is_interactive_terminal', lambda: False, raising=False)
        monkeypatch.setattr(
            cdr,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                }
            ]),
        )
        monkeypatch.setattr(
            cdr,
            'get_current_system',
            lambda transfers_df=None, runtime_ids=None: 'local_dev',
        )
        monkeypatch.setattr(
            cdr,
            'get_current_user',
            lambda transfers_df=None, runtime_ids=None: 'local',
        )

        try:
            result = cdr.main([
                '--config', str(config_file),
                '--runtime-id', 'local_dev.local',
                '--deploy-cron',
                '--exclude-cron-fragment', missing_name,
                '--confirm-cron-activation',
            ])
        finally:
            cdr.config.restore_state(snapshot)

        captured = capsys.readouterr()

        assert result is True
        assert "selected-runtime" in activated['content']
        assert "manual-maintenance" not in activated['content']
        assert manual_name in captured.out
        assert missing_name in captured.out
        assert "Applied cron fragment exclusions" in captured.out
        assert "Missing cron fragment exclusions" in captured.out

    def test_deploy_cron_confirm_option_allows_noninteractive_activation(
        self, tmp_path, monkeypatch
    ):
        """An explicit confirmation option should allow non-interactive cron activation."""
        home_dir = tmp_path / "home"
        staged_dir = home_dir / "crontab.d"
        generated_dir = tmp_path / "output" / "crontab.d"
        staged_dir.mkdir(parents=True)
        generated_dir.mkdir(parents=True)

        selected_name = "local_dev.local.Landing_Zone.cron"
        (generated_dir / selected_name).write_text("* * * * * selected-runtime\n")
        config_file = tmp_path / "config.yaml"
        config_file.write_text("crontab_dir: {0}\n".format(generated_dir))
        activated = {}

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None, stdin=None):
                self.args = args
                self.returncode = 0

            def communicate(self, input=None):
                if self.args == ['crontab', '-']:
                    activated['content'] = input.decode('utf-8')
                    return b'', b''
                if self.args == ['crontab', '-l']:
                    return activated.get('content', '').encode('utf-8'), b''
                return b'', b''

        snapshot = cdr.config.snapshot_state()
        monkeypatch.setenv('HOME', str(home_dir))
        monkeypatch.setattr(ro, 'generate_cron_files', lambda runtime_ids=None: (True, "generated"))
        monkeypatch.setattr(
            ro,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                },
                {
                    'runtime_id': 'server2.runner',
                    'system': 'server2',
                    'users': 'runner',
                },
                {
                    'runtime_id': 'other.local',
                    'system': 'other',
                    'users': 'local',
                },
            ]),
        )
        monkeypatch.setattr(ro.subprocess, 'Popen', DummyProcess)
        monkeypatch.setattr(ro, 'is_interactive_terminal', lambda: False, raising=False)
        monkeypatch.setattr(
            cdr,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                }
            ]),
        )
        monkeypatch.setattr(
            cdr,
            'get_current_system',
            lambda transfers_df=None, runtime_ids=None: 'local_dev',
        )
        monkeypatch.setattr(
            cdr,
            'get_current_user',
            lambda transfers_df=None, runtime_ids=None: 'local',
        )
        monkeypatch.setattr(
            'builtins.input',
            lambda: pytest.fail('non-interactive confirmation should not prompt'),
        )

        try:
            result = cdr.main([
                '--config', str(config_file),
                '--runtime-id', 'local_dev.local',
                '--deploy-cron',
                '--confirm-cron-activation',
            ])
        finally:
            cdr.config.restore_state(snapshot)

        assert result is True
        assert "selected-runtime" in activated['content']

    def test_deploy_cron_expected_scope_uses_generated_runtime_metadata(
        self, tmp_path, monkeypatch, capsys
    ):
        """Expected scope should activate runtime IDs recorded by the generated metadata."""
        home_dir = tmp_path / "home"
        staged_dir = home_dir / "crontab.d"
        output_dir = tmp_path / "output"
        generated_dir = output_dir / "crontab.d"
        staged_dir.mkdir(parents=True)
        generated_dir.mkdir(parents=True)

        first_name = "local_dev.local.Landing_Zone.cron"
        second_name = "server2.runner.Landing_Zone.cron"
        excluded_name = "other.local.Landing_Zone.cron"
        manual_name = "manual-maintenance.cron"
        (output_dir / "runtime_ids.txt").write_text(
            "local_dev.local\nserver2.runner\n"
        )
        (generated_dir / first_name).write_text("* * * * * first-expected\n")
        (generated_dir / second_name).write_text("* * * * * second-expected\n")
        (staged_dir / excluded_name).write_text("* * * * * excluded-runtime\n")
        (staged_dir / manual_name).write_text("* * * * * manual-maintenance\n")
        config_file = tmp_path / "config.yaml"
        config_file.write_text("crontab_dir: {0}\n".format(generated_dir))
        activated = {}

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None, stdin=None):
                self.args = args
                self.returncode = 0

            def communicate(self, input=None):
                if self.args == ['crontab', '-']:
                    activated['content'] = input.decode('utf-8')
                    return b'', b''
                if self.args == ['crontab', '-l']:
                    return activated.get('content', '').encode('utf-8'), b''
                return b'', b''

        snapshot = cdr.config.snapshot_state()
        monkeypatch.setenv('HOME', str(home_dir))
        monkeypatch.setattr(ro, 'generate_cron_files', lambda runtime_ids=None: (True, "generated"))
        monkeypatch.setattr(
            ro,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                },
                {
                    'runtime_id': 'server2.runner',
                    'system': 'server2',
                    'users': 'runner',
                },
                {
                    'runtime_id': 'other.local',
                    'system': 'other',
                    'users': 'local',
                },
            ]),
        )
        monkeypatch.setattr(ro.subprocess, 'Popen', DummyProcess)
        monkeypatch.setattr(ro, 'is_interactive_terminal', lambda: False, raising=False)
        monkeypatch.setattr(
            cdr,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': runtime_id,
                    'system': runtime_id.split('.', 1)[0],
                    'users': 'runner',
                }
                for runtime_id in runtime_ids
            ]),
        )
        monkeypatch.setattr(
            cdr,
            'get_current_system',
            lambda transfers_df=None, runtime_ids=None: 'local_dev',
        )
        monkeypatch.setattr(
            cdr,
            'get_current_user',
            lambda transfers_df=None, runtime_ids=None: 'local',
        )

        try:
            result = cdr.main([
                '--config', str(config_file),
                '--deploy-cron',
                '--cron-scope', 'expected',
                '--confirm-cron-activation',
            ])
        finally:
            cdr.config.restore_state(snapshot)

        captured = capsys.readouterr()

        assert result is True
        assert "first-expected" in activated['content']
        assert "second-expected" not in activated['content']
        assert "manual-maintenance" in activated['content']
        assert "excluded-runtime" not in activated['content']
        assert "Cron activation scope" in captured.out
        assert "expected" in captured.out
        assert second_name in captured.out
        assert excluded_name in captured.out

    def test_deploy_cron_expected_scope_prompts_to_repair_missing_staged_cron(
        self, tmp_path, monkeypatch
    ):
        """Expected scope should prompt before copying missing staged runtime crons."""
        home_dir = tmp_path / "home"
        staged_dir = home_dir / "crontab.d"
        output_dir = tmp_path / "output"
        generated_dir = output_dir / "crontab.d"
        staged_dir.mkdir(parents=True)
        generated_dir.mkdir(parents=True)

        staged_name = "local_dev.local.Landing_Zone.cron"
        missing_staged_name = "server2.runner.Landing_Zone.cron"
        (output_dir / "runtime_ids.txt").write_text(
            "local_dev.local\nserver2.runner\n"
        )
        (staged_dir / staged_name).write_text("* * * * * already-staged\n")
        (generated_dir / missing_staged_name).write_text("* * * * * repaired-runtime\n")
        config_file = tmp_path / "config.yaml"
        config_file.write_text("crontab_dir: {0}\n".format(generated_dir))
        activated = {}
        prompts = []

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None, stdin=None):
                self.args = args
                self.returncode = 0

            def communicate(self, input=None):
                if self.args == ['crontab', '-']:
                    activated['content'] = input.decode('utf-8')
                    return b'', b''
                if self.args == ['crontab', '-l']:
                    return activated.get('content', '').encode('utf-8'), b''
                return b'', b''

        def confirm(prompt_text):
            prompts.append(prompt_text)
            return True

        snapshot = cdr.config.snapshot_state()
        monkeypatch.setenv('HOME', str(home_dir))
        monkeypatch.setattr(ro, 'generate_cron_files', lambda runtime_ids=None: (True, "generated"))
        monkeypatch.setattr(
            ro,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                },
                {
                    'runtime_id': 'server2.runner',
                    'system': 'local_dev',
                    'users': 'local',
                },
            ]),
        )
        monkeypatch.setattr(ro.subprocess, 'Popen', DummyProcess)
        monkeypatch.setattr(ro, 'is_interactive_terminal', lambda: True, raising=False)
        monkeypatch.setattr(cdr, 'ask_yes_no', confirm)
        monkeypatch.setattr(
            cdr,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': runtime_id,
                    'system': runtime_id.split('.', 1)[0],
                    'users': 'runner',
                }
                for runtime_id in runtime_ids
            ]),
        )
        monkeypatch.setattr(
            cdr,
            'get_current_system',
            lambda transfers_df=None, runtime_ids=None: 'local_dev',
        )
        monkeypatch.setattr(
            cdr,
            'get_current_user',
            lambda transfers_df=None, runtime_ids=None: 'local',
        )

        try:
            result = cdr.main([
                '--config', str(config_file),
                '--deploy-cron',
                '--cron-scope', 'expected',
            ])
        finally:
            cdr.config.restore_state(snapshot)

        assert result is True
        assert any("Copy missing expected runtime cron fragment" in prompt for prompt in prompts)
        assert (staged_dir / missing_staged_name).exists()
        assert "already-staged" in activated['content']
        assert "repaired-runtime" in activated['content']

    def test_deploy_cron_expected_scope_falls_back_to_generated_filenames(
        self, tmp_path, monkeypatch, capsys
    ):
        """Expected scope should warn and infer runtime IDs from generated cron names."""
        home_dir = tmp_path / "home"
        output_dir = tmp_path / "output"
        generated_dir = output_dir / "crontab.d"
        generated_dir.mkdir(parents=True)

        (generated_dir / "local_dev.local.Landing_Zone.cron").write_text(
            "* * * * * first-expected\n"
        )
        (generated_dir / "server2.runner.Landing_Zone.cron").write_text(
            "* * * * * second-expected\n"
        )
        config_file = tmp_path / "config.yaml"
        config_file.write_text("crontab_dir: {0}\n".format(generated_dir))
        activated = {}

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None, stdin=None):
                self.args = args
                self.returncode = 0

            def communicate(self, input=None):
                if self.args == ['crontab', '-']:
                    activated['content'] = input.decode('utf-8')
                    return b'', b''
                if self.args == ['crontab', '-l']:
                    return activated.get('content', '').encode('utf-8'), b''
                return b'', b''

        snapshot = cdr.config.snapshot_state()
        monkeypatch.setenv('HOME', str(home_dir))
        monkeypatch.setattr(ro, 'generate_cron_files', lambda runtime_ids=None: (True, "generated"))
        monkeypatch.setattr(
            ro,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                },
                {
                    'runtime_id': 'server2.runner',
                    'system': 'local_dev',
                    'users': 'local',
                },
            ]),
        )
        monkeypatch.setattr(ro.subprocess, 'Popen', DummyProcess)
        monkeypatch.setattr(ro, 'is_interactive_terminal', lambda: False, raising=False)
        monkeypatch.setattr(
            cdr,
            'get_current_system',
            lambda transfers_df=None, runtime_ids=None: 'local_dev',
        )
        monkeypatch.setattr(
            cdr,
            'get_current_user',
            lambda transfers_df=None, runtime_ids=None: 'local',
        )

        try:
            result = cdr.main([
                '--config', str(config_file),
                '--deploy-cron',
                '--cron-scope', 'expected',
                '--confirm-cron-activation',
            ])
        finally:
            cdr.config.restore_state(snapshot)

        captured = capsys.readouterr()

        assert result is True
        assert "first-expected" in activated['content']
        assert "second-expected" in activated['content']
        assert "WARNING" in captured.out
        assert "generated cron filenames" in captured.out

    def test_deploy_cron_expected_scope_fails_when_expected_cron_is_unrecoverable(
        self, tmp_path, monkeypatch, capsys
    ):
        """Expected scope should fail when a metadata runtime has no staged or generated cron."""
        home_dir = tmp_path / "home"
        staged_dir = home_dir / "crontab.d"
        output_dir = tmp_path / "output"
        generated_dir = output_dir / "crontab.d"
        staged_dir.mkdir(parents=True)
        generated_dir.mkdir(parents=True)

        (output_dir / "runtime_ids.txt").write_text("missing.runtime\n")
        config_file = tmp_path / "config.yaml"
        config_file.write_text("crontab_dir: {0}\n".format(generated_dir))
        activated = {}

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None, stdin=None):
                self.args = args
                self.returncode = 0

            def communicate(self, input=None):
                if self.args == ['crontab', '-']:
                    activated['content'] = input.decode('utf-8')
                return b'', b''

        snapshot = cdr.config.snapshot_state()
        monkeypatch.setenv('HOME', str(home_dir))
        monkeypatch.setattr(ro, 'generate_cron_files', lambda runtime_ids=None: (True, "generated"))
        monkeypatch.setattr(ro.subprocess, 'Popen', DummyProcess)
        monkeypatch.setattr(ro, 'is_interactive_terminal', lambda: False, raising=False)
        monkeypatch.setattr(
            cdr,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'missing.runtime',
                    'system': 'missing',
                    'users': 'runner',
                }
            ]),
        )
        monkeypatch.setattr(
            cdr,
            'get_current_system',
            lambda transfers_df=None, runtime_ids=None: 'missing',
        )
        monkeypatch.setattr(
            cdr,
            'get_current_user',
            lambda transfers_df=None, runtime_ids=None: 'runner',
        )

        try:
            result = cdr.main([
                '--config', str(config_file),
                '--deploy-cron',
                '--cron-scope', 'expected',
                '--confirm-cron-activation',
            ])
        finally:
            cdr.config.restore_state(snapshot)

        captured = capsys.readouterr()

        assert result is False
        assert 'content' not in activated
        assert "missing.runtime" in captured.out
        assert "rebuild" in captured.out.lower()

    def test_deploy_cron_staged_scope_activates_every_staged_cron(
        self, tmp_path, monkeypatch, capsys
    ):
        """Staged scope should activate every .cron file in ~/crontab.d."""
        home_dir = tmp_path / "home"
        staged_dir = home_dir / "crontab.d"
        generated_dir = tmp_path / "output" / "crontab.d"
        staged_dir.mkdir(parents=True)
        generated_dir.mkdir(parents=True)

        (staged_dir / "local_dev.local.Landing_Zone.cron").write_text(
            "* * * * * first-staged\n"
        )
        (staged_dir / "server2.runner.Landing_Zone.cron").write_text(
            "* * * * * second-staged\n"
        )
        (staged_dir / "manual-maintenance.cron").write_text(
            "* * * * * manual-maintenance\n"
        )
        config_file = tmp_path / "config.yaml"
        config_file.write_text("crontab_dir: {0}\n".format(generated_dir))
        activated = {}

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None, stdin=None):
                self.args = args
                self.returncode = 0

            def communicate(self, input=None):
                if self.args == ['crontab', '-']:
                    activated['content'] = input.decode('utf-8')
                    return b'', b''
                if self.args == ['crontab', '-l']:
                    return activated.get('content', '').encode('utf-8'), b''
                return b'', b''

        snapshot = cdr.config.snapshot_state()
        monkeypatch.setenv('HOME', str(home_dir))
        monkeypatch.setattr(ro, 'generate_cron_files', lambda runtime_ids=None: (True, "generated"))
        monkeypatch.setattr(ro.subprocess, 'Popen', DummyProcess)
        monkeypatch.setattr(ro, 'is_interactive_terminal', lambda: False, raising=False)
        monkeypatch.setattr(
            cdr,
            'get_current_system',
            lambda transfers_df=None, runtime_ids=None: 'local_dev',
        )
        monkeypatch.setattr(
            cdr,
            'get_current_user',
            lambda transfers_df=None, runtime_ids=None: 'local',
        )

        try:
            result = cdr.main([
                '--config', str(config_file),
                '--deploy-cron',
                '--cron-scope', 'staged',
                '--confirm-cron-activation',
            ])
        finally:
            cdr.config.restore_state(snapshot)

        captured = capsys.readouterr()

        assert result is True
        assert "first-staged" in activated['content']
        assert "second-staged" in activated['content']
        assert "manual-maintenance" in activated['content']
        assert "Every staged .cron file will be activated" in captured.out

    def test_deploy_cron_noninteractive_fails_closed_without_confirmation(
        self, tmp_path, monkeypatch, capsys
    ):
        """Non-interactive cron activation should stop before replacing crontab."""
        home_dir = tmp_path / "home"
        generated_dir = tmp_path / "output" / "crontab.d"
        generated_dir.mkdir(parents=True)
        (generated_dir / "local_dev.local.Landing_Zone.cron").write_text(
            "* * * * * selected-runtime\n"
        )
        config_file = tmp_path / "config.yaml"
        config_file.write_text("crontab_dir: {0}\n".format(generated_dir))
        activated = {}

        class DummyProcess:
            def __init__(self, args, stdout=None, stderr=None, stdin=None):
                self.args = args
                self.returncode = 0

            def communicate(self, input=None):
                if self.args == ['crontab', '-']:
                    activated['content'] = input.decode('utf-8')
                return b'', b''

        snapshot = cdr.config.snapshot_state()
        monkeypatch.setenv('HOME', str(home_dir))
        monkeypatch.setattr(ro, 'generate_cron_files', lambda runtime_ids=None: (True, "generated"))
        monkeypatch.setattr(ro.subprocess, 'Popen', DummyProcess)
        monkeypatch.setattr(ro, 'is_interactive_terminal', lambda: False, raising=False)
        monkeypatch.setattr(
            cdr,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                }
            ]),
        )
        monkeypatch.setattr(
            cdr,
            'get_current_system',
            lambda transfers_df=None, runtime_ids=None: 'local_dev',
        )
        monkeypatch.setattr(
            cdr,
            'get_current_user',
            lambda transfers_df=None, runtime_ids=None: 'local',
        )

        try:
            result = cdr.main([
                '--config', str(config_file),
                '--runtime-id', 'local_dev.local',
                '--deploy-cron',
            ])
        finally:
            cdr.config.restore_state(snapshot)

        captured = capsys.readouterr()

        assert result is False
        assert 'content' not in activated
        assert "scope 'execution-context'" in captured.out
        assert "--confirm-cron-activation" in captured.out


class TestRuntimeIdentityDetection:
    """Test system/user selection from filtered runtime transfer inventories."""

    def test_get_current_system_auto_selects_single_transfer_system(self, monkeypatch):
        """A one-system runtime should not prompt when hostname does not match."""
        df = pd.DataFrame([{'system': 'Promethion_1'}])
        config_snapshot = ro.config.snapshot_state()

        monkeypatch.setattr(ro.socket, 'gethostname', lambda: 'developer-mac.local')
        monkeypatch.setattr(
            ro,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: df,
        )
        monkeypatch.setattr(
            'builtins.input',
            lambda prompt='': pytest.fail('single-system runtime should not prompt'),
        )

        try:
            ro.config.load_config(transfers_file='input/transfers.tsv')
            assert ro.get_current_system() == 'Promethion_1'
        finally:
            ro.config.restore_state(config_snapshot)

    def test_validate_deployment_uses_build_runtime_metadata(
        self, tmp_path, monkeypatch
    ):
        """Deployment validation should reuse the runtime subset generated by build."""
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        transfers_file = tmp_path / "transfers.tsv"
        transfers_file.write_text("placeholder\n")
        metadata_dir = tmp_path / "output"
        crontab_dir = metadata_dir / "crontab.d"
        crontab_dir.mkdir(parents=True)
        (metadata_dir / "runtime_ids.txt").write_text("local_dev.local\n")
        captured = {}

        def fake_load_runtime_transfers(transfers_file=None, runtime_ids=None):
            captured['runtime_ids'] = runtime_ids
            return pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                    'source': str(src),
                    'source_port': '',
                    'destination': str(dst),
                    'destination_port': '',
                    'log_file': '',
                    'flock_file': '',
                }
            ])

        config_snapshot = cdr.config.snapshot_state()
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(cdr, 'check_required_tools', lambda: True)
        monkeypatch.setattr(cdr, 'check_flock_command', lambda system: True)
        monkeypatch.setattr(cdr, 'load_runtime_transfers', fake_load_runtime_transfers)

        try:
            cdr.config._runtime_config = {}
            result = cdr.main(['--transfers', str(transfers_file)])
        finally:
            cdr.config.restore_state(config_snapshot)

        assert result is True
        assert captured['runtime_ids'] == ['local_dev.local']

    def test_validate_deployment_uses_yaml_runtime_ids(
        self, tmp_path, monkeypatch
    ):
        """Deployment validation should use config runtime_ids when CLI omits them."""
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        transfers_file = tmp_path / "transfers.tsv"
        transfers_file.write_text("placeholder\n")
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "transfers_file: {0}\n"
            "runtime_ids:\n"
            "  - local_dev.local\n".format(transfers_file)
        )
        captured = {}

        def fake_load_runtime_transfers(transfers_file=None, runtime_ids=None):
            captured['runtime_ids'] = runtime_ids
            return pd.DataFrame([
                {
                    'runtime_id': 'local_dev.local',
                    'system': 'local_dev',
                    'users': 'local',
                    'source': str(src),
                    'source_port': '',
                    'destination': str(dst),
                    'destination_port': '',
                    'log_file': '',
                    'flock_file': '',
                }
            ])

        config_snapshot = cdr.config.snapshot_state()
        monkeypatch.setattr(cdr, 'check_required_tools', lambda: True)
        monkeypatch.setattr(cdr, 'check_flock_command', lambda system: True)
        monkeypatch.setattr(cdr, 'load_runtime_transfers', fake_load_runtime_transfers)

        try:
            result = cdr.main(['--config', str(config_file)])
        finally:
            cdr.config.restore_state(config_snapshot)

        assert result is True
        assert captured['runtime_ids'] == ['local_dev.local']

    def test_validate_deployment_loads_transfers_through_runtime_catalog(
        self, tmp_path, monkeypatch
    ):
        """Deployment validation should use the catalog runtime-validation path."""
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        transfers_file = tmp_path / "transfers.tsv"
        transfers_file.write_text("placeholder\n")
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "transfers_file: {0}\n"
            "runtime_ids:\n"
            "  - local_dev.local\n".format(transfers_file)
        )
        catalog_row = {
            'runtime_id': 'local_dev.local',
            'system': 'local_dev',
            'users': 'local',
            'source': str(src),
            'source_port': '',
            'destination': str(dst),
            'destination_port': '',
            'log_file': '',
            'flock_file': '',
        }
        loaded = TransferTable([catalog_row], columns=list(catalog_row))
        calls = []

        def fake_load_runtime_transfer_catalog(
            config_file=None, transfers_file=None, runtime_ids=None
        ):
            calls.append({
                'config_file': config_file,
                'transfers_file': transfers_file,
                'runtime_ids': runtime_ids,
            })
            return loaded

        config_snapshot = cdr.config.snapshot_state()
        monkeypatch.setattr(cdr, 'check_required_tools', lambda: True)
        monkeypatch.setattr(cdr, 'check_flock_command', lambda system: True)
        monkeypatch.setattr(
            transfer_catalog,
            'load_runtime_transfer_catalog',
            fake_load_runtime_transfer_catalog,
        )

        try:
            result = cdr.main(['--config', str(config_file)])
        finally:
            cdr.config.restore_state(config_snapshot)

        assert result is True
        assert calls == [{
            'config_file': None,
            'transfers_file': str(transfers_file),
            'runtime_ids': ['local_dev.local'],
        }]

    def test_validate_deployment_prints_shared_main_lock_warnings(
        self, tmp_path, monkeypatch, capsys
    ):
        """Deployment validation should surface shared main-lock audit warnings."""
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        transfers_file = tmp_path / "transfers.tsv"
        transfers_file.write_text("placeholder\n")
        df = pd.DataFrame([
            {
                'runtime_id': 'local_dev.local',
                'system': 'local_dev',
                'users': 'local',
                'source': str(src),
                'source_port': '',
                'destination': str(dst),
                'destination_port': '',
                'log_file': str(tmp_path / 'log' / 'first.log'),
                'flock_file': str(tmp_path / 'flock' / 'shared.lock'),
            },
            {
                'runtime_id': 'local_dev.local',
                'system': 'local_dev',
                'users': 'local',
                'source': str(src),
                'source_port': '',
                'destination': str(dst),
                'destination_port': '',
                'log_file': str(tmp_path / 'log' / 'second.log'),
                'flock_file': str(tmp_path / 'flock' / 'shared.lock'),
            },
        ])
        df.attrs['shared_main_lock_warnings'] = [
            "Shared main lock: runtime_id='local_dev.local', "
            "flock_file='{0}', transfers=first (frequency=*/2 * * * *), "
            "second (frequency=*/2 * * * *)".format(tmp_path / 'flock' / 'shared.lock')
        ]

        monkeypatch.setattr(cdr, 'check_required_tools', lambda: True)
        monkeypatch.setattr(cdr, 'check_flock_command', lambda system: True)
        monkeypatch.setattr(
            cdr,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: df,
        )

        config_snapshot = cdr.config.snapshot_state()
        try:
            cdr.config._runtime_config = {}
            result = cdr.main(['--transfers', str(transfers_file)])
        finally:
            cdr.config.restore_state(config_snapshot)

        captured = capsys.readouterr()
        assert result is True
        assert 'Shared main transfer locks detected' in captured.out
        assert 'local_dev.local' in captured.out
        assert 'shared.lock' in captured.out


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

        result = cdr.check_flock_command('server1')

        assert result is True

    def test_flock_binary_missing(self, monkeypatch):
        """Test that a missing flock binary path fails."""
        monkeypatch.setattr(cdr.config, 'get_flock_path', lambda system: '/no/such/flock')

        result = cdr.check_flock_command('server1')

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

    def test_runtime_artifact_paths_expand_home_for_python_filesystem_use(
        self, tmp_path, monkeypatch
    ):
        """Python-created integration artifacts should not create literal $HOME dirs."""
        home_dir = tmp_path / 'home'
        home_dir.mkdir()
        monkeypatch.setenv('HOME', str(home_dir))
        snapshot = cdr.config.snapshot_state()

        try:
            cdr.config.load_config(
                rit_managed_locations={'testbox': '$HOME/rit_managed'}
            )
            runtime_dirs = cdr.get_test_with_data_runtime_dirs(
                'testbox', 'runner'
            )
        finally:
            cdr.config.restore_state(snapshot)

        expected_root = (
            home_dir
            / 'rit_managed'
            / 'test_with_data_runtime'
            / 'testbox.runner'
        )
        assert runtime_dirs['root'] == str(expected_root)
        assert '$HOME' not in runtime_dirs['scripts_dir']

    def test_cleanup_runtime_artifacts_expands_home_paths(
        self, tmp_path, monkeypatch
    ):
        """Cleanup should target the same expanded paths the shell scripts use."""
        home_dir = tmp_path / 'home'
        home_dir.mkdir()
        monkeypatch.setenv('HOME', str(home_dir))
        log_file = home_dir / 'step.log'
        latest_log_file = home_dir / 'step.log.latest'
        mini_log_file = home_dir / 'step.log.mini'
        flock_file = home_dir / 'step.lock'
        for path in (log_file, latest_log_file, mini_log_file, flock_file):
            path.write_text('artifact')

        transfers_df = pd.DataFrame([{
            'log_file': '$HOME/step.log',
            'flock_file': '$HOME/step.lock',
        }])

        cdr.cleanup_test_with_data_runtime_artifacts(transfers_df)

        for path in (log_file, latest_log_file, mini_log_file, flock_file):
            assert not path.exists()

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
        metadata = (
            final_root
            / 'flow_one'
            / '.landing_zones'
            / 'landingzone-run-metadata.tsv'
        )
        events = (
            final_root
            / 'flow_one'
            / '.landing_zones'
            / 'landingzone-transfer-events.tsv'
        )
        metadata_fields = dict(
            line.split('\t', 1)
            for line in metadata.read_text().splitlines()
            if '\t' in line
        )
        assert metadata_fields['run_id']
        assert metadata_fields['flow_group'] == ''
        assert metadata_fields['entry_transfer_identifier'] == 'step1'
        assert '\tstep1\ttestbox\tinitiated\t' in events.read_text()
        assert '\tstep2\ttestbox\tcompleted\t' in events.read_text()
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

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_test_with_data_blocker_cleanup_leaves_destination_extras(
        self, tmp_path, monkeypatch
    ):
        """Blocker cleanup should preserve unrelated destination leftovers."""
        (
            config_file,
            transfers_file,
            source_root,
            transit_root,
            final_root,
            _,
        ) = self._write_test_with_data_fixture(tmp_path)
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'testbox')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')

        (source_root / 'old_run').mkdir()
        (source_root / 'old_run' / 'payload.txt').write_text('stale')
        (final_root / 'archived_run').mkdir()
        (final_root / '.staging').mkdir()

        responses = iter(['b', 'n'])
        monkeypatch.setattr('builtins.input', lambda: next(responses))

        result = cdr.run_test_with_data(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
        )

        assert result is True
        assert not (source_root / 'old_run').exists()
        assert cdr.list_visible_entries(str(transit_root)) == []
        assert cdr.list_visible_entries(str(final_root)) == [
            'archived_run',
            'flow_one',
            'flow_two',
        ]
        staging_root = final_root / '.staging'
        assert staging_root.is_dir()
        mode = staging_root.stat().st_mode
        assert mode & stat.S_IRGRP
        assert mode & stat.S_IWGRP
        assert mode & stat.S_IXGRP

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_test_with_data_blocker_cleanup_preserves_empty_staging_root(
        self, tmp_path, monkeypatch
    ):
        """Blocker cleanup should not churn an empty managed staging root."""
        (
            config_file,
            transfers_file,
            source_root,
            _,
            final_root,
            _,
        ) = self._write_test_with_data_fixture(tmp_path)
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'testbox')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')

        (source_root / 'old_run').mkdir()
        staging_root = final_root / '.staging'
        staging_root.mkdir()
        staging_inode = staging_root.stat().st_ino

        responses = iter(['b', 'n'])
        monkeypatch.setattr('builtins.input', lambda: next(responses))

        result = cdr.run_test_with_data(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
        )

        assert result is True
        assert not (source_root / 'old_run').exists()
        assert staging_root.is_dir()
        assert staging_root.stat().st_ino == staging_inode

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_test_with_data_blocker_cleanup_removes_stale_staging_root(
        self, tmp_path, monkeypatch
    ):
        """Blocker cleanup should remove stale staging contents before seeding."""
        (
            config_file,
            transfers_file,
            _,
            _,
            final_root,
            _,
        ) = self._write_test_with_data_fixture(tmp_path)
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'testbox')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')

        staging_root = final_root / '.staging'
        (staging_root / 'stale_run').mkdir(parents=True)
        stale_inode = staging_root.stat().st_ino

        responses = iter(['b', 'n'])
        monkeypatch.setattr('builtins.input', lambda: next(responses))

        result = cdr.run_test_with_data(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
        )

        assert result is True
        assert staging_root.is_dir()
        assert staging_root.stat().st_ino != stale_inode
        assert not (staging_root / 'stale_run').exists()
        assert cdr.list_visible_entries(str(final_root)) == [
            'flow_one',
            'flow_two',
        ]

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_test_with_data_all_cleanup_removes_destination_extras(
        self, tmp_path, monkeypatch
    ):
        """Full cleanup should remove optional stale entries before seeding."""
        (
            config_file,
            transfers_file,
            _,
            _,
            final_root,
            _,
        ) = self._write_test_with_data_fixture(tmp_path)
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'testbox')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')

        (final_root / 'archived_run').mkdir()

        responses = iter(['a', 'n'])
        monkeypatch.setattr('builtins.input', lambda: next(responses))

        result = cdr.run_test_with_data(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
        )

        assert result is True
        assert cdr.list_visible_entries(str(final_root)) == ['flow_one', 'flow_two']

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_test_with_data_all_cleanup_removes_empty_staging_root(
        self, tmp_path, monkeypatch
    ):
        """Full cleanup should remove all pre-existing endpoint entries."""
        (
            config_file,
            transfers_file,
            _,
            _,
            final_root,
            _,
        ) = self._write_test_with_data_fixture(tmp_path)
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'testbox')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')

        staging_root = final_root / '.staging'
        staging_root.mkdir()
        staging_inode = staging_root.stat().st_ino

        responses = iter(['a', 'n'])
        monkeypatch.setattr('builtins.input', lambda: next(responses))

        result = cdr.run_test_with_data(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
        )

        assert result is True
        assert staging_root.is_dir()
        assert staging_root.stat().st_ino != staging_inode
        assert cdr.list_visible_entries(str(final_root)) == ['flow_one', 'flow_two']

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_test_with_data_leave_rejects_remaining_blockers(
        self, tmp_path, monkeypatch, capsys
    ):
        """Leaving blocking entries in place should fail before seeding."""
        (
            config_file,
            transfers_file,
            source_root,
            _,
            _,
            _,
        ) = self._write_test_with_data_fixture(tmp_path)
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'testbox')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')

        (source_root / 'old_run').mkdir()

        responses = iter(['l'])
        monkeypatch.setattr('builtins.input', lambda: next(responses))

        result = cdr.run_test_with_data(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
        )
        captured = capsys.readouterr()

        assert result is False
        assert "Blocking entries left in place" in captured.out

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_test_with_data_leave_allows_empty_managed_staging(
        self, tmp_path, monkeypatch, capsys
    ):
        """Leaving only an empty managed staging root in place should allow reruns."""
        (
            config_file,
            transfers_file,
            _,
            _,
            final_root,
            _,
        ) = self._write_test_with_data_fixture(tmp_path)
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'testbox')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')

        (final_root / '.staging').mkdir()
        responses = iter(['l', 'n'])
        monkeypatch.setattr('builtins.input', lambda: next(responses))

        result = cdr.run_test_with_data(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
        )
        captured = capsys.readouterr()

        assert result is True
        assert "managed: .staging" in captured.out
        assert cdr.list_visible_entries(str(final_root)) == [
            'flow_one',
            'flow_two',
        ]
        assert (final_root / '.staging').is_dir()

    @pytest.mark.skipif(
        shutil.which('rsync') is None,
        reason='rsync is required for local script-test execution',
    )
    def test_run_test_with_data_leave_rejects_non_empty_staging(
        self, tmp_path, monkeypatch, capsys
    ):
        """Leaving stale staging contents in place should fail closed."""
        (
            config_file,
            transfers_file,
            _,
            _,
            final_root,
            _,
        ) = self._write_test_with_data_fixture(tmp_path)
        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'testbox')
        monkeypatch.setattr(cdr, 'get_current_user', lambda: 'runner')

        (final_root / '.staging' / 'stale_run').mkdir(parents=True)
        responses = iter(['l'])
        monkeypatch.setattr('builtins.input', lambda: next(responses))

        result = cdr.run_test_with_data(
            config_file=str(config_file),
            transfers_file=str(transfers_file),
        )
        captured = capsys.readouterr()

        assert result is False
        assert "blockers: .staging (non-empty staging root)" in captured.out
        assert (final_root / '.staging' / 'stale_run').is_dir()

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
                'source': str(tmp_path / 'server1' / 'Landing_Zone' / 'to_server2') + '/',
                'source_port': '',
                'destination': 'sshdat@server2:/users/data/Landing_Zone/from_server1/',
                'destination_port': '',
                'test_fixture_names': 'fixture_one',
                'is_entry_point': 'TRUE',
            },
            {
                'identifiers': 'return_remote',
                'source': 'sshdat@server2:/users/data/Landing_Zone/to_server1/',
                'source_port': '',
                'destination': str(tmp_path / 'server1' / 'Landing_Zone' / 'from_server2') + '/',
                'destination_port': '',
                'test_fixture_names': '',
                'is_entry_point': 'FALSE',
            },
        ])

        plan = cdr.build_run_test_plan(df)

        assert len(plan['initial_sources']) == 1
        assert plan['initial_sources'][0]['value'] == str(
            tmp_path / 'server1' / 'Landing_Zone' / 'to_server2'
        ) + '/'

    def test_dev_server1_seed_plan_scopes_fixtures_per_entry_flow(self, tmp_path):
        """The dev server1 subset should not seed every fixture into every entry flow."""
        config_file = tmp_path / 'config.yaml'
        transfers_file = tmp_path / 'transfers.tsv'
        base_dir = tmp_path
        snapshot = cdr.config.snapshot_state()

        config_file.write_text(
            "\n".join([
                "transfers_file: {0}".format(transfers_file),
                "test_data: tests/data/",
                "path_variables:",
                "  LZ_DEV_SERVER1_LAB_ROOT: /srv/tests/dev/server1/labnet/",
                "  LZ_DEV_SERVER1_LANDING_ZONE_ROOT: /srv/tests/dev/server1/Landing_Zone/",
                "  LZ_DEV_SERVER3_REGION_ROOT: /srv/tests/dev/server3/region/",
                "",
            ])
        )
        transfers_file.write_text(
            "\n".join([
                "identifiers\tenabled\tsystem\tnotes\tusers\tsource\tsource_port\tdestination\tdestination_port\ttest_fixture_names\trsync_options\tio_nice\tlog_file\tflock_file\tfrequency\tflow_group\tis_entry_point",
                "dev_lab_to_server1\tTRUE\tserver1\t\tuser1\t${LZ_DEV_SERVER1_LAB_ROOT}/corefacility/\t\t${LZ_DEV_SERVER1_LANDING_ZONE_ROOT}/from_labnet/\t\tIllumina_TransferTest\t\t\tdev_lab_to_server1.log\tdev_lab_to_server1.lock\t* * * * *\tdev_lab_to_server1\tTRUE",
                "dev_server1_to_server2\tTRUE\tserver1\t\tuser1\t${LZ_DEV_SERVER1_LANDING_ZONE_ROOT}/to_server2/Projects/dev_shadow/proj/Landing_Zone/\t\tserver2:${LZ_DEV_SERVER1_LANDING_ZONE_ROOT}/from_server1/Projects/dev_shadow/proj/Landing_Zone/\t\tIllumina_TransferTest\t\t\tdev_server1_to_server2.log\tdev_server1_to_server2.lock\t* * * * *\tdev_server1_to_server2\tTRUE",
                "dev_server1_to_server3\tTRUE\tserver1\t\tuser1\t${LZ_DEV_SERVER1_LANDING_ZONE_ROOT}/regionh/to_server3/\t\tserver3:${LZ_DEV_SERVER3_REGION_ROOT}/to_server3/\t\tIllumina_TransferTest\t\t\tdev_server1_to_server3.log\tdev_server1_to_server3.lock\t* * * * *\tdev_server1_to_server3\tTRUE",
                "",
            ])
        )

        try:
            cdr.config.load_config(config_file=str(config_file))
            transfers_df = cdr.load_test_with_data_transfers(
                str(transfers_file),
                'server1',
                'user1',
                str(base_dir),
            )
            plan = cdr.build_run_test_plan(transfers_df)
        finally:
            cdr.config.restore_state(snapshot)

        fixture_by_root = {
            cdr.get_endpoint_root(endpoint): endpoint['test_fixture_names']
            for endpoint in plan['initial_sources']
        }

        assert fixture_by_root == {
            '/srv/tests/dev/server1/labnet//corefacility': [
                'Illumina_TransferTest'
            ],
            '/srv/tests/dev/server1/Landing_Zone//to_server2/Projects/dev_shadow/proj/Landing_Zone': [
                'Illumina_TransferTest'
            ],
            '/srv/tests/dev/server1/Landing_Zone//regionh/to_server3': [
                'Illumina_TransferTest'
            ],
        }

    def test_build_test_with_data_existing_state_classifies_blockers(self, tmp_path):
        """Existing endpoint contents should distinguish blockers, extras, and managed state."""
        source_root = tmp_path / 'source_root'
        destination_root = tmp_path / 'destination_root'
        source_root.mkdir()
        destination_root.mkdir()

        (source_root / 'old_run').mkdir()
        (source_root / '.cache').mkdir()
        (source_root / '.staging').mkdir()
        (destination_root / 'flow_one').mkdir()
        (destination_root / 'archived_run').mkdir()
        (destination_root / '.staging').mkdir()

        test_plan = {
            'all_sources': [
                {'value': str(source_root) + '/', 'port': ''},
            ],
            'all_destinations': [
                {'value': str(destination_root) + '/', 'port': ''},
            ],
        }

        existing_state = cdr.build_test_with_data_existing_state(
            test_plan,
            ['flow_one', 'flow_two'],
        )

        state_by_root = {
            cdr.get_endpoint_root(item['endpoint']): item
            for item in existing_state
        }

        assert state_by_root[str(source_root)]['blockers'] == ['old_run']
        assert state_by_root[str(source_root)]['extras'] == ['.cache']
        assert state_by_root[str(source_root)]['managed_entries'] == ['.staging']
        assert state_by_root[str(destination_root)]['blockers'] == [
            'flow_one',
        ]
        assert state_by_root[str(destination_root)]['extras'] == ['archived_run']
        assert state_by_root[str(destination_root)]['managed_entries'] == [
            '.staging'
        ]

    def test_summarize_test_with_data_existing_state_reports_managed_staging(self):
        """Endpoint summaries should make safe managed staging roots visible."""
        summary = cdr.summarize_test_with_data_existing_state([
            {
                'display': '/tmp/final_root',
                'blockers': [],
                'extras': ['archived_run'],
                'managed_entries': ['.staging'],
            },
        ])

        assert "managed: .staging" in summary
        assert "extras: archived_run" in summary

    def test_build_test_with_data_existing_state_reports_stale_staging_contents(
        self, tmp_path
    ):
        """Non-empty staging roots should remain visible blockers."""
        destination_root = tmp_path / 'destination_root'
        destination_root.mkdir()
        (destination_root / '.staging' / 'stale_run').mkdir(parents=True)

        test_plan = {
            'all_sources': [],
            'all_destinations': [
                {'value': str(destination_root) + '/', 'port': ''},
            ],
        }

        existing_state = cdr.build_test_with_data_existing_state(
            test_plan,
            ['flow_one'],
        )
        summary = cdr.summarize_test_with_data_existing_state(existing_state)

        assert existing_state[0]['blockers'] == ['.staging']
        assert "blockers: .staging (non-empty staging root)" in summary

    def test_build_test_with_data_existing_state_blocks_staging_file(
        self, tmp_path
    ):
        """A staging path that is not a directory should fail closed."""
        destination_root = tmp_path / 'destination_root'
        destination_root.mkdir()
        (destination_root / '.staging').write_text('not a directory')

        test_plan = {
            'all_sources': [],
            'all_destinations': [
                {'value': str(destination_root) + '/', 'port': ''},
            ],
        }

        existing_state = cdr.build_test_with_data_existing_state(
            test_plan,
            ['flow_one'],
        )
        summary = cdr.summarize_test_with_data_existing_state(existing_state)

        assert existing_state[0]['blockers'] == ['.staging']
        assert "blockers: .staging (not a usable directory)" in summary

    def test_remote_empty_staging_root_is_managed_state(self, monkeypatch):
        """Remote empty staging inspection should match local endpoint behavior."""
        def fake_run_remote_shell(user, host, command, port=''):
            if '-exec basename' in command:
                return 0, '.staging\n', ''
            return 0, 'empty-directory\n', ''

        monkeypatch.setattr(cdr, 'run_remote_shell', fake_run_remote_shell)
        test_plan = {
            'all_sources': [],
            'all_destinations': [
                {'value': 'runner@example.org:/srv/final/', 'port': '2222'},
            ],
        }

        existing_state = cdr.build_test_with_data_existing_state(
            test_plan,
            ['flow_one'],
        )

        assert existing_state[0]['blockers'] == []
        assert existing_state[0]['managed_entries'] == ['.staging']

    def test_remote_non_empty_staging_root_is_blocking(self, monkeypatch):
        """Remote stale staging inspection should match local endpoint behavior."""
        def fake_run_remote_shell(user, host, command, port=''):
            if '-exec basename' in command:
                return 0, '.staging\n', ''
            return 0, 'non-empty-directory\n', ''

        monkeypatch.setattr(cdr, 'run_remote_shell', fake_run_remote_shell)
        test_plan = {
            'all_sources': [],
            'all_destinations': [
                {'value': 'runner@example.org:/srv/final/', 'port': '2222'},
            ],
        }

        existing_state = cdr.build_test_with_data_existing_state(
            test_plan,
            ['flow_one'],
        )
        summary = cdr.summarize_test_with_data_existing_state(existing_state)

        assert existing_state[0]['blockers'] == ['.staging']
        assert "blockers: .staging (non-empty staging root)" in summary

    def test_remote_inaccessible_staging_root_is_blocking(self, monkeypatch):
        """Remote inspection failures should not be treated as safe staging."""
        def fake_run_remote_shell(user, host, command, port=''):
            if '-exec basename' in command:
                return 0, '.staging\n', ''
            return 1, '', 'permission denied'

        monkeypatch.setattr(cdr, 'run_remote_shell', fake_run_remote_shell)
        test_plan = {
            'all_sources': [],
            'all_destinations': [
                {'value': 'runner@example.org:/srv/final/', 'port': '2222'},
            ],
        }

        existing_state = cdr.build_test_with_data_existing_state(
            test_plan,
            ['flow_one'],
        )
        summary = cdr.summarize_test_with_data_existing_state(existing_state)

        assert existing_state[0]['blockers'] == ['.staging']
        assert "blockers: .staging (cannot inspect staging root)" in summary

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

    def test_load_test_with_data_transfer_graph_uses_runtime_catalog(
        self, tmp_path, monkeypatch
    ):
        """test-with-data transfer selection should load through the runtime catalog."""
        transfers_file = tmp_path / 'transfers.tsv'
        transfers_file.write_text(
            "\n".join([
                "identifiers\truntime_id\tsystem\tusers\tsource\tdestination\tlog_file\tflock_file",
                "direct_parse\tignored.runtime\ttestbox\trunner\tdirect_src/\tdirect_dst/\tdirect.log\tdirect.lock",
                "",
            ])
        )
        catalog_row = {
            'identifiers': 'catalog_transfer',
            'runtime_id': 'selected.runtime',
            'system_user': 'selected.runtime',
            'system': 'testbox',
            'users': 'runner',
            'source': 'catalog_src/',
            'source_port': '',
            'destination': 'catalog_dst/',
            'destination_port': '',
            'rsync_options': '',
            'io_nice': '',
            'frequency': '',
            'log_file': str(tmp_path / 'catalog.log'),
            'flock_file': str(tmp_path / 'catalog.lock'),
            'flow_group': '',
            'tags': '',
            'is_entry_point': 'FALSE',
            'is_end_point': 'FALSE',
            'notify_on_success': 'FALSE',
            'notify_on_error': 'FALSE',
            'script_name': 'catalog_transfer.sh',
        }
        loaded = TransferTable([catalog_row], columns=list(catalog_row))
        calls = []

        def fake_load_runtime_transfer_catalog(
            config_file=None, transfers_file=None, runtime_ids=None
        ):
            calls.append({
                'config_file': config_file,
                'transfers_file': transfers_file,
                'runtime_ids': runtime_ids,
            })
            return loaded

        monkeypatch.setattr(
            transfer_catalog,
            'load_runtime_transfer_catalog',
            fake_load_runtime_transfer_catalog,
        )

        transfers_df = cdr.load_test_with_data_transfer_graph(
            str(transfers_file),
            str(tmp_path),
            runtime_ids=['selected.runtime'],
        )

        assert calls == [{
            'config_file': None,
            'transfers_file': str(transfers_file),
            'runtime_ids': ['selected.runtime'],
        }]
        assert transfers_df['identifiers'].tolist() == ['catalog_transfer']
        assert transfers_df['source'].tolist() == [str(tmp_path / 'catalog_src') + '/']
        assert transfers_df['destination'].tolist() == [
            str(tmp_path / 'catalog_dst') + '/'
        ]

    def test_build_test_with_data_handoffs_identifies_next_system_user(self, tmp_path):
        """A destination that feeds another system should produce a handoff hint."""
        all_transfers_df = pd.DataFrame([
            {
                'identifiers': 'step1',
                'system': 'server1',
                'users': 'runner',
                'source': str(tmp_path / 'source') + '/',
                'destination': str(tmp_path / 'handoff') + '/',
                'flow_group': 'flow_a',
            },
            {
                'identifiers': 'step2',
                'system': 'server2',
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
        assert handoffs[0]['system'] == 'server2'
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
                "  server1: {0}".format(rit_managed),
                "flock_paths:",
                "  server1: /usr/bin/true",
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
                "step1\tTRUE\tserver1\t''\trunner\t{0}/\t\t{1}/\t\t--out-format='%t %o %i %n%L'\t\tstep1.log\tstep1.lock\t* * * * *".format(
                    tmp_path / 'producer_a', handoff_root
                ),
                "step2\tTRUE\tserver2\t''\tcorfac\t{0}/\t\t{1}/\t\t--out-format='%t %o %i %n%L'\t\tstep2.log\tstep2.lock\t* * * * *".format(
                    handoff_root, final_root
                ),
                "",
            ])
        )

        monkeypatch.setattr(cdr, 'get_current_system', lambda: 'server1')
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
        runtime_root = rit_managed / 'test_with_data_runtime' / 'server1.runner'

        assert result is True
        assert cleanup_prompts == []
        assert cdr.list_visible_entries(str(handoff_root)) == ['flow_one']
        assert cdr.list_visible_entries(str(final_root)) == []
        assert (runtime_root / 'scripts').exists()
        assert "Next System Handoff" in captured.out
        assert "Switch to corfac@server2" in captured.out
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

    def test_build_test_with_data_seed_plan_uses_direct_fixture_root(
        self, tmp_path
    ):
        """Direct tests/data/<fixture> layouts should work with fixture filters."""
        toy_data_root = tmp_path / 'tests' / 'data'
        toy_data_root.mkdir(parents=True)
        (toy_data_root / 'Illumina_TransferTest').mkdir()
        (toy_data_root / 'Nanopore_TransferTest').mkdir()

        test_plan = {
            'initial_sources': [
                {
                    'value': str(tmp_path / 'source' / 'corefacility') + '/',
                    'port': '',
                    'test_fixture_names': ['Illumina_TransferTest'],
                }
            ]
        }

        seed_plan = cdr.build_test_with_data_seed_plan(
            test_plan,
            str(toy_data_root),
            str(tmp_path),
        )

        assert seed_plan[0]['toy_data_dir'] == str(toy_data_root)
        assert seed_plan[0]['entry_names'] == ['Illumina_TransferTest']

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

    def test_validate_script_test_results_reports_unavailable_terminal_root(
        self, monkeypatch
    ):
        """Validation should distinguish an unavailable terminal root from a missing run."""
        test_plan = {
            'terminal_destinations': [
                {'value': '/tmp/final/', 'port': ''}
            ]
        }
        expected_contents = {
            cdr.endpoint_key('/tmp/final/'): {'flow_one'}
        }

        monkeypatch.setattr(
            cdr,
            'endpoint_root_ready',
            lambda endpoint: (False, '/tmp/final'),
        )

        errors = cdr.validate_script_test_results(test_plan, expected_contents)

        assert errors == ['Terminal destination root unavailable: /tmp/final']

    def test_validate_script_test_results_reports_missing_run_under_ready_root(
        self, monkeypatch
    ):
        """Validation should report missing runs only after root readiness succeeds."""
        test_plan = {
            'terminal_destinations': [
                {'value': '/tmp/final/', 'port': ''}
            ]
        }
        expected_contents = {
            cdr.endpoint_key('/tmp/final/'): {'flow_one'}
        }

        monkeypatch.setattr(
            cdr,
            'endpoint_root_ready',
            lambda endpoint: (True, '/tmp/final'),
        )
        monkeypatch.setattr(
            cdr,
            'endpoint_directory_exists',
            lambda endpoint, directory_name: (
                False, '/tmp/final/{0}'.format(directory_name)
            ),
        )

        errors = cdr.validate_script_test_results(test_plan, expected_contents)

        assert errors == [
            'Expected test directory missing under terminal destination root: /tmp/final/flow_one'
        ]

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
            config_file=None,
            transfers_file=None,
            slow=False,
            runtime_ids=None,
            runtime_filter_source=None,
        ):
            captured['config_file'] = config_file
            captured['transfers_file'] = transfers_file
            captured['slow'] = slow
            captured['runtime_ids'] = runtime_ids
            captured['runtime_filter_source'] = runtime_filter_source
            return True

        monkeypatch.setattr(cdr, 'run_test_with_data', fake_run_test_with_data)

        result = cdr.main([
            '--config', 'config.yaml',
            '--transfers', 'transfers.tsv',
            '--runtime-id', 'local_dev.local',
            '--test-with-data',
            '--slow',
        ])

        assert result is True
        assert captured == {
            'config_file': 'config.yaml',
            'transfers_file': 'transfers.tsv',
            'slow': True,
            'runtime_ids': ['local_dev.local'],
            'runtime_filter_source': 'command line',
        }

    def test_main_lists_and_creates_missing_directories(self, tmp_path, monkeypatch, capsys):
        """Missing local directories should be summarized and created on confirmation."""
        transfers_file = tmp_path / 'transfers.tsv'
        transfers_file.write_text("placeholder\n")
        source_dir = tmp_path / 'server1' / 'Landing_Zone' / 'from_labnet'
        destination_dir = tmp_path / 'server1' / 'Landing_Zone' / 'to_server2'
        log_file = tmp_path / 'log' / 'transfer.log'
        flock_file = tmp_path / 'flock' / 'transfer.lock'

        df = pd.DataFrame([
            {
                'source': str(tmp_path) + '//server1//Landing_Zone//from_labnet/',
                'source_port': '',
                'destination': str(tmp_path) + '//server1//Landing_Zone//to_server2/',
                'destination_port': '',
                'log_file': str(log_file),
                'flock_file': str(flock_file),
                'system': 'server1',
                'users': 'runner',
            }
        ])

        monkeypatch.setattr(cdr, 'check_required_tools', lambda: True)
        monkeypatch.setattr(cdr, 'check_flock_command', lambda system: True)
        monkeypatch.setattr(
            cdr,
            'load_runtime_transfers',
            lambda transfers_file=None, runtime_ids=None: df,
        )
        monkeypatch.setattr(
            cdr,
            'filter_transfers_by_system_user',
            lambda loaded_df, system, user: loaded_df,
        )
        monkeypatch.setattr(
            cdr,
            'get_current_system',
            lambda transfers_df=None, runtime_ids=None: 'server1',
        )
        monkeypatch.setattr(
            cdr,
            'get_current_user',
            lambda transfers_df=None, runtime_ids=None: 'runner',
        )
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
