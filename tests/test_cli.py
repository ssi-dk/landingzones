#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Test suite for the top-level operator CLI."""

import os
import subprocess

from landingzones import cli


class TestOperatorCli:
    """Test routing for the top-level `landingzones` CLI."""

    def test_build_routes_to_generator(self, monkeypatch):
        """`landingzones build` should forward arguments to the generator."""
        captured = {}

        def fake_main(argv=None):
            captured['argv'] = argv
            return 0

        monkeypatch.setattr(cli.gcf, 'main', fake_main)

        rc = cli.main([
            'build',
            '--config', 'config.yaml',
            '--scripts-dir', 'scripts',
            '--validation-scripts-dir', 'validation_scripts',
        ])

        assert rc == 0
        assert captured['argv'] == [
            '--config', 'config.yaml',
            '--scripts-dir', 'scripts',
            '--validation-scripts-dir', 'validation_scripts',
        ]

    def test_global_config_routes_to_build(self, monkeypatch):
        """Top-level --config should be accepted and forwarded to subcommands."""
        captured = {}

        def fake_main(argv=None):
            captured['argv'] = argv
            return 0

        monkeypatch.setattr(cli.gcf, 'main', fake_main)

        rc = cli.main([
            '--config', 'config/config.yaml',
            'build',
        ])

        assert rc == 0
        assert captured['argv'] == ['--config', 'config/config.yaml']

    def test_subcommand_config_overrides_global_config(self, monkeypatch):
        """Subcommand-level --config should win over the top-level value."""
        captured = {}

        def fake_main(argv=None):
            captured['argv'] = argv
            return 0

        monkeypatch.setattr(cli.gcf, 'main', fake_main)

        rc = cli.main([
            '--config', 'global.yaml',
            'build',
            '--config', 'local.yaml',
        ])

        assert rc == 0
        assert captured['argv'] == ['--config', 'local.yaml']

    def test_validate_deployment_routes_to_readiness(self, monkeypatch):
        """`landingzones validate deployment` should route to readiness checks."""
        captured = {}

        def fake_main(argv=None):
            captured['argv'] = argv
            return True

        monkeypatch.setattr(cli.cdr, 'main', fake_main)

        rc = cli.main([
            'validate',
            'deployment',
            '--config', 'config.yaml',
        ])

        assert rc == 0
        assert captured['argv'] == ['--config', 'config.yaml']

    def test_validate_integration_routes_to_test_with_data(self, monkeypatch):
        """`landingzones validate integration` should add the integration flag."""
        captured = {}

        def fake_main(argv=None):
            captured['argv'] = argv
            return True

        monkeypatch.setattr(cli.cdr, 'main', fake_main)

        rc = cli.main([
            'validate',
            'integration',
            '--config', 'config.yaml',
        ])

        assert rc == 0
        assert captured['argv'] == ['--config', 'config.yaml', '--test-with-data']

    def test_validate_integration_routes_slow_flag(self, monkeypatch):
        """`landingzones validate integration --slow` should forward the slow mode flag."""
        captured = {}

        def fake_main(argv=None):
            captured['argv'] = argv
            return True

        monkeypatch.setattr(cli.cdr, 'main', fake_main)

        rc = cli.main([
            'validate',
            'integration',
            '--config', 'config.yaml',
            '--slow',
        ])

        assert rc == 0
        assert captured['argv'] == [
            '--config', 'config.yaml', '--slow', '--test-with-data'
        ]

    def test_report_transfers_routes_to_dashboard(self, monkeypatch):
        """`landingzones report transfers` should forward its dashboard arguments."""
        captured = {}

        def fake_main(argv=None):
            captured['argv'] = argv
            return 0

        monkeypatch.setattr(cli.pts, 'main', fake_main)

        rc = cli.main([
            'report',
            'transfers',
            'input.tsv',
            '--output', 'dashboard.html',
            '--system', 'calc',
        ])

        assert rc == 0
        assert captured['argv'] == [
            'input.tsv',
            '--output', 'dashboard.html',
            '--system', 'calc',
        ]

    def test_report_transfers_routes_to_dashboard_from_config(self, monkeypatch):
        """`landingzones report transfers` should allow config-only log resolution."""
        captured = {}

        def fake_main(argv=None):
            captured['argv'] = argv
            return 0

        monkeypatch.setattr(cli.pts, 'main', fake_main)

        rc = cli.main([
            'report',
            'transfers',
            '--config', 'config.yaml',
            '--system', 'calc',
        ])

        assert rc == 0
        assert captured['argv'] == [
            '--config', 'config.yaml',
            '--system', 'calc',
        ]

    def test_validate_hop_executes_discovered_wrapper(self, tmp_path):
        """`landingzones validate hop` should find and execute the wrapper for a flow."""
        validation_dir = tmp_path / 'validation_scripts'
        validation_dir.mkdir()
        args_file = tmp_path / 'wrapper_args.txt'
        wrapper = validation_dir / 'lz_run_validation_flow-a.sh'
        wrapper.write_text(
            "#!/bin/sh\n"
            "printf '%s\\n' \"$@\" > {0}\n".format(args_file)
        )
        os.chmod(wrapper, 0o755)

        rc = cli.main([
            'validate',
            'hop',
            '--validation-scripts-dir', str(validation_dir),
            'flow-a',
            '--token', 'ABCD',
        ])

        assert rc == 0
        assert args_file.read_text().splitlines() == ['run', '--token', 'ABCD']

    def test_validate_hop_preflight_passes_explicit_action(self, tmp_path):
        """`landingzones validate hop` should pass through an explicit action to the wrapper."""
        validation_dir = tmp_path / 'validation_scripts'
        validation_dir.mkdir()
        args_file = tmp_path / 'wrapper_args.txt'
        wrapper = validation_dir / 'lz_run_validation_flow-a.sh'
        wrapper.write_text(
            "#!/bin/sh\n"
            "printf '%s\\n' \"$@\" > {0}\n".format(args_file)
        )
        os.chmod(wrapper, 0o755)

        rc = cli.main([
            'validate',
            'hop',
            '--validation-scripts-dir', str(validation_dir),
            'flow-a',
            'preflight',
        ])

        assert rc == 0
        assert args_file.read_text().splitlines() == ['preflight']

    def test_validate_hop_reports_unknown_flow(self, tmp_path, capsys):
        """Unknown flow names should list discovered wrapper flows."""
        validation_dir = tmp_path / 'validation_scripts'
        validation_dir.mkdir()
        wrapper = validation_dir / 'lz_run_validation_flow_a.sh'
        wrapper.write_text("#!/bin/sh\nexit 0\n")
        os.chmod(wrapper, 0o755)

        rc = cli.main([
            'validate',
            'hop',
            '--validation-scripts-dir', str(validation_dir),
            'missing-flow',
        ])
        captured = capsys.readouterr()

        assert rc == 1
        assert "Unknown flow_group 'missing-flow'" in captured.err
        assert "flow_a" in captured.err
