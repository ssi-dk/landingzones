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

    def test_validate_separation_routes_to_validator(self, monkeypatch):
        """`landingzones validate separation` should forward tags to the validator."""
        captured = {}

        def fake_main(argv=None):
            captured['argv'] = argv
            return 0

        monkeypatch.setattr(cli.vsep, 'main', fake_main)

        rc = cli.main([
            'validate',
            'separation',
            '--config', 'config.yaml',
            '--tag', 'heartbeat',
            '--tag', 'lab',
        ])

        assert rc == 0
        assert captured['argv'] == [
            '--config', 'config.yaml',
            '--tag', 'heartbeat',
            '--tag', 'lab',
        ]

    def test_validate_chain_runs_steps_in_order(self, monkeypatch):
        """`landingzones validate chain` should run separation, deployment, integration, then report."""
        captured = {
            'separation': [],
            'readiness': [],
            'report': [],
        }

        def fake_separation(argv=None):
            captured['separation'].append(argv)
            return 0

        def fake_readiness(argv=None):
            captured['readiness'].append(argv)
            return 0

        def fake_report(argv=None):
            captured['report'].append(argv)
            return 0

        monkeypatch.setattr(cli.vsep, 'main', fake_separation)
        monkeypatch.setattr(cli.cdr, 'main', fake_readiness)
        monkeypatch.setattr(cli.pts, 'main', fake_report)

        rc = cli.main([
            'validate',
            'chain',
            '--config', 'config.yaml',
            '--transfers', 'transfers.tsv',
            '--validation-scripts-dir', 'validation_scripts',
            '--slow',
            '--tag', 'heartbeat',
            '--report-output', 'dashboard.html',
            '--system', 'calc',
        ])

        assert rc == 0
        assert captured['separation'] == [[
            '--config', 'config.yaml',
            '--transfers', 'transfers.tsv',
            '--tag', 'heartbeat',
        ]]
        assert captured['readiness'] == [
            [
                '--config', 'config.yaml',
                '--transfers', 'transfers.tsv',
                '--validation-scripts-dir', 'validation_scripts',
            ],
            [
                '--config', 'config.yaml',
                '--transfers', 'transfers.tsv',
                '--validation-scripts-dir', 'validation_scripts',
                '--slow',
                '--test-with-data',
            ],
        ]
        assert captured['report'] == [[
            '--output', 'dashboard.html',
            '--config', 'config.yaml',
            '--transfers-file', 'transfers.tsv',
            '--system', 'calc',
            '--tag', 'heartbeat',
        ]]

    def test_validate_chain_defaults_to_any_tag_for_separation(self, monkeypatch):
        """Omitting --tag should run separation across any tagged transfer."""
        captured = {
            'separation': [],
            'readiness': [],
            'report': [],
        }

        def fake_separation(argv=None):
            captured['separation'].append(argv)
            return 0

        def fake_readiness(argv=None):
            captured['readiness'].append(argv)
            return 0

        def fake_report(argv=None):
            captured['report'].append(argv)
            return 0

        monkeypatch.setattr(cli.vsep, 'main', fake_separation)
        monkeypatch.setattr(cli.cdr, 'main', fake_readiness)
        monkeypatch.setattr(cli.pts, 'main', fake_report)

        rc = cli.main([
            'validate',
            'chain',
        ])

        assert rc == 0
        assert captured['separation'] == [[]]
        assert captured['readiness'] == [[], ['--test-with-data']]
        assert captured['report'] == [[]]

    def test_validate_chain_fails_fast_on_deployment_failure(self, monkeypatch):
        """`landingzones validate chain` should stop when an earlier step fails."""
        captured = {
            'separation': 0,
            'readiness': [],
            'report': 0,
        }

        def fake_separation(argv=None):
            captured['separation'] += 1
            return 0

        def fake_readiness(argv=None):
            captured['readiness'].append(argv)
            return 1

        def fake_report(argv=None):
            captured['report'] += 1
            return 0

        monkeypatch.setattr(cli.vsep, 'main', fake_separation)
        monkeypatch.setattr(cli.cdr, 'main', fake_readiness)
        monkeypatch.setattr(cli.pts, 'main', fake_report)

        rc = cli.main([
            'validate',
            'chain',
        ])

        assert rc == 1
        assert captured['separation'] == 1
        assert captured['readiness'] == [[]]
        assert captured['report'] == 0

    def test_deploy_cron_routes_to_readiness(self, monkeypatch):
        """`landingzones deploy cron` should route to the cron deployment prompt."""
        captured = {}

        def fake_main(argv=None):
            captured['argv'] = argv
            return True

        monkeypatch.setattr(cli.cdr, 'main', fake_main)

        rc = cli.main([
            'deploy',
            'cron',
            '--config', 'config.yaml',
        ])

        assert rc == 0
        assert captured['argv'] == ['--config', 'config.yaml', '--deploy-cron']

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

    def test_report_transfers_routes_tag_filters(self, monkeypatch):
        """`landingzones report transfers --tag` should forward repeatable tags."""
        captured = {}

        def fake_main(argv=None):
            captured['argv'] = argv
            return 0

        monkeypatch.setattr(cli.pts, 'main', fake_main)

        rc = cli.main([
            'report',
            'transfers',
            '--tag', 'heartbeat',
            '--tag', 'lab',
        ])

        assert rc == 0
        assert captured['argv'] == [
            '--tag', 'heartbeat',
            '--tag', 'lab',
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
