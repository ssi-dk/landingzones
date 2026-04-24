#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Operator-oriented CLI entrypoint for Landing Zones."""

import argparse
import os
import subprocess
import sys

from landingzones.config import config
from landingzones import check_deployment_readiness as cdr
from landingzones import generate_cron_files as gcf
from landingzones import plot_transfer_status as pts
from landingzones import validate_separation as vsep


def append_option(argv, flag, value):
    """Append a CLI flag/value pair when the value is present."""
    if value is None:
        return
    argv.extend([flag, str(value)])


def normalize_exit_code(result):
    """Normalize handler return values for console-script exit semantics."""
    if isinstance(result, bool):
        return 0 if result else 1
    if result is None:
        return 0
    return result


def resolve_cli_config(args):
    """Resolve the effective config path, preferring subcommand override over global."""
    subcommand_config = getattr(args, 'config', None)
    global_config = getattr(args, 'global_config', None)
    return subcommand_config or global_config


def build_cli_parser():
    """Build the top-level operator CLI parser."""
    parser = argparse.ArgumentParser(
        prog='landingzones',
        description=(
            'Landing Zones operator CLI. If --config is omitted, the CLI uses '
            'defaults and auto-detects ./config.yaml or ./config/config.yaml when present.'
        ),
    )
    parser.add_argument(
        '--config', '-c',
        dest='global_config',
        default=None,
        help='Optional config.yaml path. When omitted, landingzones auto-detects ./config.yaml or ./config/config.yaml when present.',
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    build_parser = subparsers.add_parser(
        'build',
        help='Generate cron files, transfer scripts, and validation wrappers',
    )
    build_parser.add_argument('--config', '-c', default=None)
    build_parser.add_argument('--transfers', '-t', default=None)
    build_parser.add_argument('--output-dir', '-o', default=None)
    build_parser.add_argument('--log-dir', '-l', default=None)
    build_parser.add_argument('--scripts-dir', '-s', default=None)
    build_parser.add_argument('--validation-scripts-dir', default=None)
    build_parser.set_defaults(handler=handle_build)

    validate_parser = subparsers.add_parser(
        'validate',
        help='Validate deployment readiness, a hop, or an integration run',
    )
    validate_subparsers = validate_parser.add_subparsers(
        dest='validate_command',
        required=True,
    )

    validate_deployment_parser = validate_subparsers.add_parser(
        'deployment',
        help='Run deployment readiness checks',
    )
    validate_deployment_parser.add_argument('--config', '-c', default=None)
    validate_deployment_parser.add_argument('--transfers', '-t', default=None)
    validate_deployment_parser.add_argument('--validation-scripts-dir', default=None)
    validate_deployment_parser.set_defaults(handler=handle_validate_deployment)

    validate_integration_parser = validate_subparsers.add_parser(
        'integration',
        help='Seed toy data and execute the configured transfer chain',
    )
    validate_integration_parser.add_argument('--config', '-c', default=None)
    validate_integration_parser.add_argument('--transfers', '-t', default=None)
    validate_integration_parser.add_argument('--validation-scripts-dir', default=None)
    validate_integration_parser.add_argument(
        '--slow',
        action='store_true',
        help='Print each completed integration step and wait for Enter before continuing',
    )
    validate_integration_parser.set_defaults(handler=handle_validate_integration)

    validate_hop_parser = validate_subparsers.add_parser(
        'hop',
        help='Run a generated per-flow hop validation wrapper',
    )
    validate_hop_parser.add_argument('--config', '-c', default=None)
    validate_hop_parser.add_argument('--validation-scripts-dir', default=None)
    validate_hop_parser.add_argument('flow_group')
    validate_hop_parser.set_defaults(handler=handle_validate_hop)

    validate_separation_parser = validate_subparsers.add_parser(
        'separation',
        help='Check whether tagged transfers collide with other enabled flows',
    )
    validate_separation_parser.add_argument('--config', '-c', default=None)
    validate_separation_parser.add_argument('--transfers', '-t', default=None)
    validate_separation_parser.add_argument(
        '--tag',
        action='append',
        default=[],
    )
    validate_separation_parser.set_defaults(handler=handle_validate_separation)

    validate_chain_parser = validate_subparsers.add_parser(
        'chain',
        help='Run separation, deployment, integration, and reporting in order',
    )
    validate_chain_parser.add_argument('--config', '-c', default=None)
    validate_chain_parser.add_argument('--transfers', '-t', default=None)
    validate_chain_parser.add_argument('--validation-scripts-dir', default=None)
    validate_chain_parser.add_argument(
        '--slow',
        action='store_true',
        help='Pass slow mode through to the integration step',
    )
    validate_chain_parser.add_argument(
        '--tag',
        action='append',
        default=[],
    )
    validate_chain_parser.add_argument('--report-input', default=None)
    validate_chain_parser.add_argument('--report-output', default=None)
    validate_chain_parser.add_argument('--system', default=None)
    validate_chain_parser.add_argument('--warning-hours', type=float, default=None)
    validate_chain_parser.add_argument('--max-runs', type=int, default=None)
    validate_chain_parser.add_argument('--title', default=None)
    validate_chain_parser.set_defaults(handler=handle_validate_chain)

    deploy_parser = subparsers.add_parser(
        'deploy',
        help='Perform operator deployment actions',
    )
    deploy_subparsers = deploy_parser.add_subparsers(
        dest='deploy_command',
        required=True,
    )

    deploy_cron_parser = deploy_subparsers.add_parser(
        'cron',
        help='Prompt to deploy cron files for the current system/user',
    )
    deploy_cron_parser.add_argument('--config', '-c', default=None)
    deploy_cron_parser.add_argument('--transfers', '-t', default=None)
    deploy_cron_parser.add_argument('--validation-scripts-dir', default=None)
    deploy_cron_parser.set_defaults(handler=handle_deploy_cron)

    report_parser = subparsers.add_parser(
        'report',
        help='Generate reporting outputs',
    )
    report_subparsers = report_parser.add_subparsers(
        dest='report_command',
        required=True,
    )

    report_transfers_parser = report_subparsers.add_parser(
        'transfers',
        help='Generate a transfer health dashboard from a shared TSV log',
    )
    report_transfers_parser.add_argument(
        'input',
        nargs='?',
        default=None,
        help='Optional path to the transfer TSV report input; defaults to report_transfer_log_file from config',
    )
    report_transfers_parser.add_argument('--output', '-o', default=None)
    report_transfers_parser.add_argument('--config', '-c', default=None)
    report_transfers_parser.add_argument('--transfers-file', '-t', default=None)
    report_transfers_parser.add_argument('--system', default=None)
    report_transfers_parser.add_argument('--warning-hours', type=float, default=None)
    report_transfers_parser.add_argument('--max-runs', type=int, default=None)
    report_transfers_parser.add_argument('--title', default=None)
    report_transfers_parser.add_argument(
        '--tag',
        action='append',
        default=[],
    )
    report_transfers_parser.set_defaults(handler=handle_report_transfers)

    return parser


def discover_validation_wrappers(validation_scripts_dir):
    """Discover generated validation wrapper scripts by sanitized flow group."""
    wrappers = {}
    if not os.path.isdir(validation_scripts_dir):
        return wrappers
    prefix = gcf.VALIDATION_WRAPPER_PREFIX
    for entry in sorted(os.listdir(validation_scripts_dir)):
        if not entry.startswith(prefix) or not entry.endswith('.sh'):
            continue
        flow_key = entry[len(prefix):-3]
        wrappers[flow_key] = os.path.join(validation_scripts_dir, entry)
    return wrappers


def handle_build(args, extra_args):
    """Route `landingzones build` to the generator implementation."""
    if extra_args:
        raise SystemExit("unrecognized arguments: {0}".format(' '.join(extra_args)))
    argv = []
    append_option(argv, '--config', resolve_cli_config(args))
    append_option(argv, '--transfers', args.transfers)
    append_option(argv, '--output-dir', args.output_dir)
    append_option(argv, '--log-dir', args.log_dir)
    append_option(argv, '--scripts-dir', args.scripts_dir)
    append_option(argv, '--validation-scripts-dir', args.validation_scripts_dir)
    return normalize_exit_code(gcf.main(argv))


def handle_validate_deployment(args, extra_args):
    """Route `landingzones validate deployment` to readiness checks."""
    if extra_args:
        raise SystemExit("unrecognized arguments: {0}".format(' '.join(extra_args)))
    argv = []
    append_option(argv, '--config', resolve_cli_config(args))
    append_option(argv, '--transfers', args.transfers)
    append_option(argv, '--validation-scripts-dir', args.validation_scripts_dir)
    return normalize_exit_code(cdr.main(argv))


def handle_validate_integration(args, extra_args):
    """Route `landingzones validate integration` to test-with-data checks."""
    if extra_args:
        raise SystemExit("unrecognized arguments: {0}".format(' '.join(extra_args)))
    argv = []
    append_option(argv, '--config', resolve_cli_config(args))
    append_option(argv, '--transfers', args.transfers)
    append_option(argv, '--validation-scripts-dir', args.validation_scripts_dir)
    if args.slow:
        argv.append('--slow')
    argv.append('--test-with-data')
    return normalize_exit_code(cdr.main(argv))


def handle_validate_hop(args, extra_args):
    """Execute a generated validation wrapper discovered from config."""
    config.load_config(
        config_file=resolve_cli_config(args),
        validation_scripts_dir=args.validation_scripts_dir,
    )
    wrappers = discover_validation_wrappers(config.validation_scripts_dir)
    flow_key = gcf.sanitize_identifier(args.flow_group)
    wrapper_path = wrappers.get(flow_key)
    if wrapper_path is None:
        available = ', '.join(sorted(wrappers)) or '(none)'
        print(
            "Unknown flow_group '{0}'. Available validation flows: {1}".format(
                args.flow_group, available
            ),
            file=sys.stderr,
        )
        return 1

    wrapper_args = list(extra_args)
    if wrapper_args and wrapper_args[0] == '--':
        wrapper_args = wrapper_args[1:]
    action = 'run'
    if wrapper_args and wrapper_args[0] in ('preflight', 'run'):
        action = wrapper_args.pop(0)
    command = [wrapper_path, action] + wrapper_args
    return subprocess.call(command)


def handle_deploy_cron(args, extra_args):
    """Route `landingzones deploy cron` to the interactive cron deployment."""
    if extra_args:
        raise SystemExit("unrecognized arguments: {0}".format(' '.join(extra_args)))
    argv = []
    append_option(argv, '--config', resolve_cli_config(args))
    append_option(argv, '--transfers', args.transfers)
    append_option(argv, '--validation-scripts-dir', args.validation_scripts_dir)
    argv.append('--deploy-cron')
    return normalize_exit_code(cdr.main(argv))


def handle_validate_separation(args, extra_args):
    """Route `landingzones validate separation` to the tag collision checker."""
    if extra_args:
        raise SystemExit("unrecognized arguments: {0}".format(' '.join(extra_args)))
    argv = []
    append_option(argv, '--config', resolve_cli_config(args))
    append_option(argv, '--transfers', args.transfers)
    for tag in args.tag:
        append_option(argv, '--tag', tag)
    return normalize_exit_code(vsep.main(argv))


def handle_validate_chain(args, extra_args):
    """Run separation, deployment, integration, and reporting in order."""
    if extra_args:
        raise SystemExit("unrecognized arguments: {0}".format(' '.join(extra_args)))

    config_path = resolve_cli_config(args)

    separation_argv = []
    append_option(separation_argv, '--config', config_path)
    append_option(separation_argv, '--transfers', args.transfers)
    for tag in args.tag:
        append_option(separation_argv, '--tag', tag)
    rc = normalize_exit_code(vsep.main(separation_argv))
    if rc != 0:
        return rc

    deployment_argv = []
    append_option(deployment_argv, '--config', config_path)
    append_option(deployment_argv, '--transfers', args.transfers)
    append_option(
        deployment_argv,
        '--validation-scripts-dir',
        args.validation_scripts_dir,
    )
    rc = normalize_exit_code(cdr.main(deployment_argv))
    if rc != 0:
        return rc

    integration_argv = []
    append_option(integration_argv, '--config', config_path)
    append_option(integration_argv, '--transfers', args.transfers)
    append_option(
        integration_argv,
        '--validation-scripts-dir',
        args.validation_scripts_dir,
    )
    if args.slow:
        integration_argv.append('--slow')
    integration_argv.append('--test-with-data')
    rc = normalize_exit_code(cdr.main(integration_argv))
    if rc != 0:
        return rc

    report_argv = []
    if args.report_input:
        report_argv.append(args.report_input)
    append_option(report_argv, '--output', args.report_output)
    append_option(report_argv, '--config', config_path)
    append_option(report_argv, '--transfers-file', args.transfers)
    append_option(report_argv, '--system', args.system)
    if args.warning_hours is not None:
        append_option(report_argv, '--warning-hours', args.warning_hours)
    if args.max_runs is not None:
        append_option(report_argv, '--max-runs', args.max_runs)
    append_option(report_argv, '--title', args.title)
    for tag in args.tag:
        append_option(report_argv, '--tag', tag)
    rc = normalize_exit_code(pts.main(report_argv))
    if rc == getattr(pts, 'REPORT_SKIPPED_EXIT_CODE', 2):
        print("Validation chain completed; report generation was skipped.")
        return 0
    return rc


def handle_report_transfers(args, extra_args):
    """Route `landingzones report transfers` to dashboard generation."""
    if extra_args:
        raise SystemExit("unrecognized arguments: {0}".format(' '.join(extra_args)))
    argv = []
    if args.input:
        argv.append(args.input)
    append_option(argv, '--output', args.output)
    append_option(argv, '--config', resolve_cli_config(args))
    append_option(argv, '--transfers-file', args.transfers_file)
    append_option(argv, '--system', args.system)
    if args.warning_hours is not None:
        append_option(argv, '--warning-hours', args.warning_hours)
    if args.max_runs is not None:
        append_option(argv, '--max-runs', args.max_runs)
    append_option(argv, '--title', args.title)
    for tag in args.tag:
        append_option(argv, '--tag', tag)
    return normalize_exit_code(pts.main(argv))


def main(argv=None):
    """Run the operator-oriented top-level CLI."""
    parser = build_cli_parser()
    args, extra_args = parser.parse_known_args(argv)
    return args.handler(args, extra_args)


if __name__ == '__main__':
    raise SystemExit(main())
