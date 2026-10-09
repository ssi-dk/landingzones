"""Credential checks use the installed CLI context without accepting work."""
import json

import pytest
import yaml

from landingzones import cli
from tests.test_execution import setup


@pytest.mark.parametrize('subcommand_config', [False, True])
def test_local_cli_validation_creates_no_request_or_event_state(setup, capsys, subcommand_config):
    root, payload, rows, write, config, request = setup
    before = {str(path.relative_to(root)) for path in root.rglob('*')}
    if subcommand_config:
        argv = ['--config', 'unused-global.yaml', 'transfer', 'validate-credentials',
                '--config', str(config), '--connection', 'a']
    else:
        argv = ['--config', str(config), 'transfer', 'validate-credentials']
    assert cli.main(argv) == 0
    report = json.loads(capsys.readouterr().out)
    assert report['status'] == 'passed'
    assert len(report['connections']) == 1
    assert {str(path.relative_to(root)) for path in root.rglob('*')} == before
    assert (payload / 'data.txt').read_text() == 'accepted data'


def test_endpoint_credential_failure_has_nonzero_exit_without_accepting_work(setup, capsys):
    root, payload, rows, write, config, request = setup
    rows[0].update(adapter='sftp', destination='sftp://upload@example.invalid/incoming',
                   credential_ref='sftp_missing')
    write()
    assert cli.main(['--config', str(config), 'transfer', 'validate-credentials']) == 1
    report = json.loads(capsys.readouterr().out)
    assert report['status'] == 'failed'
    assert report['connections'][0]['status'] == 'failed'
    assert not (root / 'state').exists()
    assert not (root / 'events.tsv').exists()


def test_unknown_connection_is_a_configuration_error(setup, capsys):
    root, payload, rows, write, config, request = setup
    assert cli.main(['transfer', 'validate-credentials', '--config', str(config),
                     '--connection', 'missing']) == 2
    assert json.loads(capsys.readouterr().out)['error']['code'] == 'configuration_error'


def test_malformed_configuration_never_echoes_secret_yaml(setup, capsys):
    root, payload, rows, write, config, request = setup
    secret = 'synthetic-secret-that-must-not-be-printed'
    config.write_text('credentials: [' + secret)
    assert cli.main(['--config', str(config), 'transfer', 'validate-credentials']) == 2
    captured = capsys.readouterr()
    assert secret not in captured.out + captured.err
    assert json.loads(captured.out)['status'] == 'failed'


def test_validation_rejects_another_execution_account(setup, capsys):
    root, payload, rows, write, config, request = setup
    settings = yaml.safe_load(config.read_text())
    settings['execution_context']['user'] = 'different-execution-account'
    config.write_text(yaml.safe_dump(settings))
    assert cli.main(['--config', str(config), 'transfer', 'validate-credentials']) == 2
    assert json.loads(capsys.readouterr().out)['status'] == 'failed'
    assert not (root / 'state').exists()
