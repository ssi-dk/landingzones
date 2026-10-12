"""Portable configured endpoints and fixed physical paths during recovery."""
import csv
import json
import shutil
from types import SimpleNamespace

import pytest
import yaml

from landingzones.execution.engine import Executor
from landingzones.execution.model import load_settings, load_steps
from landingzones.execution import credential_validation
from landingzones.execution import source as source_module
from tests.test_execution import setup, step


@pytest.fixture
def runtime(setup):
    root, payload, rows, _, original_config, request = setup
    directory = root / 'runtime'
    (directory / 'config').mkdir(parents=True)
    (directory / 'data').mkdir()
    (directory / 'data/input').symlink_to(payload.parent, target_is_directory=True)
    (directory / 'data/output').symlink_to(root / 'a', target_is_directory=True)
    table = directory / 'config/transfers.tsv'
    rows[0].update(source='data/input', destination='data/output')

    def write():
        with table.open('w') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]), delimiter='\t')
            writer.writeheader()
            writer.writerows(rows)

    write()
    settings = yaml.safe_load(original_config.read_text())
    settings.update(local_path_base='..', transfers_file='transfers.tsv',
                    state_dir='../state', event_spool='../log/events.tsv')
    config = directory / 'config/execution.yaml'
    config.write_text(yaml.safe_dump(settings))
    return SimpleNamespace(root=root, directory=directory, payload=payload, rows=rows,
                           write=write, config=config, request=request, table=table)


@pytest.mark.parametrize('operation', ['copy', 'move'])
def test_runtime_relative_links_process_and_log_absolute_paths(runtime, monkeypatch, operation):
    runtime.rows[0]['operation'] = operation
    runtime.write()
    configured = runtime.table.read_text()
    elsewhere = runtime.root / 'unrelated-working-directory'
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    state = Executor(runtime.config).submit(runtime.request)
    assert state['status'] == 'completed'
    assert runtime.payload.exists() == (operation == 'copy')
    assert (runtime.root / 'a/run-123/data.txt').read_text() == 'accepted data'
    definition = step(state)['definition']
    assert definition['source'] == str(runtime.root / 'input')
    assert definition['destination'] == str(runtime.root / 'a')
    assert runtime.table.read_text() == configured
    with (runtime.directory / 'log/events.tsv').open() as handle:
        events = list(csv.DictReader(handle, delimiter='\t'))
    assert events and all(event['source_path'] == definition['source'] for event in events)
    assert all(event['destination_path'] == definition['destination'] for event in events)


def test_identical_runtime_configuration_uses_separate_links_and_state(runtime):
    second = runtime.root / 'second-runtime'
    shutil.copytree(runtime.directory, second, symlinks=True)
    source = runtime.root / 'second-input'
    shutil.copytree(runtime.payload.parent, source)
    destination = runtime.root / 'second-output'
    destination.mkdir()
    for alias, target in [('input', source), ('output', destination)]:
        (second / 'data' / alias).unlink()
        (second / 'data' / alias).symlink_to(target, target_is_directory=True)
    assert (second / 'config/execution.yaml').read_bytes() == runtime.config.read_bytes()
    assert (second / 'config/transfers.tsv').read_bytes() == runtime.table.read_bytes()
    first_state = Executor(runtime.config).submit(runtime.request)
    second_state = Executor(second / 'config/execution.yaml').submit(runtime.request)
    assert first_state['status'] == second_state['status'] == 'completed'
    assert step(first_state)['definition']['destination'] == str(runtime.root / 'a')
    assert step(second_state)['definition']['destination'] == str(destination)
    assert (runtime.directory / 'state').is_dir() and (second / 'state').is_dir()
    assert (destination / 'run-123/data.txt').read_text() == 'accepted data'


def test_absolute_configured_aliases_work_without_a_local_base(runtime):
    settings = yaml.safe_load(runtime.config.read_text())
    settings.pop('local_path_base')
    runtime.config.write_text(yaml.safe_dump(settings))
    runtime.rows[0].update(source=str(runtime.directory / 'data/input'),
                           destination=str(runtime.directory / 'data/output'))
    runtime.write()
    assert Executor(runtime.config).submit(runtime.request)['status'] == 'completed'


def test_existing_physical_absolute_spellings_keep_receipt_identity(runtime):
    runtime.rows[0].update(source=str(runtime.payload.parent) + '/',
                           destination=str(runtime.root / 'a') + '/')
    runtime.write()
    resolved = load_steps(load_settings(runtime.config))['a'][0]
    assert resolved.source == runtime.rows[0]['source']
    assert resolved.destination == runtime.rows[0]['destination']


def test_relative_endpoints_require_an_explicit_base(runtime):
    settings = yaml.safe_load(runtime.config.read_text())
    settings.pop('local_path_base')
    runtime.config.write_text(yaml.safe_dump(settings))
    with pytest.raises(ValueError, match='local_path_base'):
        Executor(runtime.config).preflight(runtime.request)


@pytest.mark.parametrize('value', [None, '', 5, [], {}])
def test_invalid_local_base_is_rejected(runtime, value):
    settings = yaml.safe_load(runtime.config.read_text())
    settings['local_path_base'] = value
    runtime.config.write_text(yaml.safe_dump(settings))
    with pytest.raises(ValueError, match='local_path_base'):
        load_settings(runtime.config)


@pytest.mark.parametrize('value', ['missing-directory', 'transfers.tsv'])
def test_local_base_must_be_an_existing_directory(runtime, value):
    settings = yaml.safe_load(runtime.config.read_text())
    settings['local_path_base'] = value
    runtime.config.write_text(yaml.safe_dump(settings))
    with pytest.raises(ValueError, match='local_path_base'):
        load_settings(runtime.config)


@pytest.mark.parametrize('endpoint', ['../input', 'data/../input', 'https://example.invalid/input'])
def test_traversal_and_unknown_remote_schemes_are_not_local_paths(runtime, endpoint):
    runtime.rows[0]['source'] = endpoint
    runtime.write()
    with pytest.raises(ValueError):
        Executor(runtime.config).preflight(runtime.request)
    assert not (runtime.root / 'a/run-123').exists()


def test_remote_sources_and_optional_destinations_keep_their_urls(runtime):
    runtime.rows[0].update(source='ssh://reader@example.invalid/ready', credential_ref='reader')
    runtime.write()
    settings = load_settings(runtime.config)
    settings['credentials'] = {'reader': {}}
    remote = load_steps(settings)['a'][0]
    assert remote.source == 'ssh://reader@example.invalid/ready'
    assert remote.destination == str(runtime.root / 'a')
    runtime.rows[0].update(source='data/input', destination='ena://webin2.ebi.ac.uk/incoming',
                           adapter='ena')
    runtime.write()
    ena = load_steps(settings)['a'][0]
    assert ena.source == str(runtime.root / 'input')
    assert ena.destination == 'ena://webin2.ebi.ac.uk/incoming'


def test_aliased_source_destination_overlap_is_rejected(runtime):
    output = runtime.directory / 'data/output'
    output.unlink()
    output.symlink_to(runtime.payload.parent, target_is_directory=True)
    with pytest.raises(ValueError, match='overlap'):
        Executor(runtime.config).preflight(runtime.request)


def test_two_aliases_to_one_source_still_conflict_when_moving(runtime):
    (runtime.directory / 'data/other-input').symlink_to(runtime.payload.parent, target_is_directory=True)
    runtime.rows[0]['operation'] = 'move'
    runtime.rows.append(dict(runtime.rows[0], identifiers='b', flow_group='b',
                             source='data/other-input', destination=str(runtime.root / 'b')))
    runtime.write()
    with pytest.raises(ValueError, match='Conflicting shared-source'):
        load_steps(load_settings(runtime.config))


def test_an_alias_and_absolute_spelling_of_one_source_still_conflict(runtime):
    runtime.rows[0]['operation'] = 'move'
    runtime.rows.append(dict(runtime.rows[0], identifiers='b', flow_group='b',
                             source=str(runtime.payload.parent) + '/', destination=str(runtime.root / 'b')))
    runtime.write()
    with pytest.raises(ValueError, match='Conflicting shared-source'):
        load_steps(load_settings(runtime.config))


def test_directory_access_checks_use_the_resolved_target(runtime, monkeypatch):
    assert credential_validation.validate_credentials(runtime.config)['status'] == 'passed'
    original = credential_validation.os.access
    checked = []

    def access(path, mode, **kwargs):
        checked.append(path)
        return False if path == str(runtime.root / 'a') else original(path, mode, **kwargs)

    monkeypatch.setattr(credential_validation.os, 'access', access)
    result = credential_validation.validate_credentials(runtime.config)
    assert result['status'] == 'failed'
    assert str(runtime.root / 'a') in checked
    check = result['connections'][0]['checks'][-1]
    assert check['direction'] == 'destination'
    assert check['error']['category'] == 'permission_denied'


def test_payload_symlinks_are_still_rejected_below_configured_aliases(runtime):
    (runtime.payload / 'escape').symlink_to(runtime.root / 'b', target_is_directory=True)
    with pytest.raises(ValueError, match='Symlinks'):
        Executor(runtime.config).preflight(runtime.request)


def test_dangling_output_link_fails_before_transfer(runtime):
    output = runtime.directory / 'data/output'
    output.unlink()
    output.symlink_to(runtime.root / 'missing-output', target_is_directory=True)
    with pytest.raises(ValueError, match='Destination root must exist'):
        Executor(runtime.config).preflight(runtime.request)
    assert runtime.payload.exists() and not (runtime.root / 'missing-output').exists()


def test_symlink_loop_is_a_configuration_error(runtime):
    output = runtime.directory / 'data/output'
    output.unlink()
    output.symlink_to('output')
    with pytest.raises(ValueError, match='resolv'):
        Executor(runtime.config).preflight(runtime.request)


def test_move_recovery_keeps_accepted_physical_roots_after_link_changes(runtime, monkeypatch):
    runtime.rows[0]['operation'] = 'move'
    runtime.write()
    original = source_module.cleanup

    def interrupt(source, accepted):
        (source / 'empty.txt').unlink()
        raise PermissionError('cleanup interrupted')

    monkeypatch.setattr(source_module, 'cleanup', interrupt)
    state = Executor(runtime.config).submit(runtime.request)
    assert state['status'] == 'blocked'
    replacement = runtime.root / 'replacement-input'
    (replacement / 'run-123').mkdir(parents=True)
    (replacement / 'run-123/data.txt').write_text('leave this source alone')
    for alias, target in [('input', replacement), ('output', runtime.root / 'b')]:
        (runtime.directory / 'data' / alias).unlink()
        (runtime.directory / 'data' / alias).symlink_to(target, target_is_directory=True)
    monkeypatch.setattr(source_module, 'cleanup', original)
    resumed = Executor(runtime.config).resume(state['request_id'])
    assert resumed['status'] == 'completed' and not runtime.payload.exists()
    assert (runtime.root / 'a/run-123/data.txt').read_text() == 'accepted data'
    assert not (runtime.root / 'b/run-123').exists()
    assert (replacement / 'run-123/data.txt').read_text() == 'leave this source alone'
    saved = json.loads((runtime.directory / 'state' / (state['request_id'] + '.json')).read_text())
    assert step(saved)['definition']['source'] == str(runtime.root / 'input')


def test_intake_input_root_can_use_the_configured_alias(runtime):
    user = runtime.root / 'user-output'
    user.mkdir()
    runtime.payload.rename(user / runtime.payload.name)
    request = dict(runtime.request, intake=dict(source_path=str(user / runtime.payload.name),
                                               input_root='data/input', operation='move'))
    state = Executor(runtime.config).submit(request)
    assert state['status'] == 'completed'
    assert runtime.payload.exists() and not (user / runtime.payload.name).exists()
