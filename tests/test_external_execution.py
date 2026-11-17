"""Optional transports and independent delivery/submission recovery, offline."""
import builtins
import json

import pytest
import yaml

from landingzones.execution.engine import Executor
from landingzones.execution.adapters import LocalAdapter
from tests.test_execution import setup, step  # Shared synthetic execution fixture.


def test_unused_ena_credentials_and_missing_backends_do_not_affect_local(setup, monkeypatch):
    root, payload, rows, write, config, request = setup
    settings = yaml.safe_load(config.read_text())
    settings['credentials'] = {'ena_webin': {'username': 'Webin-12345', 'password_env': 'UNSET_ENA_PASSWORD'}}
    config.write_text(yaml.safe_dump(settings))
    original = builtins.__import__

    def without_optional(name, *args, **kwargs):
        if name == 'paramiko' or name.endswith(('ena', 'ena_submission')):
            raise ModuleNotFoundError(name)
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', without_optional)
    assert Executor(config).submit(request)['status'] == 'completed'
    assert payload.exists()


def test_unknown_adapter_options_fail_before_copy(setup):
    root, payload, rows, write, config, request = setup
    request['adapter_options'] = {'submission_file': '/unavailable.xml'}
    with pytest.raises(ValueError, match='options'):
        Executor(config).preflight(request)
    assert not (root / 'a/run-123').exists()


@pytest.fixture
def external(setup, monkeypatch):
    """A transport double models upload followed by a separate service call."""
    calls = {'copy': 0, 'finalize': 0, 'fail': False}

    class ExternalAdapter(LocalAdapter):
        def prepare_request(self, options, manifest):
            return dict(options or {})

        def copy(self, source, relative, accepted):
            calls['copy'] += 1
            return super().copy(source, relative, accepted)

        def promote(self, relative, final):
            super().promote(relative, final)
            return {'kind': 'test_upload', 'remote_directory': final}

        def finalize(self, plan, receipt, progress, persist):
            calls['finalize'] += 1
            assert receipt['kind'] == 'test_upload'
            if calls['fail']:
                progress['status'] = 'uncertain'
                persist()
                raise ValueError('Submission outcome uncertain')
            result = {'accessions': [{'type': 'RUN', 'accession': 'ERR123'}], 'environment': 'test'}
            progress.update(status='accepted', result=result)
            persist()
            return result

    monkeypatch.setattr('landingzones.execution.engine.adapter', ExternalAdapter)
    return setup, calls


def test_submission_receipt_is_durable_and_repeat_does_not_reupload(external):
    (root, payload, rows, write, config, request), calls = external
    request['adapter_options'] = {'metadata_digest': 'first'}
    executor = Executor(config)
    state = executor.submit(request)
    assert state['status'] == 'completed'
    assert step(state)['submission']['status'] == 'accepted'
    assert step(state)['result']['accessions'][0]['accession'] == 'ERR123'
    assert executor.status(state['request_id'])['deliveries'][0]['steps'][0]['result'] == step(state)['result']
    assert executor.submit(request)['status'] == 'completed'
    assert calls['copy'] == calls['finalize'] == 1


def test_failed_submission_resumes_without_transfer_or_publication(external):
    (root, payload, rows, write, config, request), calls = external
    request['adapter_options'] = {'metadata_digest': 'first'}
    executor = Executor(config)
    calls['fail'] = True
    state = executor.submit(request)
    assert state['status'] == 'blocked'
    assert step(state)['published']
    assert step(state)['submission']['status'] == 'uncertain'
    assert calls['copy'] == 1
    calls['fail'] = False
    state = executor.resume(state['request_id'], retry_parked=True)
    assert state['status'] == 'completed'
    assert calls['copy'] == 1
    assert [a['phase'] for a in step(state)['attempts']] == ['transfer', 'promotion', 'submission', 'submission']


def test_submission_plan_survives_restart_without_reading_metadata_again(external):
    (root, payload, rows, write, config, request), calls = external
    request['adapter_options'] = {'metadata_digest': 'snapshot'}
    calls['fail'] = True
    state = Executor(config).submit(request)
    assert state['adapter_request'] == {'metadata_digest': 'snapshot'}
    calls['fail'] = False
    state = Executor(config).resume(state['request_id'], retry_parked=True)
    assert state['status'] == 'completed'
    assert calls['copy'] == 1


def test_upload_only_does_not_call_submission(external):
    (root, payload, rows, write, config, request), calls = external
    state = Executor(config).submit(request)
    assert state['status'] == 'completed'
    assert calls['finalize'] == 0
    assert step(state)['delivery_receipt']['kind'] == 'test_upload'
