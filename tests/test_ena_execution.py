"""ENA execution and recovery integration using synthetic files and memory FTPS."""
import hashlib
import json

import pytest
import yaml

from landingzones import cli
from landingzones.execution import ena_submission
from landingzones.execution.ena import ENAAdapter
from landingzones.execution.engine import Executor
from landingzones.execution.storage import StateStore
from tests.test_ena_submission import SUCCESS, XML
from tests.test_ena_upload import ftp
from tests.test_execution import setup, step


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('Integration test attempted an actual HTTP request')
    monkeypatch.setattr(ena_submission.request, 'build_opener', forbidden)
    monkeypatch.setattr(ena_submission, '_post', forbidden)


@pytest.fixture
def ena_setup(setup, ftp):
    root, payload, rows, write, config, request = setup
    rows[0].update(adapter='ena', destination='ena://webin2.ebi.ac.uk/incoming/',
                   credential_ref='webin')
    write()
    settings = yaml.safe_load(config.read_text())
    settings['credentials'] = {'webin': {'username': 'Webin-12345',
                                        'password_env': 'TEST_WEBIN_PASSWORD'}}
    config.write_text(yaml.safe_dump(settings))
    metadata = root / 'submission.xml'
    metadata.write_text(XML.replace('reads/read.fastq.gz', 'data.txt'))
    request['adapter_options'] = {'submission_file': str(metadata)}
    return root, payload, rows, write, config, request


def uploads(ftp):
    return [call for call in ftp.calls if call[0] == 'upload']


def request_id(request):
    return hashlib.sha256(request['idempotency_key'].encode()).hexdigest()


def test_upload_and_metadata_receipts_capture_accessions_and_retain_source(ena_setup, ftp, monkeypatch):
    root, payload, rows, write, config, request = ena_setup
    posts = []
    def post(endpoint, body, username, password):
        posts.append(endpoint)
        remote = ena_submission.ET.fromstring(body).find('.//FILE').get('filename')
        assert ftp.files[remote] == b'accepted data'
        assert username == 'Webin-12345' and password == 'synthetic-password'
        return SUCCESS
    monkeypatch.setattr(ena_submission, '_post', post)
    state = Executor(config).submit(request)
    work = step(state)
    assert state['status'] == 'completed'
    assert work['delivery_receipt']['kind'] == 'ena_upload'
    assert work['delivery_receipt']['files']['data.txt']['md5'] == hashlib.md5(b'accepted data').hexdigest()
    assert work['result']['status'] == 'accepted'
    assert work['result']['receipt_xml'] == SUCCESS
    assert {item['accession'] for item in work['result']['accessions']} == {'ERR123', 'ERS123', 'ERA123'}
    assert work['result']['environment'] == state['adapter_request']['environment'] == 'test'
    assert work['result']['archive_validated'] is False
    assert (payload / 'data.txt').read_text() == 'accepted data'
    assert (payload / '.ready').exists()
    assert len(uploads(ftp)) == 2
    assert posts == [ena_submission.ENDPOINTS['test']]
    assert Executor(config).submit(request) == state
    assert len(posts) == 1 and len(uploads(ftp)) == 2


def test_upload_only_never_calls_submission_service(ena_setup, ftp):
    root, payload, rows, write, config, request = ena_setup
    request.pop('adapter_options')
    state = Executor(config).submit(request)
    work = step(state)
    assert state['status'] == 'completed'
    assert work['delivery_receipt']['archival_status'] == 'not_submitted'
    assert not state['adapter_request']
    assert 'submission' not in work
    assert all(attempt['phase'] != 'submission' for attempt in work['attempts'])
    assert len(uploads(ftp)) == 2 and payload.exists()


def test_uncertain_post_stays_guarded_and_cli_receipt_reconciliation_completes(
        ena_setup, ftp, monkeypatch, capsys):
    root, payload, rows, write, config, request = ena_setup
    posts = []
    def timeout(*args):
        posts.append(1)
        raise TimeoutError('synthetic ambiguous response')
    monkeypatch.setattr(ena_submission, '_post', timeout)
    executor = Executor(config)
    state = executor.submit(request)
    identity = state['request_id']
    assert state['status'] == 'blocked'
    assert step(state)['submission']['status'] == 'uncertain'
    assert step(state)['published'] is True
    assert cli.main(['transfer', 'resume', '--config', str(config), identity, '--retry-parked']) == 1
    retried = json.loads(capsys.readouterr().out)
    assert step(retried)['submission']['status'] == 'uncertain'
    assert len(posts) == 1 and len(uploads(ftp)) == 2
    receipt = root / 'obtained-receipt.xml'
    receipt.write_text(SUCCESS)
    assert cli.main(['transfer', 'reconcile', '--config', str(config), identity,
                     '--receipt', str(receipt), '--environment', 'production']) == 2
    assert 'environment' in capsys.readouterr().err
    receipt.write_text(SUCCESS.replace('batch-one', 'wrong-alias'))
    assert cli.main(['transfer', 'reconcile', '--config', str(config), identity,
                     '--receipt', str(receipt), '--environment', 'test']) == 2
    assert 'alias' in capsys.readouterr().err
    assert step(executor.status(identity))['submission']['status'] == 'uncertain'
    receipt.write_text(SUCCESS)
    assert cli.main(['transfer', 'reconcile', '--config', str(config), identity,
                     '--receipt', str(receipt), '--environment', 'test']) == 0
    recovered = json.loads(capsys.readouterr().out)
    assert recovered['status'] == 'completed'
    assert step(recovered)['result']['status'] == 'accepted'
    assert step(recovered)['receipt_sha256'] == hashlib.sha256(SUCCESS.encode()).hexdigest()
    assert len(posts) == 1 and len(uploads(ftp)) == 2
    assert payload.exists()


@pytest.mark.parametrize('failure', [SystemExit, OSError])
def test_crash_after_accepted_receipt_recovers_with_exhausted_attempt_budget(ena_setup, ftp, monkeypatch, failure):
    root, payload, rows, write, config, request = ena_setup
    rows[0]['max_attempts'] = '1'
    write()
    posts = []
    monkeypatch.setattr(ena_submission, '_post', lambda *args: posts.append(1) or SUCCESS)
    save = StateStore.save
    interrupted = []
    def crash_after_receipt(self, state):
        save(self, state)
        if step(state).get('submission', {}).get('status') == 'accepted' and not interrupted:
            interrupted.append(1)
            raise failure('crash after durable receipt, before completion bookkeeping')
    monkeypatch.setattr(StateStore, 'save', crash_after_receipt)
    executor = Executor(config)
    if failure is SystemExit:
        with pytest.raises(SystemExit):
            executor.submit(request)
    else:
        assert executor.submit(request)['status'] == 'blocked'
    durable = executor.status(request_id(request))
    assert step(durable)['submission']['status'] == 'accepted'
    assert sum(attempt['phase'] == 'submission' for attempt in step(durable)['attempts']) == 1
    state = Executor(config).resume(request_id(request))
    assert state['status'] == 'completed'
    assert step(state)['result']['receipt_xml'] == SUCCESS
    assert sum(attempt['phase'] == 'submission' for attempt in step(state)['attempts']) == 1
    assert len(posts) == 1 and len(uploads(ftp)) == 2


def test_unselected_ena_route_without_credentials_does_not_block_local_copy(setup, ftp):
    root, payload, rows, write, config, request = setup
    rows[0]['credential_ref'] = ''
    rows.append(dict(rows[0], identifiers='ena-unconfigured', flow_group='ena-unconfigured',
                     adapter='ena', destination='ena://webin2.ebi.ac.uk/incoming/',
                     credential_ref='not-configured'))
    write()
    state = Executor(config).submit(request)
    assert state['status'] == 'completed'
    assert (root / 'a' / 'run-123' / 'data.txt').read_text() == 'accepted data'
    assert not ftp.calls


def test_ena_move_is_rejected_before_connection_or_source_changes(ena_setup, ftp):
    root, payload, rows, write, config, request = ena_setup
    rows[0]['operation'] = 'move'
    write()
    with pytest.raises(ValueError, match='copy'):
        Executor(config).submit(request)
    assert not ftp.calls
    assert (payload / 'data.txt').read_text() == 'accepted data'
    assert (payload / '.ready').exists()


def test_missing_uuid_upload_after_interruption_is_not_inferred_to_be_delivered(ena_setup, ftp, monkeypatch):
    root, payload, rows, write, config, request = ena_setup
    def disappear(self, stage, final):
        remote = self.path(stage)
        ftp.directories = {path for path in ftp.directories if path != remote and not path.startswith(remote + '/')}
        ftp.files = {path: data for path, data in ftp.files.items() if not path.startswith(remote + '/')}
        raise SystemExit('upload directory disappeared during final receipt creation')
    monkeypatch.setattr(ENAAdapter, 'promote', disappear)
    executor = Executor(config)
    with pytest.raises(SystemExit):
        executor.submit(request)
    state = executor.resume(request_id(request))
    assert state['status'] == 'blocked'
    assert step(state)['promotion_intent'] is True
    assert not step(state).get('published')
    assert 'completion cannot be inferred' in step(state)['attempts'][-1]['error']
    assert 'submission' not in step(state)
    assert len(uploads(ftp)) == 2 and not ftp.files
    assert payload.exists()


def test_existing_payload_name_does_not_conflict_with_owned_uuid_upload(ena_setup, ftp):
    root, payload, rows, write, config, request = ena_setup
    request.pop('adapter_options')
    ftp.directories.add('incoming/run-123')
    ftp.files['incoming/run-123/foreign.txt'] = b'keep another upload'
    state = Executor(config).submit(request)
    assert state['status'] == 'completed'
    receipt = step(state)['delivery_receipt']
    assert receipt['remote_directory'].startswith('incoming/landingzones-')
    assert receipt['remote_directory'] != 'incoming/run-123'
    assert ftp.files['incoming/run-123/foreign.txt'] == b'keep another upload'
    assert len(uploads(ftp)) == 2
