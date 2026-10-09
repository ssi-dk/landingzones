"""Offline submission safety: exact intent, receipts, and ambiguous recovery."""
import copy
import hashlib
import json
from urllib.error import HTTPError, URLError

import pytest

from landingzones.execution import ena_submission as ena


XML = '''<WEBIN><SUBMISSION_SET><SUBMISSION alias="batch-one">
<ACTIONS><ACTION><ADD/></ACTION><ACTION><HOLD/></ACTION></ACTIONS>
</SUBMISSION></SUBMISSION_SET><RUN_SET><RUN alias="run-one">
<DATA_BLOCK><FILES><FILE filename="reads/read.fastq.gz"/></FILES></DATA_BLOCK>
</RUN></RUN_SET></WEBIN>'''
SUCCESS = '''<RECEIPT success="true"><RUN alias="run-one" accession="ERR123" status="PRIVATE"/>
<SAMPLE alias="sample-one" accession="ERS123"><EXT_ID accession="SAMEA123" type="biosample"/></SAMPLE>
<SUBMISSION alias="batch-one" accession="ERA123"/><MESSAGES><INFO>Accepted</INFO></MESSAGES></RECEIPT>'''
REJECTED = '<RECEIPT success="false"><MESSAGES><ERROR>Invalid taxonomy</ERROR></MESSAGES></RECEIPT>'
MANIFEST = {'reads': {'kind': 'directory'},
            'reads/read.fastq.gz': {'kind': 'file', 'size': 123, 'sha256': 'b' * 64}}
UPLOAD = {'kind': 'ena_upload', 'files': {'reads/read.fastq.gz': {
    'remote_path': 'uploads/landingzones-example/reads/read.fastq.gz',
    'md5': 'a' * 32, 'sha256': 'b' * 64, 'size': 123}}}
CREDENTIAL = {'username': 'Webin-123', 'password_env': 'ENA_TEST_PASSWORD'}


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('Test attempted an actual network request')
    monkeypatch.setattr(ena.request, 'build_opener', forbidden)
    monkeypatch.setenv('ENA_TEST_PASSWORD', 'test-secret-never-persist')


@pytest.fixture
def prepare(tmp_path):
    path = tmp_path.resolve() / 'webin.xml'
    def make(xml=XML, settings=None):
        path.write_text(xml, encoding='utf-8')
        return ena.prepare_submission({'submission_file': str(path)}, MANIFEST, settings or {})
    return make


def test_snapshot_exact_xml_and_default_test_environment(prepare):
    plan = prepare()
    assert plan == {'xml': XML, 'sha256': hashlib.sha256(XML.encode()).hexdigest(),
                    'alias': 'batch-one', 'environment': 'test'}
    assert json.loads(json.dumps(plan)) == plan
    assert prepare(settings={'ena': {'environment': 'production'}})['environment'] == 'production'
    assert ena.prepare_submission({}, MANIFEST, {}) == {}
    assert ena.prepare_submission(None, MANIFEST, {}) == {}
    assert ena.submit_metadata({}, UPLOAD, {}, {}, lambda: None) is UPLOAD


@pytest.mark.parametrize('change', [
    lambda x: x.replace('<WEBIN>', '<!DOCTYPE WEBIN><WEBIN>'),
    lambda x: x.replace('<WEBIN>', '<!ENTITY bad "value"><WEBIN>'),
    lambda x: x.replace('WEBIN', 'SUBMISSION_SET'),
    lambda x: x.replace(' alias="batch-one"', ''),
    lambda x: x.replace('<ADD/>', '<MODIFY/>'),
    lambda x: x.replace('<HOLD/>', '<RELEASE/>'),
    lambda x: x.replace('<HOLD/>', '<ADD/>'),
    lambda x: x.replace('<ACTION><HOLD/></ACTION>', '<ACTION><HOLD/></ACTION>' * 2),
    lambda x: x.replace('reads/read.fastq.gz', '../reads/read.fastq.gz'),
    lambda x: x.replace('reads/read.fastq.gz', '/reads/read.fastq.gz'),
    lambda x: x.replace('reads/read.fastq.gz', 'reads//read.fastq.gz'),
    lambda x: x.replace('reads/read.fastq.gz', 'reads/%2eread.fastq.gz'),
    lambda x: x.replace('reads/read.fastq.gz', '.ready'),
    lambda x: x.replace('reads/read.fastq.gz', 'reads'),
    lambda x: x.replace('filename="reads/read.fastq.gz"', 'filename="reads/read.fastq.gz" checksum_method="SHA256"'),
    lambda x: x.replace('filename="reads/read.fastq.gz"', 'filename="reads/read.fastq.gz" checksum="bad"'),
    lambda x: x.replace('</RUN_SET>', '<RUN alias="run-one"/></RUN_SET>'),
])
def test_reject_unsupported_or_unsafe_prepared_xml(prepare, change):
    with pytest.raises(ValueError):
        prepare(change(XML))


def test_options_file_encoding_and_environment_validation(tmp_path, prepare, monkeypatch):
    with pytest.raises(ValueError, match='Unknown'):
        ena.prepare_submission({'environment': 'production'}, MANIFEST, {})
    with pytest.raises(ValueError, match='absolute'):
        ena.prepare_submission({'submission_file': 'relative.xml'}, MANIFEST, {})
    with pytest.raises(ValueError, match='environment'):
        prepare(settings={'ena': {'environment': 'development'}})
    path = tmp_path.resolve() / 'bad.xml'
    path.write_bytes(b'\xff')
    with pytest.raises(ValueError, match='UTF-8'):
        ena.prepare_submission({'submission_file': str(path)}, MANIFEST, {})
    monkeypatch.setattr(ena, 'MAX_XML_BYTES', 5)
    with pytest.raises(ValueError, match='size limit'):
        prepare()


def test_submit_rewrites_verified_paths_md5_and_saves_before_post(prepare, monkeypatch):
    plan = prepare()
    progress, saved, posts = {}, [], []
    def persist():
        saved.append(copy.deepcopy(progress))
    def post(endpoint, body, username, password):
        assert saved[-1]['status'] == 'submitting'
        assert saved[-1]['request_sha256'] == hashlib.sha256(body).hexdigest()
        assert username == 'Webin-123' and password == 'test-secret-never-persist'
        posts.append(endpoint)
        file = ena.ET.fromstring(body).find('.//FILE')
        assert file.attrib == {'filename': UPLOAD['files']['reads/read.fastq.gz']['remote_path'],
                               'checksum_method': 'MD5', 'checksum': 'a' * 32}
        return SUCCESS
    monkeypatch.setattr(ena, '_post', post)
    result = ena.submit_metadata(plan, UPLOAD, CREDENTIAL, progress, persist)
    assert result['status'] == 'accepted' and result['archive_validated'] is False
    assert result['receipt_xml'] == SUCCESS
    assert result['accessions'][1]['external_accessions'] == [{'accession': 'SAMEA123', 'type': 'biosample'}]
    assert progress['status'] == saved[-1]['status'] == 'accepted'
    assert 'test-secret-never-persist' not in json.dumps(progress)
    assert plan['xml'] == XML
    assert ena.submit_metadata(plan, UPLOAD, {}, progress, persist) == result
    assert posts == [ena.ENDPOINTS['test']]


def test_supplied_md5_must_match_upload_before_post(prepare):
    plan = prepare(XML.replace('filename="reads/read.fastq.gz"',
                               'filename="reads/read.fastq.gz" checksum="' + 'c' * 32 + '"'))
    progress = {}
    with pytest.raises(ValueError, match='MD5 differs'):
        ena.submit_metadata(plan, UPLOAD, CREDENTIAL, progress, lambda: None)
    assert not progress


@pytest.mark.parametrize('upload', [
    {'kind': 'unknown', 'files': {}}, {'kind': 'ena_upload', 'files': {}},
    {'kind': 'ena_upload', 'files': {'reads/read.fastq.gz': {'md5': 'a' * 32, 'remote_path': '../bad'}}},
])
def test_missing_or_unsafe_upload_evidence_never_posts(prepare, upload):
    with pytest.raises(ValueError):
        ena.submit_metadata(prepare(), upload, CREDENTIAL, {}, lambda: None)


@pytest.mark.parametrize('failure', [
    TimeoutError('test-secret-never-persist'),
    URLError('test-secret-never-persist'),
    HTTPError(ena.ENDPOINTS['test'], 503, 'unavailable', {}, None),
    ConnectionResetError('test-secret-never-persist'),
])
def test_uncertain_network_outcomes_are_durable_and_never_retried(prepare, monkeypatch, failure):
    calls, saved, progress = [], [], {}
    def post(*args):
        calls.append(args[0])
        raise failure
    monkeypatch.setattr(ena, '_post', post)
    plan = prepare()
    for expected in ('outcome is uncertain', 'automatic repost refused'):
        with pytest.raises(ValueError, match=expected) as error:
            ena.submit_metadata(plan, UPLOAD, CREDENTIAL, progress,
                                lambda: saved.append(copy.deepcopy(progress)))
        assert 'test-secret-never-persist' not in str(error.value)
    assert len(calls) == 1
    assert saved[-1]['status'] == progress['status'] == 'uncertain'


@pytest.mark.parametrize('receipt', [
    '<html>Unavailable</html>', '<RECEIPT success="true"/>',
    SUCCESS.replace('batch-one', 'another-batch'),
    SUCCESS.replace(' accession="ERA123"', ''),
    SUCCESS.replace('<INFO>Accepted</INFO>', '<ERROR>Contradictory error</ERROR>'),
    '<!DOCTYPE RECEIPT><RECEIPT success="true"/>',
])
def test_invalid_or_uncorrelated_receipt_requires_reconciliation(prepare, monkeypatch, receipt):
    monkeypatch.setattr(ena, '_post', lambda *args: receipt)
    progress = {}
    with pytest.raises(ValueError, match='uncertain'):
        ena.submit_metadata(prepare(), UPLOAD, CREDENTIAL, progress, lambda: None)
    assert progress['status'] == 'uncertain'


def test_definite_rejection_saves_errors_and_refuses_automatic_repost(prepare, monkeypatch):
    calls = []
    def post(*args):
        calls.append(1)
        return REJECTED
    monkeypatch.setattr(ena, '_post', post)
    progress, plan = {}, prepare()
    with pytest.raises(ValueError, match='rejected'):
        ena.submit_metadata(plan, UPLOAD, CREDENTIAL, progress, lambda: None)
    assert progress['result']['receipt_xml'] == REJECTED
    assert progress['result']['messages']['error'] == ['Invalid taxonomy']
    with pytest.raises(ValueError, match='automatic repost'):
        ena.submit_metadata(plan, UPLOAD, CREDENTIAL, progress, lambda: None)
    assert len(calls) == 1


def test_process_interruption_leaves_guard_and_manual_receipt_recovers(prepare, monkeypatch):
    def interrupt(*args):
        raise SystemExit('interrupted after durable POST intent')
    monkeypatch.setattr(ena, '_post', interrupt)
    progress, saved, plan = {}, [], prepare()
    persist = lambda: saved.append(copy.deepcopy(progress))
    with pytest.raises(SystemExit):
        ena.submit_metadata(plan, UPLOAD, CREDENTIAL, progress, persist)
    assert progress['status'] == 'submitting'
    with pytest.raises(ValueError, match='automatic repost'):
        ena.submit_metadata(plan, UPLOAD, CREDENTIAL, progress, persist)
    for bad in (SUCCESS.replace('batch-one', 'different'), REJECTED):
        with pytest.raises(ValueError):
            ena.reconcile_submission(plan, bad, progress, persist)
        assert progress['status'] == 'submitting'
    result = ena.reconcile_submission(plan, SUCCESS, progress, persist)
    assert result['status'] == saved[-1]['status'] == 'accepted'
    assert ena.submit_metadata(plan, UPLOAD, {}, progress, persist) == result


def test_snapshot_or_environment_cannot_change_on_recovery(prepare, monkeypatch):
    monkeypatch.setattr(ena, '_post', lambda *args: SUCCESS)
    progress, plan = {}, prepare()
    ena.submit_metadata(plan, UPLOAD, CREDENTIAL, progress, lambda: None)
    changed = dict(plan, environment='production')
    with pytest.raises(ValueError, match='different request'):
        ena.submit_metadata(changed, UPLOAD, CREDENTIAL, progress, lambda: None)
    with pytest.raises(ValueError, match='snapshot'):
        ena.submit_metadata(dict(plan, xml=XML + ' '), UPLOAD, CREDENTIAL, {}, lambda: None)


def test_missing_password_has_no_post_intent(prepare, monkeypatch):
    monkeypatch.delenv('ENA_TEST_PASSWORD')
    progress = {}
    with pytest.raises(ValueError, match='unset or empty'):
        ena.submit_metadata(prepare(), UPLOAD, CREDENTIAL, progress, lambda: None)
    assert not progress


def test_https_request_uses_basic_auth_bounded_read_and_no_redirects(monkeypatch):
    observed = {}
    class Response:
        status = 200
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def read(self, size):
            observed['read_limit'] = size
            return SUCCESS.encode()
    class Opener:
        def open(self, req, timeout):
            observed.update(req=req, timeout=timeout)
            return Response()
    def build(*handlers):
        observed['handlers'] = handlers
        return Opener()
    monkeypatch.setattr(ena.request, 'build_opener', build)
    assert ena._post(ena.ENDPOINTS['test'], b'<WEBIN/>', 'Webin-123', 'synthetic') == SUCCESS
    assert observed['req'].get_method() == 'POST'
    assert observed['req'].headers['Authorization'].startswith('Basic ')
    assert observed['req'].headers['Content-type'] == 'application/xml'
    assert observed['timeout'] == ena.SUBMISSION_TIMEOUT
    assert observed['read_limit'] == ena.MAX_RECEIPT_BYTES + 1
    assert isinstance(observed['handlers'][0], ena._NoRedirect)
    assert observed['handlers'][0].redirect_request(None, None, 302, '', {}, 'https://elsewhere.invalid') is None
    context = observed['handlers'][1]._context
    assert context.check_hostname is True
    assert context.verify_mode == ena.ssl.CERT_REQUIRED
