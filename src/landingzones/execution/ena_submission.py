"""Optional ENA Webin V2 metadata submission, separate from file upload.

Protocol and receipt contract:
https://ena-docs.readthedocs.io/en/latest/submit/general-guide/programmatic.html

A successful receipt means ENA accepted the submission. It does not establish
subsequent file validation or archival completion. An interrupted POST requires
operator reconciliation; object aliases are not an HTTP idempotency guarantee.
"""
import base64
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import ssl
from urllib import request
from xml.etree import ElementTree as ET


MAX_XML_BYTES = 15 * 1024 * 1024
MAX_RECEIPT_BYTES = 4 * 1024 * 1024
SUBMISSION_TIMEOUT = 90
OBJECT_TYPES = {'SUBMISSION', 'PROJECT', 'STUDY', 'SAMPLE', 'EXPERIMENT', 'RUN', 'ANALYSIS'}
ENDPOINTS = {
    'test': 'https://wwwdev.ebi.ac.uk/ena/submit/webin-v2/submit',
    'production': 'https://www.ebi.ac.uk/ena/submit/webin-v2/submit',
}


def _parse_xml(text, limit):
    if not isinstance(text, str) or len(text.encode('utf-8')) > limit:
        raise ValueError('ENA XML exceeds the supported size limit')
    if re.search(r'<!\s*(?:DOCTYPE|ENTITY)\b', text, re.IGNORECASE):
        raise ValueError('ENA XML must not contain DTD or entity declarations')
    try:
        return ET.fromstring(text)
    except (ET.ParseError, ValueError):
        raise ValueError('Invalid ENA XML document') from None


def _file_name(value):
    if (not isinstance(value, str) or not value or '\\' in value
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
            or '%' in value or PurePosixPath(value).is_absolute()
            or any(part in ('', '.', '..') for part in value.split('/'))):
        raise ValueError('ENA FILE must name a safe account-relative file')
    return value


def _validate_document(root, manifest=None):
    if root.tag != 'WEBIN':
        raise ValueError('ENA metadata must be one Webin V2 WEBIN XML document')
    submissions = list(root.iter('SUBMISSION'))
    if len(submissions) != 1:
        raise ValueError('ENA metadata requires exactly one SUBMISSION')
    submission = submissions[0]
    alias = submission.get('alias', '')
    if not alias.strip() or any(ord(char) < 32 for char in alias):
        raise ValueError('ENA SUBMISSION requires an explicit nonempty alias')
    actions = submission.findall('ACTIONS')
    if len(actions) != 1:
        raise ValueError('ENA SUBMISSION requires ADD and optional HOLD actions')
    kinds = []
    for action in actions[0]:
        if action.tag != 'ACTION' or len(action) != 1:
            raise ValueError('Invalid ENA submission action')
        kinds.append(action[0].tag)
    if kinds.count('ADD') != 1 or kinds.count('HOLD') > 1 or set(kinds) - {'ADD', 'HOLD'}:
        raise ValueError('ENA submissions support ADD and optional HOLD only')
    aliases = set()
    for element in root.iter():
        if element.tag in OBJECT_TYPES and element.get('alias'):
            identity = (element.tag, element.get('alias'))
            if identity in aliases:
                raise ValueError('Duplicate ENA object alias within one object type')
            aliases.add(identity)
    for item in root.iter('FILE'):
        relative = _file_name(item.get('filename'))
        if manifest is not None and manifest.get(relative, {}).get('kind') != 'file':
            raise ValueError('ENA FILE references a file outside the accepted package')
        method = item.get('checksum_method', '')
        checksum = item.get('checksum', '')
        if method and method.lower() != 'md5':
            raise ValueError('ENA FILE checksum_method must be MD5')
        if checksum and not re.fullmatch(r'[0-9a-fA-F]{32}', checksum):
            raise ValueError('ENA FILE checksum must be a 32-character MD5')
    return alias


def prepare_submission(options, manifest, settings):
    """Snapshot a prepared local XML file without contacting ENA."""
    options = {} if options is None else options
    if not isinstance(options, dict) or set(options) - {'submission_file'}:
        raise ValueError('Unknown ENA request option')
    if not options:
        return {}
    filename = options.get('submission_file')
    if not isinstance(filename, str) or not filename:
        raise ValueError('ENA submission_file must be an absolute local file')
    path = Path(filename)
    if not path.is_absolute() or path.resolve() != path or not path.is_file():
        raise ValueError('ENA submission_file must be an absolute regular local file')
    with path.open('rb') as handle:
        data = handle.read(MAX_XML_BYTES + 1)
    if len(data) > MAX_XML_BYTES:
        raise ValueError('ENA XML exceeds the supported size limit')
    try:
        xml = data.decode('utf-8')
    except UnicodeDecodeError:
        raise ValueError('ENA XML must be UTF-8') from None
    alias = _validate_document(_parse_xml(xml, MAX_XML_BYTES), manifest)
    ena_settings = settings.get('ena', {})
    if not isinstance(ena_settings, dict):
        raise ValueError('Invalid ENA configuration')
    environment = ena_settings.get('environment', 'test')
    if environment not in ENDPOINTS:
        raise ValueError('ENA environment must be test or production')
    return {'xml': xml, 'sha256': hashlib.sha256(data).hexdigest(),
            'alias': alias, 'environment': environment}


def _plan_document(plan):
    try:
        xml = plan['xml']
        if (plan['environment'] not in ENDPOINTS
                or hashlib.sha256(xml.encode('utf-8')).hexdigest() != plan['sha256']):
            raise ValueError('Invalid ENA submission snapshot')
        root = _parse_xml(xml, MAX_XML_BYTES)
        if _validate_document(root) != plan['alias']:
            raise ValueError('ENA submission alias differs from its snapshot')
        return root
    except (KeyError, TypeError, AttributeError):
        raise ValueError('Invalid ENA submission snapshot') from None


def _request_xml(plan, upload_receipt):
    root = _plan_document(plan)
    if upload_receipt.get('kind') != 'ena_upload' or not isinstance(upload_receipt.get('files'), dict):
        raise ValueError('ENA metadata requires a verified upload receipt')
    for item in root.iter('FILE'):
        record = upload_receipt['files'].get(item.get('filename'), {})
        checksum = record.get('md5', '')
        if not isinstance(checksum, str) or not re.fullmatch(r'[0-9a-f]{32}', checksum):
            raise ValueError('ENA metadata file has no verified MD5 upload receipt')
        if item.get('checksum') and item.get('checksum').lower() != checksum:
            raise ValueError('ENA metadata MD5 differs from the uploaded file')
        item.set('filename', _file_name(record.get('remote_path')))
        item.set('checksum_method', 'MD5')
        item.set('checksum', checksum)
    body = ET.tostring(root, encoding='utf-8', xml_declaration=True)
    if len(body) > MAX_XML_BYTES:
        raise ValueError('Rewritten ENA XML exceeds the supported size limit')
    return body


def _receipt(plan, xml):
    root = _parse_xml(xml, MAX_RECEIPT_BYTES)
    if root.tag != 'RECEIPT' or root.get('success') not in ('true', 'false'):
        raise ValueError('ENA returned no conclusive submission receipt')
    submissions = root.findall('SUBMISSION')
    success = root.get('success') == 'true'
    # Rejected receipts may omit SUBMISSION, but a returned alias must match.
    if ((success and len(submissions) != 1) or len(submissions) > 1
            or any(item.get('alias') != plan['alias'] for item in submissions)):
        raise ValueError('ENA receipt does not match the submission alias')
    objects = []
    for item in root:
        if item.tag not in OBJECT_TYPES:
            continue
        if not item.get('accession'):
            if success:
                raise ValueError('ENA successful receipt is missing an accession')
            continue
        objects.append({'type': item.tag, 'accession': item.get('accession'),
                        'alias': item.get('alias', ''), 'status': item.get('status', ''),
                        'external_accessions': [dict(child.attrib) for child in item.iter('EXT_ID')]})
    messages = {}
    for item in root.findall('./MESSAGES/*'):
        messages.setdefault(item.tag.lower(), []).append(''.join(item.itertext()))
    if success and messages.get('error'):
        raise ValueError('ENA successful receipt contains contradictory errors')
    return {'kind': 'ena_submission', 'status': 'accepted' if success else 'rejected',
            'environment': plan['environment'], 'submission_alias': plan['alias'],
            'receipt_xml': xml, 'accessions': objects, 'messages': messages,
            'archive_validated': False}


class _NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Credentials and the metadata POST stay on the selected ENA service.
        return None


def _post(endpoint, body, username, password):
    authorization = base64.b64encode((username + ':' + password).encode('utf-8')).decode('ascii')
    req = request.Request(endpoint, data=body, method='POST', headers={
        'Content-Type': 'application/xml', 'Accept': 'application/xml',
        'Authorization': 'Basic ' + authorization,
    })
    opener = request.build_opener(_NoRedirect(), request.HTTPSHandler(context=ssl.create_default_context()))
    with opener.open(req, timeout=SUBMISSION_TIMEOUT) as response:
        if not 200 <= response.status < 300:
            raise ValueError('ENA HTTP response is inconclusive')
        data = response.read(MAX_RECEIPT_BYTES + 1)
    if len(data) > MAX_RECEIPT_BYTES:
        raise ValueError('ENA response exceeds the supported size limit')
    return data.decode('utf-8')


def _check_progress(plan, progress):
    if progress and (progress.get('plan_sha256') != plan['sha256']
                     or progress.get('environment') != plan['environment']):
        raise ValueError('ENA submission progress belongs to a different request')


def submit_metadata(plan, upload_receipt, credential, progress, persist):
    """Submit once; persist() durably saves the caller-owned progress dictionary."""
    if not plan:
        return upload_receipt
    _plan_document(plan)
    _check_progress(plan, progress)
    status = progress.get('status')
    if status == 'accepted':
        return progress['result']
    if status in ('submitting', 'uncertain', 'rejected'):
        raise ValueError('ENA submission requires receipt reconciliation or a corrected new request; automatic repost refused')
    if status is not None:
        raise ValueError('Unknown ENA submission progress state')
    body = _request_xml(plan, upload_receipt)
    username = credential.get('username')
    password_env = credential.get('password_env')
    if (not isinstance(username, str) or not username or ':' in username
            or any(ord(char) < 32 for char in username)
            or not isinstance(password_env, str)
            or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', password_env)):
        raise ValueError('ENA requires a username and password_env credential reference')
    password = os.environ.get(password_env)
    if not password:
        raise ValueError('ENA password environment variable is unset or empty')
    progress.update(status='submitting', plan_sha256=plan['sha256'],
                    environment=plan['environment'], submission_alias=plan['alias'],
                    request_sha256=hashlib.sha256(body).hexdigest())
    persist()  # A crash from this point onward must never trigger another POST.
    try:
        xml = _post(ENDPOINTS[plan['environment']], body, username, password)
        result = _receipt(plan, xml)
    except Exception:
        progress['status'] = 'uncertain'
        persist()
        raise ValueError('ENA submission outcome is uncertain; reconcile its receipt before continuing') from None
    progress.update(status=result['status'], result=result)
    persist()
    if result['status'] == 'rejected':
        raise ValueError('ENA rejected the metadata; inspect the saved receipt and create a corrected request')
    return result


def reconcile_submission(plan, receipt_xml, progress, persist):
    """Accept an operator-supplied successful receipt for an uncertain POST."""
    _plan_document(plan)
    _check_progress(plan, progress)
    if progress.get('status') == 'accepted':
        return progress['result']
    if progress.get('status') not in ('submitting', 'uncertain'):
        raise ValueError('Only an uncertain ENA submission can be reconciled')
    result = _receipt(plan, receipt_xml)
    if result['status'] != 'accepted':
        raise ValueError('ENA reconciliation requires a successful receipt for the requested alias')
    progress.update(status='accepted', result=result)
    persist()
    return result
