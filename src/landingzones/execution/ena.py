"""Optional ENA upload transport using verified explicit FTPS.

The UUID directory is the final Webin upload location. ENA uploads have no
consumer-directory rename contract, and are not archival submissions by themselves.
"""
import ftplib
import hashlib
import os
from pathlib import Path
import re
import ssl
from urllib.parse import urlsplit
import uuid

from .model import local_root
from .package import is_metadata


_HOSTS = {'webin2.ebi.ac.uk', 'webin.ebi.ac.uk'}
_COMPONENT = re.compile(r'[A-Za-z0-9_.-]+')


def _relative(value, allow_empty=False):
    if not isinstance(value, str) or (not value and not allow_empty):
        raise ValueError('ENA requires a safe relative upload path')
    if not value and allow_empty:
        return value
    if any(part in ('', '.', '..') or not _COMPONENT.fullmatch(part)
           for part in value.split('/')):
        raise ValueError('ENA requires safe relative path components without traversal')
    return value


def parse_destination(value):
    """Return the allowlisted host and account-relative Webin upload root."""
    if not isinstance(value, str) or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError('Invalid ENA destination')
    try:
        target = urlsplit(value)
        valid = (target.scheme == 'ena' and target.hostname in _HOSTS
                 and target.netloc == target.hostname and not target.query
                 and not target.fragment and (not target.path or target.path.startswith('/')))
    except ValueError:
        raise ValueError('Invalid ENA destination') from None
    if not valid:
        raise ValueError('ENA destination must use ena://webin2.ebi.ac.uk/upload-root')
    # URL slashes describe an account-relative path, never the FTP server root.
    return target.hostname, _relative(target.path.strip('/'), allow_empty=True)


def _failure(action, exc):
    # FTP replies can echo credentials or commands. Persist only the status code.
    code = re.match(r'([1-5][0-9]{2})(?:\s|$)', str(exc))
    detail = 'FTP ' + code.group(1) if code else 'transport error'
    return OSError('ENA ' + action + ' failed (' + detail + ')')


class ENAAdapter:
    publication_mode = 'upload'

    def __init__(self, step, settings):
        self.step = step
        self.settings = settings
        self.host, self.root = parse_destination(step.destination)
        if step.operation != 'copy' or step.verification != 'checksum':
            raise ValueError('ENA supports checksum-verified copies only')
        local_root(step.source)
        self.credentials = settings.get('credentials', {}).get(step.credential_ref)
        if not isinstance(self.credentials, dict):
            raise ValueError('ENA requires configured Webin credentials')
        username = self.credentials.get('username')
        password_env = self.credentials.get('password_env')
        if not isinstance(username, str) or not re.fullmatch(r'Webin-[0-9]+', username, re.IGNORECASE):
            raise ValueError('ENA credentials require a Webin account username')
        if not isinstance(password_env, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', password_env):
            raise ValueError('ENA credentials require a password_env variable name')
        self.ftp = None
        self._inspected = {}

    def _call(self, action, method, *args, **kwargs):
        try:
            return method(*args, **kwargs)
        except ftplib.all_errors as exc:
            raise _failure(action, exc) from None

    def __enter__(self):
        password = os.environ.get(self.credentials['password_env'])
        if not password:
            raise ValueError('ENA password environment variable is missing or empty')
        try:
            self.ftp = self._call('TLS initialization', ftplib.FTP_TLS,
                                  context=ssl.create_default_context(), timeout=30)
            self._call('connection', self.ftp.connect, self.host, port=21, timeout=30)
            self._call('authentication', self.ftp.login, self.credentials['username'],
                       password, secure=True)
            self._call('data protection', self.ftp.prot_p)
            self._call('passive mode', self.ftp.set_pasv, True)
            self._directory(self.root)
            return self
        except BaseException:
            if self.ftp is not None:
                try:
                    self.ftp.close()
                except Exception:
                    pass
            self.ftp = None
            raise

    def __exit__(self, exc_type=None, exc=None, traceback=None):
        if self.ftp is not None:
            ftp, self.ftp = self.ftp, None
            try:
                self._call('connection close', ftp.close)
            except OSError:
                if exc_type is None:
                    raise

    def prepare_request(self, options, manifest):
        from .ena_submission import prepare_submission
        return prepare_submission(options, manifest, self.settings)

    def finalize(self, plan, upload_receipt, progress, persist):
        if not plan:
            return {}
        from .ena_submission import submit_metadata
        return submit_metadata(plan, upload_receipt, self.credentials, progress, persist)

    def reconcile(self, plan, receipt, progress, persist):
        from .ena_submission import reconcile_submission
        return reconcile_submission(plan, receipt, progress, persist)

    def stage_name(self, token):
        try:
            identifier = str(uuid.UUID(token))
        except (ValueError, TypeError, AttributeError):
            raise ValueError('ENA staging requires a UUID owned by local execution state') from None
        return 'landingzones-' + identifier

    def _stage(self, relative):
        if not isinstance(relative, str) or not relative.startswith('landingzones-'):
            raise ValueError('ENA writes require a UUID staging directory')
        if self.stage_name(relative[len('landingzones-'):]) != relative:
            raise ValueError('ENA writes require a canonical UUID staging directory')
        return self.path(relative)

    def path(self, relative):
        return '/'.join(part for part in (self.root, _relative(relative)) if part)

    def _entries(self, path):
        _relative(path, allow_empty=True)
        # Consume the generator inside _call: MLSD failures may occur on iteration.
        entries = self._call('directory listing', lambda: list(self.ftp.mlsd(path, facts=['type', 'size'])))
        result = {}
        for entry, facts in entries:
            kind = facts.get('type', '').lower()
            if entry in ('.', '..') and kind in ('cdir', 'pdir'):
                continue
            if '/' in entry:
                raise ValueError('ENA returned an unsafe directory entry')
            _relative(entry)
            if entry in result or kind not in ('dir', 'file'):
                raise ValueError('ENA directory listing has unsupported or ambiguous file types')
            if kind == 'file':
                size = facts.get('size', '')
                if not re.fullmatch(r'[0-9]+', str(size)):
                    raise ValueError('ENA directory listing is missing a valid file size')
                result[entry] = {'kind': 'file', 'size': int(size)}
            else:
                result[entry] = {'kind': 'directory'}
        return result

    def _entry(self, path):
        _relative(path)
        parent, _, leaf = path.rpartition('/')
        self._directory(parent)
        return self._entries(parent).get(leaf)

    def _directory(self, path):
        _relative(path, allow_empty=True)
        current = ''
        for part in path.split('/') if path else []:
            entry = self._entries(current).get(part)
            if entry is None or entry['kind'] != 'directory':
                raise ValueError('ENA upload path must be an existing directory')
            current = '/'.join(filter(None, (current, part)))
        if not path:
            self._entries('')  # Require MLSD support even for the account home.

    def exists(self, relative):
        return self._entry(self.path(relative)) is not None

    def _mkdir(self, path):
        entry = self._entry(path)
        if entry is None:
            self._call('directory creation', self.ftp.mkd, path)
        elif entry['kind'] != 'directory':
            raise ValueError('ENA staging directory conflicts with an existing file')
        self._directory(path)

    def _walk(self, root, prefix=''):
        for entry, item in sorted(self._entries(root).items()):
            key = prefix + entry
            yield key, item
            if item['kind'] == 'directory':
                yield from self._walk(root + '/' + entry, key + '/')

    def copy(self, source, relative, accepted):
        from .adapters import manifest
        root = self._stage(relative)
        source = Path(local_root(str(source)))
        for key, item in accepted.items():
            _relative(key)
            if is_metadata(key) or item.get('kind') not in ('file', 'directory'):
                raise ValueError('ENA upload inventory contains unsupported content')
        if manifest(source) != accepted:
            raise ValueError('Source changed since acceptance')
        self._inspected.pop(relative, None)
        self._mkdir(root)
        # Local state owns this unguessable UUID directory across retries. Never
        # delete remote data or overwrite an unexpected path, even within it.
        for key, item in self._walk(root):
            if key not in accepted or item['kind'] != accepted[key]['kind']:
                raise ValueError('ENA staging contains unexpected content; upload refused')
        for key, item in sorted(accepted.items(), key=lambda pair: (pair[0].count('/'), pair[0])):
            target = root + '/' + key
            if item['kind'] == 'directory':
                self._mkdir(target)
            else:
                entry = self._entry(target)
                if entry is not None and entry['kind'] != 'file':
                    raise ValueError('ENA upload conflicts with an existing directory')
                with (source / key).open('rb') as handle:
                    # STOR restarts this owned file; no REST append of partial bytes.
                    self._call('file upload', self.ftp.storbinary, 'STOR ' + target, handle)

    def inspect(self, relative):
        root = self._stage(relative)
        self._inspected.pop(relative, None)
        self._directory(root)
        result, files = {}, {}
        for key, item in self._walk(root):
            if is_metadata(key):
                raise ValueError('ENA upload directory contains internal package metadata')
            if item['kind'] == 'directory':
                result[key] = item
                continue
            sha256 = hashlib.sha256()
            try:
                md5 = hashlib.md5(usedforsecurity=False)
            except TypeError:  # Python 3.8 predates the non-security checksum flag.
                md5 = hashlib.md5()
            size = 0

            def consume(chunk):
                nonlocal size
                sha256.update(chunk)
                md5.update(chunk)
                size += len(chunk)

            self._call('file verification', self.ftp.retrbinary, 'RETR ' + root + '/' + key, consume)
            if size != item['size']:
                raise ValueError('ENA file size changed during verification')
            result[key] = {'kind': 'file', 'size': size, 'sha256': sha256.hexdigest()}
            files[key] = {'remote_path': root + '/' + key, 'size': size,
                          'sha256': sha256.hexdigest(), 'md5': md5.hexdigest()}
        self._inspected[relative] = files
        return result

    def write_label(self, relative, label):
        self._stage(relative)
        # ENA receives scientific files only; labels remain in the local receipt.

    def promote(self, relative, final):
        root = self._stage(relative)
        if relative not in self._inspected:
            self.inspect(relative)
        return {'kind': 'ena_upload', 'remote_directory': root,
                'files': {key: dict(item) for key, item in self._inspected[relative].items()},
                'archival_status': 'not_submitted'}
