"""Transport operations; lifecycle decisions belong to the executor."""
import errno
import ctypes
import sys
import hashlib
import os
from pathlib import Path
import shutil
import shlex
import stat
import subprocess
from urllib.parse import urlsplit

from .model import local_root
from .package import LABEL, is_metadata


def digest(handle):
    result = hashlib.sha256()
    for chunk in iter(lambda: handle.read(1024 * 1024), b''):
        result.update(chunk)
    return result.hexdigest()


def manifest(root):
    root = Path(local_root(str(root)))
    if not root.is_dir():
        raise ValueError('Payload must be a directory: ' + str(root))
    result = {}
    for path in sorted(root.rglob('*')):
        mode = path.lstat().st_mode
        relative = path.relative_to(root).as_posix()
        if stat.S_ISLNK(mode) or not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
            raise ValueError('Symlinks and special payload files are unsupported')
        if is_metadata(relative):
            if relative in ('.ready', LABEL) and not stat.S_ISREG(mode):
                raise ValueError('Package marker must be a regular file')
            continue
        if any(c in relative for c in '\0\n\r'):
            raise ValueError('Control characters in payload names are unsupported')
        if path.is_dir():
            result[relative] = {'kind': 'directory'}
        else:
            with path.open('rb') as handle:
                result[relative] = {'kind': 'file', 'size': path.stat().st_size, 'sha256': digest(handle)}
    return result


def matches(actual, expected, verification='checksum'):
    if verification == 'size':
        def sizes(value):
            return {key: {k: v for k, v in item.items() if k != 'sha256'} for key, item in value.items()}
        return sizes(actual) == sizes(expected)
    return actual == expected


def cleanup(root, accepted):
    """Resume partial cleanup without removing unaccepted or changed content."""
    root = Path(local_root(str(root)))
    if not root.exists():
        return
    remaining = manifest(root)
    if any(key not in accepted or value != accepted[key] for key, value in remaining.items()):
        raise ValueError('Source changed; cleanup refused')
    for relative in sorted(remaining, key=lambda item: (item.count('/'), item), reverse=True):
        path = root / relative
        if remaining[relative]['kind'] == 'directory':
            path.rmdir()
        else:
            # Producer must relinquish writes after readiness; recheck before removal.
            with path.open('rb') as handle:
                if digest(handle) != accepted[relative]['sha256']:
                    raise ValueError('Source changed during cleanup')
            path.unlink()
    for metadata in (LABEL, '.landing_zones'):
        path = root / metadata
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
    marker = root / '.ready'
    if marker.exists():
        if marker.is_symlink() or not marker.is_file():
            raise ValueError('Invalid readiness marker')
        marker.unlink()
    root.rmdir()
    descriptor = os.open(str(root.parent), os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class LocalAdapter:
    def __init__(self, step, settings):
        self.step = step
        self.root = Path(local_root(step.destination))
        if not self.root.is_dir():
            raise ValueError('Destination root must already exist')
        if self.root == self.root.parent:
            raise ValueError('Destination must have a private sibling staging location')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def stage_name(self, token):
        return '../.landingzones-staging-' + self.root.name + '/' + token

    def write_label(self, relative, label):
        from .storage import atomic_json
        atomic_json(self.root / relative / LABEL, label)

    def exists(self, relative):
        path = Path(local_root(os.path.normpath(str(self.root / relative))))
        return path.exists()

    def inspect(self, relative):
        return manifest(local_root(os.path.normpath(str(self.root / relative))))

    def copy(self, source, relative, accepted):
        path = Path(local_root(os.path.normpath(str(self.root / relative))))
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.mkdir(exist_ok=True, mode=0o700)
        if path.stat().st_dev != self.root.stat().st_dev:
            raise ValueError('Staging and destination must be on the same filesystem')
        if path.parent.stat().st_uid != os.geteuid() or path.parent.stat().st_mode & 0o077:
            raise ValueError('Staging root must be private to the executor (mode 0700)')
        if self.step.adapter == 'rsync':
            subprocess.run(['rsync', '-r', '--delete', '--exclude=/.ready', '--exclude=/' + LABEL, '--exclude=/.landing_zones/', '--',
                            str(source) + '/', str(path) + '/'], check=True, timeout=3600)
        else:
            # A retry replaces only the UUID-owned staging directory.
            for key, item in accepted.items():
                destination = path / key
                local_root(str(destination))
                if item['kind'] == 'directory':
                    destination.mkdir(parents=True, exist_ok=True)
                else:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(Path(source) / key, destination)
        # Flush copied contents before publication. Directory persistence is flushed below.
        for item in path.rglob('*'):
            if item.is_file():
                with item.open('rb') as handle:
                    os.fsync(handle.fileno())
        for directory in [p for p in path.rglob('*') if p.is_dir()] + [path, path.parent, path.parent.parent]:
            descriptor = os.open(str(directory), os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def promote(self, relative, final):
        source = Path(local_root(os.path.normpath(str(self.root / relative))))
        destination = Path(local_root(str(self.root / final)))
        if destination.exists():
            raise FileExistsError('Destination already exists')
        # Refuse an existing target atomically, including competing publishers.
        libc = ctypes.CDLL(None, use_errno=True)
        if sys.platform == 'darwin':
            result = libc.renamex_np(os.fsencode(source), os.fsencode(destination), 4)  # RENAME_EXCL
        elif sys.platform.startswith('linux') and hasattr(libc, 'renameat2'):
            result = libc.renameat2(-100, os.fsencode(source), -100, os.fsencode(destination), 1)  # NOREPLACE
        else:
            raise ValueError('Atomic non-overwriting rename unsupported on this platform')
        if result:
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code), str(destination))
        for directory in (source.parent, destination.parent):
            descriptor = os.open(str(directory), os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)


class SFTPAdapter:
    def __init__(self, step, settings):
        self.step = step
        self.target = urlsplit(step.destination)
        self.root = self.target.path.rstrip('/') or '/'
        self.credentials = settings['credentials'][step.credential_ref]
        self.client = None

    def __enter__(self):
        import paramiko
        self.client = paramiko.SSHClient()
        self.client.load_host_keys(self.credentials['known_hosts_file'])
        self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
        try:
            self.client.connect(self.target.hostname, port=self.target.port or 22,
                                username=self.target.username,
                                key_filename=self.credentials['private_key_file'],
                                allow_agent=False, look_for_keys=False,
                                timeout=10, auth_timeout=10, banner_timeout=10)
            self.sftp = self.client.open_sftp()
            self.sftp.get_channel().settimeout(30)
            self.check_path(self.root)
            return self
        except BaseException:
            self.client.close()
            raise

    def __exit__(self, *args):
        if self.client:
            self.client.close()

    def check_path(self, path):
        current = ''
        attributes = self.sftp.lstat('/')
        for part in path.split('/'):
            if not part:
                continue
            current += '/' + part
            attributes = self.sftp.lstat(current)
            if stat.S_ISLNK(attributes.st_mode):
                raise ValueError('Remote symlink path refused')
        return attributes

    def path(self, relative):
        return self.root.rstrip('/') + '/' + relative

    def exists(self, relative):
        try:
            self.check_path(self.path(relative))
            return True
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                return False
            raise

    def inspect(self, relative):
        root = self.path(relative)
        if not stat.S_ISDIR(self.check_path(root).st_mode):
            raise ValueError('Destination is not a directory')
        result = {}

        def walk(path, prefix=''):
            for entry in self.sftp.listdir_attr(path):
                if entry.filename in ('.', '..') or '/' in entry.filename or any(c in entry.filename for c in '\0\n\r'):
                    raise ValueError('Unsafe remote entry name')
                key = prefix + entry.filename
                if not (stat.S_ISREG(entry.st_mode) or stat.S_ISDIR(entry.st_mode)):
                    raise ValueError('Remote special file refused')
                if is_metadata(key):
                    continue
                child = path + '/' + entry.filename
                if stat.S_ISDIR(entry.st_mode):
                    result[key] = {'kind': 'directory'}
                    walk(child, key + '/')
                elif stat.S_ISREG(entry.st_mode):
                    record = {'kind': 'file', 'size': entry.st_size}
                    if self.step.verification == 'checksum':
                        with self.sftp.open(child, 'rb') as handle:
                            record['sha256'] = digest(handle)
                    result[key] = record
                else:
                    raise ValueError('Remote special file refused')
        walk(root)
        return result

    def stage_name(self, token):
        # Remote copy staging is outside the watched destination root.
        return '../.landingzones-staging-' + self.root.rsplit('/', 1)[-1] + '/' + token

    def write_label(self, relative, label):
        import json
        with self.sftp.open(self.path(relative) + '/' + LABEL, 'w') as handle:
            handle.write(json.dumps(label, sort_keys=True))

    def mkdir(self, relative):
        path = self.path(relative)
        try:
            self.sftp.mkdir(path, mode=0o700)
        except IOError:
            if not stat.S_ISDIR(self.check_path(path).st_mode):
                raise
        self.check_path(path)

    def copy(self, source, relative, accepted):
        self.mkdir(relative.rsplit('/', 1)[0])
        self.mkdir(relative)
        for key, item in accepted.items():
            target = relative + '/' + key
            if item['kind'] == 'directory':
                self.mkdir(target)
            else:
                if self.exists(target):
                    if not stat.S_ISREG(self.check_path(self.path(target)).st_mode):
                        raise ValueError('Unexpected staging file type')
                self.sftp.put(str(Path(source) / key), self.path(target), confirm=True)

    def promote(self, relative, final):
        if self.exists(final):
            raise FileExistsError('Destination already exists')
        # Standard SFTP rename refuses an existing target (not posix_rename).
        self.sftp.rename(self.path(relative), self.path(final))


class RemoteRsyncAdapter(SFTPAdapter):
    """Rsync over SSH for bytes; SFTP for inspection and non-overwriting rename."""
    def copy(self, source, relative, accepted):
        self.mkdir(relative.rsplit('/', 1)[0])
        self.mkdir(relative)
        ssh = ['ssh', '-p', str(self.target.port or 22),
               '-i', self.credentials['private_key_file'],
               '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes',
               '-o', 'StrictHostKeyChecking=yes',
               '-o', 'UserKnownHostsFile=' + self.credentials['known_hosts_file'],
               '-o', 'ConnectTimeout=10']
        host = self.target.hostname
        if ':' in host:
            host = '[' + host + ']'
        remote = self.target.username + '@' + host + ':' + self.path(relative) + '/'
        subprocess.run(['rsync', '-r', '--delete', '--protect-args', '--exclude=/.ready', '--exclude=/' + LABEL, '--exclude=/.landing_zones/',
                        '-e', shlex.join(ssh), '--', str(source) + '/', remote],
                       check=True, capture_output=True, text=True, timeout=3600)


def adapter(step, settings):
    if step.adapter == 'sftp':
        return SFTPAdapter(step, settings)
    if step.adapter == 'rsync' and step.destination.startswith('ssh://'):
        return RemoteRsyncAdapter(step, settings)
    return LocalAdapter(step, settings)
