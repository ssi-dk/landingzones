"""Transport operations; lifecycle decisions belong to the executor."""
from contextlib import contextmanager
import errno
import ctypes
import grp
import sys
import hashlib
import json
import os
import posixpath
from pathlib import Path
import shutil
import shlex
import stat
import struct
import subprocess
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

from .progress import rsync_progress
from .model import local_root
from .package import LABEL, is_metadata, size_inventory


def digest(handle):
    result = hashlib.sha256()
    for chunk in iter(lambda: handle.read(1024 * 1024), b''):
        result.update(chunk)
    return result.hexdigest()


def manifest(root, verification='checksum'):
    root = Path(local_root(str(root)))
    if not root.is_dir():
        raise ValueError('Payload must be a directory: ' + str(root))
    result = {}
    for path in sorted(root.rglob('*')):
        attributes = path.lstat()
        mode = attributes.st_mode
        relative = path.relative_to(root).as_posix()
        if stat.S_ISLNK(mode) or not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
            raise ValueError('Symlinks and special payload files are unsupported')
        if is_metadata(relative):
            if relative in ('.ready', LABEL) and not stat.S_ISREG(mode):
                raise ValueError('Package marker must be a regular file')
            continue
        if any(c in relative for c in '\0\n\r'):
            raise ValueError('Control characters in payload names are unsupported')
        if stat.S_ISDIR(mode):
            result[relative] = {'kind': 'directory'}
        else:
            entry = {'kind': 'file', 'size': attributes.st_size}
            if verification == 'checksum':
                with path.open('rb') as handle:
                    entry['sha256'] = digest(handle)
            result[relative] = entry
    return result


def matches(actual, expected, verification='checksum'):
    if verification == 'size':
        return size_inventory(actual) == size_inventory(expected)
    return actual == expected


def cleanup(root, accepted, verification='checksum'):
    """Resume partial cleanup without removing unaccepted or changed content."""
    root = Path(local_root(str(root)))
    if not root.exists():
        return
    remaining = manifest(root, verification)
    if any(key not in accepted or value != accepted[key] for key, value in remaining.items()):
        raise ValueError('Source changed; cleanup refused')
    for relative in sorted(remaining, key=lambda item: (item.count('/'), item), reverse=True):
        path = root / relative
        if remaining[relative]['kind'] == 'directory':
            path.rmdir()
            continue
        if verification == 'checksum':
            # Producer must relinquish writes after readiness; recheck before removal.
            with path.open('rb') as handle:
                if digest(handle) != accepted[relative]['sha256']:
                    raise ValueError('Source changed during cleanup')
        else:
            attributes = path.lstat()
            if not stat.S_ISREG(attributes.st_mode) or attributes.st_size != accepted[relative]['size']:
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


def _rename_noreplace(source, destination):
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == 'darwin' and hasattr(libc, 'renamex_np'):
        result = libc.renamex_np(os.fsencode(source), os.fsencode(destination), 4)  # RENAME_EXCL
    elif sys.platform.startswith('linux') and hasattr(libc, 'renameat2'):
        result = libc.renameat2(-100, os.fsencode(source), -100, os.fsencode(destination), 1)  # RENAME_NOREPLACE
    else:
        raise OSError(errno.ENOSYS, os.strerror(errno.ENOSYS), str(destination))
    if result:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), str(destination))


class LocalAdapter:
    def __init__(self, step, settings):
        self.step = step
        self.root = Path(local_root(step.destination))
        if not self.root.is_dir():
            raise ValueError('Destination root must already exist')
        if self.root == self.root.parent:
            raise ValueError('Destination must have a private sibling staging location')
        self.exclusive_destination = str(self.root) in settings.get('exclusive_local_destinations', [])
        self.publication_gid = None
        publication_group = getattr(step, 'publication_group', '')
        if publication_group:
            try:
                self.publication_gid = grp.getgrnam(publication_group).gr_gid
            except KeyError as exc:
                raise ValueError('Unknown publication group: ' + publication_group) from exc

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
        return manifest(local_root(os.path.normpath(str(self.root / relative))), self.step.verification)

    def prepare_publication(self, relative, accepted):
        if self.publication_gid is None:
            return
        stage = Path(local_root(os.path.normpath(str(self.root / relative))))
        entries = {**accepted, LABEL: {'kind': 'file'}, '': {'kind': 'directory'}}
        for name, item in entries.items():
            path = stage / name
            flags = os.O_RDONLY | os.O_NOFOLLOW
            directory = item['kind'] == 'directory'
            if directory:
                flags |= os.O_DIRECTORY
            fd = os.open(path, flags)
            try:
                before = os.fstat(fd)
                if not (stat.S_ISDIR(before.st_mode) if directory else stat.S_ISREG(before.st_mode)):
                    raise ValueError('Unexpected publication entry type')
                mode = self.step.publication_mode_for(directory)
                group_changed = before.st_gid != self.publication_gid
                if group_changed:
                    os.fchown(fd, -1, self.publication_gid)
                mode_changed = stat.S_IMODE(os.fstat(fd).st_mode) != mode
                if mode_changed:
                    os.fchmod(fd, mode)
                after = os.fstat(fd)
                if after.st_gid != self.publication_gid or stat.S_IMODE(after.st_mode) != mode:
                    raise PermissionError('Publication group or permissions could not be set')
                if group_changed or mode_changed:
                    os.fsync(fd)
            finally:
                os.close(fd)

    @contextmanager
    def staging_directory(self, relative):
        path = Path(local_root(os.path.normpath(str(self.root / relative))))
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.mkdir(exist_ok=True, mode=0o700)
        if path.stat().st_dev != self.root.stat().st_dev:
            raise ValueError('Staging and destination must be on the same filesystem')
        if path.parent.stat().st_uid != os.geteuid() or path.parent.stat().st_mode & 0o077:
            raise ValueError('Staging root must be private to the executor (mode 0700)')
        yield path
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

    def copy(self, source, relative, accepted):
        with self.staging_directory(relative) as path:
            if self.step.adapter == 'rsync':
                subprocess.run(['rsync', '-r', '--delete', '--exclude=/.ready', '--exclude=/' + LABEL, '--exclude=/.landing_zones/', '--',
                                str(source) + '/', str(path) + '/'], check=True, timeout=3600,
                               pass_fds=self.execution_lock.pass_fds if getattr(self, 'execution_lock', None) else ())
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
                        if getattr(self, 'progress', None):
                            self.progress.advance(item['size'])

    def _rename(self, source, destination):
        try:
            _rename_noreplace(source, destination)
            return
        except OSError as exc:
            if exc.errno not in (errno.EINVAL, errno.ENOSYS, errno.ENOTSUP, errno.EOPNOTSUPP):
                raise
            if not self.exclusive_destination:
                raise OSError(exc.errno, 'Atomic no-overwrite rename unsupported for local destination',
                              str(self.root)) from exc
        # The executor holds its state lock. Every publisher to this root must share it.
        try:
            destination.lstat()
        except FileNotFoundError:
            os.rename(source, destination)
        else:
            raise FileExistsError(errno.EEXIST, 'Destination already exists', str(destination))

    def check_move_publication(self, relative, final):
        if self.exists(final):
            raise FileExistsError('Destination already exists')
        stage_parent = Path(local_root(os.path.normpath(str(self.root / relative)))).parent
        if stage_parent.stat().st_dev != self.root.stat().st_dev:
            raise ValueError('Staging and destination must be on the same filesystem')
        # Keep the capability probe private and separate from the staged package.
        with TemporaryDirectory(prefix='.rename-probe-', dir=stage_parent) as directory:
            probe = Path(directory)
            source = probe / 'source'
            source.mkdir()
            self._rename(source, probe / 'destination')

    def promote(self, relative, final):
        source = Path(local_root(os.path.normpath(str(self.root / relative))))
        destination = Path(local_root(str(self.root / final)))
        self._rename(source, destination)
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
        self.sftp = None

    def __enter__(self):
        try:
            import paramiko
        except ImportError as exc:
            raise ValueError('SFTP transport requires the optional landingzones[sftp] dependencies') from exc
        self.client = paramiko.SSHClient()
        self.sftp = None
        try:
            self.client.load_host_keys(self.credentials['known_hosts_file'])
            self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
            self.client.connect(self.target.hostname, port=self.target.port or 22,
                                username=self.target.username,
                                key_filename=self.credentials['private_key_file'],
                                allow_agent=False, look_for_keys=False,
                                timeout=10, auth_timeout=10, banner_timeout=10)
            self.sftp = self.client.open_sftp()
            self.sftp.get_channel().settimeout(30)
            if not stat.S_ISDIR(self.check_path(self.root).st_mode):
                raise ValueError('Remote root must be a directory')
            return self
        except BaseException as exc:
            try:
                self.__exit__()
            except Exception:
                pass  # Preserve the setup failure after attempting both closes.
            if isinstance(exc, paramiko.SSHException):
                raise OSError('SFTP connection or host-key validation failed: ' + type(exc).__name__) from exc
            raise

    def __exit__(self, *args):
        if self.client:
            try:
                if self.sftp is not None:
                    self.sftp.close()
            finally:
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
                    if key in ('.ready', LABEL) and not stat.S_ISREG(entry.st_mode):
                        raise ValueError('Package marker must be a regular file')
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
        if self.root == '/':
            raise ValueError('Destination must have a private sibling staging location')
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

    def prepare_stage(self, relative):
        staging_root = relative.rsplit('/', 1)[0]
        self.mkdir(staging_root)
        if self.check_path(self.path(staging_root)).st_mode & 0o077:
            raise ValueError('Staging root must be private to the executor (mode 0700)')
        self.mkdir(relative)

    def copy(self, source, relative, accepted):
        self.prepare_stage(relative)
        for key, item in accepted.items():
            target = relative + '/' + key
            if item['kind'] == 'directory':
                self.mkdir(target)
            else:
                if self.exists(target):
                    if not stat.S_ISREG(self.check_path(self.path(target)).st_mode):
                        raise ValueError('Unexpected staging file type')
                self.sftp.put(str(Path(source) / key), self.path(target), confirm=True)

    def receiving_gid(self):
        """Resolve the receiver directory group in the SFTP server's namespace."""
        from paramiko.message import Message
        from paramiko.sftp import CMD_EXTENDED, CMD_EXTENDED_REPLY
        gid = self.check_path(self.root).st_gid
        kind, reply = self.sftp._request(CMD_EXTENDED, 'users-groups-by-id@openssh.com',
                                         b'', struct.pack('>I', gid))
        if kind != CMD_EXTENDED_REPLY or reply.get_string() != b'':
            raise ValueError('SFTP server could not identify the receiving directory group')
        if Message(reply.get_string()).get_text() != self.step.publication_group:
            raise ValueError('Receiving directory group differs from the configured publication group')
        return gid

    @staticmethod
    def sync_handle(sftp, handle):
        from paramiko.sftp import CMD_EXTENDED, CMD_STATUS
        kind, _ = sftp._request(CMD_EXTENDED, 'fsync@openssh.com', handle.handle)
        if kind != CMD_STATUS:
            raise OSError('SFTP did not acknowledge fsync')

    def sync_directory(self, path):
        if not stat.S_ISDIR(self.check_path(path).st_mode):
            raise ValueError('Expected a directory for durable SFTP publication')
        with self.sftp.open(path, 'rb') as handle:
            self.sync_handle(self.sftp, handle)

    def prepare_publication(self, stage, accepted):
        """Apply group access and flush a private package before exposing it."""
        gid = self.receiving_gid() if self.step.publication_group else None
        root = self.path(stage)
        entries = {**accepted, LABEL: {'kind': 'file'}, '': {'kind': 'directory'}}
        for key in sorted(entries, key=lambda key: (key.count('/') + 1 if key else 0, key), reverse=True):
            path = root + '/' + key if key else root
            attrs = self.check_path(path)
            directory = entries[key]['kind'] == 'directory'
            if not (stat.S_ISDIR(attrs.st_mode) if directory else stat.S_ISREG(attrs.st_mode)):
                raise ValueError('Unexpected staged SFTP entry type')
            with self.sftp.open(path, 'rb') as handle:
                if gid is not None:
                    mode = self.step.publication_mode_for(directory)
                    if attrs.st_gid != gid:
                        handle.chown(attrs.st_uid, gid)
                    handle.chmod(mode)
                    result = handle.stat()
                    if result.st_gid != gid or stat.S_IMODE(result.st_mode) != mode:
                        raise ValueError('SFTP publication permissions differ from the requested group or mode')
                self.sync_handle(self.sftp, handle)
        self.sync_directory(root.rsplit('/', 1)[0])
        # Establish support on the destination filesystem before attempting rename.
        self.sync_directory(self.root)

    def confirm_publication(self, stage):
        self.sync_directory(self.root)
        self.sync_directory(self.path(stage).rsplit('/', 1)[0])

    def promote(self, relative, final):
        if self.exists(final):
            raise FileExistsError('Destination already exists')
        # Standard SFTP rename refuses an existing target (not posix_rename).
        self.sftp.rename(self.path(relative), self.path(final))


class RemoteRsyncAdapter(SFTPAdapter):
    """Rsync over SSH for bytes; SFTP for inspection and non-overwriting rename."""
    def ssh_command(self):
        return ['ssh', '-p', str(self.target.port or 22),
               '-i', self.credentials['private_key_file'],
               '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes',
               '-o', 'StrictHostKeyChecking=yes',
               '-o', 'UserKnownHostsFile=' + self.credentials['known_hosts_file'],
               '-o', 'ConnectTimeout=10']

    def remote_directory(self, relative, *, literal_source=False):
        host = self.target.hostname
        if host.startswith('-') or self.target.username.startswith('-'):
            raise ValueError('Invalid rsync SSH endpoint')
        if ':' in host:
            host = '[' + host + ']'
        path = posixpath.normpath(self.path(relative)).rstrip('/')
        if literal_source:
            # With protected arguments rsync expands source globs on the sender.
            # Escape its glob grammar independently of shell argument protection.
            path = ''.join('\\' + c if c in '\\[*?' else c for c in path)
        return self.target.username + '@' + host + ':' + path + '/'

    def pinned_ssh_command(self):
        return self.ssh_command() + ['-F', '/dev/null', '-o', 'IdentityAgent=none',
                                     '-o', 'GlobalKnownHostsFile=/dev/null', '-o', 'UpdateHostKeys=no']

    def receiver_publication(self, action, **evidence):
        if action not in ('probe', 'prepare'):
            raise ValueError('Unknown receiver publication action')
        # Only shipped code and this fixed verb go through the remote shell.
        # Names, receipt data, and all paths travel as JSON on stdin.
        helper = Path(__file__).with_name('receiver_publication.py').read_text()
        command = 'python3 -I -S -c ' + shlex.quote(helper) + ' ' + action
        ssh = self.pinned_ssh_command() + ['-o', 'ServerAliveInterval=30', '-o', 'ServerAliveCountMax=3',
                                        '--', self.target.username + '@' + self.target.hostname, command]
        root = '/' + posixpath.normpath(self.root).lstrip('/')
        request = dict(root=root, group=self.step.publication_group, **evidence)
        lock = getattr(self, 'execution_lock', None)
        try:
            result = subprocess.run(ssh, input=json.dumps(request), text=True, capture_output=True,
                                    check=True, timeout=30 if action == 'probe' else None,
                                    pass_fds=lock.pass_fds if lock is not None else ())
        except subprocess.CalledProcessError as exc:
            raise OSError(f'Receiver Python publication helper exited with status {exc.returncode}') from exc
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError('Receiver publication capability probe timed out') from exc
        try:
            reply = json.loads(result.stdout)
            if reply['protocol'] != 1 or reply['operation'] != action:
                raise ValueError
            status = reply['status']
        except (KeyError, TypeError, ValueError) as exc:
            raise OSError('Receiver did not acknowledge the publication helper protocol') from exc
        if status == 'refused':
            raise ValueError('Receiver refused publication: ' + reply['error'])
        if status != 'passed':
            raise OSError('Receiver publication failed: ' + reply.get('error', 'unexpected status'))

    def prepare_verified_publication(self, stage, accepted, label):
        token = stage.rsplit('/', 1)[-1]
        if self.stage_name(token) != stage:
            raise ValueError('Expected the private stage for this connection')
        label_hash = hashlib.sha256(json.dumps(label, sort_keys=True).encode('utf-8')).hexdigest()
        self.receiver_publication('prepare', token=token, manifest=accepted, label_sha256=label_hash,
                                  verification=self.step.verification,
                                  directory_mode=self.step.publication_mode_for(True))

    def pull_directory(self, payload, path, accepted, execution_lock):
        # Interrupted receiver temporary files must not become inventory entries.
        scratch = Path(local_root(str(path.with_name('.rsync-' + path.name))))
        scratch.mkdir(mode=0o700, exist_ok=True)
        self.copy_manifest(self.remote_directory(payload, literal_source=True), str(path) + '/',
                           str(scratch), accepted, execution_lock)
        shutil.rmtree(scratch)

    def push_directory(self, source, relative, accepted, execution_lock):
        self.prepare_stage(relative)
        parent, token = relative.rsplit('/', 1)
        scratch = parent + '/.rsync-' + token
        self.mkdir(scratch)
        scratch_path = posixpath.normpath(self.path(scratch))
        self.copy_manifest(str(source) + '/', self.remote_directory(relative),
                           scratch_path, accepted, execution_lock)
        # A previous interrupted receiver can leave its temporary file here.
        for entry in self.sftp.listdir_attr(scratch_path):
            if not stat.S_ISREG(entry.st_mode) or '/' in entry.filename:
                raise ValueError('Unexpected entry in private rsync scratch directory')
            self.sftp.remove(scratch_path + '/' + entry.filename)
        self.sftp.rmdir(scratch_path)

    def copy_manifest(self, source, destination, scratch, accepted, execution_lock):
        # --files-from implies --dirs and disables recursive descent. Only admitted
        # entries are requested, including empty directories and ordinary dotfiles.
        entries = b''.join(os.fsencode(key) + b'\0' for key in sorted(accepted))
        argv = ['rsync', '--dirs', '--times', '--chmod=Du+rwx,Fu+rw', '--from0', '--files-from=-',
                '--temp-dir=' + scratch, '--protect-args', '--timeout=300',
                '-e', shlex.join(self.pinned_ssh_command()), '--', source, destination]
        # Retain the worker's lock while a surviving rsync can write private staging.
        reporter = getattr(self, 'progress', None)
        if reporter is None:
            subprocess.run(argv, input=entries, check=True, capture_output=True,
                           pass_fds=execution_lock.pass_fds)
            return
        argv[1:1] = ['--info=progress2,name0', '--outbuf=L', '--no-human-readable']
        with rsync_progress(reporter.observe) as output:
            subprocess.run(argv, input=entries, check=True, stdout=output, stderr=subprocess.PIPE,
                           env=dict(os.environ, LC_ALL='C'), pass_fds=execution_lock.pass_fds)

    def copy(self, source, relative, accepted):
        self.prepare_stage(relative)
        remote = self.remote_directory(relative)
        subprocess.run(['rsync', '-r', '--delete', '--protect-args', '--exclude=/.ready', '--exclude=/' + LABEL, '--exclude=/.landing_zones/',
                        '-e', shlex.join(self.ssh_command()), '--', str(source) + '/', remote],
                       check=True, capture_output=True, text=True, timeout=3600,
                       pass_fds=self.execution_lock.pass_fds if getattr(self, 'execution_lock', None) else ())


def adapter(step, settings):
    if step.adapter == 'ena':
        from .ena import ENAAdapter
        return ENAAdapter(step, settings)
    if step.adapter == 'sftp':
        return SFTPAdapter(step, settings)
    if step.rsync_destination_push or (step.adapter == 'rsync' and step.destination.startswith('ssh://')):
        return RemoteRsyncAdapter(step, settings)
    return LocalAdapter(step, settings)
