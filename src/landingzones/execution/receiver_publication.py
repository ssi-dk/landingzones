"""Standard-library helper sent to a Linux receiver over pinned SSH."""
from contextlib import contextmanager
import errno
import fcntl
import grp
import hashlib
import json
import os
import stat
import sys
import uuid


LABEL = '.landingzones-package.json'
# Intermediate directories are lookup anchors only. Shared receivers can grant
# search permission on a parent while keeping its directory listing private.
TRAVERSE = getattr(os, 'O_PATH', os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW
DIRECTORY = os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY
FILE = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK


@contextmanager
def opened(parent, name, flags):
    fd = os.open(name, flags, dir_fd=parent)
    try:
        yield fd
    finally:
        os.close(fd)


@contextmanager
def queue_parent(path):
    parts = path.split('/')
    if parts[0] or not parts[1:] or any(p in ('', '.', '..') for p in parts[1:]):
        raise ValueError('Expected an absolute receiver queue without traversal')
    fd = os.open('/', TRAVERSE)
    try:
        for part in parts[1:-1]:
            child = os.open(part, TRAVERSE, dir_fd=fd)
            os.close(fd)
            fd = child
        yield fd, parts[-1]
    finally:
        os.close(fd)


def file_digest(fd):
    digest = hashlib.sha256()
    for block in iter(lambda: os.read(fd, 1024 * 1024), b''):
        digest.update(block)
    return digest.hexdigest()


def same_entry(parent, name, fd):
    return os.path.samestat(os.fstat(fd), os.stat(name, dir_fd=parent, follow_symlinks=False))


def prepare(stage, manifest, label_sha256, verification, gid, directory_mode):
    device = os.fstat(stage).st_dev
    uid = os.geteuid()
    if LABEL in manifest:
        raise ValueError('Publication label cannot be a payload entry')

    def visit(fd, relative, apply, seen):
        info = os.fstat(fd)
        if info.st_dev != device or info.st_uid != uid:
            raise ValueError('Unexpected staged filesystem or owner')
        directory = stat.S_ISDIR(info.st_mode)
        if directory:
            for child in os.listdir(fd):
                key = relative + '/' + child if relative else child
                entry = {'kind': 'file'} if key == LABEL else manifest.get(key)
                if not entry:
                    raise ValueError('Unexpected entry in private stage')
                before = os.stat(child, dir_fd=fd, follow_symlinks=False)
                mode = before.st_mode
                if entry['kind'] == 'directory' and stat.S_ISDIR(mode):
                    flags = DIRECTORY
                elif entry['kind'] == 'file' and stat.S_ISREG(mode):
                    flags = FILE
                else:
                    raise ValueError('Staged entry type differs from accepted manifest')
                with opened(fd, child, flags) as child_fd:
                    if not os.path.samestat(before, os.fstat(child_fd)):
                        raise ValueError('Staged entry changed while opening')
                    visit(child_fd, key, apply, seen)
                    if not same_entry(fd, child, child_fd):
                        raise ValueError('Staged entry changed during publication')
                seen.add(key)
        else:
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError('Expected an exclusively staged regular file')
            if relative == LABEL:
                if file_digest(fd) != label_sha256:
                    raise ValueError('Staged package label differs from receipt')
            else:
                record = manifest[relative]
                if info.st_size != record['size']:
                    raise ValueError('Staged file size differs from accepted manifest')
                if verification == 'checksum' and apply and file_digest(fd) != record['sha256']:
                    raise ValueError('Staged file checksum differs from accepted manifest')
        if apply:
            if gid is not None:
                required = directory_mode if directory else 0o660
                if info.st_gid != gid:
                    os.fchown(fd, -1, gid)
                if stat.S_IMODE(os.fstat(fd).st_mode) != required:
                    os.fchmod(fd, required)
                final = os.fstat(fd)
                if final.st_gid != gid or stat.S_IMODE(final.st_mode) != required:
                    raise PermissionError('Receiver did not apply publication group and mode')
            os.fsync(fd)  # Files and children are durable before the enclosing directory.

    expected = set(manifest) | {LABEL}
    for apply in (False, True):
        seen = set()
        visit(stage, '', apply, seen)
        if seen != expected:
            raise ValueError('Private stage is missing accepted entries')


def execute(operation, request):
    group = request['group']
    with queue_parent(request['root']) as (parent, name), opened(parent, name, DIRECTORY) as queue:
        gid = None
        if group:
            gid = os.fstat(queue).st_gid
            try:
                received_group = grp.getgrgid(gid).gr_name
            except KeyError:
                raise ValueError('Unknown receiver queue group') from None
            if received_group != group:
                raise ValueError('Receiver queue group differs from configured group')
        if operation == 'probe':
            os.fsync(queue)
            return
        if operation != 'prepare':
            raise ValueError('Unknown receiver operation')
        token = request['token']
        if str(uuid.UUID(token)) != token or request['verification'] not in ('size', 'checksum'):
            raise ValueError('Invalid stage identity or verification policy')
        mode = request['directory_mode']
        if mode not in (0o770, 0o2770):
            raise ValueError('Unsupported publication directory mode')
        with opened(parent, '.landingzones-staging-' + name, DIRECTORY) as private:
            st = os.fstat(private)
            if st.st_uid != os.geteuid() or st.st_mode & 0o777 != 0o700:
                raise ValueError('Receiver staging parent requires private ownership and mode 0700')
            if st.st_dev != os.fstat(queue).st_dev:
                raise ValueError('Receiver stage and queue are on different filesystems')
            with opened(private, token, DIRECTORY) as stage:
                if os.fstat(stage).st_dev != st.st_dev:
                    raise ValueError('Receiver stage crossed a filesystem boundary')
                # Hold a writable regular-file lock for NFS clients. If a caller
                # loses SSH, another helper cannot race this one into publication.
                before = os.stat(LABEL, dir_fd=stage, follow_symlinks=False)
                if not stat.S_ISREG(before.st_mode):
                    raise ValueError('Expected a regular receipt label')
                with opened(stage, LABEL, os.O_RDWR | os.O_NONBLOCK | os.O_NOFOLLOW) as label:
                    info = os.fstat(label)
                    if not os.path.samestat(info, before) or info.st_nlink != 1 or info.st_uid != os.geteuid():
                        raise ValueError('Expected an owned private receipt label')
                    fcntl.flock(label, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    if not same_entry(private, token, stage) or not same_entry(stage, LABEL, label):
                        raise ValueError('Staged package identity changed')
                    prepare(stage, request['manifest'], request['label_sha256'], request['verification'], gid, mode)
                    if not same_entry(private, token, stage) or not same_entry(stage, LABEL, label):
                        raise ValueError('Staged package identity changed')
                    os.fsync(private)
                    os.fsync(queue)


if __name__ == '__main__':
    action, = sys.argv[1:]
    response = {'protocol': 1, 'operation': action}
    try:
        execute(action, json.load(sys.stdin))
    except (KeyError, ValueError) as exc:
        response.update(status='refused', error=str(exc))
    except OSError as exc:
        response.update(status='failed', error=exc.strerror or str(exc), errno=exc.errno or errno.EIO)
    else:
        response.update(status='passed')
    print(json.dumps(response))
