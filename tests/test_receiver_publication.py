"""Native receiver filesystem checks use only an owned temporary package."""
import errno
import fcntl
import grp
import hashlib
import os
from pathlib import Path
import stat
import uuid

import pytest

from landingzones.execution import receiver_publication as receiver


@pytest.fixture
def stage(tmp_path):
    queue = tmp_path.resolve() / 'receiver queue; $x'
    queue.mkdir()
    private = queue.with_name('.landingzones-staging-' + queue.name)
    private.mkdir(mode=0o700)
    package = private / str(uuid.uuid4())
    package.mkdir(mode=0o700)
    (package / 'dir').mkdir()
    (package / 'dir' / 'data').write_bytes(b'abc')
    (package / receiver.LABEL).write_bytes(b'receipt')
    request = dict(root=str(queue), group=grp.getgrgid(queue.stat().st_gid).gr_name,
                   token=package.name, verification='size', directory_mode=0o770,
                   manifest={'dir': {'kind': 'directory'}, 'dir/data': {'kind': 'file', 'size': 3}},
                   label_sha256=hashlib.sha256(b'receipt').hexdigest())
    return package, request


def test_receiver_checks_expected_group_and_fsyncs_every_entry_bottom_up(stage, monkeypatch):
    package, request = stage
    inode_paths = {p.stat().st_ino: p for p in [package, *package.rglob('*'), package.parent, Path(request['root'])]}
    flushed = []
    def record(fd):
        flushed.append(inode_paths[os.fstat(fd).st_ino])
    monkeypatch.setattr(receiver.os, 'fsync', record)
    receiver.execute('prepare', request)
    assert set(flushed) == set(inode_paths.values())
    assert flushed.index(package / 'dir' / 'data') < flushed.index(package / 'dir') < flushed.index(package)
    assert flushed[-2:] == [package.parent, Path(request['root'])]
    assert all(stat.S_IMODE(p.stat().st_mode) == (0o770 if p.is_dir() else 0o660)
               for p in [package, *package.rglob('*')])


@pytest.mark.parametrize('fault', ['size', 'missing', 'extra', 'label', 'symlink', 'fifo', 'hardlink',
                                   'stage-symlink', 'queue-symlink', 'group', 'open-parent', 'token', 'checksum'])
def test_receiver_refuses_unaccepted_objects_and_never_follows_links(stage, fault):
    package, request = stage
    data = package / 'dir' / 'data'
    outside = package.parent.parent / 'outside'
    outside.write_bytes(b'abc')
    if fault == 'size':
        data.write_bytes(b'x')
    elif fault == 'missing':
        data.unlink()
    elif fault == 'extra':
        (package / 'unexpected').touch()
    elif fault == 'label':
        (package / receiver.LABEL).write_bytes(b'changed')
    elif fault == 'symlink':
        data.unlink()
        data.symlink_to(outside)
    elif fault == 'fifo':
        data.unlink()
        os.mkfifo(data)
    elif fault == 'hardlink':
        data.unlink()
        os.link(outside, data)
    elif fault == 'stage-symlink':
        package.rename(package.with_name('moved'))
        package.symlink_to(package.with_name('moved'), target_is_directory=True)
    elif fault == 'queue-symlink':
        alias = package.parent.parent / 'alias'
        alias.symlink_to(request['root'], target_is_directory=True)
        request['root'] = str(alias)
    elif fault == 'group':
        request['group'] = 'unknown_test_group'
    elif fault == 'open-parent':
        package.parent.chmod(0o770)
    elif fault == 'token':
        request['token'] = '../../outside'
    elif fault == 'checksum':
        request['verification'] = 'checksum'
        request['manifest']['dir/data']['sha256'] = '0' * 64
    before = stat.S_IMODE(outside.stat().st_mode)
    with pytest.raises((ValueError, OSError)):
        receiver.execute('prepare', request)
    assert outside.read_bytes() == b'abc' and stat.S_IMODE(outside.stat().st_mode) == before


def test_ungrouped_move_flushes_without_changing_access_and_retries(stage, monkeypatch):
    package, request = stage
    request['group'] = ''
    data = package / 'dir' / 'data'
    data.chmod(0o640)
    with (package / receiver.LABEL).open('r+b') as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        with pytest.raises(BlockingIOError):
            receiver.execute('prepare', request)
    with monkeypatch.context() as patch:
        def interrupted(fd):
            raise OSError(errno.EIO, 'test flush failure')
        patch.setattr(receiver.os, 'fsync', interrupted)
        with pytest.raises(OSError, match='test flush failure'):
            receiver.execute('prepare', request)
    receiver.execute('prepare', request)
    assert stat.S_IMODE(data.stat().st_mode) == 0o640
    assert data.read_bytes() == b'abc'
