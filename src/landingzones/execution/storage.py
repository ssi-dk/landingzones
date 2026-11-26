"""Local durable snapshots and runtime execution locks."""
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
from urllib.parse import urlsplit


def atomic_json(path, value, *, create=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(dir=str(path.parent), prefix='.state-')
    try:
        with os.fdopen(descriptor, 'w') as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        if create:
            os.link(temporary, path)
        else:
            os.replace(temporary, path)
        directory = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class TransferBusy(BlockingIOError):
    """Another worker or an exclusive maintenance action owns a required lock."""


class ExecutionLocks:
    def __init__(self, maintenance):
        self.handles = [maintenance]

    def fileno(self):
        return self.handles[0].fileno()

    @property
    def pass_fds(self):
        return tuple(handle.fileno() for handle in self.handles)


def _queue_locks(endpoints):
    # Readers on ancestors allow siblings to run together. An exclusive lock
    # on a queue excludes both the same queue and any containing/nested queue.
    modes = {}
    for endpoint in endpoints:
        if '://' in endpoint:
            parsed = urlsplit(endpoint)
            if parsed.scheme in ('ssh', 'sftp'):
                namespace = ('ssh', parsed.hostname, parsed.port or 22)
            else:
                namespace = (parsed.scheme, parsed.netloc)
            path = Path('/' + parsed.path.lstrip('/'))
        else:
            namespace = ('local',)
            path = Path(endpoint).resolve()
        for parent in path.parents:
            modes.setdefault((*namespace, str(parent)), fcntl.LOCK_SH)
        modes[(*namespace, str(path))] = fcntl.LOCK_EX
    return modes


def _try_lock(path, mode):
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    handle = os.fdopen(fd, 'r+b')
    try:
        fcntl.flock(fd, mode | fcntl.LOCK_NB)
    except BaseException:
        handle.close()
        raise
    return handle


class StateStore:
    def __init__(self, root, max_concurrent_transfers=1):
        self.root = Path(root)
        self.limit = max_concurrent_transfers
        self._locks = ContextVar('execution_locks', default=None)

    @property
    def execution_lock(self):
        return self._locks.get()

    @contextmanager
    def locked(self, connection=None):
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        parallel = self.limit > 1 and connection is not None
        with ExitStack() as stack:
            def take(path, mode):
                try:
                    return stack.enter_context(_try_lock(path, mode))
                except BlockingIOError as exc:
                    raise TransferBusy('Transfer or maintenance lock is busy') from exc

            maintenance = take(self.root / 'execution.lock', fcntl.LOCK_SH if parallel else fcntl.LOCK_EX)
            locks = ExecutionLocks(maintenance)
            if parallel:
                directory = self.root / 'transfer-locks'
                directory.mkdir(exist_ok=True, mode=0o700)
                if directory.is_symlink():
                    raise ValueError('Transfer locks must reside in the runtime state directory')
                route = hashlib.sha256(connection.encode()).hexdigest()
                locks.handles.append(take(directory / ('route-' + route + '.lock'), fcntl.LOCK_EX))
                for slot in range(1, self.limit + 1):
                    try:
                        handle = _try_lock(directory / f'slot-{slot}.lock', fcntl.LOCK_EX)
                    except BlockingIOError:
                        continue
                    locks.handles.append(stack.enter_context(handle))
                    break
                else:
                    raise TransferBusy('All configured transfer slots are busy')
            token = self._locks.set(locks)
            try:
                yield self
            finally:
                self._locks.reset(token)

    @contextmanager
    def queues(self, endpoints):
        if self.limit == 1:
            yield
            return
        locks = self.execution_lock
        previous = len(locks.handles)
        with ExitStack() as stack:
            try:
                for key, mode in sorted(_queue_locks(endpoints).items()):
                    digest = hashlib.sha256(json.dumps(key).encode()).hexdigest()
                    path = self.root / 'transfer-locks' / ('queue-' + digest + '.lock')
                    try:
                        handle = _try_lock(path, mode)
                    except BlockingIOError as exc:
                        raise TransferBusy('A transfer already owns an overlapping queue') from exc
                    locks.handles.append(stack.enter_context(handle))
                yield
            finally:
                del locks.handles[previous:]

    def path(self, request_id):
        # IDs come from hashes/UUIDs; never allow an arbitrary filesystem path.
        if not request_id or any(c not in '0123456789abcdef-' for c in request_id):
            raise ValueError('Invalid request ID')
        return self.root / (request_id + '.json')

    def read(self, request_id):
        with self.path(request_id).open() as handle:
            return json.load(handle)

    def save(self, state):
        atomic_json(self.path(state['request_id']), state)

    def create(self, state):
        # Concurrent submissions can use the same idempotency key on different
        # routes. Never replace another route's newly accepted receipt.
        atomic_json(self.path(state['request_id']), state, create=True)
