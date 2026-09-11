"""Local durable snapshots and a single runtime execution lock."""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import tempfile


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(dir=str(path.parent), prefix='.state-')
    try:
        with os.fdopen(descriptor, 'w') as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class StateStore:
    def __init__(self, root):
        self.root = Path(root)

    @contextmanager
    def locked(self):
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with (self.root / 'execution.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield self

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
