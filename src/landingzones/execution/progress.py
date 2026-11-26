"""Sample logical payload progress without enumerating or reopening staged data."""
from contextlib import contextmanager
import os
import re
from threading import Event, Thread
import time
import warnings


_PROGRESS = re.compile(r'^\s*([\d,]+)\s+\d+%\s+')


class Progress:
    interval = 10.0

    @staticmethod
    def totals(state):
        return dict(bytes_done=None, bytes_total=state['total_bytes'],
                    files_total=sum(item['kind'] == 'file' for item in state['manifest'].values()),
                    rate_bytes_per_second=None, eta_seconds=None)

    def __init__(self, state, emit):
        self.sample = self.totals(state)
        self.emit = emit
        self.latest = 0
        self.previous = 0
        self.since = None

    def observe(self, done):
        if not self.latest <= done <= self.sample['bytes_total']:
            return
        self.latest = done
        now = time.monotonic()
        delta = now - self.since if self.since is not None else None
        if delta is not None and delta < self.interval:
            return
        rate = (done - self.previous) / delta if delta else None
        eta = (self.sample['bytes_total'] - done) / rate if rate else None
        self.emit(dict(self.sample, bytes_done=done, rate_bytes_per_second=rate, eta_seconds=eta))
        self.previous, self.since = done, now

    def advance(self, size):
        self.observe(self.latest + size)

    def verified(self):
        self.emit(dict(self.sample, bytes_done=self.sample['bytes_total']))


@contextmanager
def rsync_progress(observe):
    # rsync uses carriage returns for live counters. Read stdout continuously so
    # a long transfer never buffers an entire package's progress in memory.
    read_fd, write_fd = os.pipe()
    def read():
        with os.fdopen(read_fd, encoding='ascii', errors='replace') as lines:
            for line in lines:
                match = _PROGRESS.match(line)
                if match:
                    try:
                        observe(int(match[1].replace(',', '')))
                    except Exception as exc:
                        # Keep draining stdout after a monitoring failure.
                        warnings.warn('Unable to record rsync progress: ' + str(exc))
    reader = Thread(target=read, name='rsync-progress', daemon=True)
    with os.fdopen(write_fd, 'wb') as output:
        reader.start()
        try:
            yield output
        finally:
            output.close()
            reader.join()


@contextmanager
def phase_heartbeat(emit, interval=30):
    # Long receiver flushes and source cleanup have no byte counter. Liveness
    # is a separate observation and must never imply bytes were transferred.
    finished = Event()
    def beat():
        while not finished.wait(interval):
            try:
                emit()
            except Exception as exc:
                warnings.warn('Unable to record phase heartbeat: ' + str(exc))
    thread = Thread(target=beat, name='transfer-heartbeat', daemon=True)
    thread.start()
    try:
        yield
    finally:
        finished.set()
        thread.join()
