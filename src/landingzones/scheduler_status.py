"""Read an optional atomic route-discovery snapshot from the configured local runtime."""
from contextlib import ExitStack
from datetime import datetime, timedelta
import json
import os
import stat

from landingzones.monitoring import FRESHNESS_SECONDS


ROUTE_STATES = {
    'scheduled': 'Scheduled', 'due': 'Queued for discovery', 'running': 'Scanning',
    'deferred': 'Deferred', 'failed': 'Last scan failed', 'held': 'Held',
}
WAIT_REASONS = {
    'slots': 'Waiting for a slot', 'overlapping_queue': 'Waiting for a shared queue',
    'external_contention': 'Deferred by another lock holder',
}
MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024


def _read_snapshot(state_dir):
    # Every name below state_dir is fixed. Reject symlinks and non-regular
    # files so an optional scheduler report cannot open an unrelated file or pipe.
    directory = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    with ExitStack() as stack:
        def opened(path, flags, parent=None):
            fd = os.open(path, flags, dir_fd=parent)
            stack.callback(os.close, fd)
            return fd

        root = opened(state_dir, directory)
        folder = opened('scheduler', directory, root)
        fd = opened('status.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, folder)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError('Expected a regular snapshot')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            raw = stream.read(MAX_SNAPSHOT_BYTES + 1)
        if len(raw) > MAX_SNAPSHOT_BYTES:
            raise ValueError('Oversized snapshot')
        return json.loads(raw)


def _utc(value, *, nullable=False):
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise ValueError('Expected a UTC timestamp')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.utcoffset() != timedelta(0):
        raise ValueError('Expected a UTC timestamp')
    return stamp


def _positive(value, *, nullable=False):
    if nullable and value is None:
        return None
    if type(value) is not int or value < 1:
        raise ValueError('Expected a positive integer')
    return value


def _snapshot(report, runtimes, now):
    if not isinstance(report, dict) or type(report.get('schema_version')) is not int or report['schema_version'] != 1:
        raise ValueError('Unsupported scheduler status schema')
    runtime = report['runtime_id']
    if not isinstance(runtime, str) or not runtime:
        raise ValueError('Expected a runtime identifier')
    if runtimes and runtime not in runtimes:
        return None
    stamp = _utc(report['observed_at_utc'])
    enabled = report['enabled']
    if type(enabled) is not bool or not isinstance(report['routes'], list):
        raise ValueError('Invalid scheduler snapshot')
    dispatcher = _positive(report['dispatcher_pid'], nullable=True)
    seen = set()
    routes = []
    for record in report['routes']:
        if not isinstance(record, dict):
            raise ValueError('Invalid route record')
        name, state = record['transfer_identifier'], record['state']
        if not isinstance(name, str) or not name or name in seen or state not in ROUTE_STATES:
            raise ValueError('Invalid route identity or state')
        seen.add(name)
        row = dict(transfer_identifier=name, state=state, worker_pid=_positive(record['worker_pid'], nullable=True))
        for field in ('last_scanned_at_utc', 'next_due_at_utc', 'started_at_utc'):
            _utc(record[field], nullable=True)
            row[field] = record[field]
        reason = record.get('wait_reason')
        row['wait_reason'] = reason if reason in WAIT_REASONS else None
        row['state_label'] = WAIT_REASONS.get(row['wait_reason'], ROUTE_STATES[state])
        routes.append(row)

    active = sum(row['state'] == 'running' for row in routes)
    if not enabled and not dispatcher and not active:
        observation = 'held'
    elif abs((now - stamp).total_seconds()) >= FRESHNESS_SECONDS:
        observation = 'stale'
    else:
        observation = 'recent'
    return dict(schema_version=1, runtime_id=runtime, observed_at_utc=report['observed_at_utc'],
                observation=observation, enabled=enabled, dispatcher_pid=dispatcher,
                max_concurrent_transfers=_positive(report['max_concurrent_transfers']),
                discovery_interval_seconds=_positive(report['discovery_interval_seconds']),
                running_routes=active, routes=routes)


def read_scheduler_status(state_dir, runtime_ids, now):
    if state_dir is None:
        return None
    try:
        return _snapshot(_read_snapshot(state_dir), runtime_ids, now)
    except FileNotFoundError:
        return None
    except (OSError, KeyError, TypeError, ValueError, OverflowError, RecursionError):
        # A missing or unreadable optional feed must not prevent package monitoring.
        return {'observation': 'unavailable', 'routes': []}
