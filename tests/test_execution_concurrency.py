"""Parallel transfers, maintenance exclusion, and lock lifetime across workers."""
from contextlib import ExitStack
import csv
import json
from pathlib import Path
import pwd
import os
import socket
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Barrier

import pytest
import yaml

from landingzones.cli import main
from landingzones.execution.adapters import LocalAdapter
from landingzones.execution.engine import Executor
from landingzones.execution.model import load_settings
from landingzones.execution.storage import StateStore, TransferBusy


def test_configured_slots_exclude_sixth_and_reuse_released_slot(tmp_path):
    store = StateStore(tmp_path, 5)
    with ExitStack() as stack:
        for i in range(5):
            stack.enter_context(store.locked(f'route-{i}'))
        with pytest.raises(TransferBusy, match='slots'):
            with store.locked('sixth'):
                pytest.fail('oversubscribed')
        assert len(list((tmp_path/'transfer-locks').glob('slot-*.lock'))) == 5
    with store.locked('sixth'):
        with pytest.raises(TransferBusy):
            with StateStore(tmp_path).locked():
                pytest.fail('maintenance overlapped an admitted worker')
    with StateStore(tmp_path).locked():
        with pytest.raises(TransferBusy):
            with store.locked('sixth'):
                pytest.fail('worker started during maintenance')


@pytest.mark.parametrize('a,b', [
    ('/queue/one', '/queue'),
    ('/queue', '/queue/one'),
    ('ssh://one@Example.invalid:22/queue', 'sftp://two@example.invalid/queue'),
])
def test_queue_aliases_and_nested_paths_share_guards(tmp_path, a, b):
    first = StateStore(tmp_path, 5)
    second = StateStore(tmp_path, 5)
    with first.locked('a'), first.queues([a]), second.locked('b'):
        with pytest.raises(TransferBusy):
            with second.queues([b]):
                pytest.fail('overlap admitted')
        with second.queues(['/different/tree']):
            pass


def test_lingering_rsync_keeps_slot_queue_and_maintenance_guards(tmp_path):
    store = StateStore(tmp_path, 2)
    with store.locked('first'), store.queues(['/receiver']):
        cmd = [sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)']
        child = subprocess.Popen(cmd, stdin=subprocess.PIPE, pass_fds=store.execution_lock.pass_fds)
    try:
        with pytest.raises(TransferBusy):
            with store.locked():
                pytest.fail('lingering child did not block maintenance')
        with store.locked('second'):
            with pytest.raises(TransferBusy):
                with store.queues(['/receiver']):
                    pytest.fail('orphan lost its receiving queue')
            with pytest.raises(TransferBusy, match='slots'):
                with StateStore(tmp_path, 2).locked('third'):
                    pytest.fail('orphan lost its slot')
    finally:
        child.communicate(input=b'x', timeout=5)
    with store.locked():
        pass


def make_runtime(root):
    config = dict(execution_schema_version=1, state_dir='state', event_spool='events.tsv',
                  transfers_file='routes.tsv', runtime_ids=['fixture'], max_concurrent_transfers=2,
                  execution_context=dict(system=socket.gethostname(), user=pwd.getpwuid(os.geteuid()).pw_name))
    rows = []
    for group in ('a', 'b'):
        src, dst = root/('input-'+group), root/('output-'+group)
        payload = src/'package'
        payload.mkdir(parents=True)
        (payload/'file').write_text(group)
        (payload/'.ready').touch()
        dst.mkdir()
        rows.append(dict(identifiers=group, flow_group=group, enabled='TRUE', executor='python', runtime_id='fixture',
                         system=config['execution_context']['system'], users=config['execution_context']['user'],
                         step_order='1', source=str(src), destination=str(dst), adapter='local', operation='copy',
                         verification='size', readiness_policy='producer_marker'))
    rows.append(dict(rows[0], identifiers='shared', flow_group='shared', destination=rows[1]['destination']))
    with (root/'routes.tsv').open('w') as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0], delimiter='\t')
        writer.writeheader()
        writer.writerows(rows)
    path = root/'config.yaml'
    path.write_text(yaml.safe_dump(config))
    return path


def test_discovery_runs_independent_routes_together_and_defers_conflict(tmp_path, monkeypatch):
    root = tmp_path.resolve()
    config = make_runtime(root)
    ready = Barrier(3)
    release = Event()
    copy = LocalAdapter.copy
    def hold_copy(self, *args):
        ready.wait(timeout=15)
        assert release.wait(timeout=15)
        return copy(self, *args)
    monkeypatch.setattr(LocalAdapter, 'copy', hold_copy)
    with ThreadPoolExecutor(max_workers=2) as pool:
        workers = [pool.submit(Executor(config).discover, key) for key in ('a','b')]
        try:
            ready.wait(timeout=15)
            assert main(['--config',str(config),'transfer','discover','--connection','shared']) == 75
        finally:
            release.set()
        assert all(task.result()['status'] == 'completed' for task in workers)
    events = (root/'events.tsv').read_text().splitlines()
    assert len(events) > 2 and events.count(events[0]) == 1
    receipts = [json.loads(p.read_text()) for p in (root/'state').glob('*.json')]
    assert len(receipts) == 2 and all(r['status'] == 'completed' for r in receipts)
    assert {r['deliveries'][0]['flow_group'] for r in receipts} == {'a','b'}


def test_limit_validation(tmp_path):
    path = make_runtime(tmp_path.resolve())
    settings = yaml.safe_load(path.read_text())
    for limit in (0, -1, True, '5', 1.5, 65):
        path.write_text(yaml.safe_dump(dict(settings, max_concurrent_transfers=limit)))
        with pytest.raises(ValueError, match='max_concurrent_transfers'):
            load_settings(path)
