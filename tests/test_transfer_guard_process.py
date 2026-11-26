"""Check actual transport lifetimes using a local SSH pipe with no network access."""
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time

import pytest

from landingzones.execution.storage import StateStore, TransferBusy


# The parent test kills only this worker. Real OpenSSH closes extra FDs; rsync
# and the publication supervisor keep the original local locks until SSH exits.
WORKER = """
from pathlib import Path
import sys
from types import SimpleNamespace
from urllib.parse import urlsplit
from landingzones.execution.adapters import RemoteRsyncAdapter
from landingzones.execution.storage import StateStore
root, command, proxy = sys.argv[1:]
store = StateStore(root, 2)
with store.locked('route'), store.queues(['/queue']):
    adapter = object.__new__(RemoteRsyncAdapter)
    adapter.target = urlsplit('sftp://test@fixture.invalid:22/queue')
    adapter.root = '/queue'
    adapter.step = SimpleNamespace(publication_group='fixture')
    adapter.pinned_ssh_command = lambda: ['ssh', '-F', '/dev/null', '-o', 'ProxyCommand=' + proxy,
                                         '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15']
    adapter.execution_lock = store.execution_lock
    if command == 'ssh':
        adapter.receiver_publication('prepare')
    else:
        adapter.copy_manifest(str(Path(root) / 'src') + '/', 'test@fixture.invalid:/queue', '/tmp',
                              {'file': {'kind': 'file'}}, store.execution_lock)
"""


@pytest.mark.parametrize('command', ['ssh', 'rsync'])
def test_orphaned_transport_keeps_maintenance_and_queue_guards(tmp_path, command):
    if not shutil.which('ssh') or not shutil.which(command):
        pytest.skip('requires the installed SSH/rsync client')
    if command == 'rsync':
        help_text = subprocess.check_output(['rsync', '--help'], text=True, stderr=subprocess.STDOUT)
        if '--protect-args' not in help_text and '--secluded-args' not in help_text:
            pytest.skip('requires rsync with protected argument support')
    (tmp_path / 'src').mkdir()
    (tmp_path / 'src/file').write_text('fixture')
    proxy = tmp_path / 'proxy.py'
    proxy.write_text("""from pathlib import Path
import sys, time
root = Path(sys.argv[1])
(root / 'ready').touch()
until = time.monotonic() + 12
while not (root / 'release').exists() and time.monotonic() < until:
    time.sleep(.02)
""")
    proxy_cmd = shlex.join([sys.executable, str(proxy), str(tmp_path)])
    src = Path(__file__).resolve().parents[1] / 'src'
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(src), os.environ.get('PYTHONPATH', '')]))
    store = StateStore(tmp_path, 2)
    with subprocess.Popen([sys.executable, '-c', WORKER, str(tmp_path), command, proxy_cmd],
                          stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=env) as worker:
        try:
            deadline = time.monotonic() + 8
            while not (tmp_path / 'ready').exists():
                if worker.poll() is not None:
                    pytest.fail('Transport exited before opening its local proxy: ' + worker.stderr.read().decode())
                assert time.monotonic() < deadline, 'Transport did not start'
                time.sleep(.02)
            worker.kill()
            worker.wait(timeout=5)
            with pytest.raises(TransferBusy):
                with store.locked():
                    pytest.fail('Maintenance overlapped the live transport')
            with store.locked('other'), pytest.raises(TransferBusy):
                with store.queues(['/queue']):
                    pytest.fail('Transfer overlapped the live transport')
        finally:
            (tmp_path / 'release').touch()
            if worker.poll() is None:
                worker.kill()
    deadline = time.monotonic() + 6
    while True:
        try:
            with store.locked():
                break
        except TransferBusy:
            assert time.monotonic() < deadline, 'Guard held after SSH exited'
            time.sleep(.02)
