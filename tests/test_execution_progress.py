import os

from landingzones.execution.progress import Progress, rsync_progress


def test_rsync_carriage_return_counter_is_streamed_without_file_names():
    observations = []
    with rsync_progress(observations.append) as out:
        os.write(out.fileno(), b'\r  1,024 10% 100.00kB/s 0:00:01\r  10,240 100% 1.23MB/s 0:00:00 (xfr#1)\n')
    assert observations == [1024, 10240]


def test_attempt_speed_eta_and_verified_bytes(monkeypatch):
    samples = []
    tick = [100.0]
    monkeypatch.setattr('landingzones.execution.progress.time.monotonic', lambda: tick[0])
    state = dict(total_bytes=1000, manifest={'a': {'kind':'file'}, 'd': {'kind':'directory'}})
    progress = Progress(state, samples.append)
    progress.observe(10)
    assert samples[-1]['rate_bytes_per_second'] is None
    tick[0] += 1
    progress.observe(20)
    assert len(samples) == 1
    tick[0] += 9
    progress.observe(110)
    assert samples[-1] == dict(bytes_done=110, bytes_total=1000, files_total=1, rate_bytes_per_second=10, eta_seconds=89)
    progress.observe(98)
    progress.observe(1500)
    assert len(samples) == 2
    tick[0] += 10
    progress.observe(110)
    assert samples[-1]['rate_bytes_per_second'] == 0 and samples[-1]['eta_seconds'] is None
    progress.verified()
    assert samples[-1]['bytes_done'] == 1000 and samples[-1]['eta_seconds'] is None
