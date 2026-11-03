"""Security gates run before bundle dependency downloads."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def builder():
    script = Path(__file__).resolve().parents[1] / "scripts/build_python_standalone_bundle.py"
    spec = importlib.util.spec_from_file_location("pip_security_builder", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("version", ["26.2.2", "26.3", "27.0.1"])
def test_current_pip_needs_no_bootstrap(builder, monkeypatch, version):
    monkeypatch.setattr(builder.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        returncode=0, stdout="pip " + version + " from /runtime/pip (python 3.12)"))
    calls = []
    monkeypatch.setattr(builder, "run", lambda argv: calls.append(argv))
    builder.ensure_pip("/runtime/python")
    assert not calls


@pytest.mark.parametrize("version", ["25.3", "26.2.1", "26.2.2rc1"])
def test_old_or_prerelease_pip_uses_verified_bootstrap(builder, monkeypatch, version):
    replies = iter([version, version, "26.2.2"])
    monkeypatch.setattr(builder.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        returncode=0, stdout="pip " + next(replies) + " from /runtime/pip (python 3.12)"))
    calls = []
    monkeypatch.setattr(builder, "run", lambda argv: calls.append(argv))
    builder.ensure_pip("/runtime/python")
    assert len(calls) == 1
    assert "--no-deps" in calls[0]
    assert calls[0][-1] == "pip @ " + builder.PIP_WHEEL_URL + "#sha256=" + builder.PIP_WHEEL_SHA256


def test_missing_pip_is_seeded_before_verified_upgrade(builder, monkeypatch):
    replies = iter([(1, ""), (1, ""), (0, "pip 26.2.2 from /runtime/pip (python 3.12)")])
    def probe(*args, **kwargs):
        status, output = next(replies)
        return SimpleNamespace(returncode=status, stdout=output)
    monkeypatch.setattr(builder.subprocess, "run", probe)
    calls = []
    monkeypatch.setattr(builder, "run", lambda argv: calls.append(argv))
    builder.ensure_pip("/runtime/python")
    assert calls[0] == ["/runtime/python", "-m", "ensurepip", "--upgrade"]
    assert "#sha256=" in calls[1][-1]


def test_offline_old_pip_requires_the_verified_local_wheel(builder, monkeypatch, tmp_path):
    monkeypatch.setattr(builder.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        returncode=0, stdout="pip 26.0.1 from /runtime/pip (python 3.12)"))
    calls = []
    monkeypatch.setattr(builder, "run", lambda argv: calls.append(argv))
    with pytest.raises(SystemExit, match="Offline bootstrap requires the verified wheel"):
        builder.ensure_pip("/runtime/python", str(tmp_path))
    assert not calls
    wheel = tmp_path / builder.PIP_WHEEL
    wheel.write_bytes(b"pip will verify the advisory digest before installing")
    replies = iter(["26.0.1", "26.0.1", "26.2.2"])
    monkeypatch.setattr(builder.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        returncode=0, stdout="pip " + next(replies) + " from /runtime/pip (python 3.12)"))
    builder.ensure_pip("/runtime/python", str(tmp_path))
    assert "--no-index" in calls[0]
    assert calls[0][-1] == "pip @ " + wheel.as_uri() + "#sha256=" + builder.PIP_WHEEL_SHA256


def test_failed_upgrade_cannot_continue_to_application_install(builder, monkeypatch):
    monkeypatch.setattr(builder.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        returncode=0, stdout="pip 26.0.1 from /runtime/pip (python 3.12)"))
    monkeypatch.setattr(builder, "run", lambda argv: None)
    with pytest.raises(SystemExit, match="did not provide pip >=26.2.2"):
        builder.ensure_pip("/runtime/python")


def test_legacy_instrument_runtime_is_rejected_before_build(builder, monkeypatch):
    monkeypatch.setattr(builder, 'describe_python_runtime', lambda path: {
        'version': '3.11.18', 'system': builder.platform.system(), 'machine': builder.platform.machine(),
    })
    with pytest.raises(SystemExit, match='requires Python 3.12 or newer'):
        builder.validate_runtime_matches_host('/legacy/python')
