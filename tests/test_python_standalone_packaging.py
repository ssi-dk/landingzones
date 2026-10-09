#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for the optional python-build-standalone bundle assets."""

import os
import importlib.util
from pathlib import Path
import subprocess

import pytest
import yaml


APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def test_python_standalone_build_script_help():
    """The standalone bundle builder should document required inputs."""
    script_path = os.path.join(
        APP_ROOT,
        "scripts",
        "build_python_standalone_bundle.py",
    )

    proc = subprocess.run(
        ["python", script_path, "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert "--python-bin" in proc.stdout
    assert "--python-archive" in proc.stdout
    assert "--download-python" in proc.stdout
    assert "python-build-standalone" in proc.stdout


def test_shell_wrapper_can_be_invoked_with_python():
    """The .sh wrapper should avoid SyntaxError when called through Python."""
    script_path = os.path.join(
        APP_ROOT,
        "scripts",
        "build_python_standalone_bundle.sh",
    )

    proc = subprocess.run(
        ["python", script_path, "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert "--python-bin" in proc.stdout
    assert "python-build-standalone" in proc.stdout


def test_python_standalone_build_script_contains_launcher_and_bundle_steps():
    """The builder should install into a bundle and create a CLI launcher."""
    script_path = os.path.join(
        APP_ROOT,
        "scripts",
        "build_python_standalone_bundle.py",
    )
    script_text = open(script_path, "r").read()

    assert "PBS_PYTHON" in script_text
    assert "PBS_ARCHIVE" in script_text
    assert "WHEELHOUSE" in script_text
    assert '"--target"' in script_text
    assert 'exec "$PYTHON_BIN" -m landingzones.cli' in script_text
    assert 'tarfile.open(archive_path, "w:gz")' in script_text


def test_pixi_config_includes_standalone_packaging_task():
    """Pixi should expose the standalone bundle build path."""
    pixi_path = os.path.join(APP_ROOT, "pixi.toml")
    pixi_text = open(pixi_path, "r").read()

    assert "getpybs" in pixi_text
    assert "build-standalone" in pixi_text
    assert "build_python_standalone_bundle.py --download-python" in pixi_text


def test_base_package_does_not_require_pandas():
    """The standalone core bundle should not pull pandas into CentOS 7 installs."""
    pyproject_path = os.path.join(APP_ROOT, "pyproject.toml")
    pyproject_text = open(pyproject_path, "r").read()
    base_dependencies = pyproject_text.split("[project.optional-dependencies]", 1)[0]

    assert '"pandas' not in base_dependencies
    assert "report = [" in pyproject_text


def test_sftp_backend_is_optional_for_normal_package_installation():
    """Minimal pip installs stay local-only; the named extra supplies one backend."""
    pyproject_text = (Path(APP_ROOT) / "pyproject.toml").read_text()
    base_dependencies, optional_dependencies = pyproject_text.split(
        "[project.optional-dependencies]", 1
    )
    sftp_dependencies = optional_dependencies.split("sftp = [", 1)[1].split("]", 1)[0]

    assert '"paramiko' not in base_dependencies
    assert '"paramiko' in sftp_dependencies


@pytest.mark.parametrize("offline", [False, True])
def test_standalone_install_includes_sftp_extra_without_changing_offline_mode(
    tmp_path, monkeypatch, offline
):
    """The actual pip invocation requests SFTP in both online and wheelhouse builds."""
    script = Path(APP_ROOT) / "scripts" / "build_python_standalone_bundle.py"
    spec = importlib.util.spec_from_file_location("standalone_builder", script)
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    commands = []
    monkeypatch.setattr(builder, "run", lambda command: commands.append(command))
    application = tmp_path / "source with spaces"
    monkeypatch.setattr(builder, "APP_ROOT", application)
    python_bin = tmp_path / "python" / "bin" / "python3"
    site_packages = tmp_path / "bundle" / "site-packages"
    wheelhouse = str(tmp_path / "local wheels") if offline else ""

    builder.install_application(python_bin, site_packages, wheelhouse)

    expected = [str(python_bin), "-m", "pip", "install", "--target", str(site_packages)]
    if offline:
        expected.extend(["--no-index", "--find-links", wheelhouse])
    expected.append(str(application) + "[sftp]")
    assert commands == [expected]
    assert site_packages.is_dir()


def test_github_action_builds_and_uploads_standalone_bundle():
    """The GitHub workflow should publish the standalone tarball as an artifact."""
    workflow_path = os.path.join(
        APP_ROOT,
        ".github",
        "workflows",
        "build-standalone.yml",
    )
    with open(workflow_path, "r") as handle:
        workflow = yaml.safe_load(handle)
    workflow_text = open(workflow_path, "r").read()

    assert workflow["name"] == "Build Standalone Bundle"
    assert "workflow_dispatch" in workflow_text
    assert "pixi run build-standalone" in workflow_text
    assert "landingzones-standalone-linux" in workflow_text
    assert "packaging/dist/landingzones-standalone.tar.gz" in workflow_text


def test_standalone_release_is_driven_by_version_tags():
    """Tag builds publish releases; branch dispatches only upload artifacts."""
    workflow_path = os.path.join(
        APP_ROOT, ".github", "workflows", "build-standalone.yml"
    )
    with open(workflow_path, "r") as handle:
        # BaseLoader preserves the GitHub Actions "on" key as a string.
        workflow = yaml.load(handle, Loader=yaml.BaseLoader)

    assert workflow["on"]["push"] == {"tags": ["v*"]}
    assert "workflow_dispatch" in workflow["on"]
    job = workflow["jobs"]["build-linux"]
    assert job["permissions"]["contents"] == "write"
    checkout = next(step for step in job["steps"]
                    if step.get("uses", "").startswith("actions/checkout@"))
    assert "ref" not in checkout.get("with", {})
    publish = next(step for step in job["steps"]
                   if step.get("name") == "Publish standalone bundle to GitHub Release")
    assert publish["if"] == "startsWith(github.ref, 'refs/tags/')"
    assert 'gh release create "$GITHUB_REF_NAME"' in publish["run"]
    assert 'gh release upload "$GITHUB_REF_NAME"' in publish["run"]
    assert "--clobber" in publish["run"]
    assert not os.path.exists(os.path.join(
        APP_ROOT, ".github", "workflows", "release-on-version.yml"
    ))
