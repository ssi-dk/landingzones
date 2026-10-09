#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for the optional python-build-standalone bundle assets."""

import os
import hashlib
import json
import tarfile
import sys
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
    assert "packaging/dist/landingzones-standalone-linux-${{ matrix.architecture }}.tar.gz.manifest.json" in workflow_text


def test_standalone_release_is_driven_by_version_or_explicit_test_tags():
    """Tag builds publish releases; branch dispatches only upload artifacts."""
    workflow_path = os.path.join(
        APP_ROOT, ".github", "workflows", "build-standalone.yml"
    )
    with open(workflow_path, "r") as handle:
        # BaseLoader preserves the GitHub Actions "on" key as a string.
        workflow = yaml.load(handle, Loader=yaml.BaseLoader)

    assert workflow["on"]["push"] == {"tags": ["v*", "test-*"]}
    assert "workflow_dispatch" in workflow["on"]
    job = workflow["jobs"]["build-linux"]
    assert job["permissions"]["contents"] == "read"
    checkout = next(step for step in job["steps"]
                    if step.get("uses", "").startswith("actions/checkout@"))
    assert "ref" not in checkout.get("with", {})
    assert workflow["concurrency"] == {
        "group": "${{ github.workflow }}-${{ github.ref }}", "cancel-in-progress": "false"
    }
    publishing_job = workflow["jobs"]["publish"]
    assert publishing_job["needs"] == "build-linux"
    assert publishing_job["permissions"]["contents"] == "write"
    assert publishing_job["if"] == "startsWith(github.ref, 'refs/tags/v') || startsWith(github.ref, 'refs/tags/test-')"
    publish = next(step for step in publishing_job["steps"]
                   if step.get("name") == "Publish standalone bundle to GitHub Release")
    assert 'gh release create "$GITHUB_REF_NAME"' in publish["run"]
    assert 'gh release upload "$GITHUB_REF_NAME"' in publish["run"]
    assert "--clobber" in publish["run"]
    assert not os.path.exists(os.path.join(
        APP_ROOT, ".github", "workflows", "release-on-version.yml"
    ))


@pytest.fixture
def builder():
    script = Path(APP_ROOT) / "scripts" / "build_python_standalone_bundle.py"
    spec = importlib.util.spec_from_file_location("standalone_builder", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_candidate_archives_exact_commit_and_excludes_uncommitted_changes(builder, tmp_path, monkeypatch):
    source = tmp_path / "app"
    source.mkdir()
    subprocess.run(["git", "init", str(source)], check=True, capture_output=True)
    (source / "payload.txt").write_text("committed content")
    subprocess.run(["git", "add", "."], cwd=source, check=True)
    subprocess.run(["git", "-c", "user.name=Packaging Test", "-c",
                    "user.email=packaging@example.invalid", "commit", "-m", "fixture"],
                   cwd=source, check=True, capture_output=True)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
    (source / "payload.txt").write_text("uncommitted replacement")
    (source / "untracked.txt").write_text("local only")
    monkeypatch.setattr(builder, "APP_ROOT", source)
    build = tmp_path / "build"
    build.mkdir()

    frozen, metadata = builder.prepare_source(revision, build)

    assert (frozen / "payload.txt").read_text() == "committed content"
    assert not (frozen / "untracked.txt").exists()
    assert metadata == {"source_revision": revision, "source_kind": "git-archive"}
    local, metadata = builder.prepare_source("", build)
    assert local == source
    assert metadata == {"source_revision": revision, "source_kind": "local-working-tree"}


@pytest.mark.parametrize("revision", ["main", "abc123", "HEAD", "a" * 40 + "^{commit}"])
def test_candidate_rejects_nonimmutable_revision(builder, tmp_path, revision):
    with pytest.raises(SystemExit, match="full lowercase Git commit SHA"):
        builder.prepare_source(revision, tmp_path)


def test_manifest_binds_tar_bytes_to_embedded_provenance(builder, tmp_path, monkeypatch):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    bundle = tmp_path / "landingzones-standalone"
    bundle.mkdir()
    (bundle / "landingzones").write_text("#!/bin/sh\n")
    metadata = builder.write_bundle_metadata(
        bundle, {"system": "Linux", "machine": "x86_64", "version": "3.12.12"},
        {"source_revision": "a" * 40, "source_kind": "git-archive"},
        {"landingzones": "1.1.16", "paramiko": "3.5.1"},
    )
    archive = builder.create_tarball(bundle, "candidate-linux-x86_64.tar.gz")
    manifest = json.loads(builder.write_artifact_manifest(archive, bundle, metadata).read_text())

    assert manifest["archive"] == {
        "filename": archive.name,
        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
        "root": "landingzones-standalone", "launcher": "landingzones",
    }
    assert Path(str(archive) + ".sha256").read_text() == manifest["archive"]["sha256"] + "  " + archive.name + "\n"
    with tarfile.open(archive) as handle:
        embedded = json.load(handle.extractfile("landingzones-standalone/bundle.json"))
    assert embedded == {key: value for key, value in manifest.items() if key != "archive"}
    assert "publication" not in embedded
    assert embedded["capabilities"] == ["sftp", "ena"]


def test_ci_publication_requires_source_to_match_run(builder, monkeypatch):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    for key, value in {
        "GITHUB_REPOSITORY": "example/landingzones", "GITHUB_RUN_ID": "123",
        "GITHUB_RUN_ATTEMPT": "2", "GITHUB_SHA": "b" * 40,
        "GITHUB_SERVER_URL": "https://github.com",
    }.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(SystemExit, match="exact GITHUB_SHA"):
        builder.publication_metadata({"source_kind": "git-archive", "source_revision": "a" * 40})
    with pytest.raises(SystemExit, match="exact GITHUB_SHA"):
        builder.publication_metadata({"source_kind": "local-working-tree", "source_revision": "b" * 40})
    publication = builder.publication_metadata({"source_kind": "git-archive", "source_revision": "b" * 40})
    assert publication == {
        "provider": "github-actions", "repository": "example/landingzones", "run_id": "123",
        "run_attempt": "2", "run_url": "https://github.com/example/landingzones/actions/runs/123",
    }


def test_candidate_workflow_pins_commit_and_uploads_all_verification_files():
    workflow = yaml.load((Path(APP_ROOT) / ".github/workflows/build-standalone.yml").read_text(), Loader=yaml.BaseLoader)
    steps = workflow["jobs"]["build-linux"]["steps"]
    build = next(step for step in steps if step.get("name") == "Build standalone bundle")
    assert build["env"]["SOURCE_REVISION"] == "${{ github.sha }}"
    upload = next(step for step in steps if step.get("uses", "").startswith("actions/upload-artifact@"))
    paths = upload["with"]["path"].splitlines()
    archive = "packaging/dist/landingzones-standalone-linux-${{ matrix.architecture }}.tar.gz"
    assert paths == [archive, archive + ".manifest.json", archive + ".sha256"]
    assert "${{ github.sha }}" in upload["with"]["name"]


@pytest.mark.parametrize("tag,existing", [
    ("test-request-driven-transfers-001", False),
    ("test-request-driven-transfers-001", True),
    ("v1.1.16", False),
    ("v1.1.16", True),
])
def test_test_tag_release_is_never_latest_and_stable_release_behavior_is_preserved(tmp_path, tag, existing):
    workflow = yaml.load((Path(APP_ROOT) / ".github/workflows/build-standalone.yml").read_text(), Loader=yaml.BaseLoader)
    publish = next(step for step in workflow["jobs"]["publish"]["steps"]
                   if step.get("name") == "Publish standalone bundle to GitHub Release")
    fake_gh = tmp_path / "gh"
    fake_gh.write_text(
        "#!" + sys.executable + "\n"
        "import json, os, sys\n"
        "with open(os.environ['GH_CALL_LOG'], 'a') as handle:\n"
        "    handle.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "if sys.argv[1:3] == ['release', 'view'] and os.environ['GH_RELEASE_EXISTS'] == 'no':\n"
        "    sys.exit(1)\n"
    )
    fake_gh.chmod(0o755)
    log = tmp_path / "calls.jsonl"
    environment = dict(os.environ, GITHUB_REF_NAME=tag, GH_CALL_LOG=str(log),
                       GH_RELEASE_EXISTS="yes" if existing else "no",
                       PATH=str(tmp_path) + os.pathsep + os.environ["PATH"])
    architectures = ["x86_64", "aarch64"] if tag.startswith("test-") else ["x86_64"]
    expected_assets = create_publishing_assets(tmp_path, architectures)
    subprocess.run(["bash", "-e", "-c", publish["run"]], check=True, env=environment, cwd=tmp_path)
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    creates = [call for call in calls if call[:2] == ["release", "create"]]
    assert len(creates) == (0 if existing else 1)
    edits = [call for call in calls if call[:2] == ["release", "edit"]]
    if tag.startswith("test-"):
        assert edits == [["release", "edit", tag, "--prerelease", "--latest=false"]]
        for call in creates:
            assert "--prerelease" in call and "--latest=false" in call
    else:
        assert edits == []
        assert all("--prerelease" not in call and "--latest=false" not in call for call in creates)
    uploads = [call for call in calls if call[:2] == ["release", "upload"]]
    assert len(uploads) == 1
    assert uploads[0][2] == tag
    assert uploads[0][-1] == "--clobber"
    assert set(uploads[0][3:-1]) == set(expected_assets)


def test_test_tags_do_not_publish_to_package_registries():
    for name in ("publish.yml", "publish-conda.yml"):
        workflow = yaml.load((Path(APP_ROOT) / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader)
        assert workflow["on"]["push"]["tags"] == ["v*"]



def create_publishing_assets(root, architectures):
    folder = root / "packaging/dist"
    folder.mkdir(parents=True, exist_ok=True)
    assets = []
    for architecture in architectures:
        filename = "landingzones-standalone-linux-" + architecture + ".tar.gz"
        archive = folder / filename
        archive.write_bytes(architecture.encode())
        (folder / (filename + ".manifest.json")).write_text("{}\n")
        (folder / (filename + ".sha256")).write_text(hashlib.sha256(archive.read_bytes()).hexdigest() + "  " + filename + "\n")
        assets.extend("packaging/dist/" + filename + suffix for suffix in ("", ".manifest.json", ".sha256"))
    return assets


@pytest.mark.parametrize("problem", ["missing_arm", "missing_manifest", "corrupt_archive"])
def test_publication_stops_before_release_changes_for_incomplete_or_corrupt_assets(tmp_path, problem):
    workflow = yaml.load((Path(APP_ROOT) / ".github/workflows/build-standalone.yml").read_text(), Loader=yaml.BaseLoader)
    publish = next(step for step in workflow["jobs"]["publish"]["steps"]
                   if step.get("name") == "Publish standalone bundle to GitHub Release")
    architectures = ["x86_64"] if problem == "missing_arm" else ["x86_64", "aarch64"]
    create_publishing_assets(tmp_path, architectures)
    archive = tmp_path / "packaging/dist/landingzones-standalone-linux-aarch64.tar.gz"
    if problem == "missing_manifest":
        Path(str(archive) + ".manifest.json").unlink()
    if problem == "corrupt_archive":
        archive.write_bytes(b"changed")
    log = tmp_path / "gh-called"
    fake_gh = tmp_path / "gh"
    fake_gh.write_text("#!/bin/sh\ntouch '" + str(log) + "'\n")
    fake_gh.chmod(0o755)
    environment = dict(os.environ, GITHUB_REF_NAME="test-candidate", PATH=str(tmp_path) + os.pathsep + os.environ["PATH"])
    result = subprocess.run(["bash", "-e", "-c", publish["run"]], env=environment, cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode != 0
    assert not log.exists()


def test_multiarchitecture_builds_use_native_runners_and_one_release_publisher():
    workflow = yaml.load((Path(APP_ROOT) / ".github/workflows/build-standalone.yml").read_text(), Loader=yaml.BaseLoader)
    job = workflow["jobs"]["build-linux"]
    assert job["strategy"]["matrix"]["architecture"] == "${{ fromJSON(startsWith(github.ref, 'refs/tags/test-') && '[\"x86_64\",\"aarch64\"]' || (!startsWith(github.ref, 'refs/tags/v') && inputs.architecture == 'aarch64' && '[\"aarch64\"]' || '[\"x86_64\"]')) }}"
    assert job["runs-on"] == "${{ matrix.architecture == 'aarch64' && 'ubuntu-24.04-arm' || 'ubuntu-latest' }}"
    assert workflow["on"]["workflow_dispatch"]["inputs"]["architecture"]["options"] == ["x86_64", "aarch64"]
    build = next(step for step in job["steps"] if step.get("name") == "Build standalone bundle")
    assert build["env"]["PBS_ARCHITECTURE"] == "${{ matrix.architecture }}-unknown-linux-gnu"
    assert build["env"]["ARCHIVE_NAME"] == "landingzones-standalone-linux-${{ matrix.architecture }}.tar.gz"
    publishers = [name for name, value in workflow["jobs"].items() if any("gh release upload" in step.get("run", "") for step in value["steps"])]
    assert publishers == ["publish"]
    download = workflow["jobs"]["publish"]["steps"][0]
    assert download["with"]["merge-multiple"] == "true"
    assert "${{ github.sha }}" in download["with"]["pattern"]
