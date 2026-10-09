#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build a relocatable Landing Zones bundle from python-build-standalone."""

import argparse
import hashlib
import json
import re
import os
import shutil
import subprocess
import sys
import tarfile
import platform
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUILD_ROOT = APP_ROOT / "packaging" / "build" / "python-standalone"
DEFAULT_DIST_ROOT = APP_ROOT / "packaging" / "dist" / "landingzones-standalone"


def run(command, **kwargs):
    """Run a command, failing with the child process exit code."""
    print("+ {0}".format(" ".join(str(part) for part in command)))
    subprocess.run(command, check=True, **kwargs)


def capture(command, **kwargs):
    """Run a command and return stripped stdout."""
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        **kwargs,
    )
    return result.stdout.strip()


def build_parser():
    """Build CLI parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Build a relocatable Landing Zones bundle using a "
            "python-build-standalone runtime."
        )
    )
    parser.add_argument(
        "--python-bin",
        default=os.environ.get("PBS_PYTHON", ""),
        help="Path to an extracted python-build-standalone Python executable.",
    )
    parser.add_argument(
        "--python-archive",
        default=os.environ.get("PBS_ARCHIVE", ""),
        help="Path to a python-build-standalone install_only archive.",
    )
    parser.add_argument(
        "--download-python",
        action="store_true",
        help="Download a python-build-standalone archive using getpybs.",
    )
    parser.add_argument(
        "--python-version",
        default=os.environ.get("PBS_PYTHON_VERSION", "3.12"),
        help="Python version to download when --download-python is used.",
    )
    parser.add_argument(
        "--architecture",
        default=os.environ.get("PBS_ARCHITECTURE", ""),
        help="Target python-build-standalone architecture for getpybs.",
    )
    parser.add_argument(
        "--build-version",
        default=os.environ.get("PBS_BUILD_VERSION", "latest"),
        help="python-build-standalone release for getpybs.",
    )
    parser.add_argument(
        "--build-config",
        default=os.environ.get("PBS_BUILD_CONFIG", "pgo+lto"),
        help="python-build-standalone build config for getpybs.",
    )
    parser.add_argument(
        "--content-type",
        default=os.environ.get("PBS_CONTENT_TYPE", "install_only_stripped"),
        help="python-build-standalone content type for getpybs.",
    )
    parser.add_argument(
        "--wheelhouse",
        default=os.environ.get("WHEELHOUSE", ""),
        help="Optional local wheelhouse for offline dependency installation.",
    )
    parser.add_argument(
        "--build-root",
        default=os.environ.get("BUILD_ROOT", str(DEFAULT_BUILD_ROOT)),
        help="Temporary build directory.",
    )
    parser.add_argument(
        "--dist-root",
        default=os.environ.get("DIST_ROOT", str(DEFAULT_DIST_ROOT)),
        help="Output bundle directory.",
    )
    parser.add_argument(
        "--source-revision",
        default=os.environ.get("SOURCE_REVISION", ""),
        help="Exact Git commit to archive and package; omit for local working-tree builds.",
    )
    parser.add_argument(
        "--archive-name",
        default=os.environ.get("ARCHIVE_NAME", ""),
        help="Optional output tar.gz basename, beside --dist-root.",
    )
    return parser


def getpybs_command():
    """Return the getpybs command invocation."""
    executable = shutil.which("getpybs")
    if executable:
        return [executable]
    return [sys.executable, "-m", "getpybs"]


def download_python_archive(args, download_dir):
    """Download a python-build-standalone archive via getpybs."""
    download_dir.mkdir(parents=True, exist_ok=True)
    command = getpybs_command() + [
        "--build-version",
        args.build_version,
        "--python-version",
        args.python_version,
        "--build-config",
        args.build_config,
        "--content-type",
        args.content_type,
        "--dest",
        str(download_dir),
    ]
    if args.architecture:
        command.extend(["--architecture", args.architecture])
    try:
        run(command)
    except subprocess.CalledProcessError:
        raise SystemExit(
            "Failed to download python-build-standalone with getpybs. "
            "Install the Pixi environment or pass --python-archive/--python-bin."
        )

    archives = sorted(
        path for path in download_dir.iterdir()
        if path.name.startswith("cpython-") and ".tar." in path.name
    )
    if not archives:
        raise SystemExit("getpybs did not produce a cpython tar archive")
    return archives[-1]


def extract_archive(archive, target_dir):
    """Extract a python-build-standalone archive."""
    target_dir.mkdir(parents=True, exist_ok=True)
    run(["tar", "-xf", str(archive), "-C", str(target_dir)])


def find_python_bin(root):
    """Find the Python executable inside an extracted standalone runtime."""
    candidates = []
    for pattern in ("python3.12", "python3.11", "python3", "python"):
        candidates.extend(root.glob("**/bin/{0}".format(pattern)))
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    raise SystemExit("Could not find a Python executable under {0}".format(root))


def describe_python_runtime(python_bin):
    """Return basic compatibility metadata for the bundled Python runtime."""
    script = (
        "import platform, sys; "
        "print(platform.system()); "
        "print(platform.machine()); "
        "print(sys.version.split()[0])"
    )
    output = capture([str(python_bin), "-c", script])
    system, machine, version = output.splitlines()[:3]
    return {
        "system": system,
        "machine": machine,
        "version": version,
    }


def validate_runtime_matches_host(python_bin):
    """Fail early when the downloaded runtime cannot execute on this host."""
    try:
        metadata = describe_python_runtime(python_bin)
    except Exception as exc:
        raise SystemExit(
            "The selected python-build-standalone runtime could not execute on "
            "this build host: {0}. This usually means the archive architecture "
            "or OS target does not match the machine running the build.".format(exc)
        )

    host_system = platform.system()
    host_machine = platform.machine()
    print(
        "Selected runtime: Python {version} for {system}/{machine}".format(
            **metadata
        )
    )
    print("Build host: {0}/{1}".format(host_system, host_machine))
    if metadata["system"] != host_system:
        raise SystemExit(
            "Runtime OS mismatch: selected {0}, build host is {1}. Build the "
            "Linux lab bundle on Linux, or pass a Linux python-build-standalone "
            "archive to a Linux build job.".format(metadata["system"], host_system)
        )
    if metadata["machine"] != host_machine:
        raise SystemExit(
            "Runtime architecture mismatch: selected {0}, build host is {1}.".format(
                metadata["machine"], host_machine
            )
        )
    return metadata


def ensure_pip(python_bin):
    """Ensure pip is available in the bundled runtime."""
    result = subprocess.run(
        [str(python_bin), "-m", "pip", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return
    run([str(python_bin), "-m", "ensurepip", "--upgrade"])


def install_application(python_bin, site_packages, wheelhouse, source_root=None):
    """Install Landing Zones and its SFTP backend into the bundle site-packages."""
    site_packages.mkdir(parents=True, exist_ok=True)
    command = [str(python_bin), "-m", "pip", "install", "--target", str(site_packages)]
    if wheelhouse:
        command.extend(["--no-index", "--find-links", wheelhouse])
    command.append(str(source_root or APP_ROOT) + "[sftp]")
    run(command)


def write_launcher(dist_root):
    """Write the relocatable landingzones launcher."""
    launcher = dist_root / "landingzones"
    launcher.write_text(
        """#!/bin/sh
set -eu
SELF_DIR="$(CDPATH= cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="$SELF_DIR/python/bin/python3"
if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="$SELF_DIR/python/bin/python"
fi
export PYTHONPATH="$SELF_DIR/site-packages${PYTHONPATH:+:$PYTHONPATH}"
exec "$PYTHON_BIN" -m landingzones.cli "$@"
"""
    )
    launcher.chmod(0o755)


def write_readme(dist_root):
    """Write bundle-local operator notes."""
    (dist_root / "README.txt").write_text(
        """Landing Zones standalone bundle

Run:
  ./landingzones --help
  ./landingzones --config config/config.yaml build --runtime-id <runtime_id>
  ./landingzones --config config/config.yaml validate deployment
  ./landingzones --config config/config.yaml validate integration
  ./landingzones --config config/config.yaml validate separation
  ./landingzones --config config/config.yaml deploy cron
  ./landingzones --config config/config.yaml report transfers
  ./landingzones --config config/config.yaml monitor sync-definitions
  ./landingzones --config config/config.yaml monitor ingest <event-spool.tsv>
  ./landingzones --config config/config.yaml monitor serve

Validation, deploy, reporting, and monitoring commands reuse the runtime IDs
generated by the latest build. Pass --runtime-id at the root or subcommand level
to use a different explicit subset.

This bundle carries Python and Python packages only. The target machine still
needs system tools used by generated transfer scripts: rsync, ssh, flock, curl,
and cron.
"""
    )


def prepare_source(source_revision, build_root):
    """Freeze a requested commit; otherwise identify a local-only working tree."""
    if source_revision:
        if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", source_revision):
            raise SystemExit("--source-revision must be a full lowercase Git commit SHA")
        resolved = capture(
            ["git", "rev-parse", "--verify", source_revision + "^{commit}"], cwd=APP_ROOT
        )
        if resolved != source_revision:
            raise SystemExit("--source-revision did not resolve to the supplied commit")
        archive = build_root / "source.tar"
        source_root = build_root / "source"
        source_root.mkdir()
        run(["git", "archive", "--format=tar", "--output", str(archive), resolved], cwd=APP_ROOT)
        extract_archive(archive, source_root)
        return source_root, {"source_revision": resolved, "source_kind": "git-archive"}
    try:
        revision = capture(["git", "rev-parse", "HEAD"], cwd=APP_ROOT)
    except subprocess.CalledProcessError:
        revision = None
    return APP_ROOT, {"source_revision": revision, "source_kind": "local-working-tree"}


def describe_application(python_bin, site_packages):
    """Record resolved dependency versions and verify the included backends import."""
    script = (
        "import importlib.metadata as m, json; "
        "import paramiko, landingzones.execution.ena, landingzones.execution.ena_submission; "
        "print(json.dumps({d.metadata['Name']: d.version "
        "for d in m.distributions(path=[" + repr(str(site_packages)) + "])}))"
    )
    env = dict(os.environ, PYTHONPATH=str(site_packages))
    return json.loads(capture([str(python_bin), "-c", script], env=env))


def publication_metadata(source):
    """Attach the producing Actions run only when GitHub provides its context."""
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return None
    required = ("GITHUB_REPOSITORY", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA")
    if any(not os.environ.get(key) for key in required):
        raise SystemExit("Incomplete GitHub Actions publication context")
    if source["source_kind"] != "git-archive" or source["source_revision"] != os.environ["GITHUB_SHA"]:
        raise SystemExit("GitHub Actions candidate must package its exact GITHUB_SHA")
    repository = os.environ["GITHUB_REPOSITORY"]
    run_id = os.environ["GITHUB_RUN_ID"]
    return {
        "provider": "github-actions",
        "repository": repository,
        "run_id": run_id,
        "run_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
        "run_url": os.environ.get("GITHUB_SERVER_URL", "https://github.com").rstrip("/")
        + "/" + repository + "/actions/runs/" + run_id,
    }


def write_bundle_metadata(dist_root, runtime, source, packages):
    """Write provenance inside the archive before its digest is calculated."""
    metadata = {
        "schema_version": 1,
        "application": "landingzones",
        "application_version": packages["landingzones"],
        **source,
        "platform": {"system": runtime["system"], "machine": runtime["machine"]},
        "python_version": runtime["version"],
        "capabilities": ["sftp", "ena"],
        "packages": packages,
    }
    publication = publication_metadata(source)
    if publication:
        metadata["publication"] = publication
    (dist_root / "bundle.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    return metadata


def write_artifact_manifest(archive_path, dist_root, metadata):
    """Bind archive bytes, layout and embedded provenance in a portable sidecar."""
    digest = hashlib.sha256()
    with archive_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    manifest = dict(metadata, archive={
        "filename": archive_path.name,
        "sha256": digest.hexdigest(),
        "root": dist_root.name,
        "launcher": "landingzones",
    })
    manifest_path = Path(str(archive_path) + ".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    Path(str(archive_path) + ".sha256").write_text(
        digest.hexdigest() + "  " + archive_path.name + "\n"
    )
    return manifest_path


def create_tarball(dist_root, archive_name=""):
    """Create a tar.gz archive beside the bundle directory."""
    archive_path = dist_root.with_suffix(dist_root.suffix + ".tar.gz")
    if archive_name:
        if Path(archive_name).name != archive_name or not archive_name.endswith(".tar.gz"):
            raise SystemExit("--archive-name must be a .tar.gz basename")
        archive_path = dist_root.parent / archive_name
    if archive_path.exists():
        archive_path.unlink()
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(dist_root, arcname=dist_root.name)
    return archive_path


def main(argv=None):
    """Run bundle build."""
    args = build_parser().parse_args(argv)
    build_root = Path(args.build_root).resolve()
    dist_root = Path(args.dist_root).resolve()

    shutil.rmtree(build_root, ignore_errors=True)
    shutil.rmtree(dist_root, ignore_errors=True)
    build_root.mkdir(parents=True)
    dist_root.mkdir(parents=True)

    python_archive = Path(args.python_archive).expanduser() if args.python_archive else None
    python_bin = Path(args.python_bin).expanduser() if args.python_bin else None

    if args.download_python:
        python_archive = download_python_archive(args, build_root / "downloads")

    if python_archive:
        if not python_archive.is_file():
            raise SystemExit("Python archive does not exist: {0}".format(python_archive))
        extract_archive(python_archive, build_root / "runtime")
        python_bin = find_python_bin(build_root / "runtime")

    if not python_bin:
        raise SystemExit(
            "Provide --python-bin, --python-archive, or --download-python."
        )
    if not python_bin.is_file() or not os.access(python_bin, os.X_OK):
        raise SystemExit("Python executable is not executable: {0}".format(python_bin))
    runtime = validate_runtime_matches_host(python_bin)
    source_root, source = prepare_source(args.source_revision, build_root)

    python_root = python_bin.parent.parent
    shutil.copytree(python_root, dist_root / "python", symlinks=True)
    bundle_python = dist_root / "python" / "bin" / python_bin.name
    python3_link = dist_root / "python" / "bin" / "python3"
    if not python3_link.exists():
        python3_link.symlink_to(bundle_python.name)

    ensure_pip(bundle_python)
    install_application(bundle_python, dist_root / "site-packages", args.wheelhouse, source_root)
    packages = describe_application(bundle_python, dist_root / "site-packages")
    metadata = write_bundle_metadata(dist_root, runtime, source, packages)
    write_launcher(dist_root)
    write_readme(dist_root)
    # Exercise the relocated entrypoint without a configuration or endpoint call.
    run([str(dist_root / "landingzones"), "--help"], cwd=dist_root.parent)
    run([str(dist_root / "landingzones"), "transfer", "run", "--help"], cwd=dist_root.parent)
    archive_path = create_tarball(dist_root, args.archive_name)
    manifest_path = write_artifact_manifest(archive_path, dist_root, metadata)

    print(dist_root)
    print(archive_path)
    print(manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
