#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build a relocatable Landing Zones bundle from python-build-standalone."""

import argparse
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUILD_ROOT = APP_ROOT / "packaging" / "build" / "python-standalone"
DEFAULT_DIST_ROOT = APP_ROOT / "packaging" / "dist" / "landingzones-standalone"


def run(command, **kwargs):
    """Run a command, failing with the child process exit code."""
    print("+ {0}".format(" ".join(str(part) for part in command)))
    subprocess.run(command, check=True, **kwargs)


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


def install_application(python_bin, site_packages, wheelhouse):
    """Install Landing Zones and dependencies into the bundle site-packages."""
    site_packages.mkdir(parents=True, exist_ok=True)
    command = [str(python_bin), "-m", "pip", "install", "--target", str(site_packages)]
    if wheelhouse:
        command.extend(["--no-index", "--find-links", wheelhouse])
    command.append(str(APP_ROOT))
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
  ./landingzones --config config/config.yaml build
  ./landingzones --config config/config.yaml validate deployment

This bundle carries Python and Python packages only. The target machine still
needs system tools used by generated transfer scripts: rsync, ssh, flock, curl,
and cron.
"""
    )


def create_tarball(dist_root):
    """Create a tar.gz archive beside the bundle directory."""
    archive_path = dist_root.with_suffix(dist_root.suffix + ".tar.gz")
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

    python_root = python_bin.parent.parent
    shutil.copytree(python_root, dist_root / "python", symlinks=True)
    bundle_python = dist_root / "python" / "bin" / python_bin.name
    python3_link = dist_root / "python" / "bin" / "python3"
    if not python3_link.exists():
        python3_link.symlink_to(bundle_python.name)

    ensure_pip(bundle_python)
    install_application(bundle_python, dist_root / "site-packages", args.wheelhouse)
    write_launcher(dist_root)
    write_readme(dist_root)
    archive_path = create_tarball(dist_root)

    print(dist_root)
    print(archive_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
