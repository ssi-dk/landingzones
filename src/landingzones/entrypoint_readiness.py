#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Entry-point readiness helpers for producer-visible runs."""

from dataclasses import dataclass
import argparse
import hashlib
import json
import os
import re
import sys
import time


FINGERPRINT_MODE_PATH_SIZE_MTIME = "path_size_mtime"
LANDING_ZONES_STATE_DIR_NAMES = (
    ".landing_zones",
    ".landing_zones_readiness",
    ".staging",
)
STATE_FILENAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True)
class SourceInventoryFingerprint:
    """Stable summary of the visible producer-owned files in one run."""

    mode: str
    digest: str
    file_count: int
    total_bytes: int


@dataclass(frozen=True)
class ReadinessObservation:
    """Result of observing one producer-visible run for readiness."""

    run_name: str
    eligible: bool
    status: str
    stable_observations: int
    first_seen: int
    last_seen: int
    last_changed: int
    fingerprint: SourceInventoryFingerprint


def compute_source_inventory_fingerprint(
    run_dir,
    mode=FINGERPRINT_MODE_PATH_SIZE_MTIME,
):
    """Fingerprint relative file paths, sizes, and mtimes for a run directory."""
    if mode != FINGERPRINT_MODE_PATH_SIZE_MTIME:
        raise ValueError("Unsupported readiness fingerprint mode: {0}".format(mode))

    root = os.fspath(run_dir)
    entries = []
    total_bytes = 0

    for current_dir, dir_names, file_names in os.walk(root):
        dir_names[:] = sorted(
            name for name in dir_names
            if name not in LANDING_ZONES_STATE_DIR_NAMES
        )
        for file_name in sorted(file_names):
            path = os.path.join(current_dir, file_name)
            stat_result = os.stat(path, follow_symlinks=False)
            relative_path = os.path.relpath(path, root).replace(os.sep, "/")
            total_bytes += stat_result.st_size
            entries.append(
                "{0}\t{1}\t{2}".format(
                    relative_path,
                    stat_result.st_size,
                    stat_result.st_mtime_ns,
                )
            )

    digest = hashlib.sha256()
    for entry in entries:
        digest.update(entry.encode("utf-8"))
        digest.update(b"\n")

    return SourceInventoryFingerprint(
        mode=mode,
        digest=digest.hexdigest(),
        file_count=len(entries),
        total_bytes=total_bytes,
    )


def observe_run_readiness(
    run_dir,
    state_root,
    stable_observations_required=1,
    quiet_seconds=0,
    now=None,
    fingerprint_mode=FINGERPRINT_MODE_PATH_SIZE_MTIME,
):
    """Observe a run, update durable stability state, and return eligibility."""
    required_observations = int(stable_observations_required)
    if required_observations < 1:
        raise ValueError("stable_observations_required must be at least 1")
    required_quiet_seconds = int(quiet_seconds)
    if required_quiet_seconds < 0:
        raise ValueError("quiet_seconds must be non-negative")

    observed_at = int(time.time() if now is None else now)
    root = os.fspath(run_dir)
    run_name = os.path.basename(os.path.normpath(root))
    fingerprint = compute_source_inventory_fingerprint(
        root,
        mode=fingerprint_mode,
    )

    state_path = readiness_state_path(state_root, run_name)
    previous = read_readiness_state(state_path)
    previous_digest = previous.get("fingerprint_hash")
    first_seen = int(previous.get("first_seen", observed_at))

    if previous_digest == fingerprint.digest:
        stable_observations = int(previous.get("stable_observations", 0)) + 1
        last_changed = int(previous.get("last_changed", observed_at))
    else:
        stable_observations = 1
        last_changed = observed_at

    quiet_elapsed = observed_at - last_changed >= required_quiet_seconds
    eligible = stable_observations >= required_observations and quiet_elapsed
    if eligible:
        status = "eligible"
    elif stable_observations >= required_observations:
        status = "waiting_for_quiet_period"
    else:
        status = "waiting_for_stability"

    write_readiness_state(
        state_path,
        {
            "run_name": run_name,
            "first_seen": first_seen,
            "last_seen": observed_at,
            "last_changed": last_changed,
            "fingerprint_hash": fingerprint.digest,
            "fingerprint_mode": fingerprint.mode,
            "file_count": fingerprint.file_count,
            "total_bytes": fingerprint.total_bytes,
            "stable_observations": stable_observations,
            "status": status,
        },
    )

    return ReadinessObservation(
        run_name=run_name,
        eligible=eligible,
        status=status,
        stable_observations=stable_observations,
        first_seen=first_seen,
        last_seen=observed_at,
        last_changed=last_changed,
        fingerprint=fingerprint,
    )


def readiness_state_path(state_root, run_name):
    """Return the durable state path for a run name."""
    sanitized = STATE_FILENAME_PATTERN.sub("_", str(run_name)).strip("._-")
    if not sanitized:
        sanitized = hashlib.sha256(str(run_name).encode("utf-8")).hexdigest()
    return os.path.join(os.fspath(state_root), "{0}.json".format(sanitized))


def read_readiness_state(path):
    """Read a durable readiness state file if it exists."""
    if not os.path.exists(path):
        return {}
    with open(path, "r") as handle:
        return json.load(handle)


def write_readiness_state(path, state):
    """Write durable readiness state through an atomic rename."""
    directory = os.path.dirname(path)
    if directory and not os.path.isdir(directory):
        os.makedirs(directory)
    tmp_path = "{0}.tmp".format(path)
    with open(tmp_path, "w") as handle:
        json.dump(state, handle, sort_keys=True)
        handle.write("\n")
    os.replace(tmp_path, path)


def main(argv=None):
    """Run entry-point readiness helpers from generated shell scripts."""
    parser = argparse.ArgumentParser(
        description="Observe Landing Zones entry-point readiness.",
    )
    subparsers = parser.add_subparsers(dest="command")
    observe_parser = subparsers.add_parser("observe")
    observe_parser.add_argument("--run-dir", required=True)
    observe_parser.add_argument("--state-root", required=True)
    observe_parser.add_argument("--stable-observations", type=int, default=1)
    observe_parser.add_argument("--quiet-seconds", type=int, default=0)
    observe_parser.add_argument(
        "--fingerprint-mode",
        default=FINGERPRINT_MODE_PATH_SIZE_MTIME,
    )

    args = parser.parse_args(argv)
    if args.command != "observe":
        parser.error("a command is required")

    result = observe_run_readiness(
        args.run_dir,
        args.state_root,
        stable_observations_required=args.stable_observations,
        quiet_seconds=args.quiet_seconds,
        fingerprint_mode=args.fingerprint_mode,
    )
    rows = (
        ("status", result.status),
        ("eligible", "1" if result.eligible else "0"),
        ("stable_observations", result.stable_observations),
        ("first_seen", result.first_seen),
        ("last_seen", result.last_seen),
        ("last_changed", result.last_changed),
        ("fingerprint_hash", result.fingerprint.digest),
        ("file_count", result.fingerprint.file_count),
        ("total_bytes", result.fingerprint.total_bytes),
    )
    for key, value in rows:
        sys.stdout.write("{0}\t{1}\n".format(key, value))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
