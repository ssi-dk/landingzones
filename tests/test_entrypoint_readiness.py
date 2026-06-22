#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for entry-point readiness decisions."""

import os

from landingzones.entrypoint_readiness import (
    compute_source_inventory_fingerprint,
    observe_run_readiness,
)


def test_source_inventory_fingerprint_ignores_landing_zones_state(tmp_path):
    """Consumer-owned state should not make a producer run appear unstable."""
    run_dir = tmp_path / "RunA"
    nested_dir = run_dir / "nested"
    nested_dir.mkdir(parents=True)
    payload = run_dir / "payload.txt"
    nested_payload = nested_dir / "data.bin"
    payload.write_text("payload")
    nested_payload.write_bytes(b"abc")
    os.utime(payload, ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000))
    os.utime(nested_payload, ns=(1_700_000_001_000_000_000, 1_700_000_001_000_000_000))

    before_state = compute_source_inventory_fingerprint(run_dir)

    state_dir = run_dir / ".landing_zones"
    state_dir.mkdir()
    (state_dir / "readiness.tsv").write_text("stable_observations\t1\n")

    after_state = compute_source_inventory_fingerprint(run_dir)
    assert after_state == before_state

    payload.write_text("payload changed")
    os.utime(payload, ns=(1_700_000_002_000_000_000, 1_700_000_002_000_000_000))

    after_payload_change = compute_source_inventory_fingerprint(run_dir)
    assert after_payload_change.digest != before_state.digest
    assert after_payload_change.file_count == 2


def test_run_becomes_eligible_after_repeated_stable_observations(tmp_path):
    """Durable state should let readiness accumulate across invocations."""
    run_dir = tmp_path / "RunA"
    run_dir.mkdir()
    payload = run_dir / "payload.txt"
    payload.write_text("payload")
    os.utime(payload, ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000))
    state_root = tmp_path / ".landing_zones_readiness"

    first = observe_run_readiness(
        run_dir,
        state_root,
        stable_observations_required=2,
        now=100,
    )
    second = observe_run_readiness(
        run_dir,
        state_root,
        stable_observations_required=2,
        now=200,
    )

    assert first.eligible is False
    assert first.status == "waiting_for_stability"
    assert first.stable_observations == 1

    assert second.eligible is True
    assert second.status == "eligible"
    assert second.stable_observations == 2
    assert second.first_seen == 100
    assert second.last_seen == 200
    assert second.last_changed == 100


def test_run_waits_for_configured_quiet_period(tmp_path):
    """Recently changed runs should wait until the quiet period has elapsed."""
    run_dir = tmp_path / "RunA"
    run_dir.mkdir()
    payload = run_dir / "payload.txt"
    payload.write_text("payload")
    os.utime(payload, ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000))
    state_root = tmp_path / ".landing_zones_readiness"

    first = observe_run_readiness(
        run_dir,
        state_root,
        stable_observations_required=1,
        quiet_seconds=60,
        now=100,
    )
    second = observe_run_readiness(
        run_dir,
        state_root,
        stable_observations_required=1,
        quiet_seconds=60,
        now=160,
    )

    assert first.eligible is False
    assert first.status == "waiting_for_quiet_period"
    assert second.eligible is True
    assert second.status == "eligible"
