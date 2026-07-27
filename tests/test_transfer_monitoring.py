#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Integration tests for database-backed Transfer Event monitoring."""

import csv
from datetime import datetime, timezone
import json
import os
import shutil
import subprocess
import uuid

import pytest

from landingzones import generate_cron_files as gcf
from landingzones.monitoring import (
    UnsupportedEventSpool,
    ingest_event_spool,
    query_run_detail,
    query_run_summaries,
    sync_transfer_definitions,
)
from landingzones.monitoring_service import MonitoringApplication
from landingzones.transfer_definitions import TransferDefinition
from landingzones.transfer_events import (
    EVENT_HEADER,
    create_transfer_event,
    event_to_tsv_row,
)


HAS_RSYNC = shutil.which("rsync") is not None
HAS_FLOCK = shutil.which("flock") is not None
HAS_UUIDGEN = shutil.which("uuidgen") is not None


def run_generated_transfer(
    tmp_path,
    transfer,
    env_overrides=None,
    config_overrides=None,
):
    """Execute one generated runtime against local source/destination fixtures."""
    managed_root = tmp_path / "managed"
    snapshot = gcf.config.snapshot_state()
    config_values = {
        "output_dir": str(tmp_path / "output"),
        "rit_managed_locations": {"server1": str(managed_root)},
        "rit_managed_folder_structure": {
            "sh_output": "scripts",
            "crontabs": "crontab.d",
            "log": "log",
            "flock": "flock",
        },
        "flock_paths": {"server1": shutil.which("flock") or "/usr/bin/flock"},
    }
    config_values.update(config_overrides or {})
    gcf.config.load_config(
        **config_values
    )
    try:
        script = gcf.generate_script_content(transfer)
    finally:
        gcf.config.restore_state(snapshot)
    script_path = tmp_path / "transfer.sh"
    script_path.write_text(script)
    script_path.chmod(0o755)
    environment = dict(os.environ)
    if env_overrides:
        environment.update(env_overrides)
    result = subprocess.run(
        [str(script_path)],
        cwd=str(tmp_path),
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    return result, managed_root


def test_ingestion_defers_partial_rows_and_replays_events_idempotently(tmp_path):
    """Checkpointed ingestion should consume complete events exactly once."""
    database_url = "sqlite:///{0}".format(tmp_path / "monitoring.sqlite")
    spool_path = tmp_path / "events.tsv"
    run_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    started = create_transfer_event(
        transfer_identifier="stage_lab",
        system="server1",
        runtime_id="server1_prod.user1",
        execution_user="user1",
        status="started",
        phase="transfer",
        run_id=run_id,
        attempt_id=attempt_id,
    )
    delivered = create_transfer_event(
        transfer_identifier="stage_lab",
        system="server1",
        runtime_id="server1_prod.user1",
        execution_user="user1",
        status="delivered",
        phase="promotion",
        run_id=run_id,
        attempt_id=attempt_id,
    )
    spool_path.write_text(
        EVENT_HEADER
        + "\n"
        + event_to_tsv_row(started)
        + "\n"
        + event_to_tsv_row(delivered)
    )

    first = ingest_event_spool(database_url, str(spool_path), spool_id="server1-spool")
    with spool_path.open("a") as handle:
        handle.write("\n")
    second = ingest_event_spool(database_url, str(spool_path), spool_id="server1-spool")
    replay = ingest_event_spool(database_url, str(spool_path), spool_id="replay-spool")
    detail = query_run_detail(database_url, run_id)

    assert first.inserted == 1
    assert first.deferred_bytes > 0
    assert second.inserted == 1
    assert second.deferred_bytes == 0
    assert replay.inserted == 0
    assert replay.duplicates == 2
    assert [event["status"] for event in detail["timeline"]] == [
        "started",
        "delivered",
    ]
    assert detail["timeline"][0]["message"] is None
    assert detail["timeline"][0]["exit_code"] is None


def test_monitoring_keeps_definitions_separate_and_derives_current_run_state(tmp_path):
    """Expected routes and operational history should combine only at query time."""
    database_url = "sqlite:///{0}".format(tmp_path / "monitoring.sqlite")
    spool_path = tmp_path / "events.tsv"
    run_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    events = [
        create_transfer_event(
            transfer_identifier="stage_lab",
            system="server1",
            runtime_id="server1_prod.user1",
            execution_user="user1",
            status="started",
            phase="transfer",
            run_id=run_id,
            attempt_id=attempt_id,
            tags="lab,heartbeat",
        ),
        create_transfer_event(
            transfer_identifier="stage_lab",
            system="server1",
            runtime_id="server1_prod.user1",
            execution_user="user1",
            status="delivered",
            phase="promotion",
            run_id=run_id,
            attempt_id=attempt_id,
            tags="lab,heartbeat",
        ),
        create_transfer_event(
            transfer_identifier="stage_lab",
            system="server1",
            runtime_id="server1_prod.user1",
            execution_user="user1",
            status="failed",
            phase="cleanup",
            run_id=run_id,
            attempt_id=attempt_id,
            tags="lab,heartbeat",
            reason_code="permission_denied",
            message="source cleanup failed",
            directory="/source/alpha",
        ),
    ]
    spool_path.write_text(
        EVENT_HEADER
        + "\n"
        + "\n".join(event_to_tsv_row(event) for event in events)
        + "\n"
    )
    definitions = [
        TransferDefinition(
            identifier="stage_lab",
            runtime_id="server1_prod.user1",
            system="server1",
            user="user1",
            source="/source/*",
            destination="/staging/",
            tags=("heartbeat", "lab"),
        ),
        TransferDefinition(
            identifier="promote_lab",
            runtime_id="server1_prod.user1",
            system="server1",
            user="user1",
            source="/staging/*",
            destination="/final/",
            frequency="*/15 * * * *",
            tags=("lab",),
            enabled=False,
            is_end_point=True,
        ),
    ]

    sync_transfer_definitions(database_url, definitions)
    ingest_event_spool(database_url, str(spool_path))
    summaries = query_run_summaries(
        database_url,
        runtime_ids=["server1_prod.user1"],
        tags=["heartbeat"],
    )
    all_summaries = query_run_summaries(database_url)
    html_status, _, html_body = MonitoringApplication(database_url).respond(
        "/",
        "",
    )

    assert len(summaries) == 1
    assert summaries[0]["run_id"] == run_id
    assert summaries[0]["state"] == "delivered with cleanup failed"
    assert summaries[0]["attempt_count"] == 1
    assert summaries[0]["directory"] == "/source/alpha"
    assert summaries[0]["current_status"] == "failed"
    assert summaries[0]["current_phase"] == "cleanup"
    assert summaries[0]["latest_failure_phase"] == "cleanup"
    assert summaries[0]["reason_code"] == "permission_denied"
    assert summaries[0]["message"] == "source cleanup failed"
    assert [summary["state"] for summary in all_summaries] == [
        "delivered with cleanup failed",
        "configured but never observed",
    ]
    assert all_summaries[1]["frequency"] == "*/15 * * * *"
    assert all_summaries[1]["enabled"] is False
    assert html_status == 200
    assert "<th>Directory</th>" in html_body
    assert "<th>Progress / step</th>" in html_body
    assert "/source/alpha" in html_body
    assert "delivered with cleanup failed at cleanup" in html_body
    assert "cleanup: permission_denied" in html_body
    assert "source cleanup failed" in html_body


def test_run_summaries_default_to_most_recent_last_event(tmp_path):
    """The monitoring list should show the most recently active run first."""
    database_url = "sqlite:///{0}".format(tmp_path / "monitoring.sqlite")
    spool_path = tmp_path / "events.tsv"
    older_run_id = str(uuid.uuid4())
    newer_run_id = str(uuid.uuid4())
    events = [
        create_transfer_event(
            transfer_identifier="stage_lab",
            system="server1",
            runtime_id="server1_prod.user1",
            execution_user="user1",
            status="started",
            phase="transfer",
            run_id=older_run_id,
            attempt_id=str(uuid.uuid4()),
            event_time_utc="2026-07-27T09:00:00Z",
        ),
        create_transfer_event(
            transfer_identifier="stage_lab",
            system="server1",
            runtime_id="server1_prod.user1",
            execution_user="user1",
            status="started",
            phase="transfer",
            run_id=newer_run_id,
            attempt_id=str(uuid.uuid4()),
            event_time_utc="2026-07-27T11:00:00Z",
        ),
    ]
    spool_path.write_text(
        EVENT_HEADER
        + "\n"
        + "\n".join(event_to_tsv_row(event) for event in events)
        + "\n"
    )

    ingest_event_spool(database_url, str(spool_path))

    summaries = query_run_summaries(database_url)

    assert [summary["run_id"] for summary in summaries] == [
        newer_run_id,
        older_run_id,
    ]


def test_run_state_uses_route_delivery_flow_position_and_run_creation_age(tmp_path):
    """Cross-hop failures and unfinished age should reflect the whole run."""
    database_url = "sqlite:///{0}".format(tmp_path / "monitoring.sqlite")
    definitions = [
        TransferDefinition(
            identifier="stage_lab",
            runtime_id="server1_prod.user1",
            system="server1",
            user="user1",
            source="/source/*",
            destination="user2@server2:/staging/",
        ),
        TransferDefinition(
            identifier="promote_lab",
            runtime_id="server2_prod.user2",
            system="server2",
            user="user2",
            source="/staging/*",
            destination="/final/",
            is_end_point=True,
        ),
    ]
    between_hops_run_id = str(uuid.uuid4())
    failed_run_id = str(uuid.uuid4())
    failed_stage_attempt_id = str(uuid.uuid4())
    failed_promote_attempt_id = str(uuid.uuid4())
    created_at = "2026-07-25T12:00:00Z"
    events = [
        create_transfer_event(
            transfer_identifier="stage_lab",
            system="server1",
            runtime_id="server1_prod.user1",
            execution_user="user1",
            status="completed",
            phase="cleanup",
            run_id=between_hops_run_id,
            attempt_id=str(uuid.uuid4()),
            event_time_utc="2026-07-25T13:00:00Z",
            created_at_utc=created_at,
        ),
        create_transfer_event(
            transfer_identifier="stage_lab",
            system="server1",
            runtime_id="server1_prod.user1",
            execution_user="user1",
            status="completed",
            phase="cleanup",
            run_id=failed_run_id,
            attempt_id=failed_stage_attempt_id,
            event_time_utc="2026-07-25T14:00:00Z",
            created_at_utc=created_at,
        ),
        create_transfer_event(
            transfer_identifier="promote_lab",
            system="server2",
            runtime_id="server2_prod.user2",
            execution_user="user2",
            status="started",
            phase="transfer",
            run_id=failed_run_id,
            attempt_id=failed_promote_attempt_id,
            event_time_utc="2026-07-27T10:00:00Z",
            created_at_utc=created_at,
        ),
        create_transfer_event(
            transfer_identifier="promote_lab",
            system="server2",
            runtime_id="server2_prod.user2",
            execution_user="user2",
            status="failed",
            phase="transfer",
            run_id=failed_run_id,
            attempt_id=failed_promote_attempt_id,
            event_time_utc="2026-07-27T11:00:00Z",
            created_at_utc=created_at,
            reason_code="rsync_failed",
        ),
    ]
    spool_path = tmp_path / "events.tsv"
    spool_path.write_text(
        EVENT_HEADER
        + "\n"
        + "\n".join(event_to_tsv_row(event) for event in events)
        + "\n"
    )

    sync_transfer_definitions(database_url, definitions)
    ingest_event_spool(database_url, str(spool_path))
    summaries = {
        summary["run_id"]: summary
        for summary in query_run_summaries(
            database_url,
            now=datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc),
        )
    }

    assert summaries[between_hops_run_id]["state"] == "in progress"
    assert summaries[failed_run_id]["state"] == "failed before delivery"
    assert summaries[failed_run_id]["latest_failure_phase"] == "transfer"
    assert summaries[failed_run_id]["age_seconds"] == 172800


def test_full_definition_sync_removes_routes_no_longer_configured(tmp_path):
    """A full catalog refresh should not retain stale current definitions."""
    database_url = "sqlite:///{0}".format(tmp_path / "monitoring.sqlite")
    retained = TransferDefinition(
        identifier="retained",
        runtime_id="server1_prod.user1",
        system="server1",
        user="user1",
        source="/source/retained/*",
        destination="/destination/retained/",
    )
    removed = TransferDefinition(
        identifier="removed",
        runtime_id="server2_prod.user2",
        system="server2",
        user="user2",
        source="/source/removed/*",
        destination="/destination/removed/",
    )

    sync_transfer_definitions(database_url, [retained, removed])
    sync_transfer_definitions(
        database_url,
        [],
        scope_runtime_ids=["server2_prod.user2"],
    )

    assert [
        summary["transfer_identifier"]
        for summary in query_run_summaries(database_url)
    ] == ["retained"]

    sync_transfer_definitions(database_url, [])

    assert query_run_summaries(database_url) == []


@pytest.mark.skipif(
    not (HAS_RSYNC and HAS_FLOCK and HAS_UUIDGEN),
    reason="requires rsync, flock, and uuidgen",
)
def test_generated_runtime_emits_one_ingestible_event_lifecycle(tmp_path):
    """Runtime and portable outputs should carry the same immutable event facts."""
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    run_dir = source_root / "Run42"
    run_dir.mkdir(parents=True)
    destination_root.mkdir()
    (run_dir / "payload.txt").write_text("payload")
    transfer = {
        "identifiers": "stage_lab",
        "runtime_id": "server1_prod.user1",
        "system": "server1",
        "users": "user1",
        "source": str(source_root / "*"),
        "source_port": "",
        "destination": str(destination_root) + "/",
        "destination_port": "",
        "rsync_options": "",
        "io_nice": "",
        "log_file": str(tmp_path / "stage_lab.log"),
        "flock_file": str(tmp_path / "stage_lab.lock"),
        "frequency": "",
        "tags": "heartbeat,lab",
    }

    result, managed_root = run_generated_transfer(tmp_path, transfer)
    spool_path = managed_root / "log" / "Landing_Zone_server1.transfers.tsv"
    portable_path = (
        destination_root
        / "Run42"
        / ".landing_zones"
        / "landingzone-transfer-events.tsv"
    )
    with spool_path.open(newline="") as handle:
        spool_rows = list(csv.DictReader(handle, delimiter="\t"))
    with portable_path.open(newline="") as handle:
        portable_rows = list(csv.DictReader(handle, delimiter="\t"))
    database_url = "sqlite:///{0}".format(tmp_path / "monitoring.sqlite")
    ingest_event_spool(database_url, str(spool_path))
    summaries = query_run_summaries(database_url)

    assert result.returncode == 0, result.stderr
    assert spool_path.read_text().splitlines()[0] == EVENT_HEADER
    assert [row["status"] for row in spool_rows] == [
        "started",
        "delivered",
        "completed",
    ]
    assert [row["phase"] for row in spool_rows] == [
        "transfer",
        "promotion",
        "cleanup",
    ]
    assert len({row["event_id"] for row in spool_rows}) == 3
    assert len({row["run_id"] for row in spool_rows}) == 1
    assert len({row["attempt_id"] for row in spool_rows}) == 1
    assert [row["event_id"] for row in portable_rows] == [
        row["event_id"] for row in spool_rows
    ]
    assert portable_rows == spool_rows
    assert summaries[0]["state"] == "completed"
    assert summaries[0]["attempt_count"] == 1


@pytest.mark.skipif(
    not (HAS_FLOCK and HAS_UUIDGEN),
    reason="requires flock and uuidgen",
)
def test_generated_runtime_suppresses_unchanged_readiness_without_an_attempt(tmp_path):
    """Repeated polling should retain one waiting transition under a stable run ID."""
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    run_dir = source_root / "RunWaiting"
    run_dir.mkdir(parents=True)
    destination_root.mkdir()
    (run_dir / "payload.txt").write_text("payload")
    transfer = {
        "identifiers": "stage_lab",
        "runtime_id": "server1_prod.user1",
        "system": "server1",
        "users": "user1",
        "source": str(source_root / "*"),
        "source_port": "",
        "destination": str(destination_root) + "/",
        "destination_port": "",
        "rsync_options": "",
        "io_nice": "",
        "log_file": str(tmp_path / "stage_lab.log"),
        "flock_file": str(tmp_path / "stage_lab.lock"),
        "frequency": "",
        "flow_group": "lab_flow",
        "is_entry_point": "TRUE",
        "readiness_policy": "stable_snapshot",
        "readiness_stable_observations": "3",
        "readiness_quiet_seconds": "0",
    }

    first, managed_root = run_generated_transfer(tmp_path, transfer)
    second, _ = run_generated_transfer(tmp_path, transfer)
    spool_path = managed_root / "log" / "Landing_Zone_server1.transfers.tsv"
    metadata_path = (
        run_dir / ".landing_zones" / "landingzone-run-metadata.tsv"
    )
    with spool_path.open(newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert len(rows) == 1
    assert rows[0]["status"] == "waiting"
    assert rows[0]["phase"] == "readiness"
    assert uuid.UUID(rows[0]["run_id"]).version == 4
    assert rows[0]["attempt_id"] == ""
    assert "run_id\t{0}".format(rows[0]["run_id"]) in metadata_path.read_text()
    assert not (destination_root / "RunWaiting").exists()


@pytest.mark.skipif(
    not (HAS_RSYNC and HAS_FLOCK and HAS_UUIDGEN),
    reason="requires rsync, flock, and uuidgen",
)
def test_cleanup_failure_recovers_without_retransferring_the_delivered_run(tmp_path):
    """Cleanup-only recovery should retain run identity and mint a new attempt."""
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    run_dir = source_root / "RunCleanup"
    run_dir.mkdir(parents=True)
    destination_root.mkdir()
    (run_dir / "payload.txt").write_text("payload")
    transfer = {
        "identifiers": "stage_lab",
        "runtime_id": "server1_prod.user1",
        "system": "server1",
        "users": "user1",
        "source": str(source_root / "*"),
        "source_port": "",
        "destination": str(destination_root) + "/",
        "destination_port": "",
        "rsync_options": "",
        "io_nice": "",
        "log_file": str(tmp_path / "stage_lab.log"),
        "flock_file": str(tmp_path / "stage_lab.lock"),
        "frequency": "",
    }
    source_root.chmod(0o555)
    try:
        first, managed_root = run_generated_transfer(tmp_path, transfer)
        spool_path = managed_root / "log" / "Landing_Zone_server1.transfers.tsv"
        database_url = "sqlite:///{0}".format(tmp_path / "monitoring.sqlite")
        ingest_event_spool(database_url, str(spool_path))
        failed_summary = query_run_summaries(database_url)[0]

        assert first.returncode == 0, first.stderr
        assert failed_summary["state"] == "delivered with cleanup failed"
        assert run_dir.exists()
        assert not (run_dir / "payload.txt").exists()
        assert (run_dir / ".landing_zones").exists()

        (run_dir / "should-not-transfer.txt").write_text("cleanup only")
        source_root.chmod(0o755)
        second, _ = run_generated_transfer(tmp_path, transfer)
        ingest_event_spool(database_url, str(spool_path))
    finally:
        source_root.chmod(0o755)

    with spool_path.open(newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    destination_run = destination_root / "RunCleanup"

    assert second.returncode == 0, second.stderr
    assert [row["status"] for row in rows] == [
        "started",
        "delivered",
        "failed",
        "started",
        "completed",
    ]
    assert [row["phase"] for row in rows] == [
        "transfer",
        "promotion",
        "cleanup",
        "cleanup",
        "cleanup",
    ]
    assert len({row["run_id"] for row in rows}) == 1
    assert len({row["attempt_id"] for row in rows}) == 2
    assert run_dir.exists()
    assert (run_dir / "should-not-transfer.txt").read_text() == "cleanup only"
    assert (destination_run / "payload.txt").read_text() == "payload"
    assert not (destination_run / "should-not-transfer.txt").exists()
    assert query_run_summaries(database_url)[0]["state"] == "completed"


@pytest.mark.skipif(
    not (HAS_RSYNC and HAS_FLOCK and HAS_UUIDGEN),
    reason="requires rsync, flock, and uuidgen",
)
def test_source_cleanup_preserves_files_excluded_from_transfer(tmp_path):
    """Completion must not delete source files excluded by transfer options."""
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    run_dir = source_root / "RunExcluded"
    run_dir.mkdir(parents=True)
    destination_root.mkdir()
    (run_dir / "payload.txt").write_text("payload")
    (run_dir / "excluded.txt").write_text("retain")
    transfer = {
        "identifiers": "stage_lab",
        "runtime_id": "server1_prod.user1",
        "system": "server1",
        "users": "user1",
        "source": str(source_root / "*"),
        "source_port": "",
        "destination": str(destination_root) + "/",
        "destination_port": "",
        "rsync_options": "--exclude=excluded.txt",
        "io_nice": "",
        "log_file": str(tmp_path / "stage_lab.log"),
        "flock_file": str(tmp_path / "stage_lab.lock"),
        "frequency": "",
    }

    result, managed_root = run_generated_transfer(tmp_path, transfer)
    spool_path = managed_root / "log" / "Landing_Zone_server1.transfers.tsv"
    with spool_path.open(newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))

    assert result.returncode == 0, result.stderr
    assert [row["status"] for row in rows] == [
        "started",
        "delivered",
        "completed",
    ]
    assert (destination_root / "RunExcluded" / "payload.txt").read_text() == "payload"
    assert not (destination_root / "RunExcluded" / "excluded.txt").exists()
    assert (run_dir / "excluded.txt").read_text() == "retain"


@pytest.mark.skipif(
    not (HAS_RSYNC and HAS_FLOCK and HAS_UUIDGEN),
    reason="requires rsync, flock, and uuidgen",
)
def test_pre_delivery_cleanup_preflight_failure_retries_transfer_not_cleanup(tmp_path):
    """A cleanup-phase preflight failure is not post-delivery recovery."""
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    run_dir = source_root / "RunPreflight"
    nested_dir = run_dir / "nested"
    nested_dir.mkdir(parents=True)
    destination_root.mkdir()
    (nested_dir / "payload.txt").write_text("payload")
    transfer = {
        "identifiers": "stage_lab",
        "runtime_id": "server1_prod.user1",
        "system": "server1",
        "users": "user1",
        "source": str(source_root / "*"),
        "source_port": "",
        "destination": str(destination_root) + "/",
        "destination_port": "",
        "rsync_options": "",
        "io_nice": "",
        "log_file": str(tmp_path / "stage_lab.log"),
        "flock_file": str(tmp_path / "stage_lab.lock"),
        "frequency": "",
    }

    nested_dir.chmod(0o555)
    try:
        first, managed_root = run_generated_transfer(tmp_path, transfer)
        nested_dir.chmod(0o755)
        second, _ = run_generated_transfer(tmp_path, transfer)
    finally:
        if nested_dir.exists():
            nested_dir.chmod(0o755)

    spool_path = managed_root / "log" / "Landing_Zone_server1.transfers.tsv"
    with spool_path.open(newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert [row["status"] for row in rows] == [
        "started",
        "failed",
        "started",
        "delivered",
        "completed",
    ]
    assert rows[1]["phase"] == "cleanup"
    assert rows[2]["phase"] == "transfer"
    assert len({row["run_id"] for row in rows}) == 1
    assert len({row["attempt_id"] for row in rows}) == 2
    assert (
        destination_root / "RunPreflight" / "nested" / "payload.txt"
    ).read_text() == "payload"


@pytest.mark.skipif(
    not (HAS_FLOCK and HAS_UUIDGEN),
    reason="requires flock and uuidgen",
)
def test_transfer_failure_retains_reliable_phase_reason_and_exit_code(tmp_path):
    """A failed route execution should expose diagnostics without claiming delivery."""
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_rsync = fake_bin / "rsync"
    fake_rsync.write_text(
        "#!/bin/sh\n"
        "if [ \"${1:-}\" = \"--dry-run\" ]; then exit 0; fi\n"
        "printf 'controlled rsync failure\\n' >&2\n"
        "exit 23\n"
    )
    fake_rsync.chmod(0o755)
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    run_dir = source_root / "RunFailed"
    run_dir.mkdir(parents=True)
    destination_root.mkdir()
    (run_dir / "payload.txt").write_text("payload")
    transfer = {
        "identifiers": "stage_lab",
        "runtime_id": "server1_prod.user1",
        "system": "server1",
        "users": "user1",
        "source": str(source_root / "*"),
        "source_port": "",
        "destination": str(destination_root) + "/",
        "destination_port": "",
        "rsync_options": "",
        "io_nice": "",
        "log_file": str(tmp_path / "stage_lab.log"),
        "flock_file": str(tmp_path / "stage_lab.lock"),
        "frequency": "",
    }
    path = os.pathsep.join((str(fake_bin), os.environ.get("PATH", "")))

    result, managed_root = run_generated_transfer(
        tmp_path,
        transfer,
        env_overrides={"PATH": path},
    )
    spool_path = managed_root / "log" / "Landing_Zone_server1.transfers.tsv"
    with spool_path.open(newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))

    assert result.returncode == 0, result.stderr
    assert [row["status"] for row in rows] == ["started", "failed"]
    assert rows[-1]["phase"] == "transfer"
    assert rows[-1]["reason_code"] == "rsync_failed"
    assert rows[-1]["exit_code"] == "23"
    assert "controlled rsync failure" in rows[-1]["message"]
    assert not (destination_root / "RunFailed").exists()


def test_monitoring_api_queries_current_database_state_on_every_request(tmp_path):
    """A reload should observe database changes without regenerating an HTML file."""
    database_url = "sqlite:///{0}".format(tmp_path / "monitoring.sqlite")
    definition = TransferDefinition(
        identifier="stage_lab",
        runtime_id="server1_prod.user1",
        system="server1",
        user="user1",
        source="/source/*",
        destination="/destination/",
        tags=("lab",),
    )
    sync_transfer_definitions(database_url, [definition])
    application = MonitoringApplication(database_url)

    first_status, first_headers, first_body = application.respond(
        "/api/runs",
        "runtime_id=server1_prod.user1",
    )

    run_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    completed = create_transfer_event(
        transfer_identifier="stage_lab",
        system="server1",
        runtime_id="server1_prod.user1",
        execution_user="user1",
        status="completed",
        phase="cleanup",
        run_id=run_id,
        attempt_id=attempt_id,
        tags="lab",
    )
    spool_path = tmp_path / "events.tsv"
    spool_path.write_text(
        EVENT_HEADER + "\n" + event_to_tsv_row(completed) + "\n"
    )
    ingest_event_spool(database_url, str(spool_path))

    second_status, second_headers, second_body = application.respond(
        "/api/runs",
        "runtime_id=server1_prod.user1&tag=lab",
    )
    detail_status, _, detail_body = application.respond(
        "/api/runs/{0}".format(run_id),
        "",
    )
    html_status, _, html_body = application.respond("/", "")

    assert first_status == 200
    assert first_headers["Content-Type"] == "application/json; charset=utf-8"
    assert json.loads(first_body)["runs"][0]["state"] == (
        "configured but never observed"
    )
    assert second_status == 200
    assert second_headers["Content-Type"] == "application/json; charset=utf-8"
    assert json.loads(second_body)["runs"][0]["state"] == "completed"
    assert detail_status == 200
    assert json.loads(detail_body)["timeline"][0]["event_id"] == completed.event_id
    assert html_status == 200
    assert "<th>Last event</th>" in html_body
    assert "<th>Attempts</th>" in html_body


def test_live_ingestor_rejects_an_unversioned_spool_without_guessing(tmp_path):
    """Schema-0 input should require an explicit cutover instead of live parsing."""
    spool_path = tmp_path / "legacy.tsv"
    spool_path.write_text(
        "event_time_utc\ttransfer_identifier\tsystem\tstatus\n"
        "2026-07-27T09:00:00Z\tstage_lab\tserver1\tcompleted\n"
    )
    database_url = "sqlite:///{0}".format(tmp_path / "monitoring.sqlite")

    with pytest.raises(
        UnsupportedEventSpool,
        match="live ingestion requires schema version 1",
    ):
        ingest_event_spool(database_url, str(spool_path))

    assert query_run_summaries(database_url) == []


@pytest.mark.skipif(
    not (HAS_FLOCK and HAS_UUIDGEN),
    reason="requires flock and uuidgen",
)
def test_route_discovery_failure_is_visible_without_run_or_attempt_identity(tmp_path):
    """Infrastructure failure before discovery should remain a route-level event."""
    source_root = tmp_path / "missing-source"
    destination_root = tmp_path / "destination"
    destination_root.mkdir()
    transfer = {
        "identifiers": "stage_lab",
        "runtime_id": "server1_prod.user1",
        "system": "server1",
        "users": "user1",
        "source": str(source_root / "*"),
        "source_port": "",
        "destination": str(destination_root) + "/",
        "destination_port": "",
        "rsync_options": "",
        "io_nice": "",
        "log_file": str(tmp_path / "stage_lab.log"),
        "flock_file": str(tmp_path / "stage_lab.lock"),
        "frequency": "",
    }

    result, managed_root = run_generated_transfer(tmp_path, transfer)
    repeated_result, _ = run_generated_transfer(tmp_path, transfer)
    spool_path = managed_root / "log" / "Landing_Zone_server1.transfers.tsv"
    database_url = "sqlite:///{0}".format(tmp_path / "monitoring.sqlite")
    ingest_event_spool(database_url, str(spool_path))
    summary = query_run_summaries(database_url)[0]

    assert result.returncode == 0, result.stderr
    assert repeated_result.returncode == 0, repeated_result.stderr
    assert len(spool_path.read_text().splitlines()) == 2
    assert summary["run_id"] is None
    assert summary["attempt_count"] is None
    assert summary["state"] == "failed before delivery"
    assert summary["latest_failure_phase"] == "discovery"
    assert summary["reason_code"] == "source_missing"


@pytest.mark.skipif(
    not (HAS_FLOCK and HAS_UUIDGEN),
    reason="requires flock and uuidgen",
)
def test_repeated_metadata_failure_is_suppressed(tmp_path):
    """An unchanged metadata discovery failure should remain one route fact."""
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    run_dir = source_root / "RunInvalid"
    metadata_dir = run_dir / ".landing_zones"
    metadata_dir.mkdir(parents=True)
    destination_root.mkdir()
    (metadata_dir / "landingzone-run-metadata.tsv").write_text(
        "schema_version\t1\nrun_name\tRunInvalid\n"
    )
    transfer = {
        "identifiers": "stage_lab",
        "runtime_id": "server1_prod.user1",
        "system": "server1",
        "users": "user1",
        "source": str(source_root / "*"),
        "source_port": "",
        "destination": str(destination_root) + "/",
        "destination_port": "",
        "rsync_options": "",
        "io_nice": "",
        "log_file": str(tmp_path / "stage_lab.log"),
        "flock_file": str(tmp_path / "stage_lab.lock"),
        "frequency": "",
    }

    first, managed_root = run_generated_transfer(tmp_path, transfer)
    second, _ = run_generated_transfer(tmp_path, transfer)
    second_metadata_dir = source_root / "RunAlsoInvalid" / ".landing_zones"
    second_metadata_dir.mkdir(parents=True)
    (second_metadata_dir / "landingzone-run-metadata.tsv").write_text(
        "schema_version\t1\nrun_name\tRunAlsoInvalid\n"
    )
    third, _ = run_generated_transfer(tmp_path, transfer)
    spool_path = managed_root / "log" / "Landing_Zone_server1.transfers.tsv"
    with spool_path.open(newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert third.returncode == 0, third.stderr
    assert len(rows) == 2
    assert {row["directory"] for row in rows} == {
        "RunInvalid",
        "RunAlsoInvalid",
    }
    assert {row["reason_code"] for row in rows} == {"metadata_invalid"}
    assert {row["run_id"] for row in rows} == {""}
    assert {row["attempt_id"] for row in rows} == {""}


@pytest.mark.skipif(
    not (HAS_FLOCK and HAS_UUIDGEN),
    reason="requires flock and uuidgen",
)
def test_repeated_readiness_observer_failure_is_suppressed(tmp_path):
    """An unchanged readiness diagnostic should not grow event history."""
    fake_python = tmp_path / "failing-python"
    fake_python.write_text(
        "#!/bin/sh\nprintf 'controlled readiness failure\\n' >&2\nexit 7\n"
    )
    fake_python.chmod(0o755)
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    run_dir = source_root / "RunReadinessFailure"
    run_dir.mkdir(parents=True)
    destination_root.mkdir()
    (run_dir / "payload.txt").write_text("payload")
    transfer = {
        "identifiers": "stage_lab",
        "runtime_id": "server1_prod.user1",
        "system": "server1",
        "users": "user1",
        "source": str(source_root / "*"),
        "source_port": "",
        "destination": str(destination_root) + "/",
        "destination_port": "",
        "rsync_options": "",
        "io_nice": "",
        "log_file": str(tmp_path / "stage_lab.log"),
        "flock_file": str(tmp_path / "stage_lab.lock"),
        "frequency": "",
        "is_entry_point": "TRUE",
        "readiness_policy": "stable_snapshot",
    }

    first, managed_root = run_generated_transfer(
        tmp_path,
        transfer,
        env_overrides={"LANDINGZONES_PYTHON": str(fake_python)},
    )
    second, _ = run_generated_transfer(
        tmp_path,
        transfer,
        env_overrides={"LANDINGZONES_PYTHON": str(fake_python)},
    )
    spool_path = managed_root / "log" / "Landing_Zone_server1.transfers.tsv"
    with spool_path.open(newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert rows[0]["phase"] == "readiness"
    assert "controlled readiness failure" in rows[0]["message"]
    assert rows[0]["attempt_id"] == ""


@pytest.mark.skipif(
    not (HAS_RSYNC and HAS_FLOCK and HAS_UUIDGEN),
    reason="requires rsync, flock, and uuidgen",
)
def test_event_spool_write_failure_is_diagnostic_not_a_transfer_failure(tmp_path):
    """Best-effort observability should never roll back valid data movement."""
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    run_dir = source_root / "RunObservable"
    run_dir.mkdir(parents=True)
    destination_root.mkdir()
    (run_dir / "payload.txt").write_text("payload")
    blocked_spool_path = tmp_path / "spool-is-a-directory"
    blocked_spool_path.mkdir()
    transfer = {
        "identifiers": "stage_lab",
        "runtime_id": "server1_prod.user1",
        "system": "server1",
        "users": "user1",
        "source": str(source_root / "*"),
        "source_port": "",
        "destination": str(destination_root) + "/",
        "destination_port": "",
        "rsync_options": "",
        "io_nice": "",
        "log_file": str(tmp_path / "stage_lab.log"),
        "flock_file": str(tmp_path / "stage_lab.lock"),
        "frequency": "",
    }

    result, _ = run_generated_transfer(
        tmp_path,
        transfer,
        config_overrides={
            "report_transfer_log_file": str(blocked_spool_path),
        },
    )

    assert result.returncode == 0, result.stderr
    assert not run_dir.exists()
    assert (destination_root / "RunObservable" / "payload.txt").read_text() == (
        "payload"
    )
    assert "event spool append failed" in (
        tmp_path / "stage_lab.log.mini"
    ).read_text()
