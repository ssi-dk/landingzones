#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for transfer catalog loading."""

import pytest

from landingzones import generate_cron_files as gcf
from landingzones import transfer_catalog
from landingzones.table import TransferTable
from landingzones.transfer_catalog import (
    load_reporting_transfer_definitions,
    load_reporting_transfer_catalog,
    load_runtime_transfer_catalog,
    load_runtime_transfer_definitions,
)


def test_runtime_catalog_preserves_build_loading_invariants(tmp_path):
    """Runtime catalog loading keeps build-facing TSV behavior stable."""
    transfers_file = tmp_path / "transfers.tsv"
    config_file = tmp_path / "config.yaml"
    transfers_file.write_text(
        "\n".join(
            [
                "runtime_id\tenabled\tsystem\tusers\tsource\tdestination\tlog_file\tflock_file\tflow_group\ttags\tis_entry_point\tnotify_on_success",
                "local_dev.local\tTRUE\tlocal_dev\tlocal\t${SRC_ROOT}/in/\t${DST_ROOT}/out/\ttransfer.log\ttransfer.lock\tflow one\tLab, heartbeat\tyes\t1",
                "local_dev.local\tTRUE\tlocal_dev\tlocal\t${SRC_ROOT}/second/\t${DST_ROOT}/second/\ttransfer.log\ttransfer.lock\tflow one\tLAB\tfalse\tfalse",
                "other.local\tTRUE\tlocal_dev\tlocal\t${SRC_ROOT}/other/\t${DST_ROOT}/other/\tother.log\tother.lock\tflow one\tother\tfalse\tfalse",
                "disabled.local\tFALSE\tlocal_dev\tlocal\t${SRC_ROOT}/disabled/\t${DST_ROOT}/disabled/\tdisabled.log\tdisabled.lock\t\t\t\t",
                "#commented.local\tTRUE\tlocal_dev\tlocal\t${SRC_ROOT}/commented/\t${DST_ROOT}/commented/\tcommented.log\tcommented.lock\t\t\t\t",
            ]
        )
    )
    config_file.write_text(
        "transfers_file: {0}\n"
        "runtime_ids:\n"
        "  - local_dev.local\n"
        "artifact_prefix: prod server\n"
        "path_variables:\n"
        "  SRC_ROOT: {1}\n"
        "  DST_ROOT: {2}\n"
        "rit_managed_locations:\n"
        "  local_dev: {3}\n"
        "rit_managed_folder_structure:\n"
        "  log: log\n"
        "  flock: flock\n".format(
            transfers_file,
            tmp_path / "source",
            tmp_path / "destination",
            tmp_path / "managed",
        )
    )

    snapshot = gcf.config.snapshot_state()
    try:
        catalog = load_runtime_transfer_catalog(config_file=str(config_file))
    finally:
        gcf.config.restore_state(snapshot)

    assert list(catalog["identifiers"]) == ["transfer_001", "transfer_002"]
    assert list(catalog["runtime_id"]) == ["local_dev.local", "local_dev.local"]
    assert catalog.iloc[0]["source"] == str(tmp_path / "source" / "in") + "/"
    assert catalog.iloc[0]["destination"] == str(tmp_path / "destination" / "out") + "/"
    assert catalog.iloc[0]["script_name"] == "prod_server__transfer_001.sh"
    assert catalog.iloc[0]["is_entry_point"] == "TRUE"
    assert catalog.iloc[0]["notify_on_success"] == "TRUE"
    assert catalog.iloc[0]["tags"] == "heartbeat,lab"
    assert catalog.iloc[1]["is_entry_point"] == "FALSE"
    assert catalog.iloc[1]["tags"] == "lab"
    assert catalog.attrs["shared_file_pair_warnings"]
    assert catalog.attrs["shared_main_lock_warnings"]


def test_build_command_loads_transfers_through_catalog(tmp_path, monkeypatch):
    """The build command uses the transfer catalog as its loading seam."""
    transfers_file = tmp_path / "transfers.tsv"
    crontab_dir = tmp_path / "output" / "crontab.d"
    validation_dir = tmp_path / "output" / "validation_scripts"
    config_file = tmp_path / "config.yaml"
    transfers_file.write_text(
        "identifiers\truntime_id\tsystem\tusers\tsource\tdestination\tlog_file\tflock_file\n"
        "direct_parse\tignored.local\tlocal_dev\tlocal\t/direct/src/\t/direct/dst/\tdirect.log\tdirect.lock\n"
    )
    config_file.write_text(
        "transfers_file: {0}\n"
        "crontab_dir: {1}\n"
        "validation_scripts_dir: {2}\n"
        "runtime_ids:\n"
        "  - catalog_runtime\n"
        "rit_managed_locations:\n"
        "  local_dev: {3}\n"
        "rit_managed_folder_structure:\n"
        "  log: log\n"
        "  flock: flock\n"
        "  sh_output: output/scripts\n".format(
            transfers_file,
            crontab_dir,
            validation_dir,
            tmp_path / "managed",
        )
    )

    row = {
        "identifiers": "catalog_transfer",
        "runtime_id": "catalog_runtime",
        "system_user": "catalog_runtime",
        "system": "local_dev",
        "users": "local",
        "source": "/catalog/src/",
        "destination": "/catalog/dst/",
        "destination_port": "",
        "source_port": "",
        "rsync_options": "",
        "io_nice": "",
        "frequency": "",
        "log_file": str(tmp_path / "catalog.log"),
        "flock_file": str(tmp_path / "catalog.lock"),
        "flow_group": "",
        "tags": "",
        "is_entry_point": "FALSE",
        "is_end_point": "FALSE",
        "notify_on_success": "FALSE",
        "notify_on_error": "FALSE",
        "script_name": "catalog_transfer.sh",
    }
    loaded = TransferTable([row], columns=list(row))
    loaded.attrs["shared_file_pair_warnings"] = []
    calls = []

    def fake_load_runtime_transfer_catalog(config_file=None, transfers_file=None, runtime_ids=None):
        calls.append({
            "config_file": config_file,
            "transfers_file": transfers_file,
            "runtime_ids": runtime_ids,
        })
        return loaded

    monkeypatch.setattr(
        transfer_catalog,
        "load_runtime_transfer_catalog",
        fake_load_runtime_transfer_catalog,
    )

    snapshot = gcf.config.snapshot_state()
    try:
        rc = gcf.main(["--config", str(config_file)])
    finally:
        gcf.config.restore_state(snapshot)

    assert rc == 0
    assert calls == [{
        "config_file": None,
        "transfers_file": str(transfers_file),
        "runtime_ids": ["catalog_runtime"],
    }]
    assert (tmp_path / "output" / "scripts" / "catalog_transfer.sh").exists()
    assert (crontab_dir / "catalog_runtime.Landing_Zone.cron").exists()
    assert not (tmp_path / "output" / "scripts" / "direct_parse.sh").exists()


def test_reporting_catalog_relaxes_runtime_file_validation(tmp_path):
    """Reporting catalog loading should not require runtime log/flock columns."""
    transfers_file = tmp_path / "transfers.tsv"
    config_file = tmp_path / "config.yaml"
    transfers_file.write_text(
        "\n".join(
            [
                "identifiers\truntime_id\tenabled\tsystem\tusers\tsource\tdestination\ttags",
                "report_transfer\tlocal_dev.local\tTRUE\tlocal_dev\tlocal\t/source/\t/destination/\treporting",
            ]
        )
    )
    config_file.write_text(
        "transfers_file: {0}\n"
        "runtime_ids:\n"
        "  - local_dev.local\n".format(transfers_file)
    )

    snapshot = gcf.config.snapshot_state()
    try:
        catalog = load_reporting_transfer_catalog(config_file=str(config_file))
    finally:
        gcf.config.restore_state(snapshot)

    assert list(catalog["identifiers"]) == ["report_transfer"]
    assert list(catalog["runtime_id"]) == ["local_dev.local"]


def test_runtime_catalog_keeps_runtime_file_validation_distinct_from_reporting(tmp_path):
    """Runtime-validation mode should reject TSVs accepted by reporting mode."""
    transfers_file = tmp_path / "transfers.tsv"
    config_file = tmp_path / "config.yaml"
    transfers_file.write_text(
        "\n".join(
            [
                "identifiers\truntime_id\tenabled\tsystem\tusers\tsource\tdestination\ttags",
                "report_transfer\tlocal_dev.local\tTRUE\tlocal_dev\tlocal\t/source/\t/destination/\treporting",
            ]
        )
    )
    config_file.write_text(
        "transfers_file: {0}\n"
        "runtime_ids:\n"
        "  - local_dev.local\n".format(transfers_file)
    )

    snapshot = gcf.config.snapshot_state()
    try:
        reporting_catalog = load_reporting_transfer_catalog(config_file=str(config_file))
        with pytest.raises(ValueError, match="log_file"):
            load_runtime_transfer_catalog(config_file=str(config_file))
    finally:
        gcf.config.restore_state(snapshot)

    assert list(reporting_catalog["identifiers"]) == ["report_transfer"]


def test_runtime_catalog_defaults_entry_point_readiness_policy_to_direct(tmp_path):
    """Existing entry-point transfers keep direct readiness unless configured."""
    transfers_file = tmp_path / "transfers.tsv"
    config_file = tmp_path / "config.yaml"
    transfers_file.write_text(
        "\n".join(
            [
                "identifiers\truntime_id\tenabled\tsystem\tusers\tsource\tdestination\tlog_file\tflock_file\tis_entry_point",
                "entry_transfer\tlocal_dev.local\tTRUE\tlocal_dev\tlocal\t/source/\t/destination/\ttransfer.log\ttransfer.lock\tTRUE",
            ]
        )
    )
    config_file.write_text(
        "transfers_file: {0}\n"
        "runtime_ids:\n"
        "  - local_dev.local\n"
        "rit_managed_locations:\n"
        "  local_dev: {1}\n"
        "rit_managed_folder_structure:\n"
        "  log: log\n"
        "  flock: flock\n".format(transfers_file, tmp_path / "managed")
    )

    snapshot = gcf.config.snapshot_state()
    try:
        catalog = load_runtime_transfer_catalog(config_file=str(config_file))
        definitions = load_runtime_transfer_definitions(config_file=str(config_file))
    finally:
        gcf.config.restore_state(snapshot)

    row = catalog.iloc[0]
    assert row["readiness_policy"] == "direct"
    assert row["readiness_stable_observations"] == "1"
    assert row["readiness_quiet_seconds"] == "0"
    assert row["readiness_fingerprint_mode"] == "path_size_mtime"

    definition = definitions[0]
    assert definition.readiness_policy == "direct"
    assert definition.readiness_stable_observations == 1
    assert definition.readiness_quiet_seconds == 0
    assert definition.readiness_fingerprint_mode == "path_size_mtime"


def test_runtime_catalog_normalizes_configured_stable_snapshot_readiness(tmp_path):
    """Stable-snapshot readiness settings are exposed as canonical transfer facts."""
    transfers_file = tmp_path / "transfers.tsv"
    config_file = tmp_path / "config.yaml"
    transfers_file.write_text(
        "\n".join(
            [
                "identifiers\truntime_id\tenabled\tsystem\tusers\tsource\tdestination\tlog_file\tflock_file\tis_entry_point\treadiness_policy\treadiness_stable_observations\treadiness_quiet_seconds\treadiness_fingerprint_mode",
                "entry_transfer\tlocal_dev.local\tTRUE\tlocal_dev\tlocal\t/source/\t/destination/\ttransfer.log\ttransfer.lock\tTRUE\tstable-snapshot\t3\t600\tPATH-SIZE-MTIME",
            ]
        )
    )
    config_file.write_text(
        "transfers_file: {0}\n"
        "runtime_ids:\n"
        "  - local_dev.local\n"
        "rit_managed_locations:\n"
        "  local_dev: {1}\n"
        "rit_managed_folder_structure:\n"
        "  log: log\n"
        "  flock: flock\n".format(transfers_file, tmp_path / "managed")
    )

    snapshot = gcf.config.snapshot_state()
    try:
        catalog = load_runtime_transfer_catalog(config_file=str(config_file))
        definitions = load_runtime_transfer_definitions(config_file=str(config_file))
    finally:
        gcf.config.restore_state(snapshot)

    row = catalog.iloc[0]
    assert row["readiness_policy"] == "stable_snapshot"
    assert row["readiness_stable_observations"] == "3"
    assert row["readiness_quiet_seconds"] == "600"
    assert row["readiness_fingerprint_mode"] == "path_size_mtime"

    definition = definitions[0]
    assert definition.readiness_policy == "stable_snapshot"
    assert definition.readiness_stable_observations == 3
    assert definition.readiness_quiet_seconds == 600
    assert definition.readiness_fingerprint_mode == "path_size_mtime"


def test_runtime_catalog_rejects_unknown_readiness_policy(tmp_path):
    """Readiness policy names should fail during catalog loading."""
    transfers_file = tmp_path / "transfers.tsv"
    config_file = tmp_path / "config.yaml"
    transfers_file.write_text(
        "\n".join(
            [
                "identifiers\truntime_id\tenabled\tsystem\tusers\tsource\tdestination\tlog_file\tflock_file\tis_entry_point\treadiness_policy",
                "entry_transfer\tlocal_dev.local\tTRUE\tlocal_dev\tlocal\t/source/\t/destination/\ttransfer.log\ttransfer.lock\tTRUE\toptimistic",
            ]
        )
    )
    config_file.write_text(
        "transfers_file: {0}\n"
        "runtime_ids:\n"
        "  - local_dev.local\n"
        "rit_managed_locations:\n"
        "  local_dev: {1}\n"
        "rit_managed_folder_structure:\n"
        "  log: log\n"
        "  flock: flock\n".format(transfers_file, tmp_path / "managed")
    )

    snapshot = gcf.config.snapshot_state()
    try:
        with pytest.raises(ValueError, match="readiness_policy"):
            load_runtime_transfer_catalog(config_file=str(config_file))
    finally:
        gcf.config.restore_state(snapshot)


def test_runtime_catalog_rejects_non_positive_readiness_observations(tmp_path):
    """Stable observation thresholds should be positive integers."""
    transfers_file = tmp_path / "transfers.tsv"
    config_file = tmp_path / "config.yaml"
    transfers_file.write_text(
        "\n".join(
            [
                "identifiers\truntime_id\tenabled\tsystem\tusers\tsource\tdestination\tlog_file\tflock_file\tis_entry_point\treadiness_policy\treadiness_stable_observations",
                "entry_transfer\tlocal_dev.local\tTRUE\tlocal_dev\tlocal\t/source/\t/destination/\ttransfer.log\ttransfer.lock\tTRUE\tstable_snapshot\t0",
            ]
        )
    )
    config_file.write_text(
        "transfers_file: {0}\n"
        "runtime_ids:\n"
        "  - local_dev.local\n"
        "rit_managed_locations:\n"
        "  local_dev: {1}\n"
        "rit_managed_folder_structure:\n"
        "  log: log\n"
        "  flock: flock\n".format(transfers_file, tmp_path / "managed")
    )

    snapshot = gcf.config.snapshot_state()
    try:
        with pytest.raises(ValueError, match="readiness_stable_observations"):
            load_runtime_transfer_catalog(config_file=str(config_file))
    finally:
        gcf.config.restore_state(snapshot)


def test_runtime_catalog_rejects_negative_readiness_quiet_seconds(tmp_path):
    """Readiness quiet periods should be non-negative durations."""
    transfers_file = tmp_path / "transfers.tsv"
    config_file = tmp_path / "config.yaml"
    transfers_file.write_text(
        "\n".join(
            [
                "identifiers\truntime_id\tenabled\tsystem\tusers\tsource\tdestination\tlog_file\tflock_file\tis_entry_point\treadiness_policy\treadiness_quiet_seconds",
                "entry_transfer\tlocal_dev.local\tTRUE\tlocal_dev\tlocal\t/source/\t/destination/\ttransfer.log\ttransfer.lock\tTRUE\tstable_snapshot\t-1",
            ]
        )
    )
    config_file.write_text(
        "transfers_file: {0}\n"
        "runtime_ids:\n"
        "  - local_dev.local\n"
        "rit_managed_locations:\n"
        "  local_dev: {1}\n"
        "rit_managed_folder_structure:\n"
        "  log: log\n"
        "  flock: flock\n".format(transfers_file, tmp_path / "managed")
    )

    snapshot = gcf.config.snapshot_state()
    try:
        with pytest.raises(ValueError, match="readiness_quiet_seconds"):
            load_runtime_transfer_catalog(config_file=str(config_file))
    finally:
        gcf.config.restore_state(snapshot)


def test_runtime_catalog_rejects_remote_stable_snapshot_sources(tmp_path):
    """Stable-snapshot readiness is explicit about local-source support."""
    transfers_file = tmp_path / "transfers.tsv"
    config_file = tmp_path / "config.yaml"
    transfers_file.write_text(
        "\n".join(
            [
                "identifiers\truntime_id\tenabled\tsystem\tusers\tsource\tdestination\tlog_file\tflock_file\tis_entry_point\treadiness_policy",
                "entry_transfer\tlocal_dev.local\tTRUE\tlocal_dev\tlocal\tproducer:/source/\t/destination/\ttransfer.log\ttransfer.lock\tTRUE\tstable_snapshot",
            ]
        )
    )
    config_file.write_text(
        "transfers_file: {0}\n"
        "runtime_ids:\n"
        "  - local_dev.local\n"
        "rit_managed_locations:\n"
        "  local_dev: {1}\n"
        "rit_managed_folder_structure:\n"
        "  log: log\n"
        "  flock: flock\n".format(transfers_file, tmp_path / "managed")
    )

    snapshot = gcf.config.snapshot_state()
    try:
        with pytest.raises(ValueError, match="local sources"):
            load_runtime_transfer_catalog(config_file=str(config_file))
    finally:
        gcf.config.restore_state(snapshot)


def test_reporting_catalog_exposes_normalized_definitions_and_keeps_dataframe_compatibility(tmp_path):
    """Catalog callers can use typed transfer facts without losing dataframe rows."""
    transfers_file = tmp_path / "transfers.tsv"
    config_file = tmp_path / "config.yaml"
    transfers_file.write_text(
        "\n".join(
            [
                "identifiers\truntime_id\tenabled\tsystem\tusers\tsource\tdestination\tsource_port\tdestination_port\ttags\tis_entry_point\tis_end_point\tnotify_on_success\tnotify_on_error",
                "stage_lab\tlocal_dev.local\tTRUE\tlocal_dev\tlocal\t${SRC_ROOT}/in/*\tremote:${DST_ROOT}/out/\t2222\t2200.0\tLab, heartbeat\tYES\t0\t1\tfalse",
            ]
        )
    )
    config_file.write_text(
        "transfers_file: {0}\n"
        "runtime_ids:\n"
        "  - local_dev.local\n"
        "path_variables:\n"
        "  SRC_ROOT: {1}\n"
        "  DST_ROOT: {2}\n".format(
            transfers_file,
            tmp_path / "source",
            tmp_path / "destination",
        )
    )

    snapshot = gcf.config.snapshot_state()
    try:
        definitions = load_reporting_transfer_definitions(config_file=str(config_file))
        catalog = load_reporting_transfer_catalog(config_file=str(config_file))
    finally:
        gcf.config.restore_state(snapshot)

    assert len(definitions) == 1
    definition = definitions[0]
    assert definition.identifier == "stage_lab"
    assert definition.runtime_id == "local_dev.local"
    assert definition.system_user == "local_dev.local"
    assert definition.system == "local_dev"
    assert definition.user == "local"
    assert definition.source == str(tmp_path / "source" / "in") + "/*"
    assert definition.destination == "remote:{0}/out/".format(tmp_path / "destination")
    assert definition.source_port == "2222"
    assert definition.destination_port == "2200"
    assert definition.tags == ("heartbeat", "lab")
    assert definition.is_entry_point is True
    assert definition.is_end_point is False
    assert definition.notify_on_success is True
    assert definition.notify_on_error is False
    assert definition.script_name == "stage_lab.sh"

    assert list(catalog["identifiers"]) == ["stage_lab"]
    assert catalog.iloc[0]["is_entry_point"] == "TRUE"
    assert catalog.iloc[0]["is_end_point"] == "FALSE"
    assert catalog.iloc[0]["notify_on_success"] == "TRUE"
    assert catalog.iloc[0]["notify_on_error"] == "FALSE"
    assert catalog.iloc[0]["tags"] == "heartbeat,lab"
    assert catalog.iloc[0]["destination_port"] == "2200"
