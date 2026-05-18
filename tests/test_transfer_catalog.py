#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for transfer catalog loading."""

from landingzones import generate_cron_files as gcf
from landingzones import transfer_catalog
from landingzones.table import TransferTable
from landingzones.transfer_catalog import (
    load_reporting_transfer_catalog,
    load_runtime_transfer_catalog,
)


def test_runtime_catalog_preserves_build_loading_invariants(tmp_path):
    """Runtime catalog loading keeps build-facing TSV behavior stable."""
    transfers_file = tmp_path / "transfers.tsv"
    config_file = tmp_path / "config.yaml"
    transfers_file.write_text(
        "\n".join(
            [
                "runtime_id\tenabled\tsystem\tusers\tsource\tdestination\tlog_file\tflock_file\tflow_group\tis_entry_point\tnotify_on_success",
                "local_dev.local\tTRUE\tlocal_dev\tlocal\t${SRC_ROOT}/in/\t${DST_ROOT}/out/\ttransfer.log\ttransfer.lock\tflow one\tyes\t1",
                "local_dev.local\tTRUE\tlocal_dev\tlocal\t${SRC_ROOT}/second/\t${DST_ROOT}/second/\ttransfer.log\ttransfer.lock\tflow one\tfalse\tfalse",
                "disabled.local\tFALSE\tlocal_dev\tlocal\t${SRC_ROOT}/disabled/\t${DST_ROOT}/disabled/\tdisabled.log\tdisabled.lock\t\t\t",
                "#commented.local\tTRUE\tlocal_dev\tlocal\t${SRC_ROOT}/commented/\t${DST_ROOT}/commented/\tcommented.log\tcommented.lock\t\t\t",
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
    assert catalog.iloc[1]["is_entry_point"] == "FALSE"
    assert catalog.attrs["shared_file_pair_warnings"]


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
