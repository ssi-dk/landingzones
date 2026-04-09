#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for transfer status plotting."""

from pathlib import Path

from landingzones import plot_transfer_status as pts


FIXTURE_LOG = (
    Path(__file__).resolve().parents[2]
    / "envs/main/landingzones/tests/test_local/output/log/Landing_Zone_test_local.transfers.tsv"
)


def test_normalize_directory_suffix_handles_paths_and_remote_prefixes():
    assert pts.normalize_directory_suffix("Illumina_TransferTest") == "Illumina_TransferTest"
    assert pts.normalize_directory_suffix("/tmp/a/b/Nanopore_TransferTest/") == "Nanopore_TransferTest"
    assert pts.normalize_directory_suffix("calck:/home/kimn/Landing_Zone/Illumina_TransferTest") == "Illumina_TransferTest"


def test_latest_completed_by_identifier_filters_last_day():
    df = pts.load_transfer_log(str(FIXTURE_LOG))
    anchor = df["datetime"].max()
    latest, label = pts.latest_completed_by_identifier(df, "1d", anchor_time=anchor)

    assert label == "Last 1 day"
    assert not latest.empty
    assert set(latest["status"]) == {"completed"}
    assert set(latest["identifier"]) == {
        "test_local_labnet_stage",
        "test_local_kma_stage",
        "calc_test_secondary_labnet_promote",
        "calc_test_secondary_kma_promote",
        "test_calc_kimn_pullback",
    }


def test_create_transfer_plot_writes_png(tmp_path):
    df = pts.load_transfer_log(str(FIXTURE_LOG))
    output_path = tmp_path / "transfer_plot.png"

    result = pts.create_transfer_plot(df, str(output_path), windows=("1d", "7d", "all"))

    assert result == str(output_path)
    assert output_path.exists()
    assert output_path.stat().st_size > 0
