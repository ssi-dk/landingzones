#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for transfer health dashboard generation."""

import pandas as pd

from landingzones import plot_transfer_status as pts


def make_transfers_tsv(tmp_path):
    """Create a small flow definition with one terminal identifier."""
    path = tmp_path / "transfers.tsv"
    path.write_text(
        """identifiers\tenabled\tsystem\tusers\tsource\tdestination
stage_lab\tTRUE\ttest_local\tlocal\t/source/inbox/*\t/flow/stage/
promote_calc\tTRUE\ttest_local\tlocal\t/flow/stage/\t/flow/final/
pullback\tTRUE\ttest_local\tlocal\t/flow/final/\t/flow/archive/
other_system\tTRUE\tother\tlocal\t/elsewhere/*\t/unused/
"""
    )
    return path


def make_log_df():
    """Create a log fixture spanning success, failed, warning, and in-progress."""
    rows = [
        ("2026-04-09 09:00:00+0200", "stage_lab", "alpha", "/source/inbox/alpha", "/flow/stage/alpha", "initiated"),
        ("2026-04-09 09:10:00+0200", "stage_lab", "alpha", "/source/inbox/alpha", "/flow/stage/alpha", "completed"),
        ("2026-04-09 09:20:00+0200", "promote_calc", "alpha", "/flow/stage/alpha", "/flow/final/alpha", "initiated"),
        ("2026-04-09 09:25:00+0200", "promote_calc", "alpha", "/flow/stage/alpha", "/flow/final/alpha", "completed"),
        ("2026-04-09 09:30:00+0200", "pullback", "alpha", "/flow/final/alpha", "/flow/archive/alpha", "initiated"),
        ("2026-04-09 09:35:00+0200", "pullback", "alpha", "/flow/final/alpha", "/flow/archive/alpha", "completed"),
        ("2026-04-10 07:00:00+0200", "stage_lab", "beta", "/source/inbox/beta", "/flow/stage/beta", "initiated"),
        ("2026-04-10 07:20:00+0200", "promote_calc", "beta", "/flow/stage/beta", "/flow/final/beta", "error"),
        ("2026-04-10 08:00:00+0200", "stage_lab", "gamma", "/source/inbox/gamma", "/flow/stage/gamma", "initiated"),
        ("2026-04-10 09:15:00+0200", "promote_calc", "gamma", "/flow/stage/gamma", "/flow/final/gamma", "completed"),
        ("2026-04-10 07:30:00+0200", "stage_lab", "delta", "/source/inbox/delta", "/flow/stage/delta", "initiated"),
        ("2026-04-10 09:30:00+0200", "stage_lab", "epsilon", "/source/inbox/epsilon", "/flow/stage/epsilon", "initiated"),
        ("2026-04-10 09:35:00+0200", "promote_calc", "epsilon", "/flow/stage/epsilon", "/flow/final/epsilon", "error"),
        ("2026-04-10 09:45:00+0200", "pullback", "epsilon", "/flow/final/epsilon", "/flow/archive/epsilon", "completed"),
    ]
    columns = ["datetime", "identifier", "directory", "source", "destination", "status"]
    df = pd.DataFrame(rows, columns=columns)
    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d %H:%M:%S%z")
    df["directory_suffix"] = df["directory"].apply(pts.normalize_directory_suffix)
    return df.sort_values("datetime").reset_index(drop=True)


def test_normalize_directory_suffix_handles_paths_and_remote_prefixes():
    assert pts.normalize_directory_suffix("Illumina_TransferTest") == "Illumina_TransferTest"
    assert pts.normalize_directory_suffix("/tmp/a/b/Nanopore_TransferTest/") == "Nanopore_TransferTest"
    assert pts.normalize_directory_suffix("calck:/home/kimn/Landing_Zone/Illumina_TransferTest") == "Illumina_TransferTest"


def test_build_flow_graph_identifies_terminal_identifier(tmp_path):
    transfers_df = pts.load_transfer_metadata(str(make_transfers_tsv(tmp_path)))

    system_df, edges, terminal_identifiers = pts.build_flow_graph(transfers_df, "test_local")

    assert set(system_df["identifiers"]) == {"stage_lab", "promote_calc", "pullback"}
    assert edges["stage_lab"] == {"promote_calc"}
    assert edges["promote_calc"] == {"pullback"}
    assert terminal_identifiers == ["pullback"]


def test_aggregate_runs_assigns_expected_health_states():
    log_df = make_log_df()
    anchor = log_df["datetime"].max()

    runs_df = pts.aggregate_runs(
        log_df,
        terminal_identifiers=["pullback"],
        anchor_time=anchor,
        warning_hours=2,
    )

    by_run = runs_df.set_index("run")
    assert by_run.loc["alpha", "state"] == "success"
    assert by_run.loc["beta", "state"] == "failed"
    assert by_run.loc["gamma", "state"] == "in_progress"
    assert by_run.loc["delta", "state"] == "warning"
    assert by_run.loc["epsilon", "state"] == "success"
    assert by_run.loc["epsilon", "state_identifier"] == "pullback"


def test_metric_cards_count_unique_runs_by_window():
    log_df = make_log_df()
    runs_df = pts.aggregate_runs(
        log_df,
        terminal_identifiers=["pullback"],
        anchor_time=log_df["datetime"].max(),
        warning_hours=2,
    )

    cards = {card["label"]: card for card in pts.build_metric_cards(runs_df, log_df["datetime"].max())}

    assert cards["Last day"] == {
        "label": "Last day",
        "total": 4,
        "success": 1,
        "failed": 1,
        "warning": 1,
        "in_progress": 1,
    }
    assert cards["Last 7 days"]["total"] == 5
    assert cards["Last 7 days"]["success"] == 2


def test_render_dashboard_includes_tables_truncation_and_anchor_time(tmp_path):
    transfers_df = pts.load_transfer_metadata(str(make_transfers_tsv(tmp_path)))
    log_df = make_log_df()
    anchor = log_df["datetime"].max()

    extra_rows = []
    for index in range(12):
        run = "unfinished_{0:02d}".format(index)
        timestamp = anchor - pd.Timedelta(minutes=index)
        extra_rows.append(
            {
                "datetime": timestamp,
                "identifier": "stage_lab",
                "directory": run,
                "source": "/source/inbox/{0}".format(run),
                "destination": "/flow/stage/{0}".format(run),
                "status": "initiated",
                "directory_suffix": run,
            }
        )
    for index in range(12):
        run = "success_{0:02d}".format(index)
        started = anchor - pd.Timedelta(hours=6, minutes=index)
        completed = anchor - pd.Timedelta(hours=1, minutes=index)
        extra_rows.extend(
            [
                {
                    "datetime": started,
                    "identifier": "stage_lab",
                    "directory": run,
                    "source": "/source/inbox/{0}".format(run),
                    "destination": "/flow/stage/{0}".format(run),
                    "status": "initiated",
                    "directory_suffix": run,
                },
                {
                    "datetime": completed,
                    "identifier": "pullback",
                    "directory": run,
                    "source": "/flow/final/{0}".format(run),
                    "destination": "/flow/archive/{0}".format(run),
                    "status": "completed",
                    "directory_suffix": run,
                },
            ]
        )

    expanded_log_df = pd.concat([log_df, pd.DataFrame(extra_rows)], ignore_index=True)
    output_path = tmp_path / "dashboard.html"

    result = pts.create_transfer_dashboard(
        expanded_log_df,
        transfers_df,
        system="test_local",
        output_path=str(output_path),
        warning_hours=2,
        max_runs=10,
        title="Transfer Health Dashboard",
    )

    html_output = output_path.read_text()

    assert result == str(output_path)
    assert "Anchor time: 2026-04-10 09:45:00+0200" in html_output
    assert "Unfinished Runs (Last 7 days)" in html_output
    assert "Recent Successes (Last 7 days)" in html_output
    assert "Showing 10 most recent unfinished runs; and 5 more." in html_output
    assert "Showing 10 most recent successes; and 4 more." in html_output
    assert "warning" in html_output
    assert "in progress" in html_output
    assert "failed" in html_output
