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


def test_load_transfer_log_supports_rich_event_schema(tmp_path):
    path = tmp_path / "Landing_Zone_test_local.transfers.tsv"
    path.write_text(
        "\n".join([
            "event_time_utc\ttransfer_identifier\tsystem\trun_id\trun_name\tflow_group\torigin_system\tentry_transfer_identifier\tcreated_at_utc\tdirectory\tsource_path\tdestination_path\tstatus\tmessage",
            "2026-04-10T07:00:00Z\tstage_lab\ttest_local\trun-123\talpha\tflow_a\tseqbox01\tstage_lab\t2026-04-10T06:55:00Z\talpha\t/source/inbox/alpha\t/flow/stage/alpha\tinitiated\t",
            "",
        ])
    )

    df = pts.load_transfer_log(str(path))

    assert df.loc[0, "identifier"] == "stage_lab"
    assert df.loc[0, "source"] == "/source/inbox/alpha"
    assert df.loc[0, "destination"] == "/flow/stage/alpha"
    assert df.loc[0, "run_id"] == "run-123"
    assert df.loc[0, "run_group"] == "run-123"
    assert df.loc[0, "directory_suffix"] == "alpha"


def test_main_uses_configured_report_transfer_log_file_when_input_omitted(tmp_path, monkeypatch):
    config_file = tmp_path / "config.yaml"
    log_path = tmp_path / "Landing_Zone_test_local.transfers.tsv"
    config_file.write_text(
        "transfers_file: /tmp/transfers.tsv\n"
        "report_transfer_log_file: {0}\n".format(log_path)
    )

    captured = {}

    def fake_load_transfer_log(path):
        captured["log_path"] = path
        return pd.DataFrame(
            [
                {
                    "datetime": pd.Timestamp("2026-04-10T07:00:00Z"),
                    "identifier": "stage_lab",
                    "directory": "alpha",
                    "source": "/source/inbox/alpha",
                    "destination": "/flow/stage/alpha",
                    "status": "initiated",
                    "run_id": "run-123",
                    "run_name": "alpha",
                    "directory_suffix": "alpha",
                    "run_group": "run-123",
                }
            ]
        )

    def fake_load_transfers_for_reporting(config_file=None, transfers_file=None):
        captured["config_file"] = config_file
        captured["transfers_file"] = transfers_file
        return pd.DataFrame(
            [
                {
                    "identifiers": "stage_lab",
                    "system": "test_local",
                    "source": "/source/inbox/*",
                    "destination": "/flow/stage/",
                }
            ]
        )

    def fake_create_transfer_dashboard(log_df, transfers_df, system, output_path, **kwargs):
        captured["system"] = system
        captured["output_path"] = output_path
        return output_path

    monkeypatch.setattr(pts, "load_transfer_log", fake_load_transfer_log)
    monkeypatch.setattr(pts, "load_transfers_for_reporting", fake_load_transfers_for_reporting)
    monkeypatch.setattr(pts, "create_transfer_dashboard", fake_create_transfer_dashboard)

    rc = pts.main(["--config", str(config_file), "--system", "test_local"])

    assert rc == 0
    assert captured["log_path"] == str(log_path)
    assert captured["config_file"] == str(config_file)
    assert captured["transfers_file"] is None
    assert captured["system"] == "test_local"
    assert captured["output_path"] == str(tmp_path / "Landing_Zone_test_local.transfers.health_dashboard.html")


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


def test_describe_state_logic_includes_warning_threshold():
    assert "2h threshold" in pts.describe_state_logic("warning", 2)
    assert "0.5h threshold" in pts.describe_state_logic("in_progress", 0.5)


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
    assert 'title="Warning: no newer terminal success or error exists, and the latest initiated event is older than the 2h threshold."' in html_output
    assert 'title="Failed: the most recent decisive event is an error event."' in html_output
    assert "Hover a status label for classification logic" in html_output
