#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for tag-based separation validation."""

import pandas as pd

from landingzones import validate_separation as vsep


def make_transfers_df():
    """Return a small transfer set with one tagged side flow."""
    return pd.DataFrame(
        [
            {
                "identifiers": "heartbeat_stage",
                "system": "lab-a",
                "users": "local",
                "source": "/landing/heartbeat/",
                "destination": "/server1/heartbeat/",
                "tags": "heartbeat,lab",
            },
            {
                "identifiers": "heartbeat_nested_overlap",
                "system": "lab-a",
                "users": "local",
                "source": "/landing/heartbeat/subdir/",
                "destination": "/server1/archive/",
                "tags": "",
            },
            {
                "identifiers": "server1_consumer",
                "system": "server1",
                "users": "svc",
                "source": "/server1/heartbeat/",
                "destination": "/server1/final/",
                "tags": "",
            },
            {
                "identifiers": "isolated_manual",
                "system": "lab-a",
                "users": "local",
                "source": "/landing/manual/",
                "destination": "/server1/manual/",
                "tags": "manual",
            },
        ]
    )


def test_detect_separation_collisions_reports_source_overlap_and_handoff():
    transfers_df = make_transfers_df()

    tagged_df, other_df, findings = vsep.detect_separation_collisions(
        transfers_df,
        ["heartbeat"],
    )

    assert set(tagged_df["identifiers"]) == {"heartbeat_stage"}
    assert set(other_df["identifiers"]) == {
        "heartbeat_nested_overlap",
        "server1_consumer",
        "isolated_manual",
    }
    assert {finding["type"] for finding in findings} == {
        "source_overlap",
        "destination_handoff",
    }
    assert any(
        finding["other_identifier"] == "heartbeat_nested_overlap"
        for finding in findings
        if finding["type"] == "source_overlap"
    )
    assert any(
        finding["other_identifier"] == "server1_consumer"
        for finding in findings
        if finding["type"] == "destination_handoff"
    )


def test_detect_separation_collisions_returns_no_findings_for_isolated_tag():
    transfers_df = make_transfers_df()

    tagged_df, other_df, findings = vsep.detect_separation_collisions(
        transfers_df,
        ["manual"],
    )

    assert set(tagged_df["identifiers"]) == {"isolated_manual"}
    assert len(other_df) == 3
    assert findings == []


def test_detect_separation_collisions_matches_any_requested_tag():
    transfers_df = make_transfers_df()
    transfers_df.loc[0, "tags"] = "heartbeat,side-flow"

    tagged_df, _, _ = vsep.detect_separation_collisions(
        transfers_df,
        ["manual", "heartbeat"],
    )

    assert set(tagged_df["identifiers"]) == {"heartbeat_stage", "isolated_manual"}


def test_detect_separation_collisions_defaults_to_any_tagged_transfer():
    transfers_df = make_transfers_df()

    tagged_df, _, findings = vsep.detect_separation_collisions(
        transfers_df,
        [],
    )

    assert set(tagged_df["identifiers"]) == {
        "heartbeat_stage",
        "isolated_manual",
    }
    assert {finding["type"] for finding in findings} == {
        "source_overlap",
        "destination_handoff",
    }


def test_print_separation_report_labels_default_tag_as_any(capsys):
    transfers_df = make_transfers_df()

    tagged_df, other_df, findings = vsep.detect_separation_collisions(
        transfers_df,
        [],
    )
    vsep.print_separation_report([], tagged_df, other_df, findings)

    captured = capsys.readouterr()
    assert "Separation check for tags: any" in captured.out


def test_print_separation_report_handles_no_tag_match(capsys):
    transfers_df = make_transfers_df()

    tagged_df, other_df, findings = vsep.detect_separation_collisions(
        transfers_df,
        ["missing-tag"],
    )
    vsep.print_separation_report(["missing-tag"], tagged_df, other_df, findings)

    captured = capsys.readouterr()
    assert "No matching tagged transfers found for tags: missing-tag" in captured.out


def test_main_returns_nonzero_and_prints_warnings(tmp_path, capsys):
    transfers_file = tmp_path / "transfers.tsv"
    transfers_file.write_text(
        "\n".join(
            [
                "identifiers\tenabled\tsystem\tusers\tsource\tdestination\ttags",
                "heartbeat_stage\tTRUE\tlab-a\tlocal\t/landing/heartbeat/\t/server1/heartbeat/\theartbeat,lab",
                "heartbeat_nested_overlap\tTRUE\tlab-a\tlocal\t/landing/heartbeat/subdir/\t/server1/archive/\t",
                "server1_consumer\tTRUE\tserver1\tsvc\t/server1/heartbeat/\t/server1/final/\t",
            ]
        )
    )

    rc = vsep.main([
        "--transfers", str(transfers_file),
        "--tag", "heartbeat",
    ])
    captured = capsys.readouterr()

    assert rc == 1
    assert "Separation check for tags: heartbeat" in captured.out
    assert "Found 2 collision(s)." in captured.out
