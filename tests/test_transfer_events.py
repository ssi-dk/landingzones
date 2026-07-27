#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Behavior tests for schema-version-1 Transfer Events."""

import csv
import io
import uuid

from landingzones.transfer_events import (
    EVENT_COLUMNS,
    EVENT_HEADER,
    create_transfer_event,
    event_to_tsv_row,
)


def test_transfer_event_serializes_the_exact_version_1_contract():
    """A Transfer Event should be fixed-width, sanitized, and independently identified."""
    event = create_transfer_event(
        transfer_identifier="stage_lab",
        system="server1",
        runtime_id="server1_prod.user1",
        execution_user="user1",
        status="failed",
        phase="transfer",
        run_id=str(uuid.uuid4()),
        attempt_id=str(uuid.uuid4()),
        message="rsync\tfailed\nsee route log",
        exit_code=23,
    )

    row_text = event_to_tsv_row(event)
    values = next(csv.reader(io.StringIO(row_text), delimiter="\t"))

    assert EVENT_COLUMNS == (
        "schema_version",
        "event_id",
        "event_time_utc",
        "transfer_identifier",
        "system",
        "runtime_id",
        "execution_user",
        "run_id",
        "attempt_id",
        "run_name",
        "flow_group",
        "tags",
        "origin_system",
        "entry_transfer_identifier",
        "created_at_utc",
        "directory",
        "source_path",
        "destination_path",
        "status",
        "phase",
        "reason_code",
        "exit_code",
        "message",
    )
    assert EVENT_HEADER == "\t".join(EVENT_COLUMNS)
    assert len(values) == len(EVENT_COLUMNS)
    assert values[0] == "1"
    assert uuid.UUID(values[1]).version == 4
    assert values[2].endswith("Z")
    assert values[9:18] == [""] * 9
    assert values[18:22] == ["failed", "transfer", "", "23"]
    assert values[22] == "rsync failed see route log"
