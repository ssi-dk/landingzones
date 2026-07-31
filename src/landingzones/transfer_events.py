#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Schema-version-1 Transfer Event domain model."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import csv
import io
import uuid


SCHEMA_VERSION = "1"
EVENT_COLUMNS = (
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
EVENT_HEADER = "\t".join(EVENT_COLUMNS)
EVENT_STATUSES = frozenset(("waiting", "started", "delivered", "completed", "failed"))
EVENT_PHASES = frozenset(("discovery", "readiness", "transfer", "promotion", "cleanup"))
REASON_CODES = frozenset(
    (
        "ssh_timeout",
        "ssh_authentication_failed",
        "ssh_host_unreachable",
        "ssh_failed",
        "permission_denied",
        "source_missing",
        "destination_conflict",
        "metadata_invalid",
        "rsync_failed",
    )
)


@dataclass(frozen=True)
class TransferEvent:
    """One immutable operational fact emitted by a Landing Zone Runtime."""

    schema_version: str
    event_id: str
    event_time_utc: str
    transfer_identifier: str
    system: str
    runtime_id: str
    execution_user: str
    run_id: str = None
    attempt_id: str = None
    run_name: str = None
    flow_group: str = None
    tags: str = None
    origin_system: str = None
    entry_transfer_identifier: str = None
    created_at_utc: str = None
    directory: str = None
    source_path: str = None
    destination_path: str = None
    status: str = ""
    phase: str = ""
    reason_code: str = None
    exit_code: int = None
    message: str = None


def new_identifier():
    """Return one canonical lowercase UUIDv4 identifier."""
    return str(uuid.uuid4())


def utc_now_text():
    """Return the current UTC instant in RFC 3339 form."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _validate_uuid4(value, field_name, required=False):
    if value in (None, ""):
        if required:
            raise ValueError("{0} is required".format(field_name))
        return
    try:
        parsed = uuid.UUID(str(value))
    except (AttributeError, TypeError, ValueError):
        raise ValueError("{0} must be a UUIDv4".format(field_name))
    if parsed.version != 4 or str(parsed) != str(value):
        raise ValueError("{0} must be a canonical lowercase UUIDv4".format(field_name))


def _validate_event(event):
    if event.schema_version != SCHEMA_VERSION:
        raise ValueError("unsupported Transfer Event schema version: {0}".format(event.schema_version))
    for field_name in (
        "event_id",
        "event_time_utc",
        "transfer_identifier",
        "system",
        "runtime_id",
        "execution_user",
        "status",
        "phase",
    ):
        if getattr(event, field_name) in (None, ""):
            raise ValueError("{0} is required".format(field_name))
    _validate_uuid4(event.event_id, "event_id", required=True)
    _validate_uuid4(event.run_id, "run_id")
    _validate_uuid4(event.attempt_id, "attempt_id")
    if event.status not in EVENT_STATUSES:
        raise ValueError("unsupported Transfer Event status: {0}".format(event.status))
    if event.phase not in EVENT_PHASES:
        raise ValueError("unsupported Transfer Event phase: {0}".format(event.phase))
    if event.reason_code and event.reason_code not in REASON_CODES:
        raise ValueError("unsupported Transfer Event reason_code: {0}".format(event.reason_code))
    try:
        parsed_time = datetime.fromisoformat(event.event_time_utc.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError):
        raise ValueError("event_time_utc must be an RFC 3339 timestamp")
    if not event.event_time_utc.endswith("Z") or parsed_time.utcoffset() != timezone.utc.utcoffset(None):
        raise ValueError("event_time_utc must be represented in UTC")
    if event.status in ("waiting", "started", "delivered", "completed") and not event.run_id:
        raise ValueError("run_id is required for run-specific events")
    if (
        event.status in ("started", "delivered", "completed")
        or (event.status == "failed" and event.phase in ("transfer", "promotion", "cleanup"))
    ) and not event.attempt_id:
        raise ValueError("attempt_id is required for actual attempt work")


def create_transfer_event(
    transfer_identifier,
    system,
    runtime_id,
    execution_user,
    status,
    phase,
    event_id=None,
    event_time_utc=None,
    **values
):
    """Create and validate one immutable Transfer Event."""
    event = TransferEvent(
        schema_version=SCHEMA_VERSION,
        event_id=event_id or new_identifier(),
        event_time_utc=event_time_utc or utc_now_text(),
        transfer_identifier=transfer_identifier,
        system=system,
        runtime_id=runtime_id,
        execution_user=execution_user,
        status=status,
        phase=phase,
        **values
    )
    _validate_event(event)
    return event


def sanitize_tsv_value(value):
    """Serialize a nullable value without changing TSV column alignment."""
    if value is None:
        return ""
    return str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ")


def event_to_tsv_row(event):
    """Serialize one event as an exact-width schema-version-1 TSV row."""
    _validate_event(event)
    values = asdict(event)
    return "\t".join(sanitize_tsv_value(values[column]) for column in EVENT_COLUMNS)


def event_from_tsv_row(row):
    """Parse and validate one exact-width schema-version-1 TSV row."""
    if isinstance(row, str):
        values = next(csv.reader(io.StringIO(row), delimiter="\t"))
    else:
        values = list(row)
    if len(values) != len(EVENT_COLUMNS):
        raise ValueError(
            "Transfer Event row has {0} columns; expected {1}".format(
                len(values),
                len(EVENT_COLUMNS),
            )
        )
    data = dict(zip(EVENT_COLUMNS, values))
    for field_name in EVENT_COLUMNS:
        if field_name not in (
            "schema_version",
            "event_id",
            "event_time_utc",
            "transfer_identifier",
            "system",
            "runtime_id",
            "execution_user",
            "status",
            "phase",
        ) and data[field_name] == "":
            data[field_name] = None
    if data["exit_code"] is not None:
        try:
            data["exit_code"] = int(data["exit_code"])
        except ValueError:
            raise ValueError("exit_code must be an integer or empty")
    event = TransferEvent(**data)
    _validate_event(event)
    return event
