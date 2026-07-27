#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""SQLAlchemy persistence and query boundaries for Transfer Event monitoring."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import os

from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    delete,
    select,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import make_url

from landingzones.transfer_events import (
    EVENT_COLUMNS,
    EVENT_HEADER,
    event_from_tsv_row,
    new_identifier,
    utc_now_text,
)
from landingzones.transfer_definitions import (
    normalize_tags,
    normalize_tags_text,
    normalize_transfer_path,
)


metadata = MetaData()

transfer_events = Table(
    "transfer_events",
    metadata,
    Column("ingestion_order", Integer, primary_key=True, autoincrement=True),
    Column("schema_version", String(8), nullable=False),
    Column("event_id", String(36), nullable=False, unique=True),
    Column("event_time_utc", String(40), nullable=False, index=True),
    Column("transfer_identifier", String(255), nullable=False, index=True),
    Column("system", String(255), nullable=False, index=True),
    Column("runtime_id", String(255), nullable=False, index=True),
    Column("execution_user", String(255), nullable=False, index=True),
    Column("run_id", String(36), nullable=True, index=True),
    Column("attempt_id", String(36), nullable=True, index=True),
    Column("run_name", String(255), nullable=True),
    Column("flow_group", String(255), nullable=True),
    Column("tags", Text, nullable=True),
    Column("origin_system", String(255), nullable=True),
    Column("entry_transfer_identifier", String(255), nullable=True),
    Column("created_at_utc", String(40), nullable=True),
    Column("directory", Text, nullable=True),
    Column("source_path", Text, nullable=True),
    Column("destination_path", Text, nullable=True),
    Column("status", String(32), nullable=False, index=True),
    Column("phase", String(32), nullable=False, index=True),
    Column("reason_code", String(64), nullable=True, index=True),
    Column("exit_code", Integer, nullable=True),
    Column("message", Text, nullable=True),
    Column("spool_id", String(255), nullable=False),
    Column("spool_offset", Integer, nullable=False),
    Column("ingested_at_utc", String(40), nullable=False),
    Column("ingestion_batch", String(36), nullable=False),
)

transfer_definitions = Table(
    "transfer_definitions",
    metadata,
    Column("runtime_id", String(255), primary_key=True),
    Column("transfer_identifier", String(255), primary_key=True),
    Column("system", String(255), nullable=False, index=True),
    Column("execution_user", String(255), nullable=False, index=True),
    Column("source_path", Text, nullable=True),
    Column("destination_path", Text, nullable=True),
    Column("flow_group", String(255), nullable=True),
    Column("tags", Text, nullable=True),
    Column("frequency", String(255), nullable=True),
    Column("enabled", Boolean, nullable=False),
    Column("is_entry_point", Boolean, nullable=False),
    Column("is_end_point", Boolean, nullable=False),
    Column("synced_at_utc", String(40), nullable=False),
)

spool_checkpoints = Table(
    "spool_checkpoints",
    metadata,
    Column("spool_id", String(255), primary_key=True),
    Column("spool_path", Text, nullable=False),
    Column("byte_offset", Integer, nullable=False),
    Column("header", Text, nullable=False),
    Column("updated_at_utc", String(40), nullable=False),
    Column("ingestion_batch", String(36), nullable=False),
)


class UnsupportedDatabaseBackend(ValueError):
    """Raised when a schema-v1 command is configured with a non-SQLite URL."""


class UnsupportedEventSpool(ValueError):
    """Raised when a live spool is not the exact schema-version-1 format."""


@dataclass(frozen=True)
class IngestionResult:
    """Operator-visible result of one Event Spool ingestion pass."""

    spool_id: str
    inserted: int
    duplicates: int
    checkpoint_offset: int
    deferred_bytes: int
    ingestion_batch: str


def _create_engine(database_url):
    url = make_url(database_url)
    if url.get_backend_name() != "sqlite":
        raise UnsupportedDatabaseBackend(
            "schema-version-1 monitoring supports only synchronous SQLite URLs"
        )
    database_path = url.database
    if database_path and database_path != ":memory:" and not database_path.startswith("file:"):
        directory = os.path.dirname(os.path.abspath(database_path))
        if directory and not os.path.isdir(directory):
            os.makedirs(directory)
    return create_engine(database_url, connect_args={"timeout": 30})


def create_monitoring_schema(database_url):
    """Create the schema-version-1 SQLite monitoring tables."""
    engine = _create_engine(database_url)
    metadata.create_all(engine)
    return engine


def _read_spool_chunk(spool_path, checkpoint_offset):
    file_size = os.path.getsize(spool_path)
    with open(spool_path, "rb") as handle:
        header_bytes = handle.readline()
        if not header_bytes.endswith(b"\n"):
            return None, b"", 0, file_size
        try:
            header = header_bytes.rstrip(b"\r\n").decode("utf-8")
        except UnicodeDecodeError:
            raise UnsupportedEventSpool("Event Spool header is not UTF-8")
        if header != EVENT_HEADER:
            raise UnsupportedEventSpool(
                "unsupported Event Spool header; live ingestion requires schema version 1"
            )
        header_offset = len(header_bytes)
        offset = checkpoint_offset or header_offset
        if offset < header_offset or offset > file_size:
            raise UnsupportedEventSpool(
                "Event Spool checkpoint is outside the current file; controlled cutover is required"
            )
        handle.seek(offset)
        pending = handle.read()

    final_newline = pending.rfind(b"\n")
    if final_newline < 0:
        complete = b""
        deferred = pending
    else:
        complete = pending[: final_newline + 1]
        deferred = pending[final_newline + 1 :]
    return header, complete, offset, len(deferred)


def ingest_event_spool(database_url, spool_path, spool_id=None):
    """Ingest complete spool rows and atomically advance the source checkpoint."""
    absolute_path = os.path.abspath(os.fspath(spool_path))
    source_id = str(spool_id or absolute_path)
    engine = create_monitoring_schema(database_url)
    batch_id = new_identifier()
    ingested_at = utc_now_text()

    with engine.begin() as connection:
        checkpoint = connection.execute(
            select(
                spool_checkpoints.c.byte_offset,
                spool_checkpoints.c.spool_path,
            ).where(
                spool_checkpoints.c.spool_id == source_id
            )
        ).mappings().one_or_none()
        if checkpoint is not None and checkpoint["spool_path"] != absolute_path:
            raise UnsupportedEventSpool(
                "Event Spool identity is already checkpointed for a different path"
            )
        header, complete, start_offset, deferred_bytes = _read_spool_chunk(
            absolute_path,
            checkpoint["byte_offset"] if checkpoint is not None else 0,
        )
        if header is None:
            return IngestionResult(
                spool_id=source_id,
                inserted=0,
                duplicates=0,
                checkpoint_offset=0,
                deferred_bytes=deferred_bytes,
                ingestion_batch=batch_id,
            )

        inserted = 0
        duplicates = 0
        spool_offset = start_offset
        for raw_line_with_ending in complete.splitlines(keepends=True):
            raw_line = raw_line_with_ending.rstrip(b"\r\n")
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError:
                raise UnsupportedEventSpool("Event Spool row is not UTF-8")
            event = event_from_tsv_row(line)
            event_values = asdict(event)
            event_values.update(
                {
                    "spool_id": source_id,
                    "spool_offset": spool_offset,
                    "ingested_at_utc": ingested_at,
                    "ingestion_batch": batch_id,
                }
            )
            statement = sqlite_insert(transfer_events).values(**event_values)
            statement = statement.on_conflict_do_nothing(
                index_elements=[transfer_events.c.event_id]
            )
            result = connection.execute(statement)
            if result.rowcount:
                inserted += 1
            else:
                duplicates += 1
            spool_offset += len(raw_line_with_ending)

        new_offset = start_offset + len(complete)
        checkpoint_values = {
            "spool_id": source_id,
            "spool_path": absolute_path,
            "byte_offset": new_offset,
            "header": EVENT_HEADER,
            "updated_at_utc": ingested_at,
            "ingestion_batch": batch_id,
        }
        checkpoint_statement = sqlite_insert(spool_checkpoints).values(
            **checkpoint_values
        )
        checkpoint_statement = checkpoint_statement.on_conflict_do_update(
            index_elements=[spool_checkpoints.c.spool_id],
            set_={
                key: value
                for key, value in checkpoint_values.items()
                if key != "spool_id"
            },
        )
        connection.execute(checkpoint_statement)

    return IngestionResult(
        spool_id=source_id,
        inserted=inserted,
        duplicates=duplicates,
        checkpoint_offset=new_offset,
        deferred_bytes=deferred_bytes,
        ingestion_batch=batch_id,
    )


def query_run_detail(database_url, run_id):
    """Return one Transfer Run timeline ordered by event time."""
    engine = create_monitoring_schema(database_url)
    columns = [transfer_events.c[column] for column in EVENT_COLUMNS]
    statement = (
        select(*columns)
        .where(transfer_events.c.run_id == run_id)
        .order_by(
            transfer_events.c.event_time_utc,
            transfer_events.c.ingestion_order,
        )
    )
    with engine.connect() as connection:
        timeline = [dict(row) for row in connection.execute(statement).mappings()]
    if not timeline:
        return None
    return {
        "run_id": run_id,
        "timeline": timeline,
    }


def _definition_values(definition, synced_at):
    return {
        "runtime_id": definition.runtime_id,
        "transfer_identifier": definition.identifier,
        "system": definition.system,
        "execution_user": definition.user,
        "source_path": definition.source or None,
        "destination_path": definition.destination or None,
        "flow_group": definition.flow_group or None,
        "tags": normalize_tags_text(definition.tags) or None,
        "frequency": definition.frequency or None,
        "enabled": bool(definition.enabled),
        "is_entry_point": bool(definition.is_entry_point),
        "is_end_point": bool(definition.is_end_point),
        "synced_at_utc": synced_at,
    }


def sync_transfer_definitions(database_url, definitions, scope_runtime_ids=None):
    """Synchronize current definitions, optionally within selected runtimes."""
    engine = create_monitoring_schema(database_url)
    synced_at = utc_now_text()
    values = [_definition_values(definition, synced_at) for definition in definitions]
    definition_runtime_ids = set(value["runtime_id"] for value in values)
    if scope_runtime_ids is None:
        synchronized_runtime_ids = None
    else:
        synchronized_runtime_ids = sorted(
            definition_runtime_ids.union(scope_runtime_ids)
        )
    current_keys = set(
        (value["runtime_id"], value["transfer_identifier"]) for value in values
    )

    with engine.begin() as connection:
        existing_statement = select(
            transfer_definitions.c.runtime_id,
            transfer_definitions.c.transfer_identifier,
        )
        if synchronized_runtime_ids is not None:
            if synchronized_runtime_ids:
                existing_statement = existing_statement.where(
                    transfer_definitions.c.runtime_id.in_(
                        synchronized_runtime_ids
                    )
                )
            else:
                existing_statement = None
        existing = (
            connection.execute(existing_statement)
            if existing_statement is not None
            else ()
        )
        for row in existing:
            key = (row.runtime_id, row.transfer_identifier)
            if key not in current_keys:
                connection.execute(
                    delete(transfer_definitions).where(
                        transfer_definitions.c.runtime_id == row.runtime_id,
                        transfer_definitions.c.transfer_identifier
                        == row.transfer_identifier,
                    )
                )
        for definition_values in values:
            statement = sqlite_insert(transfer_definitions).values(
                **definition_values
            )
            statement = statement.on_conflict_do_update(
                index_elements=[
                    transfer_definitions.c.runtime_id,
                    transfer_definitions.c.transfer_identifier,
                ],
                set_={
                    key: value
                    for key, value in definition_values.items()
                    if key not in ("runtime_id", "transfer_identifier")
                },
            )
            connection.execute(statement)
    return len(values)


def _parse_event_time(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _transfer_paths_connect(source_path, destination_path):
    source = normalize_transfer_path(source_path, strip_wildcard=True)
    destination = normalize_transfer_path(destination_path)
    if not source or not destination:
        return False
    return (
        source == destination
        or source.split(":", 1)[-1] == destination.split(":", 1)[-1]
    )


def _definition_is_terminal(definition, definitions):
    if definition is None or definition["is_end_point"]:
        return True
    if not normalize_transfer_path(definition["destination_path"]):
        return True
    return not any(
        candidate["enabled"]
        and (
            candidate["runtime_id"],
            candidate["transfer_identifier"],
        )
        != (
            definition["runtime_id"],
            definition["transfer_identifier"],
        )
        and (
            not definition["flow_group"]
            or not candidate["flow_group"]
            or definition["flow_group"] == candidate["flow_group"]
        )
        and _transfer_paths_connect(
            candidate["source_path"],
            definition["destination_path"],
        )
        for candidate in definitions
    )


def _run_state(events, definitions_by_key, definitions):
    latest = events[-1]
    route_key = (latest["runtime_id"], latest["transfer_identifier"])
    route_delivered = any(
        (event["runtime_id"], event["transfer_identifier"]) == route_key
        and event["status"] in ("delivered", "completed")
        for event in events
    )
    if latest["status"] == "completed":
        if not _definition_is_terminal(
            definitions_by_key.get(route_key),
            definitions,
        ):
            return "in progress"
        return "completed"
    if latest["status"] == "failed":
        if latest["phase"] == "cleanup" and route_delivered:
            return "delivered with cleanup failed"
        return "failed before delivery"
    if latest["status"] == "delivered":
        return "delivered with cleanup pending"
    if latest["status"] == "started":
        return "in progress"
    if latest["status"] == "waiting":
        return "waiting"
    return latest["status"]


def _event_summary(events, definitions_by_key, definitions, now):
    latest = events[-1]
    definition = definitions_by_key.get(
        (latest["runtime_id"], latest["transfer_identifier"])
    )
    latest_failure = next(
        (event for event in reversed(events) if event["status"] == "failed"),
        None,
    )
    attempts = set(
        event["attempt_id"] for event in events if event["attempt_id"] is not None
    )
    state = _run_state(events, definitions_by_key, definitions)
    age_candidates = [
        _parse_event_time(event["created_at_utc"] or event["event_time_utc"])
        for event in events
    ]
    age_candidates = [value for value in age_candidates if value is not None]
    if state == "completed" or not age_candidates:
        age_seconds = None
    else:
        age_seconds = max(
            0,
            int((now - min(age_candidates)).total_seconds()),
        )
    tags_value = latest["tags"]
    if not tags_value and definition is not None:
        tags_value = definition["tags"]
    return {
        "run_id": latest["run_id"],
        "run_name": latest["run_name"],
        "runtime_id": latest["runtime_id"],
        "system": latest["system"],
        "execution_user": latest["execution_user"],
        "transfer_identifier": latest["transfer_identifier"],
        "directory": latest["directory"],
        "current_status": latest["status"],
        "current_phase": latest["phase"],
        "enabled": definition["enabled"] if definition is not None else None,
        "flow_group": latest["flow_group"],
        "tags": list(normalize_tags(tags_value)),
        "frequency": definition["frequency"] if definition is not None else None,
        "state": state,
        "last_event_time_utc": latest["event_time_utc"],
        "age_seconds": age_seconds,
        "attempt_count": len(attempts) if attempts else None,
        "latest_failure_phase": (
            latest_failure["phase"] if latest_failure is not None else None
        ),
        "reason_code": (
            latest_failure["reason_code"] if latest_failure is not None else None
        ),
        "exit_code": (
            latest_failure["exit_code"] if latest_failure is not None else None
        ),
        "message": (
            latest_failure["message"] if latest_failure is not None else None
        ),
    }


def _never_observed_summary(definition):
    return {
        "run_id": None,
        "run_name": None,
        "runtime_id": definition["runtime_id"],
        "system": definition["system"],
        "execution_user": definition["execution_user"],
        "transfer_identifier": definition["transfer_identifier"],
        "directory": None,
        "current_status": None,
        "current_phase": None,
        "enabled": definition["enabled"],
        "flow_group": definition["flow_group"],
        "tags": list(normalize_tags(definition["tags"])),
        "frequency": definition["frequency"],
        "state": "configured but never observed",
        "last_event_time_utc": None,
        "age_seconds": None,
        "attempt_count": None,
        "latest_failure_phase": None,
        "reason_code": None,
        "exit_code": None,
        "message": None,
    }


def _summary_matches(
    summary,
    runtime_ids,
    systems,
    execution_users,
    transfer_identifiers,
    tags,
    states,
    reason_codes,
):
    exact_filters = (
        ("runtime_id", runtime_ids),
        ("system", systems),
        ("execution_user", execution_users),
        ("transfer_identifier", transfer_identifiers),
        ("state", states),
        ("reason_code", reason_codes),
    )
    for field_name, values in exact_filters:
        if values and summary[field_name] not in set(values):
            return False
    if tags and not set(normalize_tags(tags)).intersection(summary["tags"]):
        return False
    return True


def query_run_summaries(
    database_url,
    runtime_ids=None,
    systems=None,
    execution_users=None,
    transfer_identifiers=None,
    tags=None,
    states=None,
    reason_codes=None,
    now=None,
):
    """Query current Transfer Run state plus configured routes never observed."""
    engine = create_monitoring_schema(database_url)
    now_value = now or datetime.now(timezone.utc)
    with engine.connect() as connection:
        definitions = [
            dict(row)
            for row in connection.execute(select(transfer_definitions)).mappings()
        ]
        events = [
            dict(row)
            for row in connection.execute(
                select(
                    *[transfer_events.c[column] for column in EVENT_COLUMNS],
                    transfer_events.c.ingestion_order,
                ).order_by(
                    transfer_events.c.event_time_utc,
                    transfer_events.c.ingestion_order,
                )
            ).mappings()
        ]

    definitions_by_key = {
        (definition["runtime_id"], definition["transfer_identifier"]): definition
        for definition in definitions
    }
    observed_definition_keys = set(
        (event["runtime_id"], event["transfer_identifier"]) for event in events
    )
    grouped = {}
    for event in events:
        if event["run_id"] is None:
            group_key = (
                "route",
                event["runtime_id"],
                event["transfer_identifier"],
            )
        else:
            group_key = ("run", event["run_id"])
        grouped.setdefault(group_key, []).append(event)

    summaries = [
        _event_summary(group_events, definitions_by_key, definitions, now_value)
        for group_events in grouped.values()
    ]
    summaries.extend(
        _never_observed_summary(definition)
        for definition in definitions
        if (
            definition["runtime_id"],
            definition["transfer_identifier"],
        )
        not in observed_definition_keys
    )
    summaries = [
        summary
        for summary in summaries
        if _summary_matches(
            summary,
            runtime_ids,
            systems,
            execution_users,
            transfer_identifiers,
            tags,
            states,
            reason_codes,
        )
    ]
    summaries.sort(
        key=lambda summary: (
            summary["last_event_time_utc"] or "",
            summary["runtime_id"],
            summary["transfer_identifier"],
        ),
        reverse=True,
    )
    return summaries
