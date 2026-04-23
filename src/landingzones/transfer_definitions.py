#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Typed transfer-definition helpers shared across runtime surfaces."""

from dataclasses import dataclass
import re


TAG_SPLIT_PATTERN = re.compile(r",")
TAG_WHITESPACE_PATTERN = re.compile(r"\s+")


def normalize_tag(tag):
    """Normalize one tag token for stable matching and reporting."""
    value = str(tag).strip().lower() if tag is not None else ""
    if not value or value == "nan":
        return ""
    return TAG_WHITESPACE_PATTERN.sub("-", value)


def normalize_tags(tags):
    """Normalize a tag collection into a sorted tuple of unique tags."""
    if tags is None:
        return ()
    raw_values = [tags] if isinstance(tags, str) else list(tags)
    normalized = set()
    for raw_value in raw_values:
        candidates = (
            TAG_SPLIT_PATTERN.split(raw_value)
            if isinstance(raw_value, str)
            else [raw_value]
        )
        for candidate in candidates:
            normalized_tag = normalize_tag(candidate)
            if normalized_tag:
                normalized.add(normalized_tag)
    normalized = sorted(normalized)
    return tuple(normalized)


def normalize_tags_text(tags):
    """Return a canonical comma-separated tag string."""
    return ",".join(normalize_tags(tags))


def tags_match_any(tags, requested_tags):
    """Return True when the transfer/run tags match any requested tag."""
    normalized_tags = set(normalize_tags(tags))
    normalized_requested = set(normalize_tags(requested_tags))
    if not normalized_requested:
        return True
    return bool(normalized_tags & normalized_requested)


@dataclass(frozen=True)
class TransferDefinition:
    """Typed view of a normalized transfer row."""

    identifier: str
    system: str
    user: str
    source: str
    destination: str
    source_port: str = ""
    destination_port: str = ""
    rsync_options: str = ""
    io_nice: str = ""
    log_file: str = ""
    flock_file: str = ""
    frequency: str = ""
    flow_group: str = ""
    is_entry_point: bool = False
    is_end_point: bool = False
    notify_on_success: bool = False
    notify_on_error: bool = False
    tags: tuple = ()
    script_name: str = ""
    system_user: str = ""

    @classmethod
    def from_row(cls, row):
        """Build a typed definition from a normalized pandas row."""
        return cls(
            identifier=str(row.get("identifiers", "") or ""),
            system=str(row.get("system", "") or ""),
            user=str(row.get("users", row.get("user", "")) or ""),
            source=str(row.get("source", "") or ""),
            destination=str(row.get("destination", "") or ""),
            source_port=str(row.get("source_port", "") or ""),
            destination_port=str(row.get("destination_port", "") or ""),
            rsync_options=str(row.get("rsync_options", "") or ""),
            io_nice=str(row.get("io_nice", "") or ""),
            log_file=str(row.get("log_file", "") or ""),
            flock_file=str(row.get("flock_file", "") or ""),
            frequency=str(row.get("frequency", "") or ""),
            flow_group=str(row.get("flow_group", "") or ""),
            is_entry_point=str(row.get("is_entry_point", "FALSE") or "").upper() == "TRUE",
            is_end_point=str(row.get("is_end_point", "FALSE") or "").upper() == "TRUE",
            notify_on_success=str(row.get("notify_on_success", "FALSE") or "").upper() == "TRUE",
            notify_on_error=str(row.get("notify_on_error", "FALSE") or "").upper() == "TRUE",
            tags=normalize_tags(row.get("tags", "")),
            script_name=str(row.get("script_name", "") or ""),
            system_user=str(row.get("system_user", "") or ""),
        )


def definitions_from_dataframe(df):
    """Convert a normalized transfer DataFrame into typed definitions."""
    if df is None:
        return []
    return [TransferDefinition.from_row(row) for _, row in df.iterrows()]


def normalize_transfer_path(path, strip_wildcard=False):
    """Normalize a source or destination path for graph matching."""
    value = str(path).strip() if path is not None else ""
    if not value or value == "nan":
        return ""
    remote = ""
    inner = value
    if ":" in value:
        remote, inner = value.split(":", 1)
    if strip_wildcard and inner.endswith("/*"):
        inner = inner[:-2]
    elif strip_wildcard and inner.endswith("*"):
        inner = inner[:-1]
    inner = inner.rstrip("/")
    if remote:
        return "{0}:{1}".format(remote, inner)
    return inner
