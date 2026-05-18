#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Shared config-aware transfer loading helpers."""

import os

from landingzones.config import config
from landingzones import generate_cron_files as gcf
from landingzones import transfer_catalog
from landingzones.transfer_definitions import (
    definitions_from_dataframe,
    tags_match_any,
)


def normalize_runtime_id_args(runtime_ids):
    """Normalize CLI-provided runtime_id filters."""
    values = []
    for runtime_id in runtime_ids or []:
        value = str(runtime_id).strip()
        if value and value not in values:
            values.append(value)
    return values


def read_runtime_filter_metadata(crontab_dir):
    """Read runtime_id values written by the most recent build."""
    metadata_path = gcf.runtime_filter_metadata_path(crontab_dir)
    if not os.path.exists(metadata_path):
        return []
    values = []
    with open(metadata_path, 'r') as handle:
        for line in handle:
            value = line.strip()
            if not value or value.startswith('#'):
                continue
            if value not in values:
                values.append(value)
    return values


def discover_runtime_ids_from_crontabs(crontab_dir):
    """Infer runtime_id values from generated cron filenames."""
    if not os.path.isdir(crontab_dir):
        return []

    suffix = '.Landing_Zone.cron'
    prefix = gcf.configured_artifact_prefix()
    prefix_text = "{0}.".format(prefix) if prefix else ''
    values = []
    for entry in sorted(os.listdir(crontab_dir)):
        if not entry.endswith(suffix):
            continue
        stem = entry[:-len(suffix)]
        if prefix_text:
            if not stem.startswith(prefix_text):
                continue
            stem = stem[len(prefix_text):]
        if stem and stem not in values:
            values.append(stem)
    return values


def resolve_runtime_ids(explicit_runtime_ids=None, crontab_dir=None):
    """Resolve runtime_id filters, preferring explicit CLI input."""
    runtime_ids = normalize_runtime_id_args(explicit_runtime_ids)
    if runtime_ids:
        return runtime_ids, 'command line'

    runtime_ids = config.runtime_ids
    if runtime_ids:
        return runtime_ids, 'config'

    crontab_dir = crontab_dir or config.crontab_dir
    runtime_ids = read_runtime_filter_metadata(crontab_dir)
    if runtime_ids:
        return runtime_ids, gcf.runtime_filter_metadata_path(crontab_dir)

    runtime_ids = discover_runtime_ids_from_crontabs(crontab_dir)
    if len(runtime_ids) == 1:
        return runtime_ids, crontab_dir

    return [], None


def load_transfers(
    config_file=None,
    transfers_file=None,
    require_runtime_files=True,
    runtime_ids=None,
    system=None,
):
    """Load normalized transfer definitions through the transfer catalog."""
    return transfer_catalog.load_transfer_catalog(
        config_file=config_file,
        transfers_file=transfers_file,
        require_runtime_files=require_runtime_files,
        runtime_ids=runtime_ids,
        system=system,
    )


def load_runtime_transfers(config_file=None, transfers_file=None, runtime_ids=None):
    """Load transfers with runtime/generator validation enabled."""
    return transfer_catalog.load_runtime_transfer_catalog(
        config_file=config_file,
        transfers_file=transfers_file,
        runtime_ids=runtime_ids,
    )


def load_reporting_transfers(
    config_file=None,
    transfers_file=None,
    runtime_ids=None,
    system=None,
):
    """Load transfers with analysis/reporting validation enabled."""
    return transfer_catalog.load_reporting_transfer_catalog(
        config_file=config_file,
        transfers_file=transfers_file,
        runtime_ids=runtime_ids,
        system=system,
    )


def filter_transfers_by_system_user(transfers_df, system, user):
    """Return only transfers matching a system/user pair."""
    return transfers_df[
        (transfers_df['system'] == system) &
        (transfers_df['users'] == user)
    ].copy()


def filter_transfers_by_runtime_ids(transfers_df, runtime_ids):
    """Return only transfers matching exact runtime_id values."""
    return gcf.filter_transfers_by_runtime_ids(transfers_df, runtime_ids)


def filter_transfers_by_tags(transfers_df, requested_tags):
    """Return transfers matching any requested tag."""
    if not requested_tags:
        return transfers_df.copy()
    return transfers_df[
        transfers_df['tags'].apply(lambda value: tags_match_any(value, requested_tags))
    ].copy()


def definitions_for_system(transfers_df, system):
    """Return typed definitions for one system."""
    return [
        definition
        for definition in definitions_from_dataframe(transfers_df)
        if definition.system == system
    ]
