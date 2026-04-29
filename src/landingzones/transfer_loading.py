#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Shared config-aware transfer loading helpers."""

from landingzones.config import config
from landingzones import generate_cron_files as gcf
from landingzones.transfer_definitions import (
    definitions_from_dataframe,
    tags_match_any,
)


def load_transfers(
    config_file=None,
    transfers_file=None,
    require_runtime_files=True,
    system=None,
):
    """Load normalized transfer definitions after resolving config defaults."""
    config.load_config(config_file=config_file, transfers_file=transfers_file)
    return gcf.parse_transfers_file(
        config.transfers_file,
        require_runtime_files=require_runtime_files,
        systems=[system] if system else None,
    )


def load_runtime_transfers(config_file=None, transfers_file=None):
    """Load transfers with runtime/generator validation enabled."""
    return load_transfers(
        config_file=config_file,
        transfers_file=transfers_file,
        require_runtime_files=True,
    )


def load_reporting_transfers(config_file=None, transfers_file=None, system=None):
    """Load transfers with analysis/reporting validation enabled."""
    return load_transfers(
        config_file=config_file,
        transfers_file=transfers_file,
        require_runtime_files=False,
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
