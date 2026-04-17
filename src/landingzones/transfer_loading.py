#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Shared config-aware transfer loading helpers."""

from landingzones.config import config
from landingzones import generate_cron_files as gcf
from landingzones.transfer_definitions import definitions_from_dataframe


def load_transfers(config_file=None, transfers_file=None, require_runtime_files=True):
    """Load normalized transfer definitions after resolving config defaults."""
    config.load_config(config_file=config_file, transfers_file=transfers_file)
    return gcf.parse_transfers_file(
        config.transfers_file,
        require_runtime_files=require_runtime_files,
    )


def load_runtime_transfers(config_file=None, transfers_file=None):
    """Load transfers with runtime/generator validation enabled."""
    return load_transfers(
        config_file=config_file,
        transfers_file=transfers_file,
        require_runtime_files=True,
    )


def load_reporting_transfers(config_file=None, transfers_file=None):
    """Load transfers with analysis/reporting validation enabled."""
    return load_transfers(
        config_file=config_file,
        transfers_file=transfers_file,
        require_runtime_files=False,
    )


def filter_transfers_by_system_user(transfers_df, system, user):
    """Return only transfers matching a system/user pair."""
    return transfers_df[
        (transfers_df['system'] == system) &
        (transfers_df['users'] == user)
    ].copy()


def definitions_for_system(transfers_df, system):
    """Return typed definitions for one system."""
    return [
        definition
        for definition in definitions_from_dataframe(transfers_df)
        if definition.system == system
    ]
