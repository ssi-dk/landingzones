#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Config-aware transfer catalog loading.

The catalog is the loading seam for normalized transfer rows. Existing parser
callers can still use ``generate_cron_files.parse_transfers_file`` directly
while command paths move behind this smaller interface.
"""

from landingzones.config import config


def load_transfer_catalog(
    config_file=None,
    transfers_file=None,
    require_runtime_files=True,
    runtime_ids=None,
    system=None,
    systems=None,
):
    """Load normalized transfers after resolving config defaults."""
    from landingzones import generate_cron_files as gcf

    config.load_config(config_file=config_file, transfers_file=transfers_file)
    selected_runtime_ids = config.runtime_ids if runtime_ids is None else runtime_ids
    selected_systems = systems
    if system and selected_systems:
        raise ValueError("Use either system or systems, not both")
    if system:
        selected_systems = [system]
    return gcf.parse_transfers_file(
        config.transfers_file,
        require_runtime_files=require_runtime_files,
        runtime_ids=selected_runtime_ids,
        systems=selected_systems,
    )


def load_runtime_transfer_catalog(config_file=None, transfers_file=None, runtime_ids=None):
    """Load transfers with build/runtime validation enabled."""
    return load_transfer_catalog(
        config_file=config_file,
        transfers_file=transfers_file,
        require_runtime_files=True,
        runtime_ids=runtime_ids,
    )


def load_reporting_transfer_catalog(
    config_file=None,
    transfers_file=None,
    runtime_ids=None,
    system=None,
):
    """Load transfers with reporting/analysis validation enabled."""
    return load_transfer_catalog(
        config_file=config_file,
        transfers_file=transfers_file,
        require_runtime_files=False,
        runtime_ids=runtime_ids,
        system=system,
    )
