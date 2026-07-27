#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Config-aware transfer catalog loading.

The catalog is the loading seam for normalized transfer rows. Existing parser
callers can still use ``generate_cron_files.parse_transfers_file`` directly
while command paths move behind this smaller interface.
"""

from landingzones.config import config
from landingzones.transfer_definitions import definitions_from_dataframe


def load_transfer_catalog(
    config_file=None,
    transfers_file=None,
    require_runtime_files=True,
    runtime_ids=None,
    system=None,
    systems=None,
    include_disabled=False,
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
    parse_options = {
        "require_runtime_files": require_runtime_files,
        "runtime_ids": selected_runtime_ids,
        "systems": selected_systems,
    }
    if include_disabled:
        parse_options["include_disabled"] = True
    return gcf.parse_transfers_file(config.transfers_file, **parse_options)


def load_transfer_definitions(
    config_file=None,
    transfers_file=None,
    require_runtime_files=True,
    runtime_ids=None,
    system=None,
    systems=None,
    include_disabled=False,
):
    """Load normalized transfer definitions after resolving config defaults."""
    return definitions_from_dataframe(
        load_transfer_catalog(
            config_file=config_file,
            transfers_file=transfers_file,
            require_runtime_files=require_runtime_files,
            runtime_ids=runtime_ids,
            system=system,
            systems=systems,
            include_disabled=include_disabled,
        )
    )


def load_runtime_transfer_catalog(config_file=None, transfers_file=None, runtime_ids=None):
    """Load transfers with build/runtime validation enabled."""
    return load_transfer_catalog(
        config_file=config_file,
        transfers_file=transfers_file,
        require_runtime_files=True,
        runtime_ids=runtime_ids,
    )


def load_runtime_transfer_definitions(config_file=None, transfers_file=None, runtime_ids=None):
    """Load transfer definitions with build/runtime validation enabled."""
    return load_transfer_definitions(
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


def load_reporting_transfer_definitions(
    config_file=None,
    transfers_file=None,
    runtime_ids=None,
    system=None,
):
    """Load transfer definitions with reporting/analysis validation enabled."""
    return load_transfer_definitions(
        config_file=config_file,
        transfers_file=transfers_file,
        require_runtime_files=False,
        runtime_ids=runtime_ids,
        system=system,
    )


def load_monitoring_transfer_definitions(
    config_file=None,
    transfers_file=None,
    runtime_ids=None,
    system=None,
):
    """Load current monitoring inventory, including disabled definitions."""
    definitions = load_transfer_definitions(
        config_file=config_file,
        transfers_file=transfers_file,
        require_runtime_files=False,
        runtime_ids=[],
        system=system,
        include_disabled=True,
    )
    if runtime_ids is None:
        return definitions
    selected_runtime_ids = set(runtime_ids)
    return [
        definition
        for definition in definitions
        if definition.runtime_id in selected_runtime_ids
    ]
