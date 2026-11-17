#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Config-aware transfer catalog loading.

The catalog is the loading seam for normalized transfer rows. Existing parser
callers can still use ``generate_cron_files.parse_transfers_file`` directly
while command paths move behind this smaller interface.
"""

import csv
from pathlib import Path

import yaml

from landingzones.config import config
from landingzones.transfer_definitions import TransferDefinition, definitions_from_dataframe


def load_transfer_catalog(
    config_file=None,
    transfers_file=None,
    require_runtime_files=True,
    runtime_ids=None,
    system=None,
    systems=None,
    include_disabled=False,
    allow_missing_runtime_ids=False,
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
    if allow_missing_runtime_ids:
        parse_options["allow_missing_runtime_ids"] = True
    return gcf.parse_transfers_file(config.transfers_file, **parse_options)


def load_transfer_definitions(
    config_file=None,
    transfers_file=None,
    require_runtime_files=True,
    runtime_ids=None,
    system=None,
    systems=None,
    include_disabled=False,
    allow_missing_runtime_ids=False,
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
            allow_missing_runtime_ids=allow_missing_runtime_ids,
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
    config.load_config(config_file=config_file, transfers_file=transfers_file)
    if config.config_file:
        with open(config.config_file) as handle:
            settings = yaml.safe_load(handle) or {}
        if settings.get('execution_schema_version') == 1:
            return _load_execution_monitoring_definitions(
                config.config_file, transfers_file, runtime_ids, system,
            )
    parse_runtime_ids = [] if runtime_ids is None else runtime_ids
    definitions = load_transfer_definitions(
        config_file=config_file,
        transfers_file=transfers_file,
        require_runtime_files=False,
        runtime_ids=parse_runtime_ids,
        system=system,
        include_disabled=True,
        allow_missing_runtime_ids=True,
    )
    if runtime_ids is None:
        return definitions
    selected_runtime_ids = set(runtime_ids)
    return [
        definition
        for definition in definitions
        if definition.runtime_id in selected_runtime_ids
    ]


def _load_execution_monitoring_definitions(config_file, transfers_file, runtime_ids, system):
    """Read Python-owned routes without authenticating or executing transfers."""
    from landingzones.execution.model import load_settings, remote_url, resolve_local_endpoint

    settings = load_settings(config_file, resolve_credentials=False, validate_context=False)
    selected = set(settings['runtime_ids'] if runtime_ids is None else runtime_ids)
    table = Path(transfers_file).resolve() if transfers_file else Path(settings['transfers_file'])
    definitions = []
    identities = set()
    with table.open(newline='') as handle:
        rows = csv.DictReader((line for line in handle if not line.startswith('#')), delimiter='\t')
        for row in rows:
            if None in row or any(value is None for value in row.values()):
                raise ValueError('Malformed transfer table')
            if row.get('executor', 'legacy') != 'python' or row.get('runtime_id') not in selected:
                continue
            if system and row.get('system') != system:
                continue
            if (row.get('system'), row.get('users')) != (
                settings['execution_context']['system'], settings['execution_context']['user'],
            ):
                raise ValueError('Selected route is owned by another execution context')
            identity = (row.get('runtime_id'), row.get('identifiers'))
            if not identity[1] or identity in identities:
                raise ValueError('Missing or duplicate transfer identity')
            identities.add(identity)
            for field in ('source', 'destination'):
                value = row.get(field, '')
                if value.startswith(('ssh://', 'sftp://')):
                    remote_url(value, value.split('://', 1)[0])
                elif field == 'destination' and row.get('adapter') == 'ena':
                    pass
                else:
                    row[field] = resolve_local_endpoint(value, settings.get('local_path_base'))
            definitions.append(TransferDefinition.from_row(row))
    return definitions
