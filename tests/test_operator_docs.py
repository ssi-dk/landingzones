#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for operator-facing documentation."""

import os


APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def test_readme_documents_shared_lock_startup_jitter():
    """README should explain shared-lock jitter and operator verification."""
    readme_path = os.path.join(APP_ROOT, "README.md")
    readme_text = open(readme_path, "r").read()
    normalized_readme_text = " ".join(readme_text.split())

    assert "Shared Main Transfer Locks" in readme_text
    assert "startup jitter before acquiring the main `flock_file`" in normalized_readme_text
    assert "`flock -n`" in readme_text
    assert "Shared main transfer locks detected" in normalized_readme_text
    assert "transfer-status and notification-status locks" in normalized_readme_text
    assert "LZ_DEBUG_CLI=1" in readme_text
    assert "does not change the cron cadence" in normalized_readme_text


def test_readme_documents_transfer_catalog_loading_modes():
    """README should keep transfer-loading command boundaries explicit."""
    readme_path = os.path.join(APP_ROOT, "README.md")
    readme_text = open(readme_path, "r").read()
    normalized_readme_text = " ".join(readme_text.split())

    assert "Transfer Catalog Loading Modes" in readme_text
    assert "owner of transfer loading invariants" in normalized_readme_text
    assert "`load_runtime_transfer_catalog`" in readme_text
    assert "`load_reporting_transfer_catalog`" in readme_text
    assert "`landingzones build` uses the runtime catalog" in normalized_readme_text
    assert "`landingzones validate deployment` uses the runtime catalog" in normalized_readme_text
    assert "`landingzones validate integration` uses the runtime catalog" in normalized_readme_text
    assert "`landingzones validate separation` uses the reporting catalog" in normalized_readme_text
    assert "`landingzones report transfers` uses the reporting catalog" in normalized_readme_text
    assert "reporting analysis can omit runtime-only `log_file` and `flock_file` columns" in normalized_readme_text


def test_readme_documents_entry_point_readiness_staging():
    """README should describe readiness staging as a mitigation."""
    readme_path = os.path.join(APP_ROOT, "README.md")
    readme_text = open(readme_path, "r").read()
    normalized_readme_text = " ".join(readme_text.split())

    assert "Entry-Point Readiness Policies" in readme_text
    assert "`readiness_policy`" in readme_text
    assert "`direct`" in readme_text
    assert "`stable_snapshot`" in readme_text
    assert "mitigation, not a guarantee" in normalized_readme_text
    assert "producer-controlled atomic publish" in normalized_readme_text
    assert "`.landing_zones_readiness`" in readme_text


def test_context_names_transfer_catalog_as_invariant_owner():
    """Domain language should point future transfer-loading changes at catalog first."""
    context_path = os.path.join(APP_ROOT, "CONTEXT.md")
    context_text = open(context_path, "r").read()
    normalized_context_text = " ".join(context_text.split())

    assert "**Transfer Catalog**" in context_text
    assert "owner of transfer loading invariants" in normalized_context_text
    assert "Build/Runtime Catalog Loading" in context_text
    assert "Reporting Catalog Loading" in context_text


def test_readme_documents_execution_context_cron_activation():
    """README should describe the safe cron activation default."""
    readme_path = os.path.join(APP_ROOT, "README.md")
    readme_text = open(readme_path, "r").read()
    normalized_readme_text = " ".join(readme_text.split())

    assert "`--cron-scope execution-context`" in readme_text
    assert "same-context staged runtime cron fragments are preserved" in normalized_readme_text
    assert "`replace-selected`" in readme_text
    assert "`staged`" in readme_text
    assert "--exclude-cron-fragment" in readme_text
    assert "old-runtime.Landing_Zone.cron" in readme_text
    assert "Missing exclusion filenames are shown in the preview" in normalized_readme_text
