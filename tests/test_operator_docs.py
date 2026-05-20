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
