"""Tests for ``core.primary_reason_resolver``."""
from __future__ import annotations

import pytest

from core.primary_reason_resolver import (
    DEFAULT_EXCLUDED,
    DEFAULT_PRIORITY,
    load_excluded,
    load_priority,
    reason_key,
    resolve_primary_reason,
)


# ---------------------------------------------------------------------------
# reason_key
# ---------------------------------------------------------------------------

def test_reason_key_basic():
    assert reason_key("obi_stable=2.10") == "obi_stable"
    assert reason_key("weak_progress=1") == "weak_progress"
    assert reason_key("absorption") == "absorption"
    assert reason_key("") == ""
    assert reason_key("CVDR=0.5") == "cvdr"  # lowercased


# ---------------------------------------------------------------------------
# resolve_primary_reason — exclusion
# ---------------------------------------------------------------------------

def test_excludes_weak_progress_by_default():
    """The original bug: weak_progress always wins because it is appended first."""
    confirmations = ["weak_progress=1", "obi_stable=2.10", "fp_edge_absorb=1.3"]
    assert resolve_primary_reason(confirmations) == "fp_edge_absorb"


def test_excludes_weak_recent_by_default():
    confirmations = ["weak_recent=3/5", "absorption=12.5"]
    assert resolve_primary_reason(confirmations) == "absorption"


def test_all_excluded_falls_back():
    """Only weakness markers present → fallback wins (NOT a weakness tag)."""
    confirmations = ["weak_progress=1", "weak_recent=3/5"]
    assert resolve_primary_reason(confirmations, fallback="delta_spike") == "delta_spike"


# ---------------------------------------------------------------------------
# resolve_primary_reason — priority
# ---------------------------------------------------------------------------

def test_priority_picks_iceberg_over_delta_spike():
    confirmations = ["delta_spike=2.5", "iceberg=10000", "absorption=8"]
    assert resolve_primary_reason(confirmations) == "iceberg"


def test_priority_picks_absorption_over_obi_stable():
    """absorption is ranked above obi_stable in DEFAULT_PRIORITY."""
    confirmations = ["obi_stable=3", "absorption=15"]
    assert resolve_primary_reason(confirmations) == "absorption"


def test_priority_picks_sweep_first():
    confirmations = ["iceberg=1", "sweep_eqh=1", "absorption=1"]
    assert resolve_primary_reason(confirmations) == "sweep_eqh"


# ---------------------------------------------------------------------------
# resolve_primary_reason — fallback paths
# ---------------------------------------------------------------------------

def test_empty_confirmations_returns_fallback():
    assert resolve_primary_reason([]) == "delta_spike"
    assert resolve_primary_reason(None) == "delta_spike"  # type: ignore[arg-type]


def test_unknown_key_preserves_first_seen():
    """When no priority key matches, the first non-excluded, non-empty key wins."""
    confirmations = ["mystery_thing=1", "another_one=2"]
    assert resolve_primary_reason(confirmations) == "mystery_thing"


def test_empty_string_skipped():
    confirmations = ["", "absorption=1"]
    assert resolve_primary_reason(confirmations) == "absorption"


def test_duplicate_keys_deduped():
    confirmations = ["delta_spike=1", "delta_spike=2", "obi_stable=3"]
    assert resolve_primary_reason(confirmations) == "obi_stable"  # obi_stable > delta_spike


# ---------------------------------------------------------------------------
# resolve_primary_reason — custom config
# ---------------------------------------------------------------------------

def test_custom_priority_override():
    custom = ["delta_spike", "obi_stable"]
    confirmations = ["obi_stable=1", "delta_spike=1"]
    assert resolve_primary_reason(confirmations, priority=custom) == "delta_spike"


def test_custom_excluded_can_unblock_weak_progress():
    """Empty excluded set restores legacy behaviour where weak_progress is allowed."""
    confirmations = ["weak_progress=1", "obi_stable=2"]
    out = resolve_primary_reason(confirmations, excluded=set())
    assert out == "obi_stable"  # priority still picks the stronger one


def test_custom_excluded_only_weak_progress_left():
    """When excluded is empty AND only weak_progress is present → weak_progress wins."""
    confirmations = ["weak_progress=1"]
    out = resolve_primary_reason(confirmations, excluded=set())
    assert out == "weak_progress"


def test_custom_fallback_value():
    assert resolve_primary_reason([], fallback="custom_default") == "custom_default"


# ---------------------------------------------------------------------------
# Env loaders
# ---------------------------------------------------------------------------

def test_load_priority_default(monkeypatch):
    monkeypatch.delenv("PRIMARY_REASON_PRIORITY", raising=False)
    pri = load_priority()
    assert pri == [r.lower() for r in DEFAULT_PRIORITY]


def test_load_priority_env_override(monkeypatch):
    monkeypatch.setenv("PRIMARY_REASON_PRIORITY", "obi_stable, absorption, delta_spike")
    pri = load_priority()
    assert pri == ["obi_stable", "absorption", "delta_spike"]


def test_load_excluded_default(monkeypatch):
    monkeypatch.delenv("EXCLUDE_AS_PRIMARY_REASONS", raising=False)
    ex = load_excluded()
    assert ex == set(DEFAULT_EXCLUDED)


def test_load_excluded_env_override(monkeypatch):
    monkeypatch.setenv("EXCLUDE_AS_PRIMARY_REASONS", "weak_progress")
    ex = load_excluded()
    assert ex == {"weak_progress"}
    assert "weak_recent" not in ex  # default no longer present


def test_load_excluded_env_disable(monkeypatch):
    """Empty env disables exclusion entirely."""
    monkeypatch.setenv("EXCLUDE_AS_PRIMARY_REASONS", "")
    assert load_excluded() == set()


# ---------------------------------------------------------------------------
# Regression: the exact 2026-05-18 bug
# ---------------------------------------------------------------------------

def test_regression_weak_progress_first_loses_to_obi(monkeypatch):
    """Before fix: confirmations[0]='weak_progress' → primary_reason='weak_progress'.

    After fix: weak_progress is excluded → obi_stable wins.
    """
    monkeypatch.delenv("PRIMARY_REASON_PRIORITY", raising=False)
    monkeypatch.delenv("EXCLUDE_AS_PRIMARY_REASONS", raising=False)
    confirmations = [
        "weak_progress=1",      # appended at line ~2685 (before everything else)
        "obi_stable=2.10",      # appended at line ~2707
        "fp_edge_absorb=1.2",   # appended at line ~2734
    ]
    assert resolve_primary_reason(confirmations) == "fp_edge_absorb"


@pytest.mark.parametrize("excluded_tag", ["weak_progress", "weak_recent"])
def test_excluded_tags_never_chosen(excluded_tag):
    """Weakness markers are never selected even if they're the only known key."""
    confirmations = [f"{excluded_tag}=1", "unknown_key=1"]
    out = resolve_primary_reason(confirmations)
    # Excluded tag is dropped, unknown_key has no priority entry → first surviving.
    assert out == "unknown_key"
