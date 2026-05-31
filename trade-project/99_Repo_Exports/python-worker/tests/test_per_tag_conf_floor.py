"""Tests for ``core.per_tag_conf_floor``."""
from __future__ import annotations

import pytest

from core.per_tag_conf_floor import (
    get_min_conf_for_tag,
    load_min_conf_by_tag,
    parse_min_conf_by_tag,
)


# ---------------------------------------------------------------------------
# parse_min_conf_by_tag
# ---------------------------------------------------------------------------

def test_parse_basic():
    assert parse_min_conf_by_tag("weak_progress:95,absorption:85,ok:75") == {
        "weak_progress": 95.0,
        "absorption": 85.0,
        "ok": 75.0,
    }


def test_parse_none_and_empty():
    assert parse_min_conf_by_tag(None) == {}
    assert parse_min_conf_by_tag("") == {}
    assert parse_min_conf_by_tag("   ") == {}


def test_parse_whitespace_lowercase():
    assert parse_min_conf_by_tag(" Weak_Progress : 92.5 , absorption:80 ") == {
        "weak_progress": 92.5,
        "absorption": 80.0,
    }


def test_parse_clamps_to_0_100():
    assert parse_min_conf_by_tag("a:-5,b:200,c:50") == {"a": 0.0, "b": 100.0, "c": 50.0}


def test_parse_skips_invalid_entries():
    assert parse_min_conf_by_tag("a:foo,b:80,no_colon") == {"b": 80.0}


def test_parse_nan_skipped():
    assert parse_min_conf_by_tag("a:nan") == {}


# ---------------------------------------------------------------------------
# get_min_conf_for_tag
# ---------------------------------------------------------------------------

def test_floor_raises_above_base():
    """Per-tag floor takes precedence when higher than base."""
    floors = {"weak_progress": 95.0}
    assert get_min_conf_for_tag("weak_progress", 75.0, floors=floors) == 95.0


def test_floor_never_lowers_base():
    """Per-tag is a *minimum* — never lowers the base floor."""
    floors = {"strong_signal": 50.0}
    assert get_min_conf_for_tag("strong_signal", 80.0, floors=floors) == 80.0


def test_unknown_tag_returns_base():
    floors = {"weak_progress": 95.0}
    assert get_min_conf_for_tag("absorption", 75.0, floors=floors) == 75.0


def test_empty_tag_returns_base():
    floors = {"weak_progress": 95.0}
    assert get_min_conf_for_tag("", 75.0, floors=floors) == 75.0
    assert get_min_conf_for_tag(None, 75.0, floors=floors) == 75.0


def test_case_insensitive_lookup():
    floors = {"weak_progress": 95.0}
    assert get_min_conf_for_tag("WEAK_PROGRESS", 75.0, floors=floors) == 95.0
    assert get_min_conf_for_tag("Weak_Progress", 75.0, floors=floors) == 95.0


def test_empty_floor_map_returns_base():
    assert get_min_conf_for_tag("weak_progress", 75.0, floors={}) == 75.0


def test_negative_base_treated_as_zero_on_unknown_tag():
    """Even with bogus base, returns base unchanged when tag unknown."""
    assert get_min_conf_for_tag("absorption", -10.0, floors={}) == -10.0


# ---------------------------------------------------------------------------
# Env loader
# ---------------------------------------------------------------------------

def test_load_from_env_default_empty(monkeypatch):
    monkeypatch.delenv("MIN_CONF_BY_TAG", raising=False)
    assert load_min_conf_by_tag() == {}


def test_load_from_env(monkeypatch):
    monkeypatch.setenv("MIN_CONF_BY_TAG", "weak_progress:95,absorption:85")
    assert load_min_conf_by_tag() == {"weak_progress": 95.0, "absorption": 85.0}


def test_get_min_conf_uses_env_when_floors_omitted(monkeypatch):
    monkeypatch.setenv("MIN_CONF_BY_TAG", "weak_progress:90")
    assert get_min_conf_for_tag("weak_progress", 75.0) == 90.0
    assert get_min_conf_for_tag("absorption", 75.0) == 75.0


# ---------------------------------------------------------------------------
# Regression: the exact 2026-05-18 scenario
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("base", [30.0, 50.0, 75.0])
def test_weak_progress_always_above_meme_relax(base):
    """Even after meme-relax pushes base to 30%, weak_progress floor holds."""
    floors = {"weak_progress": 95.0}
    assert get_min_conf_for_tag("weak_progress", base, floors=floors) == 95.0
