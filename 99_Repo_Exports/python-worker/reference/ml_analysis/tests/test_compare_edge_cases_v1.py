"""Additional tests for golden_replay/compare.py edge cases."""

from ml_analysis.golden_replay.compare import (
    _as_float
    _is_number
    diff_objects
    float_close
    stable_hash
)


# ── _as_float edge cases ─────────────────────────────────────────────────────

def test_as_float_none_returns_none():
    assert _as_float(None) is None


def test_as_float_bool_returns_none():
    # bool is a subclass of int; must NOT be treated as a number here.
    assert _as_float(True) is None
    assert _as_float(False) is None


def test_as_float_nan_str_returns_none():
    assert _as_float("nan") is None
    assert _as_float(float("nan")) is None


def test_as_float_inf_returns_none():
    assert _as_float(float("inf")) is None
    assert _as_float(float("-inf")) is None
    assert _as_float("inf") is None


def test_as_float_valid_numeric_types():
    assert _as_float(1) == 1.0
    assert _as_float(3.14) == 3.14
    assert _as_float("2.5") == 2.5


def test_as_float_non_numeric_string_returns_none():
    assert _as_float("hello") is None
    assert _as_float({}) is None


# ── diff_objects: ignore_paths ────────────────────────────────────────────────

def test_diff_objects_ignore_paths_suppresses_diffs():
    a = {"x": 1.0, "y": {"z": 999.0}}
    b = {"x": 1.0, "y": {"z": 1.0}}
    # With ignore, no diff on y.z
    diffs = diff_objects(a, b, ignore_paths=["y"])
    assert len(diffs) == 0

    # Without ignore, diff is emitted
    diffs2 = diff_objects(a, b)
    assert len(diffs2) > 0


def test_diff_objects_ignore_paths_exact_key():
    a = {"ts": 1000, "v": 1.0}
    b = {"ts": 2000, "v": 1.0}
    diffs = diff_objects(a, b, ignore_paths=["ts"])
    assert len(diffs) == 0


# ── diff_objects: list length mismatch ───────────────────────────────────────

def test_diff_objects_list_length_mismatch_emits_len_diff():
    a = {"items": [1, 2, 3]}
    b = {"items": [1, 2]}
    diffs = diff_objects(a, b)
    assert any(d.kind == "len" for d in diffs)
    assert any("__len__" in d.path for d in diffs)


# ── float_close: bool input ───────────────────────────────────────────────────

def test_float_close_with_bool_falls_back_to_equality():
    # _as_float(True) returns None → falls back to a == b
    assert float_close(True, True)
    assert not float_close(True, False)
