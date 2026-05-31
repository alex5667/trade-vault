"""test_v15_of_coverage_exporter.py

Unit tests for the pure-Python pieces of
`orderflow_services/v15_of_coverage_exporter_v1.py`:

  1. Key→group map covers all 531 v15_of numeric keys (no key falls through
     to an unknown bucket — every key lands in a real group or `v14_of_base`).
  2. Group sizes sum to TOTAL_KEYS (no double-counting / dropouts).
  3. _extract_indicators handles flat / nested / JSON-blob shapes.
  4. _compute_window produces coverage and zero_rate per key correctly.
"""
from __future__ import annotations

import json

import pytest


def test_key_to_group_covers_all_v15_of_keys():
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS
    from orderflow_services.v15_of_coverage_exporter_v1 import KEY_TO_GROUP

    missing = [k for k in V15_OF_NUMERIC_KEYS if k not in KEY_TO_GROUP]
    assert missing == [], (
        f"{len(missing)} v15_of keys have no group assignment "
        f"(first 10: {missing[:10]}). Update _build_key_to_group or add a "
        "residual bucket."
    )


def test_total_keys_invariant():
    from orderflow_services.v15_of_coverage_exporter_v1 import TOTAL_KEYS, GROUP_SIZES

    assert TOTAL_KEYS == 531
    assert sum(GROUP_SIZES.values()) == TOTAL_KEYS, (
        f"Group sizes sum to {sum(GROUP_SIZES.values())}, expected {TOTAL_KEYS}. "
        "Likely a key appears in multiple _GROUP_* lists."
    )


def test_extract_indicators_flat():
    from orderflow_services.v15_of_coverage_exporter_v1 import _extract_indicators, KEY_TO_GROUP
    sample_key = next(iter(KEY_TO_GROUP))
    out = _extract_indicators({sample_key: 1.5, "unrelated": "x"})
    assert out.get(sample_key) == 1.5
    assert "unrelated" not in out


def test_extract_indicators_nested_dict():
    from orderflow_services.v15_of_coverage_exporter_v1 import _extract_indicators
    fields = {"indicators": {"k": 1}}
    assert _extract_indicators(fields) == {"k": 1}


def test_extract_indicators_json_blob_data():
    from orderflow_services.v15_of_coverage_exporter_v1 import _extract_indicators
    payload = {"indicators": {"hawkes_taker_buy_lam": 0.42}}
    fields = {"data": json.dumps(payload)}
    out = _extract_indicators(fields)
    assert out.get("hawkes_taker_buy_lam") == 0.42


def test_extract_indicators_json_blob_top_level():
    from orderflow_services.v15_of_coverage_exporter_v1 import _extract_indicators
    fields = {"indicators": json.dumps({"k": 5})}
    out = _extract_indicators(fields)
    assert out == {"k": 5}


def test_compute_window_coverage_and_zero_rate():
    from orderflow_services.v15_of_coverage_exporter_v1 import (
        _compute_window,
        KEY_TO_GROUP,
    )
    # Pick two real v15_of keys.
    k_present, k_missing = list(KEY_TO_GROUP)[:2]

    records = [
        {"indicators": {k_present: 1.0}},        # present, nonzero
        {"indicators": {k_present: 0.0}},        # present, zero
        {"indicators": {k_present: 2.0}},        # present, nonzero
        {"indicators": {}},                       # neither present
    ]
    stats = _compute_window(records)

    s = stats[k_present]
    assert s["coverage"] == pytest.approx(3 / 4)
    # 1 of 3 present samples is zero → zero_rate=1/3
    assert s["zero_rate"] == pytest.approx(1 / 3)
    assert s["n"] == 4.0

    s2 = stats[k_missing]
    assert s2["coverage"] == 0.0
    assert s2["zero_rate"] == 0.0


def test_window_ms_env_default_is_4h():
    """Guard the canonical time-window cap. Coverage on a slow shadow signal
    stream needs a 4h window so infrequent signals accumulate enough samples."""
    import os

    os.environ.pop("V15_OF_COV_WINDOW_MS", None)
    # Re-execute module-level code is too invasive; instead assert the env
    # var name + default value via source-level grep.
    import pathlib
    src = pathlib.Path(
        "/home/alex/front/trade/scanner_infra/python-worker/orderflow_services/"
        "v15_of_coverage_exporter_v1.py"
    ).read_text(encoding="utf-8")
    assert 'V15_OF_COV_WINDOW_MS' in src
    assert '"14400000"' in src  # 4h
    assert 'r.xrange' in src   # time-window read path


def test_compute_window_handles_strings_and_none():
    from orderflow_services.v15_of_coverage_exporter_v1 import (
        _compute_window,
        KEY_TO_GROUP,
    )
    k = next(iter(KEY_TO_GROUP))
    records = [
        {"indicators": {k: None}},   # None → not present
        {"indicators": {k: ""}},     # empty string → not present
        {"indicators": {k: "abc"}},  # non-empty string → present but unparseable as float → counted as nonzero
        {"indicators": {k: 5}},
    ]
    stats = _compute_window(records)
    s = stats[k]
    assert s["coverage"] == pytest.approx(2 / 4)


# ---------------------------------------------------------------------------
# v15_of schema contract tests
# ---------------------------------------------------------------------------

def test_v15_of_schema_count():
    """V15_OF_NUMERIC_KEYS должен содержать ровно 531 ключ."""
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS
    assert len(V15_OF_NUMERIC_KEYS) == 531, (
        f"Expected 531 keys, got {len(V15_OF_NUMERIC_KEYS)}. "
        "Bump _EXPECTED_KEYS if schema was intentionally extended."
    )


def test_feature_registry_v15_spec_has_531_f_keys():
    """get_edge_stack_feature_spec('v15_of') должен содержать 531 f_* колонок."""
    from core.feature_registry import get_edge_stack_feature_spec
    spec = get_edge_stack_feature_spec("v15_of")
    assert spec.ver == "v15_of"
    f_cols = [c for c in spec.feature_cols if c.startswith("f_")]
    assert len(f_cols) == 531, (
        f"Expected 531 f_* cols in v15_of spec, got {len(f_cols)}"
    )


def test_train_row_builder_handles_bucket_hour_dow():
    """build_feature_row должен кодировать bucket:/hour:/dow: — не нули."""
    import datetime as _dt
    from tools.train_edge_stack_v1_oof import build_feature_row

    # 2026-03-04 10:30 UTC → hour=10, dow=2 (Wednesday)
    ts_ms = int(_dt.datetime(2026, 3, 4, 10, 30, 0, tzinfo=_dt.timezone.utc).timestamp() * 1000)

    cols = [
        "bucket:trend",
        "bucket:range",
        "bucket:other",
        "hour:10",
        "hour:11",
        "dow:2",
        "dow:3",
    ]
    row, _ = build_feature_row(
        feature_cols=cols,
        indicators={"spread_bps": 2.0, "expected_slippage_bps": 1.0, "exec_risk_norm": 0.5},
        direction="LONG",
        scenario="trend_breakout",
        ts_ms=ts_ms,
    )
    assert len(row) == len(cols)
    # scenario "trend_breakout" → bucket="trend"
    assert row[0] == 1.0, "bucket:trend should be 1.0"
    assert row[1] == 0.0, "bucket:range should be 0.0"
    assert row[2] == 0.0, "bucket:other should be 0.0"
    # hour:10 matches ts_ms hour
    assert row[3] == 1.0, "hour:10 should be 1.0"
    assert row[4] == 0.0, "hour:11 should be 0.0"
    # dow:2 = Wednesday matches
    assert row[5] == 1.0, "dow:2 should be 1.0"
    assert row[6] == 0.0, "dow:3 should be 0.0"


def test_train_row_builder_handles_dir_prefix():
    """build_feature_row должен кодировать dir:LONG / dir:SHORT из feature_registry."""
    from tools.train_edge_stack_v1_oof import build_feature_row

    ts_ms = 1_700_000_000_000
    cols = ["dir:LONG", "dir:SHORT"]
    row, _ = build_feature_row(
        feature_cols=cols,
        indicators={"spread_bps": 2.0, "expected_slippage_bps": 1.0},
        direction="SHORT",
        scenario="range",
        ts_ms=ts_ms,
    )
    assert row[0] == 0.0, "dir:LONG should be 0.0 for SHORT"
    assert row[1] == 1.0, "dir:SHORT should be 1.0 for SHORT"
