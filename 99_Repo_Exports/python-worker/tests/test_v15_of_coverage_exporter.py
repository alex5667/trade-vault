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
