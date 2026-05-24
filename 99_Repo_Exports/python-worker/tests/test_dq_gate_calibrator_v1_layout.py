"""Layout regression tests for dq_gate_calibrator_v1.

Guards two fallback paths added after v7 NDJSON capture was found to silently
zero out DQ inputs when the payload arrives in flat `decision_*`-prefixed form
(no nested `indicators` / `dq_components` dict).
"""
from __future__ import annotations

import json
from pathlib import Path

from orderflow_services.dq_gate_calibrator_v1 import (
    _extract_dq_indicators,
    load_from_dataset_ndjson,
)


def test_dq_indicators_fallback_to_flat_decision_keys():
    flat = {
        "sid": "crypto-of:BTCUSDT:1",
        "decision_tick_gap_p95_ms": 250.0,
        "decision_tick_missing_seq_ema": 0.05,
        "decision_book_missing_seq_ema": 0.12,
        "decision_tick_time_age_ms": 80.0,
    }
    ind, thr = _extract_dq_indicators(flat)
    assert ind["tick_gap_p95_ms"] == 250.0
    assert ind["tick_missing_seq_ema"] == 0.05
    assert ind["book_missing_seq_ema"] == 0.12
    assert ind["tick_time_age_ms"] == 80.0
    # Defaults preserved for unknown keys
    assert ind["data_health"] == 1.0
    assert ind["book_health_ok"] == 1.0
    assert thr == {}


def test_nested_dq_components_take_priority_over_flat():
    """Nested `dq_components` is canonical and must beat top-level `decision_*`."""
    payload = {
        "sid": "x",
        "dq_components": {
            "tick_gap_p95_ms": 999.0,
            "tick_missing_seq_ema": 0.9,
            "book_missing_seq_ema": 0.8,
        },
        # These must NOT override:
        "decision_tick_gap_p95_ms": 1.0,
        "decision_tick_missing_seq_ema": 0.001,
    }
    ind, _ = _extract_dq_indicators(payload)
    assert ind["tick_gap_p95_ms"] == 999.0
    assert ind["tick_missing_seq_ema"] == 0.9
    assert ind["book_missing_seq_ema"] == 0.8


def test_load_from_dataset_ndjson_uses_top_level_of_score_final(tmp_path: Path):
    """v7 NDJSON has `of_score_final` at top-level (no nested indicators).
    Old code did `ind.get(...) or ind.get(...) or 0.5` → silently defaulted to 0.5.
    Patch must propagate the top-level value (even when it's 0.0)."""
    p = tmp_path / "dataset.ndjson"
    rows = [
        {"sid": "crypto-of:BTC:1", "symbol": "BTCUSDT", "of_score_final": 0.0},
        {"sid": "crypto-of:BTC:2", "symbol": "BTCUSDT", "of_score_final": 0.83},
        {"sid": "crypto-of:BTC:3", "symbol": "BTCUSDT", "indicators": {"confidence": 0.71}},
    ]
    with p.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    samples = load_from_dataset_ndjson(str(p))
    by_sid = {s.sid: s for s in samples}
    # 0.0 must propagate, not get silently replaced with 0.5
    assert by_sid["BTC:1"].p_hat == 0.0
    assert abs(by_sid["BTC:2"].p_hat - 0.83) < 1e-9
    # Nested-indicators path still works
    assert abs(by_sid["BTC:3"].p_hat - 0.71) < 1e-9
