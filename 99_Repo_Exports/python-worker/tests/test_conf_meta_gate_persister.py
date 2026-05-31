"""Plan 1 Phase 5 — decision-stream persister tests.

Cover the payload-parse / row-build path used inside the XREADGROUP loop.
The full main() loop is exercised via integration testing in CI; here we
pin the pure functions so a regression at the contract level fails fast.
"""
from __future__ import annotations

import json

from services.conf_meta_gate_persister import (
    parse_decision_payload,
    payload_to_row,
)


def _base_payload() -> dict:
    return {
        "ts_ms": 1_700_000_000_000,
        "now_ms": 1_700_000_000_500,
        "sid": "sid-1",
        "symbol": "BTCUSDT",
        "kind": "edge_stack_v1",
        "side": "long",
        "mode": "SHADOW",
        "legacy_decision": "DENY",
        "meta_decision": "SHADOW_ALLOW",
        "active_decision": "DENY",
        "p_win_raw": 0.61,
        "p_win_calibrated": 0.58,
        "p_win_floor": 0.56,
        "expected_r": 0.04,
        "expected_edge_bps": 2.3,
        "risk_multiplier": 0.0,
        "canary_bucket": 321,
        "canary_selected": False,
        "model_ver": "conf_meta_gate_lr_v1_x",
        "schema_hash": "sh-1",
        "feature_cols_hash": "fch-1",
        "spread_bps": 1.0,
        "expected_slippage_bps": 1.0,
        "fee_bps": 1.0,
        "dq_score": 0.95,
        "regime": "trending_bull",
        "session": "us",
        "reason_codes": ["mode_shadow", "probability_ok", "edge_ok", "meta_allow"],
        "latency_ms": 1.8,
    }


def test_parse_decision_payload_handles_valid_json() -> None:
    raw = json.dumps(_base_payload())
    p = parse_decision_payload(raw)
    assert p is not None
    assert p["sid"] == "sid-1"


def test_parse_decision_payload_returns_none_on_bad_input() -> None:
    assert parse_decision_payload("") is None
    assert parse_decision_payload("{not json") is None
    assert parse_decision_payload("[1, 2, 3]") is None  # list, not dict


def test_payload_to_row_rejects_missing_sid() -> None:
    p = _base_payload()
    p["sid"] = ""
    assert payload_to_row(p) is None


def test_payload_to_row_rejects_missing_ts_ms() -> None:
    p = _base_payload()
    p["ts_ms"] = 0
    assert payload_to_row(p) is None


def test_payload_to_row_rejects_missing_symbol() -> None:
    p = _base_payload()
    p["symbol"] = ""
    assert payload_to_row(p) is None


def test_payload_to_row_rejects_missing_mode() -> None:
    p = _base_payload()
    p["mode"] = ""
    assert payload_to_row(p) is None


def test_payload_to_row_rejects_missing_meta_decision() -> None:
    p = _base_payload()
    p["meta_decision"] = ""
    assert payload_to_row(p) is None


def test_payload_to_row_full_shape() -> None:
    row = payload_to_row(_base_payload())
    assert row is not None
    # The INSERT order in _INSERT_SQL drives this tuple shape.
    assert row[0] == 1_700_000_000_000  # ts_ms
    assert row[1] == "sid-1"
    assert row[2] == "BTCUSDT"
    assert row[3] == "edge_stack_v1"
    assert row[4] == "long"
    assert row[5] == "SHADOW"
    # active = inferred: active_decision=DENY, legacy_decision=DENY → not active
    assert row[6] is False
    assert row[9] == "DENY"  # legacy_decision
    assert row[10] == "SHADOW_ALLOW"  # meta_decision
    assert row[11] == "DENY"  # active_decision
    # reason_codes JSON column (index 29 per _INSERT_SQL column order)
    rc = json.loads(row[29])
    assert "meta_allow" in rc


def test_payload_to_row_infers_active_flag_when_legacy_differs() -> None:
    p = _base_payload()
    p["legacy_decision"] = "DENY"
    p["active_decision"] = "ALLOW"
    row = payload_to_row(p)
    assert row is not None
    assert row[6] is True  # active


def test_payload_to_row_respects_explicit_active_field() -> None:
    p = _base_payload()
    p["active"] = True
    p["legacy_decision"] = "DENY"
    p["active_decision"] = "DENY"  # would infer False, but explicit wins
    row = payload_to_row(p)
    assert row is not None
    assert row[6] is True


def test_payload_to_row_preserves_features_jsonb() -> None:
    p = _base_payload()
    p["features"] = {"f0": 1.0, "f1": -0.5}
    row = payload_to_row(p)
    assert row is not None
    feats = json.loads(row[30])
    assert feats == {"f0": 1.0, "f1": -0.5}


def test_payload_to_row_handles_nullable_canary_bucket() -> None:
    p = _base_payload()
    p["canary_bucket"] = None
    row = payload_to_row(p)
    assert row is not None
    assert row[27] is None
