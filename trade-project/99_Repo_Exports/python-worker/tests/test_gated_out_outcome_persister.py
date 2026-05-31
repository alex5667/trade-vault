"""Unit tests for services.gated_out_outcome_persister.

Cover only the pure-function surface (parsing + row shaping).
Loop / Redis / DB integration is exercised via shadow-mode service tests
in integration suites.
"""
from __future__ import annotations

import json

import pytest

from services.gated_out_outcome_persister import (
    parse_outcome_payload,
    payload_to_row,
)


def _outcome_payload(**overrides):
    base = {
        "v": 2,
        "sid": "of:BTCUSDT:LONG:123",
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "entry": 50000.0,
        "ts_ms": 1717000000000,
        "ts_close_ms": 1717000300000,
        "horizon_ms": 1800000,
        "close_price": 50100.0,
        "high": 50150.0,
        "low": 49980.0,
        "ret_bps": 20.0,
        "r_mult": 2.0,
        "y": 1,
        "y_edge": 1,
        "y_edge_cost_aware": 1,
        "cost_bps": 11.0,
        "cost_fees_bps": 10.0,
        "cost_spread_bps": 0.5,
        "cost_slippage_bps": 0.5,
        "edge_after_cost_bps": 9.0,
        "outcome": "TP_HIT",
        "tp_hit": 1,
        "sl_hit": 0,
        "tp_bps": 15.0,
        "sl_bps": 10.0,
        "confidence": 0.65,
        "min_conf": 0.7,
        "primary": 1,
        "gated_out": 1,
        "sample_policy": "deep_explore_v1",
        "selection_policy_version": "v1",
        "selection_prob": 0.05,
        "selection_weight": 20.0,
        "virtual_min_conf": 0.6,
        "meets_virtual_threshold": 1,
        "kind": "iceberg",
    }
    base.update(overrides)
    return base


def test_parse_outcome_payload_json():
    raw = json.dumps(_outcome_payload())
    out = parse_outcome_payload(raw)
    assert out is not None
    assert out["sid"] == "of:BTCUSDT:LONG:123"


def test_parse_outcome_payload_bad_json_returns_none():
    assert parse_outcome_payload("{not json}") is None
    assert parse_outcome_payload("") is None
    assert parse_outcome_payload(None) is None
    # non-dict JSON
    assert parse_outcome_payload(json.dumps([1, 2, 3])) is None


def test_payload_to_row_tp_hit_long():
    row = payload_to_row(_outcome_payload(), now_ms=1717000400000)
    assert row is not None
    # Spot-check shape
    assert row[0] == "of:BTCUSDT:LONG:123"   # sid
    assert row[1] == 1717000000000           # ts_ms
    assert row[3] == "BTCUSDT"               # symbol
    assert row[4] == 1                       # direction +1 long
    assert row[5] == "iceberg"               # kind
    assert row[13] == "TP_HIT"               # outcome
    assert row[14] == 1                      # label TP=+1
    assert row[20] == 1                      # tp_hit
    assert row[21] == 0                      # sl_hit
    assert row[22] == 1                      # y_edge_cost_aware
    assert row[28] == "deep_explore_v1"      # sample_policy


def test_payload_to_row_sl_hit_short():
    p = _outcome_payload(direction="SHORT", outcome="SL_HIT", tp_hit=0, sl_hit=1,
                         y_edge_cost_aware=0)
    row = payload_to_row(p, now_ms=1)
    assert row is not None
    assert row[4] == -1                     # direction SHORT
    assert row[13] == "SL_HIT"
    assert row[14] == -1                    # label SL=-1


def test_payload_to_row_timeout():
    p = _outcome_payload(outcome="TIMEOUT", tp_hit=0, sl_hit=0, y_edge_cost_aware=0)
    row = payload_to_row(p, now_ms=1)
    assert row is not None
    assert row[13] == "TIMEOUT"
    assert row[14] == 0                     # label TIMEOUT=0


def test_payload_to_row_missing_sid_rejects():
    p = _outcome_payload(sid="")
    assert payload_to_row(p, now_ms=1) is None


def test_payload_to_row_missing_ts_ms_rejects():
    p = _outcome_payload(ts_ms=0)
    assert payload_to_row(p, now_ms=1) is None


def test_payload_to_row_bad_direction_rejects():
    p = _outcome_payload(direction="FLAT")
    assert payload_to_row(p, now_ms=1) is None


def test_payload_to_row_unknown_outcome_rejects():
    p = _outcome_payload(outcome="WTF")
    assert payload_to_row(p, now_ms=1) is None


def test_payload_to_row_zero_entry_rejects():
    p = _outcome_payload(entry=0)
    assert payload_to_row(p, now_ms=1) is None


def test_payload_to_row_missing_horizon_rejects():
    p = _outcome_payload()
    p.pop("horizon_ms")
    assert payload_to_row(p, now_ms=1) is None


def test_payload_to_row_none_optionals_persisted_as_none():
    p = _outcome_payload(kind="", sample_policy="", selection_prob=None)
    row = payload_to_row(p, now_ms=1)
    assert row is not None
    assert row[5] is None                   # kind empty → None
    assert row[28] is None                  # sample_policy empty → None
    assert row[30] is None                  # selection_prob None pass-through


def test_payload_to_row_nan_inf_floats_become_none():
    p = _outcome_payload(ret_bps=float("nan"), r_mult=float("inf"))
    row = payload_to_row(p, now_ms=1)
    assert row is not None
    assert row[18] is None                  # ret_bps NaN → None
    assert row[19] is None                  # r_mult inf → None


def test_payload_to_row_label_matches_signal_outcome_convention():
    # Plan 2 invariant: gated_out label semantics align with signal_outcome
    # so signal_outcome_unified VIEW UNION is correct without remap.
    tp = payload_to_row(_outcome_payload(outcome="TP_HIT"), now_ms=1)
    sl = payload_to_row(_outcome_payload(outcome="SL_HIT"), now_ms=1)
    timeout = payload_to_row(_outcome_payload(outcome="TIMEOUT"), now_ms=1)
    assert tp[14] == 1
    assert sl[14] == -1
    assert timeout[14] == 0
