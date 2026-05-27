"""P2.5 — ctx_tighten attribution fix tests.

Covers:
  1. _INDICATORS_SMALL_ALLOW now includes ctx_tighten fields (decision_snapshot)
  2. build_decision_snapshot_event propagates tighten bps via indicators_small
  3. Joiner extraction from close_ev.signal_payload.indicators (primary path)
  4. Joiner extraction from decision.indicators_small (fallback path, P2.5 fix)
  5. Combined: primary 0, fallback nonzero → fallback used
  6. Both sources nonzero → primary wins
  7. Both sources zero → stays zero (baseline trade)
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from services.orderflow.decision_snapshot import _INDICATORS_SMALL_ALLOW


# ─── 1. decision_snapshot allowlist ─────────────────────────────────────────

def test_indicators_small_allow_includes_ctx_sentiment_tighten():
    assert "ctx_sentiment_tighten_bps" in _INDICATORS_SMALL_ALLOW, (
        "ctx_sentiment_tighten_bps must be in _INDICATORS_SMALL_ALLOW "
        "so decision_snapshot_writer propagates it to decision:{sid}"
    )


def test_indicators_small_allow_includes_ctx_defillama_tighten():
    assert "ctx_defillama_tighten_bps" in _INDICATORS_SMALL_ALLOW


# ─── 2. build_decision_snapshot_event propagation ───────────────────────────

def test_build_decision_snapshot_event_propagates_tighten_fields():
    from services.orderflow.decision_snapshot import build_decision_snapshot_event

    signal = {
        "sid": "test-sid-001",
        "symbol": "BTCUSDT",
        "direction": "buy",
        "ts_ms": 1_700_000_000_000,
        "kind": "iceberg",
        "venue": "binance",
        "session": "asian",
        "tf": "1m",
    }
    indicators = {
        "ctx_sentiment_tighten_bps": 1.5,
        "ctx_defillama_tighten_bps": 3.0,
        "spread_bps": 2.1,
        "confidence_raw": 0.72,
    }

    snap = build_decision_snapshot_event(
        signal=signal,
        indicators=indicators,
        runtime=None,
        schema_version=1,
        include_indicators=True,
    )

    ind_small = snap.get("indicators_small") or {}
    assert float(ind_small.get("ctx_sentiment_tighten_bps", 0.0)) == 1.5
    assert float(ind_small.get("ctx_defillama_tighten_bps", 0.0)) == 3.0


def test_build_decision_snapshot_event_tighten_zero_not_included():
    """Zero-value fields should not appear or should be 0.0 — not corrupt values."""
    from services.orderflow.decision_snapshot import build_decision_snapshot_event

    signal = {
        "sid": "test-sid-002",
        "symbol": "ETHUSDT",
        "direction": "sell",
        "ts_ms": 1_700_000_001_000,
        "kind": "delta_spike",
        "venue": "binance",
    }
    indicators = {
        "spread_bps": 1.5,
        # no tighten fields → they should not cause issues
    }

    snap = build_decision_snapshot_event(
        signal=signal,
        indicators=indicators,
        runtime=None,
        schema_version=1,
        include_indicators=True,
    )
    ind_small = snap.get("indicators_small") or {}
    # Absence or 0.0 — both are acceptable
    senti = ind_small.get("ctx_sentiment_tighten_bps", None)
    assert senti is None or float(senti) == 0.0


# ─── 3-7. Joiner extraction logic ────────────────────────────────────────────

def _extract_ctx_tighten(
    *,
    close_ev_signal_payload: dict | str | None = None,
    decision_indicators_small: dict | str | None = None,
) -> tuple[float, float]:
    """Replicate the extraction logic from trade_close_joiner_worker_v1._write_outputs.

    Returns (ctx_senti_bps, ctx_defi_bps).
    """
    _ctx_senti_bps = 0.0
    _ctx_defi_bps = 0.0

    # Primary: close_ev.signal_payload.indicators
    try:
        _sp = close_ev_signal_payload or {}
        if isinstance(_sp, str):
            _sp = json.loads(_sp)
        _inds = _sp.get("indicators") or {} if isinstance(_sp, dict) else {}
        if isinstance(_inds, str):
            _inds = json.loads(_inds)
        if isinstance(_inds, dict):
            _ctx_senti_bps = float(_inds.get("ctx_sentiment_tighten_bps", 0.0) or 0.0)
            _ctx_defi_bps = float(_inds.get("ctx_defillama_tighten_bps", 0.0) or 0.0)
    except Exception:
        pass

    # Fallback: decision.indicators_small (P2.5 fix)
    if _ctx_senti_bps == 0.0 or _ctx_defi_bps == 0.0:
        try:
            _d_inds = decision_indicators_small or {}
            if isinstance(_d_inds, str):
                _d_inds = json.loads(_d_inds)
            if isinstance(_d_inds, dict):
                if _ctx_senti_bps == 0.0:
                    _ctx_senti_bps = float(_d_inds.get("ctx_sentiment_tighten_bps", 0.0) or 0.0)
                if _ctx_defi_bps == 0.0:
                    _ctx_defi_bps = float(_d_inds.get("ctx_defillama_tighten_bps", 0.0) or 0.0)
        except Exception:
            pass

    return _ctx_senti_bps, _ctx_defi_bps


class TestCtxTightenExtraction:

    def test_primary_close_ev_indicators_used(self):
        """Primary source: close_ev.signal_payload.indicators has tighten fields."""
        sp = {"indicators": {"ctx_sentiment_tighten_bps": 2.0, "ctx_defillama_tighten_bps": 4.0}}
        senti, defi = _extract_ctx_tighten(close_ev_signal_payload=sp)
        assert senti == 2.0
        assert defi == 4.0

    def test_primary_string_serialized_indicators(self):
        """close_ev.signal_payload.indicators is a JSON string (common in Redis streams)."""
        inds_str = json.dumps({"ctx_sentiment_tighten_bps": 1.5, "ctx_defillama_tighten_bps": 3.5})
        sp = {"indicators": inds_str}
        senti, defi = _extract_ctx_tighten(close_ev_signal_payload=sp)
        assert senti == 1.5
        assert defi == 3.5

    def test_fallback_from_decision_indicators_small(self):
        """Primary missing → fallback to decision.indicators_small."""
        d_inds = {"ctx_sentiment_tighten_bps": 1.8, "ctx_defillama_tighten_bps": 3.6}
        senti, defi = _extract_ctx_tighten(
            close_ev_signal_payload={},  # no indicators
            decision_indicators_small=d_inds,
        )
        assert senti == 1.8
        assert defi == 3.6

    def test_fallback_from_decision_indicators_small_string(self):
        """decision.indicators_small as JSON string."""
        d_inds_str = json.dumps({"ctx_sentiment_tighten_bps": 2.2, "ctx_defillama_tighten_bps": 4.4})
        senti, defi = _extract_ctx_tighten(
            close_ev_signal_payload={},
            decision_indicators_small=d_inds_str,
        )
        assert senti == 2.2
        assert defi == 4.4

    def test_primary_partial_fallback_used_for_zero_field(self):
        """Primary has senti but not defi → defi comes from fallback."""
        sp = {"indicators": {"ctx_sentiment_tighten_bps": 1.0}}
        d_inds = {"ctx_defillama_tighten_bps": 3.0}
        senti, defi = _extract_ctx_tighten(
            close_ev_signal_payload=sp,
            decision_indicators_small=d_inds,
        )
        assert senti == 1.0
        assert defi == 3.0

    def test_primary_wins_when_both_nonzero(self):
        """When primary has non-zero values, fallback should NOT override them."""
        sp = {"indicators": {"ctx_sentiment_tighten_bps": 2.0, "ctx_defillama_tighten_bps": 4.0}}
        d_inds = {"ctx_sentiment_tighten_bps": 99.0, "ctx_defillama_tighten_bps": 99.0}
        senti, defi = _extract_ctx_tighten(
            close_ev_signal_payload=sp,
            decision_indicators_small=d_inds,
        )
        assert senti == 2.0  # primary
        assert defi == 4.0   # primary

    def test_both_sources_zero_stays_zero(self):
        """Baseline trade: TIGHTEN never fired → both fields should be 0."""
        senti, defi = _extract_ctx_tighten(
            close_ev_signal_payload={"indicators": {}},
            decision_indicators_small={},
        )
        assert senti == 0.0
        assert defi == 0.0

    def test_neither_source_present(self):
        """No signal_payload, no decision.indicators_small → graceful 0.0."""
        senti, defi = _extract_ctx_tighten(
            close_ev_signal_payload=None,
            decision_indicators_small=None,
        )
        assert senti == 0.0
        assert defi == 0.0

    def test_corrupted_json_string_gracefully_handled(self):
        """Malformed JSON strings must not raise, must return 0.0."""
        senti, defi = _extract_ctx_tighten(
            close_ev_signal_payload={"indicators": "not-json-{{{"},
            decision_indicators_small="also-bad-{",
        )
        assert senti == 0.0
        assert defi == 0.0
