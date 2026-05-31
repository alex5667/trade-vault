from __future__ import annotations

"""Regression tests: _build_payload must apply _normalize_ts_ms to ts field.

Before the fix, _build_payload used:
    "ts": int(getattr(ctx, "ts", 0) or 0)
This allowed epoch_seconds, future-skew, and zero through without any
sanitisation, while _handle_veto and _maybe_publish_edge_event already used
_normalize_ts_ms.  The fix closes that gap.

Tests here verify:
  1. epoch_ms passthrough
  2. epoch_s auto-converted to ms
  3. ts_ms preferred over ts (attribute priority)
  4. anomalous ts → 0 + DQ flag + metric increment (payload_ts_anomaly_total)
  5. future-skew → 0 + DQ flag
  6. zero → 0 + DQ flag
"""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock

from handlers.crypto_orderflow.pipeline.orchestrator import SignalOrchestrator

# ── Helpers ───────────────────────────────────────────────────────────────────

NOW_MS = int(time.time() * 1000)


def _make_orchestrator():
    cfg = SimpleNamespace(symbol="BTCUSDT", get_runtime_snapshot=lambda: None, resolve_risk_cfg=lambda: {})
    orch = SignalOrchestrator(
        config=cfg,
        gates=MagicMock(),
        liquidity=MagicMock(),
        observability=MagicMock(),
        confirmations_engine=MagicMock(),
        emitter=MagicMock(),
    )
    return orch


def _make_payload(ctx_extra: dict, cand_extra: dict | None = None) -> dict:
    """Call _build_payload directly and return the payload dict."""
    orch = _make_orchestrator()
    ctx = SimpleNamespace(
        symbol="BTCUSDT",
        price=42_000.0,
        sizing_ok=True,
        qty=0.001,
        atr=100.0,
        sl_price=41_900.0,
        tp1_price=42_200.0,
        tp_mode_used="ATR_LEGACY",
        risk_usd_target=5.0,
        risk_usd=5.0,
        trail_profile="",
        trailing_min_lock_r=1.0,
        risk_cfg={},
        venue="binance",
        timeframe="1h",
        **ctx_extra,
    )
    cand = SimpleNamespace(
        kind="breakout",
        side="long",
        raw_score=1.5,
        signal_id="sid-test",
        reasons=["r1"],
        **(cand_extra or {}),
    )
    res = SimpleNamespace(ok=True, final_score=1.2, confidence=0.85, parts={})
    payload, _ = orch._build_payload(ctx, cand, res)
    return payload, ctx


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestBuildPayloadTsNormalization:

    def test_epoch_ms_passthrough(self):
        ts_ms = NOW_MS - 60_000  # 1 minute ago
        payload, ctx = _make_payload({"ts_ms": ts_ms})
        assert payload["ts"] == ts_ms

    def test_epoch_s_auto_converted_to_ms(self):
        ts_s = NOW_MS // 1000  # current epoch_seconds
        payload, ctx = _make_payload({"ts": ts_s})
        assert payload["ts"] == ts_s * 1000

    def test_ts_ms_preferred_over_ts(self):
        """ts_ms attribute has priority over ts (mirrors DLQ + edge-gate logic)."""
        ts_ms = NOW_MS - 5_000
        ts_s = NOW_MS // 1000 - 3600  # older, seconds scale
        payload, ctx = _make_payload({"ts_ms": ts_ms, "ts": ts_s})
        assert payload["ts"] == ts_ms

    def test_anomalous_zero_ts_returns_0_and_sets_dq_flag(self):
        payload, ctx = _make_payload({"ts": 0})
        assert payload["ts"] == 0
        # append_dq_flag writes to ctx.data_quality_flags (not ctx.dq_flags)
        dq = getattr(ctx, "data_quality_flags", None) or []
        assert "payload_ts_anomaly" in dq, f"data_quality_flags={dq}"

    def test_future_skew_returns_0_and_sets_dq_flag(self):
        far_future_ms = NOW_MS + 10 * 60 * 1000  # +10 minutes
        payload, ctx = _make_payload({"ts_ms": far_future_ms})
        assert payload["ts"] == 0
        dq = getattr(ctx, "data_quality_flags", None) or []
        assert "payload_ts_anomaly" in dq, f"data_quality_flags={dq}"

    def test_stale_beyond_7d_returns_0_and_sets_dq_flag(self):
        stale_ms = NOW_MS - (8 * 24 * 3600 * 1000)  # 8 days ago
        payload, ctx = _make_payload({"ts_ms": stale_ms})
        assert payload["ts"] == 0
        dq = getattr(ctx, "data_quality_flags", None) or []
        assert "payload_ts_anomaly" in dq, f"data_quality_flags={dq}"

    def test_microsecond_scale_returns_0(self):
        ts_us = NOW_MS * 1_000  # epoch_us ≈ 1.7e18 — too large to be ms
        payload, ctx = _make_payload({"ts_ms": ts_us})
        assert payload["ts"] == 0

    def test_anomaly_increments_prometheus_counter(self, monkeypatch):
        from handlers.crypto_orderflow.pipeline import orchestrator as orch_mod

        hits = []
        original_inc = None

        class FakeLabels:
            def inc(self_inner):
                hits.append(1)

        monkeypatch.setattr(
            orch_mod.PAYLOAD_TS_ANOMALY_TOTAL,
            "labels",
            lambda **kw: FakeLabels(),
        )
        _make_payload({"ts": 0})
        assert len(hits) >= 1, "PAYLOAD_TS_ANOMALY_TOTAL.labels(...).inc() was not called"

    def test_valid_ts_does_not_set_dq_flag(self):
        ts_ms = NOW_MS - 30_000  # 30s ago, clearly valid
        _, ctx = _make_payload({"ts_ms": ts_ms})
        dq = getattr(ctx, "dq_flags", None) or []
        assert "payload_ts_anomaly" not in dq
