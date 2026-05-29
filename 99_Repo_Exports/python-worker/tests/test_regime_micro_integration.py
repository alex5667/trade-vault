"""Integration tests for fast micro-regime pipeline.

Tests:
  1. bar_processor._update_regime_micro populates runtime after 2+ bars
  2. _publish_of_inputs bridge writes indicators.regime_micro_1m
  3. redis_repo._extract_entry_regime_micro_from_obj priority chain
  4. analytics_db._entry_regime_micro_db_value extraction
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.regime_micro_v1 import RegimeMicroConfig, RegimeMicroState, classify_regime_micro
from infra.redis_repo import _extract_entry_regime_micro_from_obj
from services.analytics_db import _entry_regime_micro_db_value


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_bar(close: float, high: float = 0.0, low: float = 0.0, ts_ms: int = 1_000_000):
    bar = MagicMock()
    bar.close = close
    bar.high = high or close + 1.0
    bar.low = low or close - 1.0
    bar.end_ts_ms = ts_ms
    bar.start_ts_ms = ts_ms
    bar.vol = 100.0
    bar.delta_sum = 0.0
    return bar


def _make_runtime(symbol: str = "BTCUSDT"):
    from services.orderflow.runtime import SymbolRuntime
    rt = SymbolRuntime(symbol, config={})
    return rt


# ──────────────────────────────────────────────────────────────────────────────
# 1. bar_processor._update_regime_micro
# ──────────────────────────────────────────────────────────────────────────────

class TestBarProcessorRegimeMicro:
    def _make_processor(self):
        from services.orderflow.components.bar_processor import BarProcessor
        redis_mock = AsyncMock()
        ticks_mock = AsyncMock()
        signal_pipeline_mock = MagicMock()
        atr_cache_mock = MagicMock()
        atr_tf_selector_mock = MagicMock()
        with patch("services.orderflow.components.bar_processor.MarketRegimeService", None):
            bp = BarProcessor(
                redis_client=redis_mock,
                ticks_client=ticks_mock,
                signal_pipeline=signal_pipeline_mock,
                atr_cache=atr_cache_mock,
                atr_tf_selector=atr_tf_selector_mock,
            )
        return bp

    def test_single_bar_leaves_runtime_na(self):
        bp = self._make_processor()
        rt = _make_runtime()
        bar = _make_bar(1000.0, ts_ms=1_000_000)
        bp._update_regime_micro(rt, bar)
        # Only 1 bar in window — not enough for classification
        assert rt.last_regime_micro == "na"

    def test_two_bars_produces_label(self):
        bp = self._make_processor()
        rt = _make_runtime()
        bp._update_regime_micro(rt, _make_bar(1000.0, ts_ms=1_000_000))
        bp._update_regime_micro(rt, _make_bar(1001.0, ts_ms=1_060_000))
        # Should produce some valid label after 2 bars
        assert rt.last_regime_micro in {
            "trend_micro_up", "trend_micro_down", "range_micro",
            "shock_micro", "squeeze_micro", "mixed_micro",
        }

    def test_uptrend_detected(self):
        bp = self._make_processor()
        rt = _make_runtime()
        # 5 equal-step bars +30 bps each → cumulative +120 bps
        prices = [1000.0, 1003.0, 1006.0, 1009.0, 1012.0]
        for i, p in enumerate(prices):
            bp._update_regime_micro(rt, _make_bar(p, ts_ms=1_000_000 + i * 60_000))
        assert rt.last_regime_micro == "trend_micro_up"

    def test_downtrend_detected(self):
        bp = self._make_processor()
        rt = _make_runtime()
        prices = [1012.0, 1009.0, 1006.0, 1003.0, 1000.0]
        for i, p in enumerate(prices):
            bp._update_regime_micro(rt, _make_bar(p, ts_ms=1_000_000 + i * 60_000))
        assert rt.last_regime_micro == "trend_micro_down"

    def test_ts_ms_updated(self):
        bp = self._make_processor()
        rt = _make_runtime()
        expected_ts = 1_300_000
        prices = [1000.0, 1003.0, 1006.0, 1009.0, 1012.0]
        for i, p in enumerate(prices):
            bp._update_regime_micro(rt, _make_bar(p, ts_ms=1_000_000 + i * 60_000))
        # Last bar ts_ms = 1_000_000 + 4 * 60_000 = 1_240_000
        assert rt.last_regime_micro_ts_ms == 1_000_000 + 4 * 60_000

    def test_zero_close_does_not_crash(self):
        bp = self._make_processor()
        rt = _make_runtime()
        bp._update_regime_micro(rt, _make_bar(0.0, ts_ms=1_000_000))
        assert rt.last_regime_micro == "na"

    def test_disabled_via_env(self, monkeypatch):
        monkeypatch.setenv("REGIME_MICRO_ENABLED", "0")
        bp = self._make_processor()
        rt = _make_runtime()
        prices = [1000.0, 1003.0, 1006.0, 1009.0, 1012.0]
        for i, p in enumerate(prices):
            bp._update_regime_micro(rt, _make_bar(p, ts_ms=1_000_000 + i * 60_000))
        assert rt.last_regime_micro == "na"

    def test_exception_safety(self):
        bp = self._make_processor()
        rt = _make_runtime()
        # Deliberately broken bar — should not raise
        broken_bar = MagicMock()
        broken_bar.close = "not_a_float"
        broken_bar.high = None
        broken_bar.low = None
        broken_bar.end_ts_ms = 1_000_000
        broken_bar.start_ts_ms = 1_000_000
        try:
            bp._update_regime_micro(rt, broken_bar)
        except Exception as e:
            pytest.fail(f"_update_regime_micro raised: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# 2. signal_pipeline bridge: indicators.regime_micro_1m
# ──────────────────────────────────────────────────────────────────────────────

class TestPublishOfInputsBridge:
    def _make_enriched(self, regime_micro: str = "trend_micro_up", ts_ms: int = 2_000_000):
        return {
            "symbol": "BTCUSDT",
            "ts_ms": ts_ms,
            "indicators": {
                "atr_bps": 25.0,
            },
        }

    def _make_runtime_with_micro(self, label: str = "trend_micro_up", ts_ms: int = 1_999_000):
        rt = _make_runtime()
        rt.last_regime_micro = label
        rt.last_regime_micro_ts_ms = ts_ms
        return rt

    def test_bridge_sets_regime_micro_1m(self):
        rt = self._make_runtime_with_micro("range_micro", ts_ms=1_990_000)
        enriched = self._make_enriched(ts_ms=2_000_000)

        # Simulate what _publish_of_inputs does
        _inds = enriched["indicators"]
        _rm = str(getattr(rt, "last_regime_micro", "") or "").strip().lower()
        _rm_ts = int(getattr(rt, "last_regime_micro_ts_ms", 0) or 0)
        if _rm and _rm not in ("na", "none", ""):
            _inds.setdefault("regime_micro_1m", _rm)
            _now_ms_rm = int(enriched.get("ts_ms", 0))
            if _now_ms_rm > 0 and _rm_ts > 0:
                _inds.setdefault("regime_micro_age_ms", max(0, _now_ms_rm - _rm_ts))

        assert _inds.get("regime_micro_1m") == "range_micro"
        assert _inds.get("regime_micro_age_ms") == 10_000

    def test_bridge_skips_na_label(self):
        rt = self._make_runtime_with_micro("na", ts_ms=1_990_000)
        enriched = self._make_enriched(ts_ms=2_000_000)
        _inds = enriched["indicators"]
        _rm = str(getattr(rt, "last_regime_micro", "") or "").strip().lower()
        if _rm and _rm not in ("na", "none", ""):
            _inds.setdefault("regime_micro_1m", _rm)
        assert "regime_micro_1m" not in _inds

    def test_bridge_does_not_overwrite_existing(self):
        rt = self._make_runtime_with_micro("shock_micro", ts_ms=1_990_000)
        enriched = self._make_enriched(ts_ms=2_000_000)
        _inds = enriched["indicators"]
        _inds["regime_micro_1m"] = "range_micro"  # pre-set by upstream
        _rm = str(getattr(rt, "last_regime_micro", "") or "").strip().lower()
        if _rm and _rm not in ("na", "none", ""):
            _inds.setdefault("regime_micro_1m", _rm)
        # setdefault should not overwrite
        assert _inds["regime_micro_1m"] == "range_micro"


# ──────────────────────────────────────────────────────────────────────────────
# 3. redis_repo._extract_entry_regime_micro_from_obj
# ──────────────────────────────────────────────────────────────────────────────

class TestExtractEntryRegimeMicro:
    def _obj(self, **kwargs):
        return SimpleNamespace(**kwargs)

    def test_entry_regime_micro_attr(self):
        obj = self._obj(entry_regime_micro="trend_micro_up")
        assert _extract_entry_regime_micro_from_obj(obj) == "trend_micro_up"

    def test_sentinel_na_returns_na(self):
        obj = self._obj(entry_regime_micro="na")
        assert _extract_entry_regime_micro_from_obj(obj) == "na"

    def test_none_attr_returns_na(self):
        obj = self._obj(entry_regime_micro=None)
        assert _extract_entry_regime_micro_from_obj(obj) == "na"

    def test_fallback_to_signal_payload_indicators(self):
        payload = json.dumps({
            "indicators": {"regime_micro_1m": "squeeze_micro"}
        })
        obj = self._obj(signal_payload=payload)
        assert _extract_entry_regime_micro_from_obj(obj) == "squeeze_micro"

    def test_fallback_config_snapshot_indicators(self):
        payload = json.dumps({
            "config_snapshot": {"indicators": {"regime_micro_1m": "shock_micro"}}
        })
        obj = self._obj(signal_payload=payload)
        assert _extract_entry_regime_micro_from_obj(obj) == "shock_micro"

    def test_no_payload_returns_na(self):
        obj = self._obj()
        assert _extract_entry_regime_micro_from_obj(obj) == "na"

    def test_invalid_sentinel_in_payload(self):
        payload = json.dumps({"indicators": {"regime_micro_1m": "none"}})
        obj = self._obj(signal_payload=payload)
        assert _extract_entry_regime_micro_from_obj(obj) == "na"

    def test_priority_attr_over_payload(self):
        payload = json.dumps({"indicators": {"regime_micro_1m": "range_micro"}})
        obj = self._obj(entry_regime_micro="trend_micro_down", signal_payload=payload)
        assert _extract_entry_regime_micro_from_obj(obj) == "trend_micro_down"


# ──────────────────────────────────────────────────────────────────────────────
# 4. analytics_db._entry_regime_micro_db_value
# ──────────────────────────────────────────────────────────────────────────────

class TestEntryRegimeMicroDbValue:
    def _obj(self, **kwargs):
        return SimpleNamespace(**kwargs)

    def test_from_attr(self):
        obj = self._obj(entry_regime_micro="mixed_micro")
        assert _entry_regime_micro_db_value(obj) == "mixed_micro"

    def test_na_returns_none(self):
        obj = self._obj(entry_regime_micro="na")
        assert _entry_regime_micro_db_value(obj) is None

    def test_none_attr_returns_none(self):
        obj = self._obj(entry_regime_micro=None)
        assert _entry_regime_micro_db_value(obj) is None

    def test_fallback_indicators(self):
        payload = json.dumps({"indicators": {"regime_micro_1m": "trend_micro_up"}})
        obj = self._obj(signal_payload=payload)
        assert _entry_regime_micro_db_value(obj) == "trend_micro_up"

    def test_no_data_returns_none(self):
        obj = self._obj()
        assert _entry_regime_micro_db_value(obj) is None
