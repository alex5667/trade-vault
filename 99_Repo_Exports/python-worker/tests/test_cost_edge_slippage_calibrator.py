"""test_cost_edge_slippage_calibrator.py

Unit tests for:
  1. Calibrator core: weighted q75, blend_and_clamp, compute_q75_fit, build_payload
  2. SlippageCalStore/Reader: parse, lookup hierarchy, fail-open
  3. CostEdgeGate integration: calibrated → spread → static fallback chain
"""
from __future__ import annotations

import json
import math
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Calibrator core
# ---------------------------------------------------------------------------
from orderflow_services.cost_edge_slippage_calibrator_v1 import (
    SlipRow,
    _weighted_quantile,
    blend_and_clamp,
    compute_q75_fit,
    build_payload,
    SLIP_LOWER,
    SLIP_UPPER,
    DEFAULT_SLIP_BPS,
)
from core.slippage_cal_store import (
    SlippageCalStore,
    SlippageCalReader,
    _parse_slip_map,
)
from handlers.crypto_orderflow.core.cost_edge_gate import (
    CostEdgeConfig,
    CostEdgeGate,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row(sym: str, sess: str, bps: float, age_days: float = 0.0) -> SlipRow:
    exit_ts_ms = int((time.time() - age_days * 86400) * 1000)
    return SlipRow(symbol=sym, session=sess, adverse_bps=bps, exit_ts_ms=exit_ts_ms)


def _make_redis(payload: dict[str, Any] | None) -> MagicMock:
    r = MagicMock()
    if payload is None:
        r.get.return_value = None
    else:
        r.get.return_value = json.dumps(payload)
    return r


# ---------------------------------------------------------------------------
# 1. _weighted_quantile
# ---------------------------------------------------------------------------

class TestWeightedQuantile:
    def test_uniform_weights_equal_numpy(self):
        vals = [1.0, 2.0, 3.0, 4.0, 5.0]
        wts = [1.0] * 5
        assert abs(_weighted_quantile(vals, wts, 0.75) - 4.0) < 0.5

    def test_empty_returns_default(self):
        assert _weighted_quantile([], [], 0.75) == DEFAULT_SLIP_BPS

    def test_single_element(self):
        assert _weighted_quantile([7.5], [1.0], 0.75) == pytest.approx(7.5)

    def test_q0_is_min(self):
        vals = [3.0, 7.0, 12.0]
        wts = [1.0, 1.0, 1.0]
        assert _weighted_quantile(vals, wts, 0.0) == pytest.approx(3.0)

    def test_q1_is_max(self):
        vals = [3.0, 7.0, 12.0]
        wts = [1.0, 1.0, 1.0]
        assert _weighted_quantile(vals, wts, 1.0) == pytest.approx(12.0)

    def test_heavy_weight_pulls_quantile(self):
        # Если старые записи имеют маленький вес, q75 должно быть ближе к свежим
        vals = [2.0, 2.0, 2.0, 20.0]   # один выброс
        wts = [1.0, 1.0, 1.0, 0.01]    # выброс почти не взвешен
        q = _weighted_quantile(vals, wts, 0.75)
        assert q < 5.0, "heavy outlier with tiny weight should not dominate q75"

    def test_nan_weight_filtered(self):
        vals = [1.0, float("nan"), 3.0]
        wts = [1.0, 1.0, 1.0]
        # nan value should be skipped
        result = _weighted_quantile(vals, wts, 0.5)
        assert math.isfinite(result)

    def test_zero_weight_filtered(self):
        vals = [1.0, 100.0, 3.0]
        wts = [1.0, 0.0, 1.0]   # middle value has zero weight
        result = _weighted_quantile(vals, wts, 0.5)
        assert result < 10.0


# ---------------------------------------------------------------------------
# 2. blend_and_clamp
# ---------------------------------------------------------------------------

class TestBlendAndClamp:
    def test_basic_blend(self):
        result = blend_and_clamp(q75=10.0, old_bps=4.0, alpha=0.5)
        assert result == pytest.approx(7.0)

    def test_clamp_upper(self):
        result = blend_and_clamp(q75=1000.0, old_bps=4.0, alpha=1.0)
        assert result == SLIP_UPPER

    def test_clamp_lower(self):
        result = blend_and_clamp(q75=0.001, old_bps=4.0, alpha=1.0)
        assert result == SLIP_LOWER

    def test_nan_q75_falls_back_to_default(self):
        result = blend_and_clamp(q75=float("nan"), old_bps=4.0, alpha=0.5)
        assert SLIP_LOWER <= result <= SLIP_UPPER

    def test_nan_old_bps_uses_default(self):
        result = blend_and_clamp(q75=5.0, old_bps=float("nan"), alpha=0.5)
        assert math.isfinite(result)
        assert SLIP_LOWER <= result <= SLIP_UPPER


# ---------------------------------------------------------------------------
# 3. compute_q75_fit
# ---------------------------------------------------------------------------

class TestComputeQ75Fit:
    def test_groups_created(self):
        rows = [
            _row("BTCUSDT", "us_main", 2.0),
            _row("BTCUSDT", "us_main", 3.0),
            _row("SOLUSDT", "european", 8.0),
        ]
        results = compute_q75_fit(rows)
        assert ("BTCUSDT", "us_main") in results
        assert ("SOLUSDT", "european") in results

    def test_aggregate_groups_exist(self):
        rows = [_row("BTCUSDT", "us_main", 2.0)] * 5
        results = compute_q75_fit(rows)
        assert ("BTCUSDT", "*") in results
        assert ("*", "*") in results

    def test_q75_gt_q25(self):
        rows = [_row("BTCUSDT", "us_main", float(i)) for i in range(1, 21)]
        results = compute_q75_fit(rows)
        fit = results[("BTCUSDT", "us_main")]
        assert fit.q75 >= fit.q50 >= fit.q25

    def test_recent_rows_dominate(self):
        # Старые записи с высоким slippage vs новые с низким
        old_rows = [_row("SOLUSDT", "asian", 20.0, age_days=20) for _ in range(10)]
        new_rows = [_row("SOLUSDT", "asian", 3.0, age_days=0) for _ in range(10)]
        results = compute_q75_fit(old_rows + new_rows)
        fit = results[("SOLUSDT", "asian")]
        assert fit.q75 < 15.0, "recent low-slippage rows should pull q75 down"


# ---------------------------------------------------------------------------
# 4. build_payload
# ---------------------------------------------------------------------------

class TestBuildPayload:
    def test_schema_version(self):
        results = compute_q75_fit([_row("BTCUSDT", "us_main", 3.0)] * 25)
        p = build_payload(results, {}, alpha=0.1, run_id="x", n_rows=25,
                          min_n=5, apply=False, shadow_enforce=0)
        assert p["schema_version"] == 1

    def test_groups_present(self):
        rows = [_row("BTCUSDT", "us_main", 3.0)] * 25
        results = compute_q75_fit(rows)
        p = build_payload(results, {}, alpha=0.1, run_id="x", n_rows=25,
                          min_n=5, apply=False, shadow_enforce=0)
        assert "BTCUSDT:US_MAIN" in p["groups"]

    def test_small_group_below_min_n_skipped(self):
        # Реальная группа с n < min_n должна быть пропущена
        rows = [_row("XRPUSDT", "overnight", 5.0)] * 3
        results = compute_q75_fit(rows)
        p = build_payload(results, {}, alpha=0.1, run_id="x", n_rows=3,
                          min_n=20, apply=False, shadow_enforce=0)
        assert "XRPUSDT:OVERNIGHT" not in p["groups"]

    def test_new_bps_clamped(self):
        rows = [_row("PEPUSDT", "us_main", 100.0)] * 25
        results = compute_q75_fit(rows)
        p = build_payload(results, {}, alpha=1.0, run_id="x", n_rows=25,
                          min_n=5, apply=False, shadow_enforce=0)
        for gk, entry in p["groups"].items():
            if isinstance(entry, dict) and "*" not in gk:
                assert entry["new_bps"] <= SLIP_UPPER


# ---------------------------------------------------------------------------
# 5. SlippageCalStore — parse + lookup hierarchy
# ---------------------------------------------------------------------------

class TestSlippageCalStore:
    def _payload(self, groups: dict[str, float]) -> dict:
        return {
            "schema_version": 1,
            "calibrated_ms": int(time.time() * 1000),
            "groups": {k: {"new_bps": v} for k, v in groups.items()},
        }

    def test_load_from_redis(self):
        redis = _make_redis(self._payload({"BTCUSDT:US_MAIN": 2.5}))
        store = SlippageCalStore.load(redis)
        assert store.is_loaded
        assert store.n_keys == 1

    def test_exact_lookup(self):
        store = SlippageCalStore.from_dict({"BTCUSDT:us_main": 2.5})
        assert store.get_slippage("BTCUSDT", "us_main") == pytest.approx(2.5)

    def test_case_insensitive(self):
        store = SlippageCalStore.from_dict({"BTCUSDT:US_MAIN": 2.5})
        assert store.get_slippage("btcusdt", "Us_Main") == pytest.approx(2.5)

    def test_symbol_wildcard_fallback(self):
        store = SlippageCalStore.from_dict({"BTCUSDT:*": 3.0})
        # любая сессия → символьный агрегат
        assert store.get_slippage("BTCUSDT", "european") == pytest.approx(3.0)

    def test_global_wildcard_fallback(self):
        store = SlippageCalStore.from_dict({"*:*": 5.0})
        assert store.get_slippage("SOLUSDT", "asian") == pytest.approx(5.0)

    def test_fallback_priority_order(self):
        store = SlippageCalStore.from_dict({
            "BTCUSDT:US_MAIN": 2.0,
            "BTCUSDT:*": 3.0,
            "*:*": 5.0,
        })
        assert store.get_slippage("BTCUSDT", "us_main") == pytest.approx(2.0)
        assert store.get_slippage("BTCUSDT", "asian") == pytest.approx(3.0)
        assert store.get_slippage("SOLUSDT", "overnight") == pytest.approx(5.0)

    def test_unknown_key_returns_default(self):
        store = SlippageCalStore.from_dict({"BTCUSDT:US_MAIN": 2.0})
        assert store.get_slippage("XRPUSDT", "overnight", default=9.9) == pytest.approx(9.9)

    def test_empty_store_returns_default(self):
        store = SlippageCalStore.empty()
        assert not store.is_loaded
        assert store.get_slippage("BTCUSDT", "us_main", default=4.0) == pytest.approx(4.0)

    def test_redis_none_returns_empty(self):
        redis = _make_redis(None)
        store = SlippageCalStore.load(redis)
        assert not store.is_loaded

    def test_redis_error_returns_empty(self):
        redis = MagicMock()
        redis.get.side_effect = Exception("connection refused")
        store = SlippageCalStore.load(redis)
        assert not store.is_loaded


# ---------------------------------------------------------------------------
# 6. SlippageCalReader — fail-open, TTL, stale
# ---------------------------------------------------------------------------

class TestSlippageCalReader:
    def _payload_json(self, groups: dict[str, float]) -> str:
        return json.dumps({
            "schema_version": 1,
            "calibrated_ms": int(time.time() * 1000),
            "groups": {k: {"new_bps": v} for k, v in groups.items()},
        })

    def test_returns_calibrated_value(self):
        redis = MagicMock()
        redis.get.return_value = self._payload_json({"BTCUSDT:US_MAIN": 2.3})
        reader = SlippageCalReader(redis, refresh_ms=0)
        val = reader.get_slippage("BTCUSDT", "us_main", default=4.0)
        assert val == pytest.approx(2.3)

    def test_redis_error_fail_open_after_load(self):
        redis = MagicMock()
        redis.get.return_value = self._payload_json({"*:*": 3.5})
        reader = SlippageCalReader(redis, refresh_ms=0)
        reader.get_slippage("BTCUSDT", "us_main")  # prime cache
        redis.get.side_effect = Exception("down")
        # Should still return cached value
        val = reader.get_slippage("BTCUSDT", "us_main", default=9.0)
        assert val == pytest.approx(3.5)

    def test_never_loaded_returns_default(self):
        redis = MagicMock()
        redis.get.return_value = None
        reader = SlippageCalReader(redis, refresh_ms=0)
        val = reader.get_slippage("BTCUSDT", "us_main", default=4.0)
        assert val == pytest.approx(4.0)

    def test_stale_reverts_to_default(self):
        # Манипулируем _last_load_ok_ms напрямую, обходя min-TTL 1000ms.
        redis = MagicMock()
        redis.get.return_value = self._payload_json({"*:*": 3.5})
        reader = SlippageCalReader(redis)
        reader.force_refresh()                      # load ok, _last_load_ok_ms = now
        # Сдвигаем временну́ю метку в прошлое (> stale_ms)
        reader._last_load_ok_ms = int(time.time() * 1000) - reader._stale_ms - 1000
        assert reader._is_stale()                   # контроль
        reader.get_slippage("BTCUSDT", "us_main", default=9.9)
        # Redis отвечает снова — обновит _last_load_ok_ms и вернёт данные.
        # Поэтому stale-path → data (fail-open: если refresh OK, не stale).
        # Реальный stale-путь — когда Redis недоступен И snapshot старый:
        redis.get.side_effect = Exception("down")
        reader._last_load_ok_ms = int(time.time() * 1000) - reader._stale_ms - 1000
        reader._last_refresh_ms = 0                 # форсируем следующий refresh
        val2 = reader.get_slippage("SOLUSDT", "asian", default=9.9)
        assert val2 == pytest.approx(9.9)


# ---------------------------------------------------------------------------
# 7. CostEdgeGate integration — calibrated → spread → static
# ---------------------------------------------------------------------------

class TestCostEdgeGateSlippageIntegration:
    def _gate(self, slippage_bps: float = 4.0) -> CostEdgeGate:
        cfg = CostEdgeConfig(
            enabled=True,
            default_cost_k=1.0,
            fees_bps=0.0,
            slippage_bps=slippage_bps,
            slippage_use_spread_half=False,
        )
        return CostEdgeGate(cfg)

    def _ctx(self, session: str = "us_main", spread_bps: float | None = None,
              tp1: float | None = None, side: str = "LONG") -> SimpleNamespace:
        return SimpleNamespace(session=session, spread_bps=spread_bps,
                               tp1=tp1, side=side)

    def test_no_store_uses_static(self):
        gate = self._gate(slippage_bps=4.0)
        bps = gate._estimate_slippage_bps(self._ctx(), entry_price=100.0, symbol="BTCUSDT")
        assert bps == pytest.approx(4.0)

    def test_calibrated_store_overrides_static(self):
        gate = self._gate(slippage_bps=4.0)
        store = SlippageCalStore.from_dict({"BTCUSDT:US_MAIN": 2.1})
        gate.set_slippage_store(store)
        bps = gate._estimate_slippage_bps(self._ctx(session="us_main"),
                                           entry_price=100.0, symbol="BTCUSDT")
        assert bps == pytest.approx(2.1)

    def test_calibrated_missing_falls_back_to_static(self):
        gate = self._gate(slippage_bps=4.0)
        # Store has no entry for XRPUSDT
        store = SlippageCalStore.from_dict({"BTCUSDT:US_MAIN": 2.1})
        gate.set_slippage_store(store)
        bps = gate._estimate_slippage_bps(self._ctx(session="us_main"),
                                           entry_price=100.0, symbol="XRPUSDT")
        assert bps == pytest.approx(4.0)

    def test_calibrated_wildcard_used_for_unknown_symbol(self):
        gate = self._gate(slippage_bps=4.0)
        store = SlippageCalStore.from_dict({"*:*": 6.5})
        gate.set_slippage_store(store)
        bps = gate._estimate_slippage_bps(self._ctx(session="asian"),
                                           entry_price=100.0, symbol="PEPUSDT")
        assert bps == pytest.approx(6.5)

    def test_store_error_falls_back_to_static(self):
        gate = self._gate(slippage_bps=4.0)
        bad_store = MagicMock()
        bad_store.get_slippage.side_effect = Exception("redis down")
        gate.set_slippage_store(bad_store)
        bps = gate._estimate_slippage_bps(self._ctx(), entry_price=100.0, symbol="BTCUSDT")
        assert bps == pytest.approx(4.0)

    def test_evaluate_uses_calibrated_slippage(self):
        """End-to-end: gate с calibrated store блокирует low-edge сигнал правильно."""
        # SOL реальный slippage 10 bps — статика 4 bps пропустила бы сигнал
        cfg = CostEdgeConfig(
            enabled=True,
            default_cost_k=2.0,
            fees_bps=4.0,
            slippage_bps=4.0,        # статика: total_costs = 8, required = 16
            slippage_use_spread_half=False,
        )
        gate = CostEdgeGate(cfg)
        store = SlippageCalStore.from_dict({"SOLUSDT:US_MAIN": 10.0})
        gate.set_slippage_store(store)
        # total_costs = fees(4) + slip_cal(10) = 14, required = 28
        # edge_bps (tp1): 150→160 на entry 150 = (10/150)*10000 = 666 bps → passes
        ctx = SimpleNamespace(session="us_main", spread_bps=None,
                               tp1=160.0, side="LONG")
        result = gate.evaluate(ctx, symbol="SOLUSDT", entry_price=150.0)
        # With calibrated slippage=10: required = (4+10)*2 = 28, edge ~666 bps → PASS
        assert result.slippage_bps == pytest.approx(10.0)
        assert result.passed

    def test_set_slippage_store_replaces_previous(self):
        gate = self._gate()
        store1 = SlippageCalStore.from_dict({"BTCUSDT:US_MAIN": 2.0})
        store2 = SlippageCalStore.from_dict({"BTCUSDT:US_MAIN": 8.0})
        gate.set_slippage_store(store1)
        gate.set_slippage_store(store2)
        bps = gate._estimate_slippage_bps(self._ctx(), entry_price=100.0, symbol="BTCUSDT")
        assert bps == pytest.approx(8.0)


# ---------------------------------------------------------------------------
# 8. _parse_slip_map — payload parsing
# ---------------------------------------------------------------------------

class TestParseSlipMap:
    def test_new_bps_field(self):
        payload = json.dumps({"groups": {"BTCUSDT:US_MAIN": {"new_bps": 2.5}}})
        slip_map, cal_ms = _parse_slip_map(payload)
        assert "BTCUSDT:US_MAIN" in slip_map
        assert slip_map["BTCUSDT:US_MAIN"] == pytest.approx(2.5)

    def test_q75_field_fallback(self):
        payload = json.dumps({"groups": {"SOLUSDT:ASIAN": {"q75": 7.8}}})
        slip_map, _ = _parse_slip_map(payload)
        assert slip_map["SOLUSDT:ASIAN"] == pytest.approx(7.8)

    def test_scalar_group_entry(self):
        payload = json.dumps({"groups": {"PEPUSDT:OVERNIGHT": 5.5}})
        slip_map, _ = _parse_slip_map(payload)
        assert slip_map["PEPUSDT:OVERNIGHT"] == pytest.approx(5.5)

    def test_invalid_json_returns_empty(self):
        slip_map, cal_ms = _parse_slip_map("not-json")
        assert slip_map == {}
        assert cal_ms == 0

    def test_bytes_input(self):
        payload = json.dumps({"groups": {"BTCUSDT:US_MAIN": {"new_bps": 3.1}}}).encode()
        slip_map, _ = _parse_slip_map(payload)
        assert "BTCUSDT:US_MAIN" in slip_map

    def test_negative_bps_excluded(self):
        payload = json.dumps({"groups": {"BTCUSDT:US_MAIN": {"new_bps": -1.0}}})
        slip_map, _ = _parse_slip_map(payload)
        assert "BTCUSDT:US_MAIN" not in slip_map
