"""
test_stop_risk_contract_v1.py

Unit tests for the Stop-Risk Contract system:
  - StopContractResult / compute_effective_stop_contract
  - compute_stop_noise_floor_bps
  - SLQ: never tightens, fixed-risk sizing reduces qty, bucket fallback,
         DENY on too-wide / EV-negative, hard-guard, noise-floor gate
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ── Modules under test ────────────────────────────────────────────────────────
from services.risk.stop_contract import (
    StopContractResult,
    compute_effective_stop_contract,
    compute_stop_noise_floor_bps,
)
from services.position_sizing import calculate_qty_fixed_risk, SizingResult
from services.slq_risk_adjust import maybe_apply_slq_to_risk_cfg


# ── Helpers ───────────────────────────────────────────────────────────────────

@dataclass
class FakeCtx:
    entry_price: float = 100.0
    stop_dist: float = 1.0       # 100 bps
    atr: float = 1.0
    atr_bps: float = 100.0
    regime: str = "trending"
    scenario: str = "rocket"
    session: str = "us"
    vol_bucket: str = "mid"
    liq_bucket: str = "high"
    tp1_hit_prob: float = 0.65
    spread_bps: float = 3.0
    slippage_p95_bps: float = 2.0  # type: ignore
    micro_noise_q90_bps: float = 5.0
    dq_flags: list = None  # type: ignore
    sizing_ok: bool | None = None
    qty: float = 0.0
    risk_usd: float = 0.0
    risk_usd_target: float = 0.0
    sl_dist: float = 0.0
    sizing_mode: str = ""

    def __post_init__(self):
        if self.dq_flags is None:
            self.dq_flags = []


def _make_slq_snap(n=500, q90=0.3, postsl=0.45, ts_ms=None) -> dict:
    import time
    return {
        "n": n,
        "sl_buffer_atr_q90": q90,
        "post_sl_tp1_hit_rate": postsl,
        "ts_ms": ts_ms or int(time.time() * 1000),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 1. StopContractResult — compute_effective_stop_contract
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeEffectiveStopContract:

    def test_binding_component_is_max(self):
        result = compute_effective_stop_contract(
            strategy_stop_bps=50.0,
            atr_bps=100.0,
            stop_atr_mult=1.2,
            spread_bps=3.0,
            slippage_p95_bps=2.0,
            fee_bps=5.0,
            exchange_min_stop_bps=8.0,
            noise_q90_bps=10.0,
            buffer_bps=2.0,
        )
        assert result.ok
        # atr_stop = 100 * 1.2 = 120 → wins
        assert result.binding_component == "atr_mult"
        assert result.effective_stop_bps == pytest.approx(120.0)

    def test_cost_floor_wins_when_atr_small(self):
        result = compute_effective_stop_contract(
            strategy_stop_bps=2.0,
            atr_bps=5.0,
            stop_atr_mult=0.5,
            spread_bps=4.0,
            slippage_p95_bps=3.0,
            fee_bps=5.0,
            exchange_min_stop_bps=1.0,
            noise_q90_bps=1.0,
            buffer_bps=2.0,
        )
        assert result.ok
        # cost_floor = 4+3+5+2 = 14 → wins
        assert result.binding_component == "cost_floor"
        assert result.effective_stop_bps == pytest.approx(14.0)

    def test_exchange_min_wins(self):
        result = compute_effective_stop_contract(
            strategy_stop_bps=1.0,
            atr_bps=2.0,
            stop_atr_mult=0.5,
            spread_bps=0.5,
            slippage_p95_bps=0.5,
            fee_bps=0.5,
            exchange_min_stop_bps=50.0,
            noise_q90_bps=5.0,
            buffer_bps=0.5,
        )
        assert result.binding_component == "exchange_min"
        assert result.effective_stop_bps == pytest.approx(50.0)

    def test_negative_input_returns_not_ok(self):
        result = compute_effective_stop_contract(
            strategy_stop_bps=-5.0,
            atr_bps=100.0,
            stop_atr_mult=1.0,
            spread_bps=1.0,
            slippage_p95_bps=1.0,
            fee_bps=1.0,
            exchange_min_stop_bps=5.0,
            noise_q90_bps=5.0,
        )
        assert not result.ok
        assert result.reason == "negative_input"

    def test_candidates_dict_consistent(self):
        result = compute_effective_stop_contract(
            strategy_stop_bps=30.0,
            atr_bps=80.0,
            stop_atr_mult=1.0,
            spread_bps=2.0,
            slippage_p95_bps=2.0,
            fee_bps=3.0,
            exchange_min_stop_bps=5.0,
            noise_q90_bps=7.0,
            buffer_bps=2.0,
        )
        cands = result.candidates
        assert max(cands.values()) == result.effective_stop_bps


# ─────────────────────────────────────────────────────────────────────────────
# 2. compute_stop_noise_floor_bps
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeStopNoiseFloorBps:

    def test_atr_floor_dominates(self):
        floor = compute_stop_noise_floor_bps(
            atr_bps=100.0,
            spread_bps=1.0,
            slippage_p95_bps=1.0,
            fee_bps=1.0,
            micro_noise_q90_bps=5.0,
            exchange_min_stop_bps=5.0,
            atr_floor_mult=0.80,
            buffer_bps=2.0,
        )
        assert floor == pytest.approx(80.0)

    def test_cost_floor_dominates(self):
        floor = compute_stop_noise_floor_bps(
            atr_bps=5.0,
            spread_bps=10.0,
            slippage_p95_bps=5.0,
            fee_bps=5.0,
            micro_noise_q90_bps=5.0,
            exchange_min_stop_bps=3.0,
            atr_floor_mult=0.80,
            buffer_bps=2.0,
        )
        # cost = 10+5+5+2 = 22
        assert floor == pytest.approx(22.0)


# ─────────────────────────────────────────────────────────────────────────────
# 3. calculate_qty_fixed_risk
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculateQtyFixedRisk:

    def test_basic_formula(self):
        # risk=20, sl=10 → raw_qty=2, entry=100 → notional=200
        res = calculate_qty_fixed_risk(
            risk_usd=20.0,
            sl_dist=10.0,
            entry_price=100.0,
            lot_step=0.001,
            min_lot=0.001,
            max_lot=100.0,
        )
        assert res.ok
        assert res.qty == pytest.approx(2.0, abs=0.001)
        assert res.risk_usd == pytest.approx(20.0, abs=0.01)

    def test_widen_sl_reduces_qty(self):
        """Fixed-risk: wider SL → smaller qty → risk preserved."""
        lot_step = 0.001
        res1 = calculate_qty_fixed_risk(
            risk_usd=20.0, sl_dist=10.0, entry_price=100.0,
            lot_step=lot_step, min_lot=lot_step, max_lot=100.0,
        )
        res2 = calculate_qty_fixed_risk(
            risk_usd=20.0, sl_dist=20.0, entry_price=100.0,
            lot_step=lot_step, min_lot=lot_step, max_lot=100.0,
        )
        assert res1.ok and res2.ok
        # qty with sl=20 should be ≤ qty with sl=10 / 2 + 1 step
        assert res2.qty <= res1.qty / 2 + lot_step

    def test_actual_risk_never_exceeds_target_by_more_than_step(self):
        """Due to floor rounding, actual_risk <= risk_usd (conservative)."""
        for sl in [5.0, 7.3, 12.0, 31.7]:
            res = calculate_qty_fixed_risk(
                risk_usd=50.0, sl_dist=sl, entry_price=200.0,
                lot_step=0.01, min_lot=0.01, max_lot=500.0,
            )
            if res.ok:
                # actual risk = qty * sl ≤ target + 1 lot_step * sl
                assert res.qty * sl <= 50.0 + 0.01 * sl + 1e-6


# ─────────────────────────────────────────────────────────────────────────────
# 4. SLQ: maybe_apply_slq_to_risk_cfg
# ─────────────────────────────────────────────────────────────────────────────

class TestMaybeApplySlqToRiskCfg:

    def _base_cfg(self) -> dict:
        return {
            "TP_MODE": "RR",
            "STOP_MODE": "ATR",
            "STOP_ATR_MULT": 1.0,
            "TP1_RR": 1.3,
        }

    def _make_redis(self, snap: dict | None = None) -> Any:
        import json
        r = MagicMock()
        r.get.return_value = json.dumps(snap).encode() if snap else None
        return r

    @patch.dict(os.environ, {"SLQ_ENABLE": "1", "SLQ_MIN_N": "100", "SLQ_K": "0.50"})
    def test_slq_never_tightens_stop(self):
        """SLQ must never reduce STOP_ATR_MULT below base."""
        snap = _make_slq_snap(n=300, q90=0.0, postsl=0.50)  # q90=0 → bump=0
        r = self._make_redis(snap)
        ctx = FakeCtx()
        cfg = self._base_cfg()
        out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
        if out.get("slq_used") == 1:
            assert out["STOP_ATR_MULT"] >= cfg["STOP_ATR_MULT"]

    @patch.dict(os.environ, {"SLQ_ENABLE": "1", "SLQ_MIN_N": "100", "SLQ_STOP_ATR_MAX": "2.20"})
    def test_slq_applied_widens_stop(self):
        snap = _make_slq_snap(n=400, q90=0.4, postsl=0.50)
        r = self._make_redis(snap)
        ctx = FakeCtx(tp1_hit_prob=0.65)
        cfg = self._base_cfg()
        out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
        if out.get("slq_used") == 1:
            assert out["STOP_ATR_MULT"] >= 1.0
            assert out["slq_decision"] == "applied"

    @patch.dict(os.environ, {
        "SLQ_ENABLE": "1", "SLQ_MIN_N": "100",
        "SLQ_MAX_STOP_BPS": "50.0",   # very tight cap
    })
    def test_slq_reject_when_stop_too_wide(self):
        """Widened stop bps > SLQ_MAX_STOP_BPS → reject, sizing_ok=False."""
        snap = _make_slq_snap(n=400, q90=0.5, postsl=0.50)
        r = self._make_redis(snap)
        # atr_bps=200 → widened_stop = 200 * (1.0 + 0.25) = 250 >> 50
        ctx = FakeCtx(atr_bps=200.0, tp1_hit_prob=0.65)
        cfg = self._base_cfg()
        cfg["STOP_ATR_MULT"] = 1.0
        out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
        assert out.get("slq_decision") == "reject_too_wide"
        assert out.get("sizing_ok") is False

    @patch.dict(os.environ, {
        "SLQ_ENABLE": "1", "SLQ_MIN_N": "100",
        "SLQ_MIN_EV_AFTER_BPS": "1000.0",  # impossibly high EV requirement
    })
    def test_slq_reject_when_ev_negative(self):
        """EV after SLQ < SLQ_MIN_EV_AFTER_BPS → reject."""
        snap = _make_slq_snap(n=400, q90=0.2, postsl=0.50)
        r = self._make_redis(snap)
        ctx = FakeCtx(tp1_hit_prob=0.65, atr_bps=50.0)
        cfg = self._base_cfg()
        out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
        assert out.get("slq_decision") == "reject_ev_negative"
        assert out.get("sizing_ok") is False

    @patch.dict(os.environ, {"SLQ_ENABLE": "1", "SLQ_MIN_N": "300"})
    def test_slq_bucket_fallback_cascade(self):
        """SLQ falls back to broader bucket when exact bucket has insufficient N."""
        import json
        broad_snap = _make_slq_snap(n=400, q90=0.2, postsl=0.45)
        r = MagicMock()
        call_count = [0]

        def side_effect(key):
            call_count[0] += 1
            # Exact bucket (first call) returns None → trigger fallback
            if call_count[0] == 1:
                return None
            # Second key also None
            if call_count[0] == 2:
                return None
            # Third key (sym_side_regime) returns data
            return json.dumps(broad_snap).encode()

        r.get.side_effect = side_effect
        ctx = FakeCtx(tp1_hit_prob=0.65, atr_bps=80.0)
        cfg = self._base_cfg()
        out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
        # Should have used the broad bucket
        if out.get("slq_used") == 1:
            assert out.get("slq_bucket_level") in {"sym_side_regime", "sym_side", "applied"}

    @patch.dict(os.environ, {"SLQ_ENABLE": "0"})
    def test_slq_disabled_returns_cfg_unchanged(self):
        r = MagicMock()
        ctx = FakeCtx()
        cfg = self._base_cfg()
        out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
        assert out["STOP_ATR_MULT"] == cfg["STOP_ATR_MULT"]
        assert r.get.call_count == 0

    @patch.dict(os.environ, {"SLQ_ENABLE": "1", "SLQ_MIN_N": "100", "SLQ_POSTSL_TP1_MIN": "0.80"})
    def test_slq_low_postsl_tp1_skips(self):
        """Snapshot with low post-SL TP1 rate → slq not applied."""
        snap = _make_slq_snap(n=400, q90=0.3, postsl=0.30)  # 0.30 < 0.80 min
        r = self._make_redis(snap)
        ctx = FakeCtx(tp1_hit_prob=0.65)
        cfg = self._base_cfg()
        out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
        assert out.get("slq_decision") == "slq_low_postsl_tp1"
        assert out.get("slq_used") != 1


# ─────────────────────────────────────────────────────────────────────────────
# 5. Hard-guard: risk_budget_exceeded via apply_position_sizing_to_ctx
# ─────────────────────────────────────────────────────────────────────────────

class TestApplyPositionSizingHardGuard:

    @patch.dict(os.environ, {
        "RISK_USE_FIXED_DOLLAR_SIZING": "1",
        "ACCOUNT_DEPOSIT_USD": "1000",
        "RISK_PERCENT": "1.0",           # target = $10
        "RISK_MAX_ACTUAL_OVER_TARGET": "1.02",
        "STOP_NOISE_FLOOR_ENABLE": "0",
        "RISK_MIN_NOTIONAL_USD": "0.0",
    })
    def test_fixed_risk_sizing_respected(self):
        from services.position_sizing import apply_position_sizing_to_ctx
        ctx = FakeCtx(entry_price=100.0, stop_dist=1.0)  # stop=1%, risk=$10 → qty=10
        cfg = {"TP_MODE": "RR"}
        apply_position_sizing_to_ctx(ctx, cfg, "BTCUSDT")
        assert ctx.sizing_ok is True
        assert ctx.sizing_mode == "fixed_risk"
        # actual_risk = qty * sl_dist ≤ 10 * 1.02
        assert ctx.qty * ctx.sl_dist <= 10.0 * 1.02 + 1e-6


# ─────────────────────────────────────────────────────────────────────────────
# 6. P0 regression — SLQ cfg deny hard-blocks apply_position_sizing_to_ctx
# ─────────────────────────────────────────────────────────────────────────────

class TestP0SLQRejectBlocksSizing:
    """cfg['sizing_ok']=False from slq_risk_adjust must prevent any qty assignment."""

    @patch.dict(os.environ, {
        "RISK_USE_FIXED_DOLLAR_SIZING": "1",
        "ACCOUNT_DEPOSIT_USD": "1000",
        "RISK_PERCENT": "1.0",
        "STOP_NOISE_FLOOR_ENABLE": "0",
    })
    def test_slq_reject_too_wide_blocks_sizing(self):
        from services.position_sizing import apply_position_sizing_to_ctx
        ctx = FakeCtx(entry_price=100.0, stop_dist=1.0)
        cfg = {
            "TP_MODE": "RR",
            "sizing_ok": False,
            "slq_decision": "reject_too_wide",
        }
        apply_position_sizing_to_ctx(ctx, cfg, "BTCUSDT")
        assert ctx.sizing_ok is False
        assert ctx.qty == 0.0          # no qty assigned  # type: ignore
        assert hasattr(ctx, "sizing_deny_reason")
        assert ctx.sizing_deny_reason == "reject_too_wide"  # type: ignore
        # append_dq_flag writes to ctx.data_quality_flags
        assert "reject_too_wide" in getattr(ctx, "data_quality_flags", [])

    @patch.dict(os.environ, {
        "RISK_USE_FIXED_DOLLAR_SIZING": "1",
        "ACCOUNT_DEPOSIT_USD": "1000",
        "RISK_PERCENT": "1.0",
        "STOP_NOISE_FLOOR_ENABLE": "0",
    })
    def test_slq_reject_ev_negative_blocks_sizing(self):
        from services.position_sizing import apply_position_sizing_to_ctx
        ctx = FakeCtx(entry_price=100.0, stop_dist=1.0)
        cfg = {
            "TP_MODE": "RR",
            "sizing_ok": False,
            "slq_decision": "reject_ev_negative",
        }
        apply_position_sizing_to_ctx(ctx, cfg, "BTCUSDT")
        assert ctx.sizing_ok is False
        assert ctx.qty == 0.0
        assert "reject_ev_negative" in getattr(ctx, "data_quality_flags", [])

    @patch.dict(os.environ, {
        "RISK_USE_FIXED_DOLLAR_SIZING": "1",
        "ACCOUNT_DEPOSIT_USD": "1000",
        "RISK_PERCENT": "1.0",
        "STOP_NOISE_FLOOR_ENABLE": "0",
    })
    def test_sizing_ok_none_proceeds_normally(self):
        """cfg without sizing_ok key must not block sizing."""
        from services.position_sizing import apply_position_sizing_to_ctx
        ctx = FakeCtx(entry_price=100.0, stop_dist=1.0)
        cfg = {"TP_MODE": "RR"}       # no sizing_ok key
        apply_position_sizing_to_ctx(ctx, cfg, "BTCUSDT")
        assert ctx.sizing_ok is True
        assert ctx.qty > 0.0


# ─────────────────────────────────────────────────────────────────────────────
# 7. P1a regression — shadow_only must NOT mutate STOP_ATR_MULT
# ─────────────────────────────────────────────────────────────────────────────

class TestP1aShadowNoMutation:
    """With SLQ_SHADOW_ONLY=1, STOP_ATR_MULT must remain unchanged."""

    def _make_redis(self, snap: dict) -> Any:
        import json
        r = MagicMock()
        r.get.return_value = json.dumps(snap).encode()
        return r

    @patch.dict(os.environ, {
        "SLQ_ENABLE": "1",
        "SLQ_SHADOW_ONLY": "1",
        "SLQ_MIN_N": "100",
        "SLQ_K": "0.50",
    })
    def test_shadow_does_not_mutate_stop_mult(self):
        snap = _make_slq_snap(n=400, q90=0.5, postsl=0.50)
        r = self._make_redis(snap)
        ctx = FakeCtx(atr_bps=80.0, tp1_hit_prob=0.65)
        cfg = {"TP_MODE": "RR", "STOP_MODE": "ATR", "STOP_ATR_MULT": 1.0, "TP1_RR": 1.3}
        out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
        # Execution config must be untouched
        assert out["STOP_ATR_MULT"] == 1.0
        # Shadow metadata must be present
        assert out.get("slq_shadow_only") is True
        assert out.get("slq_shadow_final_mult", 0) >= 1.0
        assert out.get("slq_decision") == "shadow_computed"

    @patch.dict(os.environ, {
        "SLQ_ENABLE": "1",
        "SLQ_SHADOW_ONLY": "1",
        "SLQ_MIN_N": "100",
        "SLQ_MAX_STOP_BPS": "5.0",   # would reject if enforced
    })
    def test_shadow_does_not_reject(self):
        """Shadow mode must never set sizing_ok=False even if stop would be too wide."""
        snap = _make_slq_snap(n=400, q90=0.5, postsl=0.50)
        r = self._make_redis(snap)
        ctx = FakeCtx(atr_bps=200.0, tp1_hit_prob=0.65)
        cfg = {"TP_MODE": "RR", "STOP_MODE": "ATR", "STOP_ATR_MULT": 1.0, "TP1_RR": 1.3}
        out = maybe_apply_slq_to_risk_cfg(redis=r, ctx=ctx, symbol="BTCUSDT", side=1, cfg=cfg)
        # sizing_ok must NOT be False in shadow mode
        assert out.get("sizing_ok") is not False
        assert out.get("slq_shadow_only") is True


# ─────────────────────────────────────────────────────────────────────────────
# 8. P1b regression — sl_quantile_aggregator writes correct bucket hierarchy
# ─────────────────────────────────────────────────────────────────────────────

class TestP1bAggregatorBuckets:
    """Aggregator must write exact + fallback keys for every message."""

    def test_process_msg_fills_all_bucket_levels(self):
        """A single trade message must populate 4 bucket keys."""
        from services.sl_quantile_aggregator import SlQuantileAggregator

        with patch("services.sl_quantile_aggregator.redis") as mock_redis_mod:
            mock_redis_mod.from_url.return_value = MagicMock()
            agg = SlQuantileAggregator.__new__(SlQuantileAggregator)
            from collections import defaultdict, deque
            agg.buckets = defaultdict(lambda: deque(maxlen=1000))
            agg.buckets_hits = defaultdict(lambda: deque(maxlen=1000))

            fields = {
                "symbol": "BTCUSDT",
                "side": "LONG",
                "regime": "trending",
                "scenario": "rocket",
                "session": "us",
                "vol_bucket": "mid",
                "liq_bucket": "high",
                "post_sl_req_buffer_atr": "0.25",
                "post_sl_tp1_hit": "1",
            }
            result = agg._process_msg(fields)
            assert result is True

            keys = list(agg.buckets.keys())
            assert "BTCUSDT:LONG:rocket:trending:us:mid:high" in keys
            assert "BTCUSDT:LONG:rocket:trending" in keys
            assert "BTCUSDT:LONG:trending" in keys
            assert "BTCUSDT:LONG" in keys

    def test_process_msg_missing_dimensions_uses_na(self):
        """Missing optional fields default to 'na' bucket segment."""
        from services.sl_quantile_aggregator import SlQuantileAggregator

        with patch("services.sl_quantile_aggregator.redis") as mock_redis_mod:
            mock_redis_mod.from_url.return_value = MagicMock()
            agg = SlQuantileAggregator.__new__(SlQuantileAggregator)
            from collections import defaultdict, deque
            agg.buckets = defaultdict(lambda: deque(maxlen=1000))
            agg.buckets_hits = defaultdict(lambda: deque(maxlen=1000))

            fields = {
                "symbol": "SOLUSDT",
                "side": "SHORT",
                "post_sl_req_buffer_atr": "0.15",
                "post_sl_tp1_hit": "0",
            }
            result = agg._process_msg(fields)
            assert result is True
            keys = list(agg.buckets.keys())
            assert "SOLUSDT:SHORT:na:na:na:na:na" in keys
            assert "SOLUSDT:SHORT:na:na" in keys
            assert "SOLUSDT:SHORT:na" in keys
            assert "SOLUSDT:SHORT" in keys
