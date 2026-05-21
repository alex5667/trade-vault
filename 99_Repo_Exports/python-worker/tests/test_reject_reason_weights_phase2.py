"""Phase-2 IPS-weighting tests: adverse_cross / reliability / smt_coh /
confidence_threshold calibrators must accept `weight` and aggregate correctly.

All four wire to the same `core.reject_reason_weights.weight_for_reason`
policy via `v_gate_reason` from `trades:closed`. These tests verify the
math is back-compat under weight=1.0 and behaves correctly under sub-unit
weights (selection-bias correction).
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any
from unittest import mock

import pytest

from core import reject_reason_weights as rrw


@pytest.fixture(autouse=True)
def _reset_weights_cache():
    rrw.reset_cache()
    yield
    rrw.reset_cache()


# ---------------------------------------------------------------------------
# adverse_cross_calibrator.observe_outcome — weighted loss-floor
# ---------------------------------------------------------------------------


def test_adverse_observe_outcome_default_weight_back_compat():
    """observe_outcome() without weight= → identical to legacy code path."""
    from core.adverse_cross_calibrator import AdverseCrossCalibrator
    cal = AdverseCrossCalibrator(outcome_min_losses=5, outcome_max_buf=100)
    for cb in [1.0, 1.5, 2.0, 0.8, 1.2, 1.8, 1.1]:
        cal.observe_outcome(regime="btc:ny", cross_bps=cb, is_loss=True)
    # Loss floor should be approximately q80 of the LOSS-only set
    floor = cal._loss_floor("btc:ny")
    assert floor is not None
    assert 1.0 < floor < 2.0


def test_adverse_observe_outcome_zero_weight_discards():
    from core.adverse_cross_calibrator import AdverseCrossCalibrator
    cal = AdverseCrossCalibrator(outcome_min_losses=5, outcome_max_buf=100)
    # Feed 5 zero-weight samples — should produce no loss floor
    for _ in range(5):
        cal.observe_outcome(regime="btc:ny", cross_bps=2.0, is_loss=True, weight=0.0)
    assert cal._loss_floor("btc:ny") is None  # buffer is empty


def test_adverse_observe_outcome_weight_clipped():
    from core.adverse_cross_calibrator import AdverseCrossCalibrator
    cal = AdverseCrossCalibrator(outcome_min_losses=1, outcome_max_buf=100)
    cal.observe_outcome(regime="btc:ny", cross_bps=1.0, is_loss=True, weight=5.0)
    cal.observe_outcome(regime="btc:ny", cross_bps=1.0, is_loss=True, weight=-0.5)
    buf = cal._outcomes["btc:ny"]
    assert len(buf) == 1  # negative dropped
    assert buf[0][2] == 1.0  # clipped to 1.0


def test_adverse_loss_floor_weighted_vs_unweighted():
    """Heavy-weighted low-cb LOSSES should pull the q80 floor DOWN compared
    to legacy (unweighted) treatment of mixed environment samples."""
    from core.adverse_cross_calibrator import AdverseCrossCalibrator

    cal_w = AdverseCrossCalibrator(outcome_min_losses=10, outcome_max_buf=500)
    cal_u = AdverseCrossCalibrator(outcome_min_losses=10, outcome_max_buf=500)

    # 30 reliable LOSSES with low cross_bps (real passed trades)
    for cb in [0.5] * 30:
        cal_w.observe_outcome(regime="r", cross_bps=cb, is_loss=True, weight=1.0)
        cal_u.observe_outcome(regime="r", cross_bps=cb, is_loss=True, weight=1.0)
    # 60 environment-veto LOSSES with HIGH cross_bps — weighted at 0.1
    for cb in [3.0] * 60:
        cal_w.observe_outcome(regime="r", cross_bps=cb, is_loss=True, weight=0.1)
        cal_u.observe_outcome(regime="r", cross_bps=cb, is_loss=True, weight=1.0)

    floor_w = cal_w._loss_floor("r")
    floor_u = cal_u._loss_floor("r")
    assert floor_w is not None and floor_u is not None
    # Weighted floor should be CLOSER to 0.5 (real-trade dominated).
    # Unweighted is dominated by the 60 noisy 3.0 samples.
    assert floor_w < floor_u


def test_adverse_snapshot_back_compat_v1_load():
    """Legacy 2-tuple outcomes (cb, is_loss) load with weight=1.0."""
    from core.adverse_cross_calibrator import AdverseCrossCalibrator
    cal = AdverseCrossCalibrator(outcome_min_losses=1, outcome_max_buf=100)
    # Simulate legacy v1 snapshot — no weight field
    cal.load_regime_state({
        "v": 1,
        "kind": "adverse_cross",
        "regime": "r",
        "min_samples": 500,
        "enforce": False,
        "n": 100,
        "outcomes": [[1.5, 1], [2.0, 1], [0.8, 0]],
    })
    buf = cal._outcomes["r"]
    assert len(buf) == 3
    # All samples should default to weight=1.0
    assert all(entry[2] == 1.0 for entry in buf)


# ---------------------------------------------------------------------------
# reliability_calibrator — HINCRBYFLOAT path
# ---------------------------------------------------------------------------


class _FakeRedisFloat:
    """In-memory FakeRedis that handles both HINCRBY and HINCRBYFLOAT."""

    def __init__(self) -> None:
        self.h: dict[str, dict[str, float]] = defaultdict(dict)
        self.exp: dict[str, int] = {}

    def pipeline(self, transaction: bool = False) -> "_FakeRedisFloat":
        return self

    def hincrby(self, key: str, field: str, amount: int) -> int:
        cur = float(self.h[key].get(field, 0.0) or 0.0)
        cur += float(amount)
        self.h[key][field] = cur
        return int(cur)

    def hincrbyfloat(self, key: str, field: str, amount: float) -> float:
        cur = float(self.h[key].get(field, 0.0) or 0.0)
        cur += float(amount)
        self.h[key][field] = cur
        return cur

    def hset(self, key: str, field: str, value: Any) -> None:
        try:
            self.h[key][field] = float(value)
        except Exception:
            self.h[key][field] = 0.0

    def expire(self, key: str, ttl: int) -> None:
        self.exp[key] = int(ttl)

    def execute(self) -> None:
        return None

    def hgetall(self, key: str) -> dict[str, Any]:
        return {k: str(v) for k, v in (self.h.get(key) or {}).items()}


def _cfg():
    import os
    from services.reliability_calibrator import RelCalConfig
    os.environ["REL_CAL_ENABLED"] = "1"
    os.environ["REL_CAL_OUTCOMES"] = "tp2"
    os.environ["REL_CAL_BUCKET_STEP_PCT"] = "5"
    os.environ["REL_CAL_TTL_SEC"] = "3600"
    return RelCalConfig.from_env()


def _pos_closed(*, confidence_pct: float, tp2_hit: bool, v_gate_reason: str = ""):
    pos = {
        "strategy": "absorption",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "entry_ts_ms": 1700000000000,
        "signal_payload": {"confidence": confidence_pct, "venue": "binance_futures"},
        "v_gate_reason": v_gate_reason,
    }
    closed = {
        "strategy": "absorption",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "tp1_hit": tp2_hit,
        "tp2_hit": tp2_hit,
        "v_gate_reason": v_gate_reason,
    }
    return pos, closed


def test_reliability_writer_weight_disabled_uses_hincrby():
    """REJECT_REASON_WEIGHTS_ENABLED=0 → must keep using HINCRBY (int counts)."""
    from services.reliability_calibrator import update_reliability_curves
    rrw.reset_cache()
    r = _FakeRedisFloat()
    cfg = _cfg()
    pos, closed = _pos_closed(confidence_pct=65.0, tp2_hit=True, v_gate_reason="VETO_FREEZE_ACTIVE")
    update_reliability_curves(r, cfg=cfg, pos=pos, trade_closed=closed, now_ms=1700000001000)
    # With weights OFF, even VETO_FREEZE contributes 1.0 (full integer).
    key = list(r.h.keys())[0]
    assert r.h[key]["samples_total"] == 1.0
    assert r.h[key]["hits_total"] == 1.0


def test_reliability_writer_weight_enabled_uses_hincrbyfloat():
    import os
    from services.reliability_calibrator import update_reliability_curves
    with mock.patch.dict(os.environ, {"REJECT_REASON_WEIGHTS_ENABLED": "1"}):
        rrw.reset_cache()
        r = _FakeRedisFloat()
        cfg = _cfg()
        # VETO_FREEZE_ACTIVE has weight=0.10 in DEFAULT_WEIGHTS
        pos, closed = _pos_closed(
            confidence_pct=65.0, tp2_hit=True, v_gate_reason="VETO_FREEZE_ACTIVE"
        )
        update_reliability_curves(r, cfg=cfg, pos=pos, trade_closed=closed, now_ms=1700000001000)
        key = list(r.h.keys())[0]
        assert r.h[key]["samples_total"] == pytest.approx(0.10, rel=1e-6)
        assert r.h[key]["hits_total"] == pytest.approx(0.10, rel=1e-6)


def test_reliability_writer_passed_real_trade_full_weight():
    """Passed real trade (v_gate_reason='' or 'OK') → weight=1.0 (HINCRBY path)."""
    import os
    from services.reliability_calibrator import update_reliability_curves
    with mock.patch.dict(os.environ, {"REJECT_REASON_WEIGHTS_ENABLED": "1"}):
        rrw.reset_cache()
        r = _FakeRedisFloat()
        cfg = _cfg()
        pos, closed = _pos_closed(confidence_pct=65.0, tp2_hit=True, v_gate_reason="")
        update_reliability_curves(r, cfg=cfg, pos=pos, trade_closed=closed, now_ms=1700000001000)
        key = list(r.h.keys())[0]
        # weight=1.0 → HINCRBY path → exactly 1
        assert r.h[key]["samples_total"] == 1.0


# ---------------------------------------------------------------------------
# smt_coh_isotonic_calibrator.observe — weighted bins
# ---------------------------------------------------------------------------


def test_smt_coh_observe_default_weight_back_compat():
    from core.smt_coh_isotonic_calibrator import SmtCohIsotonicCalibrator
    cal = SmtCohIsotonicCalibrator()
    cal.observe(symbol="BTC", regime="trend", coh=0.7, outcome=1)
    cal.observe(symbol="BTC", regime="trend", coh=0.7, outcome=0)
    n_total = cal.n_total(symbol="BTC", regime="trend")
    assert n_total == 2.0  # back-compat: integer-valued


def test_smt_coh_observe_zero_weight_discards():
    from core.smt_coh_isotonic_calibrator import SmtCohIsotonicCalibrator
    cal = SmtCohIsotonicCalibrator()
    cal.observe(symbol="BTC", regime="trend", coh=0.7, outcome=1, weight=0.0)
    cal.observe(symbol="BTC", regime="trend", coh=0.7, outcome=1, weight=-0.5)
    assert cal.n_total(symbol="BTC", regime="trend") == 0.0


def test_smt_coh_weighted_bin_accumulation():
    """Fractional weight contributes fractionally to (n, h)."""
    from core.smt_coh_isotonic_calibrator import SmtCohIsotonicCalibrator
    cal = SmtCohIsotonicCalibrator()
    cal.observe(symbol="BTC", regime="trend", coh=0.7, outcome=1, weight=0.5)
    cal.observe(symbol="BTC", regime="trend", coh=0.7, outcome=0, weight=0.3)
    n_total = cal.n_total(symbol="BTC", regime="trend")
    assert n_total == pytest.approx(0.8, rel=1e-6)
    # bucket lookup
    key = ("BTC", "trend")
    bins = cal._bins[key]
    bkt = list(bins.keys())[0]
    n, h = bins[bkt]
    assert n == pytest.approx(0.8, rel=1e-6)
    assert h == pytest.approx(0.5, rel=1e-6)  # only WIN gets hit


def test_smt_coh_observe_clips_weight_to_one():
    from core.smt_coh_isotonic_calibrator import SmtCohIsotonicCalibrator
    cal = SmtCohIsotonicCalibrator()
    cal.observe(symbol="BTC", regime="trend", coh=0.7, outcome=1, weight=10.0)
    assert cal.n_total(symbol="BTC", regime="trend") == 1.0


# ---------------------------------------------------------------------------
# confidence_threshold_calibrator — float-tolerant read path
# ---------------------------------------------------------------------------


def test_confidence_parse_buckets_handles_float_strings():
    """HINCRBYFLOAT writes "0.1", "0.5" — _parse_buckets must accept them."""
    from core.confidence_threshold_calibrator import _parse_buckets
    hash_data = {
        "b50:n": "5.7",
        "b50:h": "2.3",
        "b55:n": "10",   # legacy integer field
        "b55:h": "7",
        "samples_total": "15.7",  # global field (ignored by parser)
    }
    buckets = _parse_buckets(hash_data)
    assert buckets[50] == pytest.approx((5.7, 2.3), rel=1e-6)
    assert buckets[55] == pytest.approx((10.0, 7.0), rel=1e-6)


def test_confidence_invert_curve_accepts_float_buckets():
    from core.confidence_threshold_calibrator import _invert_curve
    # 80% WR target. Build a curve where b75 has 50 samples / 40 hits (0.8).
    hash_data = {
        "b75:n": "50.0",
        "b75:h": "40.0",
        "b70:n": "50.0",
        "b70:h": "25.0",  # below 80% WR
    }
    result = _invert_curve(hash_data, target_wr=0.80, min_samples_above=20)
    assert result is not None
    t, hr, n = result
    assert t == 75.0
    assert hr == pytest.approx(0.8, rel=1e-6)
    assert n == 50.0
