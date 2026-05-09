import types

import pytest

from handlers.crypto_orderflow.core.crypto_orderflow_confirmations import (
    L2ConfirmAbsorption,
    L2ConfirmBreakout,
    L2ConfirmCfg,
)
from handlers.crypto_orderflow.core.crypto_orderflow_detector import CryptoEventDetector, DetectorCfg
from handlers.crypto_orderflow.core.crypto_orderflow_quality import (
    CompositeValidator,
    OBIBreakoutValidator,
    OBIFadeValidator,
    PivotsPresentValidator,
    SpreadValidator,
)
from handlers.crypto_orderflow.core.crypto_orderflow_scoring import CryptoScoreModel, ScoreModelCfg
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level, L2Snapshot
from handlers.crypto_orderflow.types.crypto_orderflow_pipeline_types import Candidate, QualityState


class DummyThresholdMgr:
    def get_thresholds(self, **kwargs):
        return None
    def observe(self, **kwargs):
        return None


def test_detector_emits_candidates_in_order():
    def nearest(price, pivots):
        return "PDH"

    def cross(price, dir_up, pivots):
        return "PDH" if dir_up else None

    det = CryptoEventDetector(
        DetectorCfg(
            main_z_threshold=1.0,
            absorption_z_threshold=1.0,
            breakout_z_threshold=1.0,
            extreme_z_threshold=2.0,
            obi_spike_thr=0.7,
        ),
        nearest_pivot_key=nearest,
        breakout_cross_info=cross,
    )

    ctx = types.SimpleNamespace(
        price=100.0,
        pivots={"PDH": 99.0},
        z_delta=1.5,
        obi_sustained=True,
        obi_avg=0.9,
    )

    cands = det.detect(ctx)
    kinds = [c.kind for c in cands]
    assert "obi_spike" in kinds
    assert "absorption" in kinds
    assert "breakout" in kinds
    assert "extreme" not in kinds  # 1.5 < 2.0


def test_spread_validator_vetoes():
    ctx = types.SimpleNamespace(spread_bps=20.0, pivots={"X": 1.0})
    cand = Candidate(kind="breakout", direction=1, raw_score=2.0, level_key="X")

    v = CompositeValidator([SpreadValidator(spread_max_bps=12.0), PivotsPresentValidator()])
    q = v.validate(ctx, cand)
    assert q.veto is True
    assert "spread" in q.veto_reason


def test_obi_breakout_validator_strict():
    ctx = types.SimpleNamespace(
        z_delta=2.0,
        obi_sustained=True,
        obi_avg=-0.5,  # против импульса
        obi_sustained_20=True,
        obi_avg_20=-0.2,
        pivots={"PDH": 1.0},
        spread_bps=1.0,
    )
    cand = Candidate(kind="breakout", direction=1, raw_score=2.0, level_key="PDH")

    v = CompositeValidator([OBIBreakoutValidator(require_obi=True, require_obi20=False)])
    q = v.validate(ctx, cand)
    assert q.veto is True
    assert q.veto_reason == "breakout_requires_obi"


def test_obi_fade_validator_vetoes_absorption_when_obi_confirms():
    ctx = types.SimpleNamespace(
        z_delta=2.0,
        obi_sustained=True,
        obi_avg=0.5,  # подтверждает импульс вверх
        pivots={"PDH": 1.0},
    )
    cand = Candidate(kind="absorption", direction=1, raw_score=-2.0, level_key="PDH")
    v = CompositeValidator([OBIFadeValidator()])
    q = v.validate(ctx, cand)
    assert q.veto is True
    assert "obi_confirms" in q.veto_reason


def test_l2_confirmer_integration_breakout_and_absorption():
    cfg = L2ConfirmCfg(top_n=1, max_age_ms=999999, breakout_imbalance_min=1.1, absorption_imbalance_min=1.1, wall_dist_bps_max=15.0)
    b = L2ConfirmBreakout(cfg=cfg, get_snapshot=L2ConfirmBreakout.default_get_snapshot, get_snapshot_ts_ms=L2ConfirmBreakout.default_get_snapshot_ts_ms)
    a = L2ConfirmAbsorption(cfg=cfg, get_snapshot=L2ConfirmAbsorption.default_get_snapshot, get_snapshot_ts_ms=L2ConfirmAbsorption.default_get_snapshot_ts_ms)

    # breakout up: bid dominates + no ask wall near
    snap1 = L2Snapshot(bids=[L2Level(price=99, size=1, notional=200)], asks=[L2Level(price=101, size=1, notional=50)])
    ctx1 = types.SimpleNamespace(ts=10_000, l2_snapshot=snap1, l2_ts_ms=9_900, wall_ask=False, wall_bid=False)
    ok1, d1 = b.check(ctx1, dir_up=True)
    assert ok1 is True
    assert d1["ok"] is True

    # absorption up (fade): asks dominate
    snap2 = L2Snapshot(bids=[L2Level(price=99, size=1, notional=50)], asks=[L2Level(price=101, size=1, notional=200)])
    ctx2 = types.SimpleNamespace(ts=10_000, l2_snapshot=snap2, l2_ts_ms=9_900)
    ok2, d2 = a.check(ctx2, dir_up=True)
    assert ok2 is True
    assert d2["ok"] is True


def test_score_model_uses_raw_score_times_conf_factor():
    # base conf_factor fn returns 0.5
    def conf_factor_fn(ctx, kind):
        return 0.5, {"base": 0.5}

    model = CryptoScoreModel(
        ScoreModelCfg(conf_floor=0.05, conf_cap=1.0, regime_w=0.25, geometry_w=0.25, liquidity_w=0.25, l3_w=0.15, micro_quality_w=0.10, veto_to_zero=True)
    )

    ctx = types.SimpleNamespace(
        regime_score=1.0,
        geometry_score=1.0,
        liquidity_score=1.0,
        l3_score=1.0,
        micro_quality_score=1.0,
    )
    cand = Candidate(kind="breakout", direction=1, raw_score=2.0, level_key="PDH")
    q = QualityState(quality_flags={}, veto=False, veto_reason="")

    res = model.score(ctx=ctx, kind=cand.kind, side=cand.direction, raw_score=cand.raw_score, quality_flags=q.quality_flags)
    assert res.conf_factor01 > 0.0
    assert res.final_score == pytest.approx(2.0 * res.conf_factor01, rel=1e-6)


def test_score_model_respects_min_final_threshold():
    model = CryptoScoreModel(
        ScoreModelCfg(conf_floor=0.05, conf_cap=1.0, regime_w=0.25, geometry_w=0.25, liquidity_w=0.25, l3_w=0.15, micro_quality_w=0.10, veto_to_zero=True)
    )

    ctx = types.SimpleNamespace(
        regime_score=0.0,
        geometry_score=0.0,
        liquidity_score=0.0,
        l3_score=0.0,
        micro_quality_score=0.0,
    )
    cand = Candidate(kind="breakout", direction=1, raw_score=0.1, level_key="PDH")
    q = QualityState(quality_flags={}, veto=False, veto_reason="")

    res = model.score(ctx=ctx, kind=cand.kind, side=cand.direction, raw_score=cand.raw_score, quality_flags=q.quality_flags)
    # conf_factor should be close to conf_floor (0.05) due to low component scores
    assert res.conf_factor01 >= 0.05
    assert res.final_score == pytest.approx(0.1 * res.conf_factor01, rel=1e-6)
