import types

import pytest

from handlers.crypto_orderflow.core.crypto_orderflow_scoring import CryptoScoreModel, ScoreModelCfg
from handlers.crypto_orderflow.types.crypto_orderflow_pipeline_types import Candidate, QualityState

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, settings
from hypothesis import strategies as st

finite = st.floats(allow_nan=False, allow_infinity=False, width=64, min_value=-1e6, max_value=1e6)
unit = st.floats(allow_nan=False, allow_infinity=False, width=64, min_value=-3.0, max_value=3.0)


@given(
    raw=finite,
    regime=unit,
    geom=unit,
    liq=unit,
    l3=unit,
    micro=unit,
)
@settings(max_examples=500)
def test_conf_factor_is_clamped_and_final_score_is_product(raw, regime, geom, liq, l3, micro):
    model = CryptoScoreModel(ScoreModelCfg(conf_floor=0.05, conf_cap=1.0))
    ctx = types.SimpleNamespace(
        regime_score=regime,
        geometry_score=geom,
        liquidity_score=liq,
        l3_score=l3,
        micro_quality_score=micro,
    )
    cand = Candidate(kind="breakout", direction=1, raw_score=raw, level_key=None, reasons=[])
    q = QualityState(flags={}, veto=False, veto_reason="")
    res = model.score(ctx, cand, q)
    assert 0.0 <= res.conf_factor <= 1.0  # conf_factor is ratio
    assert res.final_score == pytest.approx(res.raw_score * res.conf_factor, rel=0, abs=0)


def test_veto_forces_conf_to_zero_and_final_score_zero():
    model = CryptoScoreModel(ScoreModelCfg(conf_floor=0.05, conf_cap=1.0, veto_to_zero=True))
    ctx = types.SimpleNamespace(regime_score=1, geometry_score=1, liquidity_score=1, l3_score=1, micro_quality_score=1)
    cand = Candidate(kind="breakout", direction=1, raw_score=2.0, level_key=None, reasons=[])
    q = QualityState(flags={}, veto=True, veto_reason="l2_breakout")
    res = model.score(ctx, cand, q)
    assert res.conf_factor == 0.0
    assert res.final_score == 0.0
    assert res.breakdown.get("veto") is True
