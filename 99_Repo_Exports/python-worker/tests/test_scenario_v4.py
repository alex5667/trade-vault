from core.scenario_v4 import classify_v4


def test_base_range_meanrev():
    scn = classify_v4(
        sweep_recent=False,
        trend_dir=None,
        pressure_hi=False,
        churn_hi=False,
        exec_risk_bps=1.0,
        liq_regime="na",
        cancel_meta={},
        cfg={},
    )
    assert scn.id == "range_meanrev"


def test_vol_shock_priority_over_trend():
    scn = classify_v4(
        sweep_recent=False,
        trend_dir="LONG",
        pressure_hi=True,
        churn_hi=True,
        exec_risk_bps=20.0,
        liq_regime="na",
        cancel_meta={},
        cfg={},
    )
    assert scn.id == "vol_shock_news_proxy"


def test_saw_chop_from_cancel_meta():
    scn = classify_v4(
        sweep_recent=False,
        trend_dir="LONG",
        pressure_hi=False,
        churn_hi=False,
        exec_risk_bps=2.0,
        liq_regime="na",
        cancel_meta={"ready": 1, "veto_kind": "pull_without_aggr", "dir_taker": 0.1},
        cfg={},
    )
    assert scn.id == "saw_chop_spoof_proxy"

