

from core.atr_tf_sanity_calibrator import AtrTfSanityCalibrator


def test_recommend_tf_picks_min_tf_meeting_target():
    cal = AtrTfSanityCalibrator(min_samples=50, switch_margin=0.0, hold_ms=0)
    rg = "na"

    # 1m слишком мало, 5m мало, 15m достаточно, 1h тоже достаточно.
    for i in range(60):
        cal.update_many(regime=rg, atr_bps_by_tf={"1m": 2.0, "5m": 4.0, "15m": 9.2, "1h": 12.0})

    ch = cal.recommend_tf(regime=rg, target_bps=8.8, fallback_tf="1m", now_ts_ms=1_000_000, current_tf="1m", allow_switch=True)
    assert ch.tf == "15m"
    assert ch.src == "calib_p50"
    assert ch.n >= 50


def test_hold_down_prevents_frequent_switch():
    cal = AtrTfSanityCalibrator(min_samples=10, switch_margin=0.0, hold_ms=600_000)
    rg = "na"
    for i in range(12):
        cal.update_many(regime=rg, atr_bps_by_tf={"1m": 2.0, "15m": 9.5, "1h": 20.0})

    # first switch (allowed)
    ch1 = cal.recommend_tf(regime=rg, target_bps=8.0, fallback_tf="1m", now_ts_ms=1_000_000, current_tf="1m", allow_switch=True)
    assert ch1.tf == "15m"

    # now "1h" looks best, but hold-down should keep current_tf=15m
    ch2 = cal.recommend_tf(regime=rg, target_bps=8.0, fallback_tf="1m", now_ts_ms=1_100_000, current_tf="15m", allow_switch=True)
    assert ch2.tf == "15m"


def test_persistence_roundtrip():
    cal = AtrTfSanityCalibrator(min_samples=10, switch_margin=0.0, hold_ms=0)
    rg = "range"
    for i in range(20):
        cal.update_many(regime=rg, atr_bps_by_tf={"1m": 3.0, "15m": 10.0})

    st = cal.dump_regime_state(symbol="BTCUSDT", regime=rg, updated_ts_ms=123)
    cal2 = AtrTfSanityCalibrator(min_samples=10, switch_margin=0.0, hold_ms=0)
    cal2.load_regime_state(st)

    ch = cal2.recommend_tf(regime=rg, target_bps=8.0, fallback_tf="1m", now_ts_ms=1_000, current_tf="1m", allow_switch=True)
    assert ch.tf == "15m"
