from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
import time


def test_atr_sanity_calibrator_picks_fresh_and_matches_tf():
    from core.atr_sanity_calibrator import ATRSanityCalibrator

    now = get_ny_time_millis()
    cal = ATRSanityCalibrator(min_samples=3, max_age_ms=120_000)

    cands = [
        {"atr": 10.0, "src": "atr_json", "key": "k1", "tf": "M1", "ts_ms": now - 80_000, "age_ms": 80_000, "has_ts": 1},
        {"atr": 10.2, "src": "ta_last",  "key": "k2", "tf": "M1", "ts_ms": now - 1_000,  "age_ms": 1_000,  "has_ts": 1},
        {"atr": 50.0, "src": "atr_string","key":"k3","tf":"M1","ts_ms":0,"age_ms":0,"has_ts":0},
    ]

    d1 = cal.decide(tf_norm="M1", candidates=cands)
    assert d1.src_pref == "ta_last"
    assert d1.ok is False

    d2 = cal.decide(tf_norm="M1", candidates=cands)
    d3 = cal.decide(tf_norm="M1", candidates=cands)
    assert d3.ok is True
