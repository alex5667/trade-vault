from __future__ import annotations

# If your repo isn't a Python package, move _fsm_step into a separate module and import from there.
from services.smt_entry_candidate_service import RetestState, Setup, _fsm_step


def test_thin_requires_obi_or_iceberg():
    setup = Setup(bundle="b1", kind="continuation", leader="BTC", pick="ETH", trend_dir="UP", ts_ms=1000, ttl_ms=120000)
    st = RetestState(stage="WAIT_RETEST", zone_id="W_HIGH", touch_ts_ms=2000, away_ts_ms=2500)
    snap = {
        "close_px": 100.0,
        "zone_id": "W_HIGH",
        "zone_px_lo": 110.0,
        "zone_px_hi": 110.0,
        "zone_dist_bp": 10.0,
        "zone_side": "MID",
        "regime": "thin",
        "abs_lvl_th_unstable": 0,
        "obi_stable_sec": 0.0,
        "iceberg_strict": 0,
        "of_strong": 1,
        "of_dir": "LONG",
        "of_confirm_score": 1.0,
    }
    emit, reason = _fsm_step(setup=setup, st=st, snap=snap, now_ms=3000, touch_bp=12, away_bp=25, retest_bp=12)
    assert emit is False
    assert reason == "thin_need_obi_or_ice"


def test_zone_side_mismatch_blocks():
    setup = Setup(bundle="b1", kind="continuation", leader="BTC", pick="ETH", trend_dir="UP", ts_ms=1000, ttl_ms=120000)
    st = RetestState(stage="WAIT_RETEST", zone_id="X", touch_ts_ms=1, away_ts_ms=2)
    snap = {
        "close_px": 100.0,
        "zone_id": "X",
        "zone_px_lo": 110.0,
        "zone_px_hi": 110.0,
        "zone_dist_bp": 10.0,
        "zone_side": "RES",  # LONG wants SUP/MID
        "regime": "range",
        "of_strong": 1,
        "of_dir": "LONG",
        "of_confirm_score": 1.0,
    }
    emit, reason = _fsm_step(setup=setup, st=st, snap=snap, now_ms=3000, touch_bp=12, away_bp=25, retest_bp=12)
    assert emit is False
    assert reason == "zone_side_mismatch"
