from __future__ import annotations

from services.smt_entry_candidate_service import Setup, RetestState, _fsm_step


def test_retest_fsm_touch_away_retest_emit():
    setup = Setup(bundle="b1", kind="continuation", leader="BTCUSDT", pick="ETHUSDT", trend_dir="UP", ts_ms=1000, ttl_ms=120000)
    st = RetestState()
    now = 2000

    # 1) far -> no touch
    snap = {"close_px": 100.0, "zone_id": "W_HIGH", "zone_px_lo": 110.0, "zone_px_hi": 110.0, "zone_dist_bp": 100.0, "of_strong": 0, "of_dir": "NONE"}
    emit, r = _fsm_step(setup=setup, st=st, snap=snap, now_ms=now, touch_bp=12, away_bp=25, retest_bp=12)
    assert emit is False
    assert st.stage == "WAIT_TOUCH"

    # 2) touch -> stage WAIT_AWAY
    now += 100
    snap["zone_dist_bp"] = 10.0
    emit, r = _fsm_step(setup=setup, st=st, snap=snap, now_ms=now, touch_bp=12, away_bp=25, retest_bp=12)
    assert emit is False
    assert st.stage == "WAIT_AWAY"
    assert st.zone_id == "W_HIGH"

    # 3) away -> stage WAIT_RETEST
    now += 100
    snap["zone_dist_bp"] = 40.0
    emit, r = _fsm_step(setup=setup, st=st, snap=snap, now_ms=now, touch_bp=12, away_bp=25, retest_bp=12)
    assert emit is False
    assert st.stage == "WAIT_RETEST"

    # 4) retest but no of_strong -> no emit
    now += 100
    snap["zone_dist_bp"] = 10.0
    snap["of_strong"] = 0
    emit, r = _fsm_step(setup=setup, st=st, snap=snap, now_ms=now, touch_bp=12, away_bp=25, retest_bp=12)
    assert emit is False
    assert st.stage == "WAIT_RETEST"

    # 5) retest + of_strong + direction ok -> emit
    now += 100
    snap["of_strong"] = 1
    snap["of_dir"] = "LONG"
    # New policy requirements
    snap["of_confirm_score"] = 1.0  # Pass min score
    snap["zone_side"] = "MID"       # Valid for LONG
    snap["regime"] = "range"        # Not thin/news
    
    emit, r = _fsm_step(setup=setup, st=st, snap=snap, now_ms=now, touch_bp=12, away_bp=25, retest_bp=12)
    assert emit is True
    assert st.emitted == 1
