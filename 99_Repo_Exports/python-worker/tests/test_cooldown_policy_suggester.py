from services.cooldown_policy_suggester_service import propose_from_group

def test_propose_bumps_on_blocked_thin():
    cur = {"cooldown_reversal_sec": 30, "cooldown_continuation_sec": 15, "pressure_hi_sps": 0.12}
    out = propose_from_group(cur, blocked=100, replaced=40, emit_pending=3, emit_current=1,
                             p90_pressure=0.20, p90_spread=30.0, regime="thin", scenario="reversal")
    # Should bump cooldown due to thin regime + high blocked count
    assert out["cooldown_reversal_sec"] > 30
    # Should bump pressure_hi_sps based on p90_pressure (0.8 * 0.20 = 0.16)
    assert abs(out["pressure_hi_sps"] - 0.16) < 1e-9

def test_pressure_threshold_changes_only_if_big_delta():
    cur = {"cooldown_reversal_sec": 30, "cooldown_continuation_sec": 15, "pressure_hi_sps": 0.12}
    out = propose_from_group(cur, blocked=0, replaced=0, emit_pending=0, emit_current=0,
                             p90_pressure=0.14, p90_spread=10.0, regime="na", scenario="continuation")
    # 0.8 * 0.14 = 0.112. Delta from 0.12 is ~6.6% (< 15%) => keep 0.12
    assert abs(out["pressure_hi_sps"] - 0.12) < 1e-9

def test_propose_decrease_on_low_activity():
    cur = {"cooldown_reversal_sec": 30, "cooldown_continuation_sec": 15, "pressure_hi_sps": 0.12}
    out = propose_from_group(cur, blocked=0, replaced=0, emit_pending=0, emit_current=0,
                             p90_pressure=0.04, p90_spread=5.0, regime="na", scenario="reversal")
    # Low blocked + low pressure => decrease cooldown by 10%
    assert abs(out["cooldown_reversal_sec"] - 27.0) < 1e-9
