from core.active_arm_stabilizer import ActiveArmStabilizer


def test_hold_down_and_min_gap():
    s = ActiveArmStabilizer(hold_down_ms=10_000, min_switch_gap_ms=5_000)
    k = "BTC:trend:default:continuation"
    t0 = 100_000

    assert s.update(key=k, raw="A", now_ms=t0) == "A"

    # propose B but not stable => still A
    assert s.update(key=k, raw="B", now_ms=t0 + 1_000) == "A"
    assert s.update(key=k, raw="B", now_ms=t0 + 5_000) == "A"

    # stable long enough => switch to B
    assert s.update(key=k, raw="B", now_ms=t0 + 12_000) == "B"

    # try switch back quickly => min gap blocks
    assert s.update(key=k, raw="A", now_ms=t0 + 25_000) == "B"

    # after gap passed, stage A then wait hold-down
    assert s.update(key=k, raw="A", now_ms=t0 + 33_000) == "B"
    assert s.update(key=k, raw="A", now_ms=t0 + 45_000) == "A"
