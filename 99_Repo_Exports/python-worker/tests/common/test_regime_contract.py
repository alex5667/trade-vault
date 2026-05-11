from common.regime_contract import RegimeLabel, RegimeSnapshot, RegimeSwitchPolicy, should_switch


def test_regime_snapshot_age_and_stale():
    now_ms = 1000000

    snap1 = RegimeSnapshot(
        symbol="BTCUSDT",
        label=RegimeLabel.TRENDING_BULL,
        direction=1,
        score=0.8,
        confidence=0.9,
        ts_calc_ms=now_ms - 5000  # age is 5000 ms
    )

    assert snap1.age_ms(now_ms) == 5000
    assert not snap1.is_stale(now_ms, 10000)
    assert snap1.is_stale(now_ms, 4000)

def test_should_switch_hysteresis():
    policy = RegimeSwitchPolicy(
        enter_trend_score=0.40,
        confirm_bars=3,
        min_hold_ms=180_000,
        fast_override_score=0.65
    )

    now_ms = 1_000_000

    # same regime => False
    switch, reason = should_switch(
        prev_label="range", next_label="range", score=0.1,
        confirm_count=5, now_ms=now_ms, last_switch_ms=now_ms - 200_000, policy=policy
    )
    assert switch is False
    assert reason == "same_regime"

    # different regime, but min_hold_ms not met => False
    switch, reason = should_switch(
        prev_label="range", next_label="trend", score=0.5,
        confirm_count=5, now_ms=now_ms, last_switch_ms=now_ms - 50_000, policy=policy
    )
    assert switch is False
    assert reason == "min_hold"

    # fast override ignores min_hold_ms
    switch, reason = should_switch(
        prev_label="range", next_label="trend", score=0.7,
        confirm_count=1, now_ms=now_ms, last_switch_ms=now_ms - 50_000, policy=policy
    )
    assert switch is True
    assert reason == "fast_override"

    # confirm_bars not met
    switch, reason = should_switch(
        prev_label="range", next_label="trend", score=0.5,
        confirm_count=2, now_ms=now_ms, last_switch_ms=now_ms - 200_000, policy=policy
    )
    assert switch is False
    assert reason == "need_confirm"

    # normal switch
    switch, reason = should_switch(
        prev_label="range", next_label="trend", score=0.5,
        confirm_count=4, now_ms=now_ms, last_switch_ms=now_ms - 200_000, policy=policy
    )
    assert switch is True
    assert reason == "confirmed_switch"
