from services.cancellation_spike_gate import CancellationSpikeGate, CancelSpikeParams


def _gate(mode="veto"):
    # deterministic, no robust z for unit tests
    p = CancelSpikeParams(
        enable=True,
        mode=mode,
        alpha_slow=0.2,
        ratio_th=2.0,
        abs_th=0.0,
        min_baseline=0.1,
        use_robust_z=False,
        window=50,
        min_samples=3,
        z_th=3.0,
        min_taker_rate=10.0,
    )
    return CancellationSpikeGate(p)


def test_long_veto_on_bid_support_pulled():
    g = _gate("veto")
    cfg2 = {"cancel_spike_mode": "veto"}
    # warmup (baseline ~10)
    for b in range(5):
        g.check(
            symbol="BTCUSDT", direction="LONG",
            cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=10.0,
            taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
            bucket_id=b, cfg2=cfg2,
        )
    # spike on bids (support pulled)
    dec = g.check(
        symbol="BTCUSDT", direction="LONG",
        cancel_bid_rate_ema=30.0, cancel_ask_rate_ema=10.0,
        taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
        bucket_id=99, cfg2=cfg2,
    )
    assert dec.allow is False
    assert "bid" in dec.reason


def test_short_veto_on_ask_support_pulled():
    g = _gate("veto")
    cfg2 = {"cancel_spike_mode": "veto"}
    for b in range(5):
        g.check(
            symbol="BTCUSDT", direction="SHORT",
            cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=10.0,
            taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
            bucket_id=b, cfg2=cfg2,
        )
    dec = g.check(
        symbol="BTCUSDT", direction="SHORT",
        cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=30.0,
        taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
        bucket_id=99, cfg2=cfg2,
    )
    assert dec.allow is False
    assert "ask" in dec.reason


def test_monitor_never_blocks():
    g = _gate("monitor")
    cfg2 = {"cancel_spike_mode": "monitor"}
    for b in range(5):
        g.check(
            symbol="BTCUSDT", direction="LONG",
            cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=10.0,
            taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
            bucket_id=b, cfg2=cfg2,
        )
    dec = g.check(
        symbol="BTCUSDT", direction="LONG",
        cancel_bid_rate_ema=1000.0, cancel_ask_rate_ema=10.0,
        taker_buy_rate_ema=0.0, taker_sell_rate_ema=0.0,
        bucket_id=99, cfg2=cfg2,
    )
    assert dec.allow is True
    assert dec.reason.startswith("cancel_spike_monitor_")


def test_pull_without_aggression_blocks_only_when_taker_low():
    g = _gate("veto")
    cfg2 = {"cancel_spike_mode": "veto", "cancel_spike_min_taker_rate": 10.0}
    for b in range(5):
        g.check(
            symbol="BTCUSDT", direction="LONG",
            cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=10.0,
            taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
            bucket_id=b, cfg2=cfg2,
        )
    # asks pulled but taker_buy high => allow
    dec1 = g.check(
        symbol="BTCUSDT", direction="LONG",
        cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=30.0,
        taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
        bucket_id=10, cfg2=cfg2,
    )
    assert dec1.allow is True
    # asks pulled and taker_buy low => block
    dec2 = g.check(
        symbol="BTCUSDT", direction="LONG",
        cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=30.0,
        taker_buy_rate_ema=0.0, taker_sell_rate_ema=100.0,
        bucket_id=11, cfg2=cfg2,
    )
    assert dec2.allow is False
    assert "pull_without_aggr" in dec2.reason


def test_snapshot_restore_is_equivalent():
    """Test that snapshot/restore produces equivalent gate behavior."""
    cfg2 = {
        "cancel_spike_enable": 1,
        "cancel_spike_mode": "monitor",
        "cancel_spike_alpha_slow": 0.4,
        "cancel_spike_ratio_th": 10.0,
        "cancel_spike_use_robust_z": 0,
        "cancel_spike_window": 10,
        "cancel_spike_min_samples": 2,
    }
    g1 = CancellationSpikeGate()
    # Build some state
    g1.check(
        symbol="ETHUSDT",
        bucket_id=1,
        direction="LONG",
        cancel_bid_rate_ema=1.0,
        cancel_ask_rate_ema=2.0,
        taker_buy_rate_ema=1.0,
        taker_sell_rate_ema=1.0,
        cfg2=cfg2,
    )
    g1.check(
        symbol="ETHUSDT",
        bucket_id=2,
        direction="LONG",
        cancel_bid_rate_ema=1.5,
        cancel_ask_rate_ema=2.5,
        taker_buy_rate_ema=1.0,
        taker_sell_rate_ema=1.0,
        cfg2=cfg2,
    )

    snap = g1.snapshot("ETHUSDT")
    assert snap.get("present")
    assert snap.get("symbol") == "ETHUSDT"

    g2 = CancellationSpikeGate()
    g2.restore(snap)

    # Next decision should match when fed identical input
    d1 = g1.check(
        symbol="ETHUSDT",
        bucket_id=3,
        direction="LONG",
        cancel_bid_rate_ema=1.6,
        cancel_ask_rate_ema=2.6,
        taker_buy_rate_ema=1.0,
        taker_sell_rate_ema=1.0,
        cfg2=cfg2,
    )
    d2 = g2.check(
        symbol="ETHUSDT",
        bucket_id=3,
        direction="LONG",
        cancel_bid_rate_ema=1.6,
        cancel_ask_rate_ema=2.6,
        taker_buy_rate_ema=1.0,
        taker_sell_rate_ema=1.0,
        cfg2=cfg2,
    )
    assert d1.allow == d2.allow
    assert d1.reason == d2.reason
    # Check baselines are approximately equal
    assert abs(float(d1.meta.get("base_support", 0.0)) - float(d2.meta.get("base_support", 0.0))) < 0.001


def test_out_of_order_bucket_fail_open_and_no_update():
    """Test that out-of-order buckets fail-open and don't update state."""
    gate = CancellationSpikeGate()
    cfg2 = {
        "cancel_spike_enable": 1,
        "cancel_spike_mode": "veto",
        "cancel_spike_alpha_slow": 0.5,
        "cancel_spike_ratio_th": 2.0,
        "cancel_spike_use_robust_z": 0,
        "cancel_spike_window": 10,
        "cancel_spike_min_samples": 0,
    }
    gate.check(
        symbol="SOLUSDT",
        bucket_id=10,
        direction="LONG",
        cancel_bid_rate_ema=1.0,
        cancel_ask_rate_ema=1.0,
        taker_buy_rate_ema=1.0,
        taker_sell_rate_ema=1.0,
        cfg2=cfg2,
    )
    snap_before = gate.snapshot("SOLUSDT")

    # Out-of-order bucket should be fail-open and not change state
    dec = gate.check(
        symbol="SOLUSDT",
        bucket_id=9,
        direction="LONG",
        cancel_bid_rate_ema=100.0,
        cancel_ask_rate_ema=100.0,
        taker_buy_rate_ema=100.0,
        taker_sell_rate_ema=100.0,
        cfg2=cfg2,
    )
    assert dec.allow is True
    assert "duplicate" in dec.reason.lower() or "ooo" in dec.reason.lower()
    snap_after = gate.snapshot("SOLUSDT")
    # State should not change (last_bucket_id should remain 10)
    assert snap_after.get("last_bucket_id") == snap_before.get("last_bucket_id")


def test_robust_z_score_veto():
    """Test that robust z-score triggers veto based on median/MAD instead of pure ratio."""
    p = CancelSpikeParams(
        enable=True,
        mode="veto",
        alpha_slow=0.02, # fast enough adjustment
        ratio_th=100.0,  # Make ratio impossible to hit
        abs_th=0.0,
        min_baseline=0.0,
        use_robust_z=True,  # enabled!
        window=10,
        min_samples=5,
        z_th=3.5,
        min_taker_rate=0.0,
    )
    g = CancellationSpikeGate(p)
    cfg2 = {}

    # 1. Warmup with stable ~10.0 cancellation rate
    # hist will contain: [10.0, 10.0, 10.0, 10.0, 10.0, 11.0, 9.0] etc.
    values = [10.0, 10.0, 10.0, 10.0, 10.0, 11.0, 9.0]
    for i, v in enumerate(values):
        g.check(
            symbol="ETHUSDT", direction="LONG",
            cancel_bid_rate_ema=v, cancel_ask_rate_ema=v,
            taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
            bucket_id=i, cfg2=cfg2,
        )

    # Median = 10.0, MAD = 0.0 -> max(eps, sigma).
    # Next value: 30.0
    # Ratio might be 3x, but ratio_th=100.0 (won't trigger on ratio).
    # Z-score = (30 - 10) / (small sigma) > 3.5
    dec = g.check(
        symbol="ETHUSDT", direction="LONG",
        cancel_bid_rate_ema=30.0, cancel_ask_rate_ema=10.0,
        taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
        bucket_id=99, cfg2=cfg2,
    )
    assert dec.allow is False
    assert "bid_support_pulled" in dec.reason

    # Test short side too
    # Next value ask spike: 30.0
    dec_short = g.check(
        symbol="ETHUSDT", direction="SHORT",
        cancel_bid_rate_ema=10.0, cancel_ask_rate_ema=30.0,
        taker_buy_rate_ema=100.0, taker_sell_rate_ema=100.0,
        bucket_id=100, cfg2=cfg2,
    )
    assert dec_short.allow is False
    assert "ask_support_pulled" in dec_short.reason
