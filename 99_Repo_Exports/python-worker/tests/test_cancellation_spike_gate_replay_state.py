from services.cancellation_spike_gate import CancellationSpikeGate, CancelSpikeParams


def _check(g: CancellationSpikeGate, *, sym: str, direction: str, bid: float, ask: float, bucket_id: int):
    return g.check(
        symbol=sym,
        direction=direction,
        cancel_bid_rate_ema=float(bid),
        cancel_ask_rate_ema=float(ask),
        taker_buy_rate_ema=100.0,
        taker_sell_rate_ema=100.0,
        bucket_id=int(bucket_id),
        cfg2={},
    )


def test_export_import_state_preserves_veto_behavior():
    params = CancelSpikeParams(
        enable=True,
        mode="veto",
        alpha_slow=0.3,
        ratio_th=3.0,
        abs_th=0.0,
        min_baseline=0.1,
        use_robust_z=False,
        window=64,
        min_samples=5,
        z_th=3.5,
        min_taker_rate=0.0,
    )
    g1 = CancellationSpikeGate(params=params)
    sym = "BTCUSDT"
    # warmup: baseline + history
    for b in range(1, 7):
        d = _check(g1, sym=sym, direction="LONG", bid=10.0, ask=10.0, bucket_id=b)
        assert d.allow is True
    st0 = g1.export_state(symbol=sym)
    # spike: support side pulled (ratio >= ratio_th)
    d1 = _check(g1, sym=sym, direction="LONG", bid=40.0, ask=10.0, bucket_id=100)
    assert d1.allow is False
    assert "support_pulled" in d1.reason
    st1 = g1.export_state(symbol=sym)

    g2 = CancellationSpikeGate(params=params)
    g2.import_state(state=st0, replace=True)
    d2 = _check(g2, sym=sym, direction="LONG", bid=40.0, ask=10.0, bucket_id=100)
    assert d2.allow == d1.allow
    assert d2.reason == d1.reason
    st2 = g2.export_state(symbol=sym)
    # state after spike should match (deterministic replay)
    assert st2.get("last_bucket_id") == st1.get("last_bucket_id")
    assert abs(float(st2.get("base_bid", 0.0)) - float(st1.get("base_bid", 0.0))) < 1e-9
    assert abs(float(st2.get("base_ask", 0.0)) - float(st1.get("base_ask", 0.0))) < 1e-9
    assert len(st2.get("hist_bid", [])) == len(st1.get("hist_bid", []))
    assert len(st2.get("hist_ask", [])) == len(st1.get("hist_ask", []))


def test_duplicate_bucket_is_fail_open():
    params = CancelSpikeParams(enable=True, mode="veto", min_samples=1, window=8, ratio_th=2.0, abs_th=0.0, min_baseline=0.1, use_robust_z=False)
    g = CancellationSpikeGate(params=params)
    sym = "ETHUSDT"
    d1 = _check(g, sym=sym, direction="SHORT", bid=5.0, ask=5.0, bucket_id=7)
    assert d1.allow is True
    d2 = _check(g, sym=sym, direction="SHORT", bid=5.0, ask=5.0, bucket_id=7)
    assert d2.allow is True
    assert d2.reason == "cancel_spike_duplicate_bucket"

