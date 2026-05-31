from __future__ import annotations

from types import SimpleNamespace

from core.of_confirm_engine import OFConfirmEngine
from services.cancellation_spike_gate import CancellationSpikeGate, CancelSpikeParams


def test_of_confirm_cancellation_spike_veto():
    """Integration test to verify OFConfirmEngine uses CancellationSpikeGate."""

    # 1. Setup gate with tight parameters so it's easy to trigger
    p = CancelSpikeParams(
        enable=True,
        mode="veto",
        ratio_th=2.0,
        abs_th=0.0,
        min_baseline=0.0,
        use_robust_z=False,
        window=10,
        min_samples=2,
        z_th=3.5,
        min_taker_rate=0.0,
    )
    cancel_gate = CancellationSpikeGate(p)
    eng = OFConfirmEngine(version=3, cancel_gate=cancel_gate)

    # Engine required configuration
    cfg = {
        "require_strong_confirmation": False,
        "w_z": 0.3, "w_wp": 0.15, "w_reclaim": 0.2, "w_obi": 0.15, "w_ice": 0.15, "w_abs": 0.05,
        "score_z_ref": 3.0,
        "cancel_spike_enable": 1,
        "cancel_spike_mode": "veto",
    }

    # Simulate a reversal scenario (needs a sweep to be recognized as reversal)
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=True),
        last_obi_event=None,
        last_iceberg_event=None,
        last_sweep=SimpleNamespace(ts_ms=9000, kind="EQL", direction_bias="LONG"),
        last_reclaim=None,
        last_div=None,
    )

    # 2. Warm up the gate
    # We do a few engine build calls with normal cancellation rates
    # using event-time bucket increment
    for i in range(5):
        indicators_warmup = {
            "delta_z": 2.5,
            "cancel_bid_rate_ema": 10.0,
            "cancel_ask_rate_ema": 10.0,
            "taker_buy_rate_ema": 100.0,
            "taker_sell_rate_ema": 100.0,
        }
        ofc, dec = eng.build(
            symbol="BTCUSDT",
            tf="1s",
            direction="LONG",
            tick_ts_ms=1000 + (i * 1000),
            price=100.0,
            delta_z=2.5,
            runtime=runtime,
            cfg=cfg,
            indicators=indicators_warmup
        )
        assert ofc is not None
        assert bool(ofc.evidence.get("cancel_spike_veto", False)) is False

    # 3. Supply a spike in bid cancellation rates for a LONG signal
    # Ratio > 2.0 -> VETO
    indicators_spike = {
        "delta_z": 2.5,
        "cancel_bid_rate_ema": 30.0,  # 3x spike on support side
        "cancel_ask_rate_ema": 10.0,
        "taker_buy_rate_ema": 100.0,
        "taker_sell_rate_ema": 100.0,
        "bucket_id": 99,
    }

    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=10000,
        price=100.0,
        delta_z=2.5,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators_spike
    )

    # Verify the decision
    assert ofc is not None
    assert "bid_support_pulled" in str(getattr(ofc, "reason", ""))
    # Confirm it actually overrides the final decision
    if hasattr(ofc, "ok"):
        assert ofc.ok == 0

    # 4. Supply a spike in ask cancellation rate for a SHORT signal
    indicators_short_spike = {
        "delta_z": -2.5,
        "cancel_bid_rate_ema": 10.0,
        "cancel_ask_rate_ema": 30.0,  # 3x spike on support side (Ask for Short)
        "taker_buy_rate_ema": 100.0,
        "taker_sell_rate_ema": 100.0,
        "bucket_id": 100,
    }

    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="SHORT",
        tick_ts_ms=11000,
        price=100.0,
        delta_z=-2.5,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators_short_spike
    )

    assert ofc is not None
    assert "ask_support_pulled" in str(getattr(ofc, "reason", ""))
    if hasattr(ofc, "ok"):
        assert ofc.ok == 0
