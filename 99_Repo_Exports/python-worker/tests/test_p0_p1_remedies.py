import os
from unittest.mock import patch

# 1. Test Signal Outbox zero timestamp fallback
from core.signal_outbox import SignalOutboxPublisher


def test_signal_outbox_ts_zero_bypass():
    key_normal = SignalOutboxPublisher.build_dedup_key(
        "src", "strat", "BTCUSDT", "LONG", "test", "l1", "reason", 1680000000000, 1000
    )
    key_zero = SignalOutboxPublisher.build_dedup_key(
        "src", "strat", "BTCUSDT", "LONG", "test", "l1", "reason", 0, 1000
    )
    key_neg = SignalOutboxPublisher.build_dedup_key(
        "src", "strat", "BTCUSDT", "LONG", "test", "l1", "reason", -500, 1000
    )

    assert key_normal.startswith("dedup:")
    assert "bypass_" in key_zero, "Zero timestamp should use bypass hash"
    assert "bypass_" in key_neg, "Negative timestamp should use bypass hash"

# 2. Test Order Push Kill Switch
def test_order_push_kill_switch():
    with patch.dict(os.environ, {"ORDER_PUSH_ENABLE": "0"}):
        from importlib import reload

        import order_push_dispatcher
        reload(order_push_dispatcher)

        envelope = {"id": "test_id", "symbol": "BTCUSDT"}

        # This should return a mock response with stub=True
        res = order_push_dispatcher.post_order(envelope)
        assert res.get("payload", {}).get("stub") is True

# 3. Test Regime Gate Match
from handlers.crypto_orderflow.components.gates import CryptoSignalGates


def test_regime_gate_match():
    gates = CryptoSignalGates(None, None)
    gates._regime_strict = True
    gates._regime_breakout_block = {"range"}
    gates._regime_extreme_block = {"mean_reverting"}

    class DummyCtx:
        pass
    ctx = DummyCtx()

    # Substring match "range" inside "wide_range"
    ctx.regime = "wide_range"
    allowed, reason = gates.check_regime_gate(ctx=ctx, kind="breakout")
    assert not allowed, "Should block breakout because 'range' is in 'wide_range'"
    assert reason == "VETO_REGIME_BREAKOUT_BLOCK"

    # Should allow if not blocked
    ctx.regime = "trending"
    allowed, reason = gates.check_regime_gate(ctx=ctx, kind="breakout")
    assert allowed
    assert reason == "OK"
