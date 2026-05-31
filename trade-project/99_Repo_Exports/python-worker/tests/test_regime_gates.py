from handlers.crypto_orderflow.components.gates import CryptoSignalGates


class DummyCtx:
    def __init__(self, regime: str):
        self.market_regime = regime

def test_regime_gate_match():
    # Setup gate with strict mode and blocked regimes
    import os
    os.environ["REGIME_GATE_STRICT"] = "1"
    os.environ["REGIME_GATE_BREAKOUT_BLOCK"] = "range"
    os.environ["REGIME_GATE_EXTREME_BLOCK"] = "trend"

    gates = CryptoSignalGates(None, None)

    # OLD behavior: substring match. "wide_range" contains "range", so it blocks "breakout".
    ctx_wide = DummyCtx("wide_range")
    allowed, reason = gates.check_regime_gate(ctx_wide, "breakout")

    # We validate that the CURRENT codebase ACTUALLY behaves this way (substring match)
    # This document the "compound token" matching behavior so it doesn't accidentally
    # get changed to set intersection blindly without adjusting config keys.
    assert not allowed, "Expected 'wide_range' to block breakout due to substring 'range'"
    assert reason == "VETO_REGIME_BREAKOUT_BLOCK"

    # Exact match blocks too
    ctx_exact = DummyCtx("range")
    allowed, reason = gates.check_regime_gate(ctx_exact, "breakout")
    assert not allowed
    assert reason == "VETO_REGIME_BREAKOUT_BLOCK"

    # Non-matching
    ctx_clean = DummyCtx("some_other_condition")
    allowed, reason = gates.check_regime_gate(ctx_clean, "breakout")
    assert allowed
    assert reason == "OK"
