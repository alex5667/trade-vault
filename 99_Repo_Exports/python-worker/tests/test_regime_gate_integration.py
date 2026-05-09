from types import SimpleNamespace

from handlers.crypto_orderflow.utils.quality_gates import RegimeGate


def _ctx(**kwargs):
    ctx = SimpleNamespace()
    for k, v in kwargs.items():
        setattr(ctx, k, v)
    return ctx

def test_regime_integration(monkeypatch):
    """
    Test that when the `MarketRegimeService` via `data_processor.py` attaches
    `regime` to the context (e.g. as a `RegimeInfo` or string), the `RegimeGate`
    correctly parses and vetoes restricted kinds.
    """
    monkeypatch.setenv("REGIME_GATE_ENABLED", "1")
    monkeypatch.setenv("REGIME_APPLY_KINDS", "breakout,absorption,extreme,obi_spike")
    monkeypatch.setenv("REGIME_DENY_BREAKOUT", "range,squeeze")
    monkeypatch.setenv("REGIME_DENY_ABSORPTION", "trending_bull,trending_bear,expansion")

    gate = RegimeGate.from_env()

    # 1. Simulate data_processor.py attaching regime to context
    # Usually it's an object with .name attribute, e.g. RegimeInfo(name='range', ...)
    class MockRegimeInfo:
        def __init__(self, name):
            self.name = name

    # Simulate a 'range' regime
    mock_regime_info_range = MockRegimeInfo(name="range")
    ctx_range = _ctx(regime=mock_regime_info_range)

    # Simulate a 'trending_bull' regime
    mock_regime_info_bull = MockRegimeInfo(name="trending_bull")
    ctx_bull = _ctx(regime=mock_regime_info_bull)

    # 2. Evaluate 'breakout' in 'range' -> SHOULD VETO
    decision_1 = gate.evaluate(ctx=ctx_range, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert decision_1.veto is True
    assert decision_1.reason_code == "VETO_REGIME"
    assert "range" in decision_1.notes

    # 3. Evaluate 'breakout' in 'trending_bull' -> SHOULD ALLOW
    decision_2 = gate.evaluate(ctx=ctx_bull, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert decision_2.veto is False
    assert decision_2.reason_code == "OK"

    # 4. Evaluate 'absorption' in 'range' -> SHOULD ALLOW (since range is not denied for absorption)
    decision_3 = gate.evaluate(ctx=ctx_range, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert decision_3.veto is False
    assert decision_3.reason_code == "OK"

    # 5. Evaluate 'absorption' in 'trending_bull' -> SHOULD VETO
    decision_4 = gate.evaluate(ctx=ctx_bull, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert decision_4.veto is True
    assert decision_4.reason_code == "VETO_REGIME"
    assert "trending_bull" in decision_4.notes
