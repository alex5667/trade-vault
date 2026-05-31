from types import SimpleNamespace


def test_trade_monitor_extract_regime_from_signal():
    # NOTE: import from your actual module path
    from services.trade_monitor import _extract_regime_from_signal

    sig = SimpleNamespace(regime="RANGE")
    assert _extract_regime_from_signal(sig) == "range"

    sig2 = SimpleNamespace(meta={"regime_label": "SQUEEZE"})
    assert _extract_regime_from_signal(sig2) == "squeeze"
