from handlers.regime_gate import RegimeGateCfg, regime_allows


def test_breakout_requires_nonnegative_score():
    cfg = RegimeGateCfg(breakout_min_score=0.0, absorption_max_score=0.0)
    assert regime_allows("breakout", +0.1, cfg) is True
    assert regime_allows("breakout", 0.0, cfg) is True
    assert regime_allows("breakout", -0.001, cfg) is False


def test_absorption_requires_nonpositive_score():
    cfg = RegimeGateCfg(breakout_min_score=0.0, absorption_max_score=0.0)
    assert regime_allows("absorption", -0.2, cfg) is True
    assert regime_allows("absorption", 0.0, cfg) is True
    assert regime_allows("absorption", +0.001, cfg) is False


def test_sweep_allowed_any():
    cfg = RegimeGateCfg(allow_sweep_any=True)
    assert regime_allows("sweep", -1.0, cfg) is True
    assert regime_allows("sweep", +1.0, cfg) is True


def test_extreme_same_as_breakout():
    cfg = RegimeGateCfg(extreme_min_score=0.2)
    assert regime_allows("extreme", 0.3, cfg) is True
    assert regime_allows("extreme", 0.1, cfg) is False


def test_obi_spike_same_as_breakout():
    cfg = RegimeGateCfg(obi_spike_min_score=0.2)
    assert regime_allows("obi_spike", 0.3, cfg) is True
    assert regime_allows("obi_spike", 0.1, cfg) is False


def test_unknown_signal_allowed():
    cfg = RegimeGateCfg()
    assert regime_allows("unknown_signal", -1.0, cfg) is True
    assert regime_allows("unknown_signal", +1.0, cfg) is True


def test_none_regime_score_allowed():
    cfg = RegimeGateCfg()
    assert regime_allows("breakout", None, cfg) is True
    assert regime_allows("absorption", None, cfg) is True
