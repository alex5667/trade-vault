from types import SimpleNamespace

from handlers.crypto_orderflow.utils.quality_gates import RegimeSessionLiquidityGate


def _ctx(**kwargs):
    ctx = SimpleNamespace()
    for k, v in kwargs.items():
        setattr(ctx, k, v)
    return ctx

def _of(**kwargs):
    of = SimpleNamespace()
    for k, v in kwargs.items():
        setattr(of, k, v)
    return of

def test_rslg_disabled(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "0")
    g = RegimeSessionLiquidityGate.from_env()
    ctx = _ctx()
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.apply is False
    assert dec.veto is False

def test_rslg_kind_not_applicable(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_APPLY_KINDS", "absorption")
    g = RegimeSessionLiquidityGate.from_env()
    ctx = _ctx()
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.apply is False
    assert dec.veto is False
    assert dec.notes == "kind_not_applicable"

def test_rslg_regime_not_allowed(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_ALLOW_REGIMES__BREAKOUT", "trending_bull,trending_bear")
    g = RegimeSessionLiquidityGate.from_env()

    # Missing regime => OK if allowlist is provided?
    # Actually wait: if allow_regimes is present and regime_s is empty, does it veto?
    # Code: `if allow_regimes and regime_s and regime_s not in allow_regimes:`
    # If regime is not present, it won't veto on missing! (unless strict metric maybe).
    ctx = _ctx(regime="range")
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_REGIME_NOT_ALLOWED"

    ctx = _ctx(of=_of(regime="trending_bull"))
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is False

def test_rslg_session_not_allowed(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_ALLOW_SESSIONS__BREAKOUT", "us_main,european")
    g = RegimeSessionLiquidityGate.from_env()

    ctx = _ctx(session="asian")
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_SESSION_NOT_ALLOWED"

    ctx2 = _ctx(session="us_main")
    dec2 = g.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec2.veto is False

def test_rslg_spread_too_wide(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_SPREAD_MAX_BPS_DEFAULT", "10.0")
    g = RegimeSessionLiquidityGate.from_env()

    ctx = _ctx(of=_of(spread_bps=12.0))
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_SPREAD_TOO_WIDE"

def test_rslg_depth_too_low(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_DEPTH_MIN_DEFAULT", "50.0")
    g = RegimeSessionLiquidityGate.from_env()

    ctx = _ctx(of=_of(depth_bid_5=40.0, depth_ask_5=100.0))
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_DEPTH_TOO_LOW"

def test_rslg_burst_flip_high(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_BURST_FLIP_MAX_DEFAULT", "0.8")
    g = RegimeSessionLiquidityGate.from_env()

    ctx = _ctx(of=_of(burst_flip_ratio=0.9))
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_BURST_FLIP_HIGH"

def test_rslg_daily_atr_bps_out_of_range(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_DAILY_ATR_BPS_MIN_DEFAULT", "50.0")
    monkeypatch.setenv("QUALITY_DAILY_ATR_BPS_MAX_DEFAULT", "200.0")
    g = RegimeSessionLiquidityGate.from_env()

    # Below min
    ctx = _ctx(of=_of(daily_atr_bps=40.0))
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_DAILY_ATR_BPS_OUT_OF_RANGE"

    # Above max
    ctx2 = _ctx(of=_of(daily_atr_bps=250.0))
    dec2 = g.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec2.veto is True
    assert dec2.reason_code == "VETO_DAILY_ATR_BPS_OUT_OF_RANGE"

def test_rslg_atr_q14_out_of_range(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_ATR_Q14_MIN_DEFAULT", "0.0")
    monkeypatch.setenv("QUALITY_ATR_Q14_MAX_DEFAULT", "1.0")
    g = RegimeSessionLiquidityGate.from_env()

    # Out of range (low)
    ctx = _ctx(of=_of(atr_q_14=-0.5))
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_ATR_Q14_OUT_OF_RANGE"

    # In range
    ctx2 = _ctx(of=_of(atr_q_14=0.5))
    dec2 = g.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec2.veto is False

def test_rslg_strict_missing_metrics(monkeypatch):
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_STRICT_MISSING_METRICS", "1")
    g = RegimeSessionLiquidityGate.from_env()

    # Missing spread
    ctx = _ctx()
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_MISSING_SPREAD"

    ctx2 = _ctx(of=_of(spread_bps=5.0))
    monkeypatch.setenv("QUALITY_DEPTH_MIN_DEFAULT", "10.0")
    g2 = RegimeSessionLiquidityGate.from_env()
    dec2 = g2.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec2.veto is True
    assert dec2.reason_code == "VETO_MISSING_DEPTH"

def test_rslg_hierarchy_overrides(monkeypatch):
    # Hierarchy: {PREFIX}__{SYM}__{KIND}__{REGIME} -> {PREFIX}__{SYM}__{KIND} -> {PREFIX}__{KIND}__{REGIME} -> {PREFIX}__{KIND} -> {PREFIX}_DEFAULT
    monkeypatch.setenv("QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("QUALITY_SPREAD_MAX_BPS_DEFAULT", "10.0")
    monkeypatch.setenv("QUALITY_SPREAD_MAX_BPS__BREAKOUT", "12.0")
    monkeypatch.setenv("QUALITY_SPREAD_MAX_BPS__BREAKOUT__TRENDING_BULL", "14.0")
    monkeypatch.setenv("QUALITY_SPREAD_MAX_BPS__BTCUSDT__BREAKOUT", "15.0")
    monkeypatch.setenv("QUALITY_SPREAD_MAX_BPS__BTCUSDT__BREAKOUT__TRENDING_BEAR", "20.0")

    g = RegimeSessionLiquidityGate.from_env()

    # BTCUSDT, breakout, trending_bear -> picks 20.0
    ctx1 = _ctx(regime="trending_bear", of=_of(spread_bps=18.0))
    dec1 = g.evaluate(ctx=ctx1, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec1.veto is False

    ctx1_fail = _ctx(regime="trending_bear", of=_of(spread_bps=22.0))
    dec1_fail = g.evaluate(ctx=ctx1_fail, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec1_fail.veto is True

    # ETHUSDT, breakout, trending_bear -> no sym match, goes to kind__regime which is not defined,
    # so falls back to kind -> 12.0
    ctx2 = _ctx(regime="trending_bear", of=_of(spread_bps=13.0))
    dec2 = g.evaluate(ctx=ctx2, symbol="ETHUSDT", kind="breakout", side="LONG")
    assert dec2.veto is True

    # ETHUSDT, breakout, trending_bull -> kind__regime match -> 14.0
    ctx3 = _ctx(regime="trending_bull", of=_of(spread_bps=13.0))
    dec3 = g.evaluate(ctx=ctx3, symbol="ETHUSDT", kind="breakout", side="LONG")
    assert dec3.veto is False
