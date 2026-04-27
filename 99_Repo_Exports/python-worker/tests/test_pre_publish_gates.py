from types import SimpleNamespace

from handlers.crypto_orderflow.utils.pre_publish_gates import (
    HardDataQualityGate, RegimeSessionGate, ConsistencyGate
)


def test_hard_quality_gate_veto_missing_atr_ts(monkeypatch):
    monkeypatch.setenv("DATA_HARD_GATE_ENABLED", "1")
    monkeypatch.setenv("DATA_STRICT_MISSING_ATR_TS", "1")
    g = HardDataQualityGate.from_env()
    ctx = SimpleNamespace(ts_event_ms=1700000000000, of=SimpleNamespace(atr_ts_ms=None))
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    assert dec.veto is True
    assert dec.reason_code == "VETO_ATR_TS_MISSING"


def test_regime_session_gate_spread(monkeypatch):
    monkeypatch.setenv("RS_GATE_ENABLED", "1")
    monkeypatch.setenv("RS_SPREAD_MAX_BPS_DEFAULT", "10")
    g = RegimeSessionGate.from_env()
    of = SimpleNamespace(spread_bps=12.0, depth_bid_5=0.0, depth_ask_5=0.0, burst_flip_ratio=0.0, regime="range")
    ctx = SimpleNamespace(ts_event_ms=1700000000000, of=of)
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    assert dec.veto is True
    assert dec.reason_code == "VETO_RS_SPREAD"


def test_consistency_gate_breakout_require_obi(monkeypatch):
    monkeypatch.setenv("CONSISTENCY_GATE_ENABLED", "1")
    monkeypatch.setenv("BREAKOUT_REQUIRE_OBI", "1")
    monkeypatch.setenv("BTC_DELTA_Z_THRESHOLD", "2.0")
    monkeypatch.setenv("BTC_OBI_THRESHOLD", "0.35")
    g = ConsistencyGate.from_env()
    of = SimpleNamespace(z_delta=2.5, obi=0.10, obi_20=0.10, microprice_shift_bps_20=1.0)
    ctx = SimpleNamespace(of=of)
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_BREAKOUT_OBI_LOW"

def test_regime_session_gate_ctx_fallback(monkeypatch):
    monkeypatch.setenv("RS_GATE_ENABLED", "1")
    monkeypatch.setenv("RS_DEPTH_MIN_DEFAULT", "50")
    monkeypatch.setenv("RS_BURST_FLIP_MAX_DEFAULT", "0.8")
    
    g = RegimeSessionGate.from_env()
    
    # Values only on ctx, not on of
    ctx = SimpleNamespace(
        ts_event_ms=1700000000000, 
        spread_bps=5.0,
        depth_bid_5=60.0,
        depth_ask_5=60.0,
        burst_flip_ratio=0.5,
        of=SimpleNamespace(regime="range")
    )
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    assert dec.veto is False
    assert dec.reason_code == "OK"
    
    # Values only on ctx, triggering veto
    ctx2 = SimpleNamespace(
        ts_event_ms=1700000000000, 
        spread_bps=5.0,
        depth_bid_5=30.0,
        depth_ask_5=60.0,
        burst_flip_ratio=0.5,
        of=SimpleNamespace(regime="range")
    )
    dec2 = g.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="breakout")
    assert dec2.veto is True
    assert dec2.reason_code == "VETO_RS_DEPTH"

    ctx3 = SimpleNamespace(
        ts_event_ms=1700000000000, 
        spread_bps=5.0,
        depth_bid_5=60.0,
        depth_ask_5=60.0,
        burst_flip_ratio=0.9,
        of=SimpleNamespace(regime="range")
    )
    dec3 = g.evaluate(ctx=ctx3, symbol="BTCUSDT", kind="breakout")
    assert dec3.veto is True
    assert dec3.reason_code == "VETO_RS_BURST_FLIP"


def test_regime_session_gate_drift_tightening(monkeypatch):
    monkeypatch.setenv("RS_GATE_ENABLED", "1")
    monkeypatch.setenv("RS_DEPTH_MIN_DEFAULT", "100")
    monkeypatch.setenv("RS_DRIFT_TIGHTEN", "1")
    monkeypatch.setenv("RS_DRIFT_POWER", "2")
    
    # Mock load_drift_active_factor to return drift_factor = 2.0
    import handlers.crypto_orderflow.utils.pre_publish_gates as ppg
    original_load = ppg.load_drift_active_factor
    
    def mock_load(*args, **kwargs):
        return (2.0, 10.0, "mock_feat")
    
    ppg.load_drift_active_factor = mock_load
    
    try:
        g = RegimeSessionGate.from_env()
        # Depth is 300. Base min is 100.
        # drift_factor = 2.0, power = 2 => mult = 4.0
        # effective min = 400
        of = SimpleNamespace(regime="trend", depth_bid_5=300.0, depth_ask_5=300.0)
        ctx = SimpleNamespace(ts_event_ms=1700000000000, ts_ms=1700000000000, session="na", tf="na", venue="na", redis="mock", of=of)
        
        dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
        assert dec.veto is True
        assert dec.reason_code == "VETO_RS_DEPTH"
        assert "min_depth=300.000 < 400.000" in dec.notes
        
        # Test depth20 tightening
        monkeypatch.setenv("RS_DEPTH_MIN_DEFAULT", "0")
        monkeypatch.setenv("RS_DEPTH20_MIN_DEFAULT", "200")
        
        of2 = SimpleNamespace(regime="trend")
        ctx2 = SimpleNamespace(ts_event_ms=1700000000000, ts_ms=1700000000000, session="na", tf="na", venue="na", redis="mock", of=of2, depth_bid_20=500.0, depth_ask_20=500.0)
        dec2 = g.evaluate(ctx=ctx2, symbol="ETHUSDT", kind="breakout")
        assert dec2.veto is True
        assert dec2.reason_code == "VETO_RS_DEPTH20"
        assert "min_depth20=500.000 < 800.000" in dec2.notes
        
    finally:
        ppg.load_drift_active_factor = original_load

def test_regime_session_gate_overrides_matrix(monkeypatch):
    monkeypatch.setenv("RS_GATE_ENABLED", "1")
    monkeypatch.setenv("RS_SPREAD_MAX_BPS_DEFAULT", "10")
    # Matrix override for BTCUSDT breakout range
    monkeypatch.setenv("RS_SPREAD_MAX_BPS__BTCUSDT__breakout__range", "5")
    
    g = RegimeSessionGate.from_env()
    
    # 8 bps is > 5 bps (override threshold) -> VETO
    of1 = SimpleNamespace(regime="range", spread_bps=8.0)
    ctx1 = SimpleNamespace(ts_event_ms=1700000000000, of=of1)
    dec1 = g.evaluate(ctx=ctx1, symbol="BTCUSDT", kind="breakout")
    assert dec1.veto is True
    assert dec1.reason_code == "VETO_RS_SPREAD"
    
    # 8 bps is < 10 bps (default threshold for other regimes like trend) -> OK
    of2 = SimpleNamespace(regime="trend", spread_bps=8.0)
    ctx2 = SimpleNamespace(ts_event_ms=1700000000000, of=of2)
    dec2 = g.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="breakout")
    assert dec2.veto is False

def test_consistency_gate_extreme(monkeypatch):
    monkeypatch.setenv("CONSISTENCY_GATE_ENABLED", "1")
    monkeypatch.setenv("BTC_DELTA_Z_THRESHOLD", "2.0")
    monkeypatch.setenv("EXTREME_Z_THRESHOLD", "3.0") # Normally z_thr * 1.5, we override explicitly
    monkeypatch.setenv("EXTREME_L3_MAX_CANCEL_TO_TRADE", "5.0")

    g = ConsistencyGate.from_env()

    # Pass
    of1 = SimpleNamespace(z_delta=3.5, cancel_to_trade_ask=4.0)
    ctx1 = SimpleNamespace(of=of1)
    dec1 = g.evaluate(ctx=ctx1, symbol="BTCUSDT", kind="extreme", side="LONG")
    assert dec1.veto is False

    # Fail z_score
    of2 = SimpleNamespace(z_delta=2.5, cancel_to_trade_ask=4.0)
    ctx2 = SimpleNamespace(of=of2)
    dec2 = g.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="extreme", side="LONG")
    assert dec2.veto is True
    assert dec2.reason_code == "VETO_EXTREME_Z_LOW"
    
    # Fail cancel_to_trade_ask
    of3 = SimpleNamespace(z_delta=3.5, cancel_to_trade_ask=6.0)
    ctx3 = SimpleNamespace(of=of3)
    dec3 = g.evaluate(ctx=ctx3, symbol="BTCUSDT", kind="extreme", side="LONG")
    assert dec3.veto is True
    assert dec3.reason_code == "VETO_EXTREME_CANCEL_TO_TRADE_HIGH"

def test_consistency_gate_obi_spike(monkeypatch):
    monkeypatch.setenv("CONSISTENCY_GATE_ENABLED", "1")
    monkeypatch.setenv("CRYPTO_OBI_SPIKE_THR", "0.7")
    monkeypatch.setenv("OBI_SPIKE_REQUIRE_SUSTAINED", "1")

    g = ConsistencyGate.from_env()

    # Pass
    of1 = SimpleNamespace(obi_avg=0.8, obi_sustained=True)
    ctx1 = SimpleNamespace(of=of1)
    dec1 = g.evaluate(ctx=ctx1, symbol="BTCUSDT", kind="obi_spike", side="LONG")
    assert dec1.veto is False

    # Fail obi_avg
    of2 = SimpleNamespace(obi_avg=0.5, obi_sustained=True)
    ctx2 = SimpleNamespace(of=of2)
    dec2 = g.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="obi_spike", side="LONG")
    assert dec2.veto is True
    assert dec2.reason_code == "VETO_OBI_SPIKE_WEAK"

    # Fail sustained
    of3 = SimpleNamespace(obi_avg=0.8, obi_sustained=False)
    ctx3 = SimpleNamespace(of=of3)
    dec3 = g.evaluate(ctx=ctx3, symbol="BTCUSDT", kind="obi_spike", side="LONG")
    assert dec3.veto is True
    assert dec3.reason_code == "VETO_OBI_SPIKE_NOT_SUSTAINED"

    # Pass by alias CONS_OBI_SPIKE_REQUIRE_SUSTAINED=0
    monkeypatch.setenv("CONS_OBI_SPIKE_REQUIRE_SUSTAINED", "0")
    g2 = ConsistencyGate.from_env()
    of4 = SimpleNamespace(obi_avg=0.8, obi_sustained=False)
    ctx4 = SimpleNamespace(of=of4)
    dec4 = g2.evaluate(ctx=ctx4, symbol="BTCUSDT", kind="obi_spike", side="LONG")
    assert dec4.veto is False

def test_consistency_gate_absorption(monkeypatch):
    monkeypatch.setenv("CONSISTENCY_GATE_ENABLED", "1")
    monkeypatch.setenv("BTC_DELTA_Z_THRESHOLD", "2.0")
    monkeypatch.setenv("ABSORPTION_REQUIRE_TOUCH_REFILL", "1")
    monkeypatch.setenv("ABSORPTION_TOUCH_REFILL_MIN_RHO", "0.1")

    g = ConsistencyGate.from_env()

    # Pass (LONG absorption -> support -> needs touch_bid_tag="refill")
    of1 = SimpleNamespace(z_delta=2.5, weak_progress=True)
    ctx1 = SimpleNamespace(of=of1, touch_is_stale=False, touch_bid_tag="refill", touch_bid_rho=0.2)
    dec1 = g.evaluate(ctx=ctx1, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert dec1.veto is False

    # Fail z-score
    of2 = SimpleNamespace(z_delta=1.5, weak_progress=True)
    ctx2 = SimpleNamespace(of=of2, touch_is_stale=False, touch_bid_tag="refill", touch_bid_rho=0.2)
    dec2 = g.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert dec2.veto is True
    assert dec2.reason_code == "VETO_ABS_Z_LOW"

    # Fail weak progress
    of3 = SimpleNamespace(z_delta=2.5, weak_progress=False)
    ctx3 = SimpleNamespace(of=of3, touch_is_stale=False, touch_bid_tag="refill", touch_bid_rho=0.2)
    dec3 = g.evaluate(ctx=ctx3, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert dec3.veto is True
    assert dec3.reason_code == "VETO_ABS_WEAK_PROGRESS_FALSE"

    # Fail touch stale
    of4 = SimpleNamespace(z_delta=2.5, weak_progress=True)
    ctx4 = SimpleNamespace(of=of4, touch_is_stale=True, touch_bid_tag="refill", touch_bid_rho=0.2)
    dec4 = g.evaluate(ctx=ctx4, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert dec4.veto is True
    assert dec4.reason_code == "VETO_ABS_TOUCH_STALE"

    # Fail touch tag
    of5 = SimpleNamespace(z_delta=2.5, weak_progress=True)
    ctx5 = SimpleNamespace(of=of5, touch_is_stale=False, touch_bid_tag="none", touch_bid_rho=0.2)
    dec5 = g.evaluate(ctx=ctx5, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert dec5.veto is True
    assert dec5.reason_code == "VETO_ABS_NO_REFILL_TAG"

    # Fail touch rho
    of6 = SimpleNamespace(z_delta=2.5, weak_progress=True)
    ctx6 = SimpleNamespace(of=of6, touch_is_stale=False, touch_bid_tag="refill", touch_bid_rho=0.05)
    dec6 = g.evaluate(ctx=ctx6, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert dec6.veto is True
    assert dec6.reason_code == "VETO_ABS_REFILL_RHO_LOW"


def test_hard_quality_gate_symbol_override(monkeypatch):
    monkeypatch.setenv("DATA_HARD_GATE_ENABLED", "0")
    monkeypatch.setenv("DATA_HARD_GATE_ENABLED__BTCUSDT", "1")
    monkeypatch.setenv("DATA_REQUIRE_EPOCH_TS", "1")

    g = HardDataQualityGate.from_env()

    # Needs to be a valid ctx for non-vetoing cases, but missing epoch fails when enabled
    ctx = SimpleNamespace(ts_event_ms=10)

    # Disabled for ETHUSDT -> no veto
    dec_eth = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout")
    assert dec_eth.veto is False
    assert dec_eth.notes == "disabled"

    # Enabled for BTCUSDT -> vetoes due to bad timestamp
    dec_btc = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    assert dec_btc.veto is True
    assert dec_btc.reason_code == "VETO_BAD_TS_NOT_EPOCH"

