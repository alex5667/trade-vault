from types import SimpleNamespace

from handlers.crypto_orderflow.utils.quality_gates import (
    DataQualityGate,
    LiquidityGate,
    RegimeGate,
    SignalConsistencyGate,
)
from utils.time_utils import get_ny_time_millis


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

def test_consistency_gate_breakout_requires_microshift_and_obi(monkeypatch):
    monkeypatch.setenv("CONSISTENCY_GATE_ENABLED", "1")
    monkeypatch.setenv("CONSISTENCY_APPLY_KINDS", "breakout")
    monkeypatch.setenv("BREAKOUT_REQUIRE_OBI", "1")
    monkeypatch.setenv("BREAKOUT_REQUIRE_OBI20", "1")
    monkeypatch.setenv("BREAKOUT_MIN_MICROPRICE_SHIFT_BPS", "0.0")
    monkeypatch.setenv("CONS_BREAKOUT_MIN_Z", "2.0")
    monkeypatch.setenv("CONS_BREAKOUT_MIN_OBI", "0.0")

    gate = SignalConsistencyGate.from_env()
    ctx = _ctx(
        of=_of(
            z_delta=3.0,
            obi=0.2,         # too weak
            obi_20=0.25,     # ok sign
            microprice_shift_bps_20=0.10,  # too low
        ),
        # Touch snapshot exists and is fresh but tag is wrong (should be depletion by default)
        touch_is_stale=False,
        touch_ask_tag="none",
        touch_ask_rho=0.05,
        touch_ask_traded_w=0.0,
    )
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code in {
        "VETO_BREAKOUT_OBI_TOO_WEAK",
        "VETO_BREAKOUT_MICROSHIFT_TOO_LOW",
        "VETO_BREAKOUT_TOUCH_TAG_MISMATCH",
        "VETO_BREAKOUT_TOUCH_RHO_LOW",
    }

def test_consistency_gate_breakout_accepts_depletion_touch(monkeypatch):
    monkeypatch.setenv("CONSISTENCY_GATE_ENABLED", "1")
    monkeypatch.setenv("CONSISTENCY_APPLY_KINDS", "breakout")
    monkeypatch.setenv("BREAKOUT_REQUIRE_OBI", "1")
    monkeypatch.setenv("BREAKOUT_REQUIRE_OBI20", "1")
    monkeypatch.setenv("BREAKOUT_MIN_MICROPRICE_SHIFT_BPS", "0.0")
    monkeypatch.setenv("CONS_BREAKOUT_MIN_Z", "2.0")
    monkeypatch.setenv("CONS_BREAKOUT_MIN_OBI", "0.0")
    monkeypatch.setenv("CONS_BREAKOUT_MIN_TOUCH_RHO", "0.10")
    monkeypatch.setenv("CONS_BREAKOUT_TOUCH_TAG_REQUIRED", "depletion")

    gate = SignalConsistencyGate.from_env()
    ctx = _ctx(
        of=_of(z_delta=3.0, obi=1.0, obi_20=1.0, microprice_shift_bps_20=0.0),
        touch_is_stale=False,
        touch_ask_tag="depletion",
        touch_ask_rho=0.25,
        touch_ask_traded_w=10.0,
    )
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is False
    assert dec.reason_code == "OK"

def test_data_quality_gate_veto_flags(monkeypatch):
    monkeypatch.setenv("DATA_QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("DATA_REQUIRE_EPOCH_TS", "0")  # irrelevant here
    monkeypatch.setenv("DATA_VETO_FLAGS", "stale_l2,l3_missing")
    monkeypatch.setenv("DATA_STRICT_MISSING_ATR_TS", "0")
    monkeypatch.setenv("DATA_QUARANTINE_VETO", "0")

    gate = DataQualityGate.from_env()
    now = get_ny_time_millis()
    ctx = _ctx(
        ts_event_ms=now,
        data_quality_flags=["stale_l2"],
    )
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", now_ms=now, last_ts_ms=None)
    assert dec.veto is True
    assert dec.reason_code == "VETO_DATA_FLAGS"

def test_data_quality_gate_atr_stale(monkeypatch):
    monkeypatch.setenv("DATA_QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("DATA_REQUIRE_EPOCH_TS", "0")
    monkeypatch.setenv("DATA_ATR_STALE_MAX_MS", "1000")
    monkeypatch.setenv("DATA_STRICT_MISSING_ATR_TS", "1")
    monkeypatch.setenv("DATA_QUARANTINE_VETO", "0")

    gate = DataQualityGate.from_env()
    now = get_ny_time_millis()
    ctx = _ctx(
        ts_event_ms=now,
        atr_ts_ms=now - 10_000,
    )
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", now_ms=now, last_ts_ms=None)
    assert dec.veto is True
    assert dec.reason_code == "VETO_ATR_STALE"


def test_regime_gate_denies_breakout_in_range(monkeypatch):
    monkeypatch.setenv("REGIME_GATE_ENABLED", "1")
    monkeypatch.setenv("REGIME_APPLY_KINDS", "breakout")
    monkeypatch.setenv("REGIME_DENY_BREAKOUT", "range,squeeze")
    g = RegimeGate.from_env()

    ctx = _ctx(of=_of(regime="range"))
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_REGIME"

def test_regime_gate_denies_absorption_in_trend(monkeypatch):
    monkeypatch.setenv("REGIME_GATE_ENABLED", "1")
    monkeypatch.setenv("REGIME_APPLY_KINDS", "absorption")
    monkeypatch.setenv("REGIME_DENY_ABSORPTION", "trending_bull,trending_bear,expansion")
    g = RegimeGate.from_env()

    ctx = _ctx(regime="trending_bull")
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_REGIME"

def test_regime_gate_denies_extreme(monkeypatch):
    monkeypatch.setenv("REGIME_GATE_ENABLED", "1")
    monkeypatch.setenv("REGIME_APPLY_KINDS", "extreme")
    monkeypatch.setenv("REGIME_DENY_EXTREME", "trending_bull")
    g = RegimeGate.from_env()

    # Passing RegimeInfo object with 'name' attribute
    regime_obj = SimpleNamespace(name="trending_bull")
    ctx = _ctx(regime=regime_obj)
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="extreme", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_REGIME"

def test_regime_gate_denies_obi_spike(monkeypatch):
    monkeypatch.setenv("REGIME_GATE_ENABLED", "1")
    monkeypatch.setenv("REGIME_APPLY_KINDS", "obi_spike")
    monkeypatch.setenv("REGIME_DENY_OBI_SPIKE", "range")
    g = RegimeGate.from_env()

    # Passing RegimeInfo object with 'label' attribute
    regime_obj = SimpleNamespace(label="range")
    ctx = _ctx(regime=regime_obj)
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="obi_spike", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_REGIME"

def test_regime_gate_allow_different_regime(monkeypatch):
    monkeypatch.setenv("REGIME_GATE_ENABLED", "1")
    monkeypatch.setenv("REGIME_APPLY_KINDS", "breakout")
    monkeypatch.setenv("REGIME_DENY_BREAKOUT", "range,squeeze")
    g = RegimeGate.from_env()

    ctx = _ctx(regime="trending_bull")
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is False
    assert dec.reason_code == "OK"

def test_regime_gate_missing_regime_fail_open(monkeypatch):
    monkeypatch.setenv("REGIME_GATE_ENABLED", "1")
    monkeypatch.setenv("REGIME_REQUIRE_PRESENT", "0")
    g = RegimeGate.from_env()

    ctx = _ctx() # no regime provided
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is False
    assert dec.reason_code == "OK"
    assert dec.notes == "missing_regime_fail_open"

def test_regime_gate_missing_regime_veto(monkeypatch):
    monkeypatch.setenv("REGIME_GATE_ENABLED", "1")
    monkeypatch.setenv("REGIME_REQUIRE_PRESENT", "1")
    g = RegimeGate.from_env()

    ctx = _ctx() # no regime provided
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_MISSING_REGIME"

def test_regime_gate_disabled(monkeypatch):
    monkeypatch.setenv("REGIME_GATE_ENABLED", "0")
    monkeypatch.setenv("REGIME_DENY_BREAKOUT", "range")
    g = RegimeGate.from_env()

    ctx = _ctx(regime="range")
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is False
    assert dec.reason_code == "OK"

def test_regime_gate_wrong_kind(monkeypatch):
    monkeypatch.setenv("REGIME_GATE_ENABLED", "1")
    monkeypatch.setenv("REGIME_APPLY_KINDS", "absorption")
    monkeypatch.setenv("REGIME_DENY_BREAKOUT", "range")
    g = RegimeGate.from_env()

    ctx = _ctx(regime="range")
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is False
    assert dec.reason_code == "OK"

def test_liquidity_gate_veto_spread(monkeypatch):
    monkeypatch.setenv("LIQ_GATE_ENABLED", "1")
    monkeypatch.setenv("LIQ_APPLY_KINDS", "breakout")
    monkeypatch.setenv("LIQ_MAX_SPREAD_BPS", "10")
    g = LiquidityGate.from_env()

    ctx = _ctx(of=_of(spread_bps=15.0, depth_bid_5=100, depth_ask_5=100, burst_flip_ratio=0.1))
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is True
    assert dec.reason_code == "VETO_SPREAD"

def test_liquidity_gate_depth_disabled_by_default(monkeypatch):
    monkeypatch.setenv("LIQ_GATE_ENABLED", "1")
    monkeypatch.setenv("LIQ_MIN_DEPTH_5", "0")  # disabled
    g = LiquidityGate.from_env()
    ctx = _ctx(of=_of(depth_bid_5=0.0, depth_ask_5=0.0))
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec.veto is False

def test_data_quality_gate_veto_touch_stale(monkeypatch):
    monkeypatch.setenv("DATA_QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("DATA_TOUCH_STALE_VETO", "1")
    monkeypatch.setenv("DATA_TOUCH_STALE_APPLY_KINDS", "breakout,absorption")

    gate = DataQualityGate.from_env()
    now = get_ny_time_millis()
    ctx = _ctx(
        ts_event_ms=now,
        touch_is_stale=True,
    )
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", now_ms=now, last_ts_ms=None)
    assert dec.veto is True
    assert dec.reason_code == "VETO_TOUCH_STALE"

def test_data_quality_gate_allow_touch_stale_disabled(monkeypatch):
    monkeypatch.setenv("DATA_QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("DATA_TOUCH_STALE_VETO", "0")
    monkeypatch.setenv("DATA_TOUCH_STALE_APPLY_KINDS", "breakout,absorption")

    gate = DataQualityGate.from_env()
    now = get_ny_time_millis()
    ctx = _ctx(
        ts_event_ms=now,
        touch_is_stale=True,
    )
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", now_ms=now, last_ts_ms=None)
    assert dec.veto is False
    assert dec.reason_code == "OK"

def test_data_quality_gate_allow_touch_stale_wrong_kind(monkeypatch):
    monkeypatch.setenv("DATA_QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("DATA_TOUCH_STALE_VETO", "1")
    monkeypatch.setenv("DATA_TOUCH_STALE_APPLY_KINDS", "absorption")

    gate = DataQualityGate.from_env()
    now = get_ny_time_millis()
    ctx = _ctx(
        ts_event_ms=now,
        touch_is_stale=True,
    )
    # the gate should not veto on 'breakout' if it only applies to 'absorption'
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", now_ms=now, last_ts_ms=None)
    assert dec.veto is False
    assert dec.reason_code == "OK"

def test_data_quality_gate_allow_touch_fresh(monkeypatch):
    monkeypatch.setenv("DATA_QUALITY_GATE_ENABLED", "1")
    monkeypatch.setenv("DATA_TOUCH_STALE_VETO", "1")
    monkeypatch.setenv("DATA_TOUCH_STALE_APPLY_KINDS", "breakout,absorption")

    gate = DataQualityGate.from_env()
    now = get_ny_time_millis()
    ctx = _ctx(
        ts_event_ms=now,
        touch_is_stale=False,
    )
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", now_ms=now, last_ts_ms=None)
    assert dec.veto is False
    assert dec.reason_code == "OK"

def test_liquidity_gate_per_symbol_override(monkeypatch):
    monkeypatch.setenv("LIQ_GATE_ENABLED", "1")
    monkeypatch.setenv("LIQ_MAX_SPREAD_BPS", "10") # Default
    monkeypatch.setenv("LIQ_MAX_SPREAD_BPS_ETHUSDT", "20") # Override

    g = LiquidityGate.from_env()

    # BTCUSDT uses default 10
    ctx1 = _ctx(of=_of(spread_bps=15.0, depth_bid_5=100, depth_ask_5=100, burst_flip_ratio=0.1))
    dec1 = g.evaluate(ctx=ctx1, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec1.veto is True
    assert dec1.reason_code == "VETO_SPREAD"

    # ETHUSDT uses override 20
    ctx2 = _ctx(of=_of(spread_bps=15.0, depth_bid_5=100, depth_ask_5=100, burst_flip_ratio=0.1))
    dec2 = g.evaluate(ctx=ctx2, symbol="ETHUSDT", kind="breakout", side="LONG")
    assert dec2.veto is False
    assert dec2.reason_code == "OK"

def test_liquidity_gate_depth_limits(monkeypatch):
    monkeypatch.setenv("LIQ_GATE_ENABLED", "1")
    monkeypatch.setenv("LIQ_MIN_DEPTH_5", "50")

    g = LiquidityGate.from_env()

    # Both deep enough
    ctx1 = _ctx(of=_of(spread_bps=5.0, depth_bid_5=60, depth_ask_5=60, burst_flip_ratio=0.1))
    dec1 = g.evaluate(ctx=ctx1, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec1.veto is False

    # Bid too shallow
    ctx2 = _ctx(of=_of(spread_bps=5.0, depth_bid_5=40, depth_ask_5=60, burst_flip_ratio=0.1))
    dec2 = g.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec2.veto is True
    assert dec2.reason_code == "VETO_DEPTH"

    # Ask too shallow
    ctx3 = _ctx(of=_of(spread_bps=5.0, depth_bid_5=60, depth_ask_5=40, burst_flip_ratio=0.1))
    dec3 = g.evaluate(ctx=ctx3, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec3.veto is True
    assert dec3.reason_code == "VETO_DEPTH"

def test_liquidity_gate_burst_flip_ratio(monkeypatch):
    monkeypatch.setenv("LIQ_GATE_ENABLED", "1")
    monkeypatch.setenv("LIQ_MAX_BURST_FLIP_RATIO", "0.8")

    g = LiquidityGate.from_env()

    ctx1 = _ctx(of=_of(spread_bps=5.0, depth_bid_5=100, depth_ask_5=100, burst_flip_ratio=0.5))
    dec1 = g.evaluate(ctx=ctx1, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec1.veto is False

    ctx2 = _ctx(of=_of(spread_bps=5.0, depth_bid_5=100, depth_ask_5=100, burst_flip_ratio=0.9))
    dec2 = g.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec2.veto is True
    assert dec2.reason_code == "VETO_BURST_FLIP"

def test_signal_consistency_gate_absorption_meanrev(monkeypatch):
    monkeypatch.setenv("CONSISTENCY_GATE_ENABLED", "1")
    monkeypatch.setenv("CONSISTENCY_APPLY_KINDS", "absorption")
    monkeypatch.setenv("CONS_ABSORPTION_MIN_Z", "2.0")
    monkeypatch.setenv("CONS_ABSORPTION_REQUIRE_WEAK_PROGRESS", "1")
    monkeypatch.setenv("CONS_ABSORPTION_REQUIRE_TOUCH_FRESH", "1")
    monkeypatch.setenv("CONS_ABSORPTION_TOUCH_TAG_REQUIRED", "refill")
    monkeypatch.setenv("CONS_ABSORPTION_MIN_TOUCH_RHO", "0.1")

    gate = SignalConsistencyGate.from_env()

    # LONG absorption hits bid side (support)
    ctx1 = _ctx(
        of=_of(z_delta=2.5, weak_progress=True),
        touch_is_stale=False,
        touch_bid_tag="refill",
        touch_bid_rho=0.2,
        touch_bid_traded_w=5.0,
    )
    dec1 = gate.evaluate(ctx=ctx1, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert dec1.veto is False

    # Fails z-score check
    ctx2 = _ctx(
        of=_of(z_delta=1.5, weak_progress=True),
        touch_is_stale=False,
        touch_bid_tag="refill",
        touch_bid_rho=0.2,
        touch_bid_traded_w=5.0,
    )
    dec2 = gate.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert dec2.veto is True
    assert dec2.reason_code == "VETO_ABSORPTION_Z_TOO_LOW"

    # Fails weak progress
    ctx3 = _ctx(
        of=_of(z_delta=2.5, weak_progress=False),
        touch_is_stale=False,
        touch_bid_tag="refill",
        touch_bid_rho=0.2,
        touch_bid_traded_w=5.0,
    )
    dec3 = gate.evaluate(ctx=ctx3, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert dec3.veto is True
    assert dec3.reason_code == "VETO_ABSORPTION_NO_WEAK_PROGRESS"

    # Fails touch tag
    ctx4 = _ctx(
        of=_of(z_delta=2.5, weak_progress=True),
        touch_is_stale=False,
        touch_bid_tag="none",
        touch_bid_rho=0.2,
        touch_bid_traded_w=5.0,
    )
    dec4 = gate.evaluate(ctx=ctx4, symbol="BTCUSDT", kind="absorption", side="LONG")
    assert dec4.veto is True
    assert dec4.reason_code == "VETO_ABSORPTION_TOUCH_TAG_MISMATCH"

def test_signal_consistency_gate_extreme_cancel_to_trade(monkeypatch):
    monkeypatch.setenv("CONSISTENCY_GATE_ENABLED", "1")
    monkeypatch.setenv("CONSISTENCY_APPLY_KINDS", "extreme")
    monkeypatch.setenv("EXTREME_L3_MAX_CANCEL_TO_TRADE", "5.0")

    gate = SignalConsistencyGate.from_env()

    ctx1 = _ctx(of=_of(cancel_to_trade_ratio=3.0))
    dec1 = gate.evaluate(ctx=ctx1, symbol="BTCUSDT", kind="extreme", side="LONG")
    assert dec1.veto is False

    ctx2 = _ctx(of=_of(cancel_to_trade_ratio=6.0))
    dec2 = gate.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="extreme", side="LONG")
    assert dec2.veto is True
    assert dec2.reason_code == "VETO_EXTREME_CANCEL_TO_TRADE_HIGH"

def test_signal_consistency_gate_obi_spike_sustained(monkeypatch):
    monkeypatch.setenv("CONSISTENCY_GATE_ENABLED", "1")
    monkeypatch.setenv("CONSISTENCY_APPLY_KINDS", "obi_spike")
    monkeypatch.setenv("CONS_OBI_SPIKE_REQUIRE_SUSTAINED", "1")

    gate = SignalConsistencyGate.from_env()

    ctx1 = _ctx(of=_of(obi_sustained=True))
    dec1 = gate.evaluate(ctx=ctx1, symbol="BTCUSDT", kind="obi_spike", side="LONG")
    assert dec1.veto is False

    ctx2 = _ctx(of=_of(obi_sustained=False))
    dec2 = gate.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="obi_spike", side="LONG")
    assert dec2.veto is True
    assert dec2.reason_code == "VETO_OBI_SPIKE_NOT_SUSTAINED"

def test_signal_consistency_gate_strict_missing_metrics(monkeypatch):
    monkeypatch.setenv("CONSISTENCY_GATE_ENABLED", "1")
    monkeypatch.setenv("CONSISTENCY_STRICT_MISSING_METRICS", "1")
    monkeypatch.setenv("CONSISTENCY_APPLY_KINDS", "breakout")

    gate = SignalConsistencyGate.from_env()

    # Empty context - should fail on first required metric (z_delta)
    ctx1 = _ctx()
    dec1 = gate.evaluate(ctx=ctx1, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert dec1.veto is True
    assert "MISSING" in dec1.reason_code

