from __future__ import annotations

from types import SimpleNamespace
from core.of_confirm_engine import OFConfirmEngine
from utils.time_utils import get_ny_time_millis

def test_strong_gate_staleness_veto():
    """Verify that a stale OBI event results in obi_stable=False even if stable_secs is high."""
    eng = OFConfirmEngine(version=2)
    now_ms = get_ny_time_millis()

    indicators = {"delta_z": 3.0}
    cfg = {
        "require_strong_confirmation": True,
        "obi_event_ttl_ms": 1000,  # 1s TTL
        "strong_z_min": 2.0,
        "strong_need_reversal": 2,
        "of_score_min": 0.0,
    }

    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=True), # A = 1
        last_obi_event={
            "ts_ms": now_ms - 1500, # 1.5s old, older than TTL
            "direction": "LONG",
            "obi": 0.5,
            "stable_secs": 10.0  # High but stale
        },
        last_iceberg_event=None,
        last_sweep=SimpleNamespace(ts_ms=now_ms - 100, kind="EQH_SWEEP", direction_bias="LONG"),
        last_reclaim=SimpleNamespace(ts_ms=now_ms - 50, hold_bars=1, direction_bias="LONG", pool_id="p", level=100.0), # B = 1
        last_div=None,
        cont_ctx_ts_ms=0,
    )

    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=now_ms,
        price=100.0,
        delta_z=3.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )

    assert ofc is not None
    assert ofc.scenario == "reversal"
    assert ofc.ok == 1  # Passed because A=1 and B=1, need=2
    # But C (obi_stable) must be False due to staleness
    assert ofc.evidence.get("obi_stable") == 0

def test_iceberg_distance_veto():
    """Verify that iceberg_strict is False if price distance exceeds dist_bp."""
    eng = OFConfirmEngine(version=2)
    now_ms = get_ny_time_millis()

    indicators = {"delta_z": 3.0}
    cfg = {
        "require_strong_confirmation": True,
        "iceberg_strict_refresh_min": 1,
        "iceberg_strict_duration_min": 1.0,
        "iceberg_strict_dist_bp": 5.0,  # 5bp tolerance
        "strong_need_reversal": 2,
        "of_score_min": 0.0,
    }

    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=True), # A = 1
        last_obi_event=None,
        last_iceberg_event={
            "ts_ms": now_ms - 100,
            "side": "bid",
            "refresh": 10,
            "duration": 5.0,
            "price": 51000.0 # Price is 50000. Iceberg at 51000 (~200bp away)
        },
        last_sweep=SimpleNamespace(ts_ms=now_ms - 100, kind="EQH_SWEEP", direction_bias="LONG"),
        last_reclaim=SimpleNamespace(ts_ms=now_ms - 50, hold_bars=1, direction_bias="LONG", pool_id="p", level=100.0), # B = 1
        last_div=None,
        cont_ctx_ts_ms=0,
    )

    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=now_ms,
        price=50000.0,
        delta_z=3.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )

    assert ofc is not None
    assert ofc.scenario == "reversal"
    assert ofc.evidence.get("iceberg_strict") == 0 # False due to distance

def test_indicators_propagation():
    """Verify that OBI analytical indicators (z, stacking) are propagated correctly."""
    eng = OFConfirmEngine(version=2)
    now_ms = get_ny_time_millis()

    indicators = {"delta_z": 2.0}
    cfg = {
        "require_strong_confirmation": True,
        "strong_need_continuation": 1,
        "of_score_min": 0.0,
    }

    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=False),
        last_obi_event={
            "ts_ms": now_ms - 100,
            "direction": "LONG",
            "obi": 0.6,
            "stable_secs": 2.0,
            "obi_z": 2.5,
            "stacking": 0.8,
            "concentration": 0.9
        },
        last_iceberg_event=None,
        last_sweep=None,
        last_reclaim=None,
        last_div=SimpleNamespace(ts_ms=now_ms - 50, kind="bullish_hidden"), # TRIGGER CONTINUATION PATH
        cont_ctx_ts_ms=0,
    )

    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=now_ms,
        price=50000.0,
        delta_z=2.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )

    assert ofc is not None
    assert ofc.scenario == "continuation"
    assert indicators.get("obi_z") == 2.5
    assert indicators.get("obi_stacking") == 0.8
    assert indicators.get("obi_concentration") == 0.9
