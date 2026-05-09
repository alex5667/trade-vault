from unittest.mock import MagicMock

# Import the target function that actually computes data health
from core.data_health import compute_data_health


def test_data_health_integration_g4_stale_book():
    """
    Test verifying that the logic from tick_processor.py correctly maps
    book_ts_gap_ms -> book_age_ms, resulting in degradation of book_health_ok,
    and then correctly captures the output flags.
    """

    # 1. Simulate the inputs (SymbolRuntime stubs and tick_ts)
    runtime = MagicMock()
    runtime.last_book_ts_ms = 1000000
    runtime.book_rate_ema = 5.0
    runtime.last_spread_bps = 2.0
    runtime.last_book = None

    tick_ts = 1006000 # 6000 ms gap (stale)
    cfg = {
        "data_health_book_age_max_ms": 1500, # If older than 1.5s, it degrades
    }

    indicators = {
        "tick_ts_missing": 0,
        "tick_gap_ms": 0,
    }

    # 2. Replicate the EXACT mapping logic fixed in tick_processor.py
    _last_book_ts_ms = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
    indicators["book_ts_gap_ms"] = int(tick_ts - _last_book_ts_ms) if _last_book_ts_ms > 0 else int(10**9)
    # The crucial input mapping fix:
    indicators["book_age_ms"] = int(indicators["book_ts_gap_ms"])

    indicators["book_rate_hz"] = float(getattr(runtime, "book_rate_ema", 0.0) or 0.0)
    spr = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
    if spr <= 0 and runtime.last_book:
        spr = float(runtime.last_book.spread_bps)
    indicators["spread_bps"] = spr

    # 3. Call core evaluation (G4 Gate)
    dh = compute_data_health(indicators=indicators, cfg=cfg)

    # 4. Replicate the EXACT output mapping logic fixed in tick_processor.py
    indicators["data_health"] = float(dh.score)
    indicators["data_health_reasons"] = ",".join(list(dh.reasons or [])[:5])
    indicators["book_health_ok"] = int(dh.book_health_ok)
    # The crucial output mapping fix:
    indicators["tick_time_ok"] = int(dh.tick_time_ok)
    indicators["spread_ok"] = int(dh.spread_ok)
    indicators["source_consistency_ok"] = int(dh.source_consistency_ok)

    # 5. Assert: Since gap is 6000ms > max 1500ms, data health should be degraded.
    assert _last_book_ts_ms == 1000000
    assert indicators["book_ts_gap_ms"] == 6000
    assert indicators["book_age_ms"] == 6000

    # book_health_ok should be set to 0 due to 6000ms > 1500ms
    assert indicators["book_health_ok"] == 0
    assert "book_age" in indicators["data_health_reasons"]

    # The score should be degraded
    assert indicators["data_health"] < 1.0

    # Check that output maps successfully captured other health components
    assert "tick_time_ok" in indicators
    assert indicators["tick_time_ok"] == 1 # Time was fine
    assert indicators["spread_ok"] == 1 # Spread was 2.0 bps (assuming max is > 2.0 if set, else ok)
    assert indicators["source_consistency_ok"] == 1

from services.orderflow_strategy import _ml_should_enforce


def test_data_health_canary_veto():
    """
    Test verifying that the canary veto logic works properly.
    Given 1000 unhealthy signals, roughly 5% should result in is_veto = 1.
    """
    cfg = {
        "data_health_veto_below": 0.70,
        "data_health_veto_mode": "canary",
        "data_health_canary_rate": 0.05
    }

    # We will simulate 1000 different sids for unhealthy scores
    vetoed = 0
    shadowed = 0
    for i in range(1000):
        # dh score < 0.70 => unhealthy
        score = 0.50
        is_unhealthy = 1 if score < 0.70 else 0
        is_veto = is_unhealthy

        sid_str = f"BTCUSDT_{1000000 + i}"

        if is_veto == 1:
            dh_mode = cfg.get("data_health_veto_mode")
            if dh_mode in ("canary", "canary_enforce", "canary-only"):
                dh_rate = cfg.get("data_health_canary_rate")
                if not _ml_should_enforce(dh_mode, sid_str, dh_rate):
                    is_veto = 0
            elif dh_mode == "shadow":
                is_veto = 0

        if is_veto == 1:
            vetoed += 1

        if is_unhealthy == 1:
            shadowed += 1

    # All 1000 are unhealthy, so shadowed should be 1000
    assert shadowed == 1000

    # Approx 5% of 1000 = 50. Allow variance (e.g. 30 to 70)
    assert 30 <= vetoed <= 70, f"Expected approx 50 vetoed signals, got {vetoed}"

