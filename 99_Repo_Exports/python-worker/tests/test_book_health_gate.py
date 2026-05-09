import asyncio
from unittest.mock import MagicMock

from services.orderflow.runtime import SymbolRuntime
from services.orderflow_strategy import OrderFlowStrategy
from utils.time_utils import get_ny_time_millis


async def main():
    redis_mock = MagicMock()
    strategy = OrderFlowStrategy(redis_mock, None, None, None)

    runtime = SymbolRuntime("BTCUSDT_G4_TEST")

    # Configure variables for the test
    runtime.config = {"book_stale_ms": 5000}

    now_ms = get_ny_time_millis()
    # Book is STALE! > 5000ms gap
    runtime.last_book_ts_ms = now_ms - 20000
    # But rate is HIGH!
    runtime.book_rate_ema = 65.0
    runtime.dynamic_cfg = {"book_rate_min_hz": 50.0}

    tick = {"price": 100.0, "qty": 1.0, "side": "BUY", "ts_ms": now_ms}

    # We need delta_event to trigger the gate code around L1568
    mock_delta_detector = MagicMock()
    mock_delta_detector.push.return_value = {"delta": 10.0, "z": 2.5}
    mock_delta_detector.z_threshold = 2.0
    runtime.delta_detector = mock_delta_detector

    # Set dummy objects to avoid full tick process crashing
    runtime.l3_queue = MagicMock()
    runtime.cvd_state = MagicMock()
    runtime.microbar = MagicMock()
    runtime.pressure = MagicMock()
    p_snap = MagicMock()
    p_snap.per_min_ema = 1.0
    p_snap.cd_rate_ema = 0.1
    runtime.pressure.snapshot.return_value = p_snap

    runtime.liq_service = MagicMock()
    liq_score = MagicMock()
    liq_score.liq_score = 1.0
    liq_score.liq_regime = "NORMAL"
    liq_score.depth_usd_min_5 = 1000.0
    liq_score.spread_bps = 5.0
    liq_score.book_rate_ema_hz = 65.0
    liq_score.book_stale_ms = 20000
    liq_score.why = ""
    liq_score.to_dict.return_value = {}
    runtime.liq_service.score.return_value = liq_score

    runtime.tick_dn_calib = MagicMock()
    tiers = MagicMock()
    tiers.tier0_usd = 10.0
    tiers.tier1_usd = 50.0
    tiers.tier2_usd = 100.0
    tiers.src = "test"
    runtime.tick_dn_calib.tiers.return_value = tiers

    runtime.delta_log_sampler = MagicMock()
    runtime.delta_log_sampler.should_log.return_value = False

    # Run
    res = await strategy.process_tick(runtime, tick)

    # Wait, process_tick returns None if VETO, or a dict if passed.
    # What we really want to check is whether OBI or iceberg_refresh were zeroed.
    # We can inspect metrics.
    from services.orderflow.metrics import of_session_outcome_total

    print("Testing G4 OR Gate...")
    print(f"Res: {res}")
    # of_session_outcome_total metrics:
    print("Metrics:")
    for sample in of_session_outcome_total.collect()[0].samples:
        if "BTCUSDT_G4_TEST" in sample.labels.values():
            print(sample)

    print("Test finished.")

if __name__ == "__main__":
    asyncio.run(main())
