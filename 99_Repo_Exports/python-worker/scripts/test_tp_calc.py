import logging
from unittest.mock import MagicMock

from services.orderflow.signal_pipeline import SignalPipeline

logging.basicConfig(level=logging.INFO)

indicators = {}
runtime = MagicMock()
runtime.symbol = "BTCUSDT"
runtime.config = {"stop_atr_mult": 1.2, "tp_rr": "1.3,2.0,2.7"}

pipeline = SignalPipeline(publisher=MagicMock(), atr_cache=MagicMock())
pipeline._cached_sl_atr_mult_floor = 0.78 # Force the floor used

entry = 77464.90
direction = "SHORT"
atr = 31.73

print("\n--- Testing ROCKET_V1 regime override ---")
sl, tps, lot, out_atr, atr_meta = pipeline._calculate_levels(
    runtime=runtime,
    entry=entry,
    side=direction,
    indicators=indicators,
    trail_profile="rocket_v1"
)

print(f"SL: {sl}, TPs: {tps}")

rocket_mult = float(indicators.get("atr_fees_rocket_mult") or pipeline._get_rocket_multiplier("BTCUSDT"))
expected_tp1 = entry + (out_atr * rocket_mult) if (direction or "").upper() == "LONG" else entry - (out_atr * rocket_mult)
actual_tp1 = tps[0] if tps else 0
diff = abs(expected_tp1 - actual_tp1)
print(f"Expected TP1: {expected_tp1}, Actual TP1: {actual_tp1}, Diff: {diff}")
if diff > 1e-5:
    print("WARNING: Difference between expected and actual is large!")

