
import sys
import os
import time
from types import SimpleNamespace

# Mock necessary paths/modules
sys.path.append(os.getcwd())

from core.of_confirm_engine import OFConfirmEngine

def reproduce():
    print("Initializing OFConfirmEngine...")
    engine = OFConfirmEngine()
    
    # Mock inputs
    symbol = "BTCUSDT"
    tf = "1s"
    direction = "LONG"
    tick_ts = int(time.time() * 1000)
    price = 90000.0
    delta_z = 3.5
    
    # Mock Runtime
    runtime = SimpleNamespace()
    runtime.symbol = symbol
    runtime.config = {}
    runtime.dynamic_cfg = {}
    
    # Mock Config - Enable DN Gate logic that causes veto
    cfg = {
        "dn_tiers_decision": SimpleNamespace(tier1_usd=1000000.0, src="static", scale=1.0),
        "of_score_min": 0.65
    }
    
    # Mock Indicators - Set delta_usd low to trigger DN veto
    indicators = {
        "dn_tier": 1,
        "delta": 0.1, # very small delta
        "delta_z": 3.5,
        "price": 90000.0,
        "spread_bps": 1.0,
        "expected_slippage_bps": 1.0,
        # DN Gate inputs
        "dn_usd": 9000.0, # < 1M thresh
        "dn_tier_threshold": 1000000.0,
        "dn_tier_active": 1,
    }
    
    print("Calling build() with low notional (expecting Veto)...")
    
    # In the code, prior to calling build, strategy.py does some pre-calc. 
    # But inside build(), lines 1553-1602 (approx) check for DN Tier Veto.
    # Actually wait, lines 1577-1602 check `notional_usd < th` and `return None`.
    # Let's verify this path.
    
    # We need to make sure we don't crash before that check.
    # It needs `delta_event` which is not a param to build, 
    # but build calls `indicators.get("delta_notional_usd")`? 
    # No, look at line 1570: `notional_usd = abs(float(delta_event.get("delta",...`
    # Wait, build() DOES NOT take delta_event as param!
    # Let's re-read build() signature.
    # line 428: delta_z: float
    # It doesn't take delta or delta_event.
    
    # Ah, I need to check where `delta_event` comes from in `build`.
    # I don't see `delta_event` passed to `build`.
    # In `strategy.py`: `delta_event=delta_event` is NOT passed.
    # `delta_z` IS passed.
    
    # Re-reading `of_confirm_engine.py` lines 1553+.
    # `delta_event` is NOT defined in `build` scope?
    # Wait, I might have misread the file or `delta_event` is obtained from `indicators` or `runtime`?
    # Let's check `view_file` output again.
    
    # Line 1570: `notional_usd = abs(float(delta_event.get("delta", 0.0))) * float(price)`
    # Where is `delta_event` defined?
    # It is NOT defined in `build` local scope in the snippet I saw!
    # I saw lines 1000-1700.
    # Maybe it was passed in `kwargs`? No, signature is explicit.
    # Maybe it's a global? Unlikely.
    # Maybe I missed where it's defined.
    
    # Ah, lines 1500+ seem to be INSIDE `build`?
    # `build` starts at line 420.
    # I need to see if `delta_event` is retrieved from `indicators` or somewhere.
    
    # Let's check lines 420-500 of `of_confirm_engine.py` to see `build` start.
    pass

if __name__ == "__main__":
    reproduce()
