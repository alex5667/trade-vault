import json
import os
import sys

# Ensure we can import from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis
from services.slq_risk_adjust import maybe_apply_slq_to_risk_cfg

def main():
    print("=== Testing SLQ Dynamic Risk Adjust ===")
    
    # 1. Connect to local Redis 
    # (Since this runs inside python-worker or on host where redis port is exported)
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    try:
        r.ping()
    except Exception as e:
        print(f"Cannot connect to redis on localhost: {e}")
        return

    # 2. Inject mock SLQ payload into Redis
    symbol = "DOGEUSDT"
    side = "LONG"
    regime = "trend_up"
    key = f"slq:{symbol}:{side}:{regime}"
    
    # Mock data: 
    # post_sl_tp1_hit_rate = 0.30 (> 0.25 minimum)
    # sl_buffer_atr_q90 = 0.5 (need to bump stop by 0.5 * 0.7 = 0.35 ATR)
    mock_payload = {
        "n": 500,
        "sl_buffer_atr_q50": 0.2,
        "sl_buffer_atr_q75": 0.3,
        "sl_buffer_atr_q90": 0.5,
        "sl_buffer_atr_q95": 0.6,
        "post_sl_tp1_hit_rate": 0.30,
        "ts_ms": 0  # 0 means ignore max age check for testing
    }
    
    r.set(key, json.dumps(mock_payload))
    print(f"Injected mock data into {key}: {mock_payload}")
    
    # 3. Setup context and config
    class DummyCtx:
        regime = "trend_up"
        tp1_hit_prob = 0.60  # > 0.55 min
    
    ctx = DummyCtx()
    
    # Base config: SL = 1.2 ATR, TP1 = 0.78 ATR
    base_cfg = {
        "STOP_MODE": "ATR",
        "STOP_ATR_MULT": 1.2,
        "ROCKET_TP1_ATR_MULT": 0.78
    }
    
    print("\n--- Before SLQ ---")
    print(json.dumps(base_cfg, indent=2))
    
    # We must explicitly enable SLQ in ENV to bypass ENV checks
    os.environ["SLQ_ENABLE"] = "1"
    os.environ["SLQ_MIN_N"] = "100"
    os.environ["SLQ_POSTSL_TP1_MIN"] = "0.25"
    os.environ["SLQ_K"] = "0.7"
    
    # 4. Apply SLQ
    effective_cfg = maybe_apply_slq_to_risk_cfg(
        redis=r,
        ctx=ctx,
        symbol=symbol,
        side=side,
        cfg=base_cfg
    )
    
    print("\n--- After SLQ ---")
    print(json.dumps(effective_cfg, indent=2))
    
    # 5. Verify results
    assert effective_cfg["slq_used"] == 1
    assert effective_cfg["STOP_ATR_MULT"] == 1.2 + (0.5 * 0.7)  # 1.55
    expected_ratio = 1.55 / 1.2
    assert abs(effective_cfg["ROCKET_TP1_ATR_MULT"] - (0.78 * expected_ratio)) < 1e-5
    
    print("\n✅ Verification Successful: SL and TP1 were scaled correctly based on SLQ metrics!")
    
    # Cleanup
    r.delete(key)

if __name__ == "__main__":
    main()
