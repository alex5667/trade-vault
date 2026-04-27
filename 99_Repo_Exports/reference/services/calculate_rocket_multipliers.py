import redis
import json
import os

# Configuration from previous context
FEES_BPS_RT = 8
BUFFER_BPS = 6
TP1_SHARE = 0.5
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TIMEFRAME = "5m"
REDIS_KEY_PATTERN = f"atrpct:quantiles:{TIMEFRAME}"

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
    "1000PEPEUSDT", "DOGEUSDT", "1000SHIBUSDT", "1000FLOKIUSDT", 
    "1000BONKUSDT", "WIFUSDT", "SUIUSDT", "APTUSDT", "ARBUSDT"
]

def calculate_multipliers():
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        return

    print("--- Calculated Rocket Multipliers ---")
    print(f"# Window: 14d, Timeframe: {TIMEFRAME}, Formula: (8+6)/(0.5*p50_atr_bps)")
    
    for symbol in SYMBOLS:
        try:
            raw_data = r.hget(REDIS_KEY_PATTERN, symbol)
            if not raw_data:
                print(f"# Warning: No data for {symbol} in Redis. Using default 0.78.")
                print(f"      - ROCKET_TP1_ATR_MULT_{symbol}=0.78")
                continue
                
            data = json.loads(raw_data)
            p50_atrpct = data.get("p50")
            
            if p50_atrpct is None or p50_atrpct <= 0:
                print(f"# Warning: Invalid p50 for {symbol}: {p50_atrpct}. Using default 0.78.")
                print(f"      - ROCKET_TP1_ATR_MULT_{symbol}=0.78")
                continue
            
            # p50_atr_bps = p50_atrpct * 10000
            # mult = (8 + 6) / (0.5 * p50_atr_bps)
            # mult = 14 / (5000 * p50_atrpct)
            mult = 0.0028 / p50_atrpct
            
            # Clamp [1.0, 8.0]
            mult_clamped = max(1.0, min(8.0, mult))
            
            print(f"      - ROCKET_TP1_ATR_MULT_{symbol}={mult_clamped:.2f} # p50_atrpct={p50_atrpct:.6f}")
            
        except Exception as e:
            print(f"# Error processing {symbol}: {e}")

if __name__ == "__main__":
    calculate_multipliers()
