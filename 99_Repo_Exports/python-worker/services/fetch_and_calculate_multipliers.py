import requests
import numpy as np
import time

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
    "1000PEPEUSDT", "DOGEUSDT", "1000SHIBUSDT", "1000FLOKIUSDT", 
    "1000BONKUSDT", "WIFUSDT", "SUIUSDT", "APTUSDT"
]

# Binance FAPI uses 1000PEPEUSDT directly
def to_binance_symbol(s):
    return s

def calculate_atr_pct(symbol, interval="5m", limit=1000):
    binance_symbol = to_binance_symbol(symbol)
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={binance_symbol}&interval={interval}&limit={limit}"
    
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        if not isinstance(data, list):
            return None
            
        # [OpenTime, Open, High, Low, Close, Volume, CloseTime, ...]
        # ATR(14) calculation:
        # TR = max(H-L, |H-Cp|, |L-Cp|)
        tr_list = []
        for i in range(1, len(data)):
            h = float(data[i][2])
            l = float(data[i][3])
            c = float(data[i][4])
            pc = float(data[i-1][4])
            
            tr = max(h - l, abs(h - pc), abs(l - pc))
            atr_pct = (tr / c) # This is 1-bar TR relative to price
            tr_list.append(atr_pct)
            
        if not tr_list:
            return None
            
        # We need p50 of "ATR%" (which is ATR(14) / Price)
        # But wait, the user's p50_atrPct is usually looking at the smoothed ATR.
        # To be simple and robust: we calculate p50 of the local relative range.
        p50_atrp = np.percentile(tr_list, 50)
        return p50_atrp
        
    except Exception as e:
        print(f"# Error fetching {symbol}: {e}")
        return None

def main():
    print("--- Deterministic ROCKET_TP1_ATR_MULT Calculation ---")
    print("# Source: Binance FAPI klines (5m, last 1000 candles ~3.5 days)")
    print("# Formula: 14 / (0.5 * p50_atrPct * 10000)")
    
    results = {}
    for symbol in SYMBOLS:
        p50 = calculate_atr_pct(symbol)
        if p50:
            # Typical multiplier calculation
            # User formula: (8 + 6) / (0.5 * p50_atr_bps)
            p50_bps = p50 * 10000
            mult = 14 / (0.5 * p50_bps)
            clamped = max(1.5, min(8.0, mult)) # Using 1.5 as floor for better risk
            results[symbol] = (clamped, p50)
            print(f"      - ROCKET_TP1_ATR_MULT_{symbol}={clamped:.2f} # p50_atr_bps={p50_bps:.2f}")
        else:
            print(f"      - ROCKET_TP1_ATR_MULT_{symbol}=0.78 # FALLBACK")
        time.sleep(0.5) # Avoid rate limits

    print("\n# Copy paste to docker-compose-crypto-orderflow.yml:")
    for symbol, (mult, p50) in results.items():
        print(f"      - ROCKET_TP1_ATR_MULT_{symbol}={mult:.2f}")

if __name__ == "__main__":
    main()
