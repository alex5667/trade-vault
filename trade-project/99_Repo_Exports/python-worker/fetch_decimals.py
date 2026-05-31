import requests

url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
resp = requests.get(url)
data = resp.json()

symbol_info = {}
for s in data["symbols"]:
    sym = s["symbol"]
    # get price precision
    price_filter = next(f for f in s["filters"] if f["filterType"] == "PRICE_FILTER")
    tick_size = price_filter["tickSize"]
    price_decimals = len(tick_size.rstrip('0').split('.')[1]) if '.' in tick_size else 0

    # get lot precision
    lot_filter = next(f for f in s["filters"] if f["filterType"] == "LOT_SIZE")
    step_size = lot_filter["stepSize"]
    min_qty = lot_filter["minQty"]
    step_decimals = len(step_size.rstrip('0').split('.')[1]) if '.' in step_size else 0

    contract_size = 1.0  # usdt-m futures usually basically 1.0, wait, some are different? No, contractMultiplier?
    # but wait, Binance USDT-M futures contract size is in the coin amt, except for BTCUSDT etc where it's 1. 

    symbol_info[sym] = {
        "price_decimals": price_decimals,
        "volume_decimals": step_decimals,
        "min_lot": float(min_qty)
    }

print("1000PEPEUSDT", symbol_info.get("1000PEPEUSDT"))
print("1000SHIBUSDT", symbol_info.get("1000SHIBUSDT"))
print("DOGEUSDT", symbol_info.get("DOGEUSDT"))
print("1000FLOKIUSDT", symbol_info.get("1000FLOKIUSDT"))
print("1000BONKUSDT", symbol_info.get("1000BONKUSDT"))
print("WIFUSDT", symbol_info.get("WIFUSDT"))
print("SUIUSDT", symbol_info.get("SUIUSDT"))
print("APTUSDT", symbol_info.get("APTUSDT"))
print("ARBUSDT", symbol_info.get("ARBUSDT"))
print("AVAXUSDT", symbol_info.get("AVAXUSDT"))
print("LINKUSDT", symbol_info.get("LINKUSDT"))
print("INJUSDT", symbol_info.get("INJUSDT"))
