import re
import requests

url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
resp = requests.get(url)
data = resp.json()

symbol_info = {}
for s in data["symbols"]:
    sym = s["symbol"]
    try:
        price_filter = next(f for f in s["filters"] if f["filterType"] == "PRICE_FILTER")
        tick_size = price_filter["tickSize"]
        price_decimals = len(tick_size.rstrip('0').split('.')[1]) if '.' in tick_size else 0

        lot_filter = next(f for f in s["filters"] if f["filterType"] == "LOT_SIZE")
        step_size = lot_filter["stepSize"]
        min_qty = float(lot_filter["minQty"])
        step_decimals = len(step_size.rstrip('0').split('.')[1]) if '.' in step_size else 0

        symbol_info[sym] = {
            "price_decimals": price_decimals,
            "volume_decimals": step_decimals,
            "min_lot": min_qty,
            "contract_size": 1.0  
        }
    except Exception:
        pass

with open("core/instrument_config.py", "r") as f:
    content = f.read()

def replacer(m):
    obj_str = m.group(0)
    sym_match = re.search(r'symbol="([^"]+)"', obj_str)
    if sym_match:
        sym = sym_match.group(1)
        if sym in symbol_info:
            info = symbol_info[sym]
            
            # Use regex that carefully replaces the whole line or segment
            obj_str = re.sub(r'contract_size=[0-9.]+(?:,\s*#.*)?(?:,)?', f'contract_size=1.0,', obj_str)
            obj_str = re.sub(r'min_lot=[0-9.]+(?:,\s*#.*)?(?:,)?', f'min_lot={info["min_lot"]},', obj_str)
            obj_str = re.sub(r'price_decimals=[0-9]+(?:,\s*#.*)?(?:,)?', f'price_decimals={info["price_decimals"]},', obj_str)
            obj_str = re.sub(r'volume_decimals=[0-9]+(?:,\s*#.*)?(?:,)?', f'volume_decimals={info["volume_decimals"]},', obj_str)
            
            # To fix double commas if any were left by incorrect regexes previously or here
            obj_str = obj_str.replace(',,', ',')
            
            return obj_str
    return obj_str

new_content = re.sub(r'SymbolSpecs\([^)]+\)', replacer, content)

with open("core/instrument_config.py.new", "w") as f:
    f.write(new_content)

