import os
from dotenv import load_dotenv
from binance.um_futures import UMFutures

load_dotenv()
client = UMFutures(
    key=os.environ.get("BINANCE_API_KEY"), 
    secret=os.environ.get("BINANCE_API_SECRET"), 
    base_url=os.environ.get("BINANCE_BASE_URL", "https://testnet.binancefuture.com")
)
try:
    positions = client.account()["positions"]
    open_pos = [p for p in positions if float(p["positionAmt"]) != 0]

    for p in open_pos:
        sym = p["symbol"]
        amt = float(p["positionAmt"])
        print(f"Position: {sym} amt={amt}")
        orders = client.get_orders(symbol=sym)
        algo_orders = []
        try:
            algo_orders = client.get_open_algo_orders(symbol=sym)
        except:
            pass
        all_orders = orders + algo_orders
        if not all_orders:
            print("  [WARNING] NO OPEN ORDERS (No SL/TP)!")
        for o in all_orders:
            print(f"  Order: {o.get('type')} {o.get('side')} qty={o.get('origQty')} price={o.get('price')} stop={o.get('stopPrice')}")
except Exception as e:
    print("Error:", e)
