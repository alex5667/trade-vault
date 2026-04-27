import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.binance_futures_client import BinanceFuturesClient

def main():
    api_key = os.environ.get("BINANCE_DEMO_API_KEY")
    api_secret = os.environ.get("BINANCE_DEMO_API_SECRET")
    base_url = "https://testnet.binancefuture.com"

    client = BinanceFuturesClient(base_url=base_url, api_key=api_key, api_secret=api_secret)
    positions = client.get_position_risk()
    
    print("All non-zero positions:")
    for pos in positions:
        amt = float(pos.get("positionAmt", 0))
        if abs(amt) > 1e-9:
            print(f"{pos.get('symbol')} {pos.get('positionSide', '')} {amt}")

if __name__ == "__main__":
    main()
