#!/usr/bin/env python3
import argparse
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Ensure this script can import from services
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Dict, Any
from services.binance_futures_client import BinanceFuturesClient

def close_orphaned_positions(dry_run: bool = True):
    """
    Finds open positions that have NO open orders (protective SL/TP)
    and flattens them using a market order, wiping any remaining algo orders.
    """
    client_mode = os.environ.get("BINANCE_CLIENT_MODE", "")
    is_demo = client_mode.lower() == "demo"
    api_key_env = "BINANCE_DEMO_API_KEY" if is_demo else "BINANCE_API_KEY"
    api_secret_env = "BINANCE_DEMO_API_SECRET" if is_demo else "BINANCE_API_SECRET"
    
    api_key = os.environ.get(api_key_env) or os.environ.get("BINANCE_API_KEY")
    api_secret = os.environ.get(api_secret_env) or os.environ.get("BINANCE_API_SECRET")
    base_url = os.environ.get("BINANCE_DEMO_FUTURES_BASE_URL") if is_demo else os.environ.get("BINANCE_BASE_URL", "https://testnet.binancefuture.com")
    
    if not api_key or not api_secret:
        print(f"Error: {api_key_env} and {api_secret_env} must be set")
        sys.exit(1)

    print(f"Connecting to Binance: {base_url} (dry_run={dry_run})")
    client = BinanceFuturesClient(base_url=base_url, api_key=api_key, api_secret=api_secret)

    try:
        positions_risk = client.get_position_risk() or []
    except Exception as e:
        print(f"Failed to fetch position risk: {e}")
        return

    orphans = []
    
    # Identify positions
    for pos in positions_risk:
        amt = float(pos.get("positionAmt", 0))
        if abs(amt) < 1e-9:
            continue
            
        symbol = pos.get("symbol", "").upper()
        logical_side = "LONG" if amt > 0 else "SHORT"
        
        print(f"\nEvaluating position: {symbol} {logical_side} {amt}")
        
        try:
            plain_orders = client.get_open_orders(symbol) or []
            algo_orders = []
            try:
                algo_orders = client.get_open_algo_orders(symbol) or []
            except Exception:
                pass
                
            has_sl = False
            has_tp = False
            for o in plain_orders:
                otype = str(o.get('origType') or o.get('type') or '').upper()
                if 'STOP' in otype and 'TAKE_PROFIT' not in otype:
                    has_sl = True
                if 'TAKE_PROFIT' in otype:
                    has_tp = True
            for o in algo_orders:
                otype = str(o.get('origType') or o.get('type') or '').upper()
                if 'STOP' in otype and 'TAKE_PROFIT' not in otype:
                    has_sl = True
                if 'TAKE_PROFIT' in otype:
                    has_tp = True
            
            if not has_sl and not has_tp:
                print(f"  -> WARNING: No protective orders found for {symbol}. Marking as orphaned.")
                orphans.append({"symbol": symbol, "amt": amt, "side": logical_side})
            else:
                print(f"  -> Protected (SL:{has_sl}, TP:{has_tp}). Total orders: {len(plain_orders)+len(algo_orders)}")
        
        except Exception as e:
            print(f"  -> Failed to list orders for {symbol}: {e}")

    print(f"\nFound {len(orphans)} orphaned positions.")
    if not orphans:
        return

    if dry_run:
        print("\nDry run mode enabled. Would close the following:")
        for o in orphans:
            print(f"  {o['symbol']} {o['side']} {o['amt']}")
        return

    # Flatten orphans
    for o in orphans:
        sym = o['symbol']
        amt = abs(o['amt'])
        pos_side = "LONG" if o['side'] == "LONG" else "SHORT"
        close_side = "SELL" if o['side'] == "LONG" else "BUY"
        
        print(f"\n--- Flattening {sym} ---")
        # Step 1: Cancel all plain and algo orders just in case
        print("  Canceling resting plain orders...")
        try:
            client.cancel_all_orders(sym)
        except Exception as e:
            print(f"  Failed plain cancel: {e}")
        
        print("  Canceling resting algo orders...")
        try:
            client.cancel_all_algo_orders(sym)
        except Exception as e:
            print(f"  Failed algo cancel: {e}")

        # Step 2: Market Close
        params = {
            "symbol": sym
            "side": close_side
            "positionSide": pos_side, # Assuming hedge mode. Will fallback if one-way
            "type": "MARKET"
            "quantity": amt
        }
        print(f"  Sending trade: {params}")
        try:
            res = client.post_plain_order(params)
            print(f"  SUCCESS! Response: {res.get('orderId')} {res.get('status')}")
        except Exception as e:
            if "-2022" in str(e) or "-4061" in str(e): # One-way mode
                print("  Failed with positionSide, trying one-way mode ...")
                params.pop("positionSide", None)
                params["reduceOnly"] = "true"
                try:
                    res = client.post_plain_order(params)
                    print(f"  SUCCESS! Response: {res.get('orderId')} {res.get('status')}")
                except Exception as e2:
                    print(f"  FAILED to close: {e2}")
            else:
                print(f"  FAILED to close: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Close orphaned Binance Futures positions.")
    parser.add_argument("--execute", action="store_true", help="Actually place closing orders")
    args = parser.add_argument_group()
    args = parser.parse_args()
    
    close_orphaned_positions(dry_run=not args.execute)
