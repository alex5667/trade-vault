
from services.pnl_math import SymbolSpec, spec_from_symbol_info, _get_default_symbol_info

def test_pnl_bug():
    symbol = "BTCUSDT"
    info = _get_default_symbol_info(symbol)
    print(f"Defaults for {symbol}: {info}")
    
    spec = spec_from_symbol_info(info)
    print(f"Spec uses_ticks: {spec.uses_ticks}")
    print(f"Spec tick_size: {spec.tick_size}")
    
    entry = 50000.0
    exit = 50001.0
    lot = 1.0
    side = "LONG"
    
    # Expected PnL: (50001 - 50000) * 1.0 = 1.0 USD
    pnl = spec.pnl_money(entry, exit, lot, side, symbol=symbol)
    print(f"Calculated PnL: {pnl}")
    
    if pnl > 1000:
        print("BUG CONFIRMED: PnL is massively inflated due to tick_size logic.")
    else:
        print("PnL looks correct.")

if __name__ == "__main__":
    test_pnl_bug()
