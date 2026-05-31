
import logging
from typing import Dict, Any
from services.trade_metrics_service import TradeMetricsService

# Setup logger mock
logging.basicConfig(level=logging.INFO)

def run_repro():
    tm = TradeMetricsService()
    m = tm.new_metrics()

    # Mock trades based on the user's report
    # The report says: P/L net: +34411994.72 | Avg: +344119.95
    # for 100 trades.
    
    # Let's verify if summing up floats is doing something weird, or if single trade PnL is input incorrectly.
    # We will simulate a mix of winning and losing trades.
    
    # Scenario 1: Reasonable PnL inputs
    trades_reasonable = []
    for _ in range(56): # 56 wins
        trades_reasonable.append({
            "pnl_net": "50.0",
            "pnl": "50.0",
            "fees": "1.0",
            "notional_usd": "1000.0",
            "entry_ts_ms": 1000,
            "exit_ts_ms": 2000,
            "close_reason": "TP",
        })
    for _ in range(44): # 44 losses
        trades_reasonable.append({
            "pnl_net": "-40.0",
            "pnl": "-40.0",
            "fees": "1.0",
            "notional_usd": "1000.0",
            "entry_ts_ms": 1000,
            "exit_ts_ms": 2000,
            "close_reason": "SL",
        })
        
    m_reas = tm.new_metrics()
    for t in trades_reasonable:
        tm.accumulate_trade(m_reas, t)
    tm.finalize(m_reas)
    print("--- Reasonable Scenario ---")
    print(f"Total PnL: {m_reas['total_pnl']}")
    print(f"Avg PnL: {m_reas['expectancy_usd']}")
    
    # Scenario 2: Huge PnL inputs (simulating what might be happening)
    # If the user sees 34,411,994, maybe the input PnL is actually that large?
    # This checks if the metrics service handles large numbers correctly.
    trades_huge = []
    huge_pnl_val = 34411994.72 / 100  # Avg per trade ~344k
    
    trades_huge.append({
        "pnl_net": str(huge_pnl_val),
        "fees": "1.0",
        "notional_usd": "1000.0",
        "entry_ts_ms": 1000,
        "exit_ts_ms": 2000, 
        "close_reason": "TP"
    })
    
    m_huge = tm.new_metrics()
    for t in trades_huge:
        tm.accumulate_trade(m_huge, t)
    tm.finalize(m_huge)
    print("\n--- Huge Input Scenario (1 trade) ---")
    print(f"Total PnL: {m_huge['total_pnl']}")
    
    # Scenario 3: Checking formatting/parsing issues
    # Maybe scientific notation or something?
    trades_weird = [{
        "pnl_net": "3.44e7",
        "fees": "0", 
        "close_reason": "TP"
    }]
    m_weird = tm.new_metrics()
    for t in trades_weird:
        tm.accumulate_trade(m_weird, t)
    tm.finalize(m_weird)
    print("\n--- Weird Format Scenario ---")
    print(f"Total PnL: {m_weird['total_pnl']}")

if __name__ == "__main__":
    run_repro()
