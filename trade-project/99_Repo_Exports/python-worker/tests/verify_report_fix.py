
import sys

# Add project root to sys.path
# [AUTOGRAVITY CLEANUP] sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from services.trade_metrics_service import TradeMetricsService


def test_metrics_finalization():
    tm = TradeMetricsService()
    m = tm.new_metrics()

    # Mock trade data matching user's report characteristics
    # APTUSDT: WR=66%, P/L net=+192.00, Fees=19.92, 100 trades
    mock_trades = []
    for i in range(100):
        is_win = i < 66
        pnl_net = 6.53 if is_win else -0.49 # Approximated from min/max in report
        mock_trades.append({
            "order_id": f"trade_{i}",
            "pnl_net": pnl_net,
            "fees": 0.19,
            "notional_usd": 500,
            "one_r_money": 5.0, # 1R = 5 USD
            "sl_atr": 1.5,
            "tp_atr": 3.0,
            "close_reason": "TRAIL_SL" if is_win else "INITIAL_SL",
            "entry_ts_ms": 1700000000000,
            "exit_ts_ms": 1700017256000, # Avg duration 17256s
        })

    print(f"Accumulating {len(mock_trades)} mock trades...")
    for t in mock_trades:
        tm.accumulate_trade(m, t)

    print("Finalizing metrics...")
    tm.finalize(m)

    # Assertions
    print(f"Total Trades: {m['total_trades']}")
    print(f"Wins: {m['wins']}")
    print(f"Expectancy R: {m['expectancy_r']:.4f}")
    print(f"PF Net: {m['profit_factor_net']:.4f}")
    print(f"Avg SL ATR: {m['avg_sl_atr']:.2f}")

    assert m['total_trades'] == 100
    assert m['expectancy_r'] > 0
    assert m['profit_factor_net'] > 0
    assert m['avg_sl_atr'] > 0

    print("✅ Verification PASSED: Metrics are correctly populated after finalize()")

if __name__ == "__main__":
    try:
        test_metrics_finalization()
    except Exception as e:
        print(f"❌ Verification FAILED: {e}")
        sys.exit(1)
