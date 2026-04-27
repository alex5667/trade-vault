# Trades:Closed Payload Example

This payload is published to the `trades:closed` Redis stream when a trade completes. It represents the "compact" version used for the event bus.

```json
{
  "order_id": "ORD-12345-ABCDE",
  "sid": "crypto-of:BTCUSDT:1707597472000",
  "symbol": "BTCUSDT",
  "strategy": "cryptoorderflow",
  "source": "Binance-Futures",
  "tf": "1m",
  "direction": "BUY",
  "entry_ts_ms": "1707597472000",
  "exit_ts_ms": "1707597532000",
  "entry_price": "42500.0",
  "exit_price": "42650.0",
  "lot": "0.01",
  "notional_usd": "425.0",
  "pnl_net": "1.50",
  "pnl_gross": "1.75",
  "fees": "0.25",
  "pnl_pct": "0.35",
  "tp_hits": "1",
  "tp1_hit": "1",
  "tp2_hit": "0",
  "tp3_hit": "0",
  "close_reason": "tp1_trailing",
  "close_reason_norm": "tp1_trailing",
  "trailing_profile": "tight_v1",
  "entry_regime": "trend",
  "baseline_exit_reason": "sl",
  "duration_ms": "60000",
  "mfe_pnl": "2.10",
  "mae_pnl": "-0.15",
  "r_multiple": "1.2",
  "model_ver": "v8_stack_prod_001"
}
```

## Key Fields
- `sid`: Normalized Signal ID (format: `crypto-of:{symbol}:{ts_ms}`), used to join with `ml_confirm`.
- `pnl_net`: Final profit/loss after fees.
- `tp_hits`: Number of take-profit targets hit before closure.
- `close_reason`: Why the trade was closed (e.g., `sl`, `tp`, `trailing`, `time_exit`).
- `entry_regime`: Segmentation key for calibration (e.g., `trend`, `range`).
- `mfe_pnl` / `mae_pnl`: Maximum Favorable / Adverse Excursion during the trade's life.
