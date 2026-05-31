# C4 System Context

## Diagram

```mermaid
C4Context
  title System Context diagram for Trade Scanner

  Person(trader, "Trader / Analyst", "Monitors signals, reviews backtests, and configures risk parameters.")
  
  System(trade_scanner, "Trade Scanner System", "Ingests market data, computes signals, evaluates risk gates, and stores historical metrics for ML analysis.")

  System_Ext(binance, "Binance API / WebSockets", "External exchange providing real-time tick/kline data and order execution.")
  
  Rel(trader, trade_scanner, "Views signals, monitors health, adjusts ML thresholds")
  Rel(trade_scanner, binance, "Subscribes to market data and sends paper/live orders")
  Rel(binance, trade_scanner, "Streams klines, ticks, orderbook updates")
```

## Description
The Trade Scanner is a latency-sensitive, event-driven trading infrastructure. It acts as the core brain for identifying trading opportunities (signals), evaluating execution risk, and storing high-fidelity data for offline ML training. The system is designed to be highly reliable, ensuring deterministic time handling and strict observability across all layers.
