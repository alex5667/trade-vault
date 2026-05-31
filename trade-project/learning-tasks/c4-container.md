# C4 Container Diagram

## Diagram

```mermaid
C4Container
  title Container diagram for Trade Scanner

  Person(trader, "Trader / Analyst", "Views dashboard")

  System_Ext(binance, "Binance Exchange", "Provides market data")

  Container_Boundary(trade_sys, "Trade Scanner") {
    Container(go_worker, "Go Ingestion Worker", "Go", "Connects to Binance WS, normalizes data, publishes to Redis")
    ContainerDb(redis, "Redis Event Bus", "Redis Streams", "High-throughput message broker and state cache")
    Container(python_worker, "Python Signal Engine", "Python, pandas, scikit-learn", "Computes indicators, evaluates ML models, applies risk gates")
    Container(nestjs_api, "NestJS Gateway", "TypeScript, NestJS", "Aggregates signals, provides REST/WS APIs for UI")
    Container(nextjs_ui, "Next.js Dashboard", "React, Next.js", "Renders real-time signals, charts, and system health")
    ContainerDb(timescale, "TimescaleDB", "PostgreSQL", "Stores historical klines, signals, decisions, and system metrics")
  }

  Rel(binance, go_worker, "WS streams (klines, ticks)", "WSS")
  Rel(go_worker, redis, "Publishes events (epoch_ms)", "Redis Protocol")
  Rel(python_worker, redis, "Consumes events, reads/writes state", "Redis Protocol")
  Rel(python_worker, timescale, "Persists signals, decisions, ledgers", "SQL/TCP")
  Rel(nestjs_api, redis, "Subscribes to signal outputs", "Redis Protocol")
  Rel(nestjs_api, timescale, "Queries historical data", "SQL/TCP")
  Rel(nextjs_ui, nestjs_api, "Fetches data, listens to events", "REST/WS")
  Rel(trader, nextjs_ui, "Views metrics and signals", "HTTPS")
```

## Description
This container diagram breaks down the Trade Scanner into its core deployable units.
1. **Go Ingestion**: Handles high-concurrency, low-latency connection management with the exchange.
2. **Redis Streams**: The backbone event bus, ensuring decoupling and replayability.
3. **Python Engine**: The analytical core, where complex statistical and ML calculations happen.
4. **NestJS / Next.js**: The user-facing presentation layer, delivering sub-second UI updates.
5. **TimescaleDB**: The long-term memory for backtesting and SRE monitoring.
