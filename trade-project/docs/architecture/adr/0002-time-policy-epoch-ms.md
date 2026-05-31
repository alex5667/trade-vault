# ADR-0002: Use Epoch MS for Deterministic Time Policy

## Status
Accepted

## Context
In the high-frequency trade scanner, timestamps arrive from various sources (Binance REST, WebSocket, Python system time, Go system time, Postgres server). If time formats are mixed (ISO 8601 strings, UNIX seconds, microseconds) or rely on local timezones, comparing events becomes non-deterministic. We face issues like clock skew, out-of-order ticks, and drift between server processing time and exchange event time. A unified, monotonic, and standardized time policy is required to ensure accurate ML feature extraction and backtesting.

## Decision
We will use **Epoch milliseconds (epoch_ms)** as the canonical, absolute format for all timestamps across the entire pipeline (Go, Redis, Python, NestJS, Postgres). We will explicitly separate `event_time_ms` (when the exchange generated the event) from `ingest_time_ms` (when our Go worker received it).

## Alternatives considered
1. **ISO 8601 Strings (e.g., `2026-05-31T12:00:00.000Z`)**: Rejected. Parsing strings in hot paths adds unnecessary CPU overhead and allocation pressure, particularly in Go and Python.
2. **Epoch seconds**: Rejected. Lacks the resolution needed for high-frequency trading where multiple events occur within the same second.
3. **Epoch microseconds/nanoseconds**: Rejected. Python's `time.time()` float precision and Postgres timestamp limitations make sub-millisecond precision overly complex to maintain consistently without drift.

## Consequences
Positive:
- **Performance**: Integers (`int64` in Go, `int` in Python) are fast to serialize, deserialize, and compare.
- **Determinism**: Epoch MS is inherently UTC and timezone-agnostic, eliminating DST bugs.
- **Latency Tracking**: Direct integer subtraction (`ingest_time_ms - event_time_ms`) instantly yields the Unified Latency Contract (P4.1) drift metrics.

Negative:
- **Human Readability**: Integer timestamps are hard to read during manual database queries or log inspection. (Requires conversion functions in Grafana/SQL).

## Risks
- **Float Conversion**: In Python, converting float seconds to ms via `int(time.time() * 1000)` can lose precision or drift. Mitigation: Use `time.time_ns() // 1_000_000` or strict integer casting wrappers.
- **Clock Skew**: The server clock may drift relative to Binance. Mitigation: Continuously monitor `event_time_ms` vs `ingest_time_ms`.

## Observability
Metrics:
- `market_data_event_lag_ms`: Histogram of `(ingest_time_ms - event_time_ms)`.
- `market_data_stale_total`: Counter for out-of-order or severely delayed ticks.

Logs:
- Include `event_time_ms` and `ingest_time_ms` in all debug logs for event tracking.

## Rollout
- Update Go collectors to parse exchange time and emit strictly `int64` epoch_ms.
- Update Python DTOs (Pydantic) to validate `event_time_ms` as `int`.
- Update Postgres tables to use `BIGINT` or `TIMESTAMPTZ` with explicit ms casting.

## Rollback
- Reverting to mixed formats is not supported. Rollback involves fixing DTOs if a regression occurs.
