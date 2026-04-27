# Bybit DQ alerts runbook (go-worker)

Scope: Bybit public WS ingestion (trades + orderbook) in go-worker.

Signals:
- `tick_gap_p95_ms{venue="bybit",symbol}`: p95 gaps between consecutive ticks (event-time `ts`, ms)
- `book_missing_seq_ema{venue="bybit",symbol}`: EMA of missing orderbook update-ids (`u`) on **delta** messages only
- `book_seq_resets_total{venue="bybit",symbol}`: resets due to snapshots / reconnects / first sync
- `bybit_futures_messages_total{symbol,type}`: processed messages (trade/book)
- `bybit_futures_reconnects_total{reason}`: reconnects
- `bybit_parse_errors_total`: WS parse failures

Common quick checks:
1) Is ingestion alive?
   - `sum(rate(bybit_futures_messages_total[1m]))`
   - `increase(bybit_futures_reconnects_total[10m])`
2) Is Redis receiving events?
   - `redis-cli XREVRANGE stream:tick_BTCUSDT + - COUNT 2`
   - `redis-cli XREVRANGE stream:book_BTCUSDT + - COUNT 2`
3) Look at go-worker logs (Bybit logs are prefixed with `[bybit]`)

---

## TradeBybitTickGapP95HighWarn

**Meaning**: `tick_gap_p95_ms` > 2000ms for 2m. Event-time gaps indicate missed trades, WS stalls, or time drift.

**Likely causes**
- Packet loss / jitter / temporary WS stalls
- Reconnect loop (check `bybit_futures_reconnects_total`)
- Incorrect parsing of `T` (timestamp) if Bybit changed payload
- Running illiquid symbols (gaps are expected)

**Immediate actions**
- Check `increase(bybit_futures_reconnects_total[10m])` and `TradeBybitReconnectStorm*` alerts.
- Validate message flow: `sum by(type)(rate(bybit_futures_messages_total[1m]))`.
- Validate Redis stream updates: `XREVRANGE stream:tick_<SYMBOL>`.

**Mitigation**
- If reconnects are high: tune WS timeouts/ping or investigate network.
- If only illiquid symbols: increase thresholds or `for:` in alert rules.

---

## TradeBybitTickGapP95HighCrit

**Meaning**: `tick_gap_p95_ms` > 10s for 30s. High probability of data loss; treat as incident for this symbol.

**Actions**
- Confirm if the symbol is actively traded. If yes, treat as ingestion outage.
- Check go-worker process health and logs.
- Check WS reachability from the host (DNS/TLS).

---

## TradeBybitTickGapP95ExtremeCrit

**Meaning**: p95 gaps > 30s for 10s. Severe outage for tick stream.

**Actions**
- Restart go-worker (controlled) if reconnect logic is stuck.
- If recurring: route traffic via stable network/VPN off, verify MTU, check host resource saturation.

---

## TradeBybitBookMissingSeqEmaWarn

**Meaning**: `book_missing_seq_ema` > 2 for 2m. Orderbook update-id gaps on delta messages.

**Important**: snapshots do not penalize missing; resets are tracked by `book_seq_resets_total`.

**Likely causes**
- WS packet loss (deltas dropped)
- Local book state desync (snapshot not received / late)
- Reconnect storms

**Immediate actions**
- Check `increase(book_seq_resets_total{venue="bybit"}[10m])`.
- Check `increase(bybit_futures_reconnects_total[10m])`.
- Verify that orderbook messages are flowing: `rate(bybit_futures_messages_total{type="book"}[1m])`.

**Mitigation**
- Prefer deeper orderbook channel (depth 50) over level1.
- If network unstable: reduce symbols per connection / increase read timeout.

---

## TradeBybitBookMissingSeqEmaCrit

**Meaning**: `book_missing_seq_ema` > 10 for 30s. Book quality likely unusable for OF features.

**Actions**
- Treat as incident for affected symbols.
- Consider temporarily gating OF-dependent decisions (downstream) until recovered.
- Force reconnect by restarting ingestion if stuck.

---

## TradeBybitBookSeqResetsHighWarn

**Meaning**: snapshots/reconnect resets > 3 per 10m.

**Actions**
- Correlate with `bybit_futures_reconnects_total`.
- Check host CPU/RAM/network saturation.
- Verify WS ping period (`BYBIT_WS_PING_PERIOD`) and read timeout (`BYBIT_WS_READ_TIMEOUT`).

---

## TradeBybitBookSeqResetsHighCrit

**Meaning**: reset storm (>10/10m). Usually a symptom of reconnect storm or severe packet loss.

**Actions**
- Immediate incident response: stabilize network, reduce subscriptions, restart service.

---

## TradeBybitReconnectStormWarn

**Meaning**: >2 reconnects in 10m.

**Actions**
- Check Bybit WS endpoint and DNS/TLS from host.
- Confirm `BYBIT_FUTURES_WS_URL` is correct (`wss://stream.bybit.com/v5/public/linear`).
- Look for errors in logs around reconnect reason.

---

## TradeBybitReconnectStormCrit

**Meaning**: >10 reconnects in 10m. Ingestion will have sustained gaps.

**Actions**
- Treat as incident.
- Reduce load: fewer symbols / separate connections.
- If persistent across hosts: Bybit outage or ISP routing issue.

---

## TradeBybitTickOutOfOrderHighWarn

**Meaning**: out-of-order/duplicate ticks > 5 per 10m.

**Actions**
- Check time parsing and `ts` monotonicity assumptions.
- Validate `ts` field is epoch ms.
- If duplicates are expected from exchange: raise thresholds.

---

## TradeBybitBookOutOfOrderHighWarn

**Meaning**: `u` (update-id) non-increasing events > 5 per 10m.

**Actions**
- If this spikes together with resets/reconnects: expected during recovery; tune thresholds.
- If stable network but persistent: bug in state machine; capture raw payloads for a short window.

---

## TradeBybitParseErrorsWarn

**Meaning**: parser errors are happening for at least 2m.

**Actions**
- Capture one failing raw WS message (log sampling) and compare with current schema.
- Check if only one channel breaks (trade vs orderbook) by correlating with `bybit_futures_messages_total`.
- If schema changed: update normalizer.

---

## TradeBybitIngestSilentCrit

**Meaning**: no Bybit messages processed for 5m (or metric absent).

**Actions (ordered)**
1) Is go-worker running?
   - systemd/docker status, logs.
2) Is WS reachable?
   - DNS/TLS/network.
3) Is Redis reachable?
   - `redis-cli -h <host> -p <port> PING`
4) Validate streams:
   - `XINFO STREAM stream:tick_BTCUSDT`
5) If stuck: restart go-worker.

---

## Notes on tuning
- For very illiquid symbols, tick gaps are normal. Increase `for:` or threshold.
- If you shard symbols across multiple workers, you may want to add label `instance`/`job` in rules (via relabeling) and alert per shard.
