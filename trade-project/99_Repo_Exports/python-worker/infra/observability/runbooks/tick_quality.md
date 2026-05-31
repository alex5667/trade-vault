# Tick quality runbook

This runbook covers alerts from `infra/observability/tick_quality_alerts.yml`.

## Signals / alerts

### 1) TickUnknownSideHigh / TickUnknownSideEMAHigh
Meaning:
  - Upstream side/aggressor direction is missing or inconsistent.
Impact:
  - Signed volume / delta / CVD can become biased if UNKNOWN is mapped to BUY/SELL.
Actions:
  1) Check `ticks.by_side_conf` via Step13:
     `python -m tools.smoke_tick_side_quality --hours 1 --limit 20000`
  2) If UNKNOWN share is persistent: set `CRYPTO_OF_UNKNOWN_SIDE_POLICY=quarantine`
     with `TICK_SIDE_QUARANTINE_SAMPLE=0.01`, inspect `stream:tick_side:quarantine`.
  3) If upstream started dropping side fields: fix normalizer / ws adapter.

### 2) TickTimeSourceNowHigh
Meaning:
  - event_ts is often assigned from wall clock (now) because payload ts is missing/invalid.
Impact:
  - Gates that rely on event-time can misbehave; replay determinism reduces.
Actions:
  1) Verify upstream timestamps in the tick payload (adapter).
  2) Check NTP/chrony on the host.
  3) Use Step13 report: inspect `by_ts_source` and `abs(now_ms - event_ts_ms)` stats.

### 3) TickTimeSourceStreamIdHigh / TickSkewAbsEMAHigh
Meaning:
  - event_ts comes from Redis stream id often, or payload is skewed vs stream time.
Impact:
  - Time-based policies may quarantine/drop too much or become noisy.
Actions:
  1) Confirm `CRYPTO_OF_MAX_TS_SKEW_MS` is tuned from Step14 tool:
     `python -m tools.recommend_tick_quality_policy --smoke /tmp/smoke.json --format env`
  2) If skew spikes: check exchange ws lag, adapter clock, and Redis delays.

## Rollback knobs

* `CRYPTO_OF_UNKNOWN_SIDE_POLICY=ignore_delta` (fail-open; safest to resume flow)
* Reduce quarantine sampling: `TICK_SIDE_QUARANTINE_SAMPLE=0.001`
* Increase skew tolerance temporarily: `CRYPTO_OF_MAX_TS_SKEW_MS=60000`
