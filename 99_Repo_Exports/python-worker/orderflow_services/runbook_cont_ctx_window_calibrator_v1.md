# Runbook — cont_ctx_window_calibrator_v1

## Purpose
Post-analysis calibrator for `config:orderflow:{symbol}.cont_ctx_valid_ms`.
It widens the continuation-context memory only when rescued continuation near-miss
cohorts remain positive on shadow/paper outcomes.

## Inputs
- Redis Stream: `stream:ofc:cont_ctx_capture`
- Shadow Stream: `stream:ofc:cont_ctx_shadow_signals`
- Closed trades: `trades:closed`

## Outputs
- Prometheus endpoint on `CONT_CTX_CALIB_METRICS_PORT` (default `9137`)
- Redis hash: `metrics:cont_ctx_window_calib:last:{symbol}`
- Redis hash: `cfg:suggestions:cont_ctx_valid_ms:{symbol}`
- Optional apply: `HSET config:orderflow:{symbol} cont_ctx_valid_ms <value>`

## Safe defaults
- `CONT_CTX_CALIB_MODE=RECOMMEND`
- `CONT_CTX_CALIB_MIN_SAMPLE=40`
- `CONT_CTX_CALIB_MIN_RESCUED=20`
- `CONT_CTX_CALIB_FALSE_BREAKOUT_MAX=0.22`
- `CONT_CTX_CALIB_EXEC_P95_NORM_MAX=0.75`
- `CONT_CTX_CALIB_MAX_STEP_MS=30000`
- `CONT_CTX_CALIB_COOLDOWN_SEC=21600`

## How to inspect
```bash
redis-cli HGETALL metrics:cont_ctx_window_calib:last:BTCUSDT
redis-cli HGETALL cfg:suggestions:cont_ctx_valid_ms:BTCUSDT
redis-cli XRANGE stream:ofc:cont_ctx_shadow_signals - + COUNT 5
redis-cli XLEN stream:ofc:cont_ctx_capture
```

## Manual apply
```bash
redis-cli HSET config:orderflow:BTCUSDT cont_ctx_valid_ms 150000
```

## Rollback
```bash
redis-cli HSET config:orderflow:BTCUSDT cont_ctx_valid_ms 120000
redis-cli HSET config:orderflow:ETHUSDT cont_ctx_valid_ms 120000
```

## Disable auto-apply but keep observation
```bash
export CONT_CTX_CALIB_MODE=RECOMMEND
```

## Rollout phases

### Phase 0 — Observe only (24–72h)
```bash
CONT_CTX_CALIB_CAPTURE_ENABLE=1
CONT_CTX_CALIB_MODE=RECOMMEND
CONT_CTX_CALIB_SHADOW_ENABLE=1
```

### Phase 1 — Canary apply (BTCUSDT + ETHUSDT)
- MAX_STEP_MS=30000: first step 120000 → 150000
- If utility confirmed: 150000 → 180000

### Phase 2 — Wider canary (3–5 liquid symbols)
- Only after 3 consecutive stable runs

### Phase 3 — Bounded auto-apply
- Per-symbol auto-change
- Only with cooldown + hard constraints

## Failure modes
- No rows in `stream:ofc:cont_ctx_capture` → verify `CONT_CTX_CALIB_CAPTURE_ENABLE=1`
- No closed-trade samples → verify shadow executor enriches `calib=1`,
  `calib_kind=cont_ctx_window`, `candidate_window_ms`
- Frequent `blocked_cooldown` → reduce apply cadence or increase confidence/sample thresholds

## Key metrics
| Metric | Description |
|--------|-------------|
| `cont_ctx_calib_candidates_total` | Eligible single-leg continuation near-misses |
| `cont_ctx_calib_rescued_total` | Candidates rescued by wider window |
| `cont_ctx_calib_recommended_window_ms` | Current recommendation per symbol |
| `cont_ctx_calib_expectancy_r` | Expectancy of recommended cohort |
| `cont_ctx_calib_false_breakout_rate` | False breakout rate of recommended cohort |
| `cont_ctx_calib_apply_total` | Auto-apply count |
| `cont_ctx_calib_apply_block_total` | Blocked applies (cooldown/lock) |
