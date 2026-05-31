---
title: "ADR-0005: TCA EMA Priors Pipeline"
date: 2026-05-15
status: proposed
tags: [adr, ml, tca, execution, redis-streams, v5_of]
component: python-worker + new-service
schema: v5_of
parent_adr: ADR-0004
---

## Context

ADR-0004 Phase P1 outlined TCA (Transaction Cost Analysis) EMA priors as a planned feature group requiring **separate Redis time-series infrastructure**. These features cannot be computed inline in `of_confirm_engine.py` because they require:

- Cross-fill aggregation (each fill contributes to running EMA per `{symbol, kind, session}` bucket)
- Persistence across process restarts
- Backfill capability for replay

## Feature group

```
tca_eff_spread_bps_ema             # effective spread = 2 * |fill_px - mid_at_arrival|
tca_realized_spread_1s_bps_ema     # |mid(t) - mid(t+1s)| signed by side
tca_realized_spread_5s_bps_ema     # ... 5s window
tca_perm_impact_1s_bps_ema         # mid(t+1s) - mid(t) signed by side
tca_perm_impact_5s_bps_ema         # ... 5s window
tca_is_bps_ema                     # implementation shortfall = arrival_mid → execution_px
tca_samples                        # rolling sample count (guard for low-sample priors)
tca_stale_ms                       # ms since last EMA update (staleness gate)
```

## Decision (proposed)

Build a dedicated **tca-priors-exporter** service that:

1. Subscribes to fills stream (`stream:fills:filled`) via Redis consumer group
2. Maintains per-`{symbol, kind, session}` EMA state in Redis hash:
   ```
   tca:ema:{symbol}:{kind}:{session_bucket}
     fields: eff_spread, realized_1s, realized_5s, perm_1s, perm_5s, is_bps, samples, last_update_ms
   ```
3. Publishes Prometheus gauges `tca_*_bps_ema{symbol, kind, session}`
4. `of_confirm_engine.py` reads from `indicators_with_v4` after market_state hydrates the hash via single `HGETALL`

**EMA half-life:** configurable per feature (default: 5 min for spread, 1 hour for impact).

**Staleness guard:**
- `tca_samples < TCA_MIN_SAMPLES` (default 30) ⇒ feature emitted as 0.0 + zero-rate counter increment
- `tca_stale_ms > TCA_STALE_MAX_MS` (default 600_000) ⇒ feature emitted as 0.0

## Risks

- **Cold start** — new symbols have no priors; mitigate by emitting `tca_samples` and gating use in model
- **Session boundary discontinuities** — solved by `session_{asia,europe,us}` bucket label
- **Replay determinism** — fills stream is replay-safe; EMA state must be checkpointable

## Rollout

1. Build exporter as new service (port 9849, separate container)
2. Run in shadow mode 7 days; verify EMA convergence on `{BTCUSDT, ETHUSDT}`
3. Add features to `MLFeatureSchemaV5OF` extra_num — bump SCHEMA_HASH
4. Retrain v5_of challenger with TCA features; ablation vs no-TCA

## Effort

~5-7 days: exporter service (2d), Redis schema + replay tests (1d), integration into of_confirm_engine (1d), shadow validation (3d).

## References

- ADR-0004 ML v5_of Feature Expansion (parent)
- `python-worker/binance_execution/` (fills stream producer)
- `stream:fills:filled` schema in `core/redis_keys.py`
