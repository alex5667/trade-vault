---
title: "ADR-0006: Cross-Symbol + Liq/OI Feature Pipeline"
date: 2026-05-15
status: proposed
tags: [adr, ml, cross-symbol, btc-anchor, liquidations, oi, funding, v5_of]
component: python-worker + new-service
schema: v5_of
parent_adr: ADR-0004
---

## Context

ADR-0004 Phase P2 plans 18 features across two related groups:

**Cross-symbol returns (BTC/ETH anchors):**
- `btc_ret_{30s,1m,5m}`, `eth_ret_{30s,1m,5m}`, `rel_ret_{1m,5m}_vs_btc`
- `leader_direction_conflict`, `sector_breadth_{1m,5m}`

**Liquidation / OI / funding:**
- `liq_{long,short}_notional_{1m,5m}`, `liq_imbalance_{1m,5m}`
- `oi_delta_{1m,5m}`, `oi_delta_z`
- `funding_rate_z`, `premium_index_z`, `basis_pressure_score`

Both groups share infrastructure needs: rolling window aggregation per anchor symbol + careful `book_age_ms` lag-guard to avoid stale-cross-symbol leakage.

## Decision (proposed)

Single **cross-context-aggregator** service that:

1. Subscribes to:
   - `stream:tick_BTCUSDT`, `stream:tick_ETHUSDT` (anchor returns)
   - `stream:liq_evt` (liquidations)
   - `stream:oi_*`, `stream:funding_*` (OI/funding from existing Binance handlers)
2. Computes per-window rolling stats (deque-based, no SQL); persists in Redis:
   ```
   ctx:anchor:btc:returns         { ret_30s, ret_1m, ret_5m, ts_ms }
   ctx:anchor:eth:returns         { ret_30s, ret_1m, ret_5m, ts_ms }
   ctx:liq:{symbol}:imb           { long_n_1m, short_n_1m, ..._5m, ts_ms }
   ctx:oi:{symbol}:delta          { d_1m, d_5m, z, ts_ms }
   ctx:funding:{symbol}           { rate, z, premium_bps, ts_ms }
   ```
3. `of_confirm_engine.py` hydrates `indicators_with_v4` from these hashes via **one pipelined HMGET per signal** (~5 keys, ~1ms additional p99).

## Lag guard

Each hash includes `ts_ms`. Phase 7-style guard:
```python
if (now_ms - anchor_ts_ms) > CROSS_CTX_MAX_LAG_MS:
    # feature → 0.0 + zero-rate counter
```
Default `CROSS_CTX_MAX_LAG_MS=2000` (2s) — looser than tick-lag because anchor symbols are slower-moving and replication delay is acceptable.

## Risks

- **R1: Cross-symbol look-ahead** — anchor tick must be ≤ target signal_ts_ms. Enforced by ts_ms comparison.
- **R2: Funding rate update sparsity** — funding updates every 8h; emit `funding_age_ms` for model staleness handling.
- **R3: OI delta noise** — use bounded robust z-score (winsorize ±5σ) per ADR-0004 §R-stats.
- **R4: Sector breadth needs symbol→sector map** — keep mapping in `var/symbol_sectors.json`, hot-reloaded.

## Rollout

1. Build aggregator service (port TBD, separate container)
2. Shadow on 5 canary symbols 7d; verify lag p99 < 2s
3. Add features to v5_of schema in blocks (cross-symbol first, OI/funding second); bump SCHEMA_HASH per block
4. Retrain v5_of challenger; ablation per block

## Effort

~7-10 days: aggregator service (3d), liq/OI/funding integration (2d), schema additions + tests (1d), shadow validation (4d).

## References

- ADR-0004 ML v5_of Feature Expansion (parent)
- `go-worker/internal/liquidation/` (liq_evt source)
- Binance funding endpoint integration in `python-worker/services/`
