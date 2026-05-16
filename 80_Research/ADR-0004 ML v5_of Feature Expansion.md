---
title: "ADR-0004: ML v5_of Feature Expansion"
date: 2026-05-15
status: in-progress
tags: [adr, ml, features, v5_of, lgbm, triple-barrier, orderflow]
component: python-worker
schema: v5_of
baseline_schema: v13_of
---

## Context

- **Project:** `scanner_infra/python-worker`
- **Current prod schema:** `v13_of` (ENV `ML_FEATURE_SCHEMA_VER=v13_of` в `docker-compose-crypto-orderflow.yml`)
- **Target schema class:** `MLFeatureSchemaV5OF` в `core/ml_feature_schema_v5_of.py`
- **Model type:** LightGBM edge-stack classifier, метки — Triple-Barrier (path-based, v10)
- **ML Confirm gate:** `of_confirm_engine.py` + `ml_scoring_gate.py`
- **P0 выполнено:** schema selection fix + ENV обновлён до `ML_FEATURE_SCHEMA_VER=v5_of`

## Status

| Phase | State |
|---|---|
| P0 — Schema selection fix + ENV | DONE |
| P1 — exec cost + signal age + vol dynamics + DQ (Phase 7) | DONE (2026-05-15) |
| P1.2 — Extended DQ (book_age_ms, book_gap_ms, cvd_quarantine_active) | DONE (2026-05-15) |
| P1.3 — ATR freshness (atr_fresh bool, ATR_FRESH_MS env) | DONE (2026-05-15) |
| P1.4 — Gate trace (rule_have_need_gap, missing_legs_count, gate_pressure_score, soft_fail_near_pass) | DONE (2026-05-15) |
| P1.5 — Session/weekend flags (session_{asia,europe,us}, weekend_flag) | DONE (2026-05-15) |
| P1 — TCA EMA priors | PLANNED (requires separate Redis time-series infra — see ADR-0005) |
| P2 — Cross-symbol / macro features | PLANNED (requires BTC/ETH anchor feed — see ADR-0006) |
| P2.1 — LOB velocity (rolling 1s/3s windows) | PLANNED (per-symbol rolling state in runtime) |
| P2.2 — Fill-queue features (eta_fill_sec, queue_ahead_qty) | PLANNED (LOB depth snapshots) |
| P3 — VPIN/Hawkes (denylist only) | PLANNED |
| P3 — Historical priors (PIT pipeline) | PLANNED (replay-safe materialization — see ADR-0007) |
| Section 6 telemetry | DONE (2026-05-15): ml_p_edge_bucket, ml_abstain_total, ml_feature_schema_version_total, ml_feature_vector_size_mismatch_total |
| Section 8 prod-checklist | DONE (2026-05-15): n_features_in_ runtime check + train-time guard; feature_cols_hash already saved |

## Decision

Расширить схему `v5_of` четырьмя блоками фич: execution-aware, horizon-aware, DQ-aware, cross-symbol. Порядок приоритетов: сначала execution cost (наибольший ROI), затем cross-symbol breadth, VPIN/Hawkes только через E-block denylist с ablation.

**Ключевые решения:**

1. Начинаем с execution-aware фич — они дают прямой сигнал о feasibility сделки.
2. VPIN/Hawkes никогда не хардкодим как обязательные — только denylist, только после ablation.
3. Historical priors (`prior_winrate_*`, `prior_ev_r_*`) требуют отдельного point-in-time пайплайна.
4. Cyclical encoding (`sin`/`cos`) для `hour_of_day` и `day_of_week` вместо one-hot.

## Feature Groups

### P1 Phase 7 — Execution-aware + Horizon + DQ ✅ DONE (2026-05-15)

**Execution cost ratios** — вычислены в `of_confirm_engine.py` Phase 7 блок:
- `exec_cost_to_tp1_ratio` = `(half_spread + slippage + fee) / tp1_bps`
  - fallback chain: `liqmap_gate_reward_bps → tp1_bps → pred_tp1_bps → 0.0`
- `exec_cost_to_sl_ratio` = `exec_cost / sl_bps`
  - fallback chain: `liqmap_gate_risk_bps → sl_bps → atr_bps * SL_ATR_MULT(env, def=1.0)`
  - **ATR-derived fallback добавлен**: ненулевой ratio всегда когда ATR доступен
- `exec_cost_to_atr_ratio` = `exec_cost / atr_bps`

**Signal age / horizon:**
- `signal_age_ms` = `now_ms - signal_ts_ms`
- `signal_age_to_half_life` = `signal_age_ms / alpha_half_life_ms` (0.0 если HL неизвестен)
- `vol_expansion_score` = `max(0, vol_ratio_fast_slow - 1)`
- `vol_compression_score` = `max(0, 1 - vol_ratio_fast_slow)`

**Data quality freshness:**
- `dq_score` = alias `dq_health_score` (0..1), дефолт 1.0
- `dq_flag_count` = bucket by health: 0(≥0.9) / 1(≥0.7) / 2(≥0.5) / 3(<0.5)
- `tick_lag_ms` = `tick_gap_ms OR book_ts_gap_ms`, дефолт 0.0

**Observability:**
- Prometheus counter `ml_p7_feature_zero_total{feature, symbol}` в `services/orderflow/metrics.py`
- Tracked: `exec_cost_to_tp1_ratio`, `exec_cost_to_sl_ratio`, `exec_cost_to_atr_ratio`, `signal_age_to_half_life`, `tick_lag_ms`
- Grafana: `rate(ml_p7_feature_zero_total[10m])` → alert если > 30% по символу

### P1 TCA priors — PLANNED

**TCA priors (EMA-smoothed):**
- `tca_eff_spread_bps_ema`, `tca_realized_spread_{1s,5s}_bps_ema`
- `tca_perm_impact_{1s,5s}_bps_ema`, `tca_is_bps_ema`
- Требует: отдельный TCA Redis time-series pipeline (EMA за 1s/5s окна)
- Guard: `tca_samples > threshold`; stale при `tca_stale_ms > limit` → NaN

### P2 — Cross-symbol / Macro (MEDIUM ROI)

**BTC/ETH relative returns:**
- `btc_ret_30s`, `btc_ret_1m`, `btc_ret_5m`
- `eth_ret_30s`, `eth_ret_1m`, `eth_ret_5m`
- `rel_ret_1m_vs_btc`, `rel_ret_5m_vs_btc`

**Liquidations / OI / Funding:**
- `liq_long_notional_1m`, `liq_short_notional_1m`, `liq_long_notional_5m`, `liq_short_notional_5m`
- `liq_imbalance_1m`, `liq_imbalance_5m`
- `oi_delta_1m`, `oi_delta_5m`, `oi_delta_z`
- `funding_rate`, `funding_rate_z`
- `premium_index_bps`, `premium_index_z`, `basis_pressure_score`

### P2 — Cross-symbol / Macro (MEDIUM ROI)

**BTC/ETH relative returns:**
- `btc_ret_30s`, `btc_ret_1m`, `btc_ret_5m`
- `eth_ret_30s`, `eth_ret_1m`, `eth_ret_5m`
- `rel_ret_1m_vs_btc`, `rel_ret_5m_vs_btc`

**Liquidations / OI / Funding:**
- `liq_long_notional_1m`, `liq_short_notional_1m`, `liq_long_notional_5m`, `liq_short_notional_5m`
- `liq_imbalance_1m`, `liq_imbalance_5m`
- `oi_delta_1m`, `oi_delta_5m`, `oi_delta_z`
- `funding_rate`, `funding_rate_z`
- `premium_index_bps`, `premium_index_z`, `basis_pressure_score`

### P3 — VPIN / Hawkes (denylist-only, ablation required)

> Только через E-block denylist. Никогда не добавлять как hard-required фичи.

- `vpin_tox_1m`, `vpin_tox_5m`, `vpin_tox_z`, `vpin_tox_slope`
- `hawkes_taker_buy_lam`, `hawkes_taker_sell_lam`
- `hawkes_cancel_bid_lam`, `hawkes_cancel_ask_lam`

## Risks

| # | Риск | Mitigation |
|---|---|---|
| R1 | **Historical priors** (`prior_winrate_*`, `prior_ev_r_*`) — look-ahead leakage | Point-in-time as-of `signal_ts_ms`; purged CV + embargo обязателен |
| R2 | **VPIN/Hawkes** — potential look-ahead в bucket assignment | E-block denylist + ablation до промоушена; никогда не hard-coded |
| R3 | **TCA priors** — ненадёжны при малом sample count | `tca_samples > threshold` guard; stale при `tca_stale_ms` > limit → NaN |
| R4 | **Cross-symbol lag** — BTC/ETH тик старше целевого | Проверять `book_age_ms` для anchor-символов; feature-level staleness flag |
| R5 | **Missing rate** выше 5% в shadow | Gate: block P2/P3 промоушен если `missing_rate > 0.05` |

## Rollout

```
Phase 0  ML_FEATURE_SCHEMA_VER=v5_of + ML_CONFIRM_MODE=SHADOW     [DONE]
Phase 1  Shadow only — missing_rate < 5%, latency p99 stable
Phase 2  Обучить challenger (v5_of) vs champion (v13_of baseline)
Phase 3  Canary: 5% enforce share; monitor ECE + Precision@Top5%
Phase 4  Promote if: EV/R ↑, ECE not worse, Precision@Top5% ↑
```

**Rollback trigger:** missing_rate > 10% ИЛИ latency p99 > бюджет ИЛИ ECE ухудшился.

## Consequences

**Positive:**
- Execution-aware фичи напрямую улучшают feasibility-оценку на entry
- DQ фичи позволяют модели self-report на stale данных
- Cross-symbol breadth улучшает режимную чувствительность

**Negative / Trade-offs:**
- TCA prior pipeline — отдельная инфраструктура (Redis time-series или Timescale)
- Point-in-time priors требуют replay-safe materialization
- Больше фич → дольше feature computation на hot path (измерить p99 до P3)

## Checklist

- [x] P0: Schema selection fix + `ML_FEATURE_SCHEMA_VER=v5_of` в docker-compose
- [x] P1: Добавить exec cost ratios в `MLFeatureSchemaV5OF` (Phase 7, 10 фич)
- [x] P1: ATR-derived fallback для `exec_cost_to_sl_ratio` (`SL_ATR_MULT` env)
- [x] P1: Добавить `signal_age_ms`, `signal_age_to_half_life`, `vol_expansion/compression_score`
- [x] P1: Добавить DQ freshness fields (`tick_lag_ms`, `dq_score`, `dq_flag_count`)
- [x] P1: Prometheus counter `ml_p7_feature_zero_total{feature, symbol}` для missing rate
- [x] P1: Unit тесты (25 total) — formula + schema boundary + fallback tests
- [ ] P1: TCA EMA fields + staleness guard (отдельный pipeline, PLANNED)
- [ ] P1: Собрать shadow dataset 24h, проверить missing_rate < 5% по Grafana
- [ ] P2: Обучить challenger (v5_of features) vs champion (v13_of baseline)
- [ ] P2: Добавить BTC/ETH anchor returns (с `book_age_ms` lag-guard)
- [ ] P2: Добавить liq/OI/funding features
- [ ] P2: Shadow dataset validation (missing < 5%)
- [ ] P3: Реализовать VPIN/Hawkes только через E-block denylist
- [ ] P3: Ablation study до любого промоушена
- [ ] Rollout: Canary at 5% enforce share с мониторингом ECE + Precision@Top5%

## References

- `python-worker/core/ml_feature_schema_v5_of.py` — schema class
- `python-worker/handlers/crypto_orderflow/of_confirm_engine.py` — ML gate
- `python-worker/handlers/crypto_orderflow/ml_scoring_gate.py` — scoring gate
- `python-worker/core/triple_barrier.py` — label generation
- ADR-0003: Shadow before Enforce for ML Gate
- [[ADR Index]]
