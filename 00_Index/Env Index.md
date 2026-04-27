---
type: index
title: Env Index
tags: [index, env, config]
updated_at: 2026-04-18
---

# Env Index

## Time / data quality
- `TICK_TIME_MAX_PAST_MS`
- `TICK_TIME_MAX_FUTURE_MS`
- `TICK_TIME_MAX_REORDER_MS`
- `BAD_TIME_TRIGGER_STREAK`
- `BAD_TIME_STATE_FREEZE_MS`
- `BAD_TIME_RECOVERY_OK_STREAK`
- `TICK_DEDUPE_ENABLE`
- `TICK_DEDUP_WINDOW`

## ML / scoring / rollout
- `MIN_SIGNAL_CONFIDENCE`
- `ML_CONFIRM_MODE`
- `ML_CONFIRM_P_MIN`
- `ML_CONFIRM_P_MIN_HARD_FLOOR`
- `ML_CONFIRM_ABSTAIN_BAND`
- `ML_CONFIRM_CFG_KEY`
- `ML_FEATURE_SCHEMA_VER`
- `CONF_CAL_AB_MODE`
- `CONF_CAL_AB_SHARE`

## Gates / execution risk
- `FEATURE_DRIFT_Z_THRESHOLD`
- `SMT_LEADER_CONF_MIN_SCORE`
- `TAKER_FEE_BPS`
- `SIGNAL_MAX_SPREAD_BPS`
- `MIN_SIGNAL_INTERVAL_SEC`

## Execution / bridge
- `CRYPTO_PAPER_SHADOW_ENABLED`
- `TRAILING_TP1_OFFSET_ATR`
- `DEFAULT_SL_ATR_MULT`
- `SLQ_WINDOW`
- `SLQ_MULT_MIN`
- `SLQ_MULT_MAX`

## Redis / ops
- `REDIS_URL`
- `REDIS_POOL_SIZE`
- `REDIS_MIN_IDLE_CONNS`
- `REDIS_MAX_RETRIES`
- `REDIS_RETRY_MIN_BACKOFF`
- `REDIS_RETRY_MAX_BACKOFF`

## Usage rule
For every env:
- default value
- owner
- affected services
- safe change window
- rollback value / rollback procedure
