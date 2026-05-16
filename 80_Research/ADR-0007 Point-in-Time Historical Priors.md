---
title: "ADR-0007: Point-in-Time Historical Priors"
date: 2026-05-15
status: proposed
tags: [adr, ml, priors, pit, leakage, replay, v5_of]
component: python-worker + ml-replay + training-tooling
schema: v5_of
parent_adr: ADR-0004
---

## Context

ADR-0004 Phase P3 (Risk R1) flags **historical priors** as the highest-leakage feature group:

```
prior_winrate_symbol_kind_session
prior_winrate_symbol_kind
prior_ev_r_symbol_kind_session
prior_ev_r_symbol_kind
prior_sample_count_symbol_kind_session
```

A naive implementation that computes priors over *all* historical trades introduces **temporal look-ahead** during training — a trade at time T sees its own outcome in the aggregate.

## Decision (proposed)

**Point-in-time (PIT) materialization** with strict embargo:

1. **Materialization** — for each historical decision at `signal_ts_ms`:
   - Aggregate outcomes ONLY from `trades:closed` where `close_ts_ms < signal_ts_ms - EMBARGO_MS`
   - `EMBARGO_MS` default: 3_600_000 (1 hour) — eliminates outcome echo from recent same-symbol trades
2. **As-of join key** — `(symbol, kind, session_bucket, signal_ts_ms)`
3. **Two-phase pipeline:**
   - **Phase A (offline, replay):** `tools/build_pit_priors_v1.py` reads `trades:closed` history, emits `pit_priors:{symbol}:{kind}:{session}` Redis hash with timestamped EMA per as-of date. Sortable by `ts_ms` for replay.
   - **Phase B (runtime):** `of_confirm_engine.py` reads the hash entry where `ts_ms < signal_ts_ms - EMBARGO_MS`, picks closest entry.

## Training-side guards

1. **Leakage test in CI:** unit test that builds priors at T, generates a synthetic outcome at T+10ms, rebuilds priors at T+1s — asserts no echo (this is already covered by `test_no_leakage_pit_priors` pattern).
2. **Purged CV + embargo in training:** add `purged_kfold` with embargo to `train_ml_scorer_v3.py`. Reject any fold where train end overlaps with test start ± `EMBARGO_MS`.
3. **Sample-count guard:** `prior_sample_count < PIT_PRIOR_MIN_SAMPLES` (default 30) ⇒ feature → 0.0.

## Schema additions

```
prior_winrate_symbol_kind_session   (num)
prior_winrate_symbol_kind           (num)
prior_ev_r_symbol_kind_session      (num)
prior_ev_r_symbol_kind              (num)
prior_sample_count_symbol_kind      (num, log-scaled for model)
prior_age_ms                        (num, freshness of underlying aggregate)
prior_stale                         (bool, prior_age_ms > PIT_PRIOR_STALE_MS)
```

## Risks

- **R1: Cold start** — new symbols / new kinds have no priors; gate emits 0.0 + counter
- **R2: Replay-safe materialization correctness** — covered by leakage test
- **R3: Session bucket collision with TCA priors (ADR-0005)** — share `session_bucket` derivation logic
- **R4: Embargo too short** — measure outcome echo decay on historical data; tune empirically

## Rollout

1. Build `tools/build_pit_priors_v1.py`; backfill 90d
2. Wire into of_confirm_engine.py with Phase 7.6 block
3. Add purged CV to training script; mandatory pre-promotion gate
4. Shadow 14d on canary; verify prior_age_ms p99 < threshold
5. Retrain with priors as ablation arm

## Effort

~10-14 days: PIT materializer (3d), training-side purged CV (2d), runtime integration + leakage tests (3d), backfill + shadow validation (5d).

## References

- ADR-0004 ML v5_of Feature Expansion (parent)
- López de Prado, "Advances in Financial Machine Learning" §7 (purged k-fold + embargo)
- `python-worker/tests/core/test_no_leakage_*.py` existing patterns
