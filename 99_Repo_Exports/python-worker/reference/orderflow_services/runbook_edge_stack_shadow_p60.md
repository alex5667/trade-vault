# Runbook: Edge Stack Shadow Eval (P60)

## Overview
The **Edge Stack Shadow Eval** runs nightly at 04:12 UTC. It:
1. Builds a dataset from Redis (last 24h).
2. Evaluates the current **Champion** and **Candidate** models (from `cfg:ml_confirm:edge_stack_v1:*`).
3. Calculates metrics (Brier, ECE, Precision, Expectancy).
4. Optionally promotes Candidate to Champion if guards are passed (and `EDGE_STACK_AUTO_PROMOTE_GUARDED=1`).

## Alerts

### EdgeStackShadowEvalFailed
**Meaning**: The last run of the evaluation script failed (exit code != 0 or exception).
**Action**:
1. Check `metrics:edge_stack_shadow:last` in Redis for `error` field.
2. Check logs of `scanner-of-timers-worker` around 04:12 UTC.
3. Common causes: Redis unavailable, Dataset builder failed, Model file missing.

### EdgeStackShadowEvalStale
**Meaning**: No successful update in >26 hours.
**Action**:
1. Check if `scanner-of-timers-worker` is running.
2. Verify `run_edge_stack_shadow_eval` is scheduled in `of_timers_worker.py`.

### EdgeStackChampionQualityDegraded
**Meaning**: Champion model Brier score is high (>0.25).
**Action**:
1. Investigate recent market conditions.
2. Check if a bad model was promoted.
3. Consider rolling back to a previous champion manually.

## Guarded Promotion
To enable/disable automatic promotion:
- Set `EDGE_STACK_AUTO_PROMOTE_GUARDED=1` (or `0`) in `docker-compose-timers.yml` or ENV.
- Guards:
  - Brier Rel <= 1.02
  - ECE Diff <= 0.005
  - Precision Delta >= 0.0

## Manual Run
```bash
# Enter worker container
docker exec -it scanner-python-worker-1 bash

# Run manual eval
python -m tools.edge_stack_shadow_eval_bundle_v1 --window_hours 24
```
