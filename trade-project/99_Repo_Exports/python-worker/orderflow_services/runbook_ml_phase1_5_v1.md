# ML Phase 1.5 — Commit Policy Controller / Commit Executor

## Scope

This phase adds a commit-policy control layer on top of Phase 1.4 executor adapters.

It introduces:
- global commit enable / kill-switch
- per-action rollout flags
- cooldown windows
- per-action hourly commit rate limit
- canary-share gate
- dedicated commit bus
- separate commit executor

## Redis config

### Global policy

Key:
`cfg:ml:commit_policy:global`

Fields:
- `commit_enabled` = `0|1`
- `kill_switch` = `0|1`
- `kill_reason` = free text
- `executor_mode` = `DRY_RUN|COMMIT`
- `default_cooldown_sec` = integer

### Per action

Key:
`cfg:ml:commit_policy:action:<action_type>`

Fields:
- `enabled`
- `cooldown_sec`
- `require_replay_pass`
- `max_commits_per_hour`
- `canary_share`
- `executor_mode`
- `high_risk_block`

## Recommended initial config

```bash
redis-cli HSET cfg:ml:commit_policy:global \
  commit_enabled 1 \
  kill_switch 0 \
  executor_mode DRY_RUN \
  default_cooldown_sec 21600

redis-cli HSET cfg:ml:commit_policy:action:propose_threshold_canary \
  enabled 1 \
  cooldown_sec 21600 \
  require_replay_pass 1 \
  max_commits_per_hour 1 \
  canary_share 0.25 \
  executor_mode DRY_RUN \
  high_risk_block 1
```

## Rollout

1. Apply SQL
2. Start `scanner-ml-commit-policy-controller-v1`
3. Start `scanner-ml-recommendation-commit-executor-v1`
4. Keep global `executor_mode=DRY_RUN`
5. Observe audit / results streams
6. Enable `COMMIT` only for one low-risk action after replay-confirmed path

## Smoke checks

```bash
redis-cli XREVRANGE stream:ml:commit_policy_results + - COUNT 5
redis-cli XREVRANGE stream:ml:recommendation_commit_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:recommendation_apply_results + - COUNT 5
redis-cli XREVRANGE stream:ml:recommendation_audit + - COUNT 10
```

## Emergency stop

```bash
redis-cli HSET cfg:ml:commit_policy:global kill_switch 1 kill_reason emergency_stop
```

## Rollback

```bash
docker compose stop scanner-ml-commit-policy-controller-v1
docker compose stop scanner-ml-recommendation-commit-executor-v1
```

## Notes

- hot path is not touched
- default path is still `DRY_RUN`
- Phase 1.5 is still bounded and reversible

