# P4.3 Latency Contract Operational Wiring

This layer binds the P4.2 rollout gate into concrete staging/prod apply/rollout jobs.

Sensitive jobs covered:
- `conf_score_guardrails_apply_v1`
- `conf_score_guardrails_promote_v1`
- `meta_cov_rollout_controller_v1`
- `conf_score_guardrails_autopromo_controller_v1`

## Guarantees
- host-side preflight runs before `docker compose run`
- in-container preflight runs again before Python entrypoint
- `LATENCY_CONTRACT_PREFLIGHT_PURPOSE` is fixed per job
- jobs fail fast with rc=24 when rollout gate is active

## Host-side wrappers
Located in `orderflow_services/deploy/systemd/`.

## Compose jobs
Located in `orderflow_services/deploy/compose/`.

## Recommended install
1. Copy one of the example env files to `/etc/default/...`.
2. Adjust `TRADE_REPO_ROOT`, `TRADE_ORDERFLOW_IMAGE`, `REDIS_URL`, bundle/state paths.
3. Install the `.service` and `.timer` units.
4. Enable timers only for recurring jobs; keep apply/promote as manual oneshot units.

## Smoke checks
```bash
systemctl cat trade-meta-cov-rollout-controller.service
systemctl start trade-meta-cov-rollout-controller.service
systemctl start trade-conf-score-guardrails-promote.service
```

Expected when blocked:
- host-side wrapper exits before compose starts
- stderr/stdout contains `LATENCY_CONTRACT_PREFLIGHT_BLOCK`
- no sensitive container is started
