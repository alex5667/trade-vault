# Latency Contract P4.4 deploy lint

This lint runs **before** latency rollout preflight and validates:

- correct compose file for the sensitive purpose
- correct host-side wrapper binding
- presence of `EnvironmentFile=` in the systemd unit
- required runtime env for the chosen job

## Manual usage

```bash
python3 -m orderflow_services.latency_contract_deploy_lint_v1 \
  --purpose conf_score_guardrails_apply \
  --repo-root "$TRADE_REPO_ROOT" \
  --compose-file "$TRADE_REPO_ROOT/orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-apply-v1.yml" \
  --wrapper-file "$TRADE_REPO_ROOT/orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_apply_v1.sh" \
  --unit-file "$TRADE_REPO_ROOT/orderflow_services/deploy/systemd/trade-conf-score-guardrails-apply.service" \
  --env-file /etc/default/trade-latency-sensitive-jobs-staging
```

Exit code `26` means deploy lint failed.

## Automatic path

Sensitive host-side wrappers now call:

1. `latency_contract_deploy_lint_v1`
2. `latency_contract_rollout_preflight_v1`
3. `docker compose run --rm ...`

So rollout can be blocked before the container starts when file binding or env is broken.
