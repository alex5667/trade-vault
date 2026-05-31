# P4.7 — latency-contract deploy-lint ack/silence workflow

Gate remains active; only notifier noise is suppressed per `purpose` with `operator` / `ticket` / `reason` audit.

## CLI
```bash
./orderflow_services/deploy/systemd/run_trade_latency_contract_deploy_lint_silence_v1.sh status
./orderflow_services/deploy/systemd/run_trade_latency_contract_deploy_lint_silence_v1.sh ack \
  --purpose meta_cov_rollout_controller --operator alex --ticket INC-42 \
  --reason "known config drift during staged rollout" --minutes 360
./orderflow_services/deploy/systemd/run_trade_latency_contract_deploy_lint_silence_v1.sh unsilence \
  --purpose meta_cov_rollout_controller --operator alex --ticket INC-42 \
  --reason "drift fixed and rollout rechecked"
```
