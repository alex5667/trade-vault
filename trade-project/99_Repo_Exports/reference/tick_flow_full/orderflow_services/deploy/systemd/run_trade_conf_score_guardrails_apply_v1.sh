#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="${TRADE_REPO_ROOT:?TRADE_REPO_ROOT is required}"
export LATENCY_CONTRACT_DEPLOY_WRAPPER_FILE="$REPO_ROOT/python-worker/orderflow_services/deploy/systemd/run_trade_conf_score_guardrails_apply_v1.sh"
export LATENCY_CONTRACT_DEPLOY_UNIT_FILE="$REPO_ROOT/python-worker/orderflow_services/deploy/systemd/trade-conf-score-guardrails-apply.service"
exec "$REPO_ROOT/python-worker/orderflow_services/deploy/systemd/run_trade_orchestration_composite_gated_compose_job_v1.sh" \
  "$REPO_ROOT/python-worker/orderflow_services/deploy/compose/docker-compose.conf-score-guardrails-apply-v1.yml" \
  "conf-score-guardrails-apply" \
  "conf_score_guardrails_apply" \
  "$@"
