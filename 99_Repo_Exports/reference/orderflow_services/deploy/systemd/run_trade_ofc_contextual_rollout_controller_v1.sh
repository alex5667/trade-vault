#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="${TRADE_REPO_ROOT:?TRADE_REPO_ROOT is required}"
export LATENCY_CONTRACT_DEPLOY_WRAPPER_FILE="$REPO_ROOT/python-worker/orderflow_services/deploy/systemd/run_trade_ofc_contextual_rollout_controller_v1.sh"
export LATENCY_CONTRACT_DEPLOY_UNIT_FILE="$REPO_ROOT/python-worker/orderflow_services/deploy/systemd/trade-ofc-contextual-rollout-controller.service"
exec "$REPO_ROOT/python-worker/orderflow_services/deploy/systemd/run_trade_latency_gated_compose_job_v1.sh" \
  "$REPO_ROOT/python-worker/orderflow_services/deploy/compose/docker-compose.ofc-contextual-rollout-controller-v1.yml" \
  "ofc-contextual-rollout-controller" \
  "ofc_contextual_rollout_controller" \
  "$@"
