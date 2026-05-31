#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="${TRADE_REPO_ROOT:?TRADE_REPO_ROOT is required}"
export LATENCY_CONTRACT_DEPLOY_WRAPPER_FILE="$REPO_ROOT/python-worker/orderflow_services/deploy/systemd/run_trade_ofc_contextual_exporter_v1.sh"
exec docker compose \
  --env-file "${LATENCY_CONTRACT_ENV_FILE:-/etc/default/trade-latency-sensitive-jobs-staging}" \
  -f "$REPO_ROOT/python-worker/orderflow_services/deploy/compose/docker-compose.ofc-contextual-exporter-v1.yml" \
  up --remove-orphans ofc-contextual-exporter
