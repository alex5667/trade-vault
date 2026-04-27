#!/usr/bin/env bash
# Unified compose job wrapper — runs deploy-lint + composite orchestration preflight
# (includes strategy_research_stats) before exec'ing the docker compose service.
# Replaces the legacy run_trade_latency_gated_compose_job_v1.sh +
# run_trade_strategy_research_stats_gated_compose_job_v1.sh two-step chain (P6.3).
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "usage: $0 <compose-file> <service> <purpose> [compose-run-args...]" >&2
  exit 64
fi

COMPOSE_FILE="$1"
SERVICE_NAME="$2"
PURPOSE="$3"
shift 3

REPO_ROOT="${TRADE_REPO_ROOT:?TRADE_REPO_ROOT is required}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DOCKER_BIN="${DOCKER_BIN:-docker}"

cd "$REPO_ROOT"
export PYTHONPATH="$REPO_ROOT${PYTHONPATH:+:$PYTHONPATH}"
WRAPPER_FILE="${LATENCY_CONTRACT_DEPLOY_WRAPPER_FILE:-}"
UNIT_FILE="${LATENCY_CONTRACT_DEPLOY_UNIT_FILE:-}"
LINT_REPORT_PATH="${LATENCY_CONTRACT_DEPLOY_LINT_REPORT_PATH:-}"
LINT_ENV_FILE="${LATENCY_CONTRACT_ENV_FILE:-}"
"$PYTHON_BIN" -m orderflow_services.latency_contract_deploy_lint_v1 \
  --purpose "$PURPOSE" \
  --repo-root "$REPO_ROOT" \
  --compose-file "$COMPOSE_FILE" \
  ${WRAPPER_FILE:+--wrapper-file "$WRAPPER_FILE"} \
  ${UNIT_FILE:+--unit-file "$UNIT_FILE"} \
  ${LINT_ENV_FILE:+--env-file "$LINT_ENV_FILE"} \
  ${LINT_REPORT_PATH:+--json-out "$LINT_REPORT_PATH"}
"$PYTHON_BIN" -m orderflow_services.orchestration_composite_preflight_v1 --purpose "$PURPOSE" --stage-mode "${CONF_SCORE_GUARD_STAGE:-0}"
exec "$DOCKER_BIN" compose -f "$COMPOSE_FILE" run --rm "$SERVICE_NAME" "$@"
