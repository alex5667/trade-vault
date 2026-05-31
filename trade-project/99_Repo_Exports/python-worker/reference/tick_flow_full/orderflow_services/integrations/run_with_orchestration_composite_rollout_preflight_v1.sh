#!/usr/bin/env bash
# Unified composite orchestration preflight wrapper (P6.3).
#
# Usage: run_with_orchestration_composite_rollout_preflight_v1.sh <command...>
#
# Runs composite orchestration preflight (deploy-lint + latency-contract +
# strategy_research_stats) before executing the given command.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <command...>" >&2
  exit 64
fi

PURPOSE="${LATENCY_CONTRACT_PREFLIGHT_PURPOSE:-latency_contract_sensitive_apply}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

"$PYTHON_BIN" -m orderflow_services.orchestration_composite_preflight_v1 --purpose "$PURPOSE" --stage-mode "${CONF_SCORE_GUARD_STAGE:-0}"
exec "$@"
