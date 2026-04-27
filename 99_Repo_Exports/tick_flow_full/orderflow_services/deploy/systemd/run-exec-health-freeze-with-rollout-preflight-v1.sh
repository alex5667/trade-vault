#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${EXEC_HEALTH_REPO_ROOT:-/opt/scanner_infra}"
PYTHON_BIN="${EXEC_HEALTH_PYTHON_BIN:-python3}"
PURPOSE="${EXEC_HEALTH_ROLLOUT_PREFLIGHT_PURPOSE:-}"

if [[ -z "${PURPOSE}" ]]; then
  echo "EXEC_HEALTH_ROLLOUT_PREFLIGHT_PURPOSE is required" >&2
  exit 64
fi
if [[ $# -lt 1 ]]; then
  echo "usage: run-exec-health-freeze-with-rollout-preflight-v1.sh <command> [args...]" >&2
  exit 64
fi

cd "${REPO_ROOT}"
"${PYTHON_BIN}" -m orderflow_services.exec_health_freeze_rollout_preflight_v1 --purpose "${PURPOSE}" --quiet
exec "$@"
