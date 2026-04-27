#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${TRADE_REPO_ROOT:?TRADE_REPO_ROOT is required}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
OFC_RUNTIME_COMMAND="${OFC_RUNTIME_COMMAND:?OFC_RUNTIME_COMMAND is required}"

cd "$REPO_ROOT"
export PYTHONPATH="$REPO_ROOT${PYTHONPATH:+:$PYTHONPATH}"

exec "$PYTHON_BIN" -m orderflow_services.ofc_contextual_runtime_reloader_v1 \
  --overlay-env-file "${OFC_CTX_RUNTIME_OVERLAY_ENV_FILE:-/var/lib/trade/ofc_contextual_rollout.env}" \
  --rollback-flag-path "${OFC_CTX_ROLLBACK_FLAG_PATH:-/var/lib/trade/ofc_contextual.rollback}" \
  --poll-sec "${OFC_CTX_RUNTIME_POLL_SEC:-5}" \
  --cooldown-sec "${OFC_CTX_RUNTIME_RESTART_COOLDOWN_SEC:-15}" \
  --grace-sec "${OFC_CTX_RUNTIME_TERM_GRACE_SEC:-20}" \
  --state-path "${OFC_CTX_RUNTIME_RELOADER_STATE_PATH:-/var/lib/trade/ofc_contextual_runtime_reloader_state.json}" \
  -- bash -lc "$OFC_RUNTIME_COMMAND"
