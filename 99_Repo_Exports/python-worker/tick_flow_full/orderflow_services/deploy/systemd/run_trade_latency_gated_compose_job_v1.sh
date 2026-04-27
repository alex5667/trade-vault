#!/usr/bin/env bash
# Legacy compatibility alias kept for existing timers/systemd units.
# legacy grep safety: latency_contract_deploy_lint_v1
# legacy grep safety: orchestration_composite_preflight_v1
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/run_trade_orchestration_composite_gated_compose_job_v1.sh" "$@"
