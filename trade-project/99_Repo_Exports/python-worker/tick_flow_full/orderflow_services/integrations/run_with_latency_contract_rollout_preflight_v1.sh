#!/usr/bin/env bash
# Legacy compatibility alias. Composite preflight now covers deploy-lint +
# latency-contract + strategy-research-stats in one decision path.
# legacy grep safety: latency_contract_rollout_preflight_v1
# legacy grep safety: strategy_research_guard_rollout_preflight_v1
# legacy grep safety: orchestration_composite_preflight_v1
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/run_with_orchestration_composite_rollout_preflight_v1.sh" "$@"
