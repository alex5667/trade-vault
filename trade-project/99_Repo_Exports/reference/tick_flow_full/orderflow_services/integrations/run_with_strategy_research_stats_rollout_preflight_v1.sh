#!/usr/bin/env bash
# Legacy compatibility alias. Strategy research stats is now evaluated inside
# orchestration_composite_preflight_v1 together with deploy-lint and
# latency-contract.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/run_with_orchestration_composite_rollout_preflight_v1.sh" "$@"
