#!/usr/bin/env bash
# Legacy compatibility alias. Strategy research stats is now part of the
# orchestration composite preflight.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/run_trade_orchestration_composite_gated_compose_job_v1.sh" "$@"
