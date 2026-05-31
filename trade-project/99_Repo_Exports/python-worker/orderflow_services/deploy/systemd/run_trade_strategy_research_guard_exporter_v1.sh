#!/usr/bin/env bash
set -euo pipefail
cd "${TRADE_REPO_ROOT:?}"
exec python3 -m orderflow_services.strategy_research_guard_state_exporter_v1
