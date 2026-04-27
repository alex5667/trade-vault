#!/usr/bin/env bash
set -euo pipefail
cd "${TRADE_REPO_ROOT:?}"
exec python3 -m orderflow_services.orchestration_composite_preflight_exporter_v1
