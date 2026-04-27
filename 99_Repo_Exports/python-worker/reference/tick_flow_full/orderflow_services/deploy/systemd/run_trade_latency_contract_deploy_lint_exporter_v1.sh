#!/usr/bin/env bash
set -euo pipefail
cd "${TRADE_REPO_ROOT:?}"
exec python3 -m orderflow_services.latency_contract_deploy_lint_exporter_v1
