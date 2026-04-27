#!/usr/bin/env bash
set -euo pipefail
cd "${TRADE_REPO_ROOT:?}"
exec python3 -m orderflow_services.tca_nightly_report_exporter_v1
