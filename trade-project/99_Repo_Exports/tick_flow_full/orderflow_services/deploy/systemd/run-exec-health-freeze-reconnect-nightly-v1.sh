#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${EXEC_HEALTH_REPO_ROOT:-/opt/scanner_infra}"
COMPOSE_FILE="${EXEC_HEALTH_RECONNECT_SMOKE_COMPOSE_FILE:-${REPO_ROOT}/orderflow_services/deploy/docker-compose.exec-health-freeze-reconnect-nightly-v1.yml}"
SERVICE_NAME="${EXEC_HEALTH_RECONNECT_SMOKE_SERVICE_NAME:-exec-health-freeze-reconnect-nightly}"
REPORT_DIR="${EXEC_HEALTH_RECONNECT_SMOKE_REPORT_DIR:-/var/lib/trade/exec_health_reconnect_smoke}"
TEXTFILE_DIR="${EXEC_HEALTH_RECONNECT_SMOKE_TEXTFILE_DIR:-/var/lib/node_exporter/textfile_collector}"

mkdir -p "${REPORT_DIR}" "${TEXTFILE_DIR}"
cd "${REPO_ROOT}"
exec docker compose -f "${COMPOSE_FILE}" run --rm "${SERVICE_NAME}"
