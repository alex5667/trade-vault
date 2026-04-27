#!/usr/bin/env bash
set -euo pipefail

# E2E test: inject a synthetic alert into Alertmanager to validate
# the full chain: Alertmanager → webhook → Telegram.
#
# Usage:
#   ALERTMANAGER_URL=http://127.0.0.1:9093 ./scripts/send_test_alert_to_alertmanager.sh
#
# Optional env:
#   SEVERITY=critical|warning   (default: critical)
#   ALERTNAME=EdgeStackTestAlert

ALERTMANAGER_URL="${ALERTMANAGER_URL:-http://127.0.0.1:9093}"
SEVERITY="${SEVERITY:-critical}"
ALERTNAME="${ALERTNAME:-EdgeStackTestAlert}"
TEAM="${TEAM:-trade}"
COMPONENT="${COMPONENT:-edge_stack}"

NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

payload=$(cat <<JSON
[
  {
    "labels": {
      "alertname": "${ALERTNAME}",
      "severity": "${SEVERITY}",
      "team": "${TEAM}",
      "component": "${COMPONENT}",
      "job": "manual_test",
      "instance": "local"
    },
    "annotations": {
      "summary": "Manual test alert injected via Alertmanager API",
      "description": "If you see this in Telegram, Alertmanager->webhook->Telegram is working.",
      "runbook_path": "/edge_stack_train_p59.md",
      "dashboard_path": "/d/edge_stack_overview/edge-stack-overview?orgId=1"
    },
    "startsAt": "${NOW}"
  }
]
JSON
)

echo "[test] posting ${SEVERITY} alert '${ALERTNAME}' to ${ALERTMANAGER_URL}/api/v2/alerts"
curl -sS -XPOST "${ALERTMANAGER_URL}/api/v2/alerts" \
  -H 'Content-Type: application/json' \
  -d "${payload}"

echo
echo "[test] OK — check Alertmanager UI (${ALERTMANAGER_URL}) + webhook logs + Telegram"
echo "        docker logs alertmanager-telegram-webhook"
