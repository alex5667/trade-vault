#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${EXEC_HEALTH_REPO_ROOT:-/opt/scanner_infra}"
COMPOSE_FILE="${EXEC_HEALTH_ACL_POLICY_APPLY_COMPOSE_FILE:-${REPO_ROOT}/orderflow_services/deploy/docker-compose.exec-health-freeze-acl-policy-apply-v1.yml}"
SERVICE_NAME="${EXEC_HEALTH_ACL_POLICY_APPLY_SERVICE_NAME:-exec-health-freeze-acl-policy-apply}"
export EXEC_HEALTH_ROLLOUT_PREFLIGHT_PURPOSE="exec_health_freeze_acl_policy_apply"

exec "${REPO_ROOT}/orderflow_services/deploy/systemd/run-exec-health-freeze-with-rollout-preflight-v1.sh" \
  docker compose -f "${COMPOSE_FILE}" run --rm "${SERVICE_NAME}" "$@"
