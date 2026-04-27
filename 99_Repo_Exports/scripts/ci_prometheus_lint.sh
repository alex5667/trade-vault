#!/usr/bin/env bash
set -euo pipefail

# CI lint: Prometheus alert rules + docker compose config.
#
# Checks performed:
#   1. docker compose config -q  for the main compose files
#   2. promtool check rules       for all prometheus_alerts_*.yml files
#
# Requirements:
#   - docker (v20+)
#   - docker compose (v2) — `docker compose` subcommand
#
# Promtool is executed inside prom/prometheus Docker image so no local
# installation of promtool is needed.
#
# Usage:
#   chmod +x scripts/ci_prometheus_lint.sh
#   ./scripts/ci_prometheus_lint.sh
#
# In CI (GitHub Actions, GitLab CI, etc.) add a step:
#   - run: ./scripts/ci_prometheus_lint.sh

# Resolve repo root regardless of where script is called from
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PASS=0
FAIL=0

# ── 1. docker compose config validation ──────────────────────────────────────
# We validate both the main crypto-orderflow compose and the timers compose
# because both contain Edge Stack exporter services (P59/P60).

COMPOSE_FILES=(
  "docker-compose-crypto-orderflow.yml"
  "docker-compose-timers.yml"
)

for CF in "${COMPOSE_FILES[@]}"; do
  CF_PATH="${ROOT_DIR}/${CF}"
  if [[ ! -f "${CF_PATH}" ]]; then
    echo "[lint] SKIP (not found): ${CF}"
    continue
  fi
  echo "[lint] docker compose config: ${CF}"
  if docker compose -f "${CF_PATH}" config -q 2>&1; then
    echo "[lint] OK: ${CF}"
    PASS=$((PASS + 1))
  else
    echo "[lint] FAIL: ${CF}"
    FAIL=$((FAIL + 1))
  fi
done

echo ""
echo "[lint] nginx conf syntax (runbooks-web)"
if docker run --rm -v "${ROOT_DIR}/monitoring/runbooks_web/nginx.conf:/etc/nginx/conf.d/default.conf:ro" nginx:1.27-alpine nginx -t; then
  echo "[lint] OK: nginx conf syntax"
  PASS=$((PASS + 1))
else
  echo "[lint] FAIL: nginx conf syntax"
  FAIL=$((FAIL + 1))
fi

# ── 2. promtool check config ─────────────────────────────────────────────────
# Validate the trade-prometheus config (monitoring/prometheus/prometheus.yml).
# This ensures scrape_configs and alerting wiring are syntactically valid.

PROM_CONFIG="${ROOT_DIR}/monitoring/prometheus/prometheus.yml"
if [[ -f "${PROM_CONFIG}" ]]; then
  echo "[lint] promtool check config: monitoring/prometheus/prometheus.yml"
  if docker run --rm \
      --entrypoint promtool \
      -v "${ROOT_DIR}:/repo" \
      -v "${ROOT_DIR}/orderflow_services:/etc/prometheus/rules/orderflow_services:ro" \
      -w /repo \
      prom/prometheus:v2.54.1 \
      check config monitoring/prometheus/prometheus.yml; then
    echo "[lint] OK: promtool check config"
    PASS=$((PASS + 1))
  else
    echo "[lint] FAIL: promtool check config (monitoring/prometheus/prometheus.yml)"
    FAIL=$((FAIL + 1))
  fi
else
  echo "[lint] SKIP (not found): monitoring/prometheus/prometheus.yml"
fi

echo "[lint] blackbox config check"
if docker run --rm \
  -v "${ROOT_DIR}/monitoring/blackbox/blackbox.yml:/etc/blackbox/blackbox.yml:ro" \
  prom/blackbox-exporter:v0.25.0 \
  --config.file=/etc/blackbox/blackbox.yml --config.check; then
  echo "[lint] OK: blackbox config check"
  PASS=$((PASS + 1))
else
  echo "[lint] FAIL: blackbox config check"
  FAIL=$((FAIL + 1))
fi

# ── 3. Grafana dashboard lint ──────────────────────────────────────────────────
echo "[lint] grafana dashboard JSON: monitoring/grafana/dashboards/edge_stack_overview.json"
python3 - <<'PY'
import json
p='monitoring/grafana/dashboards/edge_stack_overview.json'
try:
    json.load(open(p,'r',encoding='utf-8'))
    print('[lint] OK json')
except Exception as e:
    print(f'[lint] FAIL json: {e}')
    import sys
    sys.exit(1)
PY
if [ $? -ne 0 ]; then
    FAIL=$((FAIL + 1))
else
    PASS=$((PASS + 1))
fi

# ── 4. promtool check rules ───────────────────────────────────────────────────
# Collect all prometheus_alerts_*.yml from common locations in the repo.

echo ""
echo "[lint] blackbox config check"
if docker run --rm \
  -v "${ROOT_DIR}/monitoring/blackbox/blackbox.yml:/etc/blackbox/blackbox.yml:ro" \
  prom/blackbox-exporter:v0.25.0 \
  --config.file=/etc/blackbox/blackbox.yml --config.check; then
  echo "[lint] OK: blackbox config check"
  PASS=$((PASS + 1))
else
  echo "[lint] FAIL: blackbox config check"
  FAIL=$((FAIL + 1))
fi

echo ""
echo "[lint] Collecting prometheus_alerts_*.yml files..."

RULE_FILES=()

# Root-level alert YAMLs
while IFS= read -r -d '' f; do
  RULE_FILES+=("$f")
done < <(find "${ROOT_DIR}" -maxdepth 1 -type f -name 'prometheus_alerts_*.yml' -print0 2>/dev/null)

# orderflow_services/ alert YAMLs
while IFS= read -r -d '' f; do
  RULE_FILES+=("$f")
done < <(find "${ROOT_DIR}/orderflow_services" -maxdepth 1 -type f -name 'prometheus_alerts_*.yml' -print0 2>/dev/null)

# python-worker/orderflow_services/ alert YAMLs
while IFS= read -r -d '' f; do
  RULE_FILES+=("$f")
done < <(find "${ROOT_DIR}/python-worker/orderflow_services" -maxdepth 1 -type f -name 'prometheus_alerts_*.yml' -print0 2>/dev/null)

if [[ ${#RULE_FILES[@]} -eq 0 ]]; then
  echo "[lint] FAIL: no prometheus_alerts_*.yml files found in repo"
  FAIL=$((FAIL + 1))
else
  echo "[lint] Found ${#RULE_FILES[@]} rule file(s):"
  for f in "${RULE_FILES[@]}"; do
    echo "       ${f#${ROOT_DIR}/}"
  done

  # Convert absolute paths to repo-relative paths for promtool (runs in /repo)
  REL_FILES=()
  for f in "${RULE_FILES[@]}"; do
    REL_FILES+=("${f#${ROOT_DIR}/}")
  done

  echo ""
  echo "[lint] alerts annotations check (runbook_path + dashboard_path)..."
  if python3 "${ROOT_DIR}/scripts/ci_alerts_annotations_check.py" "${RULE_FILES[@]}"; then
    echo "[lint] OK: alerts annotations check"
    PASS=$((PASS + 1))
  else
    echo "[lint] FAIL: alerts annotations check"
    FAIL=$((FAIL + 1))
  fi

  echo ""
  echo "[lint] alerts links exist check (runbooks + dashboard uids)..."
  if python3 "${ROOT_DIR}/scripts/ci_alerts_links_exist_check.py"; then
    echo "[lint] OK: alerts links exist check"
    PASS=$((PASS + 1))
  else
    echo "[lint] FAIL: alerts links exist check"
    FAIL=$((FAIL + 1))
  fi

  echo ""
  echo "[lint] smoke contract targets autogen check (alerts -> cfg:monitoring_smoke:targets)..."
  if python3 "${ROOT_DIR}/scripts/ci_smoke_contract_targets_autogen_check.py"; then
    echo "[lint] OK: smoke contract targets autogen check"
    PASS=$((PASS + 1))
  else
    echo "[lint] FAIL: smoke contract targets autogen check"
    FAIL=$((FAIL + 1))
  fi

  echo ""
  echo "[lint] promtool check rules (via prom/prometheus:v2.54.1 image)..."
  if docker run --rm \
      --entrypoint promtool \
      -v "${ROOT_DIR}:/repo" \
      -w /repo \
      prom/prometheus:v2.54.1 \
      check rules "${REL_FILES[@]}"; then
    echo "[lint] OK: promtool check rules"
    PASS=$((PASS + 1))
  else
    echo "[lint] FAIL: promtool check rules"
    FAIL=$((FAIL + 1))
  fi
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "[lint] Summary: PASS=${PASS}  FAIL=${FAIL}"

if [[ ${FAIL} -gt 0 ]]; then
  echo "[lint] ❌ CI lint FAILED"
  exit 1
else
  echo "[lint] ✅ CI lint OK"
fi
