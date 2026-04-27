#!/usr/bin/env bash
# validate_prometheus_rules_bundle_v1.sh
#
# Unified CI/manual wrapper: Prometheus rules bundle validation + runtime loaded-probe smoke.
# Catches two distinct failure modes:
#   (A) Syntax / semantic issues in rule files   → promtool + Python structural validator
#   (B) "File not picked up by Prometheus"       → prom_rules_loaded_probe_v1 (requires live Prometheus)
#
# Usage:
#   ./tools/validate_prometheus_rules_bundle_v1.sh [--skip-probe] [--skip-promtool] [--root DIR]
#
# Exit codes:
#   0  all checks passed
#   1  structural validation failed (YAML / schema errors)
#   2  runtime probe failed (files missing from Prometheus rule_files)
#   3  both (1) and (2) failed
#
# ENV:
#   PROMETHEUS_URL                   (default: http://prometheus:9090)
#   PROM_RULES_BUNDLE_SMOKE_PROMTOOL (default: auto)   — "auto|on|off"
#   ENABLE_PROM_RULES_LOADED_PROBE   (default: 1)
#   PYTHON                           (default: python3)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}/.."
PYTHON="${PYTHON:-python3}"
PROMTOOL="${PROMTOOL:-promtool}"
SKIP_PROBE=0
SKIP_PROMTOOL=0
EXIT_BUNDLE=0
EXIT_PROBE=0

export PYTHONPATH="${REPO_ROOT}"

# ─── arg parsing ─────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-probe)     SKIP_PROBE=1 ;;
        --skip-promtool)  SKIP_PROMTOOL=1 ;;
        --root)           REPO_ROOT="$2"; shift ;;
        *) echo "[WARN] unknown arg: $1" ;;
    esac
    shift
done

echo "═══════════════════════════════════════════════════════════════"
echo "  Prometheus rules bundle validator + loaded-probe  (v18)"
echo "  repo_root=${REPO_ROOT}"
echo "═══════════════════════════════════════════════════════════════"

# ─── (A) Structural validation ───────────────────────────────────────────────
echo ""
echo "── [A] Structural validation (Python validator + optional promtool) ──"

VALIDATOR="${REPO_ROOT}/orderflow_services/validate_prometheus_rules_bundle_v1.py"
if [[ ! -f "${VALIDATOR}" ]]; then
    VALIDATOR="${REPO_ROOT}/tick_flow_full/orderflow_services/validate_prometheus_rules_bundle_v1.py"
fi

if [[ -f "${VALIDATOR}" ]]; then
    echo "[RUN] ${PYTHON} ${VALIDATOR} --root ${REPO_ROOT}"
    if "${PYTHON}" "${VALIDATOR}" --root "${REPO_ROOT}"; then
        echo "[OK]  Python structural validator passed"
    else
        echo "[FAIL] Python structural validator returned non-zero"
        EXIT_BUNDLE=1
    fi
else
    echo "[WARN] validate_prometheus_rules_bundle_v1.py not found — skipping Python validator"
fi

# Optional: promtool check rules on all discovered rule files
if [[ "${SKIP_PROMTOOL}" -eq 0 ]]; then
    PROMTOOL_MODE="${PROM_RULES_BUNDLE_SMOKE_PROMTOOL:-auto}"
    if [[ "${PROMTOOL_MODE}" == "off" ]]; then
        echo "[SKIP] promtool disabled via PROM_RULES_BUNDLE_SMOKE_PROMTOOL=off"
    else
        if command -v "${PROMTOOL}" &>/dev/null; then
            echo "[RUN] promtool check rules (discovering *.yml files)"
            # Find all prometheus alert/rule YAML files in both trees
            mapfile -d '' RULE_FILES < <(find "${REPO_ROOT}/orderflow_services" "${REPO_ROOT}/tick_flow_full/orderflow_services" \
                -maxdepth 2 -name 'prometheus_alerts_*.yml' -o -name 'prometheus_rules_*.yml' \
                2>/dev/null | sort -u | tr '\n' '\0')
            if [[ ${#RULE_FILES[@]} -gt 0 ]]; then
                if "${PROMTOOL}" check rules "${RULE_FILES[@]}" 2>&1; then
                    echo "[OK]  promtool check rules passed (${#RULE_FILES[@]} files)"
                else
                    echo "[FAIL] promtool check rules failed"
                    EXIT_BUNDLE=1
                fi
            else
                echo "[WARN] No rule files found for promtool check"
            fi
        elif [[ "${PROMTOOL_MODE}" == "on" ]]; then
            echo "[FAIL] promtool not found but PROM_RULES_BUNDLE_SMOKE_PROMTOOL=on"
            EXIT_BUNDLE=1
        else
            echo "[SKIP] promtool not found (auto mode — skipping)"
        fi
    fi
fi

# ─── (B) Runtime loaded-probe ─────────────────────────────────────────────────
echo ""
echo "── [B] Runtime rules-loaded probe (Prometheus /api/v1/rules) ──"

ENABLE_PROBE="${ENABLE_PROM_RULES_LOADED_PROBE:-1}"
if [[ "${SKIP_PROBE}" -eq 1 || "${ENABLE_PROBE}" != "1" ]]; then
    echo "[SKIP] Runtime loaded-probe skipped (SKIP_PROBE=${SKIP_PROBE} ENABLE_PROM_RULES_LOADED_PROBE=${ENABLE_PROBE})"
else
    PROBE="${REPO_ROOT}/orderflow_services/prom_rules_loaded_probe_v1.py"
    if [[ ! -f "${PROBE}" ]]; then
        PROBE="${REPO_ROOT}/tick_flow_full/orderflow_services/prom_rules_loaded_probe_v1.py"
    fi

    if [[ -f "${PROBE}" ]]; then
        PROM_URL="${PROMETHEUS_URL:-http://prometheus:9090}"
        echo "[RUN] ${PYTHON} ${PROBE} --prom-url ${PROM_URL} --root ${REPO_ROOT}"
        if "${PYTHON}" "${PROBE}" --prom-url "${PROM_URL}" --root "${REPO_ROOT}"; then
            echo "[OK]  Runtime loaded-probe: all expected files are loaded in Prometheus"
        else
            RC=$?
            if [[ ${RC} -eq 2 ]]; then
                echo "[FAIL] Runtime loaded-probe: missing files detected (mount / rule_files glob issue)"
            else
                echo "[FAIL] Runtime loaded-probe returned exit code ${RC}"
            fi
            EXIT_PROBE=2
        fi
    else
        echo "[WARN] prom_rules_loaded_probe_v1.py not found — skipping runtime probe"
    fi
fi

# ─── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════════"
if [[ ${EXIT_BUNDLE} -eq 0 && ${EXIT_PROBE} -eq 0 ]]; then
    echo "  [PASS] All checks passed"
    exit 0
elif [[ ${EXIT_BUNDLE} -ne 0 && ${EXIT_PROBE} -ne 0 ]]; then
    echo "  [FAIL] Both structural validation AND runtime probe failed  → exit 3"
    exit 3
elif [[ ${EXIT_BUNDLE} -ne 0 ]]; then
    echo "  [FAIL] Structural validation failed (YAML/schema)           → exit 1"
    exit 1
else
    echo "  [FAIL] Runtime probe failed (files missing from Prometheus) → exit 2"
    exit 2
fi
