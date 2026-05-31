#!/usr/bin/env bash
# ── deploy-prom-rules-to-minik.sh ──────────────────────────────────────────
# Копирует alert rule_files из scanner_infra в minik-prometheus.
# Файлы копируются на хост (/opt/monitoring/config/prometheus_rules/alerts/),
# откуда они примонтированы в контейнер как :ro.
#
# Использование:
#   ./scripts/deploy-prom-rules-to-minik.sh
#   ./scripts/deploy-prom-rules-to-minik.sh 192.168.0.121
# ────────────────────────────────────────────────────────────────────────────
set -euo pipefail

MINIK_HOST="${1:-192.168.0.121}"
MINIK_USER="${2:-alex}"
REMOTE_RULES_DIR="/opt/monitoring/config/prometheus_rules/alerts"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "📊 Развёртывание Prometheus rule files на ${MINIK_HOST}..."
echo "   Источник: ${SCRIPT_DIR}"
echo "   Цель: ${MINIK_USER}@${MINIK_HOST}:${REMOTE_RULES_DIR}/"

# Список файлов для копирования
FILES=(
    "websocket_alerts.yml"
    "regime_alerts.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_control_plane_phase0_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_control_plane_phase0_1_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_control_plane_phase0_2_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase1_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase1_1_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase1_2_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase1_3_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase1_4_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase1_5_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase1_6_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase1_7_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase1_8_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase1_9_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_0_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_1_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_2_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_3_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_4_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_5_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_6_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_7_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_8_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_9_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_10_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_11_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_12_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_13_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_14_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_15_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_16_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_17_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_18_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_19_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_20_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_21_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_22_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_23_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_24_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_25_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase2_integration_freeze_v1.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase3_local_fallback_plane_v1.yml"
    "prometheus/ml_confirm_alerts.yml"
    "prometheus/tick_quality_alerts.yml"
    "prometheus/tick_ingest_latency_alerts.yml"
    "prometheus/alerts_meta_dq.yml"
    "prometheus/alerts_confirmations_coverage_v1.yml"
    "prometheus/pipeline_e2e_latency_alerts.yml"
    "python-worker/services/prometheus_alerts_active_symbol_guard_p12.yml"
    "prometheus/rules/trade_latency_slo.yml"
    "python-worker/orderflow_services/prometheus_alerts_ml_phase3_30_route_incident_rca_mirror_rca_winner_apply_apply_experiment_v1.yml"
    "prometheus/alerts/redis_alerts.yml"
    "prometheus/alerts/minik-advanced-alerts.yml"
)

# Шаг 1: scp файлов во /tmp/prom_rules на minik
echo ""
echo "📦 Шаг 1: Копирование файлов в /tmp/prom_rules на ${MINIK_HOST}..."
ssh "${MINIK_USER}@${MINIK_HOST}" "mkdir -p /tmp/prom_rules"

for f in "${FILES[@]}"; do
    src="${SCRIPT_DIR}/${f}"
    basename_f="$(basename "$f")"
    if [ -f "$src" ]; then
        scp -q "$src" "${MINIK_USER}@${MINIK_HOST}:/tmp/prom_rules/${basename_f}"
        echo "  ✅ ${basename_f}"
    else
        echo "  ⚠️  ${f} не найден, пропускаем"
    fi
done

# Шаг 2: Копируем на хост-путь (volume примонтирован как :ro)
# /opt/monitoring/config/prometheus_rules/ принадлежит root,
# alerts/ тоже root. Используем docker exec для копирования.
echo ""
echo "📦 Шаг 2: Копирование в хост-путь ${REMOTE_RULES_DIR}/ через docker..."
ssh "${MINIK_USER}@${MINIK_HOST}" '
    # Копируем через временный alpine контейнер с RW-доступом
    docker run --rm \
        -v /tmp/prom_rules:/src:ro \
        -v /opt/monitoring/config/prometheus_rules/alerts:/dst \
        alpine sh -c "cp /src/*.yml /dst/ && echo \"  ✅ Все файлы скопированы в /opt/monitoring/config/prometheus_rules/alerts/\"" || \
    echo "  ⚠️  Fallback: пробуем cp напрямую..." && \
    for f in /tmp/prom_rules/*.yml; do
        basename_f=$(basename "$f")
        cp "$f" '"${REMOTE_RULES_DIR}"'/"$basename_f" 2>/dev/null && \
            echo "  ✅ $basename_f" || true
    done
'

# Шаг 3: Reload Prometheus
echo ""
echo "🔄 Шаг 3: hot-reload конфигурации Prometheus..."
ssh "${MINIK_USER}@${MINIK_HOST}" '
    curl -s -X POST http://127.0.0.1:9090/-/reload && \
        echo "✅ Prometheus reloaded" || \
        echo "⚠️  Не удалось reload (попробуйте: docker restart minik-prometheus)"
'

# Шаг 4: Проверка
echo ""
echo "🔍 Шаг 4: Проверка загруженных rule groups..."
ssh "${MINIK_USER}@${MINIK_HOST}" '
    curl -s http://127.0.0.1:9090/api/v1/rules 2>/dev/null | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    groups = d.get(\"data\", {}).get(\"groups\", [])
    for g in groups:
        name = g[\"name\"]
        rules_n = len(g.get(\"rules\", []))
        print(f\"  📋 {name}: {rules_n} правил\")
    print(f\"  Всего групп: {len(groups)}\")
except Exception as e:
    print(f\"  ⚠️  Ошибка парсинга: {e}\")
" 2>/dev/null
'

# Очистка
ssh "${MINIK_USER}@${MINIK_HOST}" "rm -rf /tmp/prom_rules" 2>/dev/null || true

echo ""
echo "✅ Развёртывание завершено!"
echo "   Prometheus: http://${MINIK_HOST}:9090"
echo "   Правила: http://${MINIK_HOST}:9090/rules"
