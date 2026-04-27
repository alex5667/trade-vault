#!/usr/bin/env bash
# ── deploy-grafana-to-minik.sh ─────────────────────────────────────────────
# Развёртывает Grafana + provisioning + 18 дашбордов на minik.
# Grafana будет доступна на http://192.168.0.121:3001 (admin / admin)
#
# Использование:
#   ./scripts/deploy-grafana-to-minik.sh
#   ./scripts/deploy-grafana-to-minik.sh 192.168.0.121
# ────────────────────────────────────────────────────────────────────────────
set -euo pipefail

MINIK_HOST="${1:-192.168.0.121}"
MINIK_USER="${2:-alex}"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REMOTE_BASE="/opt/monitoring"
GRAFANA_PORT="${GRAFANA_PORT:-3002}"

echo "📊 Развёртывание Grafana на ${MINIK_HOST}:${GRAFANA_PORT}..."
echo "   Источник: ${SCRIPT_DIR}/monitoring/grafana"

# Шаг 1: Создание директорий на minik
echo ""
echo "📦 Шаг 1: Подготовка директорий на ${MINIK_HOST}..."
ssh "${MINIK_USER}@${MINIK_HOST}" "mkdir -p \
    ${REMOTE_BASE}/grafana/provisioning/datasources \
    ${REMOTE_BASE}/grafana/provisioning/dashboards \
    ${REMOTE_BASE}/grafana/dashboards"

# Шаг 2: Копирование provisioning конфигов
echo ""
echo "📦 Шаг 2: Копирование provisioning конфигов..."
scp -q "${SCRIPT_DIR}/monitoring/grafana/provisioning/datasources/"*.yml \
    "${MINIK_USER}@${MINIK_HOST}:${REMOTE_BASE}/grafana/provisioning/datasources/"
echo "  ✅ datasources/*.yml"

scp -q "${SCRIPT_DIR}/monitoring/grafana/provisioning/dashboards/dashboards.yml" \
    "${MINIK_USER}@${MINIK_HOST}:${REMOTE_BASE}/grafana/provisioning/dashboards/"
echo "  ✅ dashboards/dashboards.yml"

# Шаг 3: Копирование дашбордов
echo ""
echo "📦 Шаг 3: Копирование дашбордов..."
for f in "${SCRIPT_DIR}"/monitoring/grafana/dashboards/*.json; do
    basename_f=$(basename "$f")
    scp -q "$f" "${MINIK_USER}@${MINIK_HOST}:${REMOTE_BASE}/grafana/dashboards/"
    echo "  ✅ ${basename_f}"
done

# Шаг 4: Создать/обновить docker-compose для grafana на minik
echo ""
echo "📦 Шаг 4: Создание docker-compose-grafana.yml на ${MINIK_HOST}..."
ssh "${MINIK_USER}@${MINIK_HOST}" "cat > ${REMOTE_BASE}/docker-compose-grafana.yml << 'COMPOSE_EOF'
services:
  minik-grafana:
    image: grafana/grafana:latest
    container_name: minik-grafana
    restart: unless-stopped
    ports:
      - \"${GRAFANA_PORT:-3001}:3000\"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_LOG_LEVEL=warn
      - GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/scanner_aiops_overview.json
    volumes:
      - grafana-data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
    networks:
      - compose_trade-net

volumes:
  grafana-data:
    driver: local

networks:
  compose_trade-net:
    external: true
COMPOSE_EOF"
echo "  ✅ docker-compose-grafana.yml создан"

# Шаг 5: Запуск Grafana
echo ""
echo "🚀 Шаг 5: Запуск minik-grafana..."
ssh "${MINIK_USER}@${MINIK_HOST}" "docker rm -f minik-grafana 2>/dev/null || true; cd ${REMOTE_BASE} && docker compose -f docker-compose-grafana.yml up -d --force-recreate"

# Шаг 6: Ожидание и проверка
echo ""
echo "⏳ Ожидание запуска Grafana (5 секунд)..."
sleep 5

echo "🔍 Шаг 6: Проверка..."
ssh "${MINIK_USER}@${MINIK_HOST}" '
    if curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:'"${GRAFANA_PORT}"'/api/health" | grep -q "200"; then
        echo "  ✅ Grafana API OK"
    else
        echo "  ⚠️  Grafana ещё не готова (подождите 10-15 секунд)"
    fi
    
    # Проверка подключения к Prometheus datasource
    result=$(curl -s -u admin:admin "http://127.0.0.1:'"${GRAFANA_PORT}"'/api/datasources" 2>/dev/null)
    if echo "$result" | python3 -c "import json,sys; ds=json.load(sys.stdin); [print(f\"  📡 Datasource: {d[\"name\"]} → {d[\"url\"]}\") for d in ds]" 2>/dev/null; then
        true
    else
        echo "  ⚠️  Не удалось получить datasources (Grafana стартует)"
    fi
'

echo ""
echo "════════════════════════════════════════════════════════════"
echo "✅ Grafana развёрнута!"
echo ""
echo "   URL:      http://${MINIK_HOST}:${GRAFANA_PORT}"
echo "   Логин:    admin / admin"
echo "   Home:     Scanner AIOps Overview"
echo "   Дашборды: 18 (включая Scanner AIOps Overview)"
echo "════════════════════════════════════════════════════════════"
