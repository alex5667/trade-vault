#!/bin/bash
# Автоматический деплой REST Candle Fetchers для редких таймфреймов
# Автор: Senior DevOps Team
# Дата: 01.11.2025

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║  🚀 Деплой REST Candle Fetchers для редких таймфреймов              ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

# Проверяем что docker-compose доступен
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose не найден. Установите Docker Compose."
    exit 1
fi

echo "📋 Шаг 1: Остановка старых WebSocket воркеров..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Проверяем существуют ли старые воркеры
if docker-compose ps | grep -q "go-worker-3month"; then
    echo "  Останавливаем go-worker-3month..."
    docker-compose stop go-worker-3month || true
    docker-compose rm -f go-worker-3month || true
    echo "  ✅ go-worker-3month остановлен"
else
    echo "  ℹ️  go-worker-3month не найден (пропускаем)"
fi

if docker-compose ps | grep -q "go-worker-1y"; then
    echo "  Останавливаем go-worker-1y..."
    docker-compose stop go-worker-1y || true
    docker-compose rm -f go-worker-1y || true
    echo "  ✅ go-worker-1y остановлен"
else
    echo "  ℹ️  go-worker-1y не найден (пропускаем)"
fi

echo ""
echo "📦 Шаг 2: Проверка файлов..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ ! -f "go-worker/Dockerfile.rest-fetcher" ]; then
    echo "❌ Файл go-worker/Dockerfile.rest-fetcher не найден!"
    exit 1
fi
echo "  ✅ Dockerfile.rest-fetcher найден"

if [ ! -f "go-worker/binance/rest_candle_fetcher.go" ]; then
    echo "❌ Файл go-worker/binance/rest_candle_fetcher.go не найден!"
    exit 1
fi
echo "  ✅ rest_candle_fetcher.go найден"

if [ ! -f "go-worker/cmd/rest-fetcher/main.go" ]; then
    echo "❌ Файл go-worker/cmd/rest-fetcher/main.go не найден!"
    exit 1
fi
echo "  ✅ cmd/rest-fetcher/main.go найден"

echo ""
echo "⚙️  Шаг 3: Проверка конфигурации docker-compose.yml..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if grep -q "go-rest-fetcher-3month:" docker-compose.yml; then
    echo "  ✅ go-rest-fetcher-3month найден в docker-compose.yml"
else
    echo "  ⚠️  go-rest-fetcher-3month НЕ найден в docker-compose.yml"
    echo "     Пожалуйста, обновите docker-compose.yml согласно:"
    echo "     docs/REST_FETCHER_QUICK_START.md"
    echo ""
    echo "  Продолжить без обновления docker-compose.yml? (y/N)"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        echo "  Деплой отменен."
        exit 1
    fi
fi

if grep -q "go-rest-fetcher-1y:" docker-compose.yml; then
    echo "  ✅ go-rest-fetcher-1y найден в docker-compose.yml"
else
    echo "  ⚠️  go-rest-fetcher-1y НЕ найден в docker-compose.yml"
fi

echo ""
echo "🔨 Шаг 4: Сборка REST Fetcher образов..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if grep -q "go-rest-fetcher-3month:" docker-compose.yml; then
    echo "  Собираем go-rest-fetcher-3month..."
    docker-compose build go-rest-fetcher-3month
    echo "  ✅ go-rest-fetcher-3month собран"
fi

if grep -q "go-rest-fetcher-1y:" docker-compose.yml; then
    echo "  Собираем go-rest-fetcher-1y..."
    docker-compose build go-rest-fetcher-1y
    echo "  ✅ go-rest-fetcher-1y собран"
fi

echo ""
echo "🚀 Шаг 5: Запуск REST Fetchers..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if grep -q "go-rest-fetcher-3month:" docker-compose.yml; then
    echo "  Запускаем go-rest-fetcher-3month..."
    docker-compose up -d go-rest-fetcher-3month
    echo "  ✅ go-rest-fetcher-3month запущен"
fi

if grep -q "go-rest-fetcher-1y:" docker-compose.yml; then
    echo "  Запускаем go-rest-fetcher-1y..."
    docker-compose up -d go-rest-fetcher-1y
    echo "  ✅ go-rest-fetcher-1y запущен"
fi

echo ""
echo "⏳ Шаг 6: Ожидание готовности сервисов (30 секунд)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
sleep 30

echo ""
echo "🔍 Шаг 7: Проверка healthcheck..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Проверяем 3M fetcher
if grep -q "go-rest-fetcher-3month:" docker-compose.yml; then
    echo "  Проверяем go-rest-fetcher-3month (port 8091)..."
    if curl -s --max-time 5 http://localhost:8091/health > /dev/null 2>&1; then
        echo "  ✅ go-rest-fetcher-3month здоров (http://localhost:8091/health)"
    else
        echo "  ⚠️  go-rest-fetcher-3month не отвечает на healthcheck"
        echo "     Проверьте логи: docker-compose logs go-rest-fetcher-3month"
    fi
fi

# Проверяем 1y fetcher
if grep -q "go-rest-fetcher-1y:" docker-compose.yml; then
    echo "  Проверяем go-rest-fetcher-1y (port 8092)..."
    if curl -s --max-time 5 http://localhost:8092/health > /dev/null 2>&1; then
        echo "  ✅ go-rest-fetcher-1y здоров (http://localhost:8092/health)"
    else
        echo "  ⚠️  go-rest-fetcher-1y не отвечает на healthcheck"
        echo "     Проверьте логи: docker-compose logs go-rest-fetcher-1y"
    fi
fi

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║  ✅ ДЕПЛОЙ ЗАВЕРШЕН!                                                 ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "📊 Полезные команды:"
echo ""
echo "  # Проверить статус"
echo "  docker-compose ps | grep rest-fetcher"
echo ""
echo "  # Посмотреть логи 3M fetcher"
echo "  docker-compose logs -f --tail=50 go-rest-fetcher-3month"
echo ""
echo "  # Посмотреть логи 1y fetcher"
echo "  docker-compose logs -f --tail=50 go-rest-fetcher-1y"
echo ""
echo "  # Проверить healthcheck"
echo "  curl http://localhost:8091/health"
echo "  curl http://localhost:8092/health"
echo ""
echo "  # Проверить статистику"
echo "  curl http://localhost:8091/stats | jq"
echo "  curl http://localhost:8092/stats | jq"
echo ""
echo "  # Prometheus метрики"
echo "  curl http://localhost:8091/metrics"
echo "  curl http://localhost:8092/metrics"
echo ""
echo "📚 Документация:"
echo "  - Quick Start: docs/REST_FETCHER_QUICK_START.md"
echo "  - Полная:      docs/REST_FETCHER_FOR_RARE_TIMEFRAMES.md"
echo ""
echo "🎉 Готово! REST Fetchers работают!"

