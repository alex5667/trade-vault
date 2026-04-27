#!/bin/bash

# Скрипт для проверки доступности Binance API и WebSocket
# Используется для диагностики проблем с подключением

echo "🔍 Проверка доступности Binance API и WebSocket"
echo "================================================"

# Проверка REST API
echo "📡 Проверка REST API..."
if curl -s -f "https://api.binance.com/api/v3/ping" > /dev/null; then
    echo "✅ REST API доступен"
else
    echo "❌ REST API недоступен"
fi

# Проверка WebSocket endpoint
echo "🔌 Проверка WebSocket endpoint..."
if curl -s -f "https://stream.binance.com:9443" > /dev/null; then
    echo "✅ WebSocket endpoint доступен"
else
    echo "❌ WebSocket endpoint недоступен"
fi

# Проверка DNS
echo "🌐 Проверка DNS..."
if nslookup stream.binance.com > /dev/null 2>&1; then
    echo "✅ DNS резолвинг работает"
    nslookup stream.binance.com | grep "Address:"
else
    echo "❌ Проблемы с DNS"
fi

# Проверка порта
echo "🔌 Проверка порта 9443..."
if nc -z -w5 stream.binance.com 9443 2>/dev/null; then
    echo "✅ Порт 9443 открыт"
else
    echo "❌ Порт 9443 недоступен"
fi

# Проверка сетевого маршрута
echo "🛣️ Проверка сетевого маршрута..."
if traceroute -m 15 stream.binance.com > /dev/null 2>&1; then
    echo "✅ Сетевой маршрут доступен"
else
    echo "❌ Проблемы с сетевым маршрутом"
fi

# Проверка локальных портов
echo "🏠 Проверка локальных портов..."
echo "Порт 2112 (Go Worker): $(netstat -tlnp 2>/dev/null | grep :2112 || echo 'не используется')"
echo "Порт 6379 (Redis): $(netstat -tlnp 2>/dev/null | grep :6379 || echo 'не используется')"
echo "Порт 9090 (Prometheus): $(netstat -tlnp 2>/dev/null | grep :9090 || echo 'не используется')"
echo "Порт 3001 (Grafana): $(netstat -tlnp 2>/dev/null | grep :3001 || echo 'не используется')"

# Проверка Docker контейнеров
echo "🐳 Проверка Docker контейнеров..."
if command -v docker >/dev/null 2>&1; then
    echo "Статус контейнеров:"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep scanner
else
    echo "Docker не установлен"
fi

echo ""
echo "📊 Рекомендации:"
echo "1. Если REST API недоступен - проблемы с Binance"
echo "2. Если WebSocket недоступен - проблемы с сетью или Binance"
echo "3. Если DNS не работает - проблемы с DNS сервером"
echo "4. Если порт 9443 закрыт - проблемы с файрволом или Binance"
echo "5. Если контейнеры не запущены - перезапустите систему" 