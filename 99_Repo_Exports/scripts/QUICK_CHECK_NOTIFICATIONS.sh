#!/bin/bash
# Быстрая проверка уведомлений после исправления дублирования
# 31 октября 2025

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 ПРОВЕРКА СИСТЕМЫ УВЕДОМЛЕНИЙ (БЕЗ ДУБЛИРОВАНИЯ)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "1️⃣  Статус сервисов:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker-compose ps | grep -E "(go-gateway|notify-worker|xau-orderflow)" | grep -v "Exit"
echo ""

echo "2️⃣  Последние сигналы из notify:telegram (Redis):"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker exec scanner-redis-worker-1 redis-cli XREVRANGE notify:telegram + - COUNT 3 2>/dev/null | grep -A 2 "text"
echo ""

echo "3️⃣  Последние уведомления notify-worker:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker logs --tail 50 scanner-notify-worker 2>/dev/null | grep -A 6 "🤖 Уведомление" | tail -7
echo ""

echo "4️⃣  Проверка go-gateway (не должно быть Telegram отправок):"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker logs --tail 50 scanner-go-gateway 2>/dev/null | grep -E "(queued|Telegram|SendText)" | tail -5
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "• notify-worker: показывает отправку уведомлений"
echo "• go-gateway: НЕ отправляет в Telegram напрямую"
echo "• Redis: содержит сигналы в компактном формате"
echo "• Telegram: получает ОДНО сообщение на сигнал"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "📱 Формат уведомления (должен быть ТОЛЬКО ОДИН):"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🚨 🔴 XAUUSD SHORT @ 4021.04, Volume 5.55 lot"
echo "📝 mix:p_delta=0.13,p_speed=0.04,p_cluster=0.08,p_legacy=0.03"
echo "🛑 SL 4022.84 | TP1 4018.64 (RR 1.3); TP2 4017.44 (RR 2.0); TP3 4016.24 (RR 2.7)"
echo "🕐 17:12:57 31.10.2025 UTC"
echo "🔧 Source: AggregatedHub-V2 | ID: 1761930777576:SHORT:402104"
echo "📊 ATR=1.20 | Conf=29%"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "🔍 Для мониторинга в реальном времени:"
echo "   docker logs -f scanner-notify-worker | grep -A 6 'Уведомление'"
echo ""

