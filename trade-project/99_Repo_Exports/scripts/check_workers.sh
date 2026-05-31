#!/bin/bash

# Простая проверка конфигурации воркеров

echo "═══════════════════════════════════════════════════════"
echo "   🔍 Проверка конфигурации Multi-Worker Setup"
echo "═══════════════════════════════════════════════════════"
echo ""

echo "📋 Список воркеров в docker-compose.yml:"
echo "─────────────────────────────────────────────────────"
grep -E "^\s+go-worker-" docker-compose.yml | sed 's/://g' | awk '{print "  ✓", $1}'
echo ""

echo "📊 Настройка таймфреймов:"
echo "─────────────────────────────────────────────────────"
for worker in 1m 5m 15m 1h 4h 1d 1w 1month 3month 1y; do
  timeframe=$(grep -A 20 "go-worker-$worker:" docker-compose.yml | grep "BINANCE_WS_TIMEFRAME" | head -1 | sed 's/.*=//')
  port=$(grep -A 20 "go-worker-$worker:" docker-compose.yml | grep "PROMETHEUS_PORT" | head -1 | sed 's/.*=//')
  redis=$(grep -A 20 "go-worker-$worker:" docker-compose.yml | grep "REDIS_HOST=" | head -1 | sed 's/.*=//' | sed 's/scanner-//')
  
  if [ -n "$timeframe" ] && [ -n "$port" ]; then
    echo "  ✓ go-worker-$worker: $timeframe on $redis (Prometheus :$port)"
  else
    echo "  ✗ go-worker-$worker: ОШИБКА конфигурации"
  fi
done
echo ""

echo "🔧 Prometheus targets:"
echo "─────────────────────────────────────────────────────"
grep "go-worker-" prometheus.yml | sed 's/^[ \t-]*//' | sed 's/^/  ✓ /'
echo ""

echo "📈 Статистика:"
echo "─────────────────────────────────────────────────────"
total_workers=$(grep -c "BINANCE_WS_TIMEFRAME=kline" docker-compose.yml)
total_prometheus=$(grep -c "go-worker-" prometheus.yml)
echo "  • Воркеров в docker-compose.yml: $total_workers"
echo "  • Targets в prometheus.yml: $total_prometheus"
echo ""

if [ "$total_workers" -eq 10 ] && [ "$total_prometheus" -eq 10 ]; then
  echo "✅ Конфигурация корректна! Все 10 воркеров настроены."
  echo ""
  echo "🚀 Запуск системы:"
  echo "   docker-compose up -d"
  echo ""
  echo "📖 Документация:"
  echo "   docs/MULTI_WORKER_SETUP.md"
  echo "   docs/WORKERS_QUICK_REFERENCE.md"
  echo "   docs/CHANGES_SUMMARY.md"
else
  echo "⚠️  Предупреждение: Ожидается 10 воркеров, найдено: $total_workers"
fi

echo "═══════════════════════════════════════════════════════"

