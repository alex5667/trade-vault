#!/bin/bash

echo "📊 МОНИТОРИНГ ПРОИЗВОДИТЕЛЬНОСТИ REDIS

🔍 ОСНОВНАЯ ИНФОРМАЦИЯ:
================================"

# Проверка подключения
echo "Ping: $(docker exec scanner-redis redis-cli ping)"

# Информация о сервере
echo ""
echo "📈 ИНФОРМАЦИЯ О СЕРВЕРЕ:"
echo "Версия Redis: $(docker exec scanner-redis redis-cli info server | grep redis_version | cut -d: -f2)"
echo "Режим: $(docker exec scanner-redis redis-cli info server | grep redis_mode | cut -d: -f2)"
echo "Uptime: $(docker exec scanner-redis redis-cli info server | grep uptime_in_seconds | cut -d: -f2) секунд"

# Память
echo ""
echo "💾 ПАМЯТЬ:"
echo "Используется: $(docker exec scanner-redis redis-cli info memory | grep used_memory_human | cut -d: -f2)"
echo "Пиковое использование: $(docker exec scanner-redis redis-cli info memory | grep used_memory_peak_human | cut -d: -f2)"
echo "Максимум: $(docker exec scanner-redis redis-cli info memory | grep maxmemory_human | cut -d: -f2)"

# Клиенты
echo ""
echo "👥 КЛИЕНТЫ:"
echo "Подключено: $(docker exec scanner-redis redis-cli info clients | grep connected_clients | cut -d: -f2)"
echo "Максимум: $(docker exec scanner-redis redis-cli info clients | grep maxclients | cut -d: -f2)"

# Статистика
echo ""
echo "📊 СТАТИСТИКА:"
echo "Всего команд: $(docker exec scanner-redis redis-cli info stats | grep total_commands_processed | cut -d: -f2)"
echo "Команд в секунду: $(docker exec scanner-redis redis-cli info stats | grep instantaneous_ops_per_sec | cut -d: -f2)"
echo "Подключений: $(docker exec scanner-redis redis-cli info stats | grep total_connections_received | cut -d: -f2)"

# Стримы
echo ""
echo "📡 СТРИМЫ:"
echo "candles:data: $(docker exec scanner-redis redis-cli XLEN candles:data) записей"
echo "stream:top-gainers: $(docker exec scanner-redis redis-cli XLEN stream:top-gainers) записей"
echo "stream:top-losers: $(docker exec scanner-redis redis-cli XLEN stream:top-losers) записей"

# Производительность
echo ""
echo "⚡ ПРОИЗВОДИТЕЛЬНОСТЬ:"
echo "Средняя задержка: $(docker exec scanner-redis redis-cli --latency-history -i 1 | head -1)"
echo "CPU использование: $(docker exec scanner-redis redis-cli info cpu | grep used_cpu_sys | cut -d: -f2)"

echo ""
echo "🔧 КОНФИГУРАЦИЯ:"
echo "TCP backlog: $(docker exec scanner-redis redis-cli config get tcp-backlog | tail -1)"
echo "Timeout: $(docker exec scanner-redis redis-cli config get timeout | tail -1)"
echo "Keepalive: $(docker exec scanner-redis redis-cli config get tcp-keepalive | tail -1)"

echo ""
echo "✅ Мониторинг завершен"
