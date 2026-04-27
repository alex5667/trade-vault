#!/bin/bash

# Скрипт для мониторинга производительности Redis при сверх больших нагрузках

echo "🔍 Мониторинг производительности Redis для СВЕРХ БОЛЬШИХ НАГРУЗОК"
echo "=================================================================="

# Проверяем статус Redis
echo "📊 Статус Redis:"
docker exec scanner-redis redis-cli ping

echo ""
echo "💾 Использование памяти:"
docker exec scanner-redis redis-cli info memory | grep -E "(used_memory_human|maxmemory_human|used_memory_peak_human|used_memory_dataset_perc)"

echo ""
echo "⚡ Производительность:"
docker exec scanner-redis redis-cli info stats | grep -E "(total_commands_processed|instantaneous_ops_per_sec|keyspace_hits|keyspace_misses|connected_clients)"

echo ""
echo "🔗 Соединения:"
docker exec scanner-redis redis-cli info clients | grep -E "(connected_clients|client_recent_max_input_buffer|client_recent_max_output_buffer)"

echo ""
echo "📈 Стримы:"
docker exec scanner-redis redis-cli info keyspace | grep -E "(stream)"

echo ""
echo "🔄 Активные ключи:"
docker exec scanner-redis redis-cli dbsize

echo ""
echo "⏱️  Время отклика:"
time docker exec scanner-redis redis-cli ping

echo ""
echo "🎯 Топ ключи по размеру:"
docker exec scanner-redis redis-cli --latency-history -i 1 | head -5

echo ""
echo "✅ Мониторинг завершен!"
