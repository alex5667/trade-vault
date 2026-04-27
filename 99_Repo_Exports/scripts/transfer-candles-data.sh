#!/bin/bash

echo "🔄 ПЕРЕНОС CANDLES:DATA НА ПОРТ 6380:"
echo "===================================="
echo ""

# Очищаем существующие данные на порту 6380
echo "🧹 Очистка существующих данных на порту 6380..."
docker exec scanner-redis-worker-1 redis-cli -p 6379 DEL candles:data

# Создаем скрипт для переноса данных
echo "📝 Создание скрипта переноса..."
cat > /tmp/transfer_script.lua << 'LUA'
local source_redis = redis.call('SELECT', 0)
local target_redis = redis.call('SELECT', 0)

-- Получаем все записи из источника
local entries = redis.call('XRANGE', 'candles:data', '-', '+')

-- Переносим каждую запись
for i, entry in ipairs(entries) do
    local id = entry[1]
    local fields = entry[2]
    
    -- Создаем команду XADD для целевого Redis
    local cmd = {'XADD', 'candles:data', id}
    
    -- Добавляем все поля
    for j = 1, #fields do
        table.insert(cmd, fields[j])
    end
    
    -- Выполняем команду
    redis.call(unpack(cmd))
end

return #entries
LUA

echo "📤 Перенос данных с порта 6379 на 6380..."

# Получаем все записи с порта 6379 и переносим на 6380
docker exec scanner-redis redis-cli -p 6379 XRANGE candles:data - + | while read -r line; do
    if [[ $line =~ ^[0-9]+-[0-9]+$ ]]; then
        # Это ID записи
        entry_id="$line"
        echo "Перенос записи: $entry_id"
        
        # Получаем полную запись
        entry_data=$(docker exec scanner-redis redis-cli -p 6379 XRANGE candles:data "$entry_id" "$entry_id")
        
        # Переносим на порт 6380
        echo "$entry_data" | docker exec -i scanner-redis-worker-1 redis-cli -p 6379 XADD candles:data "$entry_id" --pipe
    fi
done

echo ""
echo "📊 Проверка результата:"
echo "Записей на порту 6379: $(docker exec scanner-redis redis-cli -p 6379 XLEN candles:data)"
echo "Записей на порту 6380: $(docker exec scanner-redis-worker-1 redis-cli -p 6379 XLEN candles:data)"

echo ""
echo "✅ ПЕРЕНОС ЗАВЕРШЕН!"
