-- Скрипт для удаления записей с таймфреймами kline_30m и kline_8h
local stream = 'candles:data'
local removed_count = 0
local batch_size = 1000
local start_id = '0'
local end_id = '+'

repeat
    local entries = redis.call('XRANGE', stream, start_id, end_id, 'COUNT', batch_size)
    local batch_removed = 0
    
    for i = 1, #entries, 2 do
        local entry_id = entries[i]
        local fields = entries[i+1]
        local should_remove = false
        
        -- Ищем поле timeframe
        for j = 1, #fields, 2 do
            if fields[j] == 'timeframe' then
                local timeframe = fields[j+1]
                -- Проверяем, нужно ли удалить
                if timeframe == 'kline_30m' or timeframe == 'kline_8h' then
                    should_remove = true
                end
                break
            end
        end
        
        -- Удаляем запись если нужно
        if should_remove then
            redis.call('XDEL', stream, entry_id)
            batch_removed = batch_removed + 1
        end
    end
    
    removed_count = removed_count + batch_removed
    
    -- Если в батче было меньше записей чем batch_size, значит достигли конца
    if #entries < batch_size * 2 then
        break
    end
    
    -- Обновляем start_id для следующего батча
    if #entries > 0 then
        start_id = '(' .. entries[#entries-1]
    end
    
until false

return removed_count 