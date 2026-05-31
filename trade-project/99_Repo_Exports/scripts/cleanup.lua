-- Lua скрипт для очистки Redis Streams
-- Удаляет старые записи и поддерживает максимальную длину

local streams = {
    {name = "stream:symbol-to-redis", maxlen = 1000},
    {name = "stream:kline_1m", maxlen = 5000},
    {name = "stream:kline_5m", maxlen = 3000},
    {name = "stream:kline_15m", maxlen = 2000},
    {name = "stream:kline_30m", maxlen = 1500},
    {name = "stream:kline_1h", maxlen = 1000},
    {name = "stream:kline_4h", maxlen = 500},
    {name = "stream:kline_1d", maxlen = 100},
    {name = "signal:telegram:raw", maxlen = 2000},
    {name = "signal:telegram:parsed", maxlen = 1000},
    {name = "notify:telegram", maxlen = 500},
    {name = "stream:volatility", maxlen = 1000},
    {name = "stream:top-gainers", maxlen = 500},
    {name = "stream:top-losers", maxlen = 500}
}

local cleaned_count = 0

for _, stream in ipairs(streams) do
    local length = redis.call("XLEN", stream.name)
    if length > stream.maxlen then
        local to_remove = length - stream.maxlen
        redis.call("XTRIM", stream.name, "MAXLEN", stream.maxlen)
        cleaned_count = cleaned_count + to_remove
        redis.log(redis.LOG_NOTICE, "Очищен стрим " .. stream.name .. ": удалено " .. to_remove .. " записей")
    end
end

redis.log(redis.LOG_NOTICE, "Всего удалено записей: " .. cleaned_count)
return cleaned_count
