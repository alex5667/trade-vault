# Timeutil Package

Утилиты для работы с временными метками в едином формате.

## Стандарт проекта

```
Формат: Unix timestamp в миллисекундах (UTC)
Тип: int64 (для вычислений), string (для Redis)
Пример: 1697366459999
```

## Использование

```go
package main

import (
    "fmt"
    "go-worker/pkg/timeutil"
)

func main() {
    // Текущее время
    ts := timeutil.GetCurrentTimestampMs()
    fmt.Println(ts)  // 1697366459999

    // Извлечь closeTime из данных Binance
    candleData := map[string]interface{}{
        "T": int64(1697366459999),
        "symbol": "BTCUSDT",
    }
    closeTime := timeutil.ExtractBinanceCloseTime(candleData)

    // Форматировать для Redis
    formatted := timeutil.FormatTimestampForRedis(closeTime)
    fmt.Println(formatted)  // "1697366459999"

    // Автоматически создать поля для Redis Stream
    fields := timeutil.CreateRedisStreamFields(
        candleData,
        "timestamp",
        true,
        "T",
    )
    // Результат: map[string]interface{}{"timestamp": "1697366459999", "symbol": "BTCUSDT"}
}
```

## Функции

### Основные

- `GetCurrentTimestampMs()` - получить текущее время UTC в миллисекундах
- `FormatTimestampForRedis(ts)` - форматировать для Redis (int64 → string)
- `ExtractEventTimestamp(data, field, fallback)` - извлечь timestamp из данных
- `ValidateTimestamp(ts)` - валидировать timestamp

### Binance специфичные

- `ExtractBinanceCloseTime(candleData)` - извлечь closeTime из свечи
- `ParseIntervalToMs(interval)` - конвертировать "1m", "5m" в миллисекунды
- `ExtractMaxTimestamp(dataArray, field)` - найти максимальный timestamp

### Конвертация

- `TimestampToISO(tsMs)` - в ISO 8601 (для логов)
- `TimestampToHuman(tsMs, layout)` - в человекочитаемый формат
- `ConvertTimestampSafely(timestamp)` - безопасная конвертация из любого типа

### Утилиты

- `CreateRedisStreamFields(data, ...)` - создать поля для XAdd
- `NormalizeTimeframe(tf)` - убрать "kline\_" префикс

## Тестирование

```bash
cd go-worker/pkg/timeutil
go test -v
go test -bench=.
```

## Документация

См. [TIMESTAMP_MIGRATION_GUIDE.md](../../../docs/TIMESTAMP_MIGRATION_GUIDE.md)
