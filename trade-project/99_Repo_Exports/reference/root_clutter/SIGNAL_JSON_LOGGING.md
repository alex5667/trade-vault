# Signal JSON Logging System

## Обзор

Система логирования сигналов в формате "1 сигнал = 1 JSON строка" для удобного анализа логов в Loki/ELK и построения метрик.

## Формат лога

Каждый отправленный сигнал логируется одной строкой JSON:

```json
{
  "signal_id": "uuid-string",
  "kind": "breakout|absorption",
  "side": "buy|sell",
  "symbol": "BTCUSDT",
  "ts": 1700000000000,
  "price": 42000.0,
  "level_key": "p:42000.0",
  "raw_score": 2.0,
  "conf_factor": 0.6,
  "final_score": 1.2,
  "features": {
    "spread_bps": 1.7,
    "obi_avg": 0.12,
    "microprice_shift": 3.3,
    "cancel_to_trade": 9.0,
    "taker_rate": 0.58,
    "regime_score": 0.35,
    "geometry_score": 0.22
  },
  "data_quality": {
    "l2_is_stale": false,
    "used_fallback_hlc": false,
    "missing_htf": false,
    "missing_l3": false
  },
  "parts": {
    "conf_factor": 0.6,
    "raw_score": 2.0,
    "final_score": 1.2,
    "l2_is_stale": false
  },
  "geometry": null
}
```

## Конфигурация

### Environment Variable

```bash
# Включить/выключить JSON логирование сигналов
SIGNAL_ONE_JSON_LOG=1  # 1 - включено, 0 - выключено
```

По умолчанию логирование включено.

## Использование в Loki/ELK

### Примеры запросов

```sql
-- Все сигналы breakout за последний час
{app="crypto-orderflow"} |= `kind.*breakout` | json | ts > now() - 1h

-- Средний conf_factor по символам
{app="crypto-orderflow"} | json | avg(conf_factor) by (symbol)

-- Сигналы с низким качеством данных
{app="crypto-orderflow"} | json | data_quality.missing_l3 == true
```

### Метрики из логов

```promql
# Количество сигналов по типам
count(rate({app="crypto-orderflow"} | json | kind [5m])) by (kind)

# Распределение conf_factor
histogram_quantile(0.95, sum(rate({app="crypto-orderflow"} | json | conf_factor [5m])) by (le))
```

## API

### `build_signal_json_log(payload, ctx, parts)`

Создает dict для JSON логирования.

**Параметры:**
- `payload`: dict с данными сигнала
- `ctx`: контекст выполнения (market data)
- `parts`: дополнительные данные из validation

**Возвращает:** dict готовый для `json.dumps()`

### `log_signal_one_json(logger, payload, ctx, parts)`

Логирует сигнал в формате JSON.

**Параметры:**
- `logger`: Python logger instance
- `payload`: dict с данными сигнала
- `ctx`: контекст выполнения
- `parts`: дополнительные данные

## Интеграция

Логирование автоматически включается в `crypto_orderflow_handler.py` после успешной отправки сигнала через `emit()`.

## Безопасность

- **Fail-open**: Ошибки логирования не влияют на торговую логику
- **Производительность**: Только базовые типы данных, без больших объектов
- **Конфигурируемость**: Можно отключить через environment variable
