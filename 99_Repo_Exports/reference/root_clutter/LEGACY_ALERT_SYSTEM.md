# Legacy Alert System

## Обзор

Система мониторинга и алертинга для "legacy reason codes" - когда в продакшене появляются старые строковые коды причин veto вместо структурированных `ReasonCode`.

## Метрики

### `reason_legacy_mapped_total{kind,from,to}`

**Тип**: Counter  
**Описание**: Количество маппингов legacy reason codes в канонические `ReasonCode`  
**Теги**:
- `kind`: Тип сигнала (breakout, absorption, etc.)
- `from`: Исходный legacy код
- `to`: Целевой канонический код

**Пример**:
```
reason_legacy_mapped_total{kind="breakout",from="bo_l2_stale",to="VETO_L2_STALE"} 15
```

## Алерт "Legacy Spike"

### Триггер
- В скользящем окне (5 минут по умолчанию) количество legacy маппингов превышает порог
- Сообщение отправляется в Telegram через `notify:telegram` stream

### Payload
```json
{
  "type": "report",
  "text": "Legacy alert: Всплеск legacy reason_code: где-то начали отдавать старые строки...",
  "symbol": "SYSTEM",
  "ts": 1234567890000,
  "labels": {
    "analytics": 1,
    "type": "legacy_alert",
    "kind": "breakout",
    "from": "bo_l2_stale", 
    "to": "VETO_L2_STALE",
    "window_s": 300,
    "count": 60,
    "hint": "Подробное описание проблемы"
  }
}
```

## Конфигурация

### Environment Variables

```bash
# Окно для подсчета всплесков (секунды)
REASON_LEGACY_ALERT_WINDOW_S=300

# Минимальное количество событий в окне для триггера
REASON_LEGACY_ALERT_MIN_EVENTS=50

# Cooldown между алертами (секунды)  
REASON_LEGACY_ALERT_COOLDOWN_S=900
```

### Дефолтные значения
- **Окно**: 5 минут (300 сек)
- **Порог**: 50 событий
- **Cooldown**: 15 минут (900 сек)

## Архитектура

### Компоненты

1. **ReasonMismatchMonitor** (`signal_scoring/reason_policy.py`)
   - Скользящее окно подсчета
   - Детекция всплесков
   - Формирование payload алерта

2. **emit_legacy_alert** (`handlers/crypto_orderflow_handler.py`)
   - Конвертация payload в формат `notify:telegram`
   - Отправка в Redis stream

3. **notify_worker.py** (`telegram-worker/`)
   - Consumer из `notify:telegram`
   - Отправка в Telegram через бота

### Flow

```
ReasonMismatchMonitor.observe_legacy_map()
    ↓
emit_legacy_alert() → Redis notify:telegram
    ↓  
notify_worker.py → Telegram бот
```

## Диагностика

### Проверка метрик
```bash
# Prometheus query
rate(reason_legacy_mapped_total[5m])
```

### Проверка алертов
```bash
# Redis CLI
XREAD COUNT 10 STREAMS notify:telegram 0
```

### Логи
```
INFO - Legacy alert sent to notify:telegram: Legacy alert: Всплеск...
INFO - Отчет отправлен (notify_worker)
```

## Troubleshooting

### Алерт не приходит
1. Проверить метрики `reason_legacy_mapped_total`
2. Проверить пороги `REASON_LEGACY_ALERT_*`
3. Проверить `notify:telegram` stream
4. Проверить логи `notify_worker.py`

### Слишком много алертов
1. Увеличить `REASON_LEGACY_ALERT_COOLDOWN_S`
2. Увеличить `REASON_LEGACY_ALERT_MIN_EVENTS`

### Алерт приходит, но текст кривой
1. Проверить `hint` в payload от `ReasonMismatchMonitor`
2. Проверить логику в `emit_legacy_alert`

## Безопасность

- **Fail-open**: Ошибки в алертинге не влияют на торговую логику
- **Rate limiting**: Cooldown предотвращает спам
- **Configurable**: Все параметры через environment variables
