# Order Flow Worker

Order Flow анализ для торговых свечей с автоматическим детектированием спайков.

## Быстрый старт

Order Flow worker автоматически запускается вместе с python-worker. Никаких дополнительных действий не требуется.

```bash
# Запуск через docker-compose
docker-compose up python-worker

# Логи Order Flow worker
docker logs -f python-worker | grep OrderFlow
```

## Что делает Order Flow Worker?

1. **Читает закрытые свечи** из `stream:kline_1m`
2. **Вычисляет метрики**:
   - Delta (покупки - продажи)
   - CVD (кумулятивная delta)
   - z-score Delta
   - Delta Ratio
   - Body ATR
   - Absorbed (длинные тени)
3. **Публикует данные** в Redis Streams:
   - `stream:of-bar` — все бары
   - `stream:of-spike` — только спайки

## Формат данных

### OF Bar

```json
{
	"type": "of_bar",
	"symbol": "BTCUSDT",
	"timeframe": "1m",
	"ts": 1697123400000,
	"o": 28500.0,
	"h": 28550.0,
	"l": 28480.0,
	"c": 28520.0,
	"volume": 125.5,
	"buyVol": 75.3,
	"sellVol": 50.2,
	"delta": 25.1,
	"cvd": 450.7,
	"deltaRatio": 0.2,
	"zDelta": 1.85,
	"bodyATR": 0.25,
	"absorbed": false,
	"windowN": 300
}
```

### OF Spike

То же самое + дополнительные поля:

```json
{
  ...
  "type": "of_spike",
  "direction": "long"  // или "short"
}
```

## Настройка параметров

Через переменные окружения в `docker-compose.yml`:

```yaml
environment:
  # Размер окна для статистики
  OF_WINDOW_BARS: 300

  # Пороги детектирования (нормальный режим)
  OF_Z_THRESHOLD: 2.5
  OF_RATIO_THRESHOLD: 0.35
  OF_MIN_VOLUME_Q: 50
  OF_MIN_BODY_ATR: 0.15

  # Пороги для прокси-режима (строже)
  OF_Z_THRESHOLD_PROXY: 3.0
  OF_RATIO_THRESHOLD_PROXY: 0.45
  OF_MIN_VOLUME_Q_PROXY: 60
```

## Примеры использования

### Python

```python
from of.example_usage import OrderFlowMonitor

monitor = OrderFlowMonitor()

# Подписка на спайки
def on_spike(data):
    print(f"Spike: {data['symbol']} {data['direction']}")

monitor.subscribe_to_spikes(on_spike)

# Или получить последние бары
bars = monitor.get_recent_bars(count=100)
```

### Redis CLI

```bash
# Чтение спайков
redis-cli -p 6380 XREAD COUNT 10 STREAMS stream:of-spike 0

# Чтение баров
redis-cli -p 6380 XREVRANGE stream:of-bar + - COUNT 100
```

## Мониторинг

Worker выводит статистику каждую минуту:

```
📊 OrderFlow Stats: Processed=1250, Spikes=45, Detectors=125
```

При детектировании спайка:

```
🎯 OF Spike detected: BTCUSDT 1m long (z=3.25, ratio=0.47)
```

## Файлы

- `candle_of_worker.py` — основной worker
- `example_usage.py` — примеры использования
- `README.md` — это руководство

## Дополнительная документация

См. [ORDER_FLOW_INTEGRATION.md](../../ORDER_FLOW_INTEGRATION.md) в корне проекта для полной документации.

## Troubleshooting

### Worker не запускается

Проверьте логи:

```bash
docker logs python-worker | grep -E "(OrderFlow|Error)"
```

### Нет спайков

1. Проверьте пороги в `docker-compose.yml`
2. Убедитесь, что приходят закрытые свечи
3. Понизьте `OF_Z_THRESHOLD` для тестирования

### Проблемы с ATR

ATR берётся из Redis кэша `atr:{symbol}:{timeframe}`.
Если кэш пуст, используется локальный расчёт (warmup: 14 свечей).

## Тестирование

Запустите тесты интеграции:

```bash
python test_of_integration.py
```

Все тесты должны пройти успешно.
