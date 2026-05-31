# Auto Calibration Service

Автоматическая калибровка параметров `stop_atr_mult` и `rr_levels` каждые N сделок.

## Описание

Система автоматически анализирует закрытые сделки и калибрует параметры торговли на основе эмпирических данных. Калибровка запускается автоматически после каждых 100 сделок (по умолчанию).

## Алгоритм калибровки

### stop_atr_mult
1. Для каждой сделки рассчитывается `mae_atr_ratio = |mae_price - entry_price| / atr`
2. Строится распределение mae_atr_ratio по последним сделкам
3. `stop_atr_mult = 75-й перцентиль mae_atr_ratio` (ограничено диапазоном 0.5-2.0)

### rr_levels
1. Рассчитывается `mfe_r = mfe_pnl / one_r_money` для каждой сделки
2. Строится распределение mfe_r
3. `rr_levels` выбираются на основе медианы, 75-го и 90-го перцентилей

## Конфигурация

### Переменные окружения

```bash
# Порог запуска калибровки (каждые N сделок)
AUTO_CALIBRATION_THRESHOLD=100

# Символы для калибровки (через запятую)
AUTO_CALIBRATION_SYMBOLS=BTCUSDT,ETHUSDT

# Source сделок для анализа
AUTO_CALIBRATION_SOURCE=CryptoOrderFlow
```

### Параметры по умолчанию

- **trades_threshold**: 100 сделок
- **enabled_symbols**: ["BTCUSDT", "ETHUSDT"]
- **source**: "CryptoOrderFlow"
- **min_trades_for_calibration**: 50 сделок минимум

## Архитектура

### Компоненты

1. **AutoCalibrationService** - основной сервис автокалибровки
2. **Analytics DB Integration** - интеграция в процесс сохранения сделок
3. **Redis Storage** - хранение счетчиков и результатов калибровки
4. **Async Processing** - асинхронный запуск калибровки

### Поток работы

```
Закрытие сделки → save_trade_closed() → increment_trade_counter()
    ↓
if counter >= threshold → run_calibration_async()
    ↓
load_calibration_data() → calibrate_stop_atr_mult() → calibrate_rr_levels()
    ↓
update_symbol_spec() → reset_trade_counter()
```

## Мониторинг

### Проверка статуса

```bash
cd python-worker/scripts
python check_auto_calibration_status.py
```

### Redis ключи

- `auto_calibration:trade_counter:{symbol}` - счетчик сделок
- `auto_calibration:last_run` - время последней калибровки
- `symbol_specs:{symbol}` - обновленные параметры

### Логи

Логи калибровки пишутся с префиксом `AutoCalibrationService`:

```
INFO AutoCalibrationService: Trade counter for BTCUSDT: 45
INFO AutoCalibrationService: Trade threshold reached for BTCUSDT (100 trades), starting calibration
INFO AutoCalibrationService: Starting auto-calibration for BTCUSDT
INFO AutoCalibrationService: Calibration updated for BTCUSDT: stop_atr_mult=1.250, rr_levels=[1.0, 2.0, 3.0]
```

## Безопасность

- **Thread Safety**: Все операции с Redis защищены блокировками
- **Error Handling**: Ошибки калибровки не влияют на основной поток торговли
- **Fallback**: При недоступности данных используются значения по умолчанию
- **Validation**: Параметры валидируются перед применением

## Примеры

### BTCUSDT после 100 сделок
```
MAE_ATR_RATIO - медиана: 0.85
MAE_ATR_RATIO - 75-й перц: 1.25
MAE_ATR_RATIO - 90-й перц: 1.85

✅ Рекомендуемый STOP_ATR_MULT: 1.25
```

### ETHUSDT после 100 сделок
```
MAE_ATR_RATIO - медиана: 0.65
MAE_ATR_RATIO - 75-й перц: 1.05
MAE_ATR_RATIO - 90-й перц: 1.45

✅ Рекомендуемый STOP_ATR_MULT: 1.05
```

## Тестирование

### Ручной запуск калибровки

```bash
cd python-worker/tools
python calibrate_stop_rr_levels.py --dsn "postgresql://..." --source CryptoOrderFlow --symbol BTCUSDT --limit 500
```

### Проверка счетчиков

```python
from services.auto_calibration_service import get_auto_calibration_service

service = get_auto_calibration_service()
print(service.get_trade_count("BTCUSDT"))  # Текущий счетчик
```

## Troubleshooting

### Проблема: Калибровка не запускается
**Решение**: Проверьте счетчики через `check_auto_calibration_status.py`

### Проблема: Ошибка подключения к БД
**Решение**: Убедитесь что `TRADES_DB_DSN` правильно настроена

### Проблема: Параметры не обновляются в Redis
**Решение**: Проверьте логи на ошибки и права доступа к Redis

## Расширение

### Добавление новых символов

```python
from services.auto_calibration_service import init_auto_calibration

init_auto_calibration(
    enabled_symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    trades_threshold=50  # Частая калибровка для новых символов
)
```

### Кастомная логика калибровки

Наследуйтесь от `AutoCalibrationService` и переопределите `_run_calibration_sync()`.
