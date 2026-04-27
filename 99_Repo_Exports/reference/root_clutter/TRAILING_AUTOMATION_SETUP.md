# Настройка автоматического трейлинга

## Обзор

Система автоматически анализирует историю сделок и корректирует параметры трейлинг-стопа для каждого символа на основе данных из Redis stream `trades:closed`.

## Архитектура

### 1. Модуль анализа (`services/trailing_size_recommender.py`)
- Анализирует выигрышные сделки по MFE (Maximum Favorable Excursion)
- Рассчитывает оптимальный `lock_r` (сколько R "залочить" после TP1)
- Конвертирует в `TRAILING_TP1_OFFSET_ATR` через `stop_atr_mult`

### 2. Скрипт рекомендаций (`tools/recommend_trailing_from_redis.py`)
- Читает сделки из Redis
- Запускает анализ для каждого символа
- Автоматически записывает рекомендации в `symbol:trailing_cfg:{SYMBOL}`

### 3. Интеграция в спецификации (`python-worker/services/pnl_math.py`)
- Функция `get_symbol_info()` подмешивает trailing-конфигурацию
- Функция `spec_from_symbol_info()` применяет настройки к SymbolSpec

## Установка

### 1. Запуск скрипта вручную

```bash
cd /home/alex/front/trade/scanner_infra

# Анализ и автозапись
TRAILING_AUTOTUNE_ENABLED=true python3 tools/recommend_trailing_from_redis.py \
  --source CryptoOrderFlow \
  --symbols ETHUSDT,BTCUSDT \
  --auto-write \
  --conf-threshold 0.6
```

### 2. Автоматический запуск каждые 6 часов

```bash
# Установка systemd timer
sudo ./install_trailing_timer.sh

# Проверка статуса
systemctl status trailing-recommender.timer
journalctl -u trailing-recommender.service
```

## Параметры

### Основные параметры скрипта

- `--source`: Источник сделок (CryptoOrderFlow)
- `--symbols`: Символы через запятую (ETHUSDT,BTCUSDT)
- `--auto-write`: Включить автозапись рекомендаций
- `--conf-threshold`: Минимальная уверенность для применения (0.6)
- `--min-trades`: Минимальное количество сделок (50)

### Переменные окружения

```bash
TRAILING_AUTOTUNE_ENABLED=true          # Включить автозапись
TRAILING_AUTOTUNE_SOURCE=CryptoOrderFlow # Источник
TRAILING_AUTOTUNE_SYMBOLS=ETHUSDT,BTCUSDT # Символы
TRAILING_AUTOTUNE_LIMIT=2000             # Лимит сделок
TRAILING_AUTOTUNE_MIN_TRADES=50          # Мин. сделок
TRAILING_AUTOTUNE_MFE_QUANTILE=0.25      # Квантиль MFE
TRAILING_AUTOTUNE_CONF_THRESHOLD=0.6     # Порог уверенности
```

## Формат хранения в Redis

Рекомендации хранятся в хэшах `symbol:trailing_cfg:{SYMBOL}`:

```redis
HGETALL symbol:trailing_cfg:ETHUSDT
```

Возвращает:
```
tp1_offset_atr: "0.650000"           # Основная рекомендация
lock_r: "0.650000"                   # В R
confidence: "0.782300"               # Уверенность
stop_atr_mult: "1.000000"            # ATR множитель для SL
trailing_after_tp1_enabled: "true"   # Включен ли трейлинг
updated_at_ms: "1766205782436"       # Время обновления

# Диагностика
all_tp1_offset_atr: "0.650000"       # Из всех сделок
all_confidence: "0.782300"
all_sample_size: "245"               # Всего сделок
all_wins_count: "189"                # Выигрышных
```

## Логика принятия решений

### Расчет lock_r

1. **MFE_R** = `mfe_pnl / one_r_money` - максимальная прибыль в R
2. **Lock_R** = 25-й перцентиль MFE_R (75% сделок имеют MFE выше)
3. **Кэп** по медиане реализованного R (не завышать)
4. **Клип** в диапазоне 0.05R - 1.0R

### Конвертация в ATR

```
TRAILING_TP1_OFFSET_ATR = lock_r * stop_atr_mult
```

### Выбор финальной рекомендации

1. Если есть trailing-рекомендация с confidence >= threshold → её
2. Иначе, если есть общая рекомендация с confidence >= threshold → её
3. Иначе не применять изменения

## Мониторинг

### Проверка статуса

```bash
# Статус таймера
systemctl status trailing-recommender.timer

# Логи последнего запуска
journalctl -u trailing-recommender.service -n 50

# Проверка рекомендаций в Redis
redis-cli HGETALL symbol:trailing_cfg:ETHUSDT
```

### Ручной запуск

```bash
# Тестовый запуск
sudo systemctl start trailing-recommender.service

# Принудительный запуск таймера
sudo systemctl start trailing-recommender.timer
```

## Отладка

### Тестирование с синтетическими данными

```bash
# Создание тестовых данных
python3 test_data.py

# Запуск анализа
python3 tools/recommend_trailing_from_redis.py \
  --source CryptoOrderFlow \
  --symbols ETHUSDT \
  --min-trades 3 \
  --auto-write
```

### Проверка интеграции

```python
# В Python
from python_worker.services.pnl_math import get_symbol_info
info = get_symbol_info("ETHUSDT")
print(info.get("trailing_tp1_offset_atr"))  # Должно показать рекомендацию
```

## Безопасность

- Автозапись включается только с `--auto-write` или `TRAILING_AUTOTUNE_ENABLED=true`
- Порог уверенности предотвращает применение ненадежных рекомендаций
- Все изменения логируются в systemd journal

## Расширение

### Добавление новых символов

```bash
# Обновить таймер
vim trailing-recommender.service
# Изменить --symbols ETHUSDT,BTCUSDT,SOLUSDT

sudo systemctl daemon-reload
sudo systemctl restart trailing-recommender.timer
```

### Настройка разных источников

```bash
# Для разных источников
python3 tools/recommend_trailing_from_redis.py \
  --source ForexOrderFlow \
  --symbols EURUSD,GBPUSD \
  --auto-write
```
