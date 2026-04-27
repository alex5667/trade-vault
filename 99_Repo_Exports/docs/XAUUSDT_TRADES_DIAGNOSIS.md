# Диагностика: Почему не открываются сделки для XAUUSDT

## Проблема

Нет сделок для XAUUSDT с `source=CryptoOrderFlow`:
- `found_trades=0`
- `filtered_wins_all=0`
- `filtered_wins_trailing=0`
- `need_trades=40`, `need_wins~=15`

## Архитектура потока сигналов

```
CryptoOrderflowService
  ↓ (обрабатывает тики из stream:tick_XAUUSDT)
  ↓ (генерирует сигналы через OrderFlowStrategy)
  ↓ (публикует в signals:crypto:raw)
TradeMonitorService
  ↓ (читает из signals:*)
  ↓ (проверяет confidence threshold)
  ↓ (проверяет ML confirm gate)
  ↓ (открывает позицию)
```

## Возможные причины

### 1. Сигналы не генерируются

**Проверка:**
- XAUUSDT должен быть в `ORDERFLOW_SYMBOLS` в docker-compose
- CryptoOrderflowService должен обрабатывать XAUUSDT
- Тики должны приходить в `stream:tick_XAUUSDT`

**Диагностика:**
```bash
# Проверить наличие тиков
redis-cli XLEN stream:tick_XAUUSDT

# Проверить конфигурацию
redis-cli HGETALL config:orderflow:XAUUSDT

# Проверить логи CryptoOrderflowService
docker logs scanner_infra_crypto-orderflow-service_1 | grep XAUUSDT
```

### 2. Сигналы блокируются confidence threshold

**Проблема:**
- TradeMonitorService фильтрует сигналы с `confidence < CRYPTO_SIGNAL_MIN_CONF`
- По умолчанию: `CRYPTO_SIGNAL_MIN_CONF=70` (70%)

**Проверка:**
```python
# В TradeMonitorService.on_signal()
sig_conf = float(sig.payload.get("confidence") or sig.payload.get("conf") or 0.0)
if sig_conf < (self.shadow_conf_threshold / 100.0):  # 0.70
    return None  # Сигнал игнорируется
```

**Решение:**
- Снизить `CRYPTO_SIGNAL_MIN_CONF` для XAUUSDT
- Или увеличить confidence в сигналах CryptoOrderFlow

### 3. Сигналы блокируются ML confirm gate

**Проблема:**
- ML confirm gate может блокировать сигналы в режиме `ENFORCE`
- Проверяется в `ml_confirm_gate.py` через `MLConfirmGate.check()`

**Проверка:**
```bash
# Проверить режим ML gate
redis-cli GET cfg:ml_confirm:champion | jq .mode

# Проверить метрики блокировок
redis-cli XREVRANGE metrics:ml_confirm COUNT 100 | grep XAUUSDT
```

**Решение:**
- Если `ML_CONFIRM_MODE=ENFORCE` и модель блокирует → переключить на `SHADOW`
- Или настроить модель для XAUUSDT

### 4. Сигналы не доходят до TradeMonitorService

**Проблема:**
- TradeMonitorService читает из streams по паттерну `signals:*`
- CryptoOrderflowService публикует в `signals:crypto:raw`

**Проверка:**
```bash
# Проверить наличие сигналов в stream
redis-cli XLEN signals:crypto:raw

# Проверить последние сигналы для XAUUSDT
redis-cli XREVRANGE signals:crypto:raw COUNT 100 | grep XAUUSDT
```

## Диагностический скрипт

Запустите скрипт для автоматической диагностики:

```bash
cd /home/alex/front/trade/scanner_infra
python3 python-worker/tools/diagnose_xauusdt_signals.py
```

Скрипт проверяет:
1. ✅ Наличие streams
2. ✅ Наличие сигналов для XAUUSDT
3. ✅ Confidence сигналов
4. ✅ Блокировки ML confirm gate
5. ✅ Другие проблемы с сигналами

## Рекомендации по исправлению

### Шаг 1: Проверить генерацию сигналов

```bash
# 1. Проверить, что XAUUSDT обрабатывается
docker logs scanner_infra_crypto-orderflow-service_1 | grep -i "XAUUSDT\|symbols"

# 2. Проверить наличие тиков
redis-cli XLEN stream:tick_XAUUSDT

# 3. Проверить конфигурацию
redis-cli HGETALL config:orderflow:XAUUSDT
```

### Шаг 2: Проверить фильтры

```bash
# Запустить диагностический скрипт
python3 python-worker/tools/diagnose_xauusdt_signals.py
```

### Шаг 3: Настроить confidence threshold

Если сигналы блокируются из-за низкой confidence:

```bash
# В docker-compose-python-workers.yml для trade-monitor
- CRYPTO_SIGNAL_MIN_CONF=60  # Снизить с 70 до 60
```

Или для конкретного символа (требует изменения кода):
- Добавить символ-специфичный threshold в TradeMonitorService

### Шаг 4: Проверить ML confirm gate

```bash
# Проверить режим
redis-cli GET cfg:ml_confirm:champion | jq .mode

# Если ENFORCE и блокирует → переключить на SHADOW
redis-cli SET cfg:ml_confirm:champion '{"mode":"SHADOW",...}'
```

## Ключевые файлы

1. **Генерация сигналов:**
   - `python-worker/services/crypto_orderflow_service.py` - основной сервис
   - `python-worker/services/orderflow/strategy.py` - стратегия генерации
   - `python-worker/services/orderflow/signal_pipeline.py` - публикация

2. **Обработка сигналов:**
   - `python-worker/services/trade_monitor.py` - открытие позиций
   - `python-worker/runners/trade_monitor_runner.py` - runner
   - `python-worker/services/ml_confirm_gate.py` - ML gate

3. **Конфигурация:**
   - `docker-compose-crypto-orderflow.yml` - символы для обработки
   - `docker-compose-python-workers.yml` - настройки TradeMonitorService

## Следующие шаги

1. ✅ Запустить диагностический скрипт
2. ⏳ Проверить логи CryptoOrderflowService
3. ⏳ Проверить наличие тиков для XAUUSDT
4. ⏳ Проверить конфигурацию orderflow для XAUUSDT
5. ⏳ Настроить фильтры при необходимости

