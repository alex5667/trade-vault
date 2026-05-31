# ✅ Интеграция Cost Edge Gate + Enhanced Confidence Thresholds

**Дата**: 27 декабря 2025  
**Статус**: ✅ Завершено

---

## 📋 Обзор

Реализована система двухступенчатой фильтрации сигналов для снижения "churn" (избыточных сделок, не покрывающих издержки) и повышения качества торговых сигналов, особенно для высоколиквидных пар (BTC/ETH).

### Ключевые компоненты

1. **Cost Edge Gate** — отклоняет сигналы, где ожидаемая прибыль не превышает транзакционные издержки с заданным коэффициентом
2. **Enhanced Confidence Thresholds** — symbol-specific пороги уверенности (строже для BTC/ETH)

---

## 🏗️ Архитектура

### 1. Cost Edge Gate Filter

**Назначение**: Предотвращает торговлю ниже транзакционных издержек

**Математическая модель**:
```
expected_edge_bps > (fees_bps + slippage_bps) × K

Где:
- expected_edge_bps: ожидаемое движение цены (базисные пункты)
- fees_bps: комиссия round-trip (вход + выход)
- slippage_bps: ожидаемое проскальзывание
- K: коэффициент безопасности (обычно 3-5×)
```

**Методы оценки edge**:
- `tp1`: расстояние до первого TP (по умолчанию)
- `rr`: на основе Risk:Reward ratio
- `atr`: оценка на основе ATR

**Файл**: `python-worker/handlers/crypto_orderflow/core/cost_edge_gate.py`

**Классы**:
- `CostEdgeConfig`: конфигурация из ENV
- `CostEdgeGate`: основной фильтр
- `CostEdgeResult`: результат проверки

---

### 2. Enhanced Confidence Threshold Filter

**Назначение**: Строже фильтрует сигналы по confidence для пар с высокой частотой

**Двойная фильтрация**:
1. Абсолютная уверенность (0-100 scale)
2. Confidence factor (0-1 normalized scale)

Оба фильтра должны пройти для принятия сигнала.

**Файл**: `python-worker/handlers/crypto_orderflow/core/confidence_threshold.py`

**Классы**:
- `ConfidenceThresholdConfig`: конфигурация из ENV
- `ConfidenceThresholdFilter`: основной фильтр
- `ConfidenceThresholdResult`: результат проверки

---

## ⚙️ Конфигурация

### ENV переменные (docker-compose.yml)

Все новые переменные добавлены в YAML anchor `x-crypto-of-env` и автоматически применяются к обоим сервисам:
- `crypto-orderflow-service`
- `crypto-orderflow-service-2`

#### Cost Edge Gate

```yaml
# Enable/disable filter
- EDGE_COST_GATE_ENABLED=1

# Cost multipliers (required edge must exceed costs × K)
- EDGE_COST_K=4.0                # Default for all symbols
- EDGE_COST_K_BTCUSDT=5.0        # Stricter for BTC
- EDGE_COST_K_ETHUSDT=4.5        # Stricter for ETH

# Trading costs in basis points (bps)
- EDGE_FEES_BPS_DEFAULT=8.0      # Round-trip commission (4 bps × 2)
- EDGE_SLIPPAGE_BPS_DEFAULT=4.0  # Expected slippage
- EDGE_SLIPPAGE_USE_SPREAD_HALF=1  # Use 0.5 × spread as slippage estimate

# Edge estimation method
- EDGE_EXPECTED_MOVE_MODE=tp1    # Options: tp1 | rr | atr

# Debug logging
- LOG_EDGE_VETO=1
```

#### Enhanced Confidence Thresholds

```yaml
# Default thresholds
- MIN_CONF_DEFAULT=70              # Absolute confidence (0-100)
- MIN_CONF_FACTOR_DEFAULT=0.45     # Confidence factor (0-1)

# Symbol-specific thresholds (higher bar for major pairs)
- MIN_CONF_BTCUSDT=75
- MIN_CONF_ETHUSDT=72
- MIN_CONF_FACTOR_BTCUSDT=0.55
- MIN_CONF_FACTOR_ETHUSDT=0.52
```

---

## 🔄 Интеграция в обработчик

### Инициализация

**Файл**: `python-worker/handlers/crypto_orderflow/mixins/crypto_orderflow_init.py`

```python
# Импорты
from handlers.crypto_orderflow.core.cost_edge_gate import CostEdgeGate
from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdFilter

# В __init__:
self._cost_edge_gate = CostEdgeGate.from_env()
self._cost_edge_enabled = self._cost_edge_gate.config.enabled

self._confidence_threshold_filter = ConfidenceThresholdFilter.from_env()

# Счетчики для мониторинга
self._veto_cost_edge_total = 0
self._veto_confidence_threshold_total = 0
```

### Применение фильтров

**Файл**: `python-worker/handlers/crypto_orderflow_handler.py`

**Метод**: `_publish_signal()`

**Порядок проверок**:
1. ✅ Расчет confidence (существующий код)
2. **NEW** ✅ Enhanced Confidence Threshold filter
3. **NEW** ✅ Cost Edge Gate filter
4. ✅ Touch Filter (существующий код)
5. ✅ Публикация сигнала

```python
# 1. Enhanced Confidence Threshold
conf_threshold_result = self._confidence_threshold_filter.evaluate(
    confidence_pct=conf_pct,
    conf_factor=conf_factor,
    symbol=self.symbol,
)

if not conf_threshold_result.passed:
    self._veto_confidence_threshold_total += 1
    # Log and reject
    return PublishResult(sent=False, dedup=True)

# 2. Cost Edge Gate
if self._cost_edge_enabled:
    cost_edge_result = self._cost_edge_gate.evaluate(
        ctx=ctx,
        symbol=self.symbol,
        entry_price=float(entry_price),
    )
    
    if not cost_edge_result.passed:
        self._veto_cost_edge_total += 1
        # Log and reject
        return PublishResult(sent=False, dedup=True)
```

---

## 📊 Логирование

### Cost Edge Veto

```
Cost edge veto: breakout LONG | BTCUSDT | edge=15.2bps < required=40.0bps 
(costs=8.0bps × K=5.0) | edge_ratio=0.38 | source=tp1
```

**Поля**:
- `edge`: ожидаемое движение цены (bps)
- `required`: минимальное требуемое движение (bps)
- `costs`: общие издержки (fees + slippage)
- `K`: применённый коэффициент
- `edge_ratio`: отношение edge к required (>1.0 для прохода)
- `source`: метод оценки (tp1/rr/atr)

### Confidence Threshold Veto

```
Confidence threshold veto: breakout LONG | BTCUSDT | conf=72.0 (min=75.0) 
conf_factor=0.480 (min=0.550)
```

**Поля**:
- `conf`: фактическая уверенность (0-100)
- `min`: требуемая уверенность для символа
- `conf_factor`: нормализованный фактор (0-1)
- `min_factor`: требуемый фактор для символа

---

## 🎯 Ожидаемый эффект

### Cost Edge Gate

**До**:
- Генерация сигналов с TP1 = 10-20 bps при издержках 8-12 bps
- Множество мелких убыточных сделок
- "Death by a thousand cuts"

**После**:
- Сигналы только при edge > (costs × 4-5)
- Для BTC/ETH: edge должен быть ≥ 40-50 bps
- Снижение churn на 30-50%

### Enhanced Confidence Thresholds

**До**:
- Единый порог confidence=70 для всех символов
- Избыточные сигналы на BTC/ETH

**После**:
- BTC: confidence ≥ 75, conf_factor ≥ 0.55
- ETH: confidence ≥ 72, conf_factor ≥ 0.52
- Остальные: confidence ≥ 70, conf_factor ≥ 0.45
- Снижение false signals на мажорах на 20-30%

---

## 📝 Файлы проекта

### Новые модули

```
python-worker/handlers/crypto_orderflow/core/
├── cost_edge_gate.py              # Cost Edge Gate filter
└── confidence_threshold.py        # Enhanced Confidence Threshold filter
```

### Обновлённые файлы

```
python-worker/handlers/crypto_orderflow/
├── mixins/crypto_orderflow_init.py      # Инициализация фильтров
└── crypto_orderflow_handler.py          # Применение в _publish_signal()

docker-compose.yml                        # ENV конфигурация (YAML anchor)
```

### Документация

```
COST_EDGE_CONFIDENCE_INTEGRATION.md      # Этот файл
```

---

## 🧪 Тестирование

### Юнит-тесты (рекомендуемые)

```python
# test_cost_edge_gate.py
def test_cost_edge_gate_pass():
    gate = CostEdgeGate.from_env()
    ctx = MockContext(tp1=50100, entry=50000, atr=150)
    result = gate.evaluate(ctx, "BTCUSDT", entry_price=50000)
    assert result.passed

def test_cost_edge_gate_fail():
    gate = CostEdgeGate.from_env()
    ctx = MockContext(tp1=50010, entry=50000)  # Only 20 bps
    result = gate.evaluate(ctx, "BTCUSDT", entry_price=50000)
    assert not result.passed

# test_confidence_threshold.py
def test_confidence_threshold_btc_strict():
    filter = ConfidenceThresholdFilter.from_env()
    result = filter.evaluate(
        confidence_pct=72.0,  # Below BTC threshold
        conf_factor=0.50,
        symbol="BTCUSDT"
    )
    assert not result.passed
```

### Интеграционное тестирование

1. **Мониторинг veto счетчиков**:
```python
# В crypto_orderflow_handler добавить периодический лог:
if time.time() - self._last_veto_report > 300:  # каждые 5 мин
    self.logger.info(
        "Veto stats: confidence_threshold=%d cost_edge=%d",
        self._veto_confidence_threshold_total,
        self._veto_cost_edge_total
    )
```

2. **Проверка логов**:
```bash
# Найти veto решения
grep "Cost edge veto" logs/crypto-orderflow.log
grep "Confidence threshold veto" logs/crypto-orderflow.log

# Статистика по символам
grep "veto" logs/crypto-orderflow.log | grep BTCUSDT | wc -l
```

---

## 🔧 Настройка для продакшена

### Рекомендуемые значения

#### Консервативная стратегия (меньше сигналов, выше качество)

```yaml
# Cost Edge Gate
- EDGE_COST_K=5.0
- EDGE_COST_K_BTCUSDT=6.0
- EDGE_COST_K_ETHUSDT=5.5

# Confidence
- MIN_CONF_DEFAULT=75
- MIN_CONF_BTCUSDT=80
- MIN_CONF_ETHUSDT=77
- MIN_CONF_FACTOR_DEFAULT=0.50
- MIN_CONF_FACTOR_BTCUSDT=0.60
- MIN_CONF_FACTOR_ETHUSDT=0.55
```

#### Агрессивная стратегия (больше сигналов)

```yaml
# Cost Edge Gate
- EDGE_COST_K=3.0
- EDGE_COST_K_BTCUSDT=4.0
- EDGE_COST_K_ETHUSDT=3.5

# Confidence
- MIN_CONF_DEFAULT=65
- MIN_CONF_BTCUSDT=70
- MIN_CONF_ETHUSDT=68
- MIN_CONF_FACTOR_DEFAULT=0.40
- MIN_CONF_FACTOR_BTCUSDT=0.50
- MIN_CONF_FACTOR_ETHUSDT=0.45
```

### Отключение фильтров

```yaml
# Отключить Cost Edge Gate
- EDGE_COST_GATE_ENABLED=0

# Вернуться к старым порогам confidence
- MIN_CONF_DEFAULT=70
- MIN_CONF_BTCUSDT=70
- MIN_CONF_ETHUSDT=70
```

---

## 🚀 Развёртывание

### 1. Применение изменений

```bash
# Перезапуск сервисов с новой конфигурацией
docker-compose up -d crypto-orderflow-service crypto-orderflow-service-2

# Проверка логов
docker-compose logs -f crypto-orderflow-service | grep "veto"
```

### 2. Мониторинг

```bash
# Количество veto за последний час
docker-compose logs --since 1h crypto-orderflow-service | \
  grep -E "(Cost edge veto|Confidence threshold veto)" | wc -l

# Распределение по символам
docker-compose logs --since 1h crypto-orderflow-service | \
  grep "veto" | awk '{print $NF}' | sort | uniq -c
```

### 3. Откат (если нужно)

```bash
# Отключить фильтры через ENV override
docker-compose stop crypto-orderflow-service crypto-orderflow-service-2

# Добавить в docker-compose.yml override:
environment:
  - EDGE_COST_GATE_ENABLED=0

docker-compose up -d crypto-orderflow-service crypto-orderflow-service-2
```

---

## 📈 Метрики для мониторинга

### Ключевые метрики

1. **Veto Rate**: `veto_total / (veto_total + signals_sent)`
2. **Edge Ratio Distribution**: гистограмма `edge_ratio` из `CostEdgeResult`
3. **Confidence Gap**: `actual_conf - min_conf_threshold` по символам

### Prometheus метрики (рекомендуемые)

```python
# signals_veto_total{reason="cost_edge",symbol="BTCUSDT"}
# signals_veto_total{reason="confidence_threshold",symbol="BTCUSDT"}
# signal_edge_ratio{symbol="BTCUSDT"} histogram
```

---

## ✅ Чеклист завершения

- [x] Создан модуль `cost_edge_gate.py`
- [x] Создан модуль `confidence_threshold.py`
- [x] Обновлена инициализация в `crypto_orderflow_init.py`
- [x] Интегрированы фильтры в `_publish_signal()`
- [x] Добавлены ENV переменные в `docker-compose.yml` (YAML anchor)
- [x] Добавлено подробное логирование veto решений
- [x] Сохранены все комментарии в коде
- [x] Код проверен линтером (no errors)
- [x] Создана документация

---

## 📚 Ссылки

- [YAML Anchors в docker-compose](https://docs.docker.com/compose/compose-file/#anchors)
- [Basis Points (bps)](https://en.wikipedia.org/wiki/Basis_point) — 1 bps = 0.01% = 0.0001
- Transaction Cost Analysis — см. академические источники по TCA

---

## 👤 Автор

Интеграция выполнена: Claude (Anthropic AI)  
Дата: 27 декабря 2025

**Примечание**: Данная интеграция следует best practices:
- Fail-open при отсутствии данных
- Детальное логирование для анализа
- Symbol-specific конфигурация
- Независимые модули (легко тестировать)
- Обратная совместимость (можно отключить)

