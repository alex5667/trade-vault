# 🔧 Crypto OrderFlow Handler - Critical Fixes

## 🎯 Исправленные критичные риски

### ✅ РИСК №1: Фантомная дельта из пустых тиков (КРИТИЧНО)

**Проблема**:
```python
# ДО: Генерировалась дельта даже из bookTicker без объёма
def _classify_delta(self, tick: Tick) -> float:
    vol = float(tick.volume) if tick.volume and tick.volume > 0 else 1.0  # ❌ vol=1.0 для пустых тиков!
    side = self._taker_side(tick)
    if side in (-1, 1):
        return side * vol  # ❌ Возвращает ±1.0 для bookTicker
    return super()._classify_delta(tick)
```

**Последствия**:
- ❌ Искусственная дельта из bookTicker/квази-тиковых фидов без объёма
- ❌ Ложные Z-всплески (Z-score рассчитывается от фантомной дельты)
- ❌ Ложные сигналы (breakout/absorption/extreme на пустых данных)

**Исправление**:
```python
# ПОСЛЕ: Проверка is_trade перед генерацией дельты
def _classify_delta(self, tick: Tick) -> float:
    """
    Binance-style delta classification with is_buyer_maker support.
    
    ✅ КРИТИЧНО: Не генерировать дельту из "пустых" тиков (bookTicker без объёма).
    """
    # ✅ РИСК №1: Проверка is_trade (та же эвристика, что и в _update_micro_fast)
    is_trade = bool(tick.flags & 1) or bool(tick.last and tick.volume and tick.volume > 0)
    if not is_trade:
        return 0.0  # важно: не генерировать дельту из "пустых" тиков
    
    vol = float(tick.volume)
    if vol <= 0:
        return 0.0  # нет объёма = нет дельты
    
    side = self._taker_side(tick)
    if side in (-1, 1):
        return side * vol
    return super()._classify_delta(tick)
```

**Результат**:
- ✅ Дельта генерируется **только** из реальных сделок (trades)
- ✅ BookTicker без объёма возвращает `0.0`
- ✅ Нет ложных Z-всплесков
- ✅ Нет фантомных сигналов

---

### ✅ РИСК №2: Поле `is_buyer_maker` в Tick (ПРОВЕРЕНО)

**Проблема**:
```python
# Если в BaseOrderFlowHandler.Tick нет поля is_buyer_maker:
Tick(..., is_buyer_maker=ibm)  # ❌ TypeError: unexpected keyword argument
```

**Проверка**:
```python
# base_orderflow_handler.py
@dataclass
class Tick:
    ts: int
    bid: float
    ask: float
    last: float
    volume: float
    flags: int
    is_buyer_maker: Optional[bool] = None  # ✅ Поле есть!
```

**Результат**:
- ✅ Поле `is_buyer_maker` уже присутствует в `Tick`
- ✅ Никаких изменений не требуется
- ✅ CryptoOrderFlowHandler корректно использует это поле

---

### ✅ РИСК №3: KeyError в `_extend_outbox_envelope` (КРИТИЧНО)

**Проблема**:
```python
# ДО: Прямое обращение к envelope["meta"] и envelope["targets"]
def _extend_outbox_envelope(self, envelope: Dict[str, Any], signal: Signal, ctx: SignalContext) -> None:
    # ...
    envelope["meta"]["manual_stream"] = self.manual_signal_stream  # ❌ KeyError если "meta" не создан!
    envelope["targets"]["manual_payload"] = manual_payload          # ❌ KeyError если "targets" не создан!
```

**Последствия**:
- ❌ Падение handler при публикации сигнала
- ❌ Потеря сигналов
- ❌ Невозможность отправки в manual-signals stream

**Исправление**:
```python
# ПОСЛЕ: Безопасное добавление с setdefault
def _extend_outbox_envelope(self, envelope: Dict[str, Any], signal: Signal, ctx: SignalContext) -> None:
    # ...
    # ✅ РИСК №3: Безопасное добавление в envelope (избегаем KeyError)
    envelope.setdefault("meta", {})
    envelope.setdefault("targets", {})
    envelope["meta"]["manual_stream"] = self.manual_signal_stream
    envelope["targets"]["manual_payload"] = manual_payload
```

**Результат**:
- ✅ Нет KeyError при отсутствии "meta" или "targets"
- ✅ Envelope корректно расширяется
- ✅ Manual signals работают стабильно

---

## 📊 Сравнение До/После

### Риск №1: Фантомная дельта

| Сценарий | До исправления | После исправления |
|----------|----------------|-------------------|
| BookTicker (bid/ask, no volume) | ❌ Delta = ±1.0 | ✅ Delta = 0.0 |
| Trade (last, volume > 0) | ✅ Delta = ±volume | ✅ Delta = ±volume |
| Пустой тик (volume = 0) | ❌ Delta = ±1.0 | ✅ Delta = 0.0 |
| Z-score от bookTicker | ❌ Ложный всплеск | ✅ Корректный (0.0) |
| Сигналы от bookTicker | ❌ Фантомные | ✅ Нет сигналов |

### Риск №2: is_buyer_maker

| Проверка | Статус |
|----------|--------|
| Поле `is_buyer_maker` в Tick | ✅ Присутствует |
| Тип поля | ✅ `Optional[bool]` |
| Использование в CryptoOrderFlowHandler | ✅ Корректное |

### Риск №3: KeyError в envelope

| Сценарий | До исправления | После исправления |
|----------|----------------|-------------------|
| envelope без "meta" | ❌ KeyError | ✅ Создается автоматически |
| envelope без "targets" | ❌ KeyError | ✅ Создается автоматически |
| envelope с "meta" и "targets" | ✅ Работает | ✅ Работает |
| Публикация сигнала | ❌ Может упасть | ✅ Стабильно |

---

## 🔍 Детальный анализ

### Риск №1: Как возникала фантомная дельта

```python
# Пример: BookTicker тик (только bid/ask, нет last/volume)
tick = Tick(
    ts=1732881234567,
    bid=96500.50,
    ask=96501.00,
    last=0.0,        # ❌ Нет last price
    volume=0.0,      # ❌ Нет объёма
    flags=0,         # ❌ Не trade
    is_buyer_maker=None
)

# ДО исправления:
vol = 1.0  # ❌ Присваивается 1.0 для пустого тика!
side = _taker_side(tick)  # Возвращает ±1 на основе bid/ask
delta = side * vol  # ❌ Возвращает ±1.0 (фантомная дельта!)

# Delta window: [0.5, -0.3, 1.0, -1.0, 0.8, 1.0, ...]  # ❌ Много ±1.0 от bookTicker
# Z-score: 2.5  # ❌ Ложный всплеск!
# Сигнал: Breakout LONG  # ❌ Фантомный сигнал!

# ПОСЛЕ исправления:
is_trade = False  # ✅ flags=0, last=0, volume=0
delta = 0.0  # ✅ Возвращает 0.0 для bookTicker

# Delta window: [0.5, -0.3, 0.0, 0.0, 0.8, 0.0, ...]  # ✅ 0.0 от bookTicker
# Z-score: 0.8  # ✅ Корректный (нет ложного всплеска)
# Сигнал: Нет  # ✅ Нет фантомного сигнала
```

### Риск №3: Как возникал KeyError

```python
# Пример: BaseOrderFlowHandler создает envelope
envelope = {
    "sid": "sig_123",
    "ts": 1732881234567,
    # ❌ "meta" не создан
    # ❌ "targets" не создан
}

# ДО исправления:
envelope["meta"]["manual_stream"] = "stream:manual-signals"  # ❌ KeyError: 'meta'

# ПОСЛЕ исправления:
envelope.setdefault("meta", {})  # ✅ Создает {"meta": {}} если нет
envelope["meta"]["manual_stream"] = "stream:manual-signals"  # ✅ Работает!
```

---

## ✅ Статус исправлений

- ✅ **Риск №1**: Исправлен (`_classify_delta` проверяет `is_trade`)
- ✅ **Риск №2**: Проверен (поле `is_buyer_maker` присутствует)
- ✅ **Риск №3**: Исправлен (`_extend_outbox_envelope` использует `setdefault`)
- ✅ **Syntax OK**: Python compile успешен
- ✅ **Linter errors**: 0
- ✅ **Ready for Production** 🚀

---

## 🧪 Тестирование

### Проверка Риска №1 (фантомная дельта):

```python
# Тест 1: BookTicker (no volume)
tick = Tick(ts=1000, bid=100.0, ask=100.5, last=0.0, volume=0.0, flags=0, is_buyer_maker=None)
delta = handler._classify_delta(tick)
assert delta == 0.0, "BookTicker должен возвращать 0.0"

# Тест 2: Trade (with volume)
tick = Tick(ts=1000, bid=100.0, ask=100.5, last=100.5, volume=1.5, flags=1, is_buyer_maker=False)
delta = handler._classify_delta(tick)
assert delta == 1.5, "Trade должен возвращать +volume (taker buy)"

# Тест 3: Пустой тик (volume=0)
tick = Tick(ts=1000, bid=100.0, ask=100.5, last=100.2, volume=0.0, flags=0, is_buyer_maker=None)
delta = handler._classify_delta(tick)
assert delta == 0.0, "Пустой тик должен возвращать 0.0"
```

### Проверка Риска №3 (KeyError):

```python
# Тест 1: Envelope без "meta" и "targets"
envelope = {"sid": "sig_123", "ts": 1000}
handler._extend_outbox_envelope(envelope, signal, ctx)
assert "meta" in envelope, "meta должен быть создан"
assert "targets" in envelope, "targets должен быть создан"
assert envelope["meta"]["manual_stream"] == handler.manual_signal_stream

# Тест 2: Envelope с "meta" и "targets"
envelope = {"sid": "sig_123", "ts": 1000, "meta": {"foo": "bar"}, "targets": {"baz": "qux"}}
handler._extend_outbox_envelope(envelope, signal, ctx)
assert envelope["meta"]["foo"] == "bar", "Существующие поля должны сохраниться"
assert envelope["meta"]["manual_stream"] == handler.manual_signal_stream
```

---

## 📝 Рекомендации

### 1. Мониторинг фантомной дельты:

```bash
# Проверить delta window на наличие ±1.0 (подозрительно)
docker logs -f scanner_infra_crypto-orderflow-service_1 | grep "delta_bucket"

# Если видите много ±1.0, возможно bookTicker генерирует дельту
# После исправления должны видеть больше 0.0 для bookTicker
```

### 2. Проверка is_trade флага:

```python
# В логах handler можно добавить debug:
if not is_trade:
    self.logger.debug(f"Skipped non-trade tick: flags={tick.flags}, last={tick.last}, volume={tick.volume}")
```

### 3. Мониторинг envelope errors:

```bash
# Проверить логи на KeyError
docker logs scanner_infra_crypto-orderflow-service_1 | grep "KeyError"

# После исправления KeyError не должно быть
```

---

## 📚 Связанные документы

- `CRYPTO_L2_INTEGRATION_COMPLETE.md` - L2-метрики и фильтры
- `L2_INTEGRATION_COMPLETE.md` - Интеграция в BaseOrderFlowHandler
- `python-worker/handlers/crypto_orderflow_handler.py` - Обновленный handler
- `python-worker/handlers/base_orderflow_handler.py` - Базовый handler с `Tick`

---

**Дата**: 2025-11-29  
**Версия**: 1.0  
**Статус**: ✅ Critical Fixes Applied  
**Приоритет**: 🔴 HIGH (критичные исправления для production)

