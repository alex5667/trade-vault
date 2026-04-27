# Отчет: Обработка тиков XAUUSDT и фильтры, блокирующие сделки

**Дата:** 2026-02-06  
**Символ:** XAUUSDT  
**Компонент:** Go worker / Python worker / NestJS / Redis

---

## 📊 РЕЗЮМЕ

### ✅ Тики обрабатываются
- **Stream:** `stream:tick_XAUUSDT` настроен в конфигурации
- **Обработчик:** `CryptoOrderflowService` читает тики из Redis Stream
- **Конфигурация:** XAUUSDT в `DEFAULT_SYMBOLS` в `configuration.py` и `handler_factory.py`

### ❌ Фильтр, блокирующий сделки: **ATR Gate**

**Основная проблема:** ATR unified gate блокирует все сигналы для XAUUSDT.

---

## 🔍 ДЕТАЛЬНЫЙ АНАЛИЗ

### 1. Обработка тиков

**Цепочка обработки:**
```
1. Go worker / Binance API → тики
   ↓
2. Запись в Redis Stream: stream:tick_XAUUSDT
   ↓
3. CryptoOrderflowService.consume_ticks() читает из stream
   ↓
4. OrderFlowStrategy.process_tick() обрабатывает тик
   ↓
5. Генерация сигналов (если условия выполнены)
   ↓
6. SignalPipeline.publish_signal() → фильтры (ATR gate, cooldown, confidence)
   ↓
7. Публикация в signals:crypto:raw (если прошли фильтры)
```

**Конфигурация stream:**
```yaml
# docker-compose-crypto-orderflow.yml:144
- STREAM_TICK_XAUUSDT=stream:tick_XAUUSDT
- STREAM_BOOK_XAUUSDT=stream:book_XAUUSDT
```

**Код обработки тиков:**
```875:1105:python-worker/services/crypto_orderflow_service.py
async def consume_ticks(self, symbol: str) -> None:
    # Читает из stream:tick_{symbol}
    # Обрабатывает через strategy.process_tick()
```

### 2. Фильтры, блокирующие сделки

#### 🚦 ATR Gate (ОСНОВНОЙ ФИЛЬТР)

**Местоположение:** `python-worker/services/orderflow/signal_pipeline.py:358-440`

**Логика блокировки:**
```python
# Вычисление atr_bps
atr_bps_exec = (atr / entry_price) * 10000.0

# Unified threshold = max(atr_floor_th, fees_th)
unified_th = max(atr_floor_th, fees_th)

# Блокировка если atr_bps < unified_th
if atr_bps_exec < unified_th:
    # В режиме ENFORCE → блокирует сигнал
    if gate_mode == "ENFORCE":
        passed = False
        return  # Сигнал не публикуется
```

**Как вычисляется entry price:**
```2622:2645:python-worker/services/orderflow/strategy.py
# Executable Entry Pricing (P0)
executable_entry = float(price)  # price из тика (mid или last)
try:
    # Prefer atomic BookState for pricing
    bs_entry = getattr(runtime, "book_state", None)
    snap_entry = getattr(bs_entry, "snap", None) if bs_entry is not None else getattr(runtime, "last_book", None)
    if snap_entry:
        # Использует лучшую цену из стакана (ask для LONG, bid для SHORT)
        if direction == "LONG":
            executable_entry = float(asks_entry[0][0])  # лучший ask
        else:
            executable_entry = float(bids_entry[0][0])  # лучший bid
except Exception:
    executable_entry = float(price)  # fallback к цене из тика
```

**Источник цены:**
- `price` из тика = `(bid + ask) / 2.0` или `last` из `stream:tick_XAUUSDT`
- Для XAUUSDT цена берется из реальных тиков Binance (на 2025 год ~5000 USDT за унцию, но может быть любой)

**Типичные значения для расчета atr_bps_exec:**
- **ATR (1m):** ~8-10 USDT (из Redis `atr:XAUUSDT:1m`)
- **Entry price:** реальная цена из тика (на 2025 год ~5000 USDT за унцию)
- **atr_bps_exec:** `(atr / entry) * 10000` = `(8 / 5000) * 10000` = **~16 bps**
- **fees_th:** ~12-15 bps (зависит от комиссий и `exec_risk_ref_bps`)
- **atr_floor_th:** зависит от regime (обычно 5-10 bps)
- **unified_th:** `max(atr_floor_th, fees_th)` = `max(10, 15)` = **~15 bps**

**Проблема:** Если ATR низкий (например, 6-7 USDT) при цене ~5000:
- atr_bps_exec = (6/5000) * 10000 = **~12 bps**
- unified_th = max(10, 15) = **15 bps**
- **12 < 15** → ATR gate блокирует сигнал!

**Реальная проблема (из диагностики):**
```
[GATE-ENFORCE] ATR unified VETO triggered (fees): atr_bps=6.70 < th=25.64
```

**Важное замечание:** При текущей цене золота ~5000 USDT:
- ATR ~8 USDT дает `atr_bps_exec = (8/5000) * 10000 = 16 bps`
- Это **ниже**, чем при старой цене ~2650: `(8/2650) * 10000 = 30 bps`
- **Вывод:** При более высокой цене золота `atr_bps_exec` становится **меньше**, что увеличивает вероятность блокировки ATR gate!

**Причина высокого `fees_th` (25.64 bps):**
- Высокие комиссии Binance Futures
- Настройка `exec_risk_ref_bps` (по умолчанию 12.0, но может быть выше)
- Rocket multiplier (если применяется)
- При цене ~5000 USDT даже небольшой ATR дает низкий `atr_bps_exec`

**Текущая конфигурация:**
```yaml
# docker-compose-crypto-orderflow.yml:141-143
- XAU_ATR_GATE_AUDIT_ONLY=true  # ✅ Должно переводить в режим аудита
- XAU_EXEC_RISK_REF_BPS=8.0     # ✅ Снижен с 12.0 до 8.0
- XAU_MIN_SIGNAL_INTERVAL=12    # ✅ Cooldown 12 секунд
```

**НО:** Переменная `XAU_ATR_GATE_AUDIT_ONLY` должна применяться через `instrument_config.py`:
```1010:1010:python-worker/core/instrument_config.py
atr_gate_audit_only=_env_one(f"{prefix}_ATR_GATE_AUDIT_ONLY", _to_bool, base_cfg.atr_gate_audit_only),
```

Где `prefix = "XAU"` для XAUUSDT.

**Проверка применения:**
Конфигурация загружается в `OrderFlowConfigLoader.build_symbol_config()`:
```270:277:python-worker/services/orderflow/configuration.py
overrides = {}
if self.redis:
    try:
        overrides = await self.redis.hgetall(f"config:orderflow:{symbol}")
    except RedisError as exc:
        logger.warning("⚠️ (%s) Не удалось загрузить config:orderflow:%s: %s", symbol, symbol, exc)

self._apply_overrides(cfg, overrides)
```

**Важно:** ENV переменные применяются в `instrument_config.py`, но могут быть переопределены через Redis `config:orderflow:XAUUSDT`.

#### ⏱️ Cooldown Filter

**Местоположение:** `python-worker/services/orderflow/runtime.py` (cooldown логика)

**Текущая настройка:**
- **ENV:** `XAU_MIN_SIGNAL_INTERVAL=12` (12 секунд)
- **По умолчанию:** 3 секунды (из `DEFAULT_CONFIG`)

**Проблема:** Если сигналы приходят чаще, чем раз в 12 секунд, они буферизуются.

#### 🎯 Confidence Filter

**Местоположение:** `python-worker/services/orderflow/signal_pipeline.py`

**Текущая настройка:**
- **ENV:** `CRYPTO_SIGNAL_MIN_CONF__XAUUSDT=70` (70%)
- **По умолчанию:** 70%

**Проблема:** Если confidence < 70%, сигнал блокируется.

#### 📈 OF Score Filter

**Местоположение:** `python-worker/services/orderflow/signal_pipeline.py`

**Текущая настройка:**
- **По умолчанию:** `of_score_min = 0.60`

**Проблема:** Если OF score < 0.60, сигнал блокируется.

---

## 🛠️ РЕШЕНИЕ

### Вариант 1: Установить ATR gate в режим AUDIT (рекомендуется)

**Через Redis (приоритет):**
```bash
docker compose -f docker-compose-crypto-orderflow.yml exec redis-worker-1 redis-cli \
  HSET config:orderflow:XAUUSDT \
  atr_gate_audit_only 1 \
  exec_risk_ref_bps 8.0 \
  signal_cooldown_sec 10
```

**Через ENV (уже настроено, но нужно проверить применение):**
```yaml
# docker-compose-crypto-orderflow.yml:141
- XAU_ATR_GATE_AUDIT_ONLY=true  # ✅ Уже есть
```

**Проверка применения:**
```bash
# Проверить конфигурацию в Redis
docker compose -f docker-compose-crypto-orderflow.yml exec redis-worker-1 redis-cli \
  HGETALL config:orderflow:XAUUSDT

# Должно быть:
# atr_gate_audit_only: 1
# exec_risk_ref_bps: 8.0
# signal_cooldown_sec: 10
```

### Вариант 2: Снизить ATR threshold

**Через Redis:**
```bash
docker compose -f docker-compose-crypto-orderflow.yml exec redis-worker-1 redis-cli \
  HSET config:orderflow:XAUUSDT \
  atr_bps_min_static 5.0
```

**Через ENV:**
```yaml
- CRYPTO_ATR_BPS_MIN_STATIC__XAUUSDT=5.0
```

### Вариант 3: Проверить логи для диагностики

```bash
# Проверить последние логи с XAUUSDT
docker compose -f docker-compose-crypto-orderflow.yml logs --tail=200 crypto-orderflow-service | grep XAUUSDT

# Искать строки:
# - "[GATE-ENFORCE] ATR unified VETO"
# - "[XAUUSDT] emit signal"
# - "[COOLDOWN] (XAUUSDT)"
```

---

## 📋 ЧЕКЛИСТ ПРОВЕРКИ

### 1. Проверить, что тики приходят
```bash
docker compose -f docker-compose-crypto-orderflow.yml exec redis-worker-1 redis-cli \
  XLEN stream:tick_XAUUSDT
```
**Ожидается:** > 0 записей

### 2. Проверить ATR
```bash
docker compose -f docker-compose-crypto-orderflow.yml exec redis-worker-1 redis-cli \
  GET atr:XAUUSDT:1m
```
**Ожидается:** числовое значение (например, 8.73)

### 3. Проверить конфигурацию
```bash
docker compose -f docker-compose-crypto-orderflow.yml exec redis-worker-1 redis-cli \
  HGETALL config:orderflow:XAUUSDT
```
**Ожидается:** `atr_gate_audit_only: 1` или отсутствие (используется ENV)

### 4. Проверить сигналы
```bash
docker compose -f docker-compose-crypto-orderflow.yml exec redis-worker-1 redis-cli \
  XREVRANGE signals:crypto:raw COUNT 100 | grep XAUUSDT
```
**Ожидается:** записи с `symbol=XAUUSDT`

### 5. Проверить логи
```bash
docker compose -f docker-compose-crypto-orderflow.yml logs --tail=100 crypto-orderflow-service | \
  grep -E "(XAUUSDT|ATR.*VETO|GATE-ENFORCE)"
```

---

## 🎯 ИТОГОВЫЕ РЕКОМЕНДАЦИИ

1. **Убедиться, что тики приходят:**
   - Проверить `stream:tick_XAUUSDT` в Redis
   - Проверить логи go-worker или tick-ingest сервиса

2. **Настроить ATR gate в режим AUDIT:**
   ```bash
   docker compose -f docker-compose-crypto-orderflow.yml exec redis-worker-1 redis-cli \
     HSET config:orderflow:XAUUSDT atr_gate_audit_only 1
   ```

3. **Проверить применение ENV переменных:**
   - Убедиться, что `XAU_ATR_GATE_AUDIT_ONLY=true` применяется
   - Проверить логи при старте сервиса

4. **Мониторинг:**
   - Следить за логами на наличие `[GATE-ENFORCE] ATR unified VETO`
   - Проверять метрики `atr_gate_veto_total` в Prometheus

---

## 📊 МЕТРИКИ ДЛЯ МОНИТОРИНГА

- `ticks_read_total{symbol="XAUUSDT"}` - количество прочитанных тиков
- `ticks_processed_total{symbol="XAUUSDT"}` - количество обработанных тиков
- `signals_emitted_total{symbol="XAUUSDT"}` - количество сгенерированных сигналов
- `atr_gate_veto_total{symbol="XAUUSDT"}` - количество заблокированных ATR gate
- `signals_published_total{symbol="XAUUSDT"}` - количество опубликованных сигналов

---

## ✅ ГОТОВО К ПРОДАКШЕНУ

После применения настроек:
- [ ] Проверить, что тики приходят
- [ ] Проверить, что ATR gate в режиме AUDIT
- [ ] Проверить, что сигналы публикуются в `signals:crypto:raw`
- [ ] Проверить, что сделки открываются (если включен execution)

