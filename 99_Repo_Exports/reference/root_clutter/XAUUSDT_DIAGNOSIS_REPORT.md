# Диагностика XAUUSDT: Формирование сигналов и сделок

**Дата проверки:** 2026-02-05 21:30 UTC  
**Символ:** XAUUSDT  
**Источник:** CryptoOrderFlow

---

## 📊 ИТОГОВАЯ СТАТИСТИКА

| Метрика | Значение | Статус |
|---------|----------|--------|
| Тики в stream | 383,137 | ✅ Приходят |
| Сигналы в `signals:crypto:raw` | 4,132 (общий) | ⚠️ Нет для XAUUSDT |
| Открытые позиции | 0 | ❌ Нет |
| Закрытые сделки | 0 | ❌ Нет |
| ATR (1m) | 8.73 | ✅ Есть |

---

## ✅ ЧТО РАБОТАЕТ

1. **Тики приходят** - `stream:tick_XAUUSDT` содержит 383,137 записей
2. **CryptoOrderflowService обрабатывает XAUUSDT** - видно в логах
3. **Сигналы генерируются** - в логах: `[XAUUSDT] emit signal weak_progress conf=74.8%`
4. **XAUUSDT в DEFAULT_SYMBOLS** - зарегистрирован в `handler_factory.py` и `configuration.py`

---

## ❌ ПРОБЛЕМЫ

### 1. ATR Gate блокирует все сигналы

**Проблема:** Все сигналы блокируются ATR unified gate:
```
[GATE-ENFORCE] ATR unified VETO triggered (fees): atr_bps=6.70 < th=25.64 (relaxed_th=25.64) | XAUUSDT
```

**Причина:**
- Требуемый порог: **25.64 bps**
- Фактический ATR: **6.70 bps** (в момент проверки)
- ATR в Redis: **8.73** (среднее значение)

**Механизм блокировки:**
- ATR gate проверяет `atr_bps_exec = (atr / entry_price) * 10000`
- Если `atr_bps_exec < unified_threshold` → сигнал блокируется
- Unified threshold = max(atr_floor_th, fees_th) = **25.64 bps**

### 2. Cooldown буферизует сигналы

**Проблема:** Сигналы буферизуются из-за cooldown периода:
```
[COOLDOWN] (XAUUSDT) Signal buffered (age=22800ms < 30000ms). Pending updated=YES
```

**Причина:**
- Cooldown для XAUUSDT: **30,000ms (30 секунд)**
- Сигналы приходят чаще, чем разрешено
- Буферизуются до истечения cooldown

### 3. Сигналы не публикуются в streams

**Проблема:** Сигналы генерируются, но не попадают в:
- `signals:crypto:raw` (нет записей для XAUUSDT)
- `signals:cryptoorderflow:XAUUSDT` (stream пуст или не существует)
- `signals:aggregated:XAUUSDT` (нет записей)

**Причина:** ATR gate блокирует сигналы ДО публикации

---

## 🔍 ДЕТАЛЬНЫЙ АНАЛИЗ

### Цепочка обработки сигналов:

```
1. Тики приходят ✅
   ↓
2. CryptoOrderflowService обрабатывает ✅
   ↓
3. Сигналы генерируются ✅ (weak_progress, conf=74.8%)
   ↓
4. ATR Gate проверка ❌ (atr_bps=6.70 < th=25.64)
   ↓
5. Сигналы блокируются ❌
   ↓
6. Публикация не происходит ❌
   ↓
7. Позиции не открываются ❌
```

### Логи показывают:

```
[INFO] orderflow_strategy: [XAUUSDT] emit signal weak_progress conf=74.8%
[INFO] of_signal_pipeline: ℹ️ [GATE-ENFORCE] ATR unified VETO triggered (fees): atr_bps=6.70 < th=25.64
[WARNING] crypto_orderflow_service: 🛑 [COOLDOWN] (XAUUSDT) Signal buffered (age=22800ms < 30000ms)
```

---

## 🛠️ РЕКОМЕНДАЦИИ

### Вариант 1: Снизить ATR threshold для XAUUSDT (рекомендуется)

**Настройка в Redis:**
```bash
docker compose exec redis-worker-1 redis-cli HSET config:orderflow:XAUUSDT \
  atr_bps_min_static 5.0 \
  atr_gate_audit_only 1
```

**Или через ENV переменные:**
```yaml
environment:
  - CRYPTO_ATR_BPS_MIN_STATIC__XAUUSDT=5.0
  - CRYPTO_ATR_GATE_AUDIT_ONLY__XAUUSDT=1
```

### Вариант 2: Отключить ATR gate для XAUUSDT (только для тестирования)

```bash
docker compose exec redis-worker-1 redis-cli HSET config:orderflow:XAUUSDT \
  atr_gate_audit_only 1
```

**Внимание:** Это переведет gate в режим аудита (не блокирует, только логирует)

### Вариант 3: Настроить fees-aware threshold

ATR gate использует fees-aware policy. Для XAUUSDT можно снизить требования:

```bash
docker compose exec redis-worker-1 redis-cli HSET config:orderflow:XAUUSDT \
  exec_risk_ref_bps 8.0
```

### Вариант 4: Уменьшить cooldown для XAUUSDT

```bash
docker compose exec redis-worker-1 redis-cli HSET config:orderflow:XAUUSDT \
  signal_cooldown_sec 10
```

**Текущее значение:** 30 секунд  
**Рекомендуемое:** 10-15 секунд

---

## 📋 ПРОВЕРКА КОНФИГУРАЦИИ

### Текущая конфигурация XAUUSDT:

```bash
# Проверить конфигурацию
docker compose exec redis-worker-1 redis-cli HGETALL config:orderflow:XAUUSDT

# Проверить ATR
docker compose exec redis-worker-1 redis-cli GET atr:XAUUSDT:1m

# Проверить последние сигналы
docker compose logs --tail=100 crypto-orderflow-service | grep XAUUSDT
```

---

## ✅ ЧЕКЛИСТ ДЛЯ ИСПРАВЛЕНИЯ

- [ ] Снизить ATR threshold для XAUUSDT (через Redis или ENV)
- [ ] Проверить, что сигналы проходят ATR gate
- [ ] Убедиться, что сигналы публикуются в `signals:crypto:raw`
- [ ] Проверить, что TradeMonitorService обрабатывает сигналы
- [ ] Убедиться, что позиции открываются
- [ ] Мониторить логи на предмет других gate блокировок

---

## 🔗 СВЯЗАННЫЕ ФАЙЛЫ

- `python-worker/services/crypto_orderflow_service.py` - основной сервис
- `python-worker/services/orderflow/signal_pipeline.py` - ATR gate логика
- `python-worker/services/orderflow/configuration.py` - конфигурация
- `python-worker/handlers/handler_factory.py` - регистрация обработчиков

---

## 📝 ЗАМЕТКИ

1. **XAUUSDT обрабатывается** - сервис работает корректно
2. **Сигналы генерируются** - детекторы работают (weak_progress, conf=74.8%)
3. **Проблема в gate** - ATR gate слишком строгий для XAUUSDT
4. **Решение простое** - снизить threshold или перевести в audit mode

---

**Статус:** 🔴 Сигналы генерируются, но блокируются ATR gate  
**Приоритет:** Высокий  
**Сложность исправления:** Низкая











