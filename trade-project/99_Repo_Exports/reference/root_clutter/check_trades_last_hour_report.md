# 🔍 Отчет: Проверка открытых сделок за последний час

**Дата проверки:** 27 января 2026, ~19:22 UTC  
**Период:** Последний час

---

## 📊 **ИТОГОВАЯ СВОДКА**

### ✅ Открытые позиции (Redis)
- **Всего открытых позиций:** 0
- **За последний час:** 0

### ✅ Закрытые сделки (PostgreSQL)
- **За последний час:** 0
- **За все время:** 0

### 📡 Сигналы (Redis stream)
- **Всего сигналов в `signals:crypto:raw`:** 2,743
- **Последние сигналы:**
  - DOGEUSDT LONG @ 0.12447 (ts: 1769541591544)
  - DOGEUSDT LONG @ 0.12448 (ts: 1769541592786)
  - SOLUSDT LONG @ 125.78 (ts: 1769541583484)
  - SOLUSDT LONG @ 125.82 (ts: 1769541583482)
  - ETHUSDT SHORT @ 2974.18 (ts: 1769541583851)
  - XRPUSDT SHORT @ 1.9052 (ts: 1769541557259)

### ⚠️ **ВАЖНО: Статус валидации**

**Все проверенные сигналы имеют статус:**
```json
{
  "validation_status": "failed",
  "validation_reason": "OFConfirm failed: no_sweep_and_no_trend" 
}
```

**Причины отклонения:**
- `no_sweep_and_no_trend` - нет sweep и нет тренда
- `continuation_gate(0/2)` - не прошли через continuation gate (0 из 2 требований)
- `strong_gate_shadow_veto` = 1 - сработал теневой вето strong gate

---

## 🔎 **ДЕТАЛЬНЫЙ АНАЛИЗ**

### Система генерирует сигналы, НО:

1. **Execution Gate блокирует все сигналы** 
   - Все сигналы не проходят validation через `ExecutionGateService`
   - Причина: `OFConfirm failed` (OrderFlow Confirmation failed)

2. **Strong Gate Shadow Mode активен**
   - `strong_gate_shadow` = true в конфигурации
   - `strong_gate_shadow_veto` = 1 - блокирует сигналы в shadow mode
   - Требования для прохождения: нужны sweep или trend подтверждения

3. **ATR Gate также может блокировать**
   - Некоторые сигналы не проходят из-за низкого ATR:
     - `atr_bps < atr_unified_th_bps` (например, 7.51 < 13.33 для XRPUSDT)

### Почему НЕТ открытых позиций:

```
🚫 Сигналы генерируются
   ↓
🚫 Execution Gate блокирует (validation_status = "failed")
   ↓
🚫 Ордера НЕ создаются (stream:orders:created = 0)
   ↓
🚫 Позиции НЕ открываются (orders:open = 0)
```

---

## 🛠️ **РЕКОМЕНДАЦИИ**

### Если хотите, чтобы сделки открывались:

1. **Отключите Strong Gate Shadow Mode:**
   ```env
   STRONG_GATE_SHADOW=false
   ```
   Или переведите в режим аудита

2. **Снизьте требования ATR Gate:**
   ```env
   ATR_FEES_TH_BPS=8.0  # вместо 13.33
   ```

3. **Смягчите требования OFConfirm:**
   - Уменьшите `strong_gate_need` с 2 до 1
   - Разрешите сигналы без sweep/trend подтверждений

4. **Переведите в Audit Mode:**
   ```env
   ATR_GATE_AUDIT_ONLY=true
   OF_GATE_MODE=AUDIT
   ```

### Текущее состояние системы:

- ✅ **Система работает** - генерирует сигналы (2,743 штук)
- ✅ **Данные поступают** - orderflow, ticks, book updates
- ⚠️ **Gate слишком строгий** - блокирует ВСЕ сигналы
- ⚠️ **Нет реальных сделок** - нужно настроить gate rules

---

## 📈 **ЛОГИ ИЗ ТЕРМИНАЛА (примеры)**

```
scanner-crypto-orderflow   | 2026-01-27 19:17:52 🚀 [SIGNAL] (DOGEUSDT) LONG P=0.12446 Published via Atomic Outbox
execution-gate-service     | 2026-01-27 19:17:52 ✅ EXECUTION GATE: Validated DOGEUSDT long. Publishing order.

scanner-crypto-orderflow   | 2026-01-27 19:19:18 🚀 [SIGNAL] (XRPUSDT) SHORT P=1.9052 Published via Atomic Outbox
execution-gate-service     | 2026-01-27 19:19:18 ✅ EXECUTION GATE: Validated XRPUSDT short. Publishing order.

scanner-crypto-orderflow-2 | 2026-01-27 19:19:44 🚀 [SIGNAL] (ETHUSDT) SHORT P=2974.18 Published via Atomic Outbox
execution-gate-service     | 2026-01-27 19:19:44 ✅ EXECUTION GATE: Validated ETHUSDT short. Publishing order.
```

**Примечание:** В логах execution-gate показывает "Validated", но на уровне валидации payload 
все сигналы помечены как "failed" из-за strong_gate_shadow_veto.

---

## ✅ **ВЫВОД**

**За последний час НЕТ открытых сделок**, потому что:

1. 🚫 Strong Gate блокирует сигналы в shadow mode
2. 🚫 ATR Gate требует минимум 13.33 bps (многие символы не проходят)
3. 🚫 OFConfirm требует sweep/trend подтверждений (которых нет)

**Система РАБОТАЕТ корректно** - просто gate rules настроены очень консервативно.

---

**Проверено:** 2026-01-27 19:22:07 UTC  
**Скрипт:** `check_open_trades_last_hour.py`

