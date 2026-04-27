---
type: document
tags: [llm-review, generated, local-llm]
title: "Review: mt5-executor-pack"
source_pack: "/home/alex/Apps/Obsidian/trade-vault/95_Context_Packs/active/mt5-executor-pack.md"
model: "deepseek-r1:14b"
updated_at: "2026-04-19T16:15:13+03:00"
---

# Review

## Goal
Реализация торгового сигнала в реальный ордер в MT5/у брокера с контролем рисков и явными правилами retry.

---

## Facts
- **Цель:** Материализовать торговый сигнал в ордер, учитывая риск и ценовые уровни Stop Loss/Take Profit.
- **Входные данные:** Очередь ордеров `orders:queue:mt5` с полями:
  - signal_id
  - action (OPEN/CLOSE)
  - symbol
  - side (BUY/SELL)
  - entry_price, sl_price, tp1_price, risk_pct.
- **Выходные данные:** Успешно размещённые ордера в MT5 или у брокера с учётом SL/TP.
- **Resp:** Чтение очереди, парсинг данных, символьное преобразование, вычисление лотовости, обработка ошибок.
- **Ошибки:**
  - Неот복ивные: invalid_symbol, broker_rejection.
  - Отбракиваемые: invalid_volume, malformed_request.
- **Метрики:** orders_published_total, orders_failure_total{reason}, ack_latency_ms.
- **Алерты:** роста отклонений, задержек в обработке, дублирования сигналов.

---

## Assumptions
- Пользователи согласны с рисками и настройками SL/TP.
- Символы в.signals преобразуются корректно в брокерскую нотацию.
- Брокерские ограничения по лотам известны и учитываемы.

---

## Risks
1. **Invalid symbol mapping:** Некорректное преобразование символов может привести к ошибкам при размещении ордеров.
2. **Incorrect lot sizing:** Вычисление размера лотов может нарушить брокерские ограничения.
3. **Requote handling:** Постоянные запросы на подтверждение может вызвать风暴 и задержки.
4. **Connection issues:** Проблемы с подключением к MT5 могут привести к остановке обработки ордеров.

---

## Plan
1. Реализовать валидацию символов и лотовости перед размещением ордера.
2. Настроить retry机制 для transient errors (requote, connection issues).
3. Проверить idempotency для предотвращения дублирования ордеров.
4. Настраивать обработку SL/TP в соответствии с брокерскими規лами.

---

## Tests
1. **Conversion test:** Проверить преобразование сигнала в ордер.
2. **Symbol mapping:** Тестировать правильность символьного преобразования.
3. **Lot size calculation:** Проверить вычисление лотовости на разных балансах.
4. **Error handling:** Проверить обработку transient и permanent errors.
5. **Idempotency test:** Отослать дублирующийся сигнал.
6. **SL/TP placement:** Проверить размещение стопов и прибыльных уровней.

---

## Metrics/Alerts
- **Метрики:**
  - orders_published_total
  - orders_failure_total{reason}
  - ack_latency_ms
  - duplicate_order_prevented_total
- **Алерты:**
  - Снижение скорости упешной обработки.
  - Рост повторяющихся ошибок.

---

## Rollout/Rollback
1. **Роллアウト:** Проверить на бумаге, затем риск_pct, символы и лоты.
2. **Роллбэк:** Остановить очередь, перейти в демо-режим, сохранить логи.
