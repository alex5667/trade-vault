---
type: document
tags: [llm-review, generated, local-llm]
title: "Review: post-trade-risk-pack"
source_pack: "/home/alex/Apps/Obsidian/trade-vault/95_Context_Packs/active/post-trade-risk-pack.md"
model: "deepseek-r1:14b"
updated_at: "2026-04-19T16:23:26+03:00"
---

# Review

## Goal
Подготовить compact context pack по post-trade risk management, фокусируясь на trailing stops, SLQ (Stop Loss Quality) и обратной связи по slippage.

---

## Facts
- Ключевые метрики выполнения:
  - `slippage_bps`
  - `slippage_ema_bps`
  - `fill_latency_ms`
  - `ack_latency_ms`
  - `symbol_mapping_error_total`
- Потребуются дашборы:
  - Сравнение количества сигналов и執кьюшнів
  - Отклонения по причинам в течение времени
  - Сліп page по символу/venue/sесзіон
  - П50/П95/П99 задержки подтверждення
- Алерти:
  - Завышенная доля `atr_bad_pct`
  - Проблеми з `atr_unavailable`
  - Задержка в обработкі превышает бюджет

---

## Assumptions
- Не вказано конкретних порогів для припустки сліп page.
- Нет додаткових деталей про митці/контракти для оповіщень.

---

## Risks
- Високий різник фінансової калібryзації через нерегулюване сліп page.
- Проблеми з ATR можуть вести до несправедливих стопов і ризнику переторгування.
- Велика відхилення у задержці подтверждення або філінгу.

---

## Plan
1. Моніторинг ключевих метрик екзекуції.
2. Виконати чеки на ATR (доступність/свежина).
3. Організовати карантин для символів з дужою відхиленнями ATR.
4. Зберігати закриті екзекуції поки ATR є вигідним.

---

## Tests
- Тестування метрик на точність.
- Тестування тригерів алертів.
- Тестування функціоналізації падежу ATR нездоровий.

---

## Metrics/Alerts
- `slippage_bps`
- `slippage_ema_bps`
- `fill_latency_ms` (budget: P50=10ms, P95=30ms)
- `ack_latency_ms` (预算: P50=20ms, P95=60ms)
- `atr_bad_pct` (threshold: >5%)
- `atr_source_selected`

---

## Rollout/Rollback
- Градуальний виконкат:
  - Заведення контроля ATR на частиці символів.
  - Надрахування на реюмнату.
- Rollback:
  - Вимкнення функціоналізації у разі проблем.
  - Повення логування для аналітиці.
