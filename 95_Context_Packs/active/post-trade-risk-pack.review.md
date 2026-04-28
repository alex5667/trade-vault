---
type: document
tags: [llm-review, generated, local-llm]
title: "Review: post-trade-risk-pack"
source_pack: "/home/alex/Apps/Obsidian/trade-vault/95_Context_Packs/active/post-trade-risk-pack.md"
model: "deepseek-r1:14b"
updated_at: "2026-04-28T03:40:18+03:00"
---

# Review

## Goal
Создать систему управления рисками после торговли, чтобы минимизировать потенциальные убытки и повысить надежность торговой деятельности.

## Facts
- Используются метрики: `slippage_bps`, `slippage_ema_bps`, `ack_latency_ms`, `fill_latency_ms`.
- Задержки в подтверждении и заполнении заказов.
- Проскальзывание влияет на результаты торговли.

## Assumptions
- Весь необходимый контекст включен в пакет.
- Система использует упомянутые метрики для анализа.

## Risks
- Spike в проскальзывании может привести к substantial losses.
- Задержки могут негативно повлиять на ликвидность и результаты торговли.
- Trading без стоп-моделей увеличивает риски.

## Plan
1. Разработка стратегии по мониторингу метрик.
2. Установление alerts для spiked slippage, задержек и отклонений.
3. Планирование действий при обнаружении рисков, включая quarantine symbols с бракованными ATR.

## Tests
- Проверка spiked reject rate.
- Тесты на ack latency breaches budget.
- Отслеживание work of duplicate prevention mechanisms.

## Metrics/Alerts
- `slippage_bps`, `slippage_ema_bps`.
- Alerts для spiked slippage и задержек.

## Rollout/Rollback
- Постепенное внедрение изменений.
- Возможность отката при обнаружении проблем.
