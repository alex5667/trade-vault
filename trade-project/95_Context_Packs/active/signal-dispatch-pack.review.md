---
type: document
tags: [llm-review, generated, local-llm]
title: "Review: signal-dispatch-pack"
source_pack: "/home/alex/Apps/Obsidian/trade-vault/95_Context_Packs/active/signal-dispatch-pack.md"
model: "deepseek-r1:14b"
updated_at: "2026-04-19T16:20:46+03:00"
---

# Review

## Goal
Убедиться, что signal dispatch корректно обрабатывает сигнали, избегает дублей, маршрутизирует их в правильные сreamy, а також пазяває діагностику без залізації на торгівельних сreamy.

## Facts
- Сигнали мушу генерувати унікальний `signal_id`.
- Semantic dedup ішов на основі символа, боку, типу.
- Publish to raw streams (`signals:crypto:raw`), execution queue (`orders:queue`, `orders:queue:mt5`).
- Notify Telegram через stream `notify:telegram`.
- Diagnostics через `stream:signals:diagnostics`.

## Assumptions
- Система працює під стандартним баглянтом, коли не вказано інакше.
- всі consumers самі обертають retry unless specified otherwise (Redis xadd retries are handled by signal-dispatch).
- Notify Telegram повинен відправляти повідомлень без дублей.

## Risks
- Semantic dedup не працює -> повторні сигнали.
- Redis xadd fails -> губимо дані.
- Diagnostics падають у tradeable сreamy.
- Notify Telegram спамирує.
- signal_id not stable across equivalent events.
- Downstream executor отримує повідомлень без необхідних полів.

## Plan
1. Verify semantic dedup works correctly under replay scenarios.
2. Test Redis xadd retry logic.
3. Ensure diagnostics are properly isolated from tradeable streams.
4. Implement rate limiting on notify Telegram.
5. Validate signal_id stability over time.
6. Enforce signal payload contract on all downstream consumers.

## Tests
- Semantic dedup test: send duplicate signals and check no duplicates in execution queue.
- Redis xadd retry test: simulate failure and verify successful retry.
- Stream isolation test: publish to diagnostics and ensure it doesn't appear in tradeable streams.
- Notify rate limiting test: send burst of notifications and check for rate limits.
- Signal_id consistency test: replay signals and verify same id is produced.
- Payload contract enforcement test: send invalid payload and check downstream handling.

## Metrics/Alerts
- Published total by stream (signals:crypto:raw, orders:queue, notify:telegram).
- Dedup hits total.
- Publish errors total (Redis xadd failures).
- Retry queue depth.
- Notify sent/skipped totals.
- Diagnostic publish total.
- Raw-to-execution ratio.

## Rollout/Rollback
### Rollout
1. Verify signal_id stability on replay of historical data.
2. Test semantic dedup under load and burst conditions.
3. Confirm diagnostics are properly separated from tradeable streams.
4. Gradual rollout with monitoring.
5. Only after verification, full deployment.

### Rollback
1. Reduce execution routing first if issues arise.
2. Keep raw stream publication for visibility.
3. Disable notify path if spamming occurs but keep execution path if safe.
4. Revert changes and roll back to previous version if critical issues detected.
