---
type: document
tags: [llm-review, generated, local-llm]
title: "Review: pre-publish-gates-pack"
source_pack: "/home/alex/Apps/Obsidian/trade-vault/95_Context_Packs/active/pre-publish-gates-pack.md"
model: "deepseek-r1:14b"
updated_at: "2026-04-19T16:18:33+03:00"
---

# Review

## Goal
Настраиваем и проверяем pre-publish gates для trade-сигналов, чтобы убедиться, что только качественные и надежные сигналы допускаются к execution.

---

## Facts
- **Gate Families**: 
  - Hard Data Quality Gate (блокирует при проблемах входных данных: stale book, missing ATR, critical tick gap).
  - Regime/Session Gate (проверяет совместимость candidate kind с текущим market regime).
  - Feature Drift Gate (обнаруживает drift рынка от распределений, на которых калиброваны model/thresholds).
  - SMT Coherence Gate (проверяет, не идет ли signal против лидеров сектора).
  - Edge Cost Gate (сравнивает expected edge против fees + spread + expected slippage).
  - Min Interval Gate (защищает от(signal spam).

- **Non-negotiable rules**:
  - Каждый veto обязан иметь reason code.
  - Veto reasons должны быть агрегируемыми.
  - Порядок gates должен быть явным и стабильным.
  - Если gate изменяет контекст, это должно быть документировано.
  - Diagnostics stream не может быть treated как tradeable path.

- **Metrics**:
  - veto_total_by_reason
  - pass_rate_by_symbol/kind
  - gate_latency
  - edge_cost_veto_rate
  - drift_veto_rate
  - regime_mismatch_rate
  - DQ_veto_rate

- **Alerts**:
  -_spike в block rate после rollout.
  - отсутствие diagnostics на veto.
  - широкая волна veto на major символах.
  - внезапный drift veto при смене market regime.

---

## Assumptions
- Пороги для gates определены и regularly reviewed.
- Diagnostics stream fully integrated and tested.
- All gate mutations properly documented.
- Baseline thresholds for each gate maintained.

---

## Risks
- Over-blocking из-за слишком строгих порогов.
- Inconsistent reason codes, затрудняющие анализ.
- False positives от Feature Drift Gate.
- Diagnostics stream missed или ignored.
- Latency issues impacting decision-making.

---

## Plan
1. Проверить каждую condition для gates на корректность.
2. Настраить reason codes и убедиться в их一致性.
3. Тестировать валидацию через shadow mode observe.
4. Настраивать monitoring для metrics/alerts.
5. Определить baselines для rollback в случае проблем.

---

## Tests
- Проверить veto conditions для каждого gate:
  - Data Quality Gate: stale book, missing ATR, critical tick gap.
  - Regime/Session Gate: совместимость kind с regime.
  - Feature Drift Gate: отслеживание drift.
  - SMT Coherence Gate: signal vs leader signals.
  - Edge Cost Gate: expected edge vs costs.
  - Min Interval Gate: защита от spam.

- Проверить reason codes для veto:
  - Коды должны быть стандартизированы и появляться в metrics/alerts.
  - Fast checks для stale book, например, `book_age_ms` и `book_rate_hz`.

- Проверить metrics:
  - Veto_total_by_reason.
  - Pass rate by symbol/kind.
  - Gate latency.

- Проверить alerts:
  - Block rate spike.
  - Missing diagnostics на veto.
  - Drift veto при смене regime.

---

## Metrics/Alerts
- **Metrics**:
  - veto_total_by_reason{reason=~"book_stale|atr_unavailable|tick_gap_critical"}
  - pass_rate_by_symbol{kind="market_open"}
  - gate_latency
  - edge_cost_veto_rate

- **Alerts**:
  - Alert on block rate spike: `block_rate_1h > 2 * normal_block_rate`.
  - Alert on missing diagnostics: `diagnostics_missing > 0`.

---

## Rollout/Rollback
### Rollout
- Настраивать gate в shadow-like observe mode.
- Сравнивать veto rates по символам с контрольной группой.
- Убедиться, что reason codes отображаются в metrics и diagnostics.

### Rollback
- Отключить最新的 gate.
- Восстановить пороги до known good baseline.
- Поддерживать counters для retrospective analysis.
