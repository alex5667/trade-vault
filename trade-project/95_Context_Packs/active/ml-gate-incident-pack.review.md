---
type: document
tags: [llm-review, generated, local-llm]
title: "Review: ml-gate-incident-pack"
source_pack: "/home/alex/Apps/Obsidian/trade-vault/95_Context_Packs/active/ml-gate-incident-pack.md"
model: "deepseek-r1:14b"
updated_at: "2026-04-19T16:11:48+03:00"
---

# Review

## Goal
Документировать и устранить проблему с неавтономной конфигурацией ML Confirm Gate, а также обеспечить безопасное продвижение из режима SHADOW в ENFORCE.

---

## Facts
- `ml-confirm-gate` не может загружать активные конфигурации champion/challenger Redis.
- Отсутствие конфигурации приводит к ошибкам/неантиципациям, что ухудшает качество блокировок/разрешений.
- Метрики (`missing_cfg_total`, `status_count`) должны фиксировать статусы и помогать в диагностике.
- Роллапт SHADOW → ENFORCE зависит от точности метрик.

---

## Assumptions
- Redis не сохранил конфигурации должным образом (несовершеннолетие/неверный URL).
- Промоционная задача не смогла записать метаданные чемпиона.
- Дrift между путем модели и схемой фич.

---

## Risks
- Пrolonged отсутствие конфигурации → ухудшение качества блокировок/разрешений.
- Increased поддержка из-за неантиципаций/пропусков.
- Нарушение предсказуемости поведения при ENFORCE.

---

## Plan
1. Проверить Redis keys и DB.
2. Убедиться в правильности пути модели и схемы.
3. Применить исправления (например, восстановить конфигурацию).
4. Перейти к SHADOW для стабилизации.

---

## Tests
- Проверить метрики (`allow_total`, `block_total`, `missing_total`).
- Убедиться в правильности status_count.
- Наблюдение за err_rate и latency.

---

## Metrics/Alerts
- Пороги на missing/error ratio.
- Latency p95/p99.
- Добавить alerts на свежесть конфигурации.

---

## Rollout/Rollback
1. Роллапт: enable ENFORCE для небольшой группы/символов → расширять при стабильности.
2. Роллбэк: set mode SHADOW, pin champion cfg.
