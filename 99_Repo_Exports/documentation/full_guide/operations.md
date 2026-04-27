# Эксплуатация и Поддержка Scanner Infrastructure

Документ предназначен для инженеров эксплуатации, SRE и дежурных. Здесь описаны ежедневные ритуалы, мониторинг, реагирование на инциденты и процедуры релизов. Используйте этот файл как оперативный справочник.

---

## 1. Роли и ответственность

| Роль                  | Ответственность                                                                          | Основные инструменты                          |
| --------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------- |
| On-call SRE           | Мониторинг системных метрик, реагирование на P1/P2 инциденты, управление инфраструктурой | Grafana, Alertmanager, `make diagnose`        |
| Market Data Engineer  | Контроль потоков тиков, задержек, реплицирование, качественное состояние тик-фидов       | `make tick-streams`, дэшборд `Tick Streams`   |
| Trading Ops           | Наблюдение за трейлингом, подтверждение исполнения, связь с MT5                          | `make trailing-stats`, дэшборд `TP1 Trailing` |
| Analytics Engineer    | Контроль Analytics V2/V3, GPU compute, calibration, dataset export                        | `make analytics-status`, `make gpu-stats`     |
| Signal Quality Analyst| Мониторинг качества сигналов, threshold tuning, performance metrics                      | `make calibration-stats`, ROC dashboards      |
| Release Manager       | Координация релизов, контроль чек-листов, коммуникация                                   | Notion/Jira, `make release-checklist`         |

---

## 2. Ежедневные процедуры

### 2.1 Утренний чек-лист (09:00 UTC)

1. `make diagnose` — проверить, что все health endpoints доступны.
2. Grafana `Tick Streams`: убедиться, что `tick_gap_seconds` < 0.4с (P95) для ключевых символов.
3. Grafana `TP1 Trailing`: проверить `trailing_latency_ms` < 2500 мс (P95).
4. `make analytics-status` — проверить Analytics V2/V3 system и GPU compute.
5. `make calibration-stats` — убедиться, что threshold tuning работает (последнее обновление < 1 час).
6. `make tracker-stats` — убедиться, что отчёты генерируются (последнее время < 20 мин).
7. Проверить Telegram `notify:telegram` — наличие свежих уведомлений и алертов.
8. Просмотреть алерты за ночь в Alertmanager и отметить решённые.

### 2.2 Вечерний чек-лист (20:00 UTC)

1. Сравнить объём сигналов сегодня и вчера (`signals_generated_total`).
2. Проверить `orders_queue_length` < 5 (Grafana/Prometheus).
3. Убедиться, что бэкапы Redis выполнены (`make backup-status`).
4. Подготовить короткий отчёт в `#scanner_ops`: статус систем, выявленные проблемы, план на завтра.

---

## 3. Мониторинг

### 3.1 Метрики (Prometheus)

| Метрика                      | Цель / порог        | Действия при нарушении                  |
| ---------------------------- | ------------------- | --------------------------------------- |
| `tick_gap_seconds{symbol}`   | P95 ≤ 0.4 с         | Проверить Go workers и источники тиков. |
| `dualredis_failover_total`   | 0                   | Если > 0 → проверить `redis-ticks`.     |
| `trailing_latency_ms`        | P95 ≤ 2500 мс       | Проверить `tp_event_listener`, MT5.     |
| `orders_queue_length`        | < 5                 | Очистить зависшие команды.              |
| `analytics_compute_latency_ms`| P95 ≤ 30000 (30 сек)| Проверить GPU service, оптимизировать запросы.|
| `calibration_optimization_score`| > 0.7              | Проверить параметры калибровки.         |
| `gpu_memory_usage_percent`   | < 85%              | Масштабировать GPU или оптимизировать память.|
| `stats_report_latency_ms`    | ≤ 300000 (5 мин)    | Проверить tracker, Redis trades.        |
| `go_gateway_rate_limit_hits` | 0                   | Увеличить лимит или исправить шторма.   |
| `redis_memory_used_bytes`    | < 75% от доступного | Расширить инстанс или очистить ключи.   |

### 3.2 Логи

- **Go сервисы**: структурированные JSON через stdout.
- **Python сервисы**: `uvicorn/gunicorn` + JSON формат (Loguru).
- Используйте `make logs SERVICE=<name>` или подключение к ELK/FluentBit (если настроено).
- Следите за `ERROR`/`CRITICAL`. Логи с пометкой `[RISK]` требуют реакции.

### 3.3 Дашборды Grafana

- `Tick Streams`: задержки, reconnects, lag consumer групп.
- `TP1 Trailing`: время реакции, количество trailing events по символам.
- `Signal Tracker`: объём сигналов, latency отчётов.
- `Analytics V2/V3`: ROC curves, threshold tuning, calibration metrics.
- `GPU Compute`: использование GPU, acceleration ratios, memory usage.
## 2. Redis Management & ACLs

### 2.1 Шардирование

Система использует три основных шарда:
- **Shard 1 (Market Data)**: `redis-ticks:6379`. Только тики и стаканы.
- **Shard 2 (Core/State)**: `redis-core:6379`. Журналы `orders:exec`, состояния позиций.
- **Shard 3 (Analytics)**: `redis-analytics:6379`. Метрики и временные ряды.

### 2.2 Redis ACL (Security)

Доступ к Redis ограничен через ACL файлы (`config/redis/*.acl`).
- **Reservers**: могут только `XADD` в свои стримы.
- **Consumers**: могут `XREAD` и `XACK`.
- **Admin**: полный доступ (только для `redis_janitor` и ручного вмешательства).

Команда для обновления ACL: `make redis-acl-sync`.

---

## 3. P4.1 SLO Monitoring

### 3.1 Ключевые дашборды

1. **P4.1 Latency (Hot Path)**: мониторинг задержек t0-t5.
2. **Gate Diagnostics**: статистика вето каждого гейта (G0-G15).
3. **Journal Integrity**: проверка лага между `orders:exec` и материализацией.

### 3.2 Реакция на SLO Violations

- **Incident P1**: P99 (t1-t3) > 10мс на протяжении 2 минут.
- **Действие**: Проверить загрузку CPU Python воркеров, наличие "heavy" гейтов, задержки сети Redis.

---

## 4. ML Governance & Drift Detection

### 4.1 Ежедневные проверки

Система автоматически рассчитывает дрейф признаков Tier-1.
- Проверка в Grafana: **Drift Detection (PSI/KS)**.
- Если **PSI > 0.2**, необходимо инициировать переобучение модели или перевести G10 в `SHADOW` режим.

### 4.2 Calibration Loops

Ночные джобы обновляют пороги уверенности (Isotonic Regression) на основе закрытых трейдов. Проверка статуса: `make ml-gov-status`.

---

## 5. Deployment & Resilience

### 5.1 Journal-First Recovery

При сбое экзекутора:
1. Не перезапускать вручную.
2. `BootstrapSupervisor` сам проверит целостность `orders:exec`.
3. При обнаружении дублей сработает идемпотентность по `sid`.

### 5.2 Rollout G-Gates

Новые гейты всегда деплоятся в режиме `SHADOW`. Для перевода в `ENFORCE`:
1. Набрать 500+ виртуальных трейдов.
2. Убедиться, что `Precision` гейта соответствует ожиданиям.
3. Обновить `GATE_ENFORCE_LIST` в ENV.

---

## 6. Алерты и эскалация

| Событие                   | Канал                 | SLA реакции      | Ответственный            |
| ------------------------- | --------------------- | ---------------- | ------------------------ |
| P1 (trading остановлен)   | PagerDuty + Slack     | ≤ 5 минут        | On-call SRE + TradingOps |
| P2 (метрики недоступны)   | Slack `#scanner_ops`  | ≤ 15 минут       | SRE                      |
| P3 (документация, отчёты) | Slack `#scanner_docs` | ≤ 1 рабочий день | Release Manager          |

### Процесс эскалации

1. Подтвердить алерт в Alertmanager.
2. Сообщить в профильный канал (приложить краткий статус).
3. Создать инцидент в Jira с severity.
4. Если затронуты реальные трейды — уведомить `@trading-ops` и актуальных менеджеров.
5. После решения оформить постмортем (см. `operations.md#постмортем`).

---

## 5. Управление релизами

### 5.1 Планирование

1. Создать задачу в Jira с описанием, согласовать окно.
2. Подготовить release branch и описание изменений.
3. Убедиться, что документация обновлена (`overview.md`, `services.md`, `data_flow.md`, `CONFIGURATION.md`).

### 5.2 Чек-лист перед деплоем

- [ ] Все тесты и линтеры пройдены (`make ci-all`).
- [ ] Секреты актуальны (`make export-vault`).
- [ ] Бэкапы Redis сделаны (`make backup-redis`).
- [ ] Создан план отката (rollback).
- [ ] Подготовлено уведомление для команд.

### 5.3 Деплой

```bash
make deploy ENV=prod VERSION=<tag>
```

Скрипт выполняет:

1. Выкат нового образа.
2. Проверку health endpoints.
3. Smoke-тесты (`make gateway-test`, `make trailing-test`).
4. Обновление статуса в Slack.

### 5.4 Постмортем

1. В течение 24 часов собрать факты: временная шкала, причина, фикс, улучшения.
2. Заполнить шаблон (Notion/Jira).
3. Обновить `troubleshooting.md`, если выявлен новый кейс.
4. При необходимости добавить задачи в `roadmap.md`.

---

## 6. Управление конфигурациями

- Все изменения `.env` и конфигов описываются в `CONFIGURATION.md`.
- Для prod-конфигов используется Vault. Изменения проходят ревью и approval.
- Для dev/stage допускаются локальные overrides (`config/custom/*.yaml`), но они не коммитятся.
- `make config-diff` покажет расхождения между локальными и эталонными конфигами.

---

## 7. Обслуживание Redis

| Процедура       | Команда                            | Примечание                                        |
| --------------- | ---------------------------------- | ------------------------------------------------- |
| Проверка памяти | `make redis-stats`                 | Показывает использование по каждому инстансу      |
| Ротация AOF     | `make redis-compact`               | Выполняйте при росте файла журнала                |
| Бэкап           | `make backup-redis`                | Складывает в `backups/redis/<date>`               |
| Восстановление  | `make restore-redis BACKUP=<path>` | Подробности в `troubleshooting.md#redis-recovery` |
| Очистка dev     | `make reset-redis`                 | Только на dev.                                    |

При превышении памяти > 80% немедленно уведомляйте SRE lead.

---

## 8. Управление MT5 интеграцией

1. Проверка соединения: `make mt5-ping`.
2. Если нет ответов, перезапустите `TickBridge` и убедитесь, что терминал онлайн.
3. При повторных сбоях перенаправьте поток на резервный MT5 (описано в `troubleshooting.md#mt5`).
4. Контролируйте очереди `/orders/poll`; задержка > 5с требует расследования.
5. Обновление токенов `MT5_EVENT_TOKEN`, `MT5_ORDER_TOKEN` — ежемесячно или при утечке.

---

## 9. Управление отчётами и уведомлениями

- Telegram: `make telegram-check` проверит токен и ID чата.
- Плановые отчёты: `make tracker-report` принудительно генерирует отчёт.
- Для временного отключения уведомлений установите `TELEGRAM_NOTIFICATIONS_ENABLED=false` (dev/stage).
- В случае ошибок доставки проверьте очередь `notify:telegram` (`redis-cli xlen notify:telegram`).

---

## 10. Документация и знания

- Все изменения процессов фиксируйте здесь и в `overview.md`.
- Для нового сервиса создайте раздел в `services.md` и обновите `architecture.md`.
- Используйте тег `documentation/full_guide` при создании issue.

---

## 11. Быстрые сценарии (Cheat Sheet)

| Ситуация                        | Действие                                                                                           |
| ------------------------------- | -------------------------------------------------------------------------------------------------- |
| Наблюдается рост задержки тиков | `make tick-streams`, проверить `go-worker` логи, перезапустить сервис, сверить сеть.               |
| Trailing команды не исполняются | Проверить `tp_event_listener` логи, очередь `orders:queue`, выполнить `make trailing-test`.        |
| Нет отчётов Signal Tracker      | `make tracker-stats`, проверить `stats_report_latency_ms`, перезапустить tracker.                  |
| MT5 не отвечает                 | `make mt5-ping`, проверить токены, перезапустить TickBridge, эскалировать TradingOps.              |
| Redis Core превышает лимит      | Снять бэкап, очистить старые ключи (`trade:timeline`), рассмотреть горизонтальное масштабирование. |
| Алерты по rate limiting         | Посмотреть `go_gateway_rate_limit_hits`, скорректировать лимиты, проверить нагрузку.               |

---

## 12. Приложения

- **A. Шаблон постмортема** — см. Notion `Scanner Postmortem Template`.
- **B. Контакты и escalation matrix** — `overview.md#контакты-и-каналы-взаимодействия`.
- **C. ADR** — ознакомиться перед архитектурными изменениями (см. `architecture.md#adr`).

Документ поддерживается командой `@sre-team`. Предложения по улучшению отправляйте в `#scanner_ops`.
