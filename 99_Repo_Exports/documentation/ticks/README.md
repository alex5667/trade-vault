# 📈 Tick Data Documentation (2026-01-01)

Документация для инженеров по маркет-данным и разработчиков ingestion-сервисов. Обновлено командой Senior Go/Python Developer + Senior Trading Systems Analyst.

---

## 🔎 Что внутри

| Документ                                           | Описание                                                                 |
| -------------------------------------------------- | ------------------------------------------------------------------------ |
| **[TICKS_ARCHITECTURE.md](TICKS_ARCHITECTURE.md)** | Архитектура ingestion, каналы Binance/MT5, `redis-ticks`, Signal Tracker |
| **[TICKS_DEVELOPMENT.md](TICKS_DEVELOPMENT.md)**   | Настройка окружения, dev-флоу, `ticks-*` Make-команды, мониторинг        |

---

## 🚦 Быстрый старт

1. Прочитайте `TICKS_ARCHITECTURE.md` для понимания потоков данных.
2. Настройте окружение по `TICKS_DEVELOPMENT.md`.
3. Запустите инфраструктуру (`make up-bg`), затем `make ticks-status`, `make ticks-streams`, `make ticks-groups` — по умолчанию эти команды целятся в `redis-worker-1` (переопределяется `TICKS_REDIS_CONTAINER`).
4. Для тестов задействуйте `python-worker/services/tick_emulator.py` и `make ticks-test` (см. dev guide).
5. Мониторинг WebSocket потоков — Grafana `Websocket Streams`, `make tracker-stats` для проверки статистики сигналов.

---

## ✅ Контроль версий

- 2026-01-21 — обновление дат документации, проверка актуальности.
- 2025-11-21 — обновление документации, синхронизация с текущим состоянием кодовой базы.
- 2025-11-13 — добавлены процедуры `redis-ticks`, Signal Performance Tracker, статистика по источникам (`stats:*:{source}`), обновлены Make-команды (`ticks-*`).
- 2025-11-08 — актуализация под tick_ingest_server v2, book analytics и Signal Performance Tracker.
- 2025-11-07 — полное обновление документов после релиза TP1 Trailing.
- Все ключи Redis и команды Makefile актуализированы.
- Ответственные: `@market-data-team`.

Вопросы и предложения → `#scanner_ticks`.
