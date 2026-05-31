# Настройка и Запуск Scanner Infrastructure

Этот документ описывает полный процесс развертывания платформы на локальной машине, стенде и в продакшене. Следуйте инструкциям последовательно, чтобы исключить пропуски. Все команды выполняйте из директории `/home/alex/front/trade/scanner_infra`.

---

## 1. Требования к окружению

| Компонент      | Версия / требование                           | Примечание                                             |
| -------------- | --------------------------------------------- | ------------------------------------------------------ |
| ОС             | Linux (Ubuntu 22.04+), macOS 13+, WSL2        | Для Windows до 11 рекомендуем WSL2.                    |
| Docker         | ≥ 24.0                                        | Проверьте: `docker version`.                           |
| Docker Compose | встроен в Docker Desktop или `docker compose` | Используется Makefile (`make up-bg`).                  |
| Go             | ≥ 1.22                                        | Установите через `asdf`, `brew` или пакетный менеджер. |
| Python         | ≥ 3.11                                        | Виртуальное окружение / `pyenv` рекомендуются.         |
| CUDA           | ≥ 11.8 (для GPU acceleration)                 | Опционально, для GPU compute service.                  |
| Make           | GNU make 4.3+                                 | Управление командами.                                  |
| Redis CLI      | ≥ 7.2                                         | Для диагностики (`redis-cli`).                         |
| PostgreSQL     | ≥ 14 (опционально для аналитики)              | Для хранения исторических данных аналитики.            |
| Node.js        | ≥ 18 (только для фронтовых утилит)            | Не обязательный компонент.                             |

Дополнительно: `curl`, `jq`, `rsync`, `git`, `openssl`.

---

## 2. Подготовка репозитория

```bash
git clone git@github.com:company/scanner_infra.git
cd scanner_infra
git submodule update --init --recursive
```

Убедитесь, что `origin` ссылается на правильный репозиторий. Если используете форк, настройте upstream.

---

## 3. Настройка переменных окружения

### 3.1 Структура конфигураций

| Файл / источник                 | Назначение                      | Где используется                           |
| ------------------------------- | ------------------------------- | ------------------------------------------ |
| `.env.local`                    | Локальные переменные dev/stage  | `docker-compose.yml`, Python/Go сервисы    |
| `config/services/*.env.example` | Шаблоны для конкретных сервисов | Копируйте в `.env` и правьте под окружение |
| Vault (prod)                    | Секреты продакшена              | Автоматически синхронизируются CI/CD       |
| `config/feeds/*.yaml`           | Профили биржевых источников     | Ingestion (Go gateway)                     |
| `config/trailing/*.yaml`        | Профили трейлинга               | `tp1_trailing_orchestrator`                |
| `config/calibration/*.yaml`     | Параметры калибровки            | Auto calibration service                   |
| `config/analytics/*.yaml`       | Настройки аналитики V2/V3       | Analytics system                           |
| `crypto_conf_scorer_baseline.yaml` | Базовые параметры для крипты    | Signal scoring                             |

### 3.2 Создание `.env.local`

```bash
cp .env.example .env.local
```

Обновите ключевые переменные:

- `REDIS_CORE_URL=redis://scanner-redis-core:6379/0` (Shard 2)
- `REDIS_TICKS_URL=redis://scanner-redis-ticks:6379/0` (Shard 1)
- `REDIS_ANALYTICS_URL=redis://scanner-redis-analytics:6379/0` (Shard 3)
- `REDIS_STATE_URL=redis://scanner-redis-state:6379/0`
- `GATE_ENFORCE_LIST=G0,G1,G6,G10` (активные гейты)
- `JOURNAL_MODE=journal-first`
- `MT5_EVENT_TOKEN=<сгенерируйте UUID>`
- `MT5_ORDER_TOKEN=<сгенерируйте UUID>`
- `TELEGRAM_BOT_TOKEN=<тестовый токен>`
- `TELEGRAM_CHAT_ID=<ID>`
- `PROMETHEUS_BASIC_AUTH=<логин:пароль>`
- `GPU_COMPUTE_ENABLED=false` (для GPU acceleration)
- `ANALYTICS_V2_ENABLED=true` (для продвинутой аналитики)
- `AUTO_CALIBRATION_ENABLED=true` (для автоматической калибровки)
- `CUDA_VISIBLE_DEVICES=0` (GPU device ID)

Для генерации токенов используйте `openssl rand -hex 16` или `uuidgen`.

### 3.3 Конфигурация Vault (prod)

1. Получите доступ к Vault (`vault login`).
2. Выполните `make export-vault ENV=prod`, чтобы выгрузить секреты.
3. Проверьте, что файлы не попали в git (`.gitignore` уже настроен).

---

## 4. Быстрый старт (локальный dev)

### 4.1 Запуск инфраструктуры

```bash
make up-bg            # поднимает Redis, Prometheus, Grafana, вспомогательные сервисы
make status           # проверяет статус контейнеров
make diagnose         # healthchecks, Redis latency, dmesg
```

Ошибки запуска см. в `troubleshooting.md#docker-compose`.

### 4.2 Запуск основных сервисов

```bash
make signals-start        # G0-G15 Pipeline + ML Gating
make execution-start      # BinanceExecutor + ProjectionWorker
make supervisor-start      # BootstrapSupervisor
make news-agent-start     # News Agent (LLM)
make ml-gov-start         # ML Governance & Drift Detection
make track-v8-start       # SignalPerformanceTracker V8
make tick-ingest-v2-start # go-worker (V2) + janitor
```

Команды запускают контейнеры/процессы в фоне через supervisor. Логи доступны через `make <service>-logs`.

### 4.3 Реплей данных

```bash
make replay-ticks FILE=fixtures/binance_btcusdt_15m.json
```

Команда воспроизводит исторические тики в `stream:tick_btcusdt`. Подробнее в `data_flow.md#реплей-данных`.

---

## 5. Настройка MT5 демо-окружения

1. Установите MT5 терминал на отдельную VM или на локальный хост.
2. Скопируйте `mt5/TickBridge.mq5` в каталог `MQL5/Experts/scanner/`.
3. В настройках эксперта укажите:
   - URL REST сервера: `http://host.docker.internal:8003` (см. `docker-compose.mt5-executor.yml`).
   - Токен `MT5_EVENT_TOKEN` из `.env.local`.
4. Включите `Allow WebRequest` и добавьте URL из п.3.
5. Запустите эксперта на нужном символе и timeframe.

Проверка: `make mt5-test` отправит тестовый запрос и проверит `/events/mt5`.

---

## 6. Stage / Prod развертывание (обзор)

> Полный CI/CD описан в `CONFIGURATION.md#deploy`. Здесь приведён краткий чек-лист.

1. **Подготовка окружения**: Terraform/Ansible (если используется) создают сервера и сети.
2. **Секреты**: подтягиваются из Vault (`make export-vault ENV=prod` на CI).
3. **Docker Compose**:
   - Stage: `docker compose -f docker-compose.yml -f docker-compose.stage.yml up -d`
   - Prod: `docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d`
4. **Проверки**:
   - `make health-prod`
   - `make metrics-check`
   - `make redis-stats`
5. **Smoke-тесты**: `make gateway-test`, `make trailing-test`, `make tracker-smoke`.
6. **Нагрузка**: опционально `make load-test TRAILING=true`.

После деплоя обновите статус в `operations.md#релизные-процедуры`.

---

## 7. Обновление и миграции

### 7.1 Обновление сервисов

```bash
git pull
make build-go
make build-python
make up-bg
make rolling-restart
```

`make rolling-restart` перезапускает только изменённые сервисы (по конфигурации). Для критических фиксов используйте `make full-restart`.

### 7.2 Миграции Redis

1. Экспортируйте бэкап: `make backup-redis`.
2. Примените миграцию (скрипты лежат в `scripts/migrations/`).
3. Проверьте структуру ключей: `make redis-schema-check`.
4. При ошибках следуйте `troubleshooting.md#redis-disaster-recovery`.

---

## 8. Проверка здоровья и диагностика

| Команда                 | Назначение                                       |
| ----------------------- | ------------------------------------------------ |
| `make status`           | Статус контейнеров                               |
| `make diagnose`         | Сводная диагностика (health+метрики)             |
| `make tick-streams`     | Lag по `stream:tick_*`, состояние consumer групп |
| `make trailing-stats`   | Метрики трейлинга                                |
| `make tracker-stats`    | Состояние Signal Performance Tracker             |
| `make redis-stats`      | Общая статистика Redis                           |
| `make logs SERVICE=...` | Поток логов конкретного сервиса                  |

Если метрики не поступают, проверьте Prometheus: <http://localhost:9090>.

---

## 9. Отладка и локальное тестирование

- **Unit-тесты**:
  - Python: `make test-python`
  - Go: `make test-go`
- **Интеграционные сценарии**:
  - `make trailing-test`
  - `make tracker-smoke`
- **Линтеры**:
  - Python: `make lint-python`
  - Go: `make lint-go`
  - Документация: `make docs-lint`
- **Профилирование**:
  - Go: `make go-profiler SERVICE=go-worker`
  - Python: `make py-profiler SERVICE=aggregated_signal_hub_v2`

Результаты сохраняются в `artifacts/`.

---

## 10. Очистка окружения

```bash
make down           # останавливает контейнеры
make clean          # удаляет кэши, временные файлы
make reset-redis    # очищает Redis (dev), см. предупреждения
```

> Внимание: `make reset-redis` необратимо очищает локальные данные. Используйте только в dev.

---

## 11. Контрольный список перед первым запуском

- [ ] Docker и Compose установлены, сервис `docker` запущен.
- [ ] `.env.local` заполнен и не закоммичен.
- [ ] Выполнены `make up-bg`, `make diagnose`.
- [ ] MT5 TickBridge настроен (если требуется интеграция).
- [ ] Выполнен smoke-тест (`make trailing-test`, `make gateway-test`).
- [ ] Проверены дашборды Grafana (Trailling, Tick Streams, Tracker).

При успешном выполнении чек-листа переходите к `data_flow.md` и `operations.md`.
