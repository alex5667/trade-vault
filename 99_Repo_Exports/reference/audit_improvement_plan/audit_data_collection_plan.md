# Требуемые данные и источники для плана улучшений

## Краткое резюме
Для формирования детального плана улучшений платформы необходимы три класса информации:
1. **Статический контекст** (код, конфиги, схемы, CI/CD) – для восстановления архитектуры, контрактов сообщений и параметров.
2. **Динамические данные** (очереди Redis, снимки состояния, логи исполнения/ошибок) – для выявления узких мест, дубликатов ордеров, частичных филлов и проверки обработки внештатных ситуаций (н-р, HTTP 503).
3. **История торговли и рынков** (сделки/PnL/fees, свечи, тики, задержки) – для оценки эффективности стратегий и расчета метрик (Sharpe/MDD/expectancy) со статистической проверкой во избежание data-snooping.

Данные собираются в порядке приоритета: Обязательно (критично для корректности) -> Желательно -> Опционально.

---

## Таблица приоритетов собираемых данных

| Артефакт/данные | Точные имена/источники | Приоритет | Мин. объём/период | Формат/пример | Чувствительность | Влияние (0–10) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Код и конфиги (исполнитель)** | `binance_executor.py`, `binance_futures_client.py` и т.д. + `docker-compose-binance.yml` | Обязательно | текущие версии | исходный код | можно | 9 |
| **Контракт Outbox/Gates** | `crypto_orderflow_service.py`, `outbox/*.py`, `docker-compose/shared` | Желательно | текущие | исходный код | можно | 7 |
| **Архитектурные доки** | `architecture/*.md` | Желательно | текущие | Markdown | можно | 6 |
| **Redis: очередь binance** | `orders:queue:binance`, `:processing`, `:dlq` | Обязательно | последние 1000–10000 | raw JSON строка на запись | конфид. (hash) | 10 |
| **Redis: stream выполнения** | `orders:exec` | Обязательно | ~5000–10000 записей | raw XREVRANGE + CSV | конфид. | 10 |
| **Redis: state по sid** | `orders:state:{sid}` | Обязательно | ~100–300 ключей | JSON (GET) | конфид. | 9 |
| **Логи исполнения** | stdout фреймворка, `binance-executor.log` | Обязательно | 7–30 дней | raw + выделенные ошибки (grep) | конфид. | 10 |
| **Логи ошибок/инцидентов** | grep `ERROR | WARN | -4120 | -1021 | -2021 | 503` | Обязательно | 7–30 дней | raw логи | конфид. | 10 |
| **История сделок/fills** | таблица/выгрузка из БД (Postgres) | Обязательно | 90–180 дней | CSV/Parquet | конфид. | 10 |
| **PnL/fees/funding** | таблицы PnL, fees, funding | Обязательно | 90–180 дней | CSV/Parquet | конфид. | 10 |
| **Рыночные данные** | источники данных (API/хранилище) | Обязательно | 90–180 дней (тики: 7-30) | Parquet/CSV | можно | 9 |
| **Latency (тайминги)** | `enqueue_ts`, `api_req_ts`, `ack_ts`, `ws_event_ts` | Желательно | 7–30 дней | CSV | можно | 8 |
| **CI/CD манифесты** | `docker-compose-*.yml`, `Dockerfile`, `.github/workflows` | Желательно | текущие | YAML | секреты вырезать | 8 |
| **Мониторинг/алерты** | `prometheus/*`, конфиги Grafana | Желательно | текущие + 30д метрик | YAML + графики | можно | 6 |
| **Политики безопасности** | Vault/KMS, RBAC, audit-логи | Желательно | текущие | MD/PDF + скриншоты | секреты нельзя | 8 |

---

## Что необходимо сделать по каждому из разделов

### 1. Анализ текущих файлов архива
**Необходимые данные:** Полный код репозитория (особенно `binance_executor.py`, `crypto_orderflow_service.py`, модули `outbox`), `docker-compose`/манифесты, схемы БД. (Текущая версия + предыдущий релиз, если был).
**Обоснование:** Подтверждение архитектуры (Go → Redis → Python → NestJS → UI), точек входа/выхода и параметров конфигурации.

### 2. Разбор алгоритма открытия/закрытия сделок
**Необходимые данные:** Срезы очередей `orders:queue:binance`, `orders:exec` стрима, `orders:state:*` ключей и логи исполнителя (`binance-executor`). (Желательно несколько тысяч сообщений и 30 дней логов).
**Обоснование:** Проверка идемпотентности, политик retry, обработки частичных fill-ов, SL/TP и логики hedge-режима.

### 3. Сравнение с лучшими практиками
**Необходимые данные:** Реальные случаи с ошибками и лимитами, конфигурации API. (30–90 дней логов, включающих периоды пиковых нагрузок). Aggregated table для `error_code -> count`.
**Обоснование:** Проверка соответствия рекомендациям Binance API (корректная обработка HTTP 503 без дублирования, использование новых Algo API ендпоинтов под SL/TP для предотвращения -4120, корректная настройка positionSide).

### 4. Набор стратегий и оценка
**Необходимые данные:** История сделок/сигналов, PnL и рыночные данные (OHLCV, тики). За 90-180 дней.
**Обоснование:** Расчет ключевых метрик (Sharpe, max drawdown, expectancy) и оценка реалистичности «минимального риска«. Использование out-of-sample проверок во избежание data-snooping.

### 5. План изменений и трудозатрат
**Необходимые данные:** CI/CD манифесты, тесты, dev/prod настройки окружения (`.env.dev`, `.env.prod`).
**Обоснование:** Оценка объемов работ по добавлению кода, тестов и мониторинга, а также процесса деплоя (rollout/rollback).

### 6. Логирование, безопасность, комплаенс
**Необходимые данные:** Политики управления секретами, журнал доступа (audit trail), документы RBAC и retention policies для логов.
**Обоснование:** Гарантия безопасности ключей и изоляция доступов. Наличие audit trail для восстановления хронологии в случае инцидентов.

---

## Методология экспорта и скрипты

### Выгрузка из Redis
Скрипт выгрузит необходимые ключи (сообщения очереди, стримы, стейт-ключи):
```bash
#!/usr/bin/env bash
set -euo pipefail

REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"
OUT="redis_export"
mkdir -p "$OUT"

QUEUE="${ORDERS_QUEUE_BINANCE:-orders:queue:binance}"
PROC="${ORDERS_QUEUE_BINANCE_PROCESSING:-orders:queue:binance:processing}"
DLQ="${ORDERS_QUEUE_BINANCE_DLQ:-orders:queue:binance:dlq}"
STREAM="${EXEC_STREAM:-orders:exec}"
STATE_PREF="${ORDERS_STATE_KEY_PREFIX:-orders:state:}"

N=2000
NS=100

redis-cli -u "$REDIS_URL" LRANGE "$QUEUE" -$N -1 > "$OUT/queue_last_$N.jsonl"
redis-cli -u "$REDIS_URL" LRANGE "$PROC" 0 -1 > "$OUT/queue_processing.jsonl"
redis-cli -u "$REDIS_URL" LRANGE "$DLQ" -$N -1 > "$OUT/queue_dlq_last_$N.jsonl"

redis-cli -u "$REDIS_URL" XREVRANGE "$STREAM" + - COUNT 10000 > "$OUT/stream_orders_exec.txt"

redis-cli -u "$REDIS_URL" --scan --pattern "${STATE_PREF}*" | head -n "$NS" > "$OUT/state_keys.txt"
while read k; do
  redis-cli -u "$REDIS_URL" GET "$k" > "$OUT/$(echo $k | tr ':/ ' '__').json"
done < "$OUT/state_keys.txt"

echo "Redis data exported to $OUT/"
```

### Выгрузка логов (Docker)
```bash
docker logs --since 30d binance-executor > executor_30d.log
docker logs --since 30d binance-executor 2>&1 | grep -E "ERROR|WARN|-4120|-1021|503|429" > executor_30d_errors.log
```

### Мониторинг
```bash
kubectl get cm prometheus-rules -o yaml > prometheus_rules.yml
```

---

## Требования к безопасности при выгрузке данных (Санитаризация)
1. **Удалить все токены**: grep-ом исключить и стереть API_KEY | SECRET | TOKEN | PASSWORD | PRIVATE_KEY | listenKey.
2. **Анонимизация IP/ID**: Заменить реальные адреса на placeholders (192.168.x.x -> X.X.X.X, Binance ID -> BINANCE).
3. **Хэширование**: orderId/algoId/clientAlgoId захэшировать sha256 с солью (соль не передавать).
4. **Масштабирование**: умножить или разделить данные qty/PnL на коэффициент для их шифровки.
5. Зашифровать полученный архив перед отправкой:
   ```bash
   age -p -o project_audit_bundle.zip.age project_audit_bundle.zip
   ```

## Формат и сроки передачи
Ожидается ZIP-архив вида:
```
project_audit_bundle/
  README.md  
  SANITIZATION_NOTES.md  
  provided_code/ (исходный код, docker, конфиги)  
  redis/ (содержимое очередей, XREVRANGE)
  logs/ (executor_30d_errors.log, *.log)
  trading_data/ (история из БД)
  infra/ (мониторинг, github-actions)
```
Ориентировочное время сбора данных: от 1 до 8 часов (в зависимости от объема).
