# OFConfirm Golden Replay - Systemd и Docker Integration

## Обзор

OFConfirm Golden Replay обеспечивает детерминированный контроль качества сигналов через сравнение baseline и candidate результатов. Система поддерживает два варианта развёртывания:

1. **Docker контейнер** (рекомендуется) - через `docker-compose-timers.yml`
2. **Systemd unit** - для bare-metal deployment

Оба варианта полностью синхронизированы и используют одинаковые переменные окружения и параметры.

---

## Вариант 1: Docker контейнер (рекомендуется)

### Конфигурация

Файл: `docker-compose-timers.yml` → `trade-of-confirm-golden-replay-timer`

### Запуск

```bash
# Запуск контейнера
docker-compose -f docker-compose-timers.yml up -d trade-of-confirm-golden-replay-timer

# Просмотр логов
docker-compose -f docker-compose-timers.yml logs -f trade-of-confirm-golden-replay-timer

# Остановка
docker-compose -f docker-compose-timers.yml stop trade-of-confirm-golden-replay-timer
```

### Расписание

Контейнер запускается автоматически каждый день в **03:15-03:25 UTC**.

### Переменные окружения

Все переменные можно переопределить через `.env` файл или docker-compose override:

```bash
# Основные настройки
OF_INPUTS_STREAM=signals:of:inputs
OF_INPUTS_STREAM_FIELD=payload
OF_INPUTS_SINCE_HOURS=24
OF_INPUTS_MAX_RECORDS=200000

# Replay настройки
OF_REPLAY_OUT_DIR=/var/lib/trade/of_replay
OF_REPLAY_BASELINE=/var/lib/trade/of_replay/baseline.ndjson
OF_REPLAY_FAIL_ON_MISMATCH=1

# Уведомления (интегрировано из systemd unit)
OF_REPLAY_NOTIFY=1
NOTIFY_TELEGRAM_STREAM=notify:telegram
```

---

## Вариант 2: Systemd unit (bare-metal)

### Установка

```bash
# Копирование unit файла
sudo cp systemd/trade-of-confirm-golden-replay.service /etc/systemd/system/

# Перезагрузка systemd
sudo systemctl daemon-reload

# Включение и запуск
sudo systemctl enable trade-of-confirm-golden-replay.service
sudo systemctl start trade-of-confirm-golden-replay.service
```

### Управление

```bash
# Статус
sudo systemctl status trade-of-confirm-golden-replay.service

# Логи
sudo journalctl -u trade-of-confirm-golden-replay.service -f

# Запуск вручную
sudo systemctl start trade-of-confirm-golden-replay.service

# Остановка
sudo systemctl stop trade-of-confirm-golden-replay.service
```

### Конфигурация

Все переменные окружения определены в unit файле и могут быть переопределены через:
- `/home/alex/front/trade/scanner_infra/python-worker/.env` (EnvironmentFile)
- Прямое редактирование unit файла

---

## Синхронизация конфигурации

### Переменные окружения

Оба варианта используют одинаковые переменные:

| Переменная | Docker | Systemd | Описание |
|-----------|--------|---------|----------|
| `OF_INPUTS_STREAM` | ✅ | ✅ | Redis stream для inputs |
| `OF_INPUTS_STREAM_FIELD` | ✅ | ✅ | Поле с payload |
| `OF_INPUTS_SINCE_HOURS` | ✅ | ✅ | Временное окно (часы) |
| `OF_INPUTS_MAX_RECORDS` | ✅ | ✅ | Максимум записей |
| `OF_REPLAY_OUT_DIR` | ✅ | ✅ | Директория для артефактов |
| `OF_REPLAY_BASELINE` | ✅ | ✅ | Путь к baseline файлу |
| `OF_REPLAY_FAIL_ON_MISMATCH` | ✅ | ✅ | Падать при mismatch (0/1) |
| `OF_REPLAY_NOTIFY` | ✅ | ✅ | Включить уведомления (0/1) |
| `NOTIFY_TELEGRAM_STREAM` | ✅ | ✅ | Redis stream для Telegram |
| `PYTHONUNBUFFERED` | ✅ | ✅ | Небуферизованный вывод |

### Параметры команды

Оба варианта используют одинаковые параметры:

```bash
--redis-url ${REDIS_URL}
--out-dir ${OF_REPLAY_OUT_DIR}
--stream ${OF_INPUTS_STREAM}
--field ${OF_INPUTS_STREAM_FIELD}
--since-hours ${OF_INPUTS_SINCE_HOURS}
--max-records ${OF_INPUTS_MAX_RECORDS}
--state-file ${OF_INPUTS_STATE_FILE}
--resume ${OF_INPUTS_RESUME}
--baseline ${OF_REPLAY_BASELINE}
--fail-on-mismatch ${OF_REPLAY_FAIL_ON_MISMATCH}
--notify ${OF_REPLAY_NOTIFY}
--notify-stream ${NOTIFY_TELEGRAM_STREAM}
```

---

## Функциональность

### Что делает Golden Replay

1. **Экспорт inputs** из Redis stream `signals:of:inputs`
2. **Replay** - воспроизведение логики OFConfirm на inputs
3. **Diff** - сравнение baseline vs candidate
4. **Уведомление** - отправка алерта в Telegram при mismatch

### Артефакты

Все артефакты сохраняются в `${OF_REPLAY_OUT_DIR}`:

- `of_inputs.ndjson` - экспортированные inputs
- `of_replay_candidate.ndjson` - результаты replay
- `of_replay_diff.json` - отчёт о различиях
- `of_replay_debug.ndjson` - debug информация

### Exit codes

- `0` - успех, нет mismatches
- `1` - ошибка инфраструктуры (export/replay/diff tool failed)
- `2` - mismatch обнаружен (если `OF_REPLAY_FAIL_ON_MISMATCH=1`)

### Уведомления

При обнаружении mismatch отправляется сообщение в `notify:telegram` с:
- Количеством missing_in_baseline, missing_in_candidate, mismatches
- Типами mismatches (mismatch_types)
- Top groups с наибольшим количеством mismatches
- Sample keys для расследования

---

## Создание baseline

Перед первым запуском необходимо создать baseline:

```bash
# Создать директорию
sudo mkdir -p /var/lib/trade/of_replay
sudo chown -R $USER:$USER /var/lib/trade/of_replay

# Экспорт inputs (72 часа, 300k записей для стабильного baseline)
python -m tools.export_of_confirm_inputs_ndjson \
  --stream signals:of:inputs \
  --field payload \
  --since-hours 72 \
  --max-records 300000 \
  --out /var/lib/trade/of_replay/of_inputs.ndjson \
  --resume 0

# Replay для создания baseline
python -m tools.of_confirm_replay_from_inputs \
  --inputs /var/lib/trade/of_replay/of_inputs.ndjson \
  --out /var/lib/trade/of_replay/baseline.ndjson \
  --debug-out /var/lib/trade/of_replay/baseline_debug.ndjson
```

---

## Проверка работы

### Ручной запуск

```bash
python -m tools.golden_replay_of_confirm_from_redis \
  --out-dir /var/lib/trade/of_replay \
  --stream signals:of:inputs \
  --field payload \
  --since-hours 24 \
  --max-records 200000 \
  --baseline /var/lib/trade/of_replay/baseline.ndjson \
  --fail-on-mismatch 1 \
  --notify 1

# Проверка exit code
echo $?

# Проверка уведомлений в Redis
redis-cli XREVRANGE notify:telegram + - COUNT 3
```

### Проверка артефактов

```bash
# Просмотр diff report
cat /var/lib/trade/of_replay/of_replay_diff.json | jq .

# Проверка количества mismatches
cat /var/lib/trade/of_replay/of_replay_diff.json | jq '.mismatches'
```

---

## Troubleshooting

### Контейнер не запускается

```bash
# Проверить логи
docker-compose -f docker-compose-timers.yml logs trade-of-confirm-golden-replay-timer

# Проверить переменные окружения
docker-compose -f docker-compose-timers.yml config | grep -A 20 trade-of-confirm-golden-replay-timer
```

### Systemd unit не запускается

```bash
# Проверить статус
sudo systemctl status trade-of-confirm-golden-replay.service

# Проверить логи
sudo journalctl -u trade-of-confirm-golden-replay.service -n 50

# Проверить синтаксис unit файла
sudo systemd-analyze verify trade-of-confirm-golden-replay.service
```

### Mismatches обнаружены

1. Проверить `of_replay_diff.json` для деталей
2. Проверить `of_replay_debug.ndjson` для debug информации
3. Проверить уведомления в `notify:telegram` stream
4. Если mismatches ожидаемы (например, после изменения логики), обновить baseline

---

## Миграция между вариантами

### Из Docker в Systemd

1. Остановить Docker контейнер
2. Установить systemd unit (см. раздел "Установка")
3. Убедиться, что переменные окружения совпадают

### Из Systemd в Docker

1. Остановить systemd service
2. Запустить Docker контейнер
3. Убедиться, что переменные окружения совпадают

---

## См. также

- `python-worker/tools/golden_replay_of_confirm_from_redis.py` - основной скрипт
- `python-worker/tools/of_confirm_diff_report.py` - diff report tool
- `python-worker/tests/test_golden_replay_of_confirm_from_redis.py` - тесты
















