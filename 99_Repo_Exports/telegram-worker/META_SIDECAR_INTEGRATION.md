# Интеграция Meta-Sidecar в Telegram Worker

## Обзор

Реализована "железобетонная" интеграция для чтения meta-sidecar (config_params) из Redis в telegram-worker. Это позволяет передавать конфигурационные параметры из python-worker в Telegram уведомления без раздувания payload'а сигналов.

## Архитектура

### 1. Python Worker (OutboxWriter)
- Сохраняет `config_params` отдельно от payload сигнала
- Ключ: `signal:meta:<signal_id>` (JSON)
- Payload содержит только `signal_id` для ссылки

### 2. Telegram Worker (Notify Worker)
- Читает из `notify:telegram` stream
- Для каждого сигнала извлекает `signal_id`
- Делает дополнительный GET по ключу `signal:meta:<signal_id>`
- Прикрепляет `config_params` к сообщению

### 3. Telegram Bot (Improved Notifier)
- Отображает `config_params` в отдельном блоке "🧩 Config Params (meta):"
- Ограничивает количество параметров для читаемости

## Реализованные компоненты

### `telegram_worker/outbox_meta.py`
- `fetch_outbox_meta(redis, signal_id)` - чтение meta из Redis
- `attach_outbox_meta(redis, parsed)` - прикрепление meta к parsed

### `telegram_worker/notify_worker.py`
- `_attach_outbox_meta(redis, entry, parsed, raw)` - локальный helper
- Интегрирован в обработку outbox сигналов
- Используется при парсинге `signal_payload`

### `telegram_worker/improved_notifier.py`
- `format_signal_message()` уже содержит вывод config_params
- Блок "🧩 **Config Params (meta):**\n" с ограничением количества параметров

## Переменные окружения

### Управление поведением
- `TG_INCLUDE_CONFIG_PARAMS=1` - включить/выключить прикрепление meta (по умолчанию: 1)
- `TG_CONFIG_PARAMS_MAX_KEYS=25` - максимальное количество ключей config_params (по умолчанию: 25)

### Настройка Redis
- `REDIS_URL=redis://redis-worker-1:6379/0` - URL Redis (настроен в docker-compose)
- `OUTBOX_META_PREFIX=signal:meta:` - префикс ключей meta (по умолчанию)

### Отладка
- `OUTBOX_META_DEBUG_ATTACH=1` - прикреплять meta к raw для отладки

## Поток данных

```
Python Worker:
1. Генерирует сигнал с config_params
2. Сохраняет payload в outbox stream
3. Сохраняет meta: {"config_params": {...}} по ключу signal:meta:<signal_id>

Notify Worker:
1. Читает сигнал из notify:telegram
2. Извлекает signal_id из payload
3. Делает GET signal:meta:<signal_id>
4. Прикрепляет config_params к parsed/raw

Telegram Bot:
1. Форматирует сообщение
2. Добавляет блок "Config Params (meta):" если есть config_params
3. Отправляет в Telegram
```

## Тестирование

### Проверка интеграции
```bash
cd telegram-worker
python test_meta_integration.py
```

### Ручная проверка
1. Запустить систему: `docker-compose up -d`
2. Проверить логи notify-worker: `docker logs scanner-notify-worker`
3. Найти сообщения с "📊 Outbox signal:" - там будет информация о meta
4. Проверить сообщения в Telegram - должен быть блок "🧩 Config Params (meta):"

### Отладка
- Включить `OUTBOX_META_DEBUG_ATTACH=1` для просмотра полного meta в логах
- Проверить Redis: `redis-cli GET signal:meta:<signal_id>`

## Безопасность и надежность

- **Fail-open**: если Redis недоступен/ключ не найден - сигнал все равно отправляется
- **Ограничение размера**: config_params ограничиваются по количеству ключей
- **Timeout**: Redis операции не блокируют основной поток
- **Валидация**: JSON парсинг с fallback на пустой dict

## Совместимость

- Полная обратная совместимость: сигналы без meta работают как прежде
- Meta опциональна: если python-worker не пишет meta - ничего не ломается
- Контракт 1:1 с OutboxWriter из python-worker
