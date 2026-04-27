# Исправление Ошибки Tick Ingest Server

## Проблема
Ошибка при запуске контейнера `scanner-tick-ingest`:
```
ImportError: cannot import name 'XAU_BOOK_STREAM' from 'core.config'
```

Uvicorn не мог загрузить приложение `services.tick_ingest_server:app` из-за отсутствия константы `XAU_BOOK_STREAM` в `core.config`.

## Решение
Добавлены недостающие константы для XAU book stream в `core/config.py`.

## Добавленные Константы

### XAU Book Stream Configuration
```python
# XAU book stream configuration
XAU_BOOK_STREAM: str = os.getenv("XAU_BOOK_STREAM", "stream:book_XAUUSD")
XAU_BOOK_STREAM_MAXLEN: int = int(os.getenv("XAU_BOOK_STREAM_MAXLEN", "20000"))
```

## Использование

### В tick_ingest_server.py
```python
from core.config import (
    XAU_TICK_STREAM,
    XAU_TICK_STREAM_MAXLEN,
    XAU_BOOK_STREAM,        # <- добавлено
    XAU_BOOK_STREAM_MAXLEN, # <- добавлено
)

TICK_STREAM = XAU_TICK_STREAM
BOOK_STREAM = XAU_BOOK_STREAM  # <- теперь работает
MAXLEN = XAU_TICK_STREAM_MAXLEN
```

## Настройка через ENV

```bash
# Stream именования
XAU_TICK_STREAM=stream:tick_XAUUSD
XAU_BOOK_STREAM=stream:book_XAUUSD

# Максимальные длины
XAU_TICK_STREAM_MAXLEN=50000
XAU_BOOK_STREAM_MAXLEN=20000
```

## Проверка

```bash
# Тест констант
cd python-worker && python3 -c "
from core.config import XAU_BOOK_STREAM, XAU_BOOK_STREAM_MAXLEN
print('✅ Constants:', XAU_BOOK_STREAM, XAU_BOOK_STREAM_MAXLEN)
"

# Тест приложения
cd python-worker && python3 -c "
from services.tick_ingest_server import app
print('✅ App loaded:', type(app), app.title)
"
```

## Результат

✅ Контейнер `scanner-tick-ingest` теперь запускается без ошибок импорта
✅ Uvicorn успешно загружает FastAPI приложение
✅ Tick Ingest Server полностью функционален
✅ Поддержка приема как тиков, так и order book данных

## Архитектура Tick Ingest Server

Сервис предоставляет HTTP API для приема данных от MT5 EA:

- `POST /tick` - прием тиковых данных
- `POST /book` - прием order book данных
- `GET /health` - health check

Данные публикуются в Redis Streams для дальнейшей обработки консьюмерами.

## Запуск

```bash
# Через docker-compose
docker-compose up tick-ingest-server

# Через uvicorn напрямую
cd python-worker && uvicorn services.tick_ingest_server:app --host 0.0.0.0 --port 8087
```

## Мониторинг

```bash
# Health check
curl http://localhost:8087/health

# Логи
docker logs scanner-tick-ingest
```
