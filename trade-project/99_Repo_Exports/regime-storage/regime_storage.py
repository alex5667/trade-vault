"""
regime_storage.py — служба для сохранения данных regime из Redis stream в PostgreSQL.

- читает из stream:regime на redis-worker-1
- сохраняет в таблицу regime_snapshot в scanner_analytics
- работает постоянно, обрабатывая новые сообщения
"""

import os
import json
import time
import redis
import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
from datetime import datetime

# Redis настройки (для чтения из stream:regime)
REDIS_HOST = os.getenv("REDIS_HOST", "redis-worker-1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_USERNAME = os.getenv("REDIS_USERNAME") or None
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

# Stream и consumer group
STREAM_NAME = "stream:regime"
CONSUMER_GROUP = "regime-storage-group"
CONSUMER_NAME = f"regime-storage-{os.getpid()}"

# База данных
DATABASE_URL = os.getenv("DATABASE_URL")

# Настройки обработки
READ_COUNT = int(os.getenv("REGIME_STORAGE_READ_COUNT", "50"))
READ_BLOCK_MS = int(os.getenv("REGIME_STORAGE_READ_BLOCK_MS", "5000"))

@contextmanager
def get_db_connection(max_retries: int = 3, base_delay: float = 1.0):
    """Контекстный менеджер для соединения с БД (с retry при transient ошибках)"""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    last_err: Exception = RuntimeError("no attempts made")
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            try:
                yield conn
            finally:
                conn.close()
            return
        except psycopg2.OperationalError as e:
            last_err = e
            delay = base_delay * (2 ** attempt)
            if attempt >= max_retries - 1:
                print(f"⚠️ DB connection failed (attempt {attempt + 1}/{max_retries}): {e}. Retry in {delay:.1f}s...")
            time.sleep(delay)
    raise last_err

def ensure_consumer_group(rclient):
    """Создает consumer group если его нет с retry логикой"""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            rclient.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
            print(f"✅ Consumer group '{CONSUMER_GROUP}' создан для '{STREAM_NAME}'")
            return
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                print(f"ℹ️ Consumer group '{CONSUMER_GROUP}' уже существует для '{STREAM_NAME}'")
                return
            else:
                print(f"❌ Ошибка создания consumer group (попытка {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
        except redis.exceptions.ConnectionError as e:
            print(f"❌ Ошибка подключения к Redis (попытка {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise
        except Exception as e:
            print(f"❌ Неожиданная ошибка создания consumer group: {e}")
            raise

def store_regime_batch(conn, regimes, batch_counter=None):
    """Сохраняет пачку regime данных в БД"""
    if batch_counter is None:
        batch_counter = [0]
    batch_counter[0] += 1

    # Выводим сообщение только каждые 10000 вызовов
    if batch_counter[0] % 10000 == 0:
        print(f"🗃️ store_regime_batch вызвана с {len(regimes)} записями (#{batch_counter[0]})")

    if not regimes:
        return

    # Дедупликация по (symbol, timeframe, ts) - берем последнее сообщение для каждого ключа
    # где ts - это конвертированный datetime для базы данных (используется для conflict resolution)
    deduped_regimes = {}
    duplicates_found = 0
    duplicate_log_counter = 0  # Счетчик для логирования дубликатов

    for regime in regimes:
        # Конвертируем timestamp в datetime для базы данных
        ts_db = datetime.fromtimestamp(regime['ts_event_ms'] / 1000.0)
        # Ключ дедупликации ДОЛЖЕН соответствовать ключу conflict resolution в БД: (symbol, timeframe, ts)
        key = (regime['symbol'], regime['timeframe'], ts_db)
        if key in deduped_regimes:
            duplicates_found += 1
            duplicate_log_counter += 1
            # Логируем только каждое 10000-е сообщение о дубликате
            if duplicate_log_counter % 10000 == 0:
                print(f"🔄 Дубликат найден для ключа: {key} (#{duplicate_log_counter})")
        # Берем последнее сообщение для каждого уникального ключа
        deduped_regimes[key] = (regime, ts_db)

    if duplicates_found > 0:
        # Выводим сообщение о дубликатах только каждые 10000 вызовов
        if batch_counter[0] % 10000 == 0:
            print(f"⚠️ Найдено {duplicates_found} дубликатов в пачке из {len(regimes)} записей (#{batch_counter[0]})")

    # Выводим сообщение о дедупликации только каждые 10000 вызовов
    if batch_counter[0] % 10000 == 0:
        print(f"📊 После дедупликации: {len(deduped_regimes)} уникальных записей из {len(regimes)} полученных")

    with conn.cursor() as cur:
        # Подготовка данных для вставки
        values = []
        for key, (regime, ts_db) in deduped_regimes.items():
            symbol, timeframe, _ = key
            values.append((
                symbol,
                timeframe,
                ts_db,  # уже конвертированный datetime
                regime.get('adx'),
                regime.get('atrPct'),  # Fixed: match regime-worker payload
                regime.get('regime'),
                regime.get('trend_score', 0.0),
                regime.get('range_score', 0.0),
                regime.get('atr'),    # Fixed: match regime-worker payload
                regime.get('atr_quantile'),
                regime.get('volatility_state'),
                regime.get('is_trending', False)
            ))

        # INSERT с игнорированием дубликатов
        query = """
            INSERT INTO regime_snapshot
                (symbol, timeframe, ts, adx, "atrPct", regime, trend_score, range_score,
                 atr_value, atr_quantile, volatility_state, is_trending)
            VALUES %s
            ON CONFLICT (symbol, timeframe, ts)
            DO NOTHING
        """

        try:
            # Используем индивидуальные INSERT вместо batch для избежания конфликтов дубликатов
            saved_count = 0
            for value_tuple in values:
                try:
                    # Создаем однострочный INSERT
                    single_query = """
                        INSERT INTO regime_snapshot
                            (symbol, timeframe, ts, adx, "atrPct", regime, trend_score, range_score,
                             atr_value, atr_quantile, volatility_state, is_trending)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, timeframe, ts)
                        DO NOTHING
                    """
                    cur.execute(single_query, value_tuple)
                    saved_count += 1
                except Exception as single_e:
                    print(f"⚠️ Ошибка сохранения одной записи: {single_e}")

            conn.commit()
            # Выводим сообщение об обработке только каждые 10000 вызовов
            if batch_counter[0] % 10000 == 0:
                print(f"💾 Обработано {len(values)} записей regime (из {len(regimes)} полученных)")
        except Exception as e:
            print(f"❌ Ошибка сохранения в БД: {e}")
            import traceback
            traceback.print_exc()
            raise

def process_message_batch(rclient, messages, message_log_counter, batch_counter=None, stream_message_counter=None):
    """Обрабатывает пачку сообщений из Redis stream"""
    regimes = []
    acks: list[tuple] = []  # (stream_name, message_id) — ack only after successful DB write

    if batch_counter is None:
        batch_counter = [0]
    if stream_message_counter is None:
        stream_message_counter = [0]

    # Выводим сообщение о получении пачки только каждые 10000 вызовов
    batch_counter[0] += 1
    if batch_counter[0] % 10000 == 0:
        print(f"📨 Получена пачка из {len(messages)} stream'ов (#{batch_counter[0]})")

    for stream_name, stream_messages in messages:
        # Выводим сообщение о стриме только каждые 10000 вызовов
        stream_message_counter[0] += 1
        if stream_message_counter[0] % 100000 == 0:
            print(f"📨 Stream {stream_name}: {len(stream_messages)} сообщений")
        for message_id, fields in stream_messages:
            try:
                # fields содержит {"data": json_string}
                data_str = fields.get('data', '{}')
                regime_data = json.loads(data_str)

                # Логируем детально только каждое 10000-е сообщение
                message_log_counter[0] += 1
                if message_log_counter[0] % 10000 == 0:
                    print(f"📄 Сообщение {message_id[:10]}...: {regime_data.get('symbol', '?')}@{regime_data.get('timeframe', '?')} ts={regime_data.get('ts_event_ms', '?')}")

                # Добавляем в пачку для сохранения
                regimes.append(regime_data)
                acks.append((stream_name, message_id))

            except Exception as e:
                print(f"❌ Ошибка обработки сообщения {message_id}: {e}")
                # Подтверждаем только сообщения с ошибками парсинга (не данные БД)
                rclient.xack(stream_name, CONSUMER_GROUP, message_id)

    # Выводим сообщение о сборе записей только каждые 10000 вызовов
    if batch_counter[0] % 100000 == 0:
        print(f"📊 Собирано {len(regimes)} записей regime для сохранения")

    # Сохраняем пачку в БД — xack только после успешной записи
    if regimes:
        try:
            with get_db_connection() as conn:
                store_regime_batch(conn, regimes, batch_counter)
            # DB write succeeded — now safe to acknowledge
            ack_failed = []
            for stream_name, message_id in acks:
                try:
                    rclient.xack(stream_name, CONSUMER_GROUP, message_id)
                except Exception as ack_e:
                    print(f"⚠️ xack failed for {message_id}: {ack_e}")
                    ack_failed.append((stream_name, message_id))
            # Retry failed XACKs once after reconnect
            if ack_failed:
                try:
                    rclient = _connect_redis_with_retry()
                    for stream_name, message_id in ack_failed:
                        try:
                            rclient.xack(stream_name, CONSUMER_GROUP, message_id)
                        except Exception:
                            print(f"⚠️ xack retry also failed for {message_id}, будет redelivered")
                except Exception:
                    print(f"⚠️ reconnect for xack retry failed, {len(ack_failed)} messages будут redelivered")
        except Exception as e:
            print(f"❌ Ошибка сохранения в БД: {e}")
            import traceback
            traceback.print_exc()
            # Do NOT xack — messages will be redelivered on next iteration

def _make_redis_client() -> redis.Redis:
    """Создаёт новый Redis-клиент (с новым connection pool)."""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=5,
        max_connections=10,
        health_check_interval=30,
        socket_keepalive=True,
        # NOTE: socket_keepalive_options удалены - вызывали Error 22 (EINVAL) в Docker
        retry_on_error=[
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
        ],
    )


def _connect_redis_with_retry() -> redis.Redis:
    """Бесконечный retry с экспоненциальным backoff до успешного PING.

    Пересоздаёт клиент при каждой попытке, чтобы сбросить устаревший
    connection pool (актуально после пересоздания контейнера redis-worker-1
    с новым внутренним IP — errno 113 / EHOSTUNREACH).
    """
    delay = 1.0
    attempt = 0
    while True:
        client = _make_redis_client()
        try:
            client.ping()
            if attempt > 0:
                print(f"✅ Подключение к Redis восстановлено (попытка {attempt + 1})")
            else:
                print("✅ Подключение к Redis успешно")
            return client
        except Exception as e:
            attempt += 1
            print(f"❌ Redis недоступен (попытка {attempt}): {e}. Повтор через {delay:.0f}с...")
            time.sleep(delay)
            delay = min(delay * 2, 30.0)


def main():
    print("🚀 Regime Storage запущен")
    print(f"   Читает из: {STREAM_NAME} (redis:{REDIS_PORT})")
    print(f"   Пишем в: scanner_analytics.regime_snapshot")

    # Подключаемся к Redis — бесконечный retry до успеха
    print("🔍 Тестируем подключение к Redis...")
    rclient = _connect_redis_with_retry()

    # Создаем consumer group
    ensure_consumer_group(rclient)

    # Счетчики для статистики
    processed_count = 0
    last_stats_time = time.time()
    last_batch_stats_count = 0
    message_log_counter = [0]
    batch_counter = [0]
    stream_message_counter = [0]

    while True:
        try:
            # Читаем пачку сообщений
            messages = rclient.xreadgroup(
                CONSUMER_GROUP,
                CONSUMER_NAME,
                {STREAM_NAME: '>'},
                count=READ_COUNT,
                block=READ_BLOCK_MS
            )

            if not messages:
                # Периодическая статистика ожидания
                if time.time() - last_stats_time > 60:
                    print(f"ℹ️ Ожидание данных... (обработано: {processed_count})")
                    last_stats_time = time.time()
                continue

            # Обрабатываем сообщения
            process_message_batch(rclient, messages, message_log_counter, batch_counter, stream_message_counter)
            batch_size = len(messages[0][1]) if messages else 0
            processed_count += batch_size

            # Статистика каждые 100000 обработанных сообщений
            if processed_count // 100000 > last_batch_stats_count // 100000:
                print(f"📊 Статистика: обработано {processed_count} сообщений (пачка: {batch_size})")
                last_batch_stats_count = processed_count

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            # errno 113 (EHOSTUNREACH) или таймаут — контейнер redis-worker-1 перезапустился
            # и получил новый внутренний IP. Пересоздаём клиент целиком.
            print(f"❌ Redis connection lost: {e}. Переподключаемся...")
            rclient = _connect_redis_with_retry()
            # consumer group пересоздадим на случай сброса stream
            ensure_consumer_group(rclient)

        except Exception as e:
            print(f"❌ Неожиданная ошибка: {e}")
            if "NOGROUP" in str(e).upper():
                print("⚠️ Consumer group не найден, пересоздаём...")
                ensure_consumer_group(rclient)
            time.sleep(1)

def test_individual_insert():
    """Тестовый запуск для проверки индивидуальных INSERT"""
    # Тестовые данные с дубликатами
    test_regimes = [
        {
            'symbol': 'TEST1',
            'timeframe': '1m',
            'ts_event_ms': 1735286789999,
            'adx': 25.0,
            'atr_pct': 0.5,
            'regime': 'range',
            'trend_score': 0.0,
            'range_score': 1.0,
            'atr_value': 0.1,
            'atr_quantile': 0.3,
            'volatility_state': 'low',
            'is_trending': False
        },
        {
            'symbol': 'TEST1',  # Дубликат по (symbol, timeframe, ts_event_ms)
            'timeframe': '1m',
            'ts_event_ms': 1735286789999,
            'adx': 30.0,  # Другие значения
            'atr_pct': 0.6,
            'regime': 'trending_bull',
            'trend_score': 1.0,
            'range_score': 0.0,
            'atr_value': 0.12,
            'atr_quantile': 0.4,
            'volatility_state': 'medium',
            'is_trending': True
        },
        {
            'symbol': 'TEST2',
            'timeframe': '1m',
            'ts_event_ms': 1735286799999,
            'adx': 40.0,
            'atr_pct': 0.8,
            'regime': 'trending_bear',
            'trend_score': -1.0,
            'range_score': 0.0,
            'atr_value': 0.15,
            'atr_quantile': 0.6,
            'volatility_state': 'high',
            'is_trending': True
        }
    ]

    print("🧪 Тестируем индивидуальные INSERT с дубликатами...")
    try:
        with get_db_connection() as conn:
            store_regime_batch(conn, test_regimes, [0])  # batch_counter for test
        print("✅ Тест пройден успешно!")
    except Exception as e:
        print(f"❌ Тест провален: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_individual_insert()
    else:
        main()
