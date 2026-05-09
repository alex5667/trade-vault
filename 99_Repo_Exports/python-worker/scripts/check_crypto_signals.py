#!/usr/bin/env python3
"""
Диагностический скрипт для проверки цепочки генерации сигналов CryptoOrderFlow.

Проверяет:
1. Наличие тиков в Redis streams
2. Наличие сигналов в signals:crypto:raw
3. Наличие сигналов в signals:cryptoorderflow:{symbol}
4. Наличие сообщений в notify:telegram
5. Значение счетчика гейтинга
6. Конфигурацию символов
"""

import json
import os
import sys
from datetime import UTC, datetime
from core.redis_keys import RedisStreams as RS

# Добавляем путь к корню проекта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import redis
except ImportError:
    print("❌ redis-py не установлен. Установите: pip install redis")
    sys.exit(1)


def get_redis_client(url: str) -> redis.Redis:
    """Создает Redis клиент."""
    try:
        return redis.from_url(url, decode_responses=True, socket_timeout=5, socket_connect_timeout=5)
    except Exception as e:
        print(f"❌ Ошибка подключения к Redis {url}: {e}")
        return None


def check_stream(r: redis.Redis, stream_name: str, count: int = 5) -> list[dict]:
    """Проверяет наличие записей в stream."""
    try:
        entries = r.xrevrange(stream_name, max="+", min="-", count=count)
        return entries
    except redis.exceptions.ResponseError as e:
        if "no such key" in str(e).lower():
            return []
        raise
    except Exception as e:
        print(f"⚠️ Ошибка чтения stream {stream_name}: {e}")
        return []


def check_key(r: redis.Redis, key: str) -> str | None:
    """Проверяет значение ключа."""
    try:
        return r.get(key)
    except Exception as e:
        print(f"⚠️ Ошибка чтения ключа {key}: {e}")
        return None


def check_set(r: redis.Redis, key: str) -> list[str]:
    """Проверяет элементы set."""
    try:
        return list(r.smembers(key))
    except Exception as e:
        print(f"⚠️ Ошибка чтения set {key}: {e}")
        return []


def format_timestamp(ts_ms: int | None) -> str:
    """Форматирует timestamp."""
    if not ts_ms:
        return "N/A"
    try:
        dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts_ms)


def main():
    # Читаем конфигурацию из env
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    redis_ticks_url = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")
    notify_stream = os.getenv("CRYPTO_NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
    notify_redis_url = os.getenv("CRYPTO_NOTIFY_REDIS_URL", redis_url)

    print("=" * 80)
    print("🔍 ДИАГНОСТИКА CRYPTO ORDERFLOW SIGNALS")
    print("=" * 80)
    print(f"📅 Время: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()

    # Подключаемся к Redis
    print("🔌 Подключение к Redis...")
    r_main = get_redis_client(redis_url)
    r_ticks = get_redis_client(redis_ticks_url)
    r_notify = get_redis_client(notify_redis_url) if notify_redis_url != redis_url else r_main

    if not r_main or not r_ticks:
        print("❌ Не удалось подключиться к Redis")
        return 1

    # Проверяем символы
    print("\n" + "=" * 80)
    print("1️⃣ ПРОВЕРКА СИМВОЛОВ")
    print("=" * 80)
    symbols_set = check_set(r_main, "crypto:symbols")
    default_symbols = ["BTCUSDT", "ETHUSDT"]
    all_symbols = set(symbols_set + default_symbols)

    print(f"📊 Символы в crypto:symbols: {symbols_set if symbols_set else '(пусто)'}")
    print(f"📊 Всего символов для проверки: {len(all_symbols)}")
    for sym in sorted(all_symbols):
        print(f"   - {sym}")

    # Проверяем тики для каждого символа
    print("\n" + "=" * 80)
    print("2️⃣ ПРОВЕРКА ТИКОВ")
    print("=" * 80)
    ticks_found = False
    for symbol in sorted(all_symbols):
        stream_name = f"stream:tick_{symbol}"
        entries = check_stream(r_ticks, stream_name, count=3)
        if entries:
            ticks_found = True
            latest = entries[0]
            msg_id, fields = latest
            ts = fields.get("ts") or fields.get("event_time") or fields.get("written_at")
            print(f"✅ {symbol}: найдено записей в {stream_name}")
            print(f"   Последняя: msg_id={msg_id}, ts={format_timestamp(ts)}")
            try:
                stream_info = r_ticks.xinfo_stream(stream_name)
                print(f"   Всего в stream: {stream_info.get('length', 0)} записей")
            except Exception:
                pass
        else:
            print(f"❌ {symbol}: НЕТ записей в {stream_name}")

    if not ticks_found:
        print("\n⚠️ ВНИМАНИЕ: Тики не найдены! Проверьте, что тики публикуются в Redis.")

    # Проверяем сырые сигналы
    print("\n" + "=" * 80)
    print("3️⃣ ПРОВЕРКА СЫРЫХ СИГНАЛОВ (signals:crypto:raw)")
    print("=" * 80)
    raw_stream = os.getenv("CRYPTO_RAW_STREAM", RS.CRYPTO_RAW)
    raw_entries = check_stream(r_main, raw_stream, count=5)

    if raw_entries:
        print(f"✅ Найдено {len(raw_entries)} последних сигналов в {raw_stream}")
        for i, (msg_id, fields) in enumerate(raw_entries[:3], 1):
            payload_str = fields.get("payload", "{}")
            try:
                payload = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
                symbol = payload.get("symbol", "N/A")
                direction = payload.get("direction", "N/A")
                confidence = payload.get("confidence", 0.0)
                ts = payload.get("generated_at") or payload.get("tick_ts")
                print(f"   {i}. {symbol} {direction} | conf={confidence:.2%} | {format_timestamp(ts)}")
            except Exception as e:
                print(f"   {i}. msg_id={msg_id} (ошибка парсинга: {e})")
    else:
        print(f"❌ НЕТ сигналов в {raw_stream}")
        print("   ⚠️ Сигналы не генерируются или не публикуются")

    # Проверяем структурированные сигналы
    print("\n" + "=" * 80)
    print("4️⃣ ПРОВЕРКА СТРУКТУРИРОВАННЫХ СИГНАЛОВ (signals:cryptoorderflow:{symbol})")
    print("=" * 80)
    signal_template = os.getenv("CRYPTO_ORDERFLOW_SIGNAL_STREAM", "signals:cryptoorderflow:{symbol}")

    for symbol in sorted(all_symbols):
        stream_name = signal_template.format(symbol=symbol)
        entries = check_stream(r_main, stream_name, count=3)
        if entries:
            print(f"✅ {symbol}: найдено записей в {stream_name}")
            for i, (msg_id, fields) in enumerate(entries[:2], 1):
                data_str = fields.get("data", "{}")
                try:
                    data = json.loads(data_str) if isinstance(data_str, str) else data_str
                    direction = data.get("side", "N/A")
                    confidence = data.get("confidence", 0.0)
                    ts = data.get("ts")
                    print(f"   {i}. {direction} | conf={confidence:.2%} | {format_timestamp(ts)}")
                except Exception:
                    print(f"   {i}. msg_id={msg_id}")
        else:
            print(f"❌ {symbol}: НЕТ записей в {stream_name}")

    # Проверяем Telegram stream
    print("\n" + "=" * 80)
    print("5️⃣ ПРОВЕРКА TELEGRAM STREAM")
    print("=" * 80)
    print(f"Stream: {notify_stream}")
    print(f"Redis: {notify_redis_url}")

    telegram_entries = check_stream(r_notify, notify_stream, count=5)
    if telegram_entries:
        print(f"✅ Найдено {len(telegram_entries)} последних сообщений в {notify_stream}")
        for i, (msg_id, fields) in enumerate(telegram_entries[:3], 1):
            symbol = fields.get("symbol", "N/A")
            direction = fields.get("direction", "N/A")
            timestamp = fields.get("timestamp", "N/A")
            print(f"   {i}. {symbol} {direction} | ts={timestamp}")
    else:
        print(f"❌ НЕТ сообщений в {notify_stream}")
        print("   ⚠️ Сообщения не публикуются или бот читает другой stream")

    # Проверяем счетчик гейтинга
    print("\n" + "=" * 80)
    print("6️⃣ ПРОВЕРКА ГЕЙТИНГА TELEGRAM")
    print("=" * 80)
    counter_key = os.getenv("NOTIFY_SIGNAL_COUNTER_KEY", RS.NOTIFY_SIGNAL_COUNTER)
    counter_value = check_key(r_notify, counter_key)
    every_n = int(os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", "1"))

    print(f"Счетчик: {counter_key} = {counter_value if counter_value else '(не установлен)'}")
    print(f"Every N: {every_n}")

    if counter_value:
        counter_int = int(counter_value)
        if every_n > 1:
            remainder = counter_int % every_n
            next_trigger = every_n - remainder
            print(f"Текущее значение: {counter_int}")
            print(f"Остаток от деления на {every_n}: {remainder}")
            print(f"До следующей отправки: {next_trigger} сигналов")
            if remainder != 0:
                print(f"⚠️ Следующий сигнал будет пропущен (остаток {remainder} != 0)")
        else:
            print("✅ Гейтинг отключен (every_n=1), все сигналы отправляются")
    else:
        print("⚠️ Счетчик не установлен (первый сигнал еще не был обработан)")

    # Проверяем конфигурацию
    print("\n" + "=" * 80)
    print("7️⃣ ПРОВЕРКА КОНФИГУРАЦИИ")
    print("=" * 80)
    min_conf = os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70")
    delta_z_threshold = os.getenv("DELTA_Z_THRESHOLD", "3.0")
    print(f"Минимальная confidence: {min_conf}%")
    print(f"Delta Z threshold: {delta_z_threshold}")
    print(f"Telegram every_n: {every_n}")

    # Итоговая сводка
    print("\n" + "=" * 80)
    print("📊 ИТОГОВАЯ СВОДКА")
    print("=" * 80)

    issues = []
    if not ticks_found:
        issues.append("❌ Тики не найдены в streams")
    if not raw_entries:
        issues.append("❌ Сырые сигналы не генерируются")
    if not telegram_entries:
        issues.append("❌ Сообщения не публикуются в Telegram")

    if issues:
        print("⚠️ Обнаружены проблемы:")
        for issue in issues:
            print(f"   {issue}")
    else:
        print("✅ Все проверки пройдены успешно!")

    print("\n" + "=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())

