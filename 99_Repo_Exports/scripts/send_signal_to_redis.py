#!/usr/bin/env python3
"""
Отправка SUI сигнала в Redis и бот.

Usage:
    TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=yyy python scripts/send_signal_to_redis.py

Or with .env:
    set -a && source .env && set +a
    python scripts/send_signal_to_redis.py
"""
import os
import sys
import json
import requests
import redis
from datetime import datetime
import pytz

# ── Configuration ─────────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")   # never hardcode
CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")      # never hardcode


def _get_redis() -> redis.Redis:
    """Lazy Redis client — created only when needed."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)


def get_ny_time_millis() -> int:
    """Current timestamp in milliseconds (New York timezone)."""
    ny_tz = pytz.timezone("America/New_York")
    return int(datetime.now(ny_tz).timestamp() * 1000)


def send_to_telegram_bot(message: str) -> bool:
    """Send message to Telegram bot. Returns True on success."""
    if not BOT_TOKEN or not CHAT_ID:
        print("⚠️  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set — skipping Telegram send")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(
            url,
            data={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if response.status_code == 200:
            print("✅ Сообщение отправлено в бот")
            return True
        print(f"❌ Ошибка отправки в бот: {response.status_code}")
        return False
    except requests.RequestException as e:
        print(f"❌ Ошибка отправки в бот: {e}")
        return False


def send_signal_to_redis() -> bool:
    """Parse test signal and publish to Redis streams."""

    # ── Test signal ───────────────────────────────────────────────────────────
    signal_text = """#SUI/USDT | SHORT ⬇️
[Фьючерсы 19x плечо]

⚪️Точка входа: 3.53$
⚪️Тип ордера: Лимитный ордер
⚪️Отгрызаем профит на: 3.34$
⚪️Стоп: 3.73$

Потенциальная прибыль когда догрызем последний тейк будет = +70% ✅"""  # noqa: RUF001

    # ── Parse signal ──────────────────────────────────────────────────────────
    # telegram-worker/app is a separate standalone app — sys.path needed here only
    telegram_worker_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "telegram-worker", "app",
    )
    if telegram_worker_path not in sys.path:
        sys.path.insert(0, telegram_worker_path)

    from parse_utils import parse_signal  # noqa: E402, PLC0415

    parsed_signal = parse_signal(signal_text)

    print("🔍 Парсинг сигнала:")
    print("-" * 30)
    for key, value in parsed_signal.items():
        if key != "raw_text":
            print(f"{key}: {value}")

    timestamp = get_ny_time_millis()
    r = _get_redis()

    # ── Connectivity check ────────────────────────────────────────────────────
    try:
        r.ping()
    except redis.ConnectionError as exc:
        print(f"❌ Redis недоступен ({REDIS_HOST}:{REDIS_PORT}): {exc}")
        return False

    try:
        # 1. Raw stream
        raw_id = r.xadd(
            "signal:telegram:raw",
            {"message": signal_text, "timestamp": timestamp,
             "source": "manual_test", "channel": "test_channel"},
        )
        print(f"✅ signal:telegram:raw → {raw_id}")

        # 2. Parsed stream
        parsed_id = r.xadd(
            "signal:telegram:parsed",
            {"signal_data": json.dumps(parsed_signal), "timestamp": timestamp,
             "source": "manual_test", "channel": "test_channel"},
        )
        print(f"✅ signal:telegram:parsed → {parsed_id}")

        # 3. Notify stream
        notify_message = (
            f"🚨 <b>НОВЫЙ СИГНАЛ</b> 🚨\n\n"
            f"📊 <b>Символ:</b> {parsed_signal.get('symbol', 'N/A')}\n"
            f"📈 <b>Направление:</b> {parsed_signal.get('direction', 'N/A')}\n"
            f"💰 <b>Вход:</b> {parsed_signal.get('entry', 'N/A')}\n"
            f"🛑 <b>Стоп:</b> {parsed_signal.get('stop', 'N/A')}\n"
            f"🎯 <b>Цель:</b> {', '.join(map(str, parsed_signal.get('tp', [])))}\n"
            f"⚡ <b>Плечо:</b> {parsed_signal.get('leverage', 'N/A')}x\n"
            f"📋 <b>Тип ордера:</b> {parsed_signal.get('orderType', 'N/A')}\n"
            f"📊 <b>Прибыль:</b> {parsed_signal.get('profitPct', 'N/A')}%\n"
            f"🎯 <b>Уверенность:</b> {parsed_signal.get('confidence', 0)}%\n\n"
            f"⏰ <b>Время:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        notify_id = r.xadd(
            "notify:telegram",
            {"message": notify_message, "timestamp": timestamp,
             "signal_data": json.dumps(parsed_signal), "source": "manual_test"},
        )
        print(f"✅ notify:telegram → {notify_id}")

        send_to_telegram_bot(notify_message)
        return True

    except redis.RedisError as e:
        print(f"❌ Ошибка Redis: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Отправка SUI сигнала в Redis и бот")
    print("=" * 50)

    if not BOT_TOKEN:
        print("⚠️  TELEGRAM_BOT_TOKEN не задан. Telegram-отправка будет пропущена.")
        print("   Запуск: TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=yyy python scripts/send_signal_to_redis.py\n")

    success = send_signal_to_redis()
    print("\n✅ Сигнал успешно отправлен!" if success else "\n❌ Ошибка отправки сигнала!")
