#!/usr/bin/env python3
"""
Проверка цепочки генерации сигналов в реальном времени.
"""

import json
import os
import sys
import time
from core.redis_keys import RedisStreams as RS

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import redis
except ImportError:
    print("❌ redis-py не установлен")
    sys.exit(1)

def check_chain():
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    redis_ticks_url = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")

    r_main = redis.from_url(redis_url, decode_responses=True)
    r_ticks = redis.from_url(redis_ticks_url, decode_responses=True)

    print("=" * 80)
    print("🔍 ПРОВЕРКА ЦЕПОЧКИ В РЕАЛЬНОМ ВРЕМЕНИ")
    print("=" * 80)
    print("Нажмите Ctrl+C для остановки\n")

    last_tick_id = {}
    last_signal_id = {}

    try:
        while True:
            for symbol in ["BTCUSDT", "ETHUSDT"]:
                # 1. Проверка тиков
                stream = f"stream:tick_{symbol}"
                entries = r_ticks.xrevrange(stream, max="+", min="-", count=1)
                if entries:
                    msg_id, fields = entries[0]
                    if msg_id != last_tick_id.get(symbol):
                        last_tick_id[symbol] = msg_id
                        ts = fields.get("ts") or fields.get("event_time", "N/A")
                        print(f"✅ {symbol}: Новый тик {msg_id} (ts={ts})")

                # 2. Проверка consumer group
                try:
                    groups = r_ticks.xinfo_groups(stream)
                    if groups:
                        for group_info in groups:
                            group_name = group_info.get("name", "N/A")
                            pending = group_info.get("pending", 0)
                            consumers = group_info.get("consumers", 0)
                            if pending > 0 or consumers > 0:
                                print(f"   📊 Consumer group: {group_name}, pending={pending}, consumers={consumers}")
                    else:
                        print(f"⚠️ {symbol}: Нет consumer groups для {stream}")
                except Exception as e:
                    if "no such key" not in str(e).lower():
                        print(f"⚠️ {symbol}: Ошибка проверки groups: {e}")

                # 3. Проверка сигналов
                raw_stream = RS.CRYPTO_RAW
                raw_entries = r_main.xrevrange(raw_stream, max="+", min="-", count=1)
                if raw_entries:
                    msg_id, fields = raw_entries[0]
                    if msg_id != last_signal_id.get("raw"):
                        last_signal_id["raw"] = msg_id
                        payload_str = fields.get("payload", "{}")
                        try:
                            payload = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
                            if payload.get("symbol") == symbol:
                                signal_ts = payload.get("generated_at") or payload.get("tick_ts")
                                direction = payload.get("direction", "N/A")
                                confidence = payload.get("confidence", 0.0)
                                delta_z = payload.get("delta_z", 0.0)
                                print(f"🚀 {symbol}: Новый сигнал! {direction} | conf={confidence:.2%} | z={delta_z:.2f} | ts={signal_ts}")
                        except Exception:
                            pass

                # 4. Проверка Telegram
                telegram_entries = r_main.xrevrange(RS.NOTIFY_TELEGRAM, max="+", min="-", count=1)
                if telegram_entries:
                    msg_id, fields = telegram_entries[0]
                    if msg_id != last_signal_id.get("telegram"):
                        text = fields.get("text", "")
                        if symbol in text and ("LONG" in text or "SHORT" in text):
                            last_signal_id["telegram"] = msg_id
                            print(f"📱 {symbol}: Сообщение в Telegram! {text[:80]}...")

            time.sleep(5)

    except KeyboardInterrupt:
        print("\n\n✅ Проверка остановлена")

if __name__ == "__main__":
    check_chain()

