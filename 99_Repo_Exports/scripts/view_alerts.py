#!/usr/bin/env python3
"""View recent alerts and channel stats from Redis."""

import json
import os
from datetime import datetime

import redis


def _get_redis_client() -> redis.Redis:
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(url, decode_responses=True)


def view_recent_alerts(limit: int = 10) -> None:
    """Print the most recent alerts from the telegram:alerts stream."""
    redis_client = _get_redis_client()
    try:
        alerts = redis_client.xrevrange("telegram:alerts", count=limit)

        if not alerts:
            print("📭 Нет алертов")
            return

        print(f"🚨 Последние {len(alerts)} алертов:")
        print("=" * 80)

        for alert_id, fields in alerts:  # noqa: B007
            alert_data = {}
            for i in range(0, len(fields), 2):
                key = fields[i]
                value = fields[i+1]
                alert_data[key] = value

            # Форматируем время
            timestamp = int(alert_data.get('timestamp', 0)) / 1000
            time_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

            print(f"🕐 {time_str}")
            print(f"📊 Тип: {alert_data.get('type', 'unknown')}")
            print(f"📺 Канал: {alert_data.get('channel') or alert_data.get('username') or alert_data.get('source') or 'Unknown Channel'}")
            print(f"💬 Сообщение: {alert_data.get('message', 'no message')}")

            if 'data' in alert_data:
                try:
                    data = json.loads(alert_data['data'])
                    if data:
                        print(f"📋 Данные: {data}")
                except (json.JSONDecodeError, ValueError):
                    pass

            print("-" * 80)

    except Exception as e:
        print(f"❌ Ошибка получения алертов: {e}")

def view_channel_stats() -> None:
    """Print per-channel message statistics."""
    redis_client = _get_redis_client()
    try:
        # Получаем все ключи каналов
        pattern = "telegram:channel:*:stats"
        keys = redis_client.keys(pattern)

        if not keys:
            print("📭 Нет статистики каналов")
            return

        print(f"📊 Статистика {len(keys)} каналов:")
        print("=" * 80)

        for key in keys:
            channel_name = key.replace("telegram:channel:", "").replace(":stats", "")
            stats = redis_client.hgetall(key)

            if stats:
                message_count = stats.get('message_count', 0)
                last_message = stats.get('last_message_time', 0)
                is_active = stats.get('is_active', 'True') == 'True'

                if last_message:
                    last_msg_time = datetime.fromtimestamp(float(last_message)).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    last_msg_time = "Никогда"

                status_emoji = "✅" if is_active else "❌"
                print(f"{status_emoji} {channel_name}")
                print(f"   📈 Сообщений: {message_count}")
                print(f"   🕐 Последнее: {last_msg_time}")
                print("-" * 40)

    except Exception as e:
        print(f"❌ Ошибка получения статистики: {e}")

if __name__ == "__main__":
    print("🔍 Просмотр алертов и статистики telegram-worker")
    print("=" * 80)

    print("\n🚨 АЛЕРТЫ:")
    view_recent_alerts(5)

    print("\n📊 СТАТИСТИКА КАНАЛОВ:")
    view_channel_stats()
