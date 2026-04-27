#!/usr/bin/env python3
"""
Отправка РЕАЛЬНОГО отчета из Signal Performance Tracker в Telegram
Запускается внутри Docker контейнера с доступом к redis-worker-1
"""

import os
import sys
import redis
import requests
from datetime import datetime

def send_report():
    """Формирование и отправка отчета"""
    print("📊 Сбор статистики из Redis...")

    # Подключение к Redis
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)

    # Получаем статистику
    overall = r.hgetall('stats:orderflow:XAUUSD:tick')

    # Получаем список всех источников динамически
    sources = r.smembers('stats:sources:orderflow:XAUUSD:tick')
    source_stats = {}

    for source in sources:
        stats_key = f'stats:orderflow:XAUUSD:tick:{source}'
        stats = r.hgetall(stats_key)
        if stats:
            source_stats[source] = stats

    # Формируем сообщение
    now = datetime.utcnow()
    message = "📊 *РЕАЛЬНЫЙ ОТЧЕТ ПО СИГНАЛАМ*\n\n"
    message += f"🕐 Время: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
    message += "📈 Символ: XAUUSD\n\n"

    message += f"{'━' * 42}\n"
    message += "*🎯 ОБЩАЯ СТАТИСТИКА*\n"
    message += f"{'━' * 42}\n"

    total = int(overall.get('total_trades', 0))
    wins = int(overall.get('wins', 0))
    losses = int(overall.get('losses', 0))
    winrate = float(overall.get('winrate', 0))
    total_pnl = float(overall.get('total_pnl', 0))

    message += f"📊 Всего сделок: *{total}*\n"
    message += f"✅ Прибыльных: {wins} ({winrate:.1f}%)\n"
    message += f"❌ Убыточных: {losses}\n"

    if total_pnl > 0:
        message += f"💰 P&L: *+${total_pnl:.2f}*\n"
    else:
        message += f"📉 P&L: *${total_pnl:.2f}*\n"

    # TP статистика
    tp1 = int(overall.get('tp1_hits', 0))
    tp2 = int(overall.get('tp2_hits', 0))
    tp3 = int(overall.get('tp3_hits', 0))

    message += "\n🎯 *TP Performance:*\n"
    if total > 0:
        message += f"  • TP1 hits: {tp1}/{total} ({tp1/total*100:.0f}%)\n"
        message += f"  • TP2 hits: {tp2}/{total} ({tp2/total*100:.0f}%)\n"
        message += f"  • TP3 hits: {tp3}/{total} ({tp3/total*100:.0f}%)\n"

    # Разбивка по источникам
    if source_stats:
        message += "\n🔧 *Разбивка по источникам:*\n"

        # Определяем эмодзи для источников
        def get_emoji(source_name):
            if 'aggregated' in source_name.lower() or 'hub' in source_name.lower():
                return "🎯"
            elif 'technical' in source_name.lower() or 'ta' in source_name.lower():
                return "📈"
            elif 'orderflow' in source_name.lower():
                return "📊"
            else:
                return "🔸"

        # Сортируем источники по прибыльности
        sorted_sources = sorted(
            source_stats.items(),
            key=lambda x: float(x[1].get('total_pnl', 0)),
            reverse=True
        )

        for source_name, stats in sorted_sources:
            src_total = int(stats.get('total_trades', 0))
            src_wr = float(stats.get('winrate', 0))
            src_pnl = float(stats.get('total_pnl', 0))

            emoji = get_emoji(source_name)

            message += f"  {emoji} *{source_name}*\n"
            message += f"     • Сделок: {src_total}, WR: {src_wr:.1f}%\n"
            if src_pnl > 0:
                message += f"     • P&L: +${src_pnl:.2f}\n"
            else:
                message += f"     • P&L: ${src_pnl:.2f}\n"

    message += f"\n{'━' * 38}\n"
    message += "_Отчет сформирован из реальных данных_\n"
    message += "_Signal Performance Tracker v2.0_"

    print("\n" + "="*60)
    print("ОТЧЕТ:")
    print("="*60)
    print(message.replace("*", "").replace("_", ""))
    print("="*60)

    # Отправка в Telegram
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        print("\n❌ TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не установлены!")
        return False

    print("\n📤 Отправка в Telegram...")
    print(f"   Bot Token: {bot_token[:25]}...")
    print(f"   Chat ID: {chat_id}")

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        response = requests.post(url, json=payload, timeout=10)

        if response.status_code == 200:
            print("\n✅ ОТЧЕТ УСПЕШНО ОТПРАВЛЕН В TELEGRAM!")
            return True
        else:
            print(f"\n❌ Ошибка Telegram API: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            return False

    except Exception as e:
        print(f"\n❌ Ошибка отправки: {e}")
        return False

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🚀 Signal Performance Tracker - REAL REPORT")
    print("="*70 + "\n")

    success = send_report()

    if success:
        print("\n✅ Готово!")
    else:
        print("\n❌ Ошибка отправки отчета")
        sys.exit(1)

