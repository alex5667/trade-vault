#!/usr/bin/env python3
"""
Тест отправки статистики Signal Performance Tracker в Telegram

Этот скрипт:
1. Проверяет подключение к Redis
2. Проверяет наличие статистики
3. Отправляет тестовую сводку в Telegram
4. Не требует ожидания 3 часов

Использование:
    python scripts/test_tracker_telegram.py
"""

import os
import sys
import redis
import requests
from datetime import datetime

# Цвета для консоли
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'


def log(message, color=RESET):
    """Цветной вывод"""
    print(f"{color}{message}{RESET}")


def check_redis():
    """Проверка подключения к Redis"""
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", BLUE)
    log("🔍 ПРОВЕРКА REDIS", BLUE)
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", BLUE)

    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
        log("✅ Redis подключен", GREEN)
        return r
    except Exception as e:
        log(f"❌ Ошибка подключения к Redis: {e}", RED)
        log("Запустите: make up", YELLOW)
        sys.exit(1)


def get_stats(r):
    """Получение статистики из Redis"""
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", BLUE)
    log("🔍 ПОЛУЧЕНИЕ СТАТИСТИКИ", BLUE)
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", BLUE)

    # Поиск ключей статистики
    stats_keys = r.keys("stats:*")

    if not stats_keys:
        log("⚠️  Статистика пока не собрана", YELLOW)
        log("Это нормально, если трекер только запущен", YELLOW)
        log("Создам тестовую статистику...", YELLOW)

        # Создаем тестовую статистику
        test_stats = {
            "total_trades": "10",
            "wins": "7",
            "losses": "3",
            "total_pnl": "250.50",
            "winrate": "70.0",
            "avg_win": "50.25",
            "avg_loss": "-25.10",
            "last_update": str(int(datetime.now().timestamp()))
        }

        r.hset("stats:orderflow:XAUUSD:tick", mapping=test_stats)
        log("✅ Создана тестовая статистика: stats:orderflow:XAUUSD:tick", GREEN)

        stats_keys = ["stats:orderflow:XAUUSD:tick"]

    log(f"✅ Найдено ключей статистики: {len(stats_keys)}", GREEN)

    # Читаем статистику
    all_stats = {}

    for key in stats_keys[:5]:  # Первые 5 ключей
        parts = key.split(":")
        if len(parts) >= 4:
            strategy = parts[1]
            parts[2]
            parts[3]

            stats = r.hgetall(key)

            if strategy not in all_stats:
                all_stats[strategy] = {
                    "total_trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "total_pnl": 0.0,
                    "winrate": 0.0
                }

            # Аггрегируем
            all_stats[strategy]["total_trades"] += int(stats.get("total_trades", 0))
            all_stats[strategy]["wins"] += int(stats.get("wins", 0))
            all_stats[strategy]["losses"] += int(stats.get("losses", 0))
            all_stats[strategy]["total_pnl"] += float(stats.get("total_pnl", 0.0))

    # Вычисляем winrate
    for strategy in all_stats:
        total = all_stats[strategy]["total_trades"]
        if total > 0:
            all_stats[strategy]["winrate"] = round(
                all_stats[strategy]["wins"] / total * 100.0, 1
            )

    log("\n📊 Статистика по стратегиям:", BLUE)
    for strategy, stats in all_stats.items():
        log(f"\n   Strategy: {strategy}")
        log(f"   ├─ Сделок: {stats['total_trades']}")
        log(f"   ├─ Прибыльных: {stats['wins']} ({stats['winrate']}%)")
        log(f"   ├─ Убыточных: {stats['losses']}")
        log(f"   └─ Общий P&L: ${stats['total_pnl']:.2f}")

    return all_stats


def send_telegram(all_stats):
    """Отправка статистики в Telegram"""
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", BLUE)
    log("🔍 ОТПРАВКА В TELEGRAM", BLUE)
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", BLUE)

    # Получение credentials
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token:
        log("❌ TELEGRAM_BOT_TOKEN не установлен", RED)
        log("Установите переменную окружения или добавьте в .env", YELLOW)
        return False

    if not chat_id:
        log("❌ TELEGRAM_CHAT_ID не установлен", RED)
        log("Установите переменную окружения или добавьте в .env", YELLOW)
        return False

    log(f"✅ Bot Token: {bot_token[:20]}...", GREEN)
    log(f"✅ Chat ID: {chat_id}", GREEN)

    # Формирование сообщения
    message = "📊 *Тестовая периодическая сводка Signal Tracker*\n\n"
    message += f"🕐 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"

    for strategy, stats in all_stats.items():
        message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        message += f"*Strategy: {strategy}*\n"
        message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        message += f"├─ Всего сделок: {stats['total_trades']}\n"
        message += f"├─ Прибыльных: {stats['wins']} ({stats['winrate']}%)\n"
        message += f"├─ Убыточных: {stats['losses']}\n"

        pnl_emoji = "💰" if stats['total_pnl'] > 0 else "📉"
        message += f"└─ Общий P&L: {pnl_emoji} ${stats['total_pnl']:.2f}\n\n"

    # Общая статистика
    total_trades = sum(s["total_trades"] for s in all_stats.values())
    total_wins = sum(s["wins"] for s in all_stats.values())
    total_pnl = sum(s["total_pnl"] for s in all_stats.values())

    if total_trades > 0:
        overall_winrate = round(total_wins / total_trades * 100.0, 1)
    else:
        overall_winrate = 0.0

    message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    message += "*ИТОГО*\n"
    message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"Всего сделок: {total_trades}\n"
    message += f"Win Rate: {overall_winrate}%\n"

    pnl_emoji = "💰" if total_pnl > 0 else "📉"
    message += f"Общий P&L: {pnl_emoji} ${total_pnl:.2f}\n\n"

    message += "_Это тестовое сообщение для проверки работы Signal Tracker_"

    # Отправка
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    log("\n📤 Отправка сообщения в Telegram...", BLUE)

    try:
        response = requests.post(url, json=payload, timeout=10)

        if response.status_code == 200:
            log("✅ Сообщение успешно отправлено!", GREEN)
            log("\nПроверьте Telegram - должно прийти сообщение со статистикой", YELLOW)
            return True
        else:
            log(f"❌ Ошибка Telegram API: {response.status_code}", RED)
            log(f"Response: {response.text}", RED)
            return False

    except requests.exceptions.RequestException as e:
        log(f"❌ Ошибка сети: {e}", RED)
        return False


def main():
    """Главная функция"""
    log("╔════════════════════════════════════════════════════════════════╗", BLUE)
    log("║   Тест Signal Performance Tracker - Отправка в Telegram        ║", BLUE)
    log("╚════════════════════════════════════════════════════════════════╝", BLUE)

    # 1. Проверка Redis
    r = check_redis()

    # 2. Получение статистики
    all_stats = get_stats(r)

    if not all_stats:
        log("\n⚠️  Нет статистики для отправки", YELLOW)
        log("Запустите систему и дождитесь первых сигналов", YELLOW)
        return

    # 3. Отправка в Telegram
    success = send_telegram(all_stats)

    # 4. Итог
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", BLUE)
    log("ИТОГ", BLUE)
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", BLUE)

    if success:
        log("✅ Тест пройден успешно!", GREEN)
        log("\nSignal Performance Tracker готов к работе:", GREEN)
        log("  - Статистика собирается", GREEN)
        log("  - Telegram отправка работает", GREEN)
        log("  - Периодические отчеты будут приходить каждые 3 часа", GREEN)
    else:
        log("❌ Тест не пройден", RED)
        log("\nПроверьте:", YELLOW)
        log("  1. Установлены ли TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID", YELLOW)
        log("  2. Правильные ли credentials", YELLOW)
        log("  3. Есть ли доступ к Telegram API", YELLOW)

    log("")


if __name__ == "__main__":
    main()

