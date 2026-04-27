#!/usr/bin/env python3
"""
Тестовый скрипт для проверки системы отчетов.

Проверяет:
1. Подключение к Redis
2. Наличие статистики
3. Формирование отчетов
4. Отправку в notify:telegram stream
5. Чтение из stream bot-nest/notify-worker
"""

import os
import time
import redis

# Добавляем python-worker в путь

from services.reporting_service import ReportingService

# ANSI colors
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
CYAN = '\033[96m'
RESET = '\033[0m'

def log(message, color=RESET):
    """Логирование с цветом"""
    print(f"{color}{message}{RESET}")

def check_redis():
    """Проверка подключения к Redis"""
    log("\n1️⃣ Проверка подключения к Redis...", BLUE)

    try:
        redis_url = os.getenv("REDIS_URL", "redis://scanner-redis-worker-1:6379/0")
        r = redis.from_url(redis_url, decode_responses=True)
        r.ping()
        log(f"   ✅ Redis подключен: {redis_url}", GREEN)
        return r
    except Exception as e:
        log(f"   ❌ Ошибка подключения к Redis: {e}", RED)
        return None

def check_statistics(redis_client):
    """Проверка наличия статистики"""
    log("\n2️⃣ Проверка статистики...", BLUE)

    try:
        # Получаем все ключи статистики
        stats_keys = redis_client.keys("stats:*")

        if not stats_keys:
            log("   ⚠️  Нет статистики в Redis", YELLOW)
            log("   Запустите сделки или дождитесь закрытия позиций", YELLOW)
            return False

        log(f"   ✅ Найдено {len(stats_keys)} ключей статистики", GREEN)

        # Показываем первые 5 ключей
        for key in stats_keys[:5]:
            stats = redis_client.hgetall(key)
            total_trades = stats.get("total_trades", "0")
            winrate = stats.get("winrate", "0")
            log(f"      • {key}: {total_trades} сделок, WR {winrate}%", CYAN)

        return True

    except Exception as e:
        log(f"   ❌ Ошибка проверки статистики: {e}", RED)
        return False

def test_reporting_service(redis_client):
    """Тест ReportingService"""
    log("\n3️⃣ Тест ReportingService...", BLUE)

    try:
        reporting = ReportingService()
        log("   ✅ ReportingService инициализирован", GREEN)

        # Получаем сводку
        log("   📊 Получение сводки...", CYAN)
        summary = reporting.get_performance_summary()

        if summary and summary.get("total_trades", 0) > 0:
            log(f"      Всего сделок: {summary['total_trades']}", CYAN)
            log(f"      WinRate: {summary['winrate']:.1f}%", CYAN)
            log(f"      P/L: {summary['total_pnl']:+.2f}", CYAN)
        else:
            log("      ⚠️  Нет сделок для отчета", YELLOW)

        # Получаем разбивку по источникам
        log("   📡 Получение разбивки по источникам...", CYAN)
        sources = reporting.get_sources_summary()

        if sources:
            for source, stats in sources.items():
                log(f"      • {source}: {stats['total_trades']} сделок", CYAN)
        else:
            log("      ⚠️  Нет данных по источникам", YELLOW)

        return reporting

    except Exception as e:
        log(f"   ❌ Ошибка ReportingService: {e}", RED)
        import traceback
        traceback.print_exc()
        return None

def test_send_report(reporting, redis_client):
    """Тест отправки отчета"""
    log("\n4️⃣ Тест отправки отчета...", BLUE)

    try:
        # Запоминаем текущую длину stream
        notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
        initial_len = redis_client.xlen(notify_stream)
        log(f"   📊 Длина {notify_stream} ДО отправки: {initial_len}", CYAN)

        # Отправляем тестовый отчет
        test_message = (
            "🧪 <b>Тестовый отчет</b>\n\n"
            "Это тестовое сообщение от системы отчетов.\n"
            f"Время: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}"
        )

        success = reporting.send_telegram_message(test_message)

        if success:
            log("   ✅ Отчет отправлен в stream", GREEN)

            # Проверяем что длина увеличилась
            time.sleep(0.5)
            new_len = redis_client.xlen(notify_stream)
            log(f"   📊 Длина {notify_stream} ПОСЛЕ отправки: {new_len}", CYAN)

            if new_len > initial_len:
                log(f"   ✅ Сообщение добавлено в stream (+{new_len - initial_len})", GREEN)
            else:
                log("   ⚠️  Длина stream не изменилась", YELLOW)

            # Читаем последнее сообщение
            messages = redis_client.xrevrange(notify_stream, count=1)
            if messages:
                msg_id, fields = messages[0]
                log(f"   📨 Последнее сообщение ID: {msg_id}", CYAN)
                log(f"      Type: {fields.get('type', 'N/A')}", CYAN)
                log(f"      Source: {fields.get('source', 'N/A')}", CYAN)
                text_preview = fields.get('text', '')[:60]
                log(f"      Text: {text_preview}...", CYAN)
        else:
            log("   ❌ Ошибка отправки отчета", RED)

        return success

    except Exception as e:
        log(f"   ❌ Ошибка теста отправки: {e}", RED)
        import traceback
        traceback.print_exc()
        return False

def check_consumers(redis_client):
    """Проверка consumer groups"""
    log("\n5️⃣ Проверка consumer groups...", BLUE)

    try:
        notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")

        # Получаем информацию о группах
        groups = redis_client.xinfo_groups(notify_stream)

        if groups:
            log(f"   ✅ Найдено {len(groups)} consumer groups:", GREEN)
            for group in groups:
                name = group.get('name', 'unknown')
                consumers = group.get('consumers', 0)
                pending = group.get('pending', 0)
                log(f"      • {name}: {consumers} consumers, {pending} pending", CYAN)
        else:
            log("   ⚠️  Нет consumer groups для notify:telegram", YELLOW)
            log("   Проверьте что bot-nest или notify-worker запущен", YELLOW)

    except Exception as e:
        log(f"   ⚠️  Не удалось получить информацию о группах: {e}", YELLOW)

def main():
    """Главная функция"""
    log("╔════════════════════════════════════════════════════════════════╗", CYAN)
    log("║            🧪 ТЕСТ СИСТЕМЫ ОТЧЕТОВ                             ║", CYAN)
    log("╚════════════════════════════════════════════════════════════════╝", CYAN)

    # 1. Проверка Redis
    redis_client = check_redis()
    if not redis_client:
        log("\n❌ Не удалось подключиться к Redis. Тест прерван.", RED)
        return

    # 2. Проверка статистики
    has_stats = check_statistics(redis_client)

    # 3. Тест ReportingService
    reporting = test_reporting_service(redis_client)
    if not reporting:
        log("\n❌ ReportingService не работает. Тест прерван.", RED)
        return

    # 4. Тест отправки
    send_success = test_send_report(reporting, redis_client)

    # 5. Проверка consumers
    check_consumers(redis_client)

    # Итоговый результат
    log("\n" + "="*60, CYAN)
    log("ИТОГОВЫЙ РЕЗУЛЬТАТ", CYAN)
    log("="*60, CYAN)

    if send_success:
        log("\n✅ Система отчетов работает!", GREEN)
        log("\n📱 Проверьте Telegram - должно прийти тестовое сообщение", CYAN)
        log("\n💡 Для автоматических отчетов запустите:", YELLOW)
        log("   docker-compose up -d periodic-reporter", YELLOW)
    else:
        log("\n❌ Система отчетов не работает полностью", RED)
        log("\nПроверьте:", YELLOW)
        log("  1. Redis подключение", YELLOW)
        log("  2. ReportingService инициализация", YELLOW)
        log("  3. Логи: docker-compose logs periodic-reporter", YELLOW)

    if not has_stats:
        log("\n⚠️  Нет статистики для отчетов", YELLOW)
        log("Дождитесь закрытия нескольких сделок", YELLOW)

    log("")

if __name__ == "__main__":
    main()

