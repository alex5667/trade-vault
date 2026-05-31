#!/usr/bin/env python3
"""
Диагностический скрипт для проверки отправки отчетов в Telegram.

Проверяет:
1. Наличие сообщений с type="report" в notify:telegram stream
2. Статус notify-worker контейнера
3. Настройки Telegram (BOT_TOKEN, CHAT_ID)
4. Последние сообщения в stream
"""

import os
import sys
import redis
from datetime import datetime

# Цвета для вывода
RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
CYAN = '\033[96m'
RESET = '\033[0m'

def log(msg: str, color: str = RESET):
    print(f"{color}{msg}{RESET}")

def check_redis_connection():
    """Проверка подключения к Redis"""
    log("\n" + "="*80, BLUE)
    log("1. ПРОВЕРКА ПОДКЛЮЧЕНИЯ К REDIS", BLUE)
    log("="*80, BLUE)

    redis_url = os.getenv("REDIS_URL", "redis://scanner-redis:6379/0")
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        r.ping()
        log(f"✅ Подключение к Redis успешно: {redis_url}", GREEN)
        return r
    except Exception as e:
        log(f"❌ Ошибка подключения к Redis: {e}", RED)
        return None

def check_notify_stream(r: redis.Redis):
    """Проверка notify:telegram stream"""
    log("\n" + "="*80, BLUE)
    log("2. ПРОВЕРКА notify:telegram STREAM", BLUE)
    log("="*80, BLUE)

    stream_name = "notify:telegram"

    try:
        # Проверяем существование stream
        info = r.xinfo_stream(stream_name)
        length = info.get("length", 0)
        log(f"✅ Stream существует: {stream_name}", GREEN)
        log(f"   Длина stream: {length} сообщений", CYAN)

        # Проверяем consumer groups
        try:
            groups = r.xinfo_groups(stream_name)
            log(f"   Consumer groups: {len(groups)}", CYAN)
            for group in groups:
                log(f"     - {group['name']}: {group['consumers']} consumers, pending: {group['pending']}", CYAN)
        except Exception as e:
            log(f"   ⚠️ Ошибка получения групп: {e}", YELLOW)

        # Читаем последние 10 сообщений
        log("\n   Последние 10 сообщений:", CYAN)
        messages = r.xrevrange(stream_name, count=10)

        if not messages:
            log("   ⚠️ Stream пуст - нет сообщений", YELLOW)
            return

        report_count = 0
        signal_count = 0

        for msg_id, fields in messages:
            msg_type = fields.get("type", "unknown")
            source = fields.get("source", "unknown")

            if msg_type == "report":
                report_count += 1
                text_preview = fields.get("text", "")[:100]
                log(f"\n   📊 ОТЧЕТ #{report_count}:", GREEN)
                log(f"      ID: {msg_id}", CYAN)
                log(f"      Source: {source}", CYAN)
                log(f"      Text preview: {text_preview}...", CYAN)
                log(f"      Timestamp: {fields.get('timestamp', 'N/A')}", CYAN)
            else:
                signal_count += 1
                if signal_count <= 3:  # Показываем только первые 3 сигнала
                    log("\n   📨 Сигнал:", YELLOW)
                    log(f"      ID: {msg_id}", CYAN)
                    log(f"      Type: {msg_type}", CYAN)
                    log(f"      Source: {source}", CYAN)

        log("\n   📊 Статистика последних 10 сообщений:", BLUE)
        log(f"      Отчетов (type=report): {report_count}", GREEN if report_count > 0 else YELLOW)
        log(f"      Сигналов: {signal_count}", CYAN)

        if report_count == 0:
            log("\n   ⚠️ ВНИМАНИЕ: Нет сообщений с type='report' в stream!", RED)
            log("   Это означает, что ReportingService не публикует отчеты или они не дошли до stream", YELLOW)

    except redis.exceptions.ResponseError as e:
        if "no such key" in str(e).lower():
            log(f"❌ Stream {stream_name} не существует!", RED)
            log("   Создайте stream, отправив хотя бы одно сообщение", YELLOW)
        else:
            log(f"❌ Ошибка проверки stream: {e}", RED)
    except Exception as e:
        log(f"❌ Ошибка: {e}", RED)

def check_telegram_config():
    """Проверка конфигурации Telegram"""
    log("\n" + "="*80, BLUE)
    log("3. ПРОВЕРКА КОНФИГУРАЦИИ TELEGRAM", BLUE)
    log("="*80, BLUE)

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    if bot_token:
        log(f"✅ TELEGRAM_BOT_TOKEN найден: {bot_token[:20]}...", GREEN)
    else:
        log("❌ TELEGRAM_BOT_TOKEN не найден!", RED)
        log("   Установите переменную окружения TELEGRAM_BOT_TOKEN", YELLOW)

    if chat_id:
        log(f"✅ TELEGRAM_CHAT_ID найден: {chat_id}", GREEN)
    else:
        log("❌ TELEGRAM_CHAT_ID не найден!", RED)
        log("   Установите переменную окружения TELEGRAM_CHAT_ID", YELLOW)

    if not bot_token or not chat_id:
        log("\n   ⚠️ БЕЗ ТОКЕНА И CHAT_ID notify-worker НЕ СМОЖЕТ ОТПРАВЛЯТЬ СООБЩЕНИЯ!", RED)

def check_notify_worker_status():
    """Проверка статуса notify-worker контейнера"""
    log("\n" + "="*80, BLUE)
    log("4. ПРОВЕРКА СТАТУСА notify-worker", BLUE)
    log("="*80, BLUE)

    import subprocess

    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=scanner-notify-worker", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode == 0 and result.stdout.strip():
            log("✅ notify-worker контейнер запущен:", GREEN)
            log(f"   {result.stdout.strip()}", CYAN)
        else:
            log("❌ notify-worker контейнер НЕ запущен!", RED)
            log("   Запустите: docker-compose up -d notify-worker", YELLOW)

    except FileNotFoundError:
        log("⚠️ Docker не найден - пропускаем проверку контейнера", YELLOW)
    except Exception as e:
        log(f"⚠️ Ошибка проверки контейнера: {e}", YELLOW)

def check_pending_messages(r: redis.Redis):
    """Проверка pending сообщений в consumer groups"""
    log("\n" + "="*80, BLUE)
    log("5. ПРОВЕРКА PENDING СООБЩЕНИЙ", BLUE)
    log("="*80, BLUE)

    stream_name = "notify:telegram"
    os.getenv("NOTIFY_GROUP", "notify-group")

    try:
        groups = r.xinfo_groups(stream_name)

        for group in groups:
            group_name_check = group['name']
            pending = group['pending']

            if pending > 0:
                log(f"⚠️ Группа {group_name_check}: {pending} pending сообщений", YELLOW)

                # Показываем pending сообщения
                pending_msgs = r.xpending_range(
                    stream_name,
                    group_name_check,
                    min="-",
                    max="+",
                    count=10
                )

                if pending_msgs:
                    log(f"   Первые {len(pending_msgs)} pending сообщений:", CYAN)
                    for msg in pending_msgs[:5]:
                        msg_id = msg['message_id']
                        consumer = msg['consumer']
                        idle = msg['idle'] / 1000  # в секундах
                        log(f"      ID: {msg_id}, Consumer: {consumer}, Idle: {idle:.1f}s", CYAN)
            else:
                log(f"✅ Группа {group_name_check}: нет pending сообщений", GREEN)

    except Exception as e:
        log(f"⚠️ Ошибка проверки pending: {e}", YELLOW)

def test_report_publication(r: redis.Redis):
    """Тестовая публикация отчета"""
    log("\n" + "="*80, BLUE)
    log("6. ТЕСТОВАЯ ПУБЛИКАЦИЯ ОТЧЕТА", BLUE)
    log("="*80, BLUE)

    stream_name = "notify:telegram"

    test_message = {
        "type": "report",
        "text": f"🧪 <b>Тестовый отчет</b>\n\nЭто тестовое сообщение для проверки доставки отчетов.\n\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "source": "DiagnosticScript",
        "timestamp": str(int(datetime.now().timestamp() * 1000)),
        "severity": "info"
    }

    try:
        msg_id = r.xadd(stream_name, test_message, maxlen=2000)
        log(f"✅ Тестовый отчет опубликован в {stream_name}", GREEN)
        log(f"   Message ID: {msg_id}", CYAN)
        log("   Проверьте, придет ли сообщение в Telegram в течение 10 секунд", YELLOW)
        return True
    except Exception as e:
        log(f"❌ Ошибка публикации тестового отчета: {e}", RED)
        return False

def main():
    log("\n" + "="*80, CYAN)
    log("🔍 ДИАГНОСТИКА ОТПРАВКИ ОТЧЕТОВ В TELEGRAM", CYAN)
    log("="*80, CYAN)

    # Проверка Redis
    r = check_redis_connection()
    if not r:
        log("\n❌ Не удалось подключиться к Redis. Проверьте REDIS_URL", RED)
        sys.exit(1)

    # Проверка stream
    check_notify_stream(r)

    # Проверка конфигурации Telegram
    check_telegram_config()

    # Проверка статуса контейнера
    check_notify_worker_status()

    # Проверка pending сообщений
    check_pending_messages(r)

    # Тестовая публикация
    log("\n" + "="*80, BLUE)
    response = input("Отправить тестовый отчет? (y/n): ")
    if response.lower() == 'y':
        test_report_publication(r)

    # Рекомендации
    log("\n" + "="*80, CYAN)
    log("📋 РЕКОМЕНДАЦИИ", CYAN)
    log("="*80, CYAN)
    log("1. Убедитесь, что notify-worker запущен: docker-compose ps notify-worker", YELLOW)
    log("2. Проверьте логи notify-worker: docker logs scanner-notify-worker --tail 50", YELLOW)
    log("3. Проверьте, что TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID установлены в .env", YELLOW)
    log("4. Проверьте логи signal-performance-tracker для отчетов", YELLOW)
    log("5. Проверьте, что ReportingService публикует отчеты в notify:telegram", YELLOW)
    log("\n")

if __name__ == "__main__":
    main()













