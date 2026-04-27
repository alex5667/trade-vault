#!/usr/bin/env python3
"""
Тестовый скрипт для отправки реального отчета в Telegram бот.
Проверяет всю цепочку: PeriodicReporter -> ReportingService -> notify:telegram -> notify_worker -> Telegram

Использование:
    # Из контейнера python-worker:
    docker exec -it scanner-python-worker-1 python3 /app/scripts/test_report_send.py

    # Или с переменной окружения:
    REDIS_URL=redis://localhost:6379/0 python3 scripts/test_report_send.py
"""
import os
import time
from datetime import datetime
from dotenv import load_dotenv

# Загружаем .env если есть
load_dotenv()

# Добавляем пути для импорта

from services.periodic_reporter import PeriodicReporter  # noqa: E402
from services.reporting_service import ReportingService  # noqa: E402
from core.redis_client import get_redis  # noqa: E402
from common.log import setup_logger  # noqa: E402

logger = setup_logger("TestReportSend")

def main():
    """Основная функция тестовой отправки отчета."""
    logger.info("=" * 80)
    logger.info("🧪 ТЕСТОВАЯ ОТПРАВКА ОТЧЕТА В TELEGRAM")
    logger.info("=" * 80)

    # Проверяем Redis URL
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    logger.info(f"🔗 Redis URL: {redis_url}")

    # Устанавливаем переменную окружения для PeriodicReporter
    if "REDIS_URL" not in os.environ:
        os.environ["REDIS_URL"] = redis_url

    # Инициализируем компоненты
    try:
        reporter = PeriodicReporter()
        redis_client = get_redis()

        # Проверяем подключение
        redis_client.ping()
        logger.info("✅ Инициализация компонентов успешна")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации: {e}", exc_info=True)
        logger.error("\n💡 Убедитесь, что:")
        logger.error("   1. Redis доступен по указанному адресу")
        logger.error("   2. Установлена переменная REDIS_URL или используется локальный Redis")
        logger.error("   3. Для Docker: используйте docker exec для запуска из контейнера")
        return 1

    # Проверяем наличие данных в Redis
    logger.info("\n📊 Проверка данных в Redis...")

    try:
        # Проверяем stream trades:closed
        recent_entries = redis_client.xrevrange("trades:closed", max="+", count=10)
        logger.info(f"   Найдено записей в trades:closed (последние 10): {len(recent_entries)}")

        if recent_entries:
            logger.info("   Пример записи:")
            _, sample_fields = recent_entries[0]
            sample_dict = {str(k): str(v) for k, v in (sample_fields or {}).items()}
            logger.info(f"   - source: {sample_dict.get('source', 'N/A')}")
            logger.info(f"   - symbol: {sample_dict.get('symbol', 'N/A')}")
            logger.info(f"   - pnl: {sample_dict.get('pnl', 'N/A')}")

        # Проверяем доступные пары
        strategies = redis_client.smembers("stats:strategies") or set()
        logger.info(f"\n   Найдено стратегий: {len(strategies)}")

        if strategies:
            logger.info(f"   Стратегии: {list(strategies)[:5]}")

        # Получаем все пары source/symbol из последних сделок
        seen_pairs = set()
        all_entries = redis_client.xrevrange("trades:closed", max="+", count=100)

        for _, fields in all_entries:
            if not fields:
                continue
            t = {str(k): str(v) for k, v in fields.items()}
            source = t.get("source", "").strip()
            symbol = t.get("symbol", "").strip()
            if source and symbol:
                from domain.normalizers import canon_source, canon_symbol
                pair = (canon_source(source), canon_symbol(symbol))
                seen_pairs.add(pair)

        logger.info(f"\n   Найдено уникальных пар source/symbol: {len(seen_pairs)}")
        if seen_pairs:
            logger.info(f"   Пары: {list(seen_pairs)[:5]}")

    except Exception as e:
        logger.error(f"❌ Ошибка проверки данных: {e}", exc_info=True)
        return 1

    # Выбираем пару для теста
    test_source = None
    test_symbol = None

    if seen_pairs:
        test_source, test_symbol = list(seen_pairs)[0]
        logger.info(f"\n🎯 Выбрана пара для теста: {test_source} / {test_symbol}")
    else:
        logger.warning("⚠️ Не найдено пар для тестирования, используем дефолтные значения")
        test_source = "OrderFlow"
        test_symbol = "XAUUSD"

    # Собираем метрики
    logger.info(f"\n📈 Сбор метрик для {test_source} / {test_symbol}...")
    try:
        metrics = reporter._gather_window_metrics_stream(test_source, test_symbol)
        total_trades = int(metrics.get("total_trades", 0))

        logger.info(f"   Всего сделок в окне: {total_trades}")
        logger.info(f"   Wins: {metrics.get('wins', 0)}")
        logger.info(f"   Losses: {metrics.get('losses', 0)}")
        logger.info(f"   Total PnL: {metrics.get('total_pnl', 0.0):.2f}")

        if total_trades == 0:
            logger.warning("⚠️ Нет сделок в окне для данной пары")
            logger.info("   Пытаемся найти другие пары...")

            # Пробуем другую пару
            if len(seen_pairs) > 1:
                test_source, test_symbol = list(seen_pairs)[1]
                logger.info(f"   Пробуем пару: {test_source} / {test_symbol}")
                metrics = reporter._gather_window_metrics_stream(test_source, test_symbol)
                total_trades = int(metrics.get("total_trades", 0))

    except Exception as e:
        logger.error(f"❌ Ошибка сбора метрик: {e}", exc_info=True)
        return 1

    # Отправляем отчет
    logger.info("\n📤 Отправка отчета через ReportingService...")
    try:
        # Используем метод _send_report напрямую для теста
        reporter._send_report(test_source, test_symbol, metrics)
        logger.info("✅ Отчет отправлен в Redis stream notify:telegram")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки отчета: {e}", exc_info=True)
        return 1

    # Проверяем, что сообщение появилось в stream
    logger.info("\n🔍 Проверка сообщения в notify:telegram stream...")
    try:
        time.sleep(1)  # Небольшая задержка

        notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
        recent_msgs = redis_client.xrevrange(notify_stream, max="+", count=1)

        if recent_msgs:
            msg_id, msg_fields = recent_msgs[0]
            msg_dict = {str(k): str(v) for k, v in (msg_fields or {}).items()}

            logger.info(f"   ✅ Сообщение найдено в stream: {msg_id}")
            logger.info(f"   Type: {msg_dict.get('type', 'N/A')}")
            logger.info(f"   Source: {msg_dict.get('source', 'N/A')}")
            logger.info(f"   Text length: {len(msg_dict.get('text', ''))} символов")
            logger.info(f"   Text preview: {msg_dict.get('text', '')[:200]}...")
        else:
            logger.warning("⚠️ Сообщение не найдено в stream")

    except Exception as e:
        logger.error(f"❌ Ошибка проверки stream: {e}", exc_info=True)

    # Дополнительная проверка: отправка напрямую через ReportingService
    logger.info("\n🔄 Дополнительная проверка: отправка напрямую через ReportingService...")
    try:
        reporting = ReportingService()

        test_message = f"""
📊 <b>ТЕСТОВЫЙ ОТЧЕТ</b>
🕐 {datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}
🪟 Окно: последние <b>60 мин</b>
========================================

<b>Тестовое сообщение</b>
Пара: {test_source} / {test_symbol}
Сделок: {total_trades}

<i>Это тестовое сообщение для проверки цепочки отправки отчетов.</i>
        """.strip()

        success = reporting.send_telegram_message(
            test_message,
            tags=["test", "report"],
            severity="info",
            dedup_key=f"test_report_{int(time.time())}"
        )

        if success:
            logger.info("✅ Тестовое сообщение отправлено в Redis stream")
        else:
            logger.error("❌ Не удалось отправить тестовое сообщение")

    except Exception as e:
        logger.error(f"❌ Ошибка прямой отправки: {e}", exc_info=True)

    logger.info("\n" + "=" * 80)
    logger.info("✅ ТЕСТ ЗАВЕРШЕН")
    logger.info("=" * 80)
    logger.info("\n💡 Проверьте:")
    logger.info("   1. Логи notify_worker для обработки сообщений из stream")
    logger.info("   2. Telegram бот - должно прийти сообщение")
    logger.info("   3. Redis stream notify:telegram - должны быть новые сообщения")

    return 0

if __name__ == "__main__":
    exit(main())
