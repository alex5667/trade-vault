#!/usr/bin/env python3
"""
Тестовый скрипт для отправки реального отчета в Telegram бот.
Проверяет всю цепочку: PeriodicReporter -> ReportingService -> notify:telegram -> notify_worker -> Telegram

Запуск из контейнера:
    docker exec -it scanner-python-worker python3 test_report_send.py
"""
import os
import sys
import time
from datetime import datetime

from services.periodic_reporter import PeriodicReporter
from services.reporting_service import ReportingService
from core.redis_client import get_redis
from common.log import setup_logger

logger = setup_logger("TestReportSend")

def main():
    """Основная функция тестовой отправки отчета."""
    logger.info("=" * 80)
    logger.info("🧪 ТЕСТОВАЯ ОТПРАВКА ОТЧЕТА В TELEGRAM")
    logger.info("=" * 80)
    
    # Инициализируем компоненты
    try:
        reporter = PeriodicReporter()
        redis_client = get_redis()
        
        # Проверяем подключение
        redis_client.ping()
        logger.info("✅ Инициализация компонентов успешна")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации: {e}", exc_info=True)
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
        from domain.normalizers import canon_source, canon_symbol
        seen_pairs = set()
        all_entries = redis_client.xrevrange("trades:closed", max="+", count=100)
        
        for _, fields in all_entries:
            if not fields:
                continue
            t = {str(k): str(v) for k, v in fields.items()}
            source = t.get("source", "").strip()
            symbol = t.get("symbol", "").strip()
            if source and symbol:
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
        pair_list = list(seen_pairs)
        test_source, test_symbol = pair_list[0]
        for s, sym in pair_list:
            if "XRPUSDT" in sym:
                test_source, test_symbol = s, sym
                break
        logger.info(f"\n🎯 Выбрана пара для теста: {test_source} / {test_symbol}")
    else:
        logger.warning("⚠️ Не найдено пар для тестирования, используем дефолтные значения")
        test_source = "CryptoOrderFlow"
        test_symbol = "XRPUSDT"
    
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
    logger.info(f"\n📤 Отправка отчета через ReportingService...")
    try:
        class MockReporter:
            def send_telegram_message(self, text, *args, **kwargs):
                logger.info(f"\n=== GENERATED REPORT BEGIN ===\n{text}\n=== GENERATED REPORT END ===\n")
                return True
        reporter.reporting = MockReporter()
        
        # Используем метод _send_report напрямую для теста
        reporter._send_report(test_source, test_symbol, metrics, window_seconds=3600)
        logger.info("✅ Отчет выведен на экран")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки отчета: {e}", exc_info=True)
        return 1
    
    # Проверяем, что сообщение появилось в stream
    logger.info(f"\n🔍 Проверка сообщения в notify:telegram stream...")
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
    logger.info(f"\n🔄 Дополнительная проверка: отправка напрямую через ReportingService...")
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
