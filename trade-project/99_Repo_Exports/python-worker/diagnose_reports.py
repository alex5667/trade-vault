from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Диагностический скрипт для проверки, почему отчеты не приходят в бот.

Проверяет:
1. Запущен ли periodic-reporter сервис
2. Есть ли пары source/symbol для отчетов
3. Есть ли сделки в окне
4. Публикуются ли сообщения в notify:telegram stream
5. Работает ли notify_worker
"""

import os
import sys
import time
from datetime import datetime, timezone

# Добавляем путь к модулям
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.redis_client import get_redis
from common.log import setup_logger
from services.periodic_reporter import PeriodicReporter, get_reporter_instance
from domain.normalizers import canon_source, canon_symbol

logger = setup_logger("DiagnoseReports")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")

def check_redis_connection():
    """Проверка подключения к Redis"""
    print("\n" + "="*70)
    print("1️⃣ ПРОВЕРКА ПОДКЛЮЧЕНИЯ К REDIS")
    print("="*70)
    try:
        import redis as redis_lib
        r = redis_lib.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        print(f"✅ Redis подключение успешно: {REDIS_URL}")
        return r
    except Exception as e:
        print(f"❌ Ошибка подключения к Redis: {e}")
        return None

def check_periodic_reporter_service():
    """Проверка, запущен ли periodic-reporter сервис"""
    print("\n" + "="*70)
    print("2️⃣ ПРОВЕРКА СЕРВИСА PERIODIC-REPORTER")
    print("="*70)
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=periodic-reporter", "--format", "{{.Names}} {{.Status}}"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            print(f"✅ Сервис найден: {result.stdout.strip()}")
        else:
            print("⚠️ Сервис periodic-reporter не запущен или не найден")
            print("   Запустите: docker-compose up -d periodic-reporter")
    except Exception as e:
        print(f"⚠️ Не удалось проверить статус сервиса: {e}")

def check_pairs(r):
    """Проверка наличия пар source/symbol"""
    print("\n" + "="*70)
    print("3️⃣ ПРОВЕРКА ПАР SOURCE/SYMBOL")
    print("="*70)
    try:
        reporter = PeriodicReporter()
        pairs = reporter._discover_pairs()
        if pairs:
            print(f"✅ Найдено {len(pairs)} пар:")
            for i, (source, symbol) in enumerate(pairs[:10], 1):
                print(f"   {i}. {source} / {symbol}")
            if len(pairs) > 10:
                print(f"   ... и еще {len(pairs) - 10} пар")
            return pairs
        else:
            print("❌ Пар не найдено")
            print("\n   Проверяю источники данных:")
            
            # Проверка stats:strategies
            try:
                strategies = r.smembers("stats:strategies") or set()
                print(f"   - stats:strategies: {len(strategies)} стратегий")
                if strategies:
                    for s in list(strategies)[:5]:
                        print(f"     • {s}")
            except Exception as e:
                print(f"   - stats:strategies: ошибка - {e}")
            
            # Проверка trades:closed stream
            try:
                entries = r.xrevrange("trades:closed", max="+", count=10) or []
                print(f"   - trades:closed stream: {len(entries)} последних записей")
            except Exception as e:
                print(f"   - trades:closed stream: ошибка - {e}")
            
            # Проверка orders:open
            try:
                open_orders = r.smembers("orders:open") or set()
                print(f"   - orders:open: {len(open_orders)} открытых позиций")
            except Exception as e:
                print(f"   - orders:open: ошибка - {e}")
            
            return []
    except Exception as e:
        print(f"❌ Ошибка при поиске пар: {e}")
        return []

def check_trades_in_window(r, source, symbol):
    """Проверка наличия сделок в окне"""
    print("\n" + "="*70)
    print(f"4️⃣ ПРОВЕРКА СДЕЛОК В ОКНЕ: {source} / {symbol}")
    print("="*70)
    try:
        reporter = PeriodicReporter()
        metrics = reporter._gather_window_metrics_stream(source, symbol)
        total = int(metrics.get("total_trades", 0))
        
        if total > 0:
            print(f"✅ Найдено {total} сделок в окне")
            print(f"   Wins: {metrics.get('wins', 0)}, Losses: {metrics.get('losses', 0)}")
            print(f"   Total PnL: {metrics.get('total_pnl', 0):+.2f}")
        else:
            from services.periodic_reporter import RECENT_WINDOW_SECONDS
            print(f"❌ Нет сделок в окне {RECENT_WINDOW_SECONDS}s")
            print("\n   Проверяю данные в Redis:")
            
            # Проверка trades:closed stream
            try:
                cutoff_ms = get_ny_time_millis() - RECENT_WINDOW_SECONDS * 1000
                min_id = f"{cutoff_ms}-0"
                entries = r.xrevrange("trades:closed", max="+", min=min_id, count=100) or []
                print(f"   - trades:closed stream: {len(entries)} записей в окне")
                
                # Проверяем, есть ли записи с нужным source/symbol
                matched = 0
                for _, fields in entries[:10]:
                    t_source = canon_source(fields.get("source") or fields.get("strategy") or "")
                    t_symbol = canon_symbol(fields.get("symbol") or "")
                    if t_source == source and t_symbol == symbol:
                        matched += 1
                print(f"   - Совпадений с {source}/{symbol}: {matched}")
            except Exception as e:
                print(f"   - trades:closed stream: ошибка - {e}")
        
        return total > 0
    except Exception as e:
        print(f"❌ Ошибка при проверке сделок: {e}")
        return False

def check_notify_stream(r):
    """Проверка notify:telegram stream"""
    print("\n" + "="*70)
    print("5️⃣ ПРОВЕРКА NOTIFY:TELEGRAM STREAM")
    print("="*70)
    try:
        stream_len = r.xlen(NOTIFY_STREAM)
        print(f"✅ Stream {NOTIFY_STREAM}: {stream_len} сообщений")
        
        # Проверяем последние сообщения
        try:
            entries = r.xrevrange(NOTIFY_STREAM, max="+", count=10) or []
            print(f"\n   Последние {len(entries)} сообщений:")
            
            report_count = 0
            for entry_id, fields in entries:
                msg_type = fields.get("type", "unknown")
                source_field = fields.get("source", "unknown")
                text_preview = (fields.get("text", "") or "")[:50]
                
                if msg_type == "report":
                    report_count += 1
                    print(f"   ✅ {entry_id}: type={msg_type}, source={source_field}, text='{text_preview}...'")
                else:
                    print(f"   📨 {entry_id}: type={msg_type}, source={source_field}")
            
            if report_count > 0:
                print(f"\n   ✅ Найдено {report_count} отчетов в последних {len(entries)} сообщениях")
            else:
                print(f"\n   ⚠️ Отчетов не найдено в последних {len(entries)} сообщениях")
        except Exception as e:
            print(f"   ⚠️ Ошибка при чтении сообщений: {e}")
        
        # Проверка consumer groups
        try:
            groups = r.xinfo_groups(NOTIFY_STREAM)
            if groups:
                print(f"\n   Consumer groups: {len(groups)}")
                for group in groups:
                    print(f"     - {group.get('name', 'unknown')}: pending={group.get('pending', 0)}, consumers={group.get('consumers', 0)}")
            else:
                print(f"\n   ⚠️ Consumer groups не найдены (notify_worker может не работать)")
        except Exception as e:
            print(f"   ⚠️ Ошибка при проверке consumer groups: {e}")
        
        return True
    except Exception as e:
        print(f"❌ Ошибка при проверке stream: {e}")
        return False

def test_report_sending(r):
    """Тестовая отправка отчета"""
    print("\n" + "="*70)
    print("6️⃣ ТЕСТОВАЯ ОТПРАВКА ОТЧЕТА")
    print("="*70)
    try:
        from services.reporting_service import ReportingService
        
        test_msg = f"""
📊 <b>Тестовый отчет</b>
🕐 {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}

<i>Это тестовое сообщение для проверки цепочки доставки отчетов.</i>
"""
        
        reporting = ReportingService(redis_url=REDIS_URL)
        success = reporting.send_telegram_message(test_msg)
        
        if success:
            print("✅ Тестовый отчет опубликован в Redis stream")
            
            # Проверяем, что сообщение попало в stream
            time.sleep(1)
            try:
                entries = r.xrevrange(NOTIFY_STREAM, max="+", count=1) or []
                if entries:
                    entry_id, fields = entries[0]
                    if fields.get("type") == "report" and "Тестовый отчет" in fields.get("text", ""):
                        print(f"✅ Тестовое сообщение найдено в stream: {entry_id}")
                    else:
                        print(f"⚠️ Тестовое сообщение не найдено в последней записи stream")
                else:
                    print("⚠️ Stream пуст после публикации")
            except Exception as e:
                print(f"⚠️ Ошибка при проверке stream: {e}")
        else:
            print("❌ Не удалось опубликовать тестовый отчет")
        
        return success
    except Exception as e:
        print(f"❌ Ошибка при тестовой отправке: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_notify_worker():
    """Проверка работы notify_worker"""
    print("\n" + "="*70)
    print("7️⃣ ПРОВЕРКА NOTIFY_WORKER")
    print("="*70)
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=notify", "--format", "{{.Names}} {{.Status}}"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            print(f"✅ Найдены сервисы notify:")
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    print(f"   - {line.strip()}")
        else:
            print("⚠️ Сервисы notify не найдены")
            print("   Проверьте, запущен ли telegram-worker или bot-nest")
    except Exception as e:
        print(f"⚠️ Не удалось проверить статус notify_worker: {e}")

def main():
    """Главная функция диагностики"""
    print("\n" + "="*70)
    print("🔍 ДИАГНОСТИКА СИСТЕМЫ ОТЧЕТОВ")
    print("="*70)
    print(f"Время: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Redis URL: {REDIS_URL}")
    print(f"Notify Stream: {NOTIFY_STREAM}")
    
    # 1. Проверка Redis
    r = check_redis_connection()
    if not r:
        print("\n❌ Не удалось подключиться к Redis. Проверьте настройки.")
        return
    
    # 2. Проверка сервиса
    check_periodic_reporter_service()
    
    # 3. Проверка пар
    pairs = check_pairs(r)
    
    # 4. Проверка сделок для первой пары
    if pairs:
        source, symbol = pairs[0]
        has_trades = check_trades_in_window(r, source, symbol)
    else:
        print("\n⏭️ Пропуск проверки сделок (нет пар)")
        has_trades = False
    
    # 5. Проверка notify stream
    check_notify_stream(r)
    
    # 6. Проверка notify_worker
    check_notify_worker()
    
    # 7. Тестовая отправка
    test_report_sending(r)
    
    # Итоги
    print("\n" + "="*70)
    print("📋 ИТОГИ ДИАГНОСТИКИ")
    print("="*70)
    
    issues = []
    if not pairs:
        issues.append("❌ Не найдено пар source/symbol для отчетов")
    if pairs and not has_trades:
        issues.append("⚠️ Нет сделок в окне для найденных пар")
    
    if issues:
        print("\nОбнаружены проблемы:")
        for issue in issues:
            print(f"  {issue}")
        print("\nРекомендации:")
        print("  1. Проверьте, что сделки закрываются и попадают в trades:closed stream")
        print("  2. Проверьте, что source и symbol корректно записываются в Redis")
        print("  3. Убедитесь, что PERIODIC_REPORT_WINDOW_SECONDS достаточно большой")
        print("  4. Проверьте логи periodic-reporter: docker-compose logs periodic-reporter")
    else:
        print("\n✅ Основные проверки пройдены")
        print("\nЕсли отчеты все еще не приходят:")
        print("  1. Проверьте логи notify_worker: docker-compose logs notify-worker")
        print("  2. Проверьте, что TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID настроены")
        print("  3. Проверьте логи telegram-worker на наличие ошибок")
    
    print("\n" + "="*70)

if __name__ == "__main__":
    main()

