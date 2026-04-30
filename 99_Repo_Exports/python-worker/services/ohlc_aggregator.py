from utils.time_utils import get_ny_time_millis
"""
OHLC Aggregator - агрегация дневных H/L/C из тиков для расчета Pivot уровней.

ФУНКЦИОНАЛ:
- Чтение тиков из stream:tick_XAUUSD через consumer group
- Агрегация в дневные H/L/C (по UTC дню)
- Публикация в Redis keys при закрытии дня
- Поддержка custom торговых сессий (NY close и т.д.)

ИНТЕГРАЦИЯ:
- Использует те же паттерны consumer groups (XREADGROUP/XACK)
- Публикует в pivots:latest (используется XAU handler)
- История в pivots:hlc:<DATE> keys
- События в stream pivots:events

ЗАПУСК:
    python services/ohlc_aggregator.py

Systemd:
    deploy/systemd/ohlc-aggregator.service
"""

import os
import json
import sys
import time
import threading
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Fallback для Python < 3.9
    from datetime import timezone as ZoneInfo
    print("⚠️ zoneinfo недоступен, используем UTC timezone")

# Добавляем путь к core для импорта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.redis_client import get_redis
from core.config import XAU_TICK_STREAM
from core.redis_stream_consumer import SyncRedisStreamHelper

# Конфигурация
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
TICK_STREAM = XAU_TICK_STREAM
GROUP = os.getenv("XAU_OHLC_GROUP", "xauusd-ohlc-group")
CONSUMER_NAME_PREFIX = os.getenv("XAU_OHLC_CONSUMER", "ohlc-aggregator")

# Торговая сессия (для правильного определения "дня")
SESSION_TZ = os.getenv("SESSION_CLOSE_TZ", "UTC")
SESSION_HOUR = int(os.getenv("SESSION_CLOSE_HOUR", "0"))  # 0..23 (0 = midnight UTC)
SESSION_MIN = int(os.getenv("SESSION_CLOSE_MIN", "0"))     # 0..59


class DailyAggregator:
    """
    Агрегатор дневных H/L/C из тиков.
    """
    
    def __init__(self):
        """Инициализация aggregator."""
        try:
            self.redis_client = get_redis()
        except Exception as e:
            print(f"❌ Ошибка подключения к Redis: {e}")
            raise
        
        self.is_running = False
        
        # Состояние текущего дня
        self.current_day = None
        self.day_high = None
        self.day_low = None
        self.day_close = None
        self.last_tick_time = None
        self.tick_count_total = 0
        
        # Timezone
        try:
            self.tz = ZoneInfo(SESSION_TZ)
        except Exception:
            self.tz = timezone.utc
            print(f"⚠️ Timezone {SESSION_TZ} недоступен, используем UTC")
        
        # Попытка загрузить последние данные из Redis при старте
        self._load_state_from_redis()
        
        print("✅ Daily OHLC Aggregator инициализирован")
        print(f"   Tick Stream: {TICK_STREAM}")
        print(f"   Consumer Group: {GROUP}")
        print(f"   Session Close: {SESSION_HOUR:02d}:{SESSION_MIN:02d} {SESSION_TZ}")
        sys.stdout.flush()
    
    def start(self) -> None:
        """Запускает aggregator в отдельном потоке."""
        if self.is_running:
            print("⚠️ Daily OHLC Aggregator уже запущен")
            return
        
        self.is_running = True
        thread = threading.Thread(target=self._run_loop, daemon=True)
        thread.start()
        print("🚀 Daily OHLC Aggregator запущен")
        sys.stdout.flush()
    
    def stop(self) -> None:
        """Останавливает aggregator."""
        self.is_running = False
        print("⛔ Daily OHLC Aggregator остановлен")
        sys.stdout.flush()
    
    def _run_loop(self) -> None:
        """Основной цикл обработки тиков."""
        try:
            consumer_name = f"{CONSUMER_NAME_PREFIX}-{os.getpid()}-{int(time.time())}"
            stream_helper = SyncRedisStreamHelper(self.redis_client, GROUP, consumer_name)
            stream_helper.ensure_group(TICK_STREAM)
            
            print(f"🔄 Запуск цикла агрегации (consumer: {consumer_name})...")
            sys.stdout.flush()
            
            tick_count = 0
            day_count = 0
            start_time = time.time()
            
            # Основной цикл
            while self.is_running:
                try:
                    # Читаем тики из стрима
                    messages = stream_helper.read(
                        {TICK_STREAM: '>'}
                        count=200
                        block=1000
                    )
                    
                    if not messages:
                        continue
                    
                    for stream, items in messages:
                        for msg_id, fields in items:
                            try:
                                # Обрабатываем тик
                                # Поддержка двух форматов: JSON в поле "data" или плоские поля Redis
                                if "data" in fields:
                                    try:
                                        tick_data = json.loads(fields["data"])
                                    except Exception:
                                        tick_data = fields
                                else:
                                    tick_data = fields
                                    
                                self._process_tick(tick_data)
                                tick_count += 1
                                
                            except Exception as e:
                                print(f"❌ Ошибка обработки тика {msg_id}: {e}")
                                sys.stdout.flush()
                            finally:
                                # ACK сообщения
                                try:
                                    stream_helper.ack(stream, msg_id)
                                except Exception as e:
                                    print(f"❌ Ошибка ACK {msg_id}: {e}")
                                    sys.stdout.flush()
                    
                    # Статистика каждые 60 секунд
                    if time.time() - start_time >= 60:
                        if tick_count > 0:
                            print(f"📊 OHLC Aggregator: {tick_count} тиков обработано за 60с (всего: {self.tick_count_total})")
                            high_str = f"{self.day_high:.2f}" if self.day_high is not None else "N/A"
                            low_str = f"{self.day_low:.2f}" if self.day_low is not None else "N/A"
                            close_str = f"{self.day_close:.2f}" if self.day_close is not None else "N/A"
                            day_str = self.current_day if self.current_day else "ожидание данных"
                            print(f"   Текущий день: {day_str}, H:{high_str}, L:{low_str}, C:{close_str}")
                            if self.last_tick_time:
                                ago = int(time.time() - self.last_tick_time)
                                print(f"   Последний тик: {ago}с назад")
                        else:
                            if self.tick_count_total == 0:
                                print(f"⏳ OHLC Aggregator: ожидание тиковых данных из {TICK_STREAM}")
                            else:
                                print(f"ℹ️ OHLC Aggregator: нет новых тиков за 60с (всего обработано: {self.tick_count_total})")
                        sys.stdout.flush()
                        tick_count = 0
                        day_count = 0
                        start_time = time.time()
                        
                except Exception as e:
                    print(f"❌ Ошибка в цикле агрегации: {e}")
                    sys.stdout.flush()
                    if "NOGROUP" in str(e).upper():
                        try:
                            self.redis_client.xgroup_create(
                                TICK_STREAM
                                GROUP
                                id='0'
                                mkstream=True
                            )
                            print(f"✅ Consumer group {GROUP} пересоздана для {TICK_STREAM}")
                            sys.stdout.flush()
                        except Exception as recreate_err:
                            if "BUSYGROUP" in str(recreate_err):
                                print(f"ℹ️ Consumer group {GROUP} уже существует после проверки")
                            else:
                                print(f"❌ Ошибка пересоздания consumer group: {recreate_err}")
                            sys.stdout.flush()
                    time.sleep(1)
                    
        except Exception as e:
            print(f"❌ Критическая ошибка Daily OHLC Aggregator: {e}")
            sys.stdout.flush()
    
    def _process_tick(self, tick_data: dict) -> None:
        """
        Обработка одного тика для агрегации.
        
        Args:
            tick_data: Данные тика
        """
        try:
            # Вычисляем mid price
            bid = float(tick_data.get("bid", 0))
            ask = float(tick_data.get("ask", 0))
            last = float(tick_data.get("last", 0))
            trade_price = float(tick_data.get("price", 0))
            
            price = (bid + ask) / 2 if (bid and ask) else (last or trade_price)
            
            if price <= 0:
                return
            
            # Определяем день (простая версия - UTC день)
            ts = int(tick_data.get("ts", get_ny_time_millis()))
            day = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date().isoformat()
            
            # Обновляем счетчики
            self.last_tick_time = time.time()
            self.tick_count_total += 1
            
            # Инициализация или смена дня
            if self.current_day is None:
                self._init_day(day, price)
            elif day != self.current_day:
                self._finalize_day()
                self._init_day(day, price)
            else:
                # Обновляем H/L/C текущего дня
                if price > self.day_high:
                    self.day_high = price
                if price < self.day_low:
                    self.day_low = price
                self.day_close = price
        except Exception as e:
            print(f"⚠️ Ошибка обработки тика: {e}, данные: {tick_data}")
            sys.stdout.flush()
    
    def _init_day(self, day: str, price: float) -> None:
        """
        Инициализирует новый день.
        
        Args:
            day: ISO дата (YYYY-MM-DD)
            price: Первая цена дня
        """
        self.current_day = day
        self.day_high = price
        self.day_low = price
        self.day_close = price
        
        print(f"📅 Новый день начат: {day}, цена открытия: {price:.2f}")
        sys.stdout.flush()
    
    def _load_state_from_redis(self) -> None:
        """
        Загружает последнее состояние из Redis при старте.
        """
        try:
            # Пытаемся загрузить последние данные
            hlc_str = self.redis_client.get("pivots:latest")
            if hlc_str:
                hlc = json.loads(hlc_str)
                today = datetime.now(tz=timezone.utc).date().isoformat()
                
                # Если данные сегодняшние - восстанавливаем состояние
                if hlc.get("day") == today:
                    self.current_day = hlc["day"]
                    self.day_high = hlc["H"]
                    self.day_low = hlc["L"]
                    self.day_close = hlc["C"]
                    print(f"📊 Восстановлено состояние дня {today}:")
                    print(f"   H: {self.day_high:.2f}, L: {self.day_low:.2f}, C: {self.day_close:.2f}")
                else:
                    print(f"ℹ️ Последние данные за {hlc.get('day')}, ждем тики для нового дня")
            else:
                print("ℹ️ Нет сохраненного состояния, ждем первые тики")
        except Exception as e:
            print(f"⚠️ Не удалось загрузить состояние из Redis: {e}")
        sys.stdout.flush()
    
    def _finalize_day(self) -> None:
        """
        Завершает текущий день и публикует H/L/C.
        """
        if self.current_day is None:
            return
        
        hlc = {
            "H": self.day_high
            "L": self.day_low
            "C": self.day_close
            "day": self.current_day
        }
        
        hlc_json = json.dumps(hlc)
        
        try:
            # 1. Сохраняем как последний (для handler)
            self.redis_client.set("pivots:latest", hlc_json)
            
            # 2. Сохраняем в историю
            self.redis_client.set(f"pivots:hlc:{self.current_day}", hlc_json)
            
            # 3. Публикуем событие
            self.redis_client.xadd(
                "pivots:events"
                {"data": json.dumps({"type": "daily_close", "hlc": hlc})}
                maxlen=100
                approximate=True
            )
            
            print(f"✅ День {self.current_day} завершен:")
            print(f"   H: {self.day_high:.2f}, L: {self.day_low:.2f}, C: {self.day_close:.2f}")
            sys.stdout.flush()
            
        except Exception as e:
            print(f"❌ Ошибка публикации H/L/C: {e}")
            sys.stdout.flush()


def main():
    """Точка входа для standalone запуска."""
    aggregator = DailyAggregator()
    aggregator.start()
    
    try:
        # Держим процесс alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("⛔ Получен сигнал завершения...")
        aggregator.stop()


if __name__ == "__main__":
    main()

