"""
MT5 Tick Streamer - публикует тиковые данные из MetaTrader 5 в Redis Stream.

⚠️ УСТАРЕЛО для Linux! Используйте HTTP Bridge вместо этого модуля:
   MT5 (Wine) → TickBridge EA → HTTP POST → tick_ingest_server → Redis Stream

ФУНКЦИОНАЛ (только для Windows):
- Подключение к MT5 терминалу через MetaTrader5 модуль
- Получение тиковых данных для  (или другого символа)
- Публикация тиков в Redis Stream для дальнейшей обработки
- Инкрементальное чтение без дублирования

РЕКОМЕНДУЕМАЯ АРХИТЕКТУРА (Linux):
1. MT5 под Wine на Linux
2. MQL5 EA TickBridge отправляет тики через HTTP
3. FastAPI сервис (tick_ingest_server) принимает и публикует в Redis
4. XAU OrderFlow Handler читает из Redis Stream

См. документацию:
- mt5/README_MT5_SETUP.md - установка MT5 под Wine
- mt5/TickBridge.mq5 - MQL5 Expert Advisor
- services/tick_ingest_server.py - HTTP сервер для приема тиков

ИСПОЛЬЗОВАНИЕ (только Windows, не рекомендуется):
- На Windows с установленным MT5 терминалом
- Требует pip install MetaTrader5
- Прямой доступ к MT5 API

ИНТЕГРАЦИЯ:
- Публикует в stream:tick_ (настраивается через env)
- Использует DualRedisClient для устойчивости к сбоям
"""

import os
import json
import time
import sys

try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    print("")
    print("=" * 80)
    print("⚠️  MetaTrader5 модуль не установлен")
    print("=" * 80)
    print("")
    print("Для Linux используйте РЕКОМЕНДУЕМУЮ архитектуру:")
    print("  1. MT5 под Wine")
    print("  2. TickBridge.mq5 EA отправляет тики через HTTP")
    print("  3. tick-ingest-server принимает и публикует в Redis")
    print("")
    print("Запуск tick-ingest-server:")
    print("  docker-compose up -d tick-ingest-server")
    print("")
    print("См. документацию:")
    print("  - mt5/README_MT5_SETUP.md")
    print("  - mt5/TickBridge.mq5")
    print("  - services/tick_ingest_server.py")
    print("")
    print("Для Windows (не рекомендуется для продакшена):")
    print("  pip install MetaTrader5")
    print("=" * 80)
    print("")

from core.dual_redis_client import get_dual_signals_redis
from core.config import XAU_TICK_STREAM, XAU_TICK_STREAM_MAXLEN


class MT5TickStreamer:
    """
    Стример тиковых данных из MT5 в Redis Stream.
    """
    
    def __init__(self):
        """Инициализация стримера с конфигурацией из переменных окружения."""
        self.symbol = os.getenv("XAU_SYMBOL")
        self.tick_stream = XAU_TICK_STREAM
        self.maxlen = XAU_TICK_STREAM_MAXLEN
        self.poll_interval = float(os.getenv("XAU_TICK_POLL_INTERVAL", "0.2"))  # 5 Hz
        self.tick_fetch_count = int(os.getenv("XAU_TICK_FETCH_COUNT", "500"))
        self.tick_lookback_sec = float(os.getenv("XAU_TICK_LOOKBACK_SEC", "5.0"))
        
        # Redis клиент (dual для надежности)
        self.redis_client = get_dual_signals_redis()
        
        # Состояние
        self.is_running = False
        self.last_ts = 0
        
        print(f"✅ MT5TickStreamer инициализирован для {self.symbol}")
        print(f"   Stream: {self.tick_stream}, Poll: {self.poll_interval}s")
        sys.stdout.flush()
    
    def start(self) -> None:
        """Запускает стример тиков."""
        if not MT5_AVAILABLE:
            print("")
            print("❌ MT5 модуль недоступен, стример не может быть запущен")
            print("")
            print("💡 Используйте HTTP Bridge архитектуру вместо прямого доступа к MT5:")
            print("   docker-compose up -d tick-ingest-server")
            print("")
            sys.stdout.flush()
            return
        
        if self.is_running:
            print("⚠️ MT5TickStreamer уже запущен")
            return
        
        # Инициализация MT5
        if not mt5.initialize():
            print(f"❌ MT5 инициализация не удалась: {mt5.last_error()}")
            sys.stdout.flush()
            return
        
        # Проверка символа
        symbol_info = mt5.symbol_info(self.symbol)
        if symbol_info is None:
            print(f"❌ Символ {self.symbol} не найден в MT5")
            mt5.shutdown()
            sys.stdout.flush()
            return
        
        if not symbol_info.visible:
            print(f"⚠️ Символ {self.symbol} не видим, активируем...")
            if not mt5.symbol_select(self.symbol, True):
                print(f"❌ Не удалось активировать символ {self.symbol}")
                mt5.shutdown()
                sys.stdout.flush()
                return
        
        self.is_running = True
        print(f"🚀 MT5TickStreamer запущен для {self.symbol}")
        sys.stdout.flush()
        
        try:
            self._stream_loop()
        except KeyboardInterrupt:
            print("⛔ Получен сигнал завершения...")
            sys.stdout.flush()
        finally:
            self.stop()
    
    def stop(self) -> None:
        """Останавливает стример и отключается от MT5."""
        self.is_running = False
        if MT5_AVAILABLE:
            mt5.shutdown()
        print("⛔ MT5TickStreamer остановлен")
        sys.stdout.flush()
    
    def _stream_loop(self) -> None:
        """Основной цикл чтения и публикации тиков."""
        tick_count = 0
        start_time = time.time()
        
        while self.is_running:
            try:
                # Получаем тики за последние N секунд
                current_time = time.time()
                from_time = current_time - self.tick_lookback_sec
                
                ticks = mt5.copy_ticks_from(
                    self.symbol, 
                    int(from_time * 1000),  # время в миллисекундах
                    self.tick_fetch_count, 
                    mt5.COPY_TICKS_ALL
                )
                
                if ticks is None or len(ticks) == 0:
                    time.sleep(self.poll_interval)
                    continue
                
                # Обрабатываем каждый тик
                new_ticks = 0
                for tick in ticks:
                    tick_ts = int(tick['time_msc'])
                    
                    # Пропускаем уже обработанные тики
                    if tick_ts <= self.last_ts:
                        continue
                    
                    self.last_ts = tick_ts
                    new_ticks += 1
                    
                    # Формируем payload
                    payload = {
                        "ts": tick_ts,
                        "bid": float(tick.get('bid', 0)),
                        "ask": float(tick.get('ask', 0)),
                        "last": float(tick.get('last', 0)),
                        "volume": float(tick.get('volume', 0)),
                        "flags": int(tick.get('flags', 0))  # для направления сделки
                    }
                    
                    # Публикуем в Redis Stream
                    try:
                        self.redis_client.xadd(
                            self.tick_stream,
                            {"data": json.dumps(payload)},
                            maxlen=self.maxlen,
                            approximate=True
                        )
                        tick_count += 1
                    except Exception as e:
                        print(f"❌ Ошибка публикации тика в Redis: {e}")
                        sys.stdout.flush()
                
                # Статистика каждые 60 секунд
                if time.time() - start_time >= 60:
                    rate = tick_count / 60.0
                    print(f"📊 MT5TickStreamer: {tick_count} тиков за 60с ({rate:.1f} тиков/с)")
                    sys.stdout.flush()
                    tick_count = 0
                    start_time = time.time()
                
                # Задержка перед следующим опросом
                time.sleep(self.poll_interval)
                
            except Exception as e:
                print(f"❌ Ошибка в цикле стримера: {e}")
                sys.stdout.flush()
                time.sleep(1)  # пауза при ошибке


def main():
    """Точка входа для запуска стримера как отдельного процесса."""
    streamer = MT5TickStreamer()
    streamer.start()


if __name__ == "__main__":
    main()

