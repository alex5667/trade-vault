from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Crypto HTF Levels Aggregator.

Вычисляет Higher Time Frame уровни для криптовалютных инструментов:
- Previous Day High/Low/Middle
- Weekly High/Low
- Session opens (Asia/Europe/US)
- Order Block zones
- Fair Value Gap zones

Consumes `candles:data` stream и публикует результаты в Redis keys:
- htf:levels:{symbol} -> JSON с HTF уровнями
- htf:updated:{symbol} -> timestamp последнего обновления

Использует consumer group для at-most-once delivery.
"""

import os
import sys
import json
import time
from typing import Dict, List, Optional
from collections import defaultdict
import threading

# Добавляем путь к core для импорта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.redis_client import get_redis
from core.redis_stream_consumer import SyncRedisStreamHelper

# Конфигурация
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
CANDLES_STREAM = os.getenv("CANDLES_STREAM", "candles:data")
GROUP = os.getenv("HTF_GROUP", "htf-aggregator-group")
CONSUMER_NAME_PREFIX = os.getenv("HTF_CONSUMER", "htf-aggregator")

# Таймфреймы для анализа
SUPPORTED_TIMEFRAMES = ["1d", "1w", "1M"]
# Получаем список символов из ENV
CRYPTO_SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",")
CRYPTO_SYMBOLS = [s.strip().upper() for s in CRYPTO_SYMBOLS if s.strip()]

# Периоды для расчета
DAILY_PERIODS = 30  # дней для анализа
WEEKLY_PERIODS = 12  # недель для анализа


class HTFAggregator:
    """
    Агрегатор HTF уровней для крипты.
    """

    def __init__(self):
        """Инициализация aggregator."""
        try:
            self.redis_client = get_redis()
        except Exception as e:
            print(f"❌ Ошибка подключения к Redis: {e}")
            raise

        self.is_running = False

        # Хранилище исторических данных: symbol -> timeframe -> bars
        self.history: Dict[str, Dict[str, List[Dict]]] = defaultdict(lambda: defaultdict(list))

        print("✅ Crypto HTF Aggregator инициализирован")
        print(f"   Candles Stream: {CANDLES_STREAM}")
        print(f"   Consumer Group: {GROUP}")
        print(f"   Symbols: {CRYPTO_SYMBOLS}")
        sys.stdout.flush()

    def start(self) -> None:
        """Запускает aggregator в отдельном потоке."""
        if self.is_running:
            print("⚠️ HTF Aggregator уже запущен")
            return

        self.is_running = True
        thread = threading.Thread(target=self._run_loop, daemon=True)
        thread.start()
        print("🚀 Crypto HTF Aggregator запущен")
        sys.stdout.flush()

    def stop(self) -> None:
        """Останавливает aggregator."""
        self.is_running = False
        print("⛔ Crypto HTF Aggregator остановлен")
        sys.stdout.flush()

    def _run_loop(self) -> None:
        """Основной цикл обработки свечей."""
        try:
            consumer_name = f"{CONSUMER_NAME_PREFIX}-{os.getpid()}-{int(time.time())}"
            stream_helper = SyncRedisStreamHelper(self.redis_client, GROUP, consumer_name)
            stream_helper.ensure_group(CANDLES_STREAM)

            print(f"🔄 Запуск цикла HTF агрегации (consumer: {consumer_name})...")
            sys.stdout.flush()

            candle_count = 0
            update_count = 0
            start_time = time.time()

            # Основной цикл
            while self.is_running:
                try:
                    # Читаем свечи из стрима
                    messages = stream_helper.read(
                        {CANDLES_STREAM: '>'},
                        count=100,
                        block=1000,
                    )

                    if not messages:
                        continue

                    for stream, items in messages:
                        for msg_id, fields in items:
                            try:
                                # Обрабатываем свечу
                                self._process_candle(fields)
                                candle_count += 1

                                # ACK сообщения
                                stream_helper.ack(CANDLES_STREAM, msg_id)

                            except Exception as e:
                                print(f"❌ Ошибка обработки свечи {msg_id}: {e}")
                                continue

                    # Периодически обновляем HTF уровни (каждые 10000 свечей)
                    if candle_count % 10000 == 0 and candle_count > 0:
                        updated_symbols = self._update_all_htf_levels()
                        update_count += len(updated_symbols)
                        print(f"📊 HTF update: {len(updated_symbols)} symbols, total candles: {candle_count}")
                        sys.stdout.flush()

                except Exception as e:
                    print(f"❌ Ошибка в основном цикле: {e}")
                    time.sleep(1)
                    continue

            # Финальная статистика
            elapsed = time.time() - start_time
            print(f"📈 HTF Aggregator stats: {candle_count} candles processed, "
                  f"{update_count} HTF updates in {elapsed:.1f}s")
            sys.stdout.flush()

        except Exception as e:
            print(f"❌ Критическая ошибка HTF Aggregator: {e}")
            raise

    def _process_candle(self, fields: Dict) -> None:
        """
        Обрабатывает одну свечу из стрима.

        Формат fields:
        {
            'symbol': 'BTCUSDT',
            'tf': '1d',
            'ts': '1760546759999',
            'payload': '{"openTime":..., "closeTime":..., "open":"...", ...}'
        }
        """
        try:
            symbol = fields.get('symbol', '').upper()
            tf = fields.get('tf', '1m')
            payload_str = fields.get('payload', '{}')

            # Фильтруем только поддерживаемые символы и таймфреймы
            if symbol not in CRYPTO_SYMBOLS or tf not in SUPPORTED_TIMEFRAMES:
                return

            # Парсим payload
            payload = json.loads(payload_str)

            # Извлекаем OHLC данные
            bar = {
                'timestamp': int(payload.get('closeTime') or payload.get('T') or fields.get('ts') or 0),
                'open': float(payload.get('open') or payload.get('o') or 0),
                'high': float(payload.get('high') or payload.get('h') or 0),
                'low': float(payload.get('low') or payload.get('l') or 0),
                'close': float(payload.get('close') or payload.get('c') or 0),
                'volume': float(payload.get('volume') or payload.get('v') or 0)
            }

            # Добавляем в историю
            self.history[symbol][tf].append(bar)

            # Ограничиваем историю (не храним слишком много данных)
            max_bars = DAILY_PERIODS if tf == '1d' else WEEKLY_PERIODS
            if len(self.history[symbol][tf]) > max_bars:
                self.history[symbol][tf] = self.history[symbol][tf][-max_bars:]

        except Exception as e:
            print(f"❌ Ошибка обработки свечи: {e}, fields: {fields}")
            raise

    def _update_all_htf_levels(self) -> List[str]:
        """Обновляет HTF уровни для всех символов."""
        updated_symbols = []

        for symbol in CRYPTO_SYMBOLS:
            try:
                htf_data = self._calculate_htf_levels(symbol)
                if htf_data:
                    self._save_htf_levels(symbol, htf_data)
                    updated_symbols.append(symbol)
            except Exception as e:
                print(f"❌ Ошибка обновления HTF для {symbol}: {e}")
                continue

        return updated_symbols

    def _calculate_htf_levels(self, symbol: str) -> Optional[Dict]:
        """
        Вычисляет HTF уровни для символа на основе исторических данных.
        """
        try:
            # Получаем дневные и недельные данные
            daily_bars = self.history[symbol].get('1d', [])
            weekly_bars = self.history[symbol].get('1w', [])

            if not daily_bars:
                return None

            # Previous Day уровни (последняя закрытая свеча)
            last_daily = daily_bars[-1]
            pdh = last_daily['high']
            pdl = last_daily['low']
            pdm = (pdh + pdl) / 2

            # Weekly уровни
            week_hi = max((bar['high'] for bar in weekly_bars), default=0.0)
            week_lo = min((bar['low'] for bar in weekly_bars), default=0.0)

            # Session opens (упрощенная логика - используем последние closes как proxy)
            # В реальности нужно определять сессии по времени
            asia_open = last_daily['close']  # stub
            europe_open = last_daily['close']  # stub
            us_open = last_daily['close']  # stub

            # Order Block и FVG zones (пока пустые списки - нужна дополнительная логика)
            ob_zones = []
            fvg_zones = []

            return {
                "pdh": pdh,
                "pdl": pdl,
                "pdm": pdm,
                "week_hi": week_hi,
                "week_lo": week_lo,
                "asia_open": asia_open,
                "europe_open": europe_open,
                "us_open": us_open,
                "ob_zones": ob_zones,
                "fvg_zones": fvg_zones,
                "updated_at": get_ny_time_millis()
            }

        except Exception as e:
            print(f"❌ Ошибка расчета HTF уровней для {symbol}: {e}")
            return None

    def _save_htf_levels(self, symbol: str, htf_data: Dict) -> None:
        """Сохраняет HTF уровни в Redis."""
        try:
            # Основной ключ с данными
            levels_key = f"htf:levels:{symbol}"
            self.redis_client.set(levels_key, json.dumps(htf_data))

            # Ключ с timestamp обновления
            update_key = f"htf:updated:{symbol}"
            self.redis_client.set(update_key, str(htf_data['updated_at']))

            print(f"💾 HTF levels saved for {symbol}: PDH={htf_data['pdh']:.2f}, "
                  f"PDL={htf_data['pdl']:.2f}")

        except Exception as e:
            print(f"❌ Ошибка сохранения HTF уровней для {symbol}: {e}")
            raise


def main():
    """Основная функция для запуска сервиса."""
    aggregator = HTFAggregator()
    aggregator.start()

    try:
        # Бесконечный цикл
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Получен сигнал остановки...")
        aggregator.stop()
        print("✅ HTF Aggregator остановлен")


if __name__ == "__main__":
    main()
