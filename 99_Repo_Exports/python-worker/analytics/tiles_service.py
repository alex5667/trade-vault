from __future__ import annotations
"""
Tiles Service - Фоновый воркер для записи данных в Parquet тайлы.

Функции:
- Чтение данных из Redis Streams
- Буферизация и батчинг
- Запись в Parquet тайлы
- Сохранение позиции чтения (last_id)

Читает потоки:
- signals:{strategy}:{symbol} → тайлы signals/
- trades:closed → тайлы orders/
- events:trades → тайлы events/

Использование:
    python -m analytics.tiles_service
"""

import os
import json
import time
import sys
import signal as sig
from typing import Dict, List
from pathlib import Path

# Добавляем python-worker в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

import redis

from common.log import setup_logger
from analytics.parquet_sink import ParquetSink


class TilesService:
    """
    Фоновый сервис для записи данных из Redis в Parquet тайлы.
    
    Читает потоки, буферизует данные и периодически пишет в Parquet.
    """

    def __init__(self):
        """Инициализация Tiles Service"""
        self.logger = setup_logger("TilesService")

        # Redis подключение
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = redis.from_url(self.redis_url, decode_responses=True)

        try:
            self.r.ping()
            self.logger.info("✅ Redis подключение установлено")
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к Redis: {e}")
            raise

        # Конфигурация потоков
        signals_patterns = os.getenv("TILES_SIGNALS", "signals:orderflow:*,signals:ta:*")
        self.signals_patterns = [p.strip() for p in signals_patterns.split(",")]

        self.closed_trades_stream = os.getenv("TILES_CLOSED_TRADES", "trades:closed")
        self.trade_events_stream = os.getenv("TILES_TRADE_EVENTS", "events:trades")

        # Параметры батчинга
        self.flush_every = int(os.getenv("TILES_FLUSH_EVERY", "500"))
        self.poll_ms = int(os.getenv("TILES_POLL_MS", "500"))

        # Parquet sink
        self.sink = ParquetSink(os.getenv("PARQUET_BASE_DIR", "/data/tiles"))

        # Буферы
        self._buf_signals: List[Dict] = []
        self._buf_orders: List[Dict] = []
        self._buf_events: List[Dict] = []

        # Флаг работы
        self.is_running = False
        self.should_stop = False

        # Счётчики
        self.stats = {
            "signals_read": 0,
            "orders_read": 0,
            "events_read": 0,
            "files_written": 0,
            "errors": 0
        }

        # Таймер статистики (запись не чаще одного раза в 60 с)
        self._last_stats_ts: float = 0.0

        # Обработка сигналов для graceful shutdown
        sig.signal(sig.SIGINT, self._signal_handler)
        sig.signal(sig.SIGTERM, self._signal_handler)

        self.logger.info("🚀 Tiles Service инициализирован")
        self.logger.info(f"📁 Директория тайлов: {self.sink.base_dir}")
        self.logger.info(f"📊 Flush каждые: {self.flush_every} записей")

    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для graceful shutdown"""
        self.logger.info(f"⚠️ Получен сигнал {signum}, завершение работы...")
        self.stop()

    def _key_last(self, stream: str) -> str:
        """Ключ для хранения last_id"""
        return f"analytics:tile:lastid:{stream}"

    def _last_id(self, stream: str) -> str:
        """Получение last_id для потока"""
        return self.r.get(self._key_last(stream)) or "0-0"

    def _save_last(self, stream: str, msg_id: str):
        """Сохранение last_id для потока"""
        self.r.set(self._key_last(stream), msg_id)

    def _resolve_streams(self, pattern: str) -> List[str]:
        """Разрешение паттерна в список потоков"""
        if "*" in pattern:
            return self.r.keys(pattern)
        return [pattern]

    def _ts_from_msg_id(self, msg_id: str) -> float:
        """
        Извлечение timestamp из Redis message ID.
        
        Args:
            msg_id: Redis message ID (format: milliseconds-sequence)
            
        Returns:
            Timestamp в секундах
        """
        try:
            ms = int(msg_id.split("-")[0])
            return ms / 1000.0
        except Exception:
            return time.time()

    def _consume_stream(self, stream: str, kind: str):
        """
        Чтение и буферизация данных из потока.
        
        Args:
            stream: Имя потока Redis
            kind: Тип данных ('signals', 'orders', 'events')
        """
        try:
            last = self._last_id(stream)

            # Читаем новые сообщения
            msgs = self.r.xread(
                {stream: last},
                count=self.flush_every,
                block=self.poll_ms
            )

            if not msgs:
                return

            for stream_name, entries in msgs:
                for msg_id, fields in entries:
                    # Формируем запись
                    row = dict(fields)

                    # Приводим timestamp
                    if "ts" not in row and "time" not in row and "timestamp" not in row:
                        row["ts"] = self._ts_from_msg_id(msg_id)
                    elif "time" in row:
                        row["ts"] = float(row.get("time", 0)) / 1000.0
                    elif "timestamp" in row:
                        row["ts"] = float(row.get("timestamp", 0)) / 1000.0

                    # Парсим JSON поля если есть
                    if "data" in row:
                        try:
                            data = json.loads(row["data"])
                            row.update(data)
                        except Exception:
                            pass

                    # Добавляем в соответствующий буфер
                    if kind == "signals":
                        self._buf_signals.append(row)
                        self.stats["signals_read"] += 1
                    elif kind == "orders":
                        self._buf_orders.append(row)
                        self.stats["orders_read"] += 1
                    else:  # events
                        self._buf_events.append(row)
                        self.stats["events_read"] += 1

                    last = msg_id

                # Сохраняем позицию
                self._save_last(stream_name, last)

        except Exception as e:
            if "timeout" not in str(e).lower():
                self.logger.error(f"❌ Ошибка чтения потока {stream}: {e}")
                self.stats["errors"] += 1

    def _flush_if_needed(self):
        """Запись буферов в Parquet если накопилось достаточно"""
        try:
            # Signals
            if len(self._buf_signals) >= self.flush_every:
                path = self.sink.write_records("signals", self._buf_signals, ts_field="ts")
                if path:
                    self.stats["files_written"] += 1
                self._buf_signals.clear()

            # Orders
            if len(self._buf_orders) >= self.flush_every:
                path = self.sink.write_records("orders", self._buf_orders, ts_field="entry_time")
                if path:
                    self.stats["files_written"] += 1
                self._buf_orders.clear()

            # Events
            if len(self._buf_events) >= self.flush_every:
                path = self.sink.write_records("events", self._buf_events, ts_field="ts")
                if path:
                    self.stats["files_written"] += 1
                self._buf_events.clear()

        except Exception as e:
            self.logger.error(f"❌ Ошибка записи буферов: {e}")
            self.stats["errors"] += 1

    def _flush_all(self):
        """Принудительная запись всех буферов"""
        try:
            if self._buf_signals:
                self.sink.write_records("signals", self._buf_signals, ts_field="ts")
                self._buf_signals.clear()

            if self._buf_orders:
                self.sink.write_records("orders", self._buf_orders, ts_field="entry_time")
                self._buf_orders.clear()

            if self._buf_events:
                self.sink.write_records("events", self._buf_events, ts_field="ts")
                self._buf_events.clear()

            self.logger.info("✅ Все буферы записаны")

        except Exception as e:
            self.logger.error(f"❌ Ошибка записи буферов: {e}")

    def run(self):
        """Главный цикл сервиса"""
        self.logger.info("🚀 Запуск Tiles Service...")
        self.logger.info(f"📊 Паттерны сигналов: {self.signals_patterns}")
        self.logger.info(f"📊 Closed trades: {self.closed_trades_stream}")
        self.logger.info(f"📊 Events: {self.trade_events_stream}")

        self.is_running = True
        self.should_stop = False

        try:
            while not self.should_stop:
                # Читаем signals из всех паттернов
                for pattern in self.signals_patterns:
                    for stream in self._resolve_streams(pattern):
                        self._consume_stream(stream, "signals")

                # Читаем closed trades
                self._consume_stream(self.closed_trades_stream, "orders")

                # Читаем trade events
                self._consume_stream(self.trade_events_stream, "events")

                # Записываем если накопилось
                self._flush_if_needed()

                # Периодическая статистика (не чаще одного раза в 60 с)
                now = time.time()
                if now - self._last_stats_ts >= 60.0:
                    self._log_stats()
                    self._last_stats_ts = now

                # Небольшая пауза
                time.sleep(self.poll_ms / 1000.0)

        except KeyboardInterrupt:
            self.logger.info("⚠️ Получен KeyboardInterrupt")
        finally:
            self.stop()

    def stop(self):
        """Остановка сервиса"""
        if not self.is_running:
            return

        self.logger.info("🛑 Остановка Tiles Service...")

        self.should_stop = True
        self.is_running = False

        # Записываем оставшиеся буферы
        self._flush_all()

        # Финальная статистика
        self._log_stats()

        self.logger.info("✅ Tiles Service остановлен")

    def _log_stats(self):
        """Логирование статистики"""
        self.logger.info(
            f"📊 Статистика: "
            f"Signals {self.stats['signals_read']} | "
            f"Orders {self.stats['orders_read']} | "
            f"Events {self.stats['events_read']} | "
            f"Files {self.stats['files_written']} | "
            f"Errors {self.stats['errors']}"
        )


def main():
    """Точка входа для запуска как standalone сервис"""
    service = TilesService()
    service.run()


if __name__ == "__main__":
    main()

