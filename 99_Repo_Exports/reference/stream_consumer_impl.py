#!/usr/bin/env python3
"""
Универсальный потребитель Redis Streams.

Назначение:
- Подключается к Redis и читает сообщения из множества стримов (через XREADGROUP).
- Создаёт consumer groups при необходимости.
- Делегирует обработку сообщений в `stream_handlers.StreamMessageHandler`.
- Ведёт простую статистику и периодически её печатает.
"""

import redis
import json
import time
import sys
import signal
import threading
import os
from collections import defaultdict
from typing import Dict, Any, List, Optional

from stream_handlers import StreamMessageHandler
from stream_statistics import StreamStatistics
from stream_utils import StreamUtils
from core.config import (
    SCANNER_CONSUMER_GROUP,
    SCANNER_STREAMS,
    SCANNER_READ_COUNT,
    SCANNER_READ_BLOCK_MS,
    SCANNER_STATS_INTERVAL_SEC,
)


class StreamConsumer:
    """
    Универсальный StreamConsumer для чтения нескольких стримов.

    Особенности:
    - Создаёт consumer groups для стримов из списка `SCANNER_STREAMS`.
    - Читает батчами (count=SCANNER_READ_COUNT) c блокировкой (block=SCANNER_READ_BLOCK_MS).
    - Подтверждает обработку сообщений через XACK.
    """
    def __init__(self, redis_host=None, redis_port=None, consumer_group=SCANNER_CONSUMER_GROUP):
        """
        Инициализация потребителя.

        Args:
            redis_host: Хост Redis (по умолчанию из переменных окружения)
            redis_port: Порт Redis (по умолчанию из переменных окружения)
            consumer_group: Имя consumer group
        """
        # Используем переменные окружения с fallback значениями
        if redis_host is None:
            redis_host = os.environ.get('REDIS_HOST', 'localhost')
        if redis_port is None:
            redis_port = int(os.environ.get('REDIS_PORT', 6379))
            
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_client = None
        self.consumer_group = consumer_group
        # consumer name (must be stable per process)
        self.consumer_name = os.getenv("SCANNER_CONSUMER_NAME", f"consumer-{os.getpid()}")

        self.running = False
        
        # Стримы для потребления
        self.streams_to_consume = SCANNER_STREAMS
        
        # MSG done markers (ACK-fail recovery)
        self.msg_done_prefix = os.getenv("SCANNER_MSG_DONE_PREFIX", "scanner:msg_done:v1")
        self.msg_done_ttl_sec = int(os.getenv("SCANNER_MSG_DONE_TTL_SEC", "86400"))

        # pending recovery (чтобы ACK-fail реально "долечивался")
        self.recover_pending_enabled = os.getenv("SCANNER_RECOVER_PENDING", "1").lower() not in {"0","false","no"}
        # If pending msg has NO msg_done marker => we MUST reprocess it (otherwise it can hang forever).
        # Safe because we still enforce at-most-once side-effects via msg_done marker on success.
        self.recover_reprocess_pending = os.getenv("SCANNER_RECOVER_REPROCESS_PENDING", "1").lower() not in {"0","false","no"}
        self.recover_interval_sec = float(os.getenv("SCANNER_RECOVER_INTERVAL_SEC", "2.0"))
        self.recover_idle_ms = int(os.getenv("SCANNER_RECOVER_IDLE_MS", "30000"))  # "зависшие" 30s+
        self.recover_count = int(os.getenv("SCANNER_RECOVER_COUNT", "200"))

        self._last_recover_ts = 0.0

        try:
            _ = self._ctr
        except Exception:
            self._ctr = defaultdict(int)

        # Инициализируем компоненты
        self.handler = StreamMessageHandler()
        self.stats = StreamStatistics()
        self.utils = StreamUtils()
        
        # Настройка обработчика сигналов для graceful shutdown
        if threading.current_thread() is threading.main_thread():
            try:
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
            except ValueError as e:
                print(f"⚠️ Не удалось установить обработчики сигналов: {e}")
        else:
            print("ℹ️ StreamConsumer создан в дочернем потоке, пропускаем установку обработчиков сигналов")
    
    def _signal_handler(self, signum, frame):
        """Обработчик сигналов завершения (SIGINT/SIGTERM)."""
        print(f"\n🛑 Получен сигнал {signum}, завершение работы...")
        self.stop()
        sys.exit(0)

    # -----------------------------
    # MSG done markers (ACK-fail recovery)
    # -----------------------------
    def _msg_done_key(self, stream_name: str, msg_id: str) -> str:
        # include stream to avoid cross-stream collisions
        return f"{self.msg_done_prefix}:{stream_name}:{msg_id}"

    def _mark_msg_done(self, stream_name: str, msg_id: str) -> None:
        try:
            k = self._msg_done_key(stream_name, msg_id)
            self.redis_client.set(k, "1", ex=self.msg_done_ttl_sec, nx=True)
        except Exception:
            return

    def _is_msg_done(self, stream_name: str, msg_id: str) -> bool:
        try:
            k = self._msg_done_key(stream_name, msg_id)
            v = self.redis_client.get(k)
            return bool(v)
        except Exception:
            return False

    def _xack_only(self, stream_name: str, message_id: str) -> None:
        self.redis_client.xack(stream_name, self.consumer_group, message_id)

    def connect(self) -> bool:
        """
        Подключение к Redis и проверка доступности.
        """
        try:
            # URL-first: для тестов или production с URL
            redis_url = os.getenv("TEST_REDIS_URL") or os.getenv("REDIS_URL")
            if redis_url:
                self.redis_client = redis.Redis.from_url(
                    redis_url,
                    decode_responses=True,
                    socket_connect_timeout=30,
                    socket_timeout=120,
                    health_check_interval=30
                )
            else:
                # fallback: host/port
                self.redis_client = redis.Redis(
                    host=self.redis_host,
                    port=self.redis_port,
                    decode_responses=True,
                    socket_connect_timeout=30,
                    socket_timeout=120,
                    health_check_interval=30
                )
            
            # Проверяем соединение
            self.redis_client.ping()
            print(f"✅ Подключен к Redis: {self.redis_host}:{self.redis_port}")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка подключения к Redis: {e}")
            return False
    
    def start(self):
        """Запуск потребителя стримов."""
        print("🚀 Запуск потребителя Redis Streams...")
        
        # Подключаемся к Redis
        if not self.connect():
            return False
        
        # Создаем consumer groups
        if not self.utils.create_consumer_groups(self.redis_client, self.streams_to_consume, self.consumer_group):
            print("⚠️ Некоторые consumer groups не удалось создать, продолжаем...")
        
        self.running = True
        
        # Запускаем поток для периодического вывода статистики
        stats_thread = threading.Thread(target=self._periodic_stats, daemon=True)
        stats_thread.start()
        
        try:
            # Основной цикл потребления
            self.consume_streams()
        except KeyboardInterrupt:
            print("\n🛑 Получен сигнал прерывания")
        finally:
            self.stop()
        
        return True
    
    def consume_streams(self):
        """Основной цикл чтения из стримов (XREADGROUP)."""
        print("🔄 Запуск потребления Redis Streams...")
        
        while self.running:
            try:
                # periodic pending recovery (bounded, cheap)
                try:
                    now = time.time()
                    if self.recover_pending_enabled and (now - float(self._last_recover_ts)) >= float(self.recover_interval_sec):
                        self._last_recover_ts = now
                        self._recover_pending_once()
                except Exception:
                    pass

                # Читаем сообщения из всех стримов
                messages = self.redis_client.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    dict.fromkeys(self.streams_to_consume, '>'),
                    count=SCANNER_READ_COUNT,
                    block=SCANNER_READ_BLOCK_MS
                )
                
                if messages:
                    # Обрабатываем сообщения
                    self._process_messages(messages)
                
            except redis.exceptions.ConnectionError as e:
                print(f"❌ Ошибка подключения к Redis: {e}")
                self.stats.increment_errors()
                if self.running:
                    time.sleep(5)
                    
            except Exception as e:
                print(f"❌ Ошибка при чтении стримов: {e}")
                self.stats.increment_errors()
                if "NOGROUP" in str(e).upper():
                    print("⚠️ Обнаружен NOGROUP, пересоздаём consumer groups...")
                    if not self.utils.create_consumer_groups(self.redis_client, self.streams_to_consume, self.consumer_group):
                        print("❌ Не удалось пересоздать все consumer groups.")
                if self.running:
                    time.sleep(1)
    
    def _process_messages(self, messages):
        """Обрабатывает пакет сообщений из одного или нескольких стримов."""
        # Redis может возвращать список или словарь в зависимости от версии
        if isinstance(messages, list):
            # Формат: [[stream_name, [[message_id, fields], ...]], ...]
            for stream_data in messages:
                stream_name = stream_data[0]
                stream_messages = stream_data[1]
                for message_id, fields in stream_messages:
                    self._handle_single_message(stream_name, message_id, fields)
        else:
            # Формат словаря: {stream_name: [[message_id, fields], ...], ...}
            for stream_name, stream_messages in messages.items():
                for message_id, fields in stream_messages:
                    self._handle_single_message(stream_name, message_id, fields)
    
    def _handle_single_message(self, stream_name: str, message_id: str, fields: Dict[str, str]):
        """Обрабатывает одиночное сообщение: делегирует в handler, обновляет статистику, ACK-ает."""
        # ------------------------------------------------------------
        # ACK-fail hardening:
        #   if msg_done marker exists => ACK-only (never run handler again)
        # ------------------------------------------------------------
        if self._is_msg_done(stream_name, message_id):
            try:
                self.redis_client.xack(stream_name, self.consumer_group, message_id)
            except Exception:
                pass
            return

        # Process side-effects exactly once per msg_id (under ACK flaps).
        self.handler.process_stream_message(stream_name, message_id, fields)

        # Обновляем статистику
        self.stats.update_stats(stream_name, message_id)

        # Подтверждаем обработку сообщения
        try:
            # mark BEFORE ack: if ack fails transiently, recovery becomes ACK-only
            self._mark_msg_done(stream_name, message_id)
            self.redis_client.xack(stream_name, self.consumer_group, message_id)
        except Exception as ack_error:
            print(f"❌ Ошибка подтверждения сообщения {message_id}: {ack_error}")

    def _recover_pending_once(self) -> None:
        """
        Two-lane recovery:
          Lane A) msg_done=1  => ACK-only (never run handler again)
          Lane B) msg_done=0  => reprocess (claim -> handler -> mark msg_done -> ACK)
        """
        if not self.recover_pending_enabled:
            return
        try:
            streams = list(getattr(self, "streams_to_consume", []) or [])
        except Exception:
            streams = []
        if not streams:
            return

        for stream_name in streams:
            try:
                # prefer xautoclaim if available to iterate pending safely
                xautoclaim = getattr(self.redis_client, "xautoclaim", None)
                if callable(xautoclaim):
                    start_id = "0-0"
                    # loop once (bounded): keeps hot-path cheap; can be extended by cursor loop later
                    res = xautoclaim(stream_name, self.consumer_group, self.consumer_name, min_idle_time=self.recover_idle_ms, start_id=start_id, count=self.recover_count)
                    # redis-py returns (next_id, [(id, {fields})...], deleted_ids)
                    msgs = res[1] if isinstance(res, (list, tuple)) and len(res) >= 2 else []
                    for mid, _fields in (msgs or []):
                        if self._is_msg_done(stream_name, mid):
                            # Lane A: ACK-only
                            try:
                                self.redis_client.xack(stream_name, self.consumer_group, mid)
                                self._ctr["recover_ack_only_ok"] += 1
                            except Exception:
                                self._ctr["recover_ack_only_fail"] += 1
                            continue

                        # Lane B: reprocess if enabled
                        if not self.recover_reprocess_pending:
                            continue
                        try:
                            self.handler.process_stream_message(stream_name, mid, _fields)
                            try:
                                self.stats.update_stats(stream_name, mid)
                            except Exception:
                                pass
                            self._mark_msg_done(stream_name, mid)
                            try:
                                self.redis_client.xack(stream_name, self.consumer_group, mid)
                                self._ctr["recover_reprocess_ok"] += 1
                            except Exception:
                                # if XACK fails: msg_done already set => next recovery becomes ACK-only
                                self._ctr["recover_reprocess_ack_fail"] += 1
                        except Exception:
                            # do NOT mark msg_done on handler error; keep pending for retry
                            self._ctr["recover_reprocess_handler_fail"] += 1
                    continue
            except Exception:
                # fall back to XPENDING_RANGE path below
                pass

            # fallback: XPENDING_RANGE + ack if msg_done
            try:
                xpr = getattr(self.redis_client, "xpending_range", None)
                if not callable(xpr):
                    continue
                pend = xpr(stream_name, self.consumer_group, min="-", max="+", count=self.recover_count)
                # entries are objects/dicts/tuples depending on redis-py version
                for it in (pend or []):
                    mid = None
                    try:
                        mid = it.get("message_id") if isinstance(it, dict) else it["message_id"]
                    except Exception:
                        try:
                            mid = it[0]
                        except Exception:
                            mid = None
                    if not mid:
                        continue
                    if self._is_msg_done(stream_name, mid):
                        try:
                            self.redis_client.xack(stream_name, self.consumer_group, mid)
                            self._ctr["recover_ack_only_ok"] += 1
                        except Exception:
                            self._ctr["recover_ack_only_fail"] += 1
                        continue

                    # Lane B fallback: XCLAIM + reprocess
                    if not self.recover_reprocess_pending:
                        continue
                    try:
                        xclaim = getattr(self.redis_client, "xclaim", None)
                        if not callable(xclaim):
                            continue
                        claimed = xclaim(stream_name, self.consumer_group, self.consumer_name, min_idle_time=self.recover_idle_ms, message_ids=[mid])
                        # redis-py returns list[(id, {fields})]
                        for cmid, cfields in (claimed or []):
                            if self._is_msg_done(stream_name, cmid):
                                try:
                                    self.redis_client.xack(stream_name, self.consumer_group, cmid)
                                except Exception:
                                    pass
                                continue
                            try:
                                self.handler.process_stream_message(stream_name, cmid, cfields)
                                try:
                                    self.stats.update_stats(stream_name, cmid)
                                except Exception:
                                    pass
                                self._mark_msg_done(stream_name, cmid)
                                try:
                                    self.redis_client.xack(stream_name, self.consumer_group, cmid)
                                    self._ctr["recover_reprocess_ok"] += 1
                                except Exception:
                                    self._ctr["recover_reprocess_ack_fail"] += 1
                            except Exception:
                                self._ctr["recover_reprocess_handler_fail"] += 1
                    except Exception:
                        continue
            except Exception:
                continue

    def _periodic_stats(self):
        """Фоновая периодическая печать статистики раз в `SCANNER_STATS_INTERVAL_SEC` секунд."""
        while self.running:
            time.sleep(SCANNER_STATS_INTERVAL_SEC)
            if self.running and self.stats.get_total_messages() > 0:
                self.stats.print_stats()
    
    def stop(self):
        """Остановка потребителя, печать финальной статистики и закрытие соединения с Redis."""
        if self.running:
            print("🛑 Остановка потребителя стримов...")
            self.running = False
            
            # Выводим финальную статистику
            self.stats.print_stats()
            
            # Закрываем соединение с Redis
            if self.redis_client:
                try:
                    self.redis_client.close()
                    print("✅ Соединение с Redis закрыто")
                except Exception as e:
                    print(f"⚠️ Ошибка закрытия соединения с Redis: {e}")


def main():
    """Основная функция для запуска потребителя стримов"""
    print("=" * 60)
    print("🚀 REDIS STREAMS CONSUMER")
    print("=" * 60)
    
    # Создаем и запускаем потребителя
    consumer = StreamConsumer()
    
    try:
        success = consumer.start()
        if not success:
            print("❌ Не удалось запустить потребителя стримов")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        sys.exit(1)
    finally:
        print("\n👋 Завершение работы потребителя стримов")


if __name__ == "__main__":
    main() 