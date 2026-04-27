#!/usr/bin/env python3
"""
Универсальный потребитель Redis Streams.

Назначение:
- Подключается к Redis и читает сообщения из множества стримов (через XREADGROUP).
- Создаёт consumer groups при необходимости.
- Делегирует обработку сообщений в `stream_handlers.StreamMessageHandler`.
- Ведёт простую статистику и периодически её печатает.
"""

import os
import signal
import sys
import threading
import time
from collections import defaultdict
from typing import Dict, List, Optional

import redis

from stream_handlers import StreamMessageHandler
from stream_statistics import StreamStatistics
from prometheus_client import Counter

XACK_FAIL_REASON_TOTAL = Counter(
    "xack_fail_reason_total",
    "Total xack failures",
    ["stream", "reason"]
)

CONSUMER_GROUP_RECOVERY_ATTEMPTS = Counter(
    "consumer_group_recovery_attempts",
    "Total NOGROUP consumer group recovery attempts",
    ["stream"]
)

from core.config import (
    SCANNER_CONSUMER_GROUP,
    SCANNER_STREAMS,
    SCANNER_READ_COUNT,
    SCANNER_READ_BLOCK_MS,
    SCANNER_STATS_INTERVAL_SEC,
)
from core.redis_keys import RedisStreams as RS, STREAM_RETENTION


class StreamConsumer:
    """Универсальный StreamConsumer для чтения нескольких стримов.

    Особенности:
    - Создаёт consumer groups для стримов из списка `SCANNER_STREAMS`.
    - Читает батчами (count=SCANNER_READ_COUNT) c блокировкой (block=SCANNER_READ_BLOCK_MS).
    - Подтверждает обработку сообщений через XACK.
    - Поддерживает at-least-once/at-most-once hardening через msg_done марки.
    """

    def __init__(
        self,
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = None,
        consumer_group: str = SCANNER_CONSUMER_GROUP,
    ) -> None:
        """Инициализация потребителя.

        Args:
            redis_host: Хост Redis (по умолчанию из переменных окружения).
            redis_port: Порт Redis (по умолчанию из переменных окружения).
            consumer_group: Имя consumer group.
        """
        self.redis_host: str = redis_host or os.environ.get("REDIS_HOST", "localhost")
        self.redis_port: int = redis_port or int(os.environ.get("REDIS_PORT", 6379))
        self.redis_client: Optional[redis.Redis] = None
        self.consumer_group: str = consumer_group
        # Consumer name must be stable across container restarts to prevent PEL accumulation.
        # socket.gethostname() returns the Docker container ID, which is stable across soft restarts.
        import socket
        self.consumer_name: str = os.getenv(
            "SCANNER_CONSUMER_NAME", f"consumer-{socket.gethostname()}"
        )

        self.running: bool = False
        self.streams_to_consume: List[str] = SCANNER_STREAMS

        # MSG-done markers (ACK-fail recovery)
        self.msg_done_prefix: str = os.getenv("SCANNER_MSG_DONE_PREFIX", "scanner:msg_done:v1")
        self.msg_done_ttl_sec: int = int(os.getenv("SCANNER_MSG_DONE_TTL_SEC", "86400"))

        # Pending recovery settings
        self.recover_pending_enabled: bool = (
            os.getenv("SCANNER_RECOVER_PENDING", "1").lower() not in {"0", "false", "no"}
        )
        self.recover_reprocess_pending: bool = (
            os.getenv("SCANNER_RECOVER_REPROCESS_PENDING", "1").lower()
            not in {"0", "false", "no"}
        )
        self.recover_interval_sec: float = float(os.getenv("SCANNER_RECOVER_INTERVAL_SEC", "2.0"))
        self.recover_idle_ms: int = int(os.getenv("SCANNER_RECOVER_IDLE_MS", "30000"))
        self.recover_count: int = int(os.getenv("SCANNER_RECOVER_COUNT", "200"))

        self._last_recover_ts: float = 0.0
        self._ctr: Dict[str, int] = defaultdict(int)

        # Sub-components
        self.handler = StreamMessageHandler()
        self.stats = StreamStatistics()
        self.utils = StreamUtils()

        # Signal handlers only on the main thread.
        if threading.current_thread() is threading.main_thread():
            try:
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
            except ValueError as exc:
                print(f"⚠️ Не удалось установить обработчики сигналов: {exc}")
        else:
            print("ℹ️ StreamConsumer создан в дочернем потоке, пропускаем установку обработчиков сигналов")

    def _signal_handler(self, signum: int, frame: object) -> None:  # noqa: ARG002
        """Обработчик сигналов завершения (SIGINT/SIGTERM)."""
        print(f"\n🛑 Получен сигнал {signum}, завершение работы...")
        self.stop()
        sys.exit(0)

    # ------------------------------------------------------------------
    # MSG-done marker helpers
    # ------------------------------------------------------------------

    def _msg_done_key(self, stream_name: str, msg_id: str) -> str:
        """Ключ Redis для маркера обработки сообщения."""
        return f"{self.msg_done_prefix}:{stream_name}:{msg_id}"

    def _mark_msg_done(self, stream_name: str, msg_id: str) -> None:
        """Устанавливает маркер 'сообщение обработано' (best-effort, не бросает)."""
        try:
            self.redis_client.set(
                self._msg_done_key(stream_name, msg_id),
                "1",
                ex=self.msg_done_ttl_sec,
                nx=True,
            )
        except Exception:  # Redis unavailable — best-effort, never raise
            pass

    def _is_msg_done(self, stream_name: str, msg_id: str) -> bool:
        """Проверяет, обработано ли сообщение (best-effort, не бросает)."""
        try:
            return bool(self.redis_client.get(self._msg_done_key(stream_name, msg_id)))
        except Exception:  # Redis unavailable — assume not done to avoid message drop
            return False

    def _xack_only(self, stream_name: str, message_id: str) -> None:
        """Подтверждает сообщение без повторной обработки."""
        self.redis_client.xack(stream_name, self.consumer_group, message_id)

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _build_redis_client(self) -> redis.Redis:
        """Создаёт и возвращает Redis-клиент (URL-first, fallback host:port)."""
        common_kwargs = dict(
            decode_responses=True,
            socket_connect_timeout=30,
            socket_timeout=120,
            health_check_interval=30,
        )
        redis_url = os.getenv("TEST_REDIS_URL") or os.getenv("REDIS_URL")
        if redis_url:
            return redis.Redis.from_url(redis_url, **common_kwargs)
        return redis.Redis(host=self.redis_host, port=self.redis_port, **common_kwargs)

    def connect(self) -> bool:
        """Подключение к Redis и проверка доступности."""
        try:
            self.redis_client = self._build_redis_client()
            self.redis_client.ping()
            print(f"✅ Подключен к Redis: {self.redis_host}:{self.redis_port}")
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"❌ Ошибка подключения к Redis: {exc}")
            return False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> bool:
        """Запуск потребителя стримов."""
        print("🚀 Запуск потребителя Redis Streams...")
        if not self.connect():
            return False

        if not self.utils.create_consumer_groups(
            self.redis_client, self.streams_to_consume, self.consumer_group
        ):
            print("⚠️ Некоторые consumer groups не удалось создать, продолжаем...")

        self.running = True
        stats_thread = threading.Thread(target=self._periodic_stats, daemon=True)
        stats_thread.start()

        try:
            self.consume_streams()
        except KeyboardInterrupt:
            print("\n🛑 Получен сигнал прерывания")
        finally:
            self.stop()

        return True

    def consume_streams(self) -> None:
        """Основной цикл чтения из стримов (XREADGROUP)."""
        print("🔄 Запуск потребления Redis Streams...")

        while self.running:
            try:
                # Periodic pending recovery (bounded, cheap).
                now = time.time()
                if (
                    self.recover_pending_enabled
                    and (now - self._last_recover_ts) >= self.recover_interval_sec
                ):
                    self._last_recover_ts = now
                    try:
                        self._recover_pending_once()
                    except Exception:  # never let recovery kill the main loop
                        pass

                messages = self.redis_client.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    dict.fromkeys(self.streams_to_consume, ">"),
                    count=SCANNER_READ_COUNT,
                    block=SCANNER_READ_BLOCK_MS,
                )
                if messages:
                    self._process_messages(messages)

            except redis.exceptions.ConnectionError as exc:
                print(f"❌ Ошибка подключения к Redis: {exc}")
                self.stats.increment_errors()
                if self.running:
                    time.sleep(5)
            except Exception as exc:  # noqa: BLE001
                print(f"❌ Ошибка при чтении стримов: {exc}")
                self.stats.increment_errors()
                if "NOGROUP" in str(exc).upper():
                    print("⚠️ Обнаружен NOGROUP, пересоздаём consumer groups...")
                    try:
                        for s in self.streams_to_consume:
                            CONSUMER_GROUP_RECOVERY_ATTEMPTS.labels(stream=s).inc()
                    except Exception:
                        pass
                    if not self.utils.create_consumer_groups(
                        self.redis_client, self.streams_to_consume, self.consumer_group
                    ):
                        print("❌ Не удалось пересоздать все consumer groups.")
                if self.running:
                    time.sleep(1)

    def _process_messages(self, messages: object) -> None:
        """Обрабатывает пакет сообщений из одного или нескольких стримов."""
        # Redis may return a list or a dict depending on version.
        if isinstance(messages, list):
            for stream_data in messages:
                stream_name, stream_messages = stream_data[0], stream_data[1]
                for message_id, fields in stream_messages:
                    self._handle_single_message(stream_name, message_id, fields)
        else:
            for stream_name, stream_messages in messages.items():
                for message_id, fields in stream_messages:
                    self._handle_single_message(stream_name, message_id, fields)

    def _handle_single_message(
        self,
        stream_name: str,
        message_id: str,
        fields: Dict[str, str],
    ) -> None:
        """Обрабатывает одиночное сообщение: делегирует handler, обновляет статистику, ACK."""
        # ACK-fail hardening: if msg_done marker exists → ACK-only, never re-run handler.
        if self._is_msg_done(stream_name, message_id):
            try:
                self.redis_client.xack(stream_name, self.consumer_group, message_id)
            except Exception as e:  # noqa: BLE001 — best-effort ACK
                try:
                    XACK_FAIL_REASON_TOTAL.labels(stream=stream_name, reason=type(e).__name__).inc()
                except Exception:
                    pass
            return

        self.handler.process_stream_message(stream_name, message_id, fields)
        self.stats.update_stats(stream_name, message_id)

        try:
            # Mark BEFORE ack: if ack fails transiently, recovery becomes ACK-only.
            self._mark_msg_done(stream_name, message_id)
            self.redis_client.xack(stream_name, self.consumer_group, message_id)
        except Exception as ack_error:  # noqa: BLE001
            try:
                XACK_FAIL_REASON_TOTAL.labels(stream=stream_name, reason=type(ack_error).__name__).inc()
            except Exception:
                pass
            
            # P1-4: Critical DLQ routing for XACK failures
            err_msg = f"❌ Ошибка подтверждения сообщения {message_id}: {ack_error}"
            print(f"CRITICAL: {err_msg}")
            
            self._push_ack_dlq(stream_name, [message_id], ack_error)
            
            # Raise to stop batch processing in consume_streams loop
            raise RuntimeError(f"XACK_FAILED:{stream_name}:{message_id}") from ack_error

    # ------------------------------------------------------------------
    # Pending recovery
    # ------------------------------------------------------------------

    def _recover_pending_once(self) -> None:
        """Two-lane recovery for all streams in `streams_to_consume`.

        Lane A) msg_done=1  → ACK-only (never run handler again).
        Lane B) msg_done=0  → reprocess (claim → handler → mark_done → ACK).
        """
        if not self.recover_pending_enabled:
            return
        streams: List[str] = list(self.streams_to_consume or [])
        for stream_name in streams:
            self._recover_stream_once(stream_name)

    def _recover_stream_once(self, stream_name: str) -> None:
        """Выполняет recovery для одного стрима.

        Пытается использовать XAUTOCLAIM (Redis ≥ 6.2), при недоступности
        откатывается к XPENDING_RANGE + XCLAIM.
        """
        try:
            if self._try_xautoclaim(stream_name):
                return
        except Exception:  # noqa: BLE001
            pass  # fall through to XPENDING_RANGE path

        self._try_xpending_fallback(stream_name)

    def _try_xautoclaim(self, stream_name: str) -> bool:
        """XAUTOCLAIM-based recovery (Redis ≥ 6.2). Returns True if executed."""
        xautoclaim = getattr(self.redis_client, "xautoclaim", None)
        if not callable(xautoclaim):
            return False

        res = xautoclaim(
            stream_name,
            self.consumer_group,
            self.consumer_name,
            min_idle_time=self.recover_idle_ms,
            start_id="0-0",
            count=self.recover_count,
        )
        # redis-py returns (next_id, [(id, {fields}), ...], deleted_ids)
        msgs = res[1] if isinstance(res, (list, tuple)) and len(res) >= 2 else []
        for mid, fields in (msgs or []):
            self._recover_one_message(stream_name, mid, fields)
        return True

    def _try_xpending_fallback(self, stream_name: str) -> None:
        """Fallback: XPENDING_RANGE + XCLAIM (Redis < 6.2)."""
        try:
            xpr = getattr(self.redis_client, "xpending_range", None)
            if not callable(xpr):
                return
            pending = xpr(
                stream_name, self.consumer_group, min="-", max="+", count=self.recover_count
            )
            for it in pending or []:
                mid = _extract_pending_id(it)
                if not mid:
                    continue
                if self._is_msg_done(stream_name, mid):
                    self._safe_xack(stream_name, mid, "recover_ack_only")
                    continue
                if not self.recover_reprocess_pending:
                    continue
                self._xclaim_and_reprocess(stream_name, mid)
        except Exception:  # noqa: BLE001
            pass

    def _xclaim_and_reprocess(self, stream_name: str, mid: str) -> None:
        """Claim + reprocess a single pending message (from XPENDING_RANGE path)."""
        try:
            xclaim = getattr(self.redis_client, "xclaim", None)
            if not callable(xclaim):
                return
            claimed = xclaim(
                stream_name,
                self.consumer_group,
                self.consumer_name,
                min_idle_time=self.recover_idle_ms,
                message_ids=[mid],
            )
            for cmid, cfields in (claimed or []):
                self._recover_one_message(stream_name, cmid, cfields)
        except Exception:  # noqa: BLE001
            pass

    def _recover_one_message(
        self,
        stream_name: str,
        mid: str,
        fields: Dict[str, str],
    ) -> None:
        """Applies the two-lane recovery policy to a single pending message."""
        if self._is_msg_done(stream_name, mid):
            # Lane A: ACK-only — handler already ran.
            self._safe_xack(stream_name, mid, "recover_ack_only")
            return

        if not self.recover_reprocess_pending:
            return

        # Lane B: reprocess.
        try:
            self.handler.process_stream_message(stream_name, mid, fields)
            try:
                self.stats.update_stats(stream_name, mid)
            except Exception:  # noqa: BLE001
                pass
            self._mark_msg_done(stream_name, mid)
            self._safe_xack(stream_name, mid, "recover_reprocess")
        except Exception:  # noqa: BLE001 — keep pending for retry; do NOT mark done
            self._ctr["recover_reprocess_handler_fail"] += 1

    def _safe_xack(self, stream_name: str, message_id: str, ctr_prefix: str) -> None:
        """Подтверждает сообщение и обновляет счётчики.
        
        P1-4: При сбое записывает в DLQ.
        """
        try:
            self.redis_client.xack(stream_name, self.consumer_group, message_id)
            self._ctr[f"{ctr_prefix}_ok"] += 1
        except Exception as e:  # noqa: BLE001
            self._ctr[f"{ctr_prefix}_fail"] += 1
            try:
                XACK_FAIL_REASON_TOTAL.labels(stream=stream_name, reason=type(e).__name__).inc()
            except Exception:
                pass
            print(f"CRITICAL: ❌ XACK failure in recovery ({ctr_prefix}): {e}")
            self._push_ack_dlq(stream_name, [message_id], e)

    def _push_ack_dlq(self, stream_name: str, ids: List[str], error: Exception) -> None:
        """Записывает сбой XACK в аховый DLQ стрим."""
        try:
            dlq_stream = RS.SIGNAL_ACK_DLQ
            payload = {
                "consumer_group": str(self.consumer_group),
                "consumer_name": str(self.consumer_name),
                "stream": str(stream_name),
                "ids": ",".join(ids),
                "error": str(error),
                "ts_ms": str(int(time.time() * 1000)),
            }
            # Best-effort write to DLQ.
            # maxlen is sourced from the canonical STREAM_RETENTION map so that
            # producer and janitor always stay in sync.
            self.redis_client.xadd(dlq_stream, payload, maxlen=STREAM_RETENTION[RS.SIGNAL_ACK_DLQ], approximate=True)
        except Exception as ex:
            print(f"☣️ Критическая ошибка при записи в ACK DLQ: {ex}")

    # ------------------------------------------------------------------
    # Background stats
    # ------------------------------------------------------------------

    def _periodic_stats(self) -> None:
        """Фоновая периодическая печать статистики."""
        while self.running:
            time.sleep(SCANNER_STATS_INTERVAL_SEC)
            if self.running and self.stats.get_total_messages() > 0:
                self.stats.print_stats()

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def stop(self) -> None:
        """Остановка потребителя, финальная статистика и закрытие Redis."""
        if not self.running:
            return
        print("🛑 Остановка потребителя стримов...")
        self.running = False
        self.stats.print_stats()
        if self.redis_client is not None:
            try:
                self.redis_client.close()
                print("✅ Соединение с Redis закрыто")
            except Exception as exc:  # noqa: BLE001
                print(f"⚠️ Ошибка закрытия соединения с Redis: {exc}")


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _extract_pending_id(it: object) -> Optional[str]:
    """Извлекает message_id из pending-записи, совместимо с разными версиями redis-py."""
    try:
        if isinstance(it, dict):
            return it.get("message_id")
        return it["message_id"]  # type: ignore[index]
    except Exception:  # noqa: BLE001
        try:
            return it[0]  # type: ignore[index]
        except Exception:  # noqa: BLE001
            return None


# ------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------

def main() -> None:
    """Основная функция для запуска потребителя стримов."""
    print("=" * 60)
    print("🚀 REDIS STREAMS CONSUMER")
    print("=" * 60)

    consumer = StreamConsumer()
    try:
        if not consumer.start():
            print("❌ Не удалось запустить потребителя стримов")
            sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        print(f"❌ Критическая ошибка: {exc}")
        sys.exit(1)
    finally:
        print("\n👋 Завершение работы потребителя стримов")


if __name__ == "__main__":
    main()