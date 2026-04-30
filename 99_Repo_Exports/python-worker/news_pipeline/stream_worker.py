from __future__ import annotations

import os
import time
import logging
import signal
from typing import Dict, Any, Iterable, Tuple, Optional

import redis

log = logging.getLogger(__name__)


class StreamWorker:
    """
    Надёжный шаблон:
    - XGROUP CREATE MKSTREAM
    - XREADGROUP BLOCK
    - XAUTOCLAIM для зависших pending
    - fail-open DLQ
    """

    def __init__(
        self
        *
        redis: redis.Redis
        stream: str
        group: str
        consumer: str
        dlq_stream: str
        block_ms: int = 2000
        count: int = 50
        claim_idle_ms: int = 60_000
    ):
        self.r = redis
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.dlq_stream = dlq_stream
        self.block_ms = int(block_ms)
        self.count = int(count)
        self.claim_idle_ms = int(claim_idle_ms)
        self._shutdown = False

        self._ensure_group()
        self._setup_signal_handlers()

    def _ensure_group(self) -> None:
        import redis as _redis
        import time

        max_retries = 30  # 5 минут при 10сек задержке
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.r.xgroup_create(self.stream, self.group, id="0-0", mkstream=True)
                return
            except _redis.BusyLoadingError:
                retry_count += 1
                log.warning(f"Redis is loading dataset, retrying consumer group creation ({retry_count}/{max_retries})...")
                time.sleep(10)
            except (_redis.exceptions.ConnectionError, _redis.exceptions.TimeoutError, ConnectionResetError) as e:
                retry_count += 1
                delay = min(2 ** retry_count, 30)
                log.warning(f"Redis connection error during group creation ({retry_count}/{max_retries}): {e}. Retrying in {delay}s...")
                time.sleep(delay)
            except Exception as e:
                # BUSYGROUP is ok
                if "BUSYGROUP" not in str(e):
                    raise
                return

        raise Exception(f"Failed to create consumer group after {max_retries} retries due to Redis loading/connection errors")

    def _setup_signal_handlers(self) -> None:
        """Устанавливает обработчики сигналов для graceful shutdown."""
        def signal_handler(signum, frame):
            log.info(f"🔻 Получен сигнал {signum}, инициируем graceful shutdown...")
            self._shutdown = True
            import sys
            sys.stdout.flush()
            sys.stderr.flush()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def _dlq(self, msg_id: str, fields: Dict[str, Any], err: str) -> None:
        try:
            payload = dict(fields)
            payload["__src_stream"] = self.stream
            payload["__src_id"] = msg_id
            payload["__err"] = (err or "")[:500]
            self.r.xadd(self.dlq_stream, payload, maxlen=200000, approximate=True)
        except Exception:
            pass

    def handle_message(self, msg_id: str, fields: Dict[str, Any]) -> None:
        """
        Переопределить в наследнике.
        Должен бросать исключение на реальных ошибках.
        """
        raise NotImplementedError

    def on_idle(self) -> None:
        """Called after an iteration that yielded no messages. Override to write heartbeats, update metrics, etc."""
        pass

    def _iter_batch(self) -> Iterable[Tuple[str, Dict[str, Any]]]:
        # 1) auto-claim старые pending (failover)
        try:
            res = self.r.xautoclaim(self.stream, self.group, self.consumer, min_idle_time=self.claim_idle_ms, start_id="0-0", count=self.count)
            # res: (next_start_id, [(id, {fields})...], deleted_ids)
            claimed = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else []
            for msg_id, fields in claimed or []:
                yield msg_id, fields
        except Exception:
            pass

        # 2) read new
        data = self.r.xreadgroup(self.group, self.consumer, streams={self.stream: ">"}, count=self.count, block=self.block_ms)
        for _stream, items in data or []:
            for msg_id, fields in items:
                yield msg_id, fields

    def run_forever(self) -> None:
        log.info(f"🚀 StreamWorker запущен (stream={self.stream}, group={self.group}, consumer={self.consumer})")
        while not self._shutdown:
            try:
                any_msg = False
                for msg_id, fields in self._iter_batch():
                    if self._shutdown:
                        break
                    any_msg = True
                    try:
                        self.handle_message(msg_id, fields)
                        self.r.xack(self.stream, self.group, msg_id)
                    except Exception as e:
                        self._dlq(msg_id, fields, str(e))
                        try:
                            self.r.xack(self.stream, self.group, msg_id)
                        except Exception:
                            pass

                if not any_msg and not self._shutdown:
                    self.on_idle()
                    time.sleep(0.05)
            except redis.exceptions.ResponseError as e:
                # Catch NOGROUP error and re-create group
                if "NOGROUP" in str(e):
                    log.warning(f"⚠️ Consumer group missing (NOGROUP), trying to recreate: {e}")
                    try:
                        self._ensure_group()
                    except Exception as ensure_err:
                        log.error(f"❌ Failed to recreate group: {ensure_err}")
                        time.sleep(5)
                else:
                    # Re-raise other ResponseErrors
                    if not self._shutdown:
                         log.exception(f"❌ Redis ResponseError in run_forever loop: {e}")
                         time.sleep(1)
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
                if not self._shutdown:
                    log.error("❌ Redis connection lost. Waiting 5s before retry...")
                    # Smart sleep to allow faster shutdown
                    for _ in range(50):
                        if self._shutdown:
                            break
                        time.sleep(0.1)
            except KeyboardInterrupt:
                log.info("🔻 Получен KeyboardInterrupt, инициируем shutdown...")
                self._shutdown = True
                break
            except Exception as e:
                if not self._shutdown:
                    log.exception(f"❌ Unexpected error in run_forever loop: {e}")
                    time.sleep(1)
        
        log.info("✅ StreamWorker завершён корректно")
        import sys
        sys.stdout.flush()
        sys.stderr.flush()
