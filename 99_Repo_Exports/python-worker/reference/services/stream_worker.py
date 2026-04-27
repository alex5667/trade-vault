"""
StreamWorker - единый каркас для обработки Redis Streams с поддержкой retry/pending-claim.

Функционал:
- Lossless и realtime режимы ACK
- Автоматический drain pending сообщений
- Claim orphan pending сообщений
- Retry механизм с DLQ
- Единый pipeline для всех потоков

Использование:
    from services.stream_worker import StreamWorker, WorkerPolicy
    
    policy = WorkerPolicy(
        ack_mode="lossless",
        read_count=50,
        block_ms=2000,
        drain_pending_every_s=10,
        claim_orphan_every_s=60,
        min_idle_ms=60_000,
        max_attempts=5,
    )
    
    worker = StreamWorker(
        name="my_worker",
        client=redis_client,
        group="my_group",
        consumer="my_consumer",
        build_streams=lambda: ["stream:1", "stream:2"],
        process=my_processor,
        policy=policy,
        logger=logger,
    )
    
    worker.run_loop(lambda: running_flag)
"""

from __future__ import annotations

import time
import json
from dataclasses import dataclass
from typing import Callable, Dict, Any, List, Optional, Tuple

import redis
from redis.exceptions import RedisError


@dataclass
class WorkerPolicy:
    """Политика обработки сообщений для StreamWorker."""
    ack_mode: str = "lossless"  # "lossless" | "realtime"
    read_count: int = 50
    block_ms: int = 2000

    # lossless only
    drain_pending_every_s: int = 10
    claim_orphan_every_s: int = 60
    min_idle_ms: int = 60_000
    max_attempts: int = 5
    retry_state_ttl_s: int = 7 * 24 * 3600

    dlq_stream: str = "dlq:stream_worker"


ProcessFn = Callable[[str, str, Dict[str, Any]], bool]  # (stream, msg_id, data) -> ok?


class StreamWorker:
    """
    Единый воркер для обработки Redis Streams с поддержкой retry/pending-claim.
    
    Особенности:
    - Lossless режим: сообщения не теряются при падениях
    - Realtime режим: быстрая обработка, ACK всегда
    - Автоматический drain pending сообщений
    - Claim orphan pending сообщений
    - Retry механизм с DLQ
    """
    
    def __init__(
        self,
        *,
        name: str,
        client: redis.Redis,
        group: str,
        consumer: str,
        build_streams: Callable[[], List[str]],
        process: ProcessFn,
        policy: WorkerPolicy,
        logger,
        health_cb: Optional[Callable[[str, str, Dict[str, Any]], None]] = None,
    ):
        self.name = name
        self.client = client
        self.group = group
        self.consumer = consumer
        self.build_streams = build_streams
        self.process = process
        self.policy = policy
        self.log = logger
        self.health_cb = health_cb

        self._streams: List[str] = []
        self._last_streams_refresh = 0.0
        self._last_pending_drain = 0.0
        self._last_orphan_claim = 0.0

        self._retry_hash = f"retry:state:{group}:{name}"  # msg retry attempts + timestamps

        # for XAUTOCLAIM cursor per stream
        self._claim_cursor: Dict[str, str] = {}

    def _streams_dict(self, stream_id: str) -> Dict[str, str]:
        """Формирует словарь streams для xreadgroup."""
        return {s: stream_id for s in self._streams}

    def _refresh_streams(self) -> None:
        """Обновляет список streams и инициализирует cursors для claim."""
        self._streams = self.build_streams()
        for s in self._streams:
            self._claim_cursor.setdefault(s, "0-0")

    def _bump_retry(self, stream: str, msg_id: str) -> int:
        """Увеличивает счетчик попыток обработки сообщения."""
        key = f"{stream}:{msg_id}"
        attempts = int(self.client.hincrby(self._retry_hash, key, 1))
        self.client.expire(self._retry_hash, self.policy.retry_state_ttl_s)
        return attempts

    def _clear_retry(self, stream: str, msg_id: str) -> None:
        """Очищает счетчик попыток для успешно обработанного сообщения."""
        key = f"{stream}:{msg_id}"
        try:
            self.client.hdel(self._retry_hash, key)
        except Exception:
            pass

    def _to_dlq(self, stream: str, msg_id: str, data: Dict[str, Any], err: str, attempts: int) -> None:
        """Отправляет сообщение в Dead Letter Queue."""
        payload = {
            "ts": int(time.time()),
            "worker": self.name,
            "group": self.group,
            "consumer": self.consumer,
            "stream": stream,
            "msg_id": msg_id,
            "attempts": attempts,
            "error": err[:5000],
            "data": json.dumps(data, ensure_ascii=False)[:50_000],
        }
        try:
            self.client.xadd(self.policy.dlq_stream, payload, maxlen=200_000, approximate=True)
        except Exception as e:
            self.log.warning("DLQ write failed: %s", e)

    def _ack(self, stream: str, msg_id: str) -> None:
        """Подтверждает обработку сообщения."""
        self.client.xack(stream, self.group, msg_id)

    def _handle_one(self, stream: str, msg_id: str, data: Dict[str, Any]) -> None:
        """Обрабатывает одно сообщение с учетом политики retry."""
        try:
            ok = self.process(stream, msg_id, data)
            if ok:
                self._clear_retry(stream, msg_id)
                self._ack(stream, msg_id)
                return

            # process returned False => considered failure
            if self.policy.ack_mode == "realtime":
                self._ack(stream, msg_id)
                return

            # lossless: do not ACK => stays pending
            attempts = self._bump_retry(stream, msg_id)
            if attempts >= self.policy.max_attempts:
                self._to_dlq(stream, msg_id, data, err="max_attempts", attempts=attempts)
                self._clear_retry(stream, msg_id)
                self._ack(stream, msg_id)

        except Exception as e:
            if self.policy.ack_mode == "realtime":
                try:
                    self._ack(stream, msg_id)
                except Exception:
                    pass
                self.log.error("[%s] error but acked (realtime): %s", self.name, e)
                return

            # lossless: no ACK, retry later via pending drain / claim
            attempts = self._bump_retry(stream, msg_id)
            self.log.error("[%s] processing error (attempt=%d): %s", self.name, attempts, e)
            if attempts >= self.policy.max_attempts:
                self._to_dlq(stream, msg_id, data, err=str(e), attempts=attempts)
                self._clear_retry(stream, msg_id)
                try:
                    self._ack(stream, msg_id)
                except Exception:
                    pass

    def _read_new(self) -> List[Tuple[str, List[Tuple[str, Dict[str, Any]]]]]:
        """Читает новые сообщения из streams."""
        if not self._streams:
            return []
        try:
            return self.client.xreadgroup(
                self.group,
                self.consumer,
                self._streams_dict(">"),
                count=self.policy.read_count,
                block=self.policy.block_ms,
            )
        except RedisError as e:
            # ✅ Auto-create consumer group if NOGROUP error
            if "NOGROUP" in str(e):
                self.log.warning("[%s] ⚠️ Consumer group missing, auto-creating...", self.name)
                for stream in self._streams:
                    try:
                        self.client.xgroup_create(stream, self.group, id='$', mkstream=True)
                        self.log.info("[%s] ✅ Auto-created consumer group for %s", self.name, stream)
                    except RedisError as create_err:
                        if "BUSYGROUP" not in str(create_err):
                            self.log.warning("[%s] ⚠️ Failed to create consumer group for %s: %s", self.name, stream, create_err)
                # Retry read after creating groups
                try:
                    return self.client.xreadgroup(
                        self.group,
                        self.consumer,
                        self._streams_dict(">"),
                        count=self.policy.read_count,
                        block=self.policy.block_ms,
                    )
                except RedisError as retry_err:
                    self.log.error("[%s] ❌ Retry read failed after group creation: %s", self.name, retry_err)
                    raise
            raise

    def _drain_own_pending_once(self) -> int:
        """Обрабатывает pending сообщения текущего consumer."""
        if not self._streams:
            return 0
        drained = 0
        # ID='0' => deliver pending messages to this consumer
        try:
            messages = self.client.xreadgroup(
                self.group,
                self.consumer,
                self._streams_dict("0"),
                count=min(200, self.policy.read_count),
                block=1,
            )
        except RedisError as e:
            # ✅ Auto-create consumer group if NOGROUP error
            if "NOGROUP" in str(e):
                self.log.warning("[%s] ⚠️ Consumer group missing in drain, auto-creating...", self.name)
                for stream in self._streams:
                    try:
                        self.client.xgroup_create(stream, self.group, id='0', mkstream=True)
                        self.log.info("[%s] ✅ Auto-created consumer group for %s (pending drain)", self.name, stream)
                    except RedisError as create_err:
                        if "BUSYGROUP" not in str(create_err):
                            self.log.warning("[%s] ⚠️ Failed to create consumer group for %s: %s", self.name, stream, create_err)
                # Retry after creating groups
                try:
                    messages = self.client.xreadgroup(
                        self.group,
                        self.consumer,
                        self._streams_dict("0"),
                        count=min(200, self.policy.read_count),
                        block=1,
                    )
                except RedisError:
                    return 0  # No pending messages or error
            else:
                return 0  # Other error, skip this drain cycle
        
        for stream, msgs in messages or []:
            for msg_id, data in msgs:
                drained += 1
                self._handle_one(stream, msg_id, data)
        return drained

    def _claim_orphan_pending(self) -> int:
        """Забирает orphan pending сообщения других consumer'ов."""
        if not self._streams:
            return 0
        claimed_total = 0

        for stream in self._streams:
            start_id = self._claim_cursor.get(stream, "0-0")
            try:
                # redis-py: xautoclaim(name, groupname, consumername, min_idle_time, start_id, count=None)
                next_id, msgs, _deleted = self.client.xautoclaim(
                    stream, self.group, self.consumer, self.policy.min_idle_ms, start_id, count=100
                )
                self._claim_cursor[stream] = next_id or "0-0"
                for msg_id, data in msgs or []:
                    claimed_total += 1
                    self._handle_one(stream, msg_id, data)
            except Exception as e:
                # ✅ Auto-create consumer group if NOGROUP error
                if "NOGROUP" in str(e):
                    try:
                        self.client.xgroup_create(stream, self.group, id='0', mkstream=True)
                        self.log.info("[%s] ✅ Auto-created consumer group for %s", self.name, stream)
                        # Retry claim after creating group
                        try:
                            next_id, msgs, _deleted = self.client.xautoclaim(
                                stream, self.group, self.consumer, self.policy.min_idle_ms, start_id, count=100
                            )
                            self._claim_cursor[stream] = next_id or "0-0"
                            for msg_id, data in msgs or []:
                                claimed_total += 1
                                self._handle_one(stream, msg_id, data)
                        except Exception as retry_err:
                            self.log.debug("[%s] xautoclaim retry failed on %s: %s", self.name, stream, retry_err)
                    except Exception as create_err:
                        if "BUSYGROUP" not in str(create_err):
                            self.log.warning("[%s] ⚠️ Failed to create consumer group for %s: %s", self.name, stream, create_err)
                else:
                    # Other errors - just log
                    self.log.debug("[%s] xautoclaim failed on %s: %s", self.name, stream, e)

        return claimed_total

    def run_loop(self, running_flag: Callable[[], bool]) -> None:
        """Основной цикл обработки сообщений."""
        self.log.info("Worker %s started (group=%s consumer=%s ack_mode=%s)", 
                     self.name, self.group, self.consumer, self.policy.ack_mode)

        self._refresh_streams()

        while running_flag():
            now = time.time()

            # refresh stream list if producer added symbols/aliases
            if now - self._last_streams_refresh > 5:
                self._refresh_streams()
                self._last_streams_refresh = now

            try:
                messages = self._read_new()
                if messages:
                    batch = 0
                    for stream, msgs in messages:
                        for msg_id, data in msgs:
                            batch += 1
                            self._handle_one(stream, msg_id, data)

                    if self.health_cb:
                        self.health_cb(self.name, "ok", {"batch": batch, "streams": len(self._streams)})

                else:
                    if self.health_cb:
                        self.health_cb(self.name, "ok", {"batch": 0, "streams": len(self._streams)})

            except RedisError as e:
                # ✅ NOGROUP ошибки уже обработаны в _read_new(), но на всякий случай проверяем здесь
                if "NOGROUP" in str(e):
                    self.log.warning("[%s] ⚠️ Consumer group missing (should be auto-created): %s", self.name, e)
                    # Попробуем создать группы для всех streams
                    for stream in self._streams:
                        try:
                            self.client.xgroup_create(stream, self.group, id='$', mkstream=True)
                            self.log.info("[%s] ✅ Created consumer group for %s", self.name, stream)
                        except RedisError as create_err:
                            if "BUSYGROUP" not in str(create_err):
                                self.log.warning("[%s] ⚠️ Failed to create group for %s: %s", self.name, stream, create_err)
                    time.sleep(1)  # Короткая пауза перед повтором
                else:
                    self.log.error("[%s] redis error: %s", self.name, e)
                    if self.health_cb:
                        self.health_cb(self.name, "error", {"reason": str(e)})
                    time.sleep(2)
            except Exception as e:
                self.log.error("[%s] loop error: %s", self.name, e)
                if self.health_cb:
                    self.health_cb(self.name, "error", {"reason": str(e)})
                time.sleep(1)

            # lossless recovery
            if self.policy.ack_mode == "lossless":
                if now - self._last_pending_drain >= self.policy.drain_pending_every_s:
                    drained = self._drain_own_pending_once()
                    self._last_pending_drain = now
                    if drained:
                        self.log.debug("[%s] drained own pending: %d", self.name, drained)

                if now - self._last_orphan_claim >= self.policy.claim_orphan_every_s:
                    claimed = self._claim_orphan_pending()
                    self._last_orphan_claim = now
                    if claimed:
                        self.log.debug("[%s] claimed orphan pending: %d", self.name, claimed)

        self.log.info("Worker %s stopped", self.name)

