from __future__ import annotations

import os
import time
import logging
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
        self,
        *,
        redis: redis.Redis,
        stream: str,
        group: str,
        consumer: str,
        dlq_stream: str,
        block_ms: int = 2000,
        count: int = 50,
        claim_idle_ms: int = 60_000,
    ):
        self.r = redis
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.dlq_stream = dlq_stream
        self.block_ms = int(block_ms)
        self.count = int(count)
        self.claim_idle_ms = int(claim_idle_ms)

        self._ensure_group()

    def _ensure_group(self) -> None:
        try:
            self.r.xgroup_create(self.stream, self.group, id="0-0", mkstream=True)
        except Exception as e:
            # BUSYGROUP is ok
            if "BUSYGROUP" not in str(e):
                raise

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
        while True:
            any_msg = False
            for msg_id, fields in self._iter_batch():
                any_msg = True
                try:
                    self.handle_message(msg_id, fields)
                    self.r.xack(self.stream, self.group, msg_id)
                except Exception as e:
                    self._dlq(msg_id, fields, str(e))
                    # ACK чтобы pending не раздувался. Если хотите retry — делайте requeue внутри handle_message.
                    try:
                        self.r.xack(self.stream, self.group, msg_id)
                    except Exception:
                        pass

            if not any_msg:
                time.sleep(0.05)
