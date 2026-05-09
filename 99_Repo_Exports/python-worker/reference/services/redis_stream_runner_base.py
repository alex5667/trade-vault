# python-worker/services/redis_stream_runner_base.py
from __future__ import annotations

import os
import socket
from collections.abc import Iterable
from dataclasses import dataclass

import redis

from utils.time_utils import get_ny_time_millis


@dataclass(frozen=True)
class StreamMsg:
    stream: str
    msg_id: str
    fields: dict[str, str]


class RedisStreamRunner:
    """
    SRE-grade runner:
      - ensure consumer-group exists (MKSTREAM)
      - read with XREADGROUP (>)
      - periodically XAUTOCLAIM pending (min_idle_ms)
      - ACK only on success
      - poison messages -> DLQ + ACK (чтобы не стопорить партицию)
    """

    def __init__(
        self,
        r: redis.Redis,
        group: str,
        consumer: str | None = None,
        block_ms: int = 2000,
        read_count: int = 50,
        autoclaim_min_idle_ms: int = 45000,
        autoclaim_count: int = 100,
        dlq_prefix: str = "dlq",
    ):
        self.r = r
        self.group = group
        self.consumer = consumer or f"{socket.gethostname()}:{os.getpid()}"
        self.block_ms = block_ms
        self.read_count = read_count
        self.autoclaim_min_idle_ms = autoclaim_min_idle_ms
        self.autoclaim_count = autoclaim_count
        self.dlq_prefix = dlq_prefix

    def ensure_group(self, stream: str) -> None:
        try:
            # MKSTREAM: создаст stream, если его нет
            self.r.xgroup_create(stream, self.group, id="0-0", mkstream=True)
        except redis.ResponseError as e:
            # BUSYGROUP -> ok
            if "BUSYGROUP" not in str(e):
                raise

    def ensure_groups(self, streams: Iterable[str]) -> None:
        for s in streams:
            self.ensure_group(s)

    def _xautoclaim(self, stream: str, start_id: str = "0-0") -> tuple[str, list[StreamMsg]]:
        """
        redis-py: xautoclaim(stream, group, consumer, min_idle_time, start_id, count=?)
        return: (next_start_id, [(msg_id, {field:val}),...], deleted_ids?)
        """
        try:
            res = self.r.xautoclaim(
                name=stream,
                groupname=self.group,
                consumername=self.consumer,
                min_idle_time=self.autoclaim_min_idle_ms,
                start_id=start_id,
                count=self.autoclaim_count,
            )
        except Exception:
            # fallback if xautoclaim missing in older redis-py
            res = self.r.execute_command(
                "XAUTOCLAIM",
                stream,
                self.group,
                self.consumer,
                self.autoclaim_min_idle_ms,
                start_id,
                "COUNT",
                self.autoclaim_count,
            )

        next_id = res[0]
        raw_msgs = res[1] or []
        msgs: list[StreamMsg] = []
        for msg_id, fields in raw_msgs:
            msgs.append(StreamMsg(stream=stream, msg_id=msg_id, fields={k: v for k, v in fields.items()}))
        return next_id, msgs

    def claim_cycle(self, streams: list[str]) -> list[StreamMsg]:
        """
        Один проход autoclaim по всем streams: возвращает пачку полученных сообщений.
        """
        out: list[StreamMsg] = []
        for s in streams:
            start = "0-0"
            # один проход (можно сделать while, но лучше дозировать)
            nxt, msgs = self._xautoclaim(s, start_id=start)
            out.extend(msgs)
        return out

    def read_new(self, streams: list[str]) -> list[StreamMsg]:
        """
        Читает новые (>) из нескольких стримов сразу.
        """
        if not streams:
            return []

        streams_map = dict.fromkeys(streams, ">")
        res = self.r.xreadgroup(
            groupname=self.group,
            consumername=self.consumer,
            streams=streams_map,
            count=self.read_count,
            block=self.block_ms,
        )
        out: list[StreamMsg] = []
        for stream_name, items in res:
            for msg_id, fields in items:
                out.append(StreamMsg(stream=stream_name, msg_id=msg_id, fields={k: v for k, v in fields.items()}))
        return out

    def ack(self, stream: str, msg_id: str) -> None:
        self.r.xack(stream, self.group, msg_id)

    def to_dlq(self, stream: str, msg: StreamMsg, reason: str) -> None:
        key = f"{self.dlq_prefix}:{stream}"
        now_ms = get_ny_time_millis()
        payload = {
            "ts": str(now_ms),
            "reason": reason,
            "src_stream": msg.stream,
            "src_id": msg.msg_id,
            "fields": str(msg.fields),
            "consumer": self.consumer,
            "group": self.group,
        }
        # DLQ тоже тримим
        self.r.xadd(key, payload, maxlen=100000, approximate=True)

