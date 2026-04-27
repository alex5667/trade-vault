from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class XAddEntry:
    stream: str
    fields: Dict[str, str]
    maxlen: Optional[int] = None
    approximate: bool = True


class FakeRedis:
    """In-memory Redis stub for unit/integration tests.

    Design goals:
      - async-compatible API surface (xadd/get/set) used by this repo.
      - deterministic behavior; no external IO.
      - minimal correctness: sufficient for write_decision_record(), cb_state.update(),
        and gate-metrics emission paths.

    NOT a full Redis emulator.
    """

    def __init__(self) -> None:
        self.kv: Dict[str, str] = {}
        self.streams: Dict[str, List[Tuple[str, Dict[str, str]]]] = {}
        self.xadd_log: List[XAddEntry] = []
        self._seq: int = 0

    def _next_id(self) -> str:
        # Redis stream ids look like <ms>-<seq>. We keep it simple and monotone.
        self._seq += 1
        return f"0-{self._seq}"

    async def get(self, key: str) -> Optional[str]:
        return self.kv.get(key)

    async def hgetall(self, key: str) -> Dict[str, str]:
        return {}

    async def hset(self, name: str, key: Optional[str] = None, value: Optional[str] = None, mapping: Optional[Dict[str, Any]] = None) -> None:
        pass

    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:  # noqa: ARG002
        self.kv[str(key)] = str(value)
        return True

    async def incr(self, key: str) -> int:
        v = int(float(self.kv.get(key, "0") or 0))
        v += 1
        self.kv[key] = str(v)
        return v

    async def xadd(
        self,
        stream: str,
        fields: Dict[str, Any],
        maxlen: Optional[int] = None,
        approximate: bool = True,
    ) -> str:
        sid = self._next_id()
        s = str(stream)
        f = {str(k): str(v) for k, v in (fields or {}).items()}

        self.streams.setdefault(s, []).append((sid, f))
        self.xadd_log.append(XAddEntry(stream=s, fields=f, maxlen=maxlen, approximate=approximate))

        # Enforce maxlen best-effort (approximate trimming).
        if maxlen is not None and maxlen > 0:
            q = self.streams.get(s) or []
            if len(q) > int(maxlen):
                self.streams[s] = q[-int(maxlen) :]

        # Give the scheduler a chance if caller expects "awaitable" semantics.
        await asyncio.sleep(0)
        return sid


class FakePublisher:
    """Minimal stub for publisher objects used by tick_processor."""

    def __init__(self) -> None:
        self.published: List[Tuple[str, Dict[str, Any]]] = []

    async def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        self.published.append((str(topic), dict(payload)))

