from __future__ import annotations

"""
ReplayRecorder: env-driven JSONL recorder for tick/ctx/signal.

Env:
  REPLAY_RECORD=1
  REPLAY_RECORD_PATH=/tmp/replay.jsonl
  REPLAY_RECORD_TYPES=ctx,signal,tick
  REPLAY_RECORD_FLUSH=1
  REPLAY_RECORD_FSYNC=0
  REPLAY_RECORD_SAMPLE_EVERY=1
"""

import os
import time
from typing import Any, Dict, Optional, Set

from replay.ctx_export import export_ctx
from replay.jsonl import JsonlWriter


def _parse_types(s: str) -> Set[str]:
    ss = (s or "").strip().lower()
    if not ss:
        return {"ctx"}
    out: Set[str] = set()
    for x in ss.split(","):
        t = x.strip()
        if t:
            out.add(t)
    return out or {"ctx"}


class ReplayRecorder:
    def __init__(self) -> None:
        self.enabled = os.getenv("REPLAY_RECORD", "0").lower() in {"1", "true", "yes", "on"}
        self.path = os.getenv("REPLAY_RECORD_PATH", "").strip()
        self.types = _parse_types(os.getenv("REPLAY_RECORD_TYPES", "ctx"))
        self.flush = os.getenv("REPLAY_RECORD_FLUSH", "1").lower() not in {"0", "false", "no"}
        self.fsync = os.getenv("REPLAY_RECORD_FSYNC", "0").lower() in {"1", "true", "yes", "on"}
        self.sample_every = max(1, int(os.getenv("REPLAY_RECORD_SAMPLE_EVERY", "1") or 1))
        self._w: Optional[JsonlWriter] = None
        self._n = 0

        if self.enabled and self.path:
            self._w = JsonlWriter(self.path, flush=self.flush, fsync=self.fsync)

    def _write(self, rec: Dict[str, Any]) -> None:
        if not self._w:
            return
        self._n += 1
        if (self._n % self.sample_every) != 0:
            return
        self._w.write(rec)

    def record_tick(self, payload: Dict[str, Any]) -> None:
        if not self.enabled or "tick" not in self.types:
            return
        self._write({"type": "tick", "ts_ms": int(time.time() * 1000), "payload": payload})

    def record_ctx(self, ctx: Any) -> None:
        if not self.enabled or "ctx" not in self.types:
            return
        self._write({"type": "ctx", "ts_ms": int(time.time() * 1000), "payload": export_ctx(ctx)})

    def record_signal(self, payload: Dict[str, Any]) -> None:
        if not self.enabled or "signal" not in self.types:
            return
        # payload already dict-like
        self._write({"type": "signal", "ts_ms": int(time.time() * 1000), "payload": dict(payload)})

    def close(self) -> None:
        if self._w:
            self._w.close()
