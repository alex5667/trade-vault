from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import redis

from services.signal_preprocess import preprocess_signal_for_publish


def _json_dumps_safe(obj: Any) -> str:
    """
    Sync hot-path JSON: MUST NOT raise.
    default=str is deliberate (Enums/Decimals/np types appear in the wild).
    """
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        return '{"error":"json_dumps_failed"}'


@dataclass(frozen=True)
class StreamSink:
    name: str
    field: str = "payload"
    maxlen: int = 10000


@dataclass(frozen=True)
class SyncPublishResult:
    ok: bool
    busy_loading: bool
    errors: int


class SyncSignalPublisher:
    """
    Shared publisher for sync producers (redis-py).

    Goals:
      - one place for FAIL-OPEN publish semantics
      - BusyLoading short-circuit
      - contract normalization before publish
    """

    def __init__(self, *, redis_client: Any, source: str, metrics_prefix: str = "signals_publish_sync", logger: Any = None) -> None:
        self.r = redis_client
        self.source = (source or "na")
        self.metrics_prefix = (metrics_prefix or "signals_publish_sync")
        self.logger = logger

    def xadd_json(self, *, sink: StreamSink, payload: dict[str, Any], symbol: str, approximate: bool = True) -> SyncPublishResult:
        errors = 0
        try:
            preprocess_signal_for_publish(payload, symbol=symbol, source=self.source, logger=self.logger)
        except Exception:
            pass

        ser = _json_dumps_safe(payload)
        try:
            self.r.xadd(
                sink.name,
                {str(sink.field or "payload"): ser},
                maxlen=int(sink.maxlen),
                approximate=bool(approximate),
            )
            return SyncPublishResult(ok=True, busy_loading=False, errors=0)
        except redis.exceptions.BusyLoadingError:
            return SyncPublishResult(ok=False, busy_loading=True, errors=0)
        except Exception as e:
            errors += 1
            try:
                if self.r is not None:
                    self.r.incr(f"{self.metrics_prefix}:xadd_errors_total")
            except Exception:
                pass
            try:
                if self.logger is not None:
                    self.logger.warning("sync_publish.xadd failed stream=%s err=%r", sink.name, e)
            except Exception:
                pass
            return SyncPublishResult(ok=False, busy_loading=False, errors=errors)
