# publish_adapter.py
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import time
from typing import Any, Dict, Optional

from .outbox_utils import (
    ensure_ts_ms, normalize_to_bucket,
    normalize_kind,
    PublishResult,
)


class OutboxPublishAdapter:
    def __init__(self, outbox: Any, *, source: str, strategy: str, dedup_bucket_ms: int = 60_000):
        self.outbox = outbox
        self.source = source
        self.strategy = strategy
        self.dedup_bucket_ms = int(dedup_bucket_ms)

    def publish(
        self,
        *,
        symbol: str,
        side: str,
        kind: str | None,
        level_key: str | None,
        ts_ms: int | float | None,
        envelope: Dict[str, Any],
        dedup_ttl_ms: Optional[int] = None,
    ) -> PublishResult:
        if not self.outbox:
            return PublishResult(sent=False, dedup=False, msg_id=None)

        ts = ensure_ts_ms(ts_ms)
        if ts <= 0:
            ts = get_ny_time_millis()

        ts_norm = normalize_to_bucket(ts, self.dedup_bucket_ms)
        k = normalize_kind(kind)

        try:
            msg_id = self.outbox.publish(
                source=self.source,
                strategy=self.strategy,
                symbol=symbol,
                side=side.upper(),
                kind=k,
                level_key=(level_key or ""),
                ts_ms=ts_norm,
                envelope=envelope,
                dedup_ttl_ms=dedup_ttl_ms,
            )
            # по вашей спецификации: None => дедуп сработал
            if msg_id is None:
                return PublishResult(sent=False, dedup=True, msg_id=None)
            return PublishResult(sent=True, dedup=False, msg_id=str(msg_id))
        except Exception:
            return PublishResult(sent=False, dedup=False, msg_id=None)
