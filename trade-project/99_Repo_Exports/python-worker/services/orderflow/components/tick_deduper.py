from __future__ import annotations

import contextlib
from typing import Any

from services.orderflow.configuration import _safe_int
from services.orderflow.metrics import tick_dedup_drop_total
from services.orderflow.utils import _compute_tick_uid

def is_duplicate_tick(tick: dict, runtime: Any, symbol: str, raw: dict, *, msg_id: str = "") -> bool:
    """Market-level dedup: trade_id > content-hash(exchange_ts_ms|price|qty|side|bm).

    stream_id (Redis msg_id) is intentionally excluded from the economic dedup UID —
    a re-XADDed tick with a new stream_id but identical exchange payload must still
    be detected as a duplicate. stream_id is only relevant for ACK/PEL bookkeeping.
    """
    try:
        uid = (tick.get("tick_uid") or "")
        if uid.startswith(tick.get("symbol", symbol).upper() + ":h") or not uid:
            # Use exchange_ts_ms (immutable) for the content hash so that the UID is
            # stable across re-XADDs (same exchange payload → same hash regardless of
            # which Redis stream entry carried it).
            exchange_ts = _safe_int(tick.get("exchange_ts_ms") or tick.get("payload_ts_ms") or tick.get("ts_ms") or 0)
            uid = _compute_tick_uid(
                symbol=str(tick.get("symbol") or symbol),
                trade_id=tick.get("trade_id"),
                ts_ms=exchange_ts,
                price_src=raw.get("price") or raw.get("last") or raw.get("mid"),
                qty_src=raw.get("qty") or raw.get("volume"),
                side=(tick.get("side") or ""),
                is_buyer_maker=tick.get("is_buyer_maker"),
                stream_id=None,  # excluded from market-level dedupe UID
            )
            tick["tick_uid"] = uid
        if uid and runtime.is_duplicate_tick_uid(uid):
            with contextlib.suppress(Exception):
                tick_dedup_drop_total.labels(symbol=symbol).inc()
            return True
    except Exception:
        pass
    return False
