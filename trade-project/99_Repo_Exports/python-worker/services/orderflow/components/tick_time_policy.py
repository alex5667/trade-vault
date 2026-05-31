from __future__ import annotations

from services.orderflow.configuration import _safe_int

def _msgid_to_ms(msg_id: str) -> int:
    try:
        return int(str(msg_id).split("-", 1)[0])
    except Exception:
        return 0

def coerce_event_ts_ms(
    *,
    msg_id: str,
    payload_ts_ms: int,
    now_ms: int,
    max_ts_skew_ms: int,
) -> tuple[int, str]:
    """Детерминированный выбор event_time:
    1) tick.ts_ms если в пределах max_ts_skew_ms от wall-clock
    2) Redis stream-id ms
    3) wall-clock (last resort)
    """
    ts = _safe_int(payload_ts_ms or 0)
    if ts > 0 and abs(now_ms - ts) <= max_ts_skew_ms:
        return ts, "payload"
    mid = _msgid_to_ms(msg_id)
    if mid > 0:
        return mid, "stream_id"
    return now_ms, "now"
