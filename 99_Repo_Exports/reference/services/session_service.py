from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

try:
    # Python 3.9+ stdlib
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

from domain.time_utils import normalize_ts_ms


def _minutes_of_day(dt: datetime) -> int:
    # Existing helper may already exist in your file.
    # If you already have _minutes_of_day(dt) -> keep your original implementation.
    return dt.hour * 60 + dt.minute


def session_key_from_epoch_ms(ts_ms: Any) -> str:
    """
    Convert epoch ms -> session key.

    Fail-open:
      - invalid ts -> "na"
      - timezone conversion failures -> "na"
    """
    t = normalize_ts_ms(ts_ms)
    if t <= 0:
        return "na"

    try:
        dt_utc = datetime.fromtimestamp(t / 1000.0, tz=timezone.utc)
    except Exception:
        return "na"

    # If zoneinfo is unavailable, fall back to UTC-only coarse sessions.
    if ZoneInfo is None:  # pragma: no cover
        m_utc = _minutes_of_day(dt_utc)
        if 8 * 60 <= m_utc < 16 * 60:
            return "european"
        return "overnight"

    try:
        # Asia: 09:00–17:00 JST
        dt_jst = dt_utc.astimezone(ZoneInfo("Asia/Tokyo"))
        m_jst = _minutes_of_day(dt_jst)
        if 9 * 60 <= m_jst < 17 * 60:
            return "asian"

        # Europe: 08:00–16:00 GMT/UTC (use UTC directly)
        m_utc = _minutes_of_day(dt_utc)
        if 8 * 60 <= m_utc < 16 * 60:
            return "european"

        # US main: 09:00–16:00 New York (DST-aware)
        dt_ny = dt_utc.astimezone(ZoneInfo("America/New_York"))
        m_ny = _minutes_of_day(dt_ny)
        if 9 * 60 <= m_ny < 16 * 60:
            return "us_main"
    except Exception:
        # Any tz conversion issues must not break callers.
        return "na"

    return "overnight"


def session_key_from_ctx(ctx: Any) -> str:
    """
    Extract + normalize ts from ctx and map to session key.
    """
    try:
        ts = getattr(ctx, "ts_ms", None)
        if ts is None:
            ts = getattr(ctx, "ts", None)
        return session_key_from_epoch_ms(ts)
    except Exception:
        return "na"
