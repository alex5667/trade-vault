from __future__ import annotations

# core/sessions.py
from datetime import datetime, timezone
from typing import Literal

Session = Literal["asia", "europe", "us"]

def get_session(ts: datetime) -> Session:
    """
    Определяет сессию по времени в UTC.

    Asia: 00:00–08:00 UTC
    Europe: 08:00–16:00 UTC
    US: 16:00–24:00 UTC
    """
    # ts в UTC
    h = ts.hour
    if 0 <= h < 8:
        return "asia"
    if 8 <= h < 16:
        return "europe"
    return "us"


def get_session_from_ts(ts_utc: float) -> Session:
    """
    Определяет сессию по Unix timestamp (в секундах).
    """
    dt = datetime.fromtimestamp(ts_utc, tz=timezone.utc)
    return get_session(dt)

