"""Calendar-derived deterministic flags for feature engineering (B2).

Goals
-----
- Provide small, low-cardinality calendar context for models (EOM/EOQ).
- Deterministic in UTC, independent of local timezone.
- Safe for real-time: fail-open callers can drop fields on any error.

Design
------
We expose boolean EOM/EOQ flags as well as optional numeric DOM/DOQ:
- cal_eom_utc: 1 if timestamp falls on the last UTC day of its month.
- cal_eoq_utc: 1 if timestamp falls on the last UTC day of its quarter.
- cal_dom_utc: day-of-month (1..31) in UTC.
- cal_doq_utc: day-of-quarter (1..92) in UTC.

These keys are intended to be appended to MLFeatureSchemaV7OF.
"""

from __future__ import annotations

from datetime import datetime, timezone, date
import calendar
from typing import Dict


def _utc_date_from_ts_ms(ts_ms: int) -> date:
    """Convert epoch milliseconds -> UTC date.

    Notes:
    - Uses timezone.utc to avoid local timezone dependence.
    - Expects ts_ms to be epoch milliseconds.
    """
    # Defensive: callers may pass floats/strings, but we require int here.
    # Let ValueError/TypeError bubble; callers are expected to be fail-open.
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.date()


def is_end_of_month_utc(ts_ms: int) -> int:
    """Return 1 if ts_ms is on the last day of the month in UTC."""
    d = _utc_date_from_ts_ms(ts_ms)
    last_dom = calendar.monthrange(d.year, d.month)[1]
    return 1 if d.day == last_dom else 0


def is_end_of_quarter_utc(ts_ms: int) -> int:
    """Return 1 if ts_ms is on the last day of the quarter in UTC."""
    d = _utc_date_from_ts_ms(ts_ms)
    if d.month not in (3, 6, 9, 12):
        return 0
    last_dom = calendar.monthrange(d.year, d.month)[1]
    return 1 if d.day == last_dom else 0


def day_of_month_utc(ts_ms: int) -> int:
    """Day-of-month (1..31) in UTC."""
    d = _utc_date_from_ts_ms(ts_ms)
    return int(d.day)


def day_of_quarter_utc(ts_ms: int) -> int:
    """Day-of-quarter (1..92) in UTC.

    Quarter start months are 1,4,7,10.
    """
    d = _utc_date_from_ts_ms(ts_ms)
    q_start_month = 1 + ((d.month - 1) // 3) * 3
    q0 = date(d.year, q_start_month, 1)
    return int((d - q0).days + 1)


def calendar_flags_utc(ts_ms: int) -> Dict[str, int]:
    """Compute calendar features for a UTC timestamp (epoch ms).

    Returns a dict with the canonical B2 keys.

    Fail-open recommendation for callers:
    >>> try:
    ...     indicators.update(calendar_flags_utc(ts_ms))
    ... except Exception:
    ...     pass
    """
    ts_ms_i = int(ts_ms)
    if ts_ms_i <= 0:
        # Keep deterministic zeros for missing/invalid timestamps.
        return {
            "cal_eom_utc": 0,
            "cal_eoq_utc": 0,
            "cal_dom_utc": 0,
            "cal_doq_utc": 0,
        }

    # Compute once to avoid double timestamp conversions.
    d = _utc_date_from_ts_ms(ts_ms_i)
    last_dom = calendar.monthrange(d.year, d.month)[1]

    eom = 1 if d.day == last_dom else 0
    eoq = 1 if (d.month in (3, 6, 9, 12) and d.day == last_dom) else 0

    q_start_month = 1 + ((d.month - 1) // 3) * 3
    q0 = date(d.year, q_start_month, 1)
    doq = int((d - q0).days + 1)

    return {
        "cal_eom_utc": int(eom),
        "cal_eoq_utc": int(eoq),
        "cal_dom_utc": int(d.day),
        "cal_doq_utc": int(doq),
    }
