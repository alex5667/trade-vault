from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import time
from typing import Any, Dict


def _ms_now() -> int:
    return get_ny_time_millis()


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)



def guard_view(doc: Dict[str, Any] | None, *, now_ms: int | None = None) -> Dict[str, Any]:
    """Return the canonical reader-facing semantics for active_symbol guard docs.

    This utility is the single contract for all readers of
    orders:active_symbol_sid:{SYMBOL}. Writers may add more fields, but readers
    should derive blocking / released / pending-release meaning only via this
    function so tombstone semantics remain consistent across executor
    projection, repair-worker, exporters and tests.
    """
    raw = dict(doc or {})
    now = int(now_ms or _ms_now())
    symbol = str(raw.get("symbol") or "").strip().upper()
    sid = str(raw.get("sid") or "").strip()
    status = str(raw.get("guard_status") or "active").strip().lower()
    if status not in {"active", "released"}:
        status = "active"
    released_at_ms = _i(raw.get("released_at_ms") or raw.get("updated_at_ms") or raw.get("guard_writer_ts_ms"), 0)
    updated_at_ms = _i(raw.get("updated_at_ms") or raw.get("guard_writer_ts_ms") or raw.get("guard_lease_epoch_ms"), 0)
    guard_version = _i(raw.get("guard_version"), 0)
    pending_release = bool(raw.get("guard_release_pending"))
    terminalish = bool(raw.get("state_terminalish"))
    tombstone_age_ms = 0
    if status == "released" and released_at_ms > 0:
        tombstone_age_ms = max(0, now - released_at_ms)

    is_blocking = bool(symbol and sid and status == "active")
    return {
        "symbol": symbol,
        "sid": sid,
        "status": status,
        "is_present": bool(raw),
        "is_active": status == "active",
        "is_released": status == "released",
        "is_blocking": is_blocking,
        "guard_version": guard_version,
        "guard_release_pending": pending_release,
        "guard_release_policy": str(raw.get("guard_release_policy") or ""),
        "guard_release_reason": str(raw.get("guard_release_reason") or raw.get("release_reason") or ""),
        "state_terminalish": terminalish,
        "released_at_ms": released_at_ms,
        "updated_at_ms": updated_at_ms,
        "tombstone_age_ms": tombstone_age_ms,
        "guard_writer": str(raw.get("guard_writer") or ""),
        "guard_lease_token": str(raw.get("guard_lease_token") or "")
    }



def active_guard_doc(doc: Dict[str, Any] | None, *, now_ms: int | None = None) -> Dict[str, Any]:
    raw = dict(doc or {})
    return raw if guard_view(raw, now_ms=now_ms).get("is_blocking") else {}



def released_tombstone_doc(doc: Dict[str, Any] | None, *, now_ms: int | None = None) -> Dict[str, Any]:
    raw = dict(doc or {})
    return raw if guard_view(raw, now_ms=now_ms).get("is_released") else {}
