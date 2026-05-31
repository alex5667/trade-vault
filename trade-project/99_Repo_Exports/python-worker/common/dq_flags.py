from __future__ import annotations

from typing import Any


def ensure_dq_flags(ctx: Any) -> list[str]:
    """
    Ensure ctx.data_quality_flags is a mutable list[str].
      - If absent -> create []
      - If tuple/set/iterable -> convert to list for in-place append

    Fail-open:
      - If ctx is not writable -> return [] (caller should not crash)
    """
    if ctx is None:
        return []
    flags = getattr(ctx, "data_quality_flags", None)
    if flags is None:
        flags = []
        try:
            ctx.data_quality_flags = flags
        except Exception:
            return []
        return flags
    if isinstance(flags, list):
        return flags
    try:
        lst = list(flags)  # type: ignore[arg-type]
        ctx.data_quality_flags = lst
        return lst
    except Exception:
        return []


def append_dq_flag(ctx: Any, flag: str) -> None:
    """
    Append a DQ flag to ctx.data_quality_flags.
    - trims whitespace
    - avoids duplicates
    - fail-open (never raises)
    """
    try:
        f = (flag or "").strip()
        if not f:
            return
        flags = ensure_dq_flags(ctx)
        if f not in flags:
            flags.append(f)
    except Exception:
        return
