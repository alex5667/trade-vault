from __future__ import annotations

import math
import os
from datetime import datetime, timezone
from typing import Any, Optional

# NOTE: zoneinfo is stdlib in Python 3.9+.
# If it's missing for any reason (exotic runtime), we fall back to UTC buckets.
try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

# ---------------------------------------------------------------------------
# Epoch ms normalization (single source of truth)
#
# Policy:
#   - invalid / <=0 / non-finite -> 0
#   - < 1e12 -> treat as "seconds", normalize to ms (x1000)
#   - else -> already ms
#
# Why 1e12:
#   1_000_000_000_000 ms ~= 2001-09-09. For modern streams, any real epoch-ms
#   should be >= 1e12. This gives a stable, low-risk heuristic.
# ---------------------------------------------------------------------------
_EPOCH_MS_CUTOFF = 1_000_000_000_000  # 1e12

# ---------------------------------------------------------------------
# STRICT EPOCH FLOOR + PLAUSIBILITY WINDOW (anti-regression)
# ---------------------------------------------------------------------
# If ts comes as "minutes-of-day" (0..1439) or other non-epoch small numbers,
# classic normalize_ts_ms would treat it as seconds and multiply -> still non-epoch.
# Strict normalization rejects anything that does not look like real epoch ms.
# Hard normalization additionally rejects far-future/far-past timestamps that
# can appear due to parsing bugs / clock domain mixups.
_STRICT_EPOCH_SEC_MIN = int(os.getenv("STRICT_EPOCH_SEC_MIN", "1000000000"))  # ~2001-09-09
_STRICT_EPOCH_MS_MIN = _STRICT_EPOCH_SEC_MIN * 1000
_STRICT_FUTURE_SKEW_MS = int(os.getenv("STRICT_EPOCH_FUTURE_SKEW_MS", str(10 * 60 * 1000)))   # 10 minutes
_STRICT_MAX_AGE_MS     = int(os.getenv("STRICT_EPOCH_MAX_AGE_MS",     str(10 * 365 * 24 * 3600 * 1000)))  # 10 years


def _minutes_of_day(dt: datetime) -> int:
    return int(dt.hour) * 60 + int(dt.minute)


def session_from_ts_ms(ts_ms: Any) -> str:
    """
    Единый источник истины для вычисления "торговой сессии" из epoch ms.
    Используется:
      - в slippage EMA (gate reader)
      - в stats writer (post-close EMA writer)

    Жёсткая политика (зафиксирована тестами):
      - ts_ms <= 0 -> "na"
      - seconds -> нормализуем в ms (через normalize_ts_ms)
      - non-epoch -> "na"

    Сессии (как в вашем описании session_service.py):
      - us_main    : 09:00-16:00 America/New_York
      - european   : 08:00-16:00 Europe/London (GMT/BST)
      - asian      : 09:00-17:00 Asia/Tokyo
      - overnight  : иначе
    """
    try:
        t = normalize_ts_ms(int(float(ts_ms or 0)))
    except Exception:
        t = 0
    if t <= 0:
        return "na"

    # If zoneinfo isn't available, fall back to a simple UTC partitioning.
    if ZoneInfo is None:  # pragma: no cover
        try:
            dt = datetime.fromtimestamp(t / 1000.0, tz=timezone.utc)
            h = int(dt.hour)
            if 0 <= h < 7:
                return "asian"
            if 7 <= h < 13:
                return "european"
            if 13 <= h < 21:
                return "us_main"
            return "overnight"
        except Exception:
            return "na"

    try:
        # Prefer explicit TZ windows to match your session_service semantics.
        dt_utc = datetime.fromtimestamp(t / 1000.0, tz=timezone.utc)

        dt_us = dt_utc.astimezone(ZoneInfo("America/New_York"))
        mus = _minutes_of_day(dt_us)
        if 9 * 60 <= mus < 16 * 60:
            return "us_main"

        dt_eu = dt_utc.astimezone(ZoneInfo("Europe/London"))
        meu = _minutes_of_day(dt_eu)
        if 8 * 60 <= meu < 16 * 60:
            return "european"

        dt_as = dt_utc.astimezone(ZoneInfo("Asia/Tokyo"))
        mas = _minutes_of_day(dt_as)
        if 9 * 60 <= mas < 17 * 60:
            return "asian"

        return "overnight"
    except Exception:
        return "na"


def normalize_ts_ms(ts: Any) -> int:
    """
    Normalize input timestamp to epoch milliseconds.
    Returns 0 on invalid values (fail-open for downstream gates).
    """
    try:
        if ts is None:
            return 0
        if isinstance(ts, str):
            s = ts.strip()
            if not s:
                return 0
            v = float(s)
        else:
            v = float(ts)

        if not math.isfinite(v):
            return 0
        x = int(v)
        if x <= 0:
            return 0
        if x < _EPOCH_MS_CUTOFF:
            # seconds -> ms
            return x * 1000
        return x
    except Exception:
        return 0


def normalize_ts_ms_strict(ts: Any) -> int:
    """
    STRICT variant for signal/gate paths where non-epoch timestamps must not leak.

    Policy:
      - First apply normalize_ts_ms (string->float->int; seconds->ms).
      - Then enforce plausible epoch-ms floor:
          if 0 < ms < _STRICT_EPOCH_MS_MIN => treat as invalid (0).

    Rationale:
      - Protect against regressions where "minutes-of-day" or other non-epoch clocks
        accidentally end up in ctx.ts/ts_ms and get interpreted as epoch.
      - For true epoch seconds (e.g. 1_700_000_000) normalization yields 1_700_000_000_000
        which passes the strict floor.
      - Fail-open: invalid -> 0; downstream must fall back to "na" session / no EMA / use now().
    """
    ms = int(normalize_ts_ms(ts) or 0)
    if ms > 0 and ms < _STRICT_EPOCH_MS_MIN:
        return 0
    return ms


def ctx_epoch_ms(ctx: Any) -> int:
    """
    Extract + normalize epoch ms from ctx-like objects.
    Priority: ts_ms -> ts -> timestamp.
    Returns 0 if missing/invalid.
    """
    if ctx is None:
        return 0
    for name in ("ts_ms", "ts", "timestamp"):
        try:
            v = getattr(ctx, name, None)
            if v is not None:
                t = normalize_ts_ms(v)
                if t > 0:
                    return t
        except Exception:
            pass
    return 0


_EPOCH_MS_MIN = 1_000_000_000_000  # 10^12 (~2001-09-09 in ms). Below this is suspicious for "epoch ms".

def normalize_epoch_ms_strict(ts_any: Any) -> int:
    """
    Strict epoch-ms normalizer for *gates* and *session extraction*.

    Why this exists:
      - In the pipeline you may see non-epoch time representations elsewhere (e.g. minutes-of-day),
        and even if "it shouldn't happen" for signal ctx, the safest approach is to harden the
        execution-cost/entry-quality gates against regressions.

    Policy (fail-open):
      - If ts is invalid or <= 0                  -> return 0 (caller disables session/EMA usage).
      - If ts looks like seconds epoch (< 1e12)   -> attempt seconds->ms conversion.
      - If still non-epoch after conversion       -> return 0.

    IMPORTANT:
      - This does NOT change trade logic directly. It only protects optional EMA/session-based logic.
    """
    raw = 0
    try:
        raw = int(float(ts_any or 0))
    except Exception:
        return 0
    if raw <= 0:
        return 0

    # Reuse existing normalizer if present in this module.
    # If you already have normalize_ts_ms() here, keep using it as the single source of truth.
    try:
        t1 = int(normalize_ts_ms(raw))  # type: ignore[name-defined]
    except Exception:
        t1 = raw

    if t1 <= 0:
        return 0

    if t1 < _EPOCH_MS_MIN:
        # Likely seconds epoch, try *1000 and re-normalize.
        try:
            t2 = int(normalize_ts_ms(int(t1) * 1000))  # type: ignore[name-defined]
        except Exception:
            t2 = int(t1) * 1000
        return int(t2) if t2 >= _EPOCH_MS_MIN else 0

    return int(t1)


def normalize_ts_ms_hard(ts: Any, *, now_ms: int | None = None) -> int:
    """
    HARD variant for live signal/gate paths.

    It is intentionally *stricter* than normalize_ts_ms_strict:
      1) normalize_ts_ms_strict(ts)  -> epoch-ms or 0
      2) reject if:
          - ms is far in the future: ms > now + STRICT_EPOCH_FUTURE_SKEW_MS
          - ms is far in the past:   ms < now - STRICT_EPOCH_MAX_AGE_MS

    Why:
      - Some bugs produce "valid-looking" epoch numbers but from a wrong clock domain:
        e.g., stale cached ts, unit mismatch, or timestamp from another system.
      - Using such ts in session extraction / EMA keys silently poisons stats.

    Fail-open:
      - returns 0 on any suspicion; downstream must skip EMA/session and use defaults.

    NOTE:
      - Defaults are chosen to be safe for LIVE while still allowing long replays if needed
        (tune STRICT_EPOCH_MAX_AGE_MS for replay environments).
    """
    ms = int(normalize_ts_ms_strict(ts) or 0)
    if ms <= 0:
        return 0
    try:
        n = int(now_ms if now_ms is not None else (time.time() * 1000))
    except Exception:
        n = 0
    if n > 0:
        if ms > (n + int(_STRICT_FUTURE_SKEW_MS)):
            return 0
        if ms < (n - int(_STRICT_MAX_AGE_MS)):
            return 0
    return ms


# NOTE: keep any existing functions below as-is.