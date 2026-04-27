from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BookHealth:
    ok: int
    state: str            # "OK"|"WARN"|"ERR"
    rate_hz: float
    ok_min_hz: float
    crit_hz: float
    age_ms: int
    max_age_ms: int
    reason: str


def compute_book_health(
    *,
    now_ts_ms: int,
    last_book_ts_ms: int,
    rate_hz: float,
    ok_min_hz: float,
    crit_hz: float,
    age_floor_ms: int = 500,
    age_mult: float = 3.0,
) -> BookHealth:
    """
    Deterministic health:
    - Rate: runtime.book_rate_ema (Hz)
    - Age: now_ts_ms - last_book_ts_ms (exchange timestamps)
    Gate rule:
      ok = (rate_hz >= ok_min_hz) AND (age_ms <= max_age_ms)
    """
    if now_ts_ms <= 0 or last_book_ts_ms <= 0:
        return BookHealth(
            ok=0, state="ERR",
            rate_hz=float(rate_hz), ok_min_hz=float(ok_min_hz), crit_hz=float(crit_hz),
            age_ms=10**9, max_age_ms=int(age_floor_ms),
            reason="no_ts",
        )
    age_ms = max(0, int(now_ts_ms) - int(last_book_ts_ms))
    exp_dt = int(1000.0 / ok_min_hz) if ok_min_hz > 0 else 10**9
    max_age_ms = max(int(age_floor_ms), int(age_mult * float(exp_dt)))

    ok = int((rate_hz >= ok_min_hz) and (age_ms <= max_age_ms))
    if ok == 1:
        return BookHealth(ok=1, state="OK", rate_hz=float(rate_hz), ok_min_hz=float(ok_min_hz), crit_hz=float(crit_hz),
                          age_ms=age_ms, max_age_ms=max_age_ms, reason="ok")

    # WARN/ERR classification for observability (gate is purely on ok)
    if rate_hz < crit_hz:
        return BookHealth(ok=0, state="ERR", rate_hz=float(rate_hz), ok_min_hz=float(ok_min_hz), crit_hz=float(crit_hz),
                          age_ms=age_ms, max_age_ms=max_age_ms, reason="rate_below_crit")
    if age_ms > max_age_ms:
        return BookHealth(ok=0, state="WARN", rate_hz=float(rate_hz), ok_min_hz=float(ok_min_hz), crit_hz=float(crit_hz),
                          age_ms=age_ms, max_age_ms=max_age_ms, reason="age_too_high")
    return BookHealth(ok=0, state="WARN", rate_hz=float(rate_hz), ok_min_hz=float(ok_min_hz), crit_hz=float(crit_hz),
                      age_ms=age_ms, max_age_ms=max_age_ms, reason="rate_below_ok_min")
