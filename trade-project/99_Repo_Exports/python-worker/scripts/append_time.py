with open("common/time_utils.py", "a") as f:
    f.write('''
from dataclasses import dataclass

@dataclass(frozen=True)
class NormalizedTime:
    ts_ms: int
    src_unit: str  # 'ms'|'s'|'us'|'ns'|'unknown'
    ok: bool
    err: str = ""

def normalize_epoch_ms(
    x: Any,
    *,
    now_ms: Optional[int] = None,
    max_future_ms: int = 2 * 24 * 3600_000,
    max_past_ms: int = 10 * 365 * 24 * 3600_000,
) -> NormalizedTime:
    """
    Normalize epoch-like timestamp to epoch milliseconds.

    Heuristics:
    - < 1e11 -> seconds
    - 1e14..1e17 -> microseconds
    - >= 1e17 -> nanoseconds
    """
    if now_ms is None:
        now_ms = get_ny_time_millis()

    try:
        if x is None:
            return NormalizedTime(0, "unknown", False, "ts_missing")
        if isinstance(x, bool):
            return NormalizedTime(0, "unknown", False, "ts_bool")
        if isinstance(x, (int, float)):
            v = int(x)
        else:
            s = str(x).strip()
            if s == "":
                return NormalizedTime(0, "unknown", False, "ts_empty")
            v = int(float(s)) if "." in s else int(s)
    except Exception:
        return NormalizedTime(0, "unknown", False, "ts_parse")

    unit = "ms"
    ts_ms = v

    if ts_ms > 0 and ts_ms < 100_000_000_000:
        unit = "s"
        ts_ms *= 1000
    elif ts_ms >= 100_000_000_000_000 and ts_ms < 100_000_000_000_000_000:
        unit = "us"
        ts_ms //= 1000
    elif ts_ms >= 100_000_000_000_000_000:
        unit = "ns"
        ts_ms //= 1_000_000

    if ts_ms <= 0:
        return NormalizedTime(0, unit, False, "ts_nonpositive")
    if ts_ms > now_ms + max_future_ms:
        return NormalizedTime(ts_ms, unit, False, "ts_future")
    if ts_ms < now_ms - max_past_ms:
        return NormalizedTime(ts_ms, unit, False, "ts_too_old")

    return NormalizedTime(ts_ms, unit, True, "")
''')
