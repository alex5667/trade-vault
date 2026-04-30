import time


def get_epoch_ms() -> int:
    """Canonical UTC epoch milliseconds. Use this everywhere.

    Returns int(time.time() * 1000) — timezone-independent by definition.
    """
    return int(time.time() * 1000)


def get_ny_time_millis() -> int:
    """Deprecated alias for get_epoch_ms().

    Historically used pytz/America/New_York, but .timestamp() is always UTC-based
    so the timezone had no numeric effect on the returned epoch_ms value.
    Kept for backward compatibility; prefer get_epoch_ms() for new code.
    """
    return get_epoch_ms()
