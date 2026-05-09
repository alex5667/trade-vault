from __future__ import annotations

import os
from typing import Any


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default)) or default))
    except Exception:
        return default


def _canon(x: Any) -> str:
    return ((x or "").strip().lower() or "na")


def write_empirical_time_buffers(
    redis_client: Any,
    *,
    kind: str,
    symbol: str,
    tf: str,
    regime: str,
    bucket_ms: int,
    mfe_bps: float | None,
    mae_bps: float | None,
) -> None:
    """
    Pushes (MFE@bucket, MAE@bucket) into Redis lists:
      statsbuf:{kind}:{symbol}:{tf}:{regime}:mfe_bps_t{bucket_ms}
      statsbuf:{kind}:{symbol}:{tf}:{regime}:mae_bps_t{bucket_ms}
      statsbuf:{kind}:{symbol}:{tf}:{regime}:alive_t{bucket_ms}   (sample counter)

    We store bps values (positive magnitudes).
    """
    if redis_client is None:
        return
    if not _env_bool("EMP_TIME_LEVELS_ENABLED", True):
        return

    buf_max = max(50, _env_int("EMP_TIME_LEVELS_BUF_MAX", 300))
    buf_ttl = max(0, _env_int("EMP_TIME_LEVELS_BUF_TTL_SEC", 7 * 24 * 3600))

    k = _canon(kind)
    s = ((symbol or "").strip().upper() or "NA")
    t = _canon(tf)
    r = _canon(regime)
    b = int(bucket_ms)
    if b <= 0:
        return

    key_mfe = f"statsbuf:{k}:{s}:{t}:{r}:mfe_bps_t{b}"
    key_mae = f"statsbuf:{k}:{s}:{t}:{r}:mae_bps_t{b}"
    key_alive = f"statsbuf:{k}:{s}:{t}:{r}:alive_t{b}"

    pipe = None
    try:
        pipe = redis_client.pipeline(transaction=False)
    except Exception:
        pipe = None

    def _lpush_trim_exp(key: str, val: str) -> None:
        client = pipe if pipe is not None else redis_client
        client.lpush(key, val)
        client.ltrim(key, 0, buf_max - 1)
        if buf_ttl > 0:
            try:
                client.expire(key, buf_ttl)
            except Exception:
                pass

    try:
        # Sample counter: push "1" so LLEN is usable as n (also trimmed).
        _lpush_trim_exp(key_alive, "1")
        if mfe_bps is not None and float(mfe_bps) > 0:
            _lpush_trim_exp(key_mfe, f"{float(mfe_bps):.8f}")
        if mae_bps is not None and float(mae_bps) > 0:
            _lpush_trim_exp(key_mae, f"{float(mae_bps):.8f}")
        if pipe is not None:
            pipe.execute()
    except Exception:
        try:
            if pipe is not None:
                pipe.reset()
        except Exception:
            pass


def write_empirical_trade_counter(
    redis_client: Any,
    *,
    kind: str,
    symbol: str,
    tf: str,
    regime: str,
) -> None:
    """
    Sliding-window denominator for survival-aware calibration:
      trades_key = statsbuf:{kind}:{symbol}:{tf}:{regime}:trades
    We LPUSH "1" and trim/ttl similarly to time buffers, so LLEN(trades) is stable.
    """
    if redis_client is None:
        return
    if not _env_bool("EMP_TIME_LEVELS_ENABLED", True):
        return
    buf_max = max(50, _env_int("EMP_TIME_LEVELS_BUF_MAX", 300))
    buf_ttl = max(0, _env_int("EMP_TIME_LEVELS_BUF_TTL_SEC", 7 * 24 * 3600))
    k = _canon(kind)
    s = ((symbol or "").strip().upper() or "NA")
    t = _canon(tf)
    r = _canon(regime)
    key_trades = f"statsbuf:{k}:{s}:{t}:{r}:trades"
    try:
        pipe = redis_client.pipeline(transaction=False)
    except Exception:
        pipe = None
    try:
        client = pipe if pipe is not None else redis_client
        client.lpush(key_trades, "1")
        client.ltrim(key_trades, 0, buf_max - 1)
        if buf_ttl > 0:
            try:
                client.expire(key_trades, buf_ttl)
            except Exception:
                pass
        if pipe is not None:
            pipe.execute()
    except Exception:
        try:
            if pipe is not None:
                pipe.reset()
        except Exception:
            pass
