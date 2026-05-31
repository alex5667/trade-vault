from __future__ import annotations

import math
import os
from typing import Any, Dict, Optional

from domain.time_utils import normalize_ts_ms, session_from_ts_ms


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _boolish(x: Any) -> bool:
    """
    Accepts 1/"1"/true/"true"/yes/on.
    Used to match StatsAggregator final-close rules without importing internal helpers.
    """
    try:
        if isinstance(x, bool):
            return bool(x)
        if x is None:
            return False
        if isinstance(x, (int, float)):
            return int(x) != 0
        s = str(x).strip().lower()
        return s in {"1", "true", "yes", "on"}
    except Exception:
        return False


def _set_nx_compat(redis_client: Any, key: str, value: str, ttl_s: int) -> bool:
    """
    Cross-compat for redis-py and FakeRedis.
    Must return True iff key was set (NX).
    """
    if redis_client is None:
        return False
    try:
        ok = redis_client.set(key, value, nx=True, ex=int(ttl_s))
        # redis-py: True/None; FakeRedis often returns bool
        return bool(ok)
    except TypeError:
        try:
            ok = redis_client.set(key, value, ex=int(ttl_s), nx=True)
            return bool(ok)
        except Exception:
            return False
    except Exception:
        return False


def _hset_compat(redis_client: Any, key: str, mapping: Dict[str, Any]) -> None:
    if redis_client is None:
        return
    try:
        redis_client.hset(key, mapping=mapping)
        return
    except TypeError:
        pass
    try:
        redis_client.hset(key, mapping)
    except Exception:
        return


def _expire_compat(redis_client: Any, key: str, ttl_s: int) -> None:
    if redis_client is None:
        return
    try:
        fn = getattr(redis_client, "expire", None)
        if callable(fn):
            fn(key, int(ttl_s))
    except Exception:
        pass


def update_slippage_ema(
    redis_client: Any,
    *,
    closed: Dict[str, Any],
    pos: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Writer for:
      EMA(realized_slippage_bps) per (symbol×venue×session×tf×kind)

    Why:
      EdgeCostGate reads:
        slipema:v2:{symbol}:{venue}:{session}:{tf}:{kind}
      and uses:
        slippage_bps = max(default, spread/2, EMA)

    Fail-open:
      - any errors -> do nothing

    Backward compatible:
      - we also keep legacy v1 key updated (symbol×venue×session)
        so old readers still work.
    """
    try:
        if redis_client is None:
            return
        if not closed:
            return
        # Only final-close trades must affect execution-quality statistics.
        # Otherwise we may learn from partial closes and poison EMA.
        if not _boolish(closed.get("is_final_close", True)):
            return

        # Per-trade dedupe for this writer.
        # StatsAggregator core has Lua-dedupe, but its finally always runs even when applied==0.
        # Without this NX guard, repeated update_stats(...) calls would double-count EMA.
        order_id = str(closed.get("order_id") or closed.get("orderId") or closed.get("id") or "")
        exit_ts = int(_safe_float(closed.get("exit_ts_ms") or closed.get("closed_time") or closed.get("close_time") or 0, 0.0))
        if order_id and exit_ts > 0:
            ttl_dedupe = int(_safe_float(os.getenv("SLIPPAGE_WRITER_DEDUPE_TTL_S", "604800"), 604800))  # 7d
            dk = f"dedupe:slipema:v1:{order_id}:{exit_ts}"
            if not _set_nx_compat(redis_client, dk, "1", ttl_dedupe):
                return

        # input
        sym = str((closed or {}).get("symbol") or "").upper()
        if not sym:
            return

        tf = str((closed or {}).get("tf") or "na").lower()

        # "kind" may be dynamic field added into TradeClosed (recommended)
        knd = str((closed or {}).get("kind") or (closed or {}).get("strategy") or "na").lower()

        venue = str((closed or {}).get("venue") or (pos or {}).get("venue") or "na").lower()

        # session from entry_ts_ms (stable). if invalid -> "na"
        tsm = normalize_ts_ms((closed or {}).get("entry_ts_ms") or (closed or {}).get("exit_ts_ms") or 0)
        sess = "na"
        if tsm > 0:
            sess = str(session_from_ts_ms(int(tsm)) or "na").lower()

        slip = _safe_float((closed or {}).get("realized_slippage_bps"), 0.0)
        if slip <= 0:
            return

        # config
        alpha = _safe_float(os.getenv("SLIPPAGE_EMA_ALPHA", "0.05"), 0.05)
        if not (0.0 < alpha <= 1.0):
            alpha = 0.05
        ttl_s = int(_safe_float(os.getenv("SLIPPAGE_EMA_TTL_S", "604800"), 604800))  # 7d

        # keys
        k2 = f"slipema:v2:{sym}:{venue}:{sess}:{tf}:{knd}"
        k1 = f"slipema:{sym}:{venue}:{sess}"

        def _read_hash(key: str) -> Dict[str, str]:
            try:
                h = redis_client.hgetall(key) or {}
            except Exception:
                return {}
            out: Dict[str, str] = {}
            for kk, vv in dict(h).items():
                ks = kk.decode("utf-8", errors="ignore") if isinstance(kk, (bytes, bytearray)) else str(kk)
                vs = vv.decode("utf-8", errors="ignore") if isinstance(vv, (bytes, bytearray)) else str(vv)
                out[ks] = vs
            return out

        def _upd(key: str) -> None:
            h = _read_hash(key)
            try:
                n = int(float(h.get("n") or h.get("samples") or 0))
            except Exception:
                n = 0
            try:
                ema = float(h.get("ema_bps") or h.get("ema_slippage_bps") or h.get("ema") or 0.0)
            except Exception:
                ema = 0.0

            if n <= 0 or ema <= 0:
                ema2 = float(slip)
                n2 = 1
            else:
                ema2 = (1.0 - alpha) * float(ema) + alpha * float(slip)
                n2 = n + 1

            _hset_compat(redis_client, key, {"n": n2, "ema_bps": float(ema2), "last_ts_ms": int(tsm or 0)})
            _expire_compat(redis_client, key, ttl_s)

        # update v2 + v1
        _upd(k2)
        _upd(k1)
    except Exception:
        return