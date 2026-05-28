"""Cross-asset breadth reader (P1.B, 2026-05-27).

Side-channel reader для `ctx:breadth:{SYMBOL}` + `ctx:breadth:global` HASH.
Producer: services/cross_asset_breadth_producer_v1.py.

Used by EntryPolicyGate (HTF_LONG_BIAS) and indicator-enricher to populate:
  market_breadth_ret_5m
  cg_rel_strength_btc_1h
  symbol_rel_strength_vs_btc_1m
  btc_ret_1m / btc_ret_5m

Fail-open: any error → returns empty dict.

ENV:
  BREADTH_READER_REDIS_URL      fallback REDIS_URL
  BREADTH_READER_STALE_SEC      default 120; older snapshot → ignored
  BREADTH_READER_CACHE_TTL_MS   default 5000 (in-memory cache to avoid hot Redis calls)
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_GLOBAL_KEY = "ctx:breadth:global"
_PER_SYM_PREFIX = "ctx:breadth:"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _redis_url() -> str:
    return (
        os.environ.get("BREADTH_READER_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or "redis://redis-worker-1:6379/0"
    )


_RC: Any = None
_RC_LOCK = threading.Lock()
_CACHE: dict[str, tuple[int, dict[str, float]]] = {}
_CACHE_LOCK = threading.Lock()


def _get_redis() -> Any:
    global _RC
    if _RC is not None:
        return _RC
    with _RC_LOCK:
        if _RC is None:
            try:
                import redis  # type: ignore
                _RC = redis.from_url(_redis_url(), decode_responses=True, socket_timeout=0.5)
            except Exception as e:
                logger.debug("breadth_reader: redis init fail (fail-open): %s", e)
                _RC = None
        return _RC


def _read_hash(rc: Any, key: str) -> dict[str, float]:
    try:
        raw = rc.hgetall(key)
    except Exception:
        return {}
    if not raw:
        return {}
    out: dict[str, float] = {}
    for k, v in raw.items():
        ks = k.decode() if isinstance(k, bytes) else k
        vs = v.decode() if isinstance(v, bytes) else v
        try:
            out[str(ks)] = float(vs)
        except (TypeError, ValueError):
            # non-numeric (source/ts_ms string)
            try:
                out[str(ks) + "_raw"] = vs  # type: ignore[assignment]
            except Exception:
                pass
    return out


def get_breadth_for_symbol(symbol: str) -> dict[str, float]:
    """Return breadth feature dict for `symbol`. Empty if stale/missing.

    Keys included (when fresh):
      market_breadth_ret_5m, btc_ret_1m, btc_ret_5m, btc_ret_1h,
      symbol_rel_strength_vs_btc_1m, cg_rel_strength_btc_1h
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {}

    cache_ttl_ms = int(os.environ.get("BREADTH_READER_CACHE_TTL_MS", "5000") or 5000)
    now_ms = _now_ms()
    with _CACHE_LOCK:
        cached = _CACHE.get(sym)
        if cached and (now_ms - cached[0]) < cache_ttl_ms:
            return dict(cached[1])

    rc = _get_redis()
    if rc is None:
        return {}

    stale_sec = int(os.environ.get("BREADTH_READER_STALE_SEC", "120") or 120)
    out: dict[str, float] = {}

    # Per-symbol
    per_sym = _read_hash(rc, _PER_SYM_PREFIX + sym)
    if per_sym:
        ts = int(per_sym.get("ts_ms") or 0)
        if ts > 0 and (now_ms - ts) <= stale_sec * 1000:
            for k in (
                "btc_ret_1m", "btc_ret_5m", "btc_ret_1h",
                "symbol_rel_strength_vs_btc_1m", "cg_rel_strength_btc_1h",
                "sym_ret_5m", "sym_ret_1m",
            ):
                if k in per_sym:
                    out[k] = per_sym[k]

    # Global
    glob = _read_hash(rc, _GLOBAL_KEY)
    if glob:
        ts = int(glob.get("ts_ms") or 0)
        if ts > 0 and (now_ms - ts) <= stale_sec * 1000:
            if "market_breadth_ret_5m" in glob:
                out["market_breadth_ret_5m"] = glob["market_breadth_ret_5m"]

    with _CACHE_LOCK:
        _CACHE[sym] = (now_ms, dict(out))
    return out


def reset_cache_for_tests() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()
