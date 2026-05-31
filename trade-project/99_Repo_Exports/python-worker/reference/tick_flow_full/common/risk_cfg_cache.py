from __future__ import annotations

import time
from typing import Any


def resolve_risk_cfg_cached(
    *,
    resolver: Any,
    symbol: str,
    cache: dict[str, Any],
    cache_ts: dict[str, float] | None = None,
    ttl_sec: float = 0.0,
) -> Any:
    """
    Resolve RiskCfgResolver.resolve(symbol) with per-process cache.

    Why:
      - RiskCfgResolver pulls many ENV keys and normalizes values.
      - In hot paths (publish / gates / levels enrich) resolve() may be called 2-5 times per signal.
      - ENV обычно статичен во время жизни процесса -> безопасно кэшировать.

    TTL:
      - ttl_sec <= 0  => cache forever (default).
      - ttl_sec > 0   => refresh if older than ttl_sec (useful for experiments without restart).

    Contract:
      - fail-open: never raises
      - returns dict-like (if resolver returns dict); otherwise returns raw value
      - stores a shallow copy for dicts to prevent accidental mutation leaks
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        sym = (symbol or "")

    try:
        if ttl_sec and ttl_sec > 0.0 and isinstance(cache_ts, dict):
            ts = float(cache_ts.get(sym, 0.0) or 0.0)
            if sym in cache and (time.time() - ts) < float(ttl_sec):
                return cache[sym]
        else:
            if sym in cache:
                return cache[sym]
    except Exception:
        # if cache structures are broken, proceed to compute
        pass

    try:
        cfg = resolver.resolve(sym)  # type: ignore[attr-defined]
    except Exception:
        return {}

    try:
        if isinstance(cfg, dict):
            cache[sym] = dict(cfg)  # shallow copy
        else:
            cache[sym] = cfg
        if ttl_sec and ttl_sec > 0.0 and isinstance(cache_ts, dict):
            cache_ts[sym] = time.time()
    except Exception:
        pass
    return cache.get(sym, cfg)


def invalidate_risk_cfg_cache(
    *,
    cache: dict[str, Any],
    cache_ts: dict[str, float] | None = None,
    symbol: str | None = None,
) -> None:
    """
    Explicit invalidation hook (optional, for live tuning).
    """
    try:
        if symbol:
            sym = symbol.strip().upper()
            cache.pop(sym, None)
            if isinstance(cache_ts, dict):
                cache_ts.pop(sym, None)
        else:
            cache.clear()
            if isinstance(cache_ts, dict):
                cache_ts.clear()
    except Exception:
        return
