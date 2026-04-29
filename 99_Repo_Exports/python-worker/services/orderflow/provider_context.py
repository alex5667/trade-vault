from __future__ import annotations

"""ProviderContextSnapshot — slow market-context reader (CoinPaprika / CMC fallback).

Redis contract
--------------
Key:   ``ctx:provider:{SYMBOL}``  (JSON, written by Go ProviderFallbackScheduler, TTL 300s)

Design rules
------------
- Epoch milliseconds only.
- Fail-open: bad payloads → None; missing provider → None (not an error).
- NOT a tick trigger — slow context only (5m–15m cadence).
- schema_version=1 — forward-compatible.
"""

import json
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

SCHEMA_VERSION = 1
DEFAULT_CTX_PREFIX = "ctx:provider:"
_CACHE_TTL_MS = 5_000  # 5-second local cache


@dataclass(frozen=True)
class ProviderContextSnapshot:
    schema_version: int
    symbol: str
    ts_ms: int

    provider_global_mcap: float
    provider_total_volume: float
    provider_btc_dominance: float
    provider_eth_dominance: float

    mcap_disagreement_bps: float
    volume_disagreement_bps: float
    btc_dom_disagreement_bps: float

    provider_quality: str        # "ok" | "degraded" | "fallback" | "unknown"
    provider_top_gainer: int     # 1 if symbol is top gainer 24h, 0 otherwise
    provider_top_loser: int      # 1 if symbol is top loser 24h, 0 otherwise
    provider_rel_strength_24h: float
    provider_volume_mcap_ratio: float

    quality_status: str


# ─── Numeric helpers ──────────────────────────────────────────────────────────

def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _s(v: Any, d: str = "") -> str:
    return str(v or d).strip()


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return d


# ─── Deserialisation ──────────────────────────────────────────────────────────

def ctx_key(symbol: str, prefix: str = DEFAULT_CTX_PREFIX) -> str:
    return f"{prefix}{str(symbol or '').upper()}"


def from_dict(payload: Dict[str, Any]) -> Optional[ProviderContextSnapshot]:
    try:
        symbol = _s(payload.get("symbol")).upper()
        if not symbol:
            return None
        return ProviderContextSnapshot(
            schema_version=_i(payload.get("schema_version"), SCHEMA_VERSION),
            symbol=symbol,
            ts_ms=_i(payload.get("ts_ms"), 0),
            provider_global_mcap=_f(payload.get("provider_global_mcap")),
            provider_total_volume=_f(payload.get("provider_total_volume")),
            provider_btc_dominance=_f(payload.get("provider_btc_dominance")),
            provider_eth_dominance=_f(payload.get("provider_eth_dominance")),
            mcap_disagreement_bps=_f(payload.get("mcap_disagreement_bps")),
            volume_disagreement_bps=_f(payload.get("volume_disagreement_bps")),
            btc_dom_disagreement_bps=_f(payload.get("btc_dom_disagreement_bps")),
            provider_quality=_s(payload.get("provider_quality"), "unknown"),
            provider_top_gainer=_i(payload.get("provider_top_gainer"), 0),
            provider_top_loser=_i(payload.get("provider_top_loser"), 0),
            provider_rel_strength_24h=_f(payload.get("provider_rel_strength_24h")),
            provider_volume_mcap_ratio=_f(payload.get("provider_volume_mcap_ratio")),
            quality_status=_s(payload.get("quality_status"), "UNKNOWN"),
        )
    except Exception:
        return None


def from_json(raw: Any) -> Optional[ProviderContextSnapshot]:
    try:
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if isinstance(raw, str):
            raw = json.loads(raw)
        if not isinstance(raw, dict):
            return None
        return from_dict(raw)
    except Exception:
        return None


# ─── Async reader with 5-second local cache — fail-open ──────────────────────

_LOCAL_CACHE: Dict[str, Tuple[int, Optional[ProviderContextSnapshot]]] = {}


def _now_ms() -> int:
    return int(time.monotonic() * 1000)


async def aread_provider_context(
    redis,
    *,
    symbol: str,
    prefix: str = DEFAULT_CTX_PREFIX,
) -> Optional[ProviderContextSnapshot]:
    """Read provider context from Redis with 5-second local cache.

    Fail-open: returns None on Redis errors, parse errors, or missing key.
    Missing provider is NOT an error — pipeline runs without it.
    """
    if redis is None:
        return None

    now_ms = _now_ms()
    key = ctx_key(symbol, prefix=prefix)

    cached = _LOCAL_CACHE.get(key)
    if cached is not None:
        cached_ts, snap = cached
        if now_ms - cached_ts < _CACHE_TTL_MS:
            return snap

    try:
        raw = await redis.get(key)
        snap = from_json(raw)
        _LOCAL_CACHE[key] = (now_ms, snap)
        return snap
    except Exception:
        return None
