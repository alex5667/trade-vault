from __future__ import annotations

"""Cross-venue normalized snapshot reader.

Redis contract
--------------
Key:   ``ctx:crossvenue:{SYMBOL}``
Value: JSON object (written by Go CrossVenueAggregator every ~1 s, TTL 10 s)

Design rules
------------
- Epoch milliseconds only.
- JSON numbers only — no Decimal / pandas.
- Fail-open: bad payloads → None.
- NOT a tick trigger — per-signal context read, 2-second local cache.
- schema_version=1 — Python reader is forward-compatible.
"""

import json
import math
import time
from dataclasses import dataclass
from typing import Any

SCHEMA_VERSION = 1
DEFAULT_CTX_PREFIX = "ctx:crossvenue:"


# ---------------------------------------------------------------------------
# Snapshot dataclass (immutable)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CrossVenueContextSnapshot:
    schema_version: int
    symbol: str
    ts_ms: int
    primary_venue: str

    # Per-venue mid-price diffs (bps, signed: positive = Binance is higher)
    cross_venue_mid_spread_bps: float      # max - min across all active venues
    binance_vs_coinbase_mid_bps: float     # (binance - coinbase) / coinbase * 10000
    binance_vs_kraken_mid_bps: float       # (binance - kraken) / kraken * 10000
    binance_vs_okx_mid_bps: float          # (binance - okx) / okx * 10000

    # Market-quality signals
    cross_venue_direction_agree: float     # fraction of venues agreeing on direction [0, 1]
    cross_venue_trade_imbalance: float     # (buy_notional - sell_notional) / total [-1, 1]
    venue_dislocation_z: float             # robust-z of |binance - median_external| bps
    venue_stale_count: int                 # number of stale venues at snapshot time
    quality_status: str                    # "OK" | "DEGRADED" | "STALE" | "UNKNOWN"


# ---------------------------------------------------------------------------
# Small numeric helpers — deterministic, dependency-free
# ---------------------------------------------------------------------------

def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _s(v: Any, d: str = "") -> str:
    return str(v or d).strip()


def ctx_key(symbol: str, prefix: str = DEFAULT_CTX_PREFIX) -> str:
    return f"{prefix}{(symbol or '').upper()}"


# ---------------------------------------------------------------------------
# Deserialisation helpers
# ---------------------------------------------------------------------------

def from_dict(payload: dict[str, Any]) -> CrossVenueContextSnapshot | None:
    try:
        symbol = _s(payload.get("symbol")).upper()
        if not symbol:
            return None
        return CrossVenueContextSnapshot(
            schema_version=int(payload.get("schema_version") or SCHEMA_VERSION),
            symbol=symbol,
            ts_ms=int(payload.get("ts_ms") or 0),
            primary_venue=_s(payload.get("primary_venue"), "binance_usdm"),

            cross_venue_mid_spread_bps=_f(payload.get("cross_venue_mid_spread_bps")),
            binance_vs_coinbase_mid_bps=_f(payload.get("binance_vs_coinbase_mid_bps")),
            binance_vs_kraken_mid_bps=_f(payload.get("binance_vs_kraken_mid_bps")),
            binance_vs_okx_mid_bps=_f(payload.get("binance_vs_okx_mid_bps")),

            cross_venue_direction_agree=_f(payload.get("cross_venue_direction_agree")),
            cross_venue_trade_imbalance=_f(payload.get("cross_venue_trade_imbalance")),
            venue_dislocation_z=_f(payload.get("venue_dislocation_z")),
            venue_stale_count=int(payload.get("venue_stale_count") or 0),
            quality_status=_s(payload.get("quality_status"), "UNKNOWN") or "UNKNOWN",
        )
    except Exception:
        return None


def from_json(raw: Any) -> CrossVenueContextSnapshot | None:
    """Parse JSON bytes / str / dict → snapshot. Returns None on any error."""
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


# ---------------------------------------------------------------------------
# Async reader with 2-second local cache (per symbol, fail-open)
# ---------------------------------------------------------------------------

_LOCAL_CACHE: dict[str, tuple[int, CrossVenueContextSnapshot | None]] = {}
_CACHE_TTL_MS = 2_000


def _now_ms() -> int:
    return int(time.monotonic() * 1000)


async def aread_crossvenue_context(
    redis,
    *,
    symbol: str,
    prefix: str = DEFAULT_CTX_PREFIX,
) -> CrossVenueContextSnapshot | None:
    """Read cross-venue context from Redis, with 2-second local cache.

    Fail-open: returns None on Redis unavailability or parse errors.
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
