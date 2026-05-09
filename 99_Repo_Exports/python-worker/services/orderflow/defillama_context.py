from __future__ import annotations

"""DefiLlama normalized slow-context snapshot for macro/liquidity regime.

Redis contract
--------------
Key: ``ctx:defillama:<SYMBOL>``
Value: JSON object (written by Go DefiLlamaScheduler)

Design rules
------------
- Epoch milliseconds only.
- JSON numbers only (no Decimal / pandas dependency).
- Fail-open readers: bad payloads return None.
- NOT a tick trigger — slow context only.
"""

import json
import math
from dataclasses import asdict, dataclass
from typing import Any

SCHEMA_VERSION = 1
DEFAULT_CTX_PREFIX = "ctx:defillama:"


@dataclass(frozen=True)
class DefiLlamaContextSnapshot:
    schema_version: int
    symbol: str
    chain: str
    ts_ms: int

    stablecoin_mcap_total: float
    stablecoin_mcap_delta_1d: float
    stablecoin_mcap_delta_7d: float
    stablecoin_risk_regime: str

    chain_tvl_usd: float
    chain_tvl_delta_1d_pct: float

    dex_volume_24h_usd: float
    dex_volume_delta_1d_pct: float
    dex_volume_spike_z: float

    fees_24h_usd: float
    revenue_24h_usd: float
    fees_revenue_momentum: float

    defillama_perps_oi_usd: float
    defillama_perps_oi_delta_1d_pct: float

    quality_status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


# ---------------------------------------------------------------------------
# Small numeric helpers (dependency-free, deterministic)
# ---------------------------------------------------------------------------

def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _s(v: Any) -> str:
    return (v or "").strip()


def ctx_key(symbol: str, prefix: str = DEFAULT_CTX_PREFIX) -> str:
    return f"{prefix}{(symbol or '').upper()}"


def from_dict(payload: dict[str, Any]) -> DefiLlamaContextSnapshot | None:
    try:
        symbol = _s(payload.get("symbol")).upper()
        if not symbol:
            return None
        return DefiLlamaContextSnapshot(
            schema_version=int(payload.get("schema_version") or SCHEMA_VERSION),
            symbol=symbol,
            chain=_s(payload.get("chain")),
            ts_ms=int(payload.get("ts_ms") or 0),

            stablecoin_mcap_total=_f(payload.get("stablecoin_mcap_total")),
            stablecoin_mcap_delta_1d=_f(payload.get("stablecoin_mcap_delta_1d")),
            stablecoin_mcap_delta_7d=_f(payload.get("stablecoin_mcap_delta_7d")),
            stablecoin_risk_regime=_s(payload.get("stablecoin_risk_regime")) or "unknown",

            chain_tvl_usd=_f(payload.get("chain_tvl_usd")),
            chain_tvl_delta_1d_pct=_f(payload.get("chain_tvl_delta_1d_pct")),

            dex_volume_24h_usd=_f(payload.get("dex_volume_24h_usd")),
            dex_volume_delta_1d_pct=_f(payload.get("dex_volume_delta_1d_pct")),
            dex_volume_spike_z=_f(payload.get("dex_volume_spike_z")),

            fees_24h_usd=_f(payload.get("fees_24h_usd")),
            revenue_24h_usd=_f(payload.get("revenue_24h_usd")),
            fees_revenue_momentum=_f(payload.get("fees_revenue_momentum")),

            defillama_perps_oi_usd=_f(payload.get("defillama_perps_oi_usd")),
            defillama_perps_oi_delta_1d_pct=_f(payload.get("defillama_perps_oi_delta_1d_pct")),

            quality_status=_s(payload.get("quality_status")) or "UNKNOWN",
        )
    except Exception:
        return None


def from_json(raw: Any) -> DefiLlamaContextSnapshot | None:
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
# Async reader with local cache (2-second TTL) — fail-open
# ---------------------------------------------------------------------------

_LOCAL_CACHE: dict[str, tuple[int, DefiLlamaContextSnapshot | None]] = {}


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)


async def aread_defillama_context(
    redis, *, symbol: str, prefix: str = DEFAULT_CTX_PREFIX
) -> DefiLlamaContextSnapshot | None:
    if redis is None:
        return None
    now_ms = _now_ms()
    cache_key = ctx_key(symbol, prefix=prefix)

    # Check local cache (2-second TTL)
    if cache_key in _LOCAL_CACHE:
        ts, snap = _LOCAL_CACHE[cache_key]
        if now_ms - ts < 2000:
            return snap

    try:
        raw = await redis.get(cache_key)
        snap = from_json(raw)
        _LOCAL_CACHE[cache_key] = (now_ms, snap)
        return snap
    except Exception:
        return None
