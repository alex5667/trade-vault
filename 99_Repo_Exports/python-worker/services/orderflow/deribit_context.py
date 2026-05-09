from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DeribitContextSnapshot:
    schema_version: int
    symbol: str
    currency: str
    ts_ms: int
    btc_options_oi_proxy: float
    eth_options_oi_proxy: float
    deribit_iv_proxy: float
    deribit_iv_z: float
    deribit_funding_8h: float
    deribit_perp_basis_bps: float
    btc_eth_vol_regime: str
    quality_status: str


def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


async def aread_deribit_context(
    redis, *, symbol: str
) -> DeribitContextSnapshot | None:
    """
    Read Deribit volatility context snapshot from Redis.

    For BTCUSDT/ETHUSDT → reads ctx:deribit:{symbol} (symbol-specific snapshot).
    For all other symbols → reads ctx:deribit:global (BTC/ETH leader context).

    Returns None on any error (fail-open: missing Deribit is not a blocker).
    """
    if redis is None:
        return None

    sym = (symbol or "").upper()
    key = f"ctx:deribit:{sym}" if sym in {"BTCUSDT", "ETHUSDT"} else "ctx:deribit:global"

    try:
        raw = await redis.get(key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        obj = json.loads(raw)
    except Exception:
        return None

    if not isinstance(obj, dict):
        return None

    # Resolve iv_proxy and iv_z: symbol-level keys take precedence over global keys.
    iv_proxy = _f(
        obj.get("deribit_iv_proxy")
        or obj.get("btc_deribit_iv_proxy")
        or obj.get("eth_deribit_iv_proxy")
    )
    iv_z = _f(
        obj.get("deribit_iv_z")
        or obj.get("btc_deribit_iv_z")
        or obj.get("eth_deribit_iv_z")
    )
    funding_8h = _f(
        obj.get("deribit_funding_8h")
        or obj.get("btc_deribit_funding_8h")
        or obj.get("eth_deribit_funding_8h")
    )

    return DeribitContextSnapshot(
        schema_version=_i(obj.get("schema_version"), 1),
        symbol=str(obj.get("symbol") or sym),
        currency=(obj.get("currency") or ""),
        ts_ms=_i(obj.get("ts_ms")),
        btc_options_oi_proxy=_f(obj.get("btc_options_oi_proxy")),
        eth_options_oi_proxy=_f(obj.get("eth_options_oi_proxy")),
        deribit_iv_proxy=iv_proxy,
        deribit_iv_z=iv_z,
        deribit_funding_8h=funding_8h,
        deribit_perp_basis_bps=_f(obj.get("deribit_perp_basis_bps")),
        btc_eth_vol_regime=(obj.get("btc_eth_vol_regime") or "unknown"),
        quality_status=(obj.get("quality_status") or "UNKNOWN"),
    )
