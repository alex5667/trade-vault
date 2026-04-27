from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Normalized derivatives context for perpetual futures.

This module centralizes the *read/write contract* for cross-service derivatives
context consumed by runtime/orderflow gates.

Why this exists
---------------
The project already had partial funding ingestion (`funding_handler_impl.py`) and
public market helpers (`binance_futures_client.py`), but there was no single
normalized Redis snapshot that policy layers could consume deterministically.

This module defines that snapshot and provides:
- pure math helpers (`basis_bps`, `oi_notional_usd`, robust z)
- normalization of raw exchange payloads into a stable JSON contract
- async/sync Redis readers/writers (fail-open at call sites)

Redis contract
--------------
Key: ``ctx:deriv:<SYMBOL>``
Value: JSON object

Example::
    {
      "schema_version": 1,
      "symbol": "BTCUSDT",
      "ts_ms": 1731000000000,
      "venue": "binance",
      "funding_rate": 0.0001,
      "funding_rate_abs": 0.0001,
      "funding_rate_z": 2.3,
      "premium_index": 0.0008,
      "basis_bps": 8.0,
      "open_interest": 12345.0,
      "delta_oi_5m": 234.0,
      "oi_notional_usd": 123450000.0,
      "funding_extreme": 0,
      "basis_extreme": 0,
      "oi_accel": 0
    }

Design rules
------------
- Epoch milliseconds only.
- JSON numbers only (no Decimal / pandas dependency).
- Fail-open readers: bad payloads return None.
- Writers are explicit and deterministic.
"""

import json
import math
import time
from dataclasses import asdict, dataclass
from statistics import median
from typing import Any, Dict, Iterable, Optional, Sequence

SCHEMA_VERSION = 1
DEFAULT_CTX_PREFIX = "ctx:deriv:"


@dataclass(frozen=True)
class DerivativesContextSnapshot:
    schema_version: int
    symbol: str
    ts_ms: int
    venue: str
    funding_rate: float
    funding_rate_abs: float
    funding_rate_z: float
    premium_index: float
    basis_bps: float
    open_interest: float
    delta_oi_5m: float
    oi_notional_usd: float
    funding_extreme: int
    basis_extreme: int
    oi_accel: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


# ---------------------------------------------------------------------------
# Small numeric helpers (dependency-free, deterministic)
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return get_ny_time_millis()


def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _i(v: Any, d: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return d


def _s(v: Any) -> str:
    return str(v or "").strip()


def ctx_key(symbol: str, prefix: str = DEFAULT_CTX_PREFIX) -> str:
    return f"{prefix}{str(symbol or '').upper()}"


def basis_bps(*, mark_price: float, index_price: float) -> float:
    """Compute premium/basis in bps.

    We intentionally use mark vs index because that is the most stable public
    proxy available from Binance premiumIndex without requiring spot connector
    joins inside the hot path.
    """
    mark = _f(mark_price, 0.0)
    index_ = _f(index_price, 0.0)
    if mark <= 0.0 or index_ <= 0.0:
        return 0.0
    out = ((mark - index_) / index_) * 10_000.0
    return float(out) if math.isfinite(out) else 0.0


def oi_notional_usd(*, open_interest: float, mark_price: float) -> float:
    oi = _f(open_interest, 0.0)
    px = _f(mark_price, 0.0)
    if oi <= 0.0 or px <= 0.0:
        return 0.0
    out = oi * px
    return float(out) if math.isfinite(out) else 0.0


def robust_zscore(*, x: float, history: Sequence[float]) -> float:
    """Median/MAD-based z-score.

    Production rationale:
    - derivatives context is sparse / heavy-tailed;
    - mean/std explodes under exchange stress regimes;
    - median/MAD remains stable enough for gating.
    """
    xv = _f(x, 0.0)
    vals = [abs(_f(v, 0.0)) for v in (history or []) if math.isfinite(_f(v, 0.0))]
    if len(vals) < 5:
        return 0.0
    med = float(median(vals))
    mad = float(median([abs(v - med) for v in vals]))
    if mad <= 1e-12:
        return 0.0
    # 1.4826 = consistency constant for normal distribution.
    z = (abs(xv) - med) / (1.4826 * mad)
    if not math.isfinite(z):
        return 0.0
    return float(max(-50.0, min(50.0, z)))


def delta_open_interest(*, current_oi: float, previous_oi: float) -> float:
    cur = _f(current_oi, 0.0)
    prev = _f(previous_oi, 0.0)
    if cur <= 0.0 or prev <= 0.0:
        return 0.0
    out = cur - prev
    return float(out) if math.isfinite(out) else 0.0


def build_snapshot(
    *,
    symbol: str,
    ts_ms: int,
    venue: str,
    funding_rate: float,
    funding_history: Sequence[float],
    premium_index: float,
    mark_price: float,
    index_price: float,
    open_interest: float,
    previous_open_interest: float,
    funding_extreme_abs: float,
    basis_extreme_abs_bps: float,
    oi_accel_abs_usd: float,
) -> DerivativesContextSnapshot:
    """Build normalized derivatives snapshot from raw values.

    Inputs are intentionally already scalarized so collectors can source them
    from REST, Redis, caches, or synthetic replay fixtures.
    """
    fr = _f(funding_rate, 0.0)
    px_mark = _f(mark_price, 0.0)
    px_index = _f(index_price, 0.0)
    basis = basis_bps(mark_price=px_mark, index_price=px_index)
    oi = _f(open_interest, 0.0)
    doi = delta_open_interest(current_oi=oi, previous_oi=previous_open_interest)
    oi_usd = oi_notional_usd(open_interest=oi, mark_price=px_mark)
    doi_usd = abs(_f(doi, 0.0) * px_mark)
    fz = robust_zscore(x=fr, history=funding_history)

    funding_extreme = 1 if abs(fr) >= max(_f(funding_extreme_abs, 0.0), 1e-12) or fz >= 3.0 else 0
    basis_extreme = 1 if abs(basis) >= max(_f(basis_extreme_abs_bps, 0.0), 1e-12) else 0
    oi_accel = 1 if doi_usd >= max(_f(oi_accel_abs_usd, 0.0), 1e-12) and doi != 0.0 else 0

    return DerivativesContextSnapshot(
        schema_version=SCHEMA_VERSION,
        symbol=str(symbol or "").upper(),
        ts_ms=int(ts_ms or _now_ms()),
        venue=str(venue or "binance").lower(),
        funding_rate=float(fr),
        funding_rate_abs=float(abs(fr)),
        funding_rate_z=float(fz),
        premium_index=float(_f(premium_index, 0.0)),
        basis_bps=float(basis),
        open_interest=float(oi),
        delta_oi_5m=float(doi),
        oi_notional_usd=float(oi_usd),
        funding_extreme=int(funding_extreme),
        basis_extreme=int(basis_extreme),
        oi_accel=int(oi_accel),
    )


def from_dict(payload: Dict[str, Any]) -> Optional[DerivativesContextSnapshot]:
    try:
        symbol = _s(payload.get("symbol")).upper()
        if not symbol:
            return None
        return DerivativesContextSnapshot(
            schema_version=_i(payload.get("schema_version"), SCHEMA_VERSION),
            symbol=symbol,
            ts_ms=_i(payload.get("ts_ms"), 0),
            venue=_s(payload.get("venue")) or "binance",
            funding_rate=_f(payload.get("funding_rate"), 0.0),
            funding_rate_abs=_f(payload.get("funding_rate_abs"), 0.0),
            funding_rate_z=_f(payload.get("funding_rate_z"), 0.0),
            premium_index=_f(payload.get("premium_index"), 0.0),
            basis_bps=_f(payload.get("basis_bps"), 0.0),
            open_interest=_f(payload.get("open_interest"), 0.0),
            delta_oi_5m=_f(payload.get("delta_oi_5m"), 0.0),
            oi_notional_usd=_f(payload.get("oi_notional_usd"), 0.0),
            funding_extreme=_i(payload.get("funding_extreme"), 0),
            basis_extreme=_i(payload.get("basis_extreme"), 0),
            oi_accel=_i(payload.get("oi_accel"), 0),
        )
    except Exception:
        return None


def from_json(raw: Any) -> Optional[DerivativesContextSnapshot]:
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


async def aread_derivatives_context(redis, *, symbol: str, prefix: str = DEFAULT_CTX_PREFIX) -> Optional[DerivativesContextSnapshot]:
    if redis is None:
        return None
    try:
        raw = await redis.get(ctx_key(symbol, prefix=prefix))
    except Exception:
        return None
    return from_json(raw)


def read_derivatives_context_sync(redis, *, symbol: str, prefix: str = DEFAULT_CTX_PREFIX) -> Optional[DerivativesContextSnapshot]:
    if redis is None:
        return None
    try:
        raw = redis.get(ctx_key(symbol, prefix=prefix))
    except Exception:
        return None
    return from_json(raw)


async def awrite_derivatives_context(redis, snap: DerivativesContextSnapshot, *, ttl_s: int = 180, prefix: str = DEFAULT_CTX_PREFIX) -> bool:
    if redis is None:
        return False
    try:
        await redis.set(ctx_key(snap.symbol, prefix=prefix), snap.to_json(), ex=int(ttl_s))
        return True
    except Exception:
        return False


def write_derivatives_context_sync(redis, snap: DerivativesContextSnapshot, *, ttl_s: int = 180, prefix: str = DEFAULT_CTX_PREFIX) -> bool:
    if redis is None:
        return False
    try:
        redis.set(ctx_key(snap.symbol, prefix=prefix), snap.to_json(), ex=int(ttl_s))
        return True
    except Exception:
        return False


def partial_funding_payload_from_exchange(payload: Dict[str, Any], *, venue: str = "binance", now_ms: Optional[int] = None) -> Dict[str, Any]:
    """Normalize a funding stream payload into a minimal partial context payload.

    This helper is intentionally limited to fields that can realistically arrive
    from a funding stream. It does not invent OI/basis if those values are not
    present. The collector can merge this partial payload with REST polling.
    """
    obj = dict(payload or {})
    symbol = _s(obj.get("symbol")).upper()
    ts_ms = _i(obj.get("time") or obj.get("fundingTime") or obj.get("ts_ms") or now_ms or _now_ms())
    rate = _f(obj.get("lastFundingRate", obj.get("fundingRate", 0.0)), 0.0)
    premium = _f(obj.get("lastFundingRate", 0.0), 0.0) if "premiumIndex" not in obj else _f(obj.get("premiumIndex"), 0.0)
    return {
        "schema_version": SCHEMA_VERSION,
        "symbol": symbol,
        "ts_ms": ts_ms,
        "venue": str(venue or "binance").lower(),
        "funding_rate": float(rate),
        "funding_rate_abs": float(abs(rate)),
        "funding_rate_z": 0.0,
        "premium_index": float(premium),
        "basis_bps": 0.0,
        "open_interest": 0.0,
        "delta_oi_5m": 0.0,
        "oi_notional_usd": 0.0,
        "funding_extreme": 0,
        "basis_extreme": 0,
        "oi_accel": 0,
    }
