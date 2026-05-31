from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Inline execution-health state for hot/warm path feedback.

Purpose
-------
Use *freshly observed* entry fills to maintain a lightweight rolling view of
Implementation Shortfall (IS) per trading bucket so that the next signal can be
penalized before post-trade hourly rollups catch up.

Design
------
1. ``tca_worker`` already scans fills and joins them with decision snapshots.
   We reuse that warm-path to update a compact Redis cache.
2. For every entry fill we accumulate per-``sid`` fill state (VWAP + fee USD).
3. The latest cumulative IS for each ``sid`` becomes one sample in a bounded
   rolling window keyed by ``symbol/side/session/kind/tf``.
4. From that bounded sample set we derive:
   - ema_bps
   - p50_bps
   - p95_bps
   - count
5. ``EdgeCostGate`` reads those rollups synchronously and can monitor / tighten /
   veto based on *very recent* execution degradation.

Why keep one sample per sid instead of one sample per partial fill?
------------------------------------------------------------------
Partial fills should improve the *current trade* VWAP, not overweight the same
trade multiple times in the rolling percentile. Therefore we overwrite the
sample for the same ``sid`` with the latest cumulative VWAP-based IS.

Key layout
----------
Session-aware primary keys:
  - exec:inline:is:<SYM>:<SIDE>:<SESSION>:<KIND>:<TF>

Compatibility / aggregate mirror keys (latest session):
  - exec:inline:is:<SYM>:<SIDE>:<KIND>:<TF>

Internal warm-path state:
  - exec:inline:is:sid:<SID>
  - exec:inline:is:samples:<SYM>:<SIDE>:<SESSION>:<KIND>:<TF>
  - exec:inline:is:index:<SYM>:<SIDE>:<SESSION>:<KIND>:<TF>

The module is intentionally fail-open: malformed values simply skip updates.
"""

import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from services.posttrade.tca_math import implementation_shortfall_bps


def _now_ms() -> int:
    return get_ny_time_millis()


def _s(v: Any, default: str = "") -> str:
    try:
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "replace")
        s = (v or "").strip()
        return s if s else default
    except Exception:
        return default


def _f(v: Any, default: float = float("nan")) -> float:
    try:
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "replace")
        x = float(v)
    except Exception:
        return default
    if not math.isfinite(x):
        return default
    return float(x)


def _i(v: Any, default: int = 0) -> int:
    try:
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "replace")
        return int(float(v))
    except Exception:
        return default


def _decode(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "replace")
    return str(v)


def _side(v: Any) -> str:
    s = _s(v, "NA").upper()
    if s in {"BUY", "LONG"}:
        return "LONG"
    if s in {"SELL", "SHORT"}:
        return "SHORT"
    return s or "NA"


def _sess(v: Any) -> str:
    return _s(v, "na").lower()


def _kind(v: Any) -> str:
    return _s(v, "na").lower()


def _tf(v: Any) -> str:
    return _s(v, "na").lower()


@dataclass(frozen=True)
class InlineExecDims:
    symbol: str
    side: str
    session: str
    kind: str
    tf: str

    def norm(self) -> InlineExecDims:
        return InlineExecDims(
            symbol=_s(self.symbol).upper(),
            side=_side(self.side),
            session=_sess(self.session),
            kind=_kind(self.kind),
            tf=_tf(self.tf),
        )


def make_rollup_key(dims: InlineExecDims, *, include_session: bool = True) -> str:
    d = dims.norm()
    if include_session:
        return f"exec:inline:is:{d.symbol}:{d.side}:{d.session}:{d.kind}:{d.tf}"
    return f"exec:inline:is:{d.symbol}:{d.side}:{d.kind}:{d.tf}"


def make_samples_key(dims: InlineExecDims) -> str:
    return f"exec:inline:is:samples:{make_rollup_key(dims, include_session=True).split('exec:inline:is:', 1)[1]}"


def make_index_key(dims: InlineExecDims) -> str:
    return f"exec:inline:is:index:{make_rollup_key(dims, include_session=True).split('exec:inline:is:', 1)[1]}"


def make_sid_state_key(sid: str) -> str:
    return f"exec:inline:is:sid:{_s(sid)}"


def _percentile(sorted_vals: Sequence[float], q: float) -> float:
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    pos = max(0.0, min(1.0, float(q))) * (len(sorted_vals) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_vals[lo])
    w = float(pos - lo)
    return float(sorted_vals[lo] * (1.0 - w) + sorted_vals[hi] * w)


def _ema_from_ordered(vals: Sequence[float], *, alpha: float) -> float:
    if not vals:
        return float("nan")
    aa = min(1.0, max(0.001, float(alpha)))
    ema = float(vals[0])
    for v in vals[1:]:
        ema = aa * float(v) + (1.0 - aa) * ema
    return float(ema)


def _json_load(v: Any) -> dict[str, Any]:
    try:
        raw = _decode(v)
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}


def _json_dump(v: dict[str, Any]) -> str:
    return json.dumps(v, separators=(",", ":"), sort_keys=True)


def inline_is_from_cumulative_state(*, decision_mid: float, side: str, cum_notional: float, cum_qty: float, cum_fee_usd: float) -> float | None:
    """Compute cumulative VWAP-based IS from a partially filled entry order.

    ``implementation_shortfall_bps`` expects fill VWAP and fee_bps. We derive the
    latter from cumulative fee USD over cumulative notional.
    """
    mid0 = _f(decision_mid)
    qty = _f(cum_qty)
    notional = _f(cum_notional)
    fee_usd = _f(cum_fee_usd, 0.0)
    if not math.isfinite(mid0) or mid0 <= 0:
        return None
    if not math.isfinite(qty) or qty <= 0:
        return None
    if not math.isfinite(notional) or notional <= 0:
        return None
    vwap = float(notional) / float(qty)
    fee_bps = 0.0
    if math.isfinite(fee_usd) and fee_usd > 0:
        fee_bps = float(fee_usd) / float(notional) * 10_000.0
    v = implementation_shortfall_bps(
        vwap_fill_px=float(vwap),
        decision_mid=float(mid0),
        side=_side(side),
        fee_bps=float(fee_bps),
    )
    if v is None or not math.isfinite(float(v)):
        return None
    return float(v)


async def update_inline_exec_from_fill(
    *,
    redis: Any,
    sid: str,
    dims: InlineExecDims,
    decision_mid: float,
    fill_px: float,
    fill_qty: float,
    fee_bps: float,
    ts_fill_ms: int,
    ttl_sec: int = 86_400,
    max_samples: int = 128,
    ema_alpha: float = 0.2,
) -> dict[str, float]:
    """Update bounded Redis state from one entry fill and return current rollup.

    The function is safe to call repeatedly for partial fills of the same sid.
    The latest cumulative IS overwrites the sample for this sid.
    """
    if redis is None:
        return {}
    sid_s = _s(sid)
    if not sid_s:
        return {}
    d = dims.norm()
    px = _f(fill_px)
    qty = _f(fill_qty)
    fee = _f(fee_bps, 0.0)
    mid0 = _f(decision_mid)
    if not (math.isfinite(px) and px > 0 and math.isfinite(qty) and qty > 0 and math.isfinite(mid0) and mid0 > 0):
        return {}

    sid_key = make_sid_state_key(sid_s)
    state_raw = await redis.hgetall(sid_key)
    state: dict[str, Any] = {_decode(k): _decode(v) for k, v in (state_raw or {}).items()}

    cum_qty = _f(state.get("cum_qty"), 0.0) + float(qty)
    cum_notional = _f(state.get("cum_notional"), 0.0) + (float(px) * float(qty))
    cum_fee_usd = _f(state.get("cum_fee_usd"), 0.0) + (max(0.0, float(fee)) * float(px) * float(qty) / 10_000.0)
    inline_is = inline_is_from_cumulative_state(
        decision_mid=float(mid0),
        side=d.side,
        cum_notional=float(cum_notional),
        cum_qty=float(cum_qty),
        cum_fee_usd=float(cum_fee_usd),
    )
    if inline_is is None:
        return {}

    sid_state = {
        "sid": sid_s,
        "symbol": d.symbol,
        "side": d.side,
        "session": d.session,
        "kind": d.kind,
        "tf": d.tf,
        "decision_mid": float(mid0),
        "cum_qty": float(cum_qty),
        "cum_notional": float(cum_notional),
        "cum_fee_usd": float(cum_fee_usd),
        "last_fill_ms": int(ts_fill_ms),
        "last_inline_is_bps": float(inline_is),
        "updated_at_ms": _now_ms(),
    }
    await redis.hset(sid_key, mapping={k: str(v) for k, v in sid_state.items()})
    await redis.expire(sid_key, int(ttl_sec))

    samples_key = make_samples_key(d)
    index_key = make_index_key(d)
    sample_payload = {
        "sid": sid_s,
        "is_bps": float(inline_is),
        "ts_ms": int(ts_fill_ms),
    }
    await redis.hset(samples_key, mapping={sid_s: _json_dump(sample_payload)})
    await redis.expire(samples_key, int(ttl_sec))
    await redis.zadd(index_key, {sid_s: int(ts_fill_ms)})
    await redis.expire(index_key, int(ttl_sec))

    try:
        zcard = int(await redis.zcard(index_key))
    except Exception:
        zcard = 0
    if zcard > int(max_samples):
        extra = zcard - int(max_samples)
        try:
            old = await redis.zrange(index_key, 0, extra - 1)
        except Exception:
            old = []
        if old:
            await redis.zrem(index_key, *old)
            await redis.hdel(samples_key, *old)

    ids = await redis.zrange(index_key, 0, -1)
    series: list[tuple[int, float]] = []
    for raw_sid in ids or []:
        raw = await redis.hget(samples_key, raw_sid)
        obj = _json_load(raw)
        v = _f(obj.get("is_bps"))
        ts = _i(obj.get("ts_ms"), 0)
        if math.isfinite(v):
            series.append((int(ts), float(v)))

    series.sort(key=lambda x: x[0])
    vals = [float(v) for _, v in series]
    count = len(vals)
    if count <= 0:
        return {}

    vals_sorted = sorted(vals)
    stats = {
        "ema_bps": float(_ema_from_ordered(vals, alpha=float(ema_alpha))),
        "p50_bps": float(_percentile(vals_sorted, 0.50)),
        "p95_bps": float(_percentile(vals_sorted, 0.95)),
        "count": int(count),
        "last_is_bps": float(vals[-1]),
        "last_sid": sid_s,
        "updated_at_ms": _now_ms(),
        "last_fill_ms": int(ts_fill_ms),
        "session": d.session,
        "kind": d.kind,
        "tf": d.tf,
        "side": d.side,
        "symbol": d.symbol,
    }

    for key in (make_rollup_key(d, include_session=True), make_rollup_key(d, include_session=False)):
        await redis.hset(key, mapping={k: str(v) for k, v in stats.items()})
        await redis.expire(key, int(ttl_sec))

    return {k: float(v) if isinstance(v, (int, float)) else v for k, v in stats.items()}  # type: ignore[return-value]


def _try_hash(redis_client: Any, key: str) -> dict[str, Any]:
    try:
        d = redis_client.hgetall(key) or {}
        return {_decode(k): _decode(v) for k, v in dict(d).items()}
    except Exception:
        return {}


def read_inline_exec_rollup_sync(
    redis_client: Any,
    *,
    symbol: str,
    side: str,
    session: str,
    kind: str,
    tf: str,
    min_count: int = 1,
) -> dict[str, float]:
    """Read latest inline IS rollup using session-aware key with aggregate fallback."""
    if redis_client is None:
        return {}
    dims = InlineExecDims(symbol=symbol, side=side, session=session, kind=kind, tf=tf).norm()
    keys = [make_rollup_key(dims, include_session=True), make_rollup_key(dims, include_session=False)]
    for key in keys:
        d = _try_hash(redis_client, key)
        if not d:
            continue
        cnt = _i(d.get("count"), 0)
        if cnt < int(min_count):
            continue
        p95 = _f(d.get("p95_bps"))
        p50 = _f(d.get("p50_bps"))
        ema = _f(d.get("ema_bps"))
        upd = _i(d.get("updated_at_ms"), 0)
        out: dict[str, float] = {}
        if math.isfinite(ema):
            out["ema_bps"] = float(ema)
        if math.isfinite(p50):
            out["p50_bps"] = float(p50)
        if math.isfinite(p95):
            out["p95_bps"] = float(p95)
        out["count"] = float(cnt)
        out["updated_at_ms"] = float(upd)
        out["session_exact"] = 1.0 if key == keys[0] else 0.0
        return out
    return {}


def read_perm_impact_rollup_sync(
    redis_client: Any,
    *,
    symbol: str,
    side: str,
    session: str,
    kind: str,
    tf: str,
    venue: str = "binance",
    delta_sec: int = 1,
) -> float | None:
    """Read post-trade permanent impact p95 from canonical TCA Redis keys."""
    if redis_client is None:
        return None
    sym = _s(symbol).upper()
    ven = _s(venue, "binance").lower()
    sess = _sess(session)
    tfv = _tf(tf)
    kindv = _kind(kind)
    sidev = _side(side)
    keys = [
        f"tca:perm_impact_p95_bps:{int(delta_sec)}:{sym}:{ven}:{sess}:{tfv}:{kindv}:{sidev}",
        f"tca:perm_impact_p95_bps:{int(delta_sec)}:{sym}:{ven}:{sess}:{tfv}:all:{sidev}",
        f"tca:perm_impact_p95_bps:{int(delta_sec)}:{sym}:{ven}:all:all:all:{sidev}",
    ]
    for key in keys:
        try:
            v = redis_client.get(key)
        except Exception:
            v = None
        fv = _f(v)
        if math.isfinite(fv):
            return float(fv)
    return None


@dataclass(frozen=True)
class InlineExecPolicyDecision:
    apply: bool
    veto: bool
    reason_code: str
    tighten_add_bps: float = 0.0


def resolve_mode(mode: str, *, profile: str) -> str:
    m = _s(mode, "auto").lower()
    if m in {"off", "monitor", "tighten", "veto"}:
        return m
    p = _s(profile, "default").lower()
    if p == "hard":
        return "veto"
    if p == "strict":
        return "tighten"
    return "monitor"


def decide_inline_exec_health(*, p95_bps: float, warn_bps: float, crit_bps: float, perm_impact_p95_bps: float, max_perm_impact_p95_bps: float, mode: str) -> InlineExecPolicyDecision:
    """Pure policy for inline execution-health.

    Rules:
    - monitor: annotate only
    - tighten: if p95 > warn => add slippage
    - veto: if p95 > crit AND perm_impact_p95 is also bad
    """
    p95 = _f(p95_bps)
    warn = max(0.0, _f(warn_bps, 0.0))
    crit = max(float(warn), _f(crit_bps, warn))
    perm = _f(perm_impact_p95_bps)
    perm_thr = max(0.0, _f(max_perm_impact_p95_bps, 0.0))
    mm = _s(mode, "monitor").lower()
    if not math.isfinite(p95) or p95 <= 0:
        return InlineExecPolicyDecision(apply=False, veto=False, reason_code="INLINE_EXEC_NONE")
    if mm == "off":
        return InlineExecPolicyDecision(apply=False, veto=False, reason_code="INLINE_EXEC_OFF")
    if p95 <= warn:
        return InlineExecPolicyDecision(apply=False, veto=False, reason_code="INLINE_EXEC_OK")
    if mm == "monitor":
        return InlineExecPolicyDecision(apply=True, veto=False, reason_code="INLINE_EXEC_MONITOR")
    sev = 1.0 if warn <= 0 else max(1.0, p95 / max(warn, 1e-9))
    tighten_add = max(0.0, (sev - 1.0) * 5.0)
    perm_bad = bool(perm_thr > 0 and math.isfinite(perm) and perm >= perm_thr)
    if mm == "veto" and p95 >= crit and perm_bad:
        return InlineExecPolicyDecision(apply=True, veto=True, reason_code="VETO_INLINE_IMPL_SHORTFALL_P95", tighten_add_bps=float(tighten_add))
    return InlineExecPolicyDecision(apply=True, veto=False, reason_code="INLINE_EXEC_TIGHTEN", tighten_add_bps=float(tighten_add))
