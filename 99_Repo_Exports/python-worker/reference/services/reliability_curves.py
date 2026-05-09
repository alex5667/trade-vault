from __future__ import annotations

import math
import os
import re
from typing import Any

from utils.time_utils import get_ny_time_millis

# =============================================================================
# Reliability curves (post-calibration) for confidence.
#
# Goal:
#   Keep base confidence as-is, but learn how it behaves under "contexts",
#   especially SMT leader/coherence context:
#
#     smt_conf   = leader_confirm (0/1)
#     smt_coh_hi = coh >= threshold (0/1)
#     smt_align  = signal direction matches leader_dir (0/1)
#
# We store curves as best-effort Redis HASH counters (fail-open):
#   Key:
#     rel:v2:{target}:{strategy}:{symbol}:{tf}:{ctx}
#   Fields:
#     samples, hits, last_ts_ms
#     n:{bucket}, h:{bucket}  (bucket = 0..100 step=RELIABILITY_BUCKET_STEP)
#
# We also always maintain a global curve (ctx="na") for the same dims,
# and a context-specific curve (ctx="smtc{0/1}_coh{0/1}_al{0/1}") when SMT fields exist.
#
# Targets (selectable via docker-compose env):
#   RELIABILITY_TARGETS=tp1|win|tp2|tp1_not_sl|all
# Default recommended: tp2
# =============================================================================


# -------------------------- small utils (fail-open) --------------------------
def _env_str(k: str, default: str) -> str:
    v = os.getenv(k, default)
    return str(v or default)


def _env_int(k: str, default: int) -> int:
    try:
        return int(float(os.getenv(k, str(default))))
    except Exception:
        return default


def _env_float(k: str, default: float) -> float:
    try:
        return float(os.getenv(k, str(default)))
    except Exception:
        return default


def _b2s(x: Any) -> str:
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="ignore")
    return str(x)


def _boolish(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return int(x) != 0
    s = str(x).strip().lower()
    return s in {"1", "true", "yes", "on"}


def _safe_float(x: Any, d: float = float("nan")) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return d
        return v
    except Exception:
        return d


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _canon_symbol(s: Any) -> str:
    return (s or "").strip().upper() or "NA"


def _canon_tf(s: Any) -> str:
    return (s or "").strip().lower() or "na"


def _canon_strategy(s: Any) -> str:
    return (s or "").strip() or "unknown"


def _canon_venue(s: Any) -> str:
    # venue can be "binance_futures", "mt5", etc.
    v = (s or "").strip().lower()
    v = re.sub(r"[^a-z0-9_\-]+", "_", v)
    return v or "na"


def _canon_kind(s: Any) -> str:
    # kind in envelope is pattern-specific, keep it stable and compact.
    v = (s or "").strip().lower()
    v = re.sub(r"[^a-z0-9_\-]+", "_", v)
    return v or "na"


def _canon_regime(s: Any) -> str:
    v = (s or "").strip().lower()
    v = re.sub(r"[^a-z0-9_\-]+", "_", v)
    return v or "na"


def _canon_target(s: str) -> str:
    s = (s or "").strip().lower()
    if s in {"tp1", "tp1_hit"}:
        return "tp1"
    if s in {"tp2", "tp2_hit"}:
        return "tp2"
    if s in {"win", "pnl"}:
        return "win"
    if s in {"tp1_not_sl", "tp1nosl", "tp1_no_sl"}:
        return "tp1_not_sl"
    return s or "tp1"


def _parse_targets() -> list[str]:
    # NOTE:
    #   - historically we used "tp1" as default
    #   - for most systems we recommend "tp2" as default (better quality proxy)
    raw = _env_str("RELIABILITY_TARGETS", "tp2").strip().lower()
    if raw in {"all", "*"}:
        return ["tp1", "win", "tp2", "tp1_not_sl"]
    # Accept both "a|b|c" and "a,b,c" to make docker-compose less error-prone.
    raw = raw.replace(",", "|")
    parts = [p.strip() for p in raw.split("|") if p.strip()]
    if not parts:
        return ["tp2"]
    return [_canon_target(p) for p in parts]


def _bucket_confidence(conf: float, *, step: int) -> int:
    """
    Normalize confidence into [0..100] and bucketize to step.
    Supports both 0..1 and 0..100 inputs.
    """
    if not math.isfinite(conf):
        return -1
    x = float(conf)
    if x <= 1.0:
        x = x * 100.0
    x = max(0.0, min(100.0, x))
    b = int(round(x / float(step)) * step)
    b = max(0, min(100, b))
    return b


def _extract_ctx(pos: dict[str, Any] | None, closed: dict[str, Any]) -> dict[str, Any]:
    """
    SMT fields live in payload.ctx (producer writes ctx.__dict__ into envelope).
    In runtime update_stats() receives:
      - pos: PositionState.__dict__ (includes signal_payload)
      - closed: TradeClosed.__dict__
    We prefer pos.signal_payload.ctx because it preserves entry-time ctx snapshot.
    """
    # 1) closed may already have flattened ctx (rare) -> accept.
    if isinstance(closed.get("ctx"), dict):
        return closed["ctx"]  # type: ignore
    # 2) PositionState.signal_payload.ctx is expected shape.
    if isinstance(pos, dict):
        sp = pos.get("signal_payload")
        if isinstance(sp, dict) and isinstance(sp.get("ctx"), dict):
            return sp["ctx"]  # type: ignore
    return {}


def _extract_base_confidence(pos: dict[str, Any] | None, closed: dict[str, Any]) -> float | None:
    """
    Best-effort extraction of "base confidence" from common field names.
    We deliberately support multiple synonyms to avoid protocol break.
    """
    # 1) Prefer explicit fields in closed (if downstream already copied them).
    for k in ("confidence", "final_score", "score", "conf", "prob", "p"):
        v = _safe_float(closed.get(k), float("nan"))
        if math.isfinite(v):
            return float(v)
    # 2) Otherwise take from ctx snapshot.
    ctx = _extract_ctx(pos, closed)
    for k in ("confidence", "final_score", "score", "conf", "prob", "p"):
        v = _safe_float(ctx.get(k), float("nan"))
        if math.isfinite(v):
            return float(v)
    return None


def _extract_envelope(pos: dict[str, Any] | None, closed: dict[str, Any]) -> dict[str, Any]:
    """
    PositionState.signal_payload is the original envelope from outbox.
    We use it for kind and entry-time ctx snapshot.
    """
    if isinstance(pos, dict):
        sp = pos.get("signal_payload")
        if isinstance(sp, dict):
            return sp
    return {}


def _extract_ctx_from_envelope(env: dict[str, Any]) -> dict[str, Any]:
    ctx = env.get("ctx")
    return ctx if isinstance(ctx, dict) else {}


def _extract_venue(pos: dict[str, Any] | None, closed: dict[str, Any]) -> str:
    """
    venue is present in:
      - envelope top-level: payload["venue"] = ctx.venue
      - ctx dict (sometimes) as ctx["venue"]
      - closed usually doesn't have it (TradeClosed has source/strategy only)
    """
    env = _extract_envelope(pos, closed)
    v = env.get("venue")
    if v:
        return _canon_venue(v)
    ctx = _extract_ctx_from_envelope(env)
    v = ctx.get("venue")
    if v:
        return _canon_venue(v)
    # last resort: some callers may pass venue into closed dict dynamically later
    v = closed.get("venue")
    return _canon_venue(v)


def _extract_kind(pos: dict[str, Any] | None, closed: dict[str, Any]) -> str:
    env = _extract_envelope(pos, closed)
    # kind is guaranteed at top-level envelope in producer
    k = env.get("kind") or closed.get("kind")  # closed usually doesn't have it; safe fallback
    return _canon_kind(k)


def _extract_entry_regime(pos: dict[str, Any] | None, closed: dict[str, Any]) -> str:
    # Source of truth after close: TradeClosed.entry_regime (you already set it in finalize_trade)
    r = closed.get("entry_regime")
    if r:
        return _canon_regime(r)
    env = _extract_envelope(pos, closed)
    # Fallbacks (entry-time snapshot):
    r = env.get("entry_regime") or env.get("regime") or env.get("regime_label") or "na"
    return _canon_regime(r)


def make_reliability_key(
    *,
    target: str,
    strategy: str,
    symbol: str,
    tf: str,
    ctx_key: str,
) -> str:
    target = _canon_target(target)
    return f"rel:v2:{target}:{_canon_strategy(strategy)}:{_canon_symbol(symbol)}:{_canon_tf(tf)}:{(ctx_key or 'na')}"


def make_reliability_key_v3(
    *,
    target: str,
    strategy: str,
    symbol: str,
    tf: str,
    kind: str,
    regime: str,
    ctx_key: str,
) -> str:
    """
    v3 key adds kind×regime dimension to prevent "one calibration fits all".
    """
    target = _canon_target(target)
    return (
        f"rel:v3:{target}:{_canon_strategy(strategy)}:{_canon_symbol(symbol)}:{_canon_tf(tf)}:"
        f"{_canon_kind(kind)}:{_canon_regime(regime)}:{(ctx_key or 'na')}"
    )


def make_reliability_key_v4(
    *,
    target: str,
    strategy: str,
    symbol: str,
    tf: str,
    venue: str,
    kind: str,
    regime: str,
    ctx_key: str,
) -> str:
    """
    v4 key adds venue dimension to prevent mixing fills/execution realities across venues.
    """
    target = _canon_target(target)
    return (
        f"rel:v4:{target}:{_canon_strategy(strategy)}:{_canon_symbol(symbol)}:{_canon_tf(tf)}:"
        f"{_canon_venue(venue)}:{_canon_kind(kind)}:{_canon_regime(regime)}:{(ctx_key or 'na')}"
    )


def _dir_to_ud(direction: str) -> str:
    d = (direction or "").strip().upper()
    if d == "LONG":
        return "UP"
    if d == "SHORT":
        return "DOWN"
    return "NA"


def _smt_context_key(
    *,
    pos: dict[str, Any] | None,
    closed: dict[str, Any],
    coh_thr: float,
) -> str | None:
    """
    Build compact SMT context key:
      smtc{0/1}_coh{0/1}_al{0/1}
    Returns None if SMT fields are absent (so caller can skip context curve).
    """
    ctx = _extract_ctx(pos, closed)
    if not isinstance(ctx, dict):
        return None
    if "smt_leader_confirm" not in ctx and "smt_coh" not in ctx and "smt_leader_dir" not in ctx:
        return None

    leader_confirm = 1 if _boolish(ctx.get("smt_leader_confirm")) else 0
    coh = _safe_float(ctx.get("smt_coh"), float("nan"))
    coh_hi = 1 if (math.isfinite(coh) and float(coh) >= float(coh_thr)) else 0

    leader_dir = (ctx.get("smt_leader_dir") or "NA").strip().upper()
    sig_dir_ud = _dir_to_ud((closed.get("direction") or ""))
    align = 1 if (leader_dir in {"UP", "DOWN"} and sig_dir_ud in {"UP", "DOWN"} and leader_dir == sig_dir_ud) else 0

    return f"smtc{leader_confirm}_coh{coh_hi}_al{align}"


def _target_y(target: str, *, closed: dict[str, Any]) -> int | None:
    """
    Convert trade outcome to binary label for target.
    """
    t = _canon_target(target)
    if t == "tp1":
        return 1 if _boolish(closed.get("tp1_hit")) else 0
    if t == "tp2":
        return 1 if _boolish(closed.get("tp2_hit")) else 0
    if t == "win":
        pnl = _safe_float(closed.get("pnl_net"), float("nan"))
        if not math.isfinite(pnl):
            pnl = _safe_float(closed.get("pnl"), float("nan"))
        if not math.isfinite(pnl):
            return None
        return 1 if pnl > 0.0 else 0
    if t == "tp1_not_sl":
        # Definition (simple & stable):
        #   label=1 if TP1 hit AND final close is not SL.
        if not _boolish(closed.get("tp1_hit")):
            return 0
        reason = str(closed.get("close_reason") or closed.get("close_reason_raw") or "").upper()
        return 0 if "SL" in reason else 1
    return None


def _hgetall(redis_client: Any, key: str) -> dict[str, str]:
    try:
        raw = redis_client.hgetall(key) or {}
    except Exception:
        return {}
    d: dict[str, str] = {}
    try:
        for k, v in dict(raw).items():
            d[_b2s(k)] = _b2s(v)
    except Exception:
        return {}
    return d


def _hset(redis_client: Any, key: str, mapping: dict[str, Any]) -> None:
    """
    FakeRedis supports hset(key, mapping=...).
    Real redis-py supports hset(name, mapping=...).
    """
    try:
        redis_client.hset(key, mapping=mapping)
    except TypeError:
        # fallback signature variants
        redis_client.hset(name=key, mapping=mapping)


def _update_curve_one(
    redis_client: Any,
    *,
    key: str,
    bucket: int,
    y: int,
    ts_ms: int,
) -> None:
    """
    Best-effort increment. Not atomic, but executed in StatsAggregator.finally,
    so correctness is "eventually consistent" and must never break runtime.
    """
    d = _hgetall(redis_client, key)
    samples = _safe_int(d.get("samples"), 0) + 1
    hits = _safe_int(d.get("hits"), 0) + int(y)
    nb = _safe_int(d.get(f"n:{bucket}"), 0) + 1
    hb = _safe_int(d.get(f"h:{bucket}"), 0) + int(y)
    _hset(
        redis_client,
        key,
        {
            "samples": str(samples),
            "hits": str(hits),
            "last_ts_ms": str(int(ts_ms)),
            f"n:{bucket}": str(nb),
            f"h:{bucket}": str(hb),
        },
    )


def load_bucket_rate(
    redis_client: Any,
    *,
    target: str,
    strategy: str,
    symbol: str,
    tf: str,
    venue: str | None = None,
    kind: str | None = None,
    regime: str | None = None,
    ctx_key: str,
    bucket: int,
) -> tuple[float | None, int]:
    """
    Return (rate, n) for a given bucket in the curve.
    Fail-open: (None, 0) on missing.
    """
    if redis_client is None or bucket < 0:
        return (None, 0)
    # Prefer v4 (venue×kind×regime). If missing, fallback to v3 then v2.
    d: dict[str, Any] = {}

    if venue is not None and kind is not None and regime is not None:
        k4 = make_reliability_key_v4(
            target=_canon_target(target),
            strategy=strategy,
            symbol=symbol,
            tf=tf,
            venue=str(venue),
            kind=str(kind),
            regime=str(regime),
            ctx_key=ctx_key,
        )
        d = _hgetall(redis_client, k4)

    if not d and kind is not None and regime is not None:
        k3 = make_reliability_key_v3(
            target=_canon_target(target),
            strategy=strategy,
            symbol=symbol,
            tf=tf,
            kind=str(kind),
            regime=str(regime),
            ctx_key=ctx_key,
        )
        d = _hgetall(redis_client, k3)

    if not d:
        k2 = make_reliability_key(
            target=_canon_target(target),
            strategy=strategy,
            symbol=symbol,
            tf=tf,
            ctx_key=ctx_key,
        )
        d = _hgetall(redis_client, k2)
    n = _safe_int(d.get(f"n:{bucket}"), 0)
    h = _safe_int(d.get(f"h:{bucket}"), 0)
    if n <= 0:
        return (None, 0)
    return (float(h) / float(n), int(n))


def update_reliability_curve(redis_client: Any, *, closed: dict[str, Any], pos: dict[str, Any] | None = None) -> None:
    """
    Writer called from StatsAggregator.finally (must be fail-open).
    Writes:
      - global curve (ctx="na")
      - SMT context curve (ctx="smtc*_coh*_al*") if SMT fields exist in ctx snapshot.
    """
    if redis_client is None:
        return
    if not isinstance(closed, dict) or not closed:
        return

    # Dims (strategy/symbol/tf exist in TradeClosed; kind/regime come from envelope/closed)
    venue = _extract_venue(pos, closed)
    kind = _extract_kind(pos, closed)
    regime = _extract_entry_regime(pos, closed)

    # Legacy writers control (to avoid write-amplification when stable).
    # Default: write v4 + v3 + v2 for safe rollout. Later you can set RELIABILITY_WRITE_LEGACY=0.
    write_legacy = True
    try:
        write_legacy = bool(int(os.getenv("RELIABILITY_WRITE_LEGACY", "1")))
    except Exception:
        write_legacy = True

    # Config
    step = max(1, min(20, _env_int("RELIABILITY_BUCKET_STEP", 5)))
    coh_thr = float(_env_float("RELIABILITY_SMT_COH_THR", 0.65))
    targets = _parse_targets()

    # Dims (strategy/symbol/tf are stable and exist in TradeClosed)
    strategy = _canon_strategy(closed.get("strategy") or (pos.get("strategy") if isinstance(pos, dict) else None))
    symbol = _canon_symbol(closed.get("symbol") or (pos.get("symbol") if isinstance(pos, dict) else None))
    tf = _canon_tf(closed.get("tf") or (pos.get("tf") if isinstance(pos, dict) else None))

    # Base confidence must exist to update curves.
    base_conf = _extract_base_confidence(pos, closed)
    if base_conf is None or not math.isfinite(float(base_conf)):
        return

    bucket = _bucket_confidence(float(base_conf), step=step)
    if bucket < 0:
        return

    ts_ms = _safe_int(closed.get("exit_ts_ms"), 0)
    if ts_ms <= 0:
        ts_ms = get_ny_time_millis()

    # SMT context key (optional).
    smt_ctx = _smt_context_key(pos=pos, closed=closed, coh_thr=coh_thr)

    # Update both global and context curves for each selected target.
    for tgt in targets:
        y = _target_y(tgt, closed=closed)
        if y is None:
            continue

        # 1) Global curve (v4 preferred; v3/v2 optional)
        k4_global = make_reliability_key_v4(
            target=tgt, strategy=strategy, symbol=symbol, tf=tf,
            venue=venue, kind=kind, regime=regime, ctx_key="na",
        )
        try:
            _update_curve_one(redis_client, key=k4_global, bucket=bucket, y=int(y), ts_ms=ts_ms)
        except Exception:
            pass

        if write_legacy:
            k3_global = make_reliability_key_v3(
                target=tgt, strategy=strategy, symbol=symbol, tf=tf, kind=kind, regime=regime, ctx_key="na"
            )
            k2_global = make_reliability_key(
                target=tgt, strategy=strategy, symbol=symbol, tf=tf, ctx_key="na"
            )
            try:
                _update_curve_one(redis_client, key=k3_global, bucket=bucket, y=int(y), ts_ms=ts_ms)
            except Exception:
                pass
            try:
                _update_curve_one(redis_client, key=k2_global, bucket=bucket, y=int(y), ts_ms=ts_ms)
            except Exception:
                pass

        # 2) SMT context curve (only if SMT fields exist)
        if smt_ctx:
            k4_ctx = make_reliability_key_v4(
                target=tgt, strategy=strategy, symbol=symbol, tf=tf,
                venue=venue, kind=kind, regime=regime, ctx_key=smt_ctx,
            )
            try:
                _update_curve_one(redis_client, key=k4_ctx, bucket=bucket, y=int(y), ts_ms=ts_ms)
            except Exception:
                pass

            if write_legacy:
                k3_ctx = make_reliability_key_v3(
                    target=tgt, strategy=strategy, symbol=symbol, tf=tf, kind=kind, regime=regime, ctx_key=smt_ctx
                )
                k2_ctx = make_reliability_key(
                    target=tgt, strategy=strategy, symbol=symbol, tf=tf, ctx_key=smt_ctx
                )
                try:
                    _update_curve_one(redis_client, key=k3_ctx, bucket=bucket, y=int(y), ts_ms=ts_ms)
                except Exception:
                    pass
                try:
                    _update_curve_one(redis_client, key=k2_ctx, bucket=bucket, y=int(y), ts_ms=ts_ms)
                except Exception:
                    pass
