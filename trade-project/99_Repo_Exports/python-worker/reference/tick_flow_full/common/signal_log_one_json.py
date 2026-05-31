from __future__ import annotations

import logging
import math
from typing import Any

from common.json_fast import dumps1


def _f(x: Any) -> float | None:
    """Fast, safe float extractor: returns None for non-finite / non-castable."""
    try:
        v = float(x)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    return v


def _b(x: Any) -> int:
    """Stable bool->int (0/1) for compact logging and easy dashboarding."""
    try:
        return 1 if bool(x) else 0
    except Exception:
        return 0


def _max2(a: float | None, b: float | None) -> float | None:
    if a is None:
        return b
    if b is None:
        return a
    return a if a >= b else b


def _infer_l2_stale(ctx: Any) -> int | None:
    """
    Best-effort L2 staleness signal for logs:
      - prefer explicit ctx flags if present
      - else compute from (ts_ms - l2_ts_ms) if both are present
    Returns:
      1/0 or None if unknown.
    """
    ga = getattr
    # Explicit (already computed by upstream) -> fastest / most reliable.
    v = ga(ctx, "l2_is_stale", None)
    if v is not None:
        return _b(v)
    v = ga(ctx, "l2_stale", None)
    if v is not None:
        return _b(v)

    ts = _f(ga(ctx, "ts_ms", None))
    l2_ts = _f(ga(ctx, "l2_ts_ms", None))
    if ts is None or l2_ts is None:
        return None
    # NOTE: threshold is validator-specific; for logs we only capture "seems stale".
    # Real veto reason_code is the source of truth.
    return 1 if (ts - l2_ts) > 1500.0 else 0


def _infer_missing_l3(ctx: Any) -> int:
    """
    L3 is "missing" when none of the typical L3 features exist in ctx.
    We intentionally keep this heuristic conservative to avoid false alarms.
    """
    ga = getattr
    # Any of these implies L3 pipeline is alive.
    if _f(ga(ctx, "taker_rate_ema", None)) is not None:
        return 0
    if _f(ga(ctx, "cancel_to_trade_bid_5s", None)) is not None:
        return 0
    if _f(ga(ctx, "cancel_to_trade_ask_5s", None)) is not None:
        return 0
    if _f(ga(ctx, "microprice_shift_bps_20", None)) is not None:
        return 0
    return 1


def _infer_missing_htf(ctx: Any) -> int:
    """
    HTF/geometry is "missing" if geometry_score is absent AND no geometry snapshot.
    This is fail-open by policy; here we only log the condition.
    """
    ga = getattr
    if _f(ga(ctx, "geometry_score", None)) is not None:
        return 0
    if ga(ctx, "geometry", None) is not None:
        return 0
    return 1


def build_signal_one_json_obj(
    *,
    payload: dict[str, Any],
    ctx: Any,
    parts: dict[str, Any] | None = None,
    emitted: bool,
    emit_ok: bool | None = None,
    conf_factor01: float | None = None,
    veto_reason_code: str | None = None,
    veto_reason_u16: int | None = None,
) -> dict[str, Any]:
    """
    Build a stable, compact log object:
      - Always same top-level keys (dashboards love stable schemas)
      - Values are finite floats or None
      - Flags are 0/1

    IMPORTANT: this is for logs only; payload wire-format remains separate.
    """
    ga = getattr

    # From payload (already computed) -> cheapest
    kind = payload.get("kind")
    side = payload.get("side")
    symbol = payload.get("symbol")
    ts = payload.get("ts")
    signal_id = payload.get("signal_id")
    level_key = payload.get("level_key")  # may be absent
    raw_score = _f(payload.get("raw_score"))
    final_score = _f(payload.get("final_score"))

    # Confidence model axis: conf_factor01 and final_score are the main observability points.
    cf01 = _f(conf_factor01)
    if cf01 is not None:
        # clamp in logs (defensive): conf_factor is always expected in [0..1]
        if cf01 < 0.0:
            cf01 = 0.0
        elif cf01 > 1.0:
            cf01 = 1.0

    # --- Top features (best-effort; do NOT raise on bad ctx) ---
    spread_bps = _f(ga(ctx, "spread_bps", None))
    obi_avg = _f(ga(ctx, "obi_avg", None))
    microprice_shift = _f(ga(ctx, "microprice_shift_bps_20", None))
    if microprice_shift is None:
        microprice_shift = _f(ga(ctx, "microprice_shift_bps", None))

    c2t_bid_5s = _f(ga(ctx, "cancel_to_trade_bid_5s", None))
    c2t_ask_5s = _f(ga(ctx, "cancel_to_trade_ask_5s", None))
    cancel_to_trade = _max2(c2t_bid_5s, c2t_ask_5s)

    taker_rate = _f(ga(ctx, "taker_rate_ema", None))
    if taker_rate is None:
        taker_rate = _f(ga(ctx, "taker_rate", None))

    # Regime score: prefer unified field if present; else derive.
    regime_score = _f(ga(ctx, "market_regime_score", None))
    if regime_score is None:
        tr = _f(ga(ctx, "regime_trend_score", None))
        rr = _f(ga(ctx, "regime_range_score", None))
        if tr is not None and rr is not None:
            regime_score = tr - rr

    geometry_score = _f(ga(ctx, "geometry_score", None))
    if geometry_score is None:
        g = ga(ctx, "geometry", None)
        if g is not None:
            # geometry snapshot can store score under different naming
            geometry_score = _f(ga(g, "score01", None))
            if geometry_score is None:
                geometry_score = _f(ga(g, "geometry_score", None))

    # Data quality flags
    l2_is_stale = _infer_l2_stale(ctx)
    used_fallback_hlc = 0
    try:
        dq = ga(ctx, "data_quality_flags", None)
        if isinstance(dq, (list, tuple, set)):
            used_fallback_hlc = 1 if ("hlc_fallback" in dq) else 0
    except Exception:
        used_fallback_hlc = 0
    missing_htf = _infer_missing_htf(ctx)
    missing_l3 = _infer_missing_l3(ctx)

    # Keep schema stable: always emit all keys.
    return {
        "type": "signal_sent" if emitted else "signal_veto",
        "signal_id": signal_id,
        "symbol": symbol,
        "kind": kind,
        "side": side,
        "ts": ts,
        "level_key": level_key,
        "raw_score": raw_score,
        "conf_factor01": cf01,
        "final_score": final_score,
        "emit_ok": _b(emit_ok) if emit_ok is not None else None,
        "veto_reason_code": veto_reason_code or None,
        "veto_reason_u16": veto_reason_u16 or None,
        # top features
        "spread_bps": spread_bps,
        "obi_avg": obi_avg,
        "microprice_shift": microprice_shift,
        "cancel_to_trade": cancel_to_trade,
        "taker_rate": taker_rate,
        "regime_score": regime_score,
        "geometry_score": geometry_score,
        # data quality flags
        "l2_is_stale": l2_is_stale,
        "used_fallback_hlc": used_fallback_hlc,
        "missing_htf": missing_htf,
        "missing_l3": missing_l3,
    }


def log_signal_one_json(
    logger: Any,
    *,
    payload: dict[str, Any],
    ctx: Any,
    parts: dict[str, Any] | None = None,
    emitted: bool,
    emit_ok: bool | None = None,
    conf_factor01: float | None = None,
    veto_reason_code: str | None = None,
    veto_reason_u16: int | None = None,
) -> None:
    """
    PERF CRITICAL:
      - If INFO disabled -> return immediately (do not touch ctx at all)
      - Build a single dict and dump to single-line JSON
    """
    try:
        if not logger.isEnabledFor(logging.INFO):
            return
        obj = build_signal_one_json_obj(
            payload=payload,
            ctx=ctx,
            parts=parts,
            emitted=emitted,
            emit_ok=emit_ok,
            conf_factor01=conf_factor01,
            veto_reason_code=veto_reason_code,
            veto_reason_u16=veto_reason_u16,
        )
        logger.info(dumps1(obj))
    except Exception:
        # Fail-open: logging must never break trading pipeline.
        return
