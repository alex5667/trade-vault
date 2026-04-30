"""Decision Context Enrichment (A1): freeze execution-relevant snapshot at EMIT time.

Purpose
-------
Downstream TCA (effective/realized spread, impact, implementation shortfall) needs a
*joinable* and *time-consistent* snapshot of market state at the decision/emit moment.

This helper enriches the signal ctx dict in-place with a minimal set of `decision_*` fields:
  - decision_ts_ms (epoch ms, deterministic: should be ts_emit_ms/tick_ts)
  - decision_bid/ask/mid/spread_bps
  - decision_depth_* (best-effort)
  - decision_book_slope_* (best-effort)
  - decision_dws_bps (depth-weighted spread proxy; top5 VWAP spread if available)
  - decision_ofi_norm (best-effort)
  - decision_expected_slippage_bps / decision_exec_risk_norm (best-effort)
  - decision_price (currently = decision_mid)

Also adds:
  - tca_ready: bool (do we have a minimally valid decision snapshot?)
  - book_sanity_flags: list[str] (annotation-only sanity flags; no veto here)

Design principles
-----------------
- Fail-open: never raises, never blocks publishing.
- Deterministic time: do NOT call time.time() unless `now_ms` explicitly provided.
- Bounded CPU: only uses top5 book if present.
- Low-cardinality: no ids become metric labels; these are ctx fields only.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Optional, Tuple, List


def _safe_f(v: Any) -> Optional[float]:
    try:
        f = float(v)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return float(f)


def _safe_i(v: Any) -> Optional[int]:
    try:
        i = int(v)
    except Exception:
        return None
    return int(i)


def _first_num(d: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[float]:
    for k in keys:
        if k in d:
            f = _safe_f(d.get(k))
            if f is not None:
                return f
    return None


def _first_int(d: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[int]:
    for k in keys:
        if k in d:
            i = _safe_i(d.get(k))
            if i is not None:
                return i
    return None


def _vwap(levels: Any) -> Optional[float]:
    """VWAP(px, qty) for list[(px,qty)]"""
    if not levels or not isinstance(levels, (list, tuple)):
        return None
    num = 0.0
    den = 0.0
    for it in levels:
        try:
            px = float(it[0]); qty = float(it[1])
        except Exception:
            continue
        if not (math.isfinite(px) and math.isfinite(qty)) or qty <= 0:
            continue
        num += px * qty
        den += qty
    if den <= 0:
        return None
    return num / den


def _extract_top5_book(ctx: Dict[str, Any], runtime: Any = None) -> Tuple[Optional[list], Optional[list]]:
    """Return (bids, asks) top5 lists if available."""
    # ctx may carry `bids/asks` or `book` structures in some versions.
    bids = None
    asks = None
    try:
        b = ctx.get("bids")
        a = ctx.get("asks")
        if isinstance(b, list):
            bids = b
        if isinstance(a, list):
            asks = a
    except Exception:
        pass

    # strategy attaches best_bid/best_ask only; runtime has last_book snapshot
    if (bids is None or asks is None) and runtime is not None:
        try:
            lb = getattr(runtime, "last_book", None)
            if lb is not None:
                b = lb.get("bids") if hasattr(lb, "get") else None
                a = lb.get("asks") if hasattr(lb, "get") else None
                if isinstance(b, list):
                    bids = b
                if isinstance(a, list):
                    asks = a
        except Exception:
            pass
        # also support atomic book_state.snap (BookState dataclass)
        try:
            bs = getattr(runtime, "book_state", None)
            snap = getattr(bs, "snap", None) if bs is not None else None
            if snap is not None:
                b = getattr(snap, "bids", None)
                a = getattr(snap, "asks", None)
                if isinstance(b, list):
                    bids = b
                if isinstance(a, list):
                    asks = a
        except Exception:
            pass
    return bids, asks


def ensure_decision_ctx_fields(
    ctx: Dict[str, Any]
    *
    indicators: Optional[Dict[str, Any]] = None
    runtime: Any = None
    now_ms: Optional[int] = None
) -> None:
    """Enrich ctx in-place with decision_* fields (best-effort, fail-open)."""
    try:
        indicators = indicators if isinstance(indicators, dict) else {}

        # ------------------------------------------------------------------
        # 1) Timestamp (event-time). Deterministic by default.
        # ------------------------------------------------------------------
        ts = (
            _first_int(ctx, ("decision_ts_ms",))
            or _first_int(ctx, ("ts_emit_ms", "tick_ts", "ts_ms", "ts", "event_time"))
            or _first_int(indicators, ("ts_emit_ms", "tick_ts", "ts_ms", "ts"))
            or (_safe_i(now_ms) if now_ms is not None else None)
        )
        if ts is not None:
            ctx.setdefault("decision_ts_ms", int(ts))
            # also pin ts_emit_ms for downstream consumers if missing
            ctx.setdefault("ts_emit_ms", int(ts))

        # ------------------------------------------------------------------
        # 2) Bid/Ask/Mid/Spread (from ctx/micro/runtime).
        # ------------------------------------------------------------------
        micro = ctx.get("micro") if isinstance(ctx.get("micro"), dict) else {}

        bid = _first_num(ctx, ("decision_bid", "best_bid", "bid", "best_bid_px", "bid_px"))
        ask = _first_num(ctx, ("decision_ask", "best_ask", "ask", "best_ask_px", "ask_px"))

        if bid is None:
            bid = _first_num(micro, ("best_bid", "best_bid_px", "bid", "bid_px"))
        if ask is None:
            ask = _first_num(micro, ("best_ask", "best_ask_px", "ask", "ask_px"))

        if (bid is None or ask is None) and runtime is not None:
            try:
                lb = getattr(runtime, "last_book", None)
                if lb is not None and hasattr(lb, "get"):
                    bid = bid if bid is not None else _safe_f(lb.get("best_bid_px") or lb.get("best_bid") or 0.0)
                    ask = ask if ask is not None else _safe_f(lb.get("best_ask_px") or lb.get("best_ask") or 0.0)
            except Exception:
                pass
            try:
                bs = getattr(runtime, "book_state", None)
                snap = getattr(bs, "snap", None) if bs is not None else None
                if snap is not None:
                    bid = bid if bid is not None else _safe_f(getattr(snap, "best_bid_px", None))
                    ask = ask if ask is not None else _safe_f(getattr(snap, "best_ask_px", None))
            except Exception:
                pass

        # mid & spread
        mid = None
        spread_bps = None
        if bid is not None and ask is not None and bid > 0 and ask > 0:
            mid = 0.5 * (bid + ask)
            if mid > 0:
                spread_raw = ask - bid
                spread_bps = (spread_raw / mid) * 10_000.0

        # If spread_bps already exists in micro/indicators, keep it (but prefer computed if sane)
        micro_spread = _first_num(micro, ("spread_bps",))
        ind_spread = _first_num(indicators, ("spread_bps", "liq_spread_bps"))
        if spread_bps is None:
            spread_bps = micro_spread if micro_spread is not None else ind_spread

        # Persist
        if bid is not None:
            ctx.setdefault("decision_bid", float(bid))
        if ask is not None:
            ctx.setdefault("decision_ask", float(ask))
        if mid is not None and mid > 0:
            ctx.setdefault("decision_mid", float(mid))
        if spread_bps is not None and _safe_f(spread_bps) is not None:
            ctx.setdefault("decision_spread_bps", float(spread_bps))

        # decision_price rule: for now use mid (arrival/decision price proxy)
        if ctx.get("decision_price") is None:
            if ctx.get("decision_mid") is not None:
                ctx["decision_price"] = float(ctx["decision_mid"])

        # ------------------------------------------------------------------
        # 3) Depth 5/20 (best-effort)
        # ------------------------------------------------------------------
        def _maybe_set(name: str, val: Any) -> None:
            if val is None:
                return
            if name in ctx:
                return
            fv = _safe_f(val)
            if fv is None:
                return
            ctx[name] = float(fv)

        # already computed elsewhere?
        _maybe_set("decision_depth_bid_5", indicators.get("depth_bid_5") or indicators.get("depth_5_bid_vol") or micro.get("depth_bid_5"))
        _maybe_set("decision_depth_ask_5", indicators.get("depth_ask_5") or indicators.get("depth_5_ask_vol") or micro.get("depth_ask_5"))
        _maybe_set("decision_depth_bid_20", indicators.get("depth_bid_20") or micro.get("depth_bid_20"))
        _maybe_set("decision_depth_ask_20", indicators.get("depth_ask_20") or micro.get("depth_ask_20"))

        # fallback from runtime snapshot top5 sums
        if runtime is not None:
            try:
                lb = getattr(runtime, "last_book", None)
                if lb is not None:
                    _maybe_set("decision_depth_bid_5", getattr(lb, "depth_5_bid_vol", None) if hasattr(lb, "depth_5_bid_vol") else lb.get("depth_5_bid_vol") if hasattr(lb,"get") else None)
                    _maybe_set("decision_depth_ask_5", getattr(lb, "depth_5_ask_vol", None) if hasattr(lb, "depth_5_ask_vol") else lb.get("depth_5_ask_vol") if hasattr(lb,"get") else None)
            except Exception:
                pass
            # some runtimes store depth fields directly
            _maybe_set("decision_depth_bid_5", getattr(runtime, "last_depth_bid_5", None))
            _maybe_set("decision_depth_ask_5", getattr(runtime, "last_depth_ask_5", None))
            _maybe_set("decision_depth_bid_20", getattr(runtime, "last_depth_bid_20", None))
            _maybe_set("decision_depth_ask_20", getattr(runtime, "last_depth_ask_20", None))

        # ------------------------------------------------------------------
        # 4) Book slope + DWS proxy (top5 VWAP spread)
        # ------------------------------------------------------------------
        _maybe_set("decision_book_slope_bid", indicators.get("book_slope_bid") or indicators.get("lob_depth_slope_bid"))
        _maybe_set("decision_book_slope_ask", indicators.get("book_slope_ask") or indicators.get("lob_depth_slope_ask"))
        if runtime is not None:
            _maybe_set("decision_book_slope_bid", getattr(runtime, "lob_depth_slope_bid", None))
            _maybe_set("decision_book_slope_ask", getattr(runtime, "lob_depth_slope_ask", None))

        # DWS: if already computed elsewhere
        _maybe_set("decision_dws_bps", indicators.get("dws_bps") or indicators.get("dw_spread_bps") or micro.get("dws_bps"))

        # If still missing, compute a bounded proxy using top5 VWAP bid/ask
        if "decision_dws_bps" not in ctx:
            bids, asks = _extract_top5_book(ctx, runtime=runtime)
            vb = _vwap(bids)
            va = _vwap(asks)
            dm = _safe_f(ctx.get("decision_mid"))
            if vb is not None and va is not None and dm is not None and dm > 0:
                dws_bps = ((va - vb) / dm) * 10_000.0
                if math.isfinite(dws_bps):
                    ctx["decision_dws_bps"] = float(dws_bps)

        # ------------------------------------------------------------------
        # 5) OFI normalized + exec health proxies (best-effort)
        # ------------------------------------------------------------------
        _maybe_set("decision_ofi_norm", indicators.get("ofi_norm") or indicators.get("ofi_depth_norm"))
        if runtime is not None and "decision_ofi_norm" not in ctx:
            try:
                ev = getattr(runtime, "last_ofi_event", None)
                if isinstance(ev, dict):
                    _maybe_set("decision_ofi_norm", ev.get("ofi_norm") or ev.get("ofi_depth_norm"))
            except Exception:
                pass

        _maybe_set("decision_expected_slippage_bps", indicators.get("expected_slippage_bps") or indicators.get("expected_slippage") or ctx.get("expected_slippage_bps"))
        _maybe_set("decision_exec_risk_norm", indicators.get("exec_risk_norm") or ctx.get("exec_risk_norm"))

        # ------------------------------------------------------------------
        # 6) Annotation-only sanity flags
        # ------------------------------------------------------------------
        flags: List[str] = []
        b = _safe_f(ctx.get("decision_bid"))
        a = _safe_f(ctx.get("decision_ask"))
        m = _safe_f(ctx.get("decision_mid"))
        if b is None or a is None or b <= 0 or a <= 0:
            flags.append("missing_bbo")
        if b is not None and a is not None and a < b:
            flags.append("crossed_bbo")
        if m is None or m <= 0:
            flags.append("bad_mid")
        sb = _safe_f(ctx.get("decision_spread_bps"))
        if sb is None or (sb is not None and sb < 0):
            flags.append("bad_spread")

        # merge into ctx (dedup)
        existing = ctx.get("book_sanity_flags")
        if not isinstance(existing, list):
            existing = []
        seen = set([str(x) for x in existing if x is not None])
        for f in flags:
            if f not in seen:
                existing.append(f)
                seen.add(f)
        ctx["book_sanity_flags"] = existing

        # tca_ready: minimal fields exist and sane
        tca_ready = bool(
            (_safe_i(ctx.get("decision_ts_ms")) or 0) > 0
            and (_safe_f(ctx.get("decision_mid")) or 0.0) > 0.0
            and (_safe_f(ctx.get("decision_bid")) or 0.0) > 0.0
            and (_safe_f(ctx.get("decision_ask")) or 0.0) > 0.0
            and ("crossed_bbo" not in existing)
        )
        ctx["tca_ready"] = bool(tca_ready)

    except Exception:
        # absolute fail-open: never break signal publishing
        return
