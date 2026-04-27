from __future__ import annotations

import os
import math
from typing import Any, Dict, Optional, Tuple, List

try:
    # Used by publish_decision_snapshot (optional)
    from services.async_signal_publisher import StreamSink  # type: ignore
except Exception:  # pragma: no cover
    StreamSink = None  # type: ignore


def _safe_float(x: Any) -> Optional[float]:
    try:
        f = float(x)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return float(f)


def _safe_int(x: Any) -> Optional[int]:
    try:
        i = int(float(x))
    except Exception:
        return None
    return int(i)


def _extract_bbo(signal: Dict[str, Any], runtime: Any | None) -> Tuple[Optional[float], Optional[float]]:
    # Prefer decision_* if already frozen by A1.
    bid = _safe_float(signal.get("decision_bid"))
    ask = _safe_float(signal.get("decision_ask"))

    if bid is None:
        bid = _safe_float(signal.get("best_bid"))
    if ask is None:
        ask = _safe_float(signal.get("best_ask"))

    # Fallback: micro dict
    micro = signal.get("micro") if isinstance(signal.get("micro"), dict) else {}
    if bid is None:
        bid = _safe_float(micro.get("best_bid"))
    if ask is None:
        ask = _safe_float(micro.get("best_ask"))

    # Runtime fallback: last_book top levels
    if (bid is None or ask is None) and runtime is not None:
        try:
            book = getattr(runtime, "last_book", None)
            if isinstance(book, dict):
                bids = book.get("bids") or []
                asks = book.get("asks") or []
                if bid is None and bids:
                    bid = _safe_float(bids[0][0])
                if ask is None and asks:
                    ask = _safe_float(asks[0][0])
        except Exception:
            pass

    return bid, ask


def _calc_mid_spread_bps(bid: Optional[float], ask: Optional[float]) -> Tuple[Optional[float], Optional[float], List[str]]:
    flags: List[str] = []
    if bid is None or ask is None:
        flags.append("missing_bbo")
        return None, None, flags
    if bid <= 0 or ask <= 0:
        flags.append("bad_bbo")
        return None, None, flags
    if bid >= ask:
        flags.append("crossed_bbo")
        # still compute mid best-effort
    mid = (bid + ask) / 2.0
    if mid <= 0:
        flags.append("bad_mid")
        return None, None, flags
    spread_bps = max(0.0, (ask - bid) / mid * 10_000.0)
    if not math.isfinite(spread_bps):
        flags.append("bad_spread")
        spread_bps = None
    return float(mid), (float(spread_bps) if spread_bps is not None else None), flags


def _depth_sum_levels(book_side: Any, n: int) -> Optional[float]:
    # book_side expected list[[px, qty], ...]
    try:
        arr = book_side or []
        s = 0.0
        k = 0
        for lvl in arr:
            if k >= n:
                break
            if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                continue
            q = _safe_float(lvl[1])
            if q is None:
                continue
            s += float(q)
            k += 1
        return float(s) if k > 0 else None
    except Exception:
        return None


def build_decision_snapshot_event(
    *,
    signal: Dict[str, Any],
    indicators: Dict[str, Any] | None,
    runtime: Any | None,
    schema_version: int = 1,
    include_indicators: bool = False,
) -> Dict[str, Any]:
    """Build a compact decision_snapshot event for Redis Stream.

    Contract goals:
    - joinable by sid + decision_ts_ms
    - contains decision_* microstructure context needed for TCA
    - best-effort: never throws, missing fields allowed
    """
    indicators = indicators or {}

    symbol = str(signal.get("symbol") or (getattr(runtime, "symbol", None) if runtime is not None else "") or "")
    sid = str(signal.get("sid") or signal.get("signal_id") or "")
    signal_id = str(signal.get("signal_id") or sid or "")

    ts_decision_ms = (
        _safe_int(signal.get("decision_ts_ms"))
        or _safe_int(signal.get("ts_emit_ms"))
        or _safe_int(signal.get("tick_ts"))
        or _safe_int(signal.get("ts_ms"))
        or 0
    )

    venue = str(signal.get("venue") or indicators.get("venue") or "binance")
    session = str(signal.get("session") or indicators.get("session") or "na")
    tf = str(signal.get("tf") or indicators.get("tf") or "na")
    kind = str(signal.get("kind") or indicators.get("kind") or signal.get("entry_tag") or "na")

    direction = str(signal.get("direction") or "").upper().strip()
    side = str(signal.get("side") or direction.lower() or "na")

    bid, ask = _extract_bbo(signal, runtime)
    mid, spread_bps, flags = _calc_mid_spread_bps(bid, ask)

    # Depth best-effort: decision_* preferred, then runtime.last_book top-level sums.
    depth_bid_5 = _safe_float(signal.get("decision_depth_bid_5"))
    depth_ask_5 = _safe_float(signal.get("decision_depth_ask_5"))
    depth_bid_20 = _safe_float(signal.get("decision_depth_bid_20"))
    depth_ask_20 = _safe_float(signal.get("decision_depth_ask_20"))

    if runtime is not None and (depth_bid_5 is None or depth_ask_5 is None or depth_bid_20 is None or depth_ask_20 is None):
        try:
            book = getattr(runtime, "last_book", None)
            if isinstance(book, dict):
                bids = book.get("bids") or []
                asks = book.get("asks") or []
                depth_bid_5 = depth_bid_5 if depth_bid_5 is not None else _depth_sum_levels(bids, 5)
                depth_ask_5 = depth_ask_5 if depth_ask_5 is not None else _depth_sum_levels(asks, 5)
                depth_bid_20 = depth_bid_20 if depth_bid_20 is not None else _depth_sum_levels(bids, 20)
                depth_ask_20 = depth_ask_20 if depth_ask_20 is not None else _depth_sum_levels(asks, 20)
        except Exception:
            pass

    # Optional geometry/toxicity values (best-effort)
    slope_bid = _safe_float(signal.get("decision_book_slope_bid")) or _safe_float(indicators.get("book_slope_bid")) or _safe_float(getattr(runtime, "lob_depth_slope_bid", None) if runtime is not None else None)
    slope_ask = _safe_float(signal.get("decision_book_slope_ask")) or _safe_float(indicators.get("book_slope_ask")) or _safe_float(getattr(runtime, "lob_depth_slope_ask", None) if runtime is not None else None)
    dws_bps = _safe_float(signal.get("decision_dws_bps")) or _safe_float(indicators.get("dws_bps")) or _safe_float(indicators.get("depth_weighted_spread_bps"))
    ofi_norm = _safe_float(signal.get("decision_ofi_norm")) or _safe_float(indicators.get("ofi_norm"))

    exp_slip = _safe_float(signal.get("decision_expected_slippage_bps")) or _safe_float(indicators.get("expected_slippage_bps")) or _safe_float(signal.get("expected_slippage_bps"))
    exec_risk = _safe_float(signal.get("decision_exec_risk_norm")) or _safe_float(indicators.get("exec_risk_norm")) or _safe_float(signal.get("exec_risk_norm"))

    # A1 fields if already set
    tca_ready = bool(signal.get("tca_ready")) if signal.get("tca_ready") is not None else False
    sanity_flags = signal.get("book_sanity_flags") if isinstance(signal.get("book_sanity_flags"), list) else []
    sanity_flags = [str(x) for x in sanity_flags if x is not None]

    # Merge flags (A1 + derived)
    merged_flags: List[str] = []
    seen = set()
    for x in (sanity_flags + flags):
        s = str(x).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        merged_flags.append(s)

    # If A1 didn't compute tca_ready, do a conservative best-effort check here.
    if signal.get("tca_ready") is None:
        tca_ready = bool(sid and ts_decision_ms and mid is not None and (bid is not None and ask is not None) and ("crossed_bbo" not in merged_flags))

    out: Dict[str, Any] = {
        "schema_version": int(schema_version),
        "producer": str(os.getenv("SERVICE_NAME", "python-worker")),
        "sid": sid,
        "signal_id": signal_id,
        "symbol": symbol,
        "venue": venue,
        "session": session,
        "tf": tf,
        "kind": kind,
        "direction": direction,
        "side": side,
        "decision_ts_ms": int(ts_decision_ms),
        "decision_bid": bid,
        "decision_ask": ask,
        "decision_mid": mid,
        "decision_spread_bps": spread_bps,
        "decision_depth_bid_5": depth_bid_5,
        "decision_depth_ask_5": depth_ask_5,
        "decision_depth_bid_20": depth_bid_20,
        "decision_depth_ask_20": depth_ask_20,
        "decision_book_slope_bid": slope_bid,
        "decision_book_slope_ask": slope_ask,
        "decision_dws_bps": dws_bps,
        "decision_ofi_norm": ofi_norm,
        "decision_expected_slippage_bps": exp_slip,
        "decision_exec_risk_norm": exec_risk,
        "decision_price": mid,  # current rule: decision_price == decision_mid
        "tca_ready": bool(tca_ready),
        "book_sanity_flags": merged_flags,
    }

    if include_indicators:
        # Keep small allow-list to avoid massive payloads by accident.
        allow = {
            "delta_z", "obi", "ofi_z", "ofi_stability_score", "obi_stability_score",
            "book_ts_gap_ms", "book_stale_ms", "spread_bps", "confidence_raw", "confidence_cal",
        }
        out["indicators_small"] = {k: indicators.get(k) for k in allow if k in indicators}

    return out


def build_decision_snapshot(
    signal: Dict[str, Any],
    *,
    runtime: Any | None,
    indicators: Dict[str, Any] | None,
    schema_version: int = 1,
    include_indicators: bool = False,
) -> Dict[str, Any]:
    """Backward-compatible wrapper used by SignalPipeline.

    Some runtime paths already import ``build_decision_snapshot(...)`` while the
    original module shipped only ``build_decision_snapshot_event(...)``. Keeping
    this alias here avoids fragile import-name drift.
    """
    return build_decision_snapshot_event(
        signal=signal,
        indicators=indicators,
        runtime=runtime,
        schema_version=schema_version,
        include_indicators=include_indicators,
    )


async def publish_decision_snapshot(
    *,
    publisher: Any,
    stream: str,
    maxlen: int,
    symbol: str,
    evt: Dict[str, Any] | None = None,
    snapshot: Dict[str, Any] | None = None,
) -> None:
    """Smoke-test friendly wrapper: publish decision snapshot using AsyncSignalPublisher.xadd_json.

    This wrapper exists to allow unit-testing publication behavior without importing SignalPipeline.
    """
    payload = evt if isinstance(evt, dict) else snapshot if isinstance(snapshot, dict) else {}
    if StreamSink is None:
        # Fallback: try direct xadd_json signature without StreamSink (some test stubs may accept it).
        await publisher.xadd_json(stream=stream, payload=payload, symbol=symbol)
        return
    await publisher.xadd_json(
        sink=StreamSink(name=str(stream), field="payload", maxlen=int(maxlen)),
        payload=payload,
        symbol=str(symbol),
    )
