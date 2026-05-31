from __future__ import annotations

import math
import os
from dataclasses import asdict, dataclass
from typing import Any

try:
    # Used by publish_decision_snapshot (optional)
    from services.async_signal_publisher import StreamSink  # type: ignore
except Exception:  # pragma: no cover
    StreamSink = None  # type: ignore


def _safe_float(x: Any) -> float | None:
    try:
        f = float(x)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return float(f)


def _safe_int(x: Any) -> int | None:
    try:
        i = int(float(x))
    except Exception:
        return None
    return int(i)


_LIQMAP_WINDOWS = ("5m", "1h")
_LIQMAP_WINDOW_FIELDS = (
    "age_ms",
    "levels_n",
    "total_usd",
    "near_total_usd",
    "near_long_usd",
    "near_short_usd",
    "near_imb",
    "dist_up_bps",
    "dist_dn_bps",
    "peak_up1_usd",
    "peak_dn1_usd",
    "peak_up1_share",
    "peak_dn1_share",
    "peaks_up",
    "peaks_dn",
)
_LIQMAP_GATE_FIELDS = (
    "liqmap_gate_shadow_veto",
    "liqmap_gate_veto",
    "liqmap_gate_rr",
    "liqmap_gate_risk_bps",
    "liqmap_gate_reward_bps",
    "liqmap_gate_adverse_peak_usd",
    "liqmap_gate_favorable_peak_usd",
)
_LIQMAP_INDICATOR_KEYS = {
    *(f"liqmap_{window}_{field}" for window in _LIQMAP_WINDOWS for field in _LIQMAP_WINDOW_FIELDS),
    *_LIQMAP_GATE_FIELDS,
}
_INDICATORS_SMALL_ALLOW = {
    "delta_z", "obi", "ofi_z", "ofi_stability_score", "obi_stability_score",
    "book_ts_gap_ms", "book_stale_ms", "spread_bps", "confidence_raw", "confidence_cal",
    # P2.5 — ctx_tighten attribution: must survive into decision:{sid} for joiner
    "ctx_sentiment_tighten_bps", "ctx_defillama_tighten_bps",
    *_LIQMAP_INDICATOR_KEYS,
}

@dataclass(slots=True)
class DecisionSnapshotContractDTO:
    """Dataclass Type-Checked payload for decision_snapshot."""

    schema_version: int
    producer: str
    sid: str
    signal_id: str
    symbol: str
    venue: str
    session: str
    tf: str
    kind: str
    direction: str
    side: str
    decision_ts_ms: int
    tca_ready: bool
    book_sanity_flags: list[str]
    decision_bid: float | None = None
    decision_ask: float | None = None
    decision_mid: float | None = None
    decision_spread_bps: float | None = None
    decision_depth_bid_5: float | None = None
    decision_depth_ask_5: float | None = None
    decision_depth_bid_20: float | None = None
    decision_depth_ask_20: float | None = None
    decision_book_slope_bid: float | None = None
    decision_book_slope_ask: float | None = None
    decision_dws_bps: float | None = None
    decision_ofi_norm: float | None = None
    decision_expected_slippage_bps: float | None = None
    decision_exec_risk_norm: float | None = None
    decision_price: float | None = None
    is_virtual: bool | None = None
    validation_status: str | None = None
    validation_reason: str | None = None
    indicators_small: dict[str, Any] | None = None
    # Joiner-affinity fields (P46 trade_close_joiner reads these to populate
    # `bucket` / `model_ver` / `market_regime` on trades:closed rows).
    meta_enforce_bucket: str | None = None
    ml_model_ver: str | None = None
    market_regime: str | None = None


def _extract_bbo(signal: dict[str, Any], runtime: Any | None) -> tuple[float | None, float | None]:
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
        bid = _safe_float(micro.get("best_bid"))  # type: ignore
    if ask is None:
        ask = _safe_float(micro.get("best_ask"))  # type: ignore

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


def _calc_mid_spread_bps(bid: float | None, ask: float | None) -> tuple[float | None, float | None, list[str]]:
    flags: list[str] = []
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


def _depth_sum_levels(book_side: Any, n: int) -> float | None:
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
    signal: dict[str, Any],
    indicators: dict[str, Any] | None,
    runtime: Any | None,
    schema_version: int = 1,
    include_indicators: bool = False,
) -> dict[str, Any]:
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

    direction = (signal.get("direction") or "").upper().strip()
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
    sanity_flags = [str(x) for x in sanity_flags if x is not None]  # type: ignore

    # Merge flags (A1 + derived)
    merged_flags: list[str] = []
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

    # Joiner-affinity fields — best-effort extraction so trade_close_joiner
    # can populate bucket/model_ver/regime on trades:closed without consulting
    # separate stores. All optional: missing values stay None.
    def _pick_str(*candidates: Any) -> str | None:
        for c in candidates:
            if c is None:
                continue
            s = str(c).strip()
            if s and s.lower() not in ("nan", "none"):
                return s
        return None

    meta_enforce_bucket = _pick_str(
        signal.get("meta_enforce_bucket"),
        signal.get("meta_enforce_cov_bucket"),
        indicators.get("meta_enforce_bucket"),
        indicators.get("meta_enforce_cov_bucket"),
    )
    ml_model_ver = _pick_str(
        signal.get("ml_model_ver"),
        signal.get("model_ver"),
        indicators.get("ml_model_ver"),
        indicators.get("model_ver"),
    )
    market_regime = _pick_str(
        signal.get("market_regime"),
        signal.get("entry_regime"),
        indicators.get("market_regime"),
        indicators.get("regime"),
    )

    out: dict[str, Any] = {
        "schema_version": int(schema_version),
        "producer": os.getenv("SERVICE_NAME", "python-worker"),
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
        "meta_enforce_bucket": meta_enforce_bucket,
        "ml_model_ver": ml_model_ver,
        "market_regime": market_regime,
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
        "is_virtual": bool(int(signal.get("is_virtual", 0))) if "is_virtual" in signal else None,
        "validation_status": (signal.get("validation_status")) if "validation_status" in signal else None,
        "validation_reason": (signal.get("validation_reason")) if "validation_reason" in signal else None,
    }

    if include_indicators:
        # Keep small allow-list to avoid massive payloads by accident while
        # preserving decision-time v9 liqmap features for replay/train datasets.
        out["indicators_small"] = {k: indicators.get(k) for k in _INDICATORS_SMALL_ALLOW if k in indicators}

    # Validate output through strict dataclass initialization before returning
    try:
        validated = DecisionSnapshotContractDTO(**out)
        return asdict(validated)
    except Exception as e:
        import logging
        logging.getLogger("decision_snapshot").warning("DecisionSnapshot dataclass validation failed: %s", e)
        # Fail-open: return the raw dict
        return out


def build_decision_snapshot(
    signal: dict[str, Any],
    *,
    runtime: Any | None,
    indicators: dict[str, Any] | None,
    schema_version: int = 1,
    include_indicators: bool = False,
) -> dict[str, Any]:
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
    evt: dict[str, Any] | None = None,
    snapshot: dict[str, Any] | None = None,
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
        symbol=symbol,
    )
