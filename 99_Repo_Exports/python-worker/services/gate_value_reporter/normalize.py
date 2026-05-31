"""Normalize three different stream shapes into a single GateOutcomeRecord.

Inputs:
  - labels:tb              → JSON payload with sid/y_edge/r_mult/primary/…
  - metrics:ml_confirm     → flat fields with sid/kind/p_edge[_cal]/confidence
  - stream:signals:gated_out_outcomes → flat fields written by gated_out_outcome_tracker

For passed cohort: we need labels:tb (outcome) joined with metrics:ml_confirm
(kind/p_edge) on sid.

sid normalization mirrors ml_confirm_sre_poller.outcome_metrics._normalize_sid
to keep the cross-stream join consistent.
"""

from __future__ import annotations

import json
import math
from typing import Any

from services.gate_value_reporter.contracts import GateOutcomeRecord


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return default


def _bool_field(x: Any) -> bool:
    return str(x).strip().lower() in {"1", "true", "yes", "y"}


def normalize_sid(raw_sid: Any) -> str:
    """Canonicalize sid to `crypto-of:SYMBOL:ts_ms` for cross-stream join.

    metrics:ml_confirm uses `crypto-of:SYMBOL:ts_ms`; labels:tb uses
    `<kind>:SYMBOL:ts_ms[:DIR]`. We drop kind/direction here so passed-cohort
    join works irrespective of which trainer produced the label.
    """
    s = str(raw_sid or "").strip()
    if not s:
        return ""
    parts = s.split(":")
    if len(parts) < 3:
        return s
    sym = (parts[1] or "").upper()
    try:
        t = int(parts[2])
    except (TypeError, ValueError):
        return s
    return f"crypto-of:{sym}:{t}"


def _outcome_reason(tp_hit: bool, sl_hit: bool) -> str:
    if tp_hit:
        return "tp"
    if sl_hit:
        return "sl"
    return "timeout"


def build_ml_confirm_by_sid(
    entries: list[tuple[str, dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    """Group metrics:ml_confirm entries by normalized sid. Latest wins."""
    out: dict[str, dict[str, Any]] = {}
    for _entry_id, fields in entries:
        sid = normalize_sid(fields.get("sid"))
        if not sid:
            continue
        out[sid] = {
            "kind": str(fields.get("kind") or "unknown") or "unknown",
            "p_edge": _f(fields.get("p_edge_cal"), _f(fields.get("p_edge"), 0.0)),
            "confidence": _f(fields.get("confidence"), 0.0),
            "symbol": str(fields.get("symbol") or "").upper(),
            "direction": str(fields.get("direction") or "").upper(),
        }
    return out


def normalize_passed_label(
    fields: dict[str, Any],
    ml_by_sid: dict[str, dict[str, Any]],
    *,
    source_stream: str,
) -> GateOutcomeRecord | None:
    """Decode a labels:tb entry into a passed-cohort GateOutcomeRecord.

    Filters to primary horizon only (matches ml_confirm_sre_poller behaviour).
    Returns None on bad payload or missing join.
    """
    raw = fields.get("payload")
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if _i(payload.get("primary"), 0) != 1:
        return None
    raw_sid = payload.get("sid")
    sid = normalize_sid(raw_sid)
    if not sid:
        return None

    ml = ml_by_sid.get(sid, {})

    y = 1 if _i(payload.get("y_edge"), 0) > 0 else 0
    r_mult = _f(payload.get("r_mult"), 0.0)
    tp_hit = y == 1
    sl_hit = r_mult <= -0.95
    reason = _outcome_reason(tp_hit, sl_hit)

    symbol = str(
        payload.get("symbol") or ml.get("symbol") or ""
    ).upper()
    direction = str(
        payload.get("direction") or ml.get("direction") or "LONG"
    ).upper()
    side: Any = "SHORT" if direction == "SHORT" else "LONG"

    return GateOutcomeRecord(
        sid=str(raw_sid),
        cohort="passed",
        symbol=symbol,
        kind=str(ml.get("kind") or payload.get("kind") or "unknown"),
        side=side,
        ts_ms=_i(payload.get("ts_ms"), 0),
        horizon_ms=_i(payload.get("h_ms"), _i(payload.get("horizon_ms"), 0)),
        entry_px=_f(payload.get("entry_px") or payload.get("entry"), 0.0),
        tp_bps=_f(payload.get("tp_bps"), 0.0),
        sl_bps=_f(payload.get("sl_bps"), 0.0),
        ret_bps=_f(payload.get("ret_bps"), 0.0),
        r_mult=r_mult,
        y=y,
        tp_hit=tp_hit,
        sl_hit=sl_hit,
        outcome_reason=reason,  # type: ignore[arg-type]
        p_edge=ml.get("p_edge") if "p_edge" in ml else None,
        confidence=ml.get("confidence") if "confidence" in ml else None,
        source_stream=source_stream,
    )


def normalize_gated_out_outcome(
    fields: dict[str, Any],
    *,
    source_stream: str,
) -> GateOutcomeRecord | None:
    """Decode a stream:signals:gated_out_outcomes entry into gated_out cohort."""
    raw_sid = fields.get("sid")
    if not raw_sid:
        return None

    y = 1 if _i(fields.get("y"), 0) > 0 else 0
    tp_hit = _bool_field(fields.get("tp_hit"))
    sl_hit = _bool_field(fields.get("sl_hit"))
    reason = _outcome_reason(tp_hit, sl_hit)

    direction = str(fields.get("direction") or "LONG").upper()
    side: Any = "SHORT" if direction == "SHORT" else "LONG"

    has_p_edge = "p_edge" in fields
    has_confidence = "confidence" in fields

    return GateOutcomeRecord(
        sid=str(raw_sid),
        cohort="gated_out",
        symbol=str(fields.get("symbol") or "").upper(),
        kind=str(fields.get("kind") or "confidence_v1_gated_out"),
        side=side,
        ts_ms=_i(fields.get("ts_ms"), 0),
        horizon_ms=_i(fields.get("horizon_ms"), 0),
        entry_px=_f(fields.get("entry"), 0.0),
        tp_bps=_f(fields.get("tp_bps"), 0.0),
        sl_bps=_f(fields.get("sl_bps"), 0.0),
        ret_bps=_f(fields.get("ret_bps"), 0.0),
        r_mult=_f(fields.get("r_mult"), 0.0),
        y=y,
        tp_hit=tp_hit,
        sl_hit=sl_hit,
        outcome_reason=reason,  # type: ignore[arg-type]
        p_edge=_f(fields.get("p_edge"), 0.0) if has_p_edge else None,
        confidence=_f(fields.get("confidence"), 0.0) if has_confidence else None,
        source_stream=source_stream,
    )


def _tp_sl_bucket(bps: float, step: float = 5.0) -> int:
    if not math.isfinite(bps) or bps <= 0:
        return 0
    return int(round(bps / step) * step)


def group_key(r: GateOutcomeRecord) -> tuple[str, str, int, int, int]:
    """(symbol, kind, horizon_ms, tp_bucket, sl_bucket)."""
    return (
        r.symbol or "UNKNOWN",
        r.kind or "unknown",
        int(r.horizon_ms or 0),
        _tp_sl_bucket(r.tp_bps),
        _tp_sl_bucket(r.sl_bps),
    )
