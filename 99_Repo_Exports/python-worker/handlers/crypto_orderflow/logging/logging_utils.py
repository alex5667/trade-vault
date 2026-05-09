from __future__ import annotations

import json
from typing import Any

from core.signal_json_logger import log_signal_one_json
from utils.time_utils import get_ny_time_millis
import contextlib


def _safe_float(x: Any) -> float | None:
    try:
        v = float(x)
    except Exception:
        return None
    if not v.isfinite():
        return None
    return v


def _ctx_quality_flags(ctx: Any) -> dict[str, int]:
    """
    Compact data-quality flags for "1 signal = 1 JSON".
    Keep ints (0/1) to make downstream parsing cheap and stable.
    """
    def b(v: Any) -> int:
        return 1 if bool(v) else 0

    return {
        "l2_is_stale": b(getattr(ctx, "l2_is_stale", False)),
        "used_fallback_hlc": b(getattr(ctx, "hlc_fallback", False) or getattr(ctx, "used_fallback_hlc", False)),
        "missing_htf": b(getattr(ctx, "missing_htf", False)),
        "missing_l3": b(getattr(ctx, "missing_l3", False)),
    }


def log_signal_one_json_unified(
    logger: Any,
    *,
    payload: dict[str, Any],
    ctx: Any,
    parts: dict[str, Any],
    # NEW (micro-step): allow logging veto decisions with stable reason_code/u16.
    veto: bool = False,
    veto_reason_code: str = "",
    veto_reason_u16: int = 0,
    conf_factor: float | None = None,
    event: str = "emit",  # "emit" | "veto" | "candidate"
) -> None:
    """
    5.3 "1 сигнал = 1 JSON"

    Requirements:
      - one line per event (emit/veto)
      - stable keys (low cardinality)
      - include veto_reason_code/u16 for fast dashboard/debug
      - include top features + data quality flags
    """
    try:
        obj: dict[str, Any] = {
            "event": str(event),
            "ts_log_ms": get_ny_time_millis(),
            # identity
            "signal_id": payload.get("signal_id"),
            "kind": payload.get("kind"),
            "side": payload.get("side"),
            "symbol": payload.get("symbol"),
            "ts": payload.get("ts"),
            "level_key": payload.get("level_key") or payload.get("level_price"),
            # scores
            "raw_score": _safe_float(payload.get("raw_score")),
            "conf_factor": _safe_float(conf_factor) if conf_factor is not None else _safe_float(parts.get("conf_factor01")),
            "final_score": _safe_float(payload.get("final_score")),
            "confidence": _safe_float(payload.get("confidence")),  # 0..100
            # veto
            "veto": 1 if bool(veto) else 0,
            "veto_reason_code": (veto_reason_code or ""),
            "veto_reason_u16": int(veto_reason_u16 or 0),
            # top features (ctx)
            "spread_bps": _safe_float(getattr(ctx, "spread_bps", None)),
            "obi_avg": _safe_float(getattr(ctx, "obi_avg", None)),
            "microprice_shift": _safe_float(getattr(ctx, "microprice_shift_bps_20", None) or getattr(ctx, "microprice_shift_bps", None)),
            "cancel_to_trade": _safe_float(
                getattr(ctx, "cancel_to_trade_bid_5s", None) or getattr(ctx, "cancel_to_trade_ask_5s", None)
            ),
            "taker_rate": _safe_float(getattr(ctx, "taker_rate_ema", None) or getattr(ctx, "taker_rate", None)),
            "regime_score": _safe_float(getattr(ctx, "market_regime_score", None) or getattr(ctx, "regime_trend_score", None)),
            "geometry_score": _safe_float(getattr(ctx, "geometry_score", None)),
            # data quality flags
            "dq": _ctx_quality_flags(ctx),
        }

        # Keep parts in logs but cap size: avoid huge dicts in hot path.
        # (Downstream can join by signal_id if deep debug is needed.)
        if isinstance(parts, dict) and parts:
            obj["parts"] = parts

        msg = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        logger.info(msg)
    except Exception:
        # Fail-open: logging must never break the trading pipeline.
        with contextlib.suppress(Exception):
            logger.exception("log_signal_one_json failed")


def log_signal_one_json(logger, *, payload: dict[str, Any], ctx: Any, parts: dict[str, Any], emitted: bool) -> None:
    """
    Single-line JSON log per signal attempt (emitted or vetoed).
    - message field is json.dumps(obj) to keep ingest simple.
    - keep schema stable: dashboards/alerts depend on it.
    """
    def g(name: str, default=None):
        return getattr(ctx, name, default)
    obj = {
        "event": "signal",
        "emitted": bool(emitted),
        "ts_ms": int(payload.get("ts") or 0),
        "signal_id": payload.get("signal_id"),
        "kind": payload.get("kind"),
        "side": payload.get("side"),
        "symbol": payload.get("symbol"),
        "level_key": payload.get("level_key"),
        "level_price": payload.get("level_price"),
        "raw_score": payload.get("raw_score"),
        "final_score": payload.get("final_score"),
        "confidence": payload.get("confidence"),  # pct 0..100
        # NEW: soft reason ids (compact) for post-hoc calibration.
        "soft_u16": payload.get("soft_u16"),
        "soft16": payload.get("soft16"),
        # Optional debug strings if enabled in payload:
        "soft_codes": payload.get("soft_codes"),
        # top features (requested)
        "spread_bps": g("spread_bps"),
        "obi_avg": g("obi_avg"),
        "microprice_shift_bps_20": g("microprice_shift_bps_20"),
        "cancel_to_trade_bid_20s": g("cancel_to_trade_bid_20s"),
        "cancel_to_trade_ask_20s": g("cancel_to_trade_ask_20s"),
        "taker_rate_ema": g("taker_rate_ema"),
        "regime_score": g("market_regime_score"),
        "geometry_score": g("geometry_score"),
        # data quality flags (requested)
        "l2_is_stale": bool(g("l2_is_stale", False)),
        "used_fallback_hlc": bool(g("used_fallback_hlc", False)),
        "missing_htf": bool(g("missing_htf", False)),
        "missing_l3": bool(g("missing_l3", False)),
        # parts and qf are kept for debug; parts may be large -> keep as-is but compact separators.
        "qf": payload.get("qf"),
        "qf16": payload.get("qf16"),
        "parts": parts or {},
        "reasons": payload.get("reasons") or [],
        "reason_code": payload.get("reason_code"),
        "reason_u16": payload.get("reason_u16"),
    }
    try:
        logger.info(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        # fail-open logging: never break signal path
        pass


def _log_veto_one_json(
    self,
    *,
    cand: Any,
    ctx: Any,
    veto_reason_code: str,
    veto_reason_u16: int,
    parts: dict[str, Any],
) -> None:
    """
    1 veto = 1 JSON (аналогично "1 сигнал = 1 JSON").

    Зачем:
    - быстрый разбор "что именно душит поток"
    - калибровка порогов по structured кодам
    """
    try:
        obj = {
            "event": "signal_veto",
            "ts": int(getattr(ctx, "ts", 0) or 0),
            "signal_id": str(getattr(cand, "meta", {}).get("signal_id") or ""),  # если вы кладёте заранее
            "kind": str(getattr(cand, "kind", "") or ""),
            "side": int(getattr(cand, "side", 0) or 0),
            "level_price": getattr(cand, "level_price", None),
            "level_key": getattr(cand, "level_key", None),
            "raw_score": float(getattr(cand, "raw_score", 0.0) or 0.0),
            "veto_reason_code": (veto_reason_code or ""),
            "veto_reason_u16": int(veto_reason_u16 or 0),
            # Top-features (best-effort, fail-open)
            "spread_bps": getattr(ctx, "spread_bps", None),
            "obi_avg": getattr(ctx, "obi_avg", None),
            "microprice_shift_bps_20": getattr(ctx, "microprice_shift_bps_20", None),
            "cancel_to_trade_bid_5s": getattr(ctx, "cancel_to_trade_bid_5s", None),
            "cancel_to_trade_ask_5s": getattr(ctx, "cancel_to_trade_ask_5s", None),
            "taker_rate_ema": getattr(ctx, "taker_rate_ema", None),
            "regime_score": getattr(ctx, "market_regime_score", None),
            "geometry_score": getattr(ctx, "geometry_score", None),
            # parts (детализация) — может быть большой, но для veto обычно полезно.
            "parts": parts or {},
        }
        self.logger.info(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        # fail-open: лог не должен ломать торговый пайплайн
        pass
