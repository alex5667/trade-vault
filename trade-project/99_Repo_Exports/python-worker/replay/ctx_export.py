from __future__ import annotations

"""
Ctx export for record & replay.

ВАЖНО:
  - "compact" режим (по умолчанию) — стабильный whitelist полей, маленький размер.
  - "full" режим — экспорт всех "простых" атрибутов ctx (для случаев, когда pipeline
    зависит от дополнительных полей). Full всё равно защищён от утягивания больших
    структур: не сериализуем сложные объекты/большие коллекции.

Env:
  REPLAY_RECORD_CTX_MODE=compact|full
  REPLAY_RECORD_MAX_LIST=256
  REPLAY_RECORD_MAX_DICT=256
"""

import math
import os
from collections.abc import Iterable
from typing import Any


def _isfinite(x: float) -> bool:
    return not (math.isnan(x) or math.isinf(x))


def _safe_num(v: Any) -> Any:
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return float(v) if _isfinite(v) else None
    return v


def _is_primitive(v: Any) -> bool:
    return v is None or isinstance(v, (str, int, float, bool))


def _safe_small_list(v: Any, *, max_n: int) -> list[Any] | None:
    if not isinstance(v, (list, tuple)):
        return None
    if len(v) > max_n:
        return None
    out: list[Any] = []
    for x in v:
        if _is_primitive(x):
            out.append(_safe_num(x))
        else:
            return None
    return out


def _safe_small_dict(v: Any, *, max_n: int) -> dict[str, Any] | None:
    if not isinstance(v, dict):
        return None
    if len(v) > max_n:
        return None
    out: dict[str, Any] = {}
    for k, x in v.items():
        if not isinstance(k, (str, int)):
            return None
        if not _is_primitive(x):
            return None
        out[str(k)] = _safe_num(x)
    return out


# Stable core fields used by scoring/debug dashboards
DEFAULT_CTX_FIELDS: tuple[str, ...] = (
    # identity
    "ts",
    "ts_utc",
    "symbol",
    "venue",
    "timeframe",
    "family",
    # prices
    "price",
    "last_price",
    "bid",
    "ask",
    # core features
    "z_delta",
    "obi",
    "obi_avg",
    "obi_sustained",
    "atr",
    "atr_14_bps",
    "atr_q_14",
    "atr_quantile",
    "weak_progress",
    "weak_progress_raw",
    "weak_progress_ratio",
    "cum_delta_slope",
    "delta_bucket",
    "current_delta",
    # regime
    "regime",
    "market_regime",
    "market_regime_score",
    "regime_trend_score",
    "regime_range_score",
    "is_trending",
    # anchors
    "vwap",
    "daily_open",
    "daily_open_dist_bps",
    "htf_level_dist_bps",
    # L3-lite top features
    "spread_bps",
    "microprice_shift_bps_20",
    "cancel_to_trade_bid_5s",
    "cancel_to_trade_ask_5s",
    "cancel_to_trade_bid_20s",
    "cancel_to_trade_ask_20s",
    "obi_5",
    "obi_20",
    "obi_50",
    # data quality flags
    "data_quality_flags",
)


def _export_compact(ctx: Any, *, fields: Iterable[str]) -> dict[str, Any]:
    max_list = max(16, int(os.getenv("REPLAY_RECORD_MAX_LIST", "256") or 256))
    max_dict = max(16, int(os.getenv("REPLAY_RECORD_MAX_DICT", "256") or 256))
    out: dict[str, Any] = {}
    for k in fields:
        if not hasattr(ctx, k):
            continue
        v = getattr(ctx, k)
        if _is_primitive(v):
            out[k] = _safe_num(v)
            continue
        lst = _safe_small_list(v, max_n=max_list)
        if lst is not None:
            out[k] = lst
            continue
        d2 = _safe_small_dict(v, max_n=max_dict)
        if d2 is not None:
            out[k] = d2
            continue
        # deliberately skip heavy fields
    return out


def _export_full(ctx: Any) -> dict[str, Any]:
    """
    Full mode: export all public attributes that are primitives/small collections.
    This is useful when replay must preserve more fields than the compact whitelist,
    BUT still safe against gigantic payloads.
    """
    max_list = max(16, int(os.getenv("REPLAY_RECORD_MAX_LIST", "256") or 256))
    max_dict = max(16, int(os.getenv("REPLAY_RECORD_MAX_DICT", "256") or 256))

    # try dataclass / normal object
    keys: list[str] = []
    if hasattr(ctx, "__dict__") and isinstance(ctx.__dict__, dict):
        keys = [str(k) for k in ctx.__dict__.keys()]
    else:
        # last resort: dir() filtering
        keys = [k for k in dir(ctx) if not k.startswith("_")]

    out: dict[str, Any] = {}
    for k in keys:
        if k.startswith("_"):
            continue
        try:
            v = getattr(ctx, k)
        except Exception:
            continue
        if callable(v):
            continue
        if _is_primitive(v):
            out[k] = _safe_num(v)
            continue
        lst = _safe_small_list(v, max_n=max_list)
        if lst is not None:
            out[k] = lst
            continue
        d2 = _safe_small_dict(v, max_n=max_dict)
        if d2 is not None:
            out[k] = d2
            continue
        # skip heavy
    return out


def export_ctx(ctx: Any, *, fields: Iterable[str] | None = None) -> dict[str, Any]:
    mode = (os.getenv("REPLAY_RECORD_CTX_MODE", "compact") or "compact").strip().lower()
    if mode in {"full", "all"}:
        return _export_full(ctx)
    return _export_compact(ctx, fields=fields or DEFAULT_CTX_FIELDS)
