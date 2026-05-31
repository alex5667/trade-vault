"""Canonical categorical-feature encoders for the Phase-3 scorer.

Train/serve symmetry contract:
- `encode_categorical_from_record(d, inds)` — used by the trainer when reading
  edge_live JSONL rows.
- `encode_categorical_from_ctx(ctx)` — used by the inference path
  (`confidence_calculation/confidence_scorer.py`) when scoring a live signal.

Both functions MUST return identical keys with identical encoders, so that
the model trained on one is valid for the other.

Feature naming: synthetic names use the `_cat_` prefix so the inference loop
can recognise them and route through this helper instead of `getattr(ctx,fn)`.

Unknown / missing values are encoded as `-1` so that LightGBM treats them as
a distinct category rather than collapsing them into 0 (a real bucket).
"""
from __future__ import annotations

from typing import Any

# Ordered list of categorical feature names (used by trainer and inference).
SCORER_CATEGORICAL_FEATURES: list[str] = [
    "_cat_symbol_idx",
    "_cat_regime_idx",
    "_cat_session_idx",
    "_cat_direction_idx",
]

# --- Encoders --------------------------------------------------------------

SYMBOL_MAP: dict[str, int] = {
    "BTCUSDT": 0,
    "ETHUSDT": 1,
    "SOLUSDT": 2,
    "1000PEPEUSDT": 3,
}
SYMBOL_OTHER = 4
SYMBOL_UNKNOWN = -1

# regime values stored lowercase (matches inds["regime"] / inds["market_regime"]).
REGIME_MAP: dict[str, int] = {
    "range": 0,
    "trending_bull": 1,
    "trending_bear": 2,
    "expansion": 3,
    "squeeze": 4,
    "choppy": 5,
}
REGIME_OTHER = 6
REGIME_UNKNOWN = -1

SESSION_MAP: dict[str, int] = {
    "NY": 0,
    "EU": 1,
    "LONDON": 1,  # alias
    "ASIA": 2,
    "OFF": 3,
}
SESSION_OTHER = 4
SESSION_UNKNOWN = -1

DIRECTION_LONG = 1
DIRECTION_SHORT = -1
DIRECTION_UNKNOWN = 0

_EMPTY_TOKENS = {"", "?", "na", "none", "null", "unknown"}


def _norm_upper(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip().upper()


def encode_symbol(raw: Any) -> int:
    s = _norm_upper(raw)
    if not s or s.lower() in _EMPTY_TOKENS:
        return SYMBOL_UNKNOWN
    return SYMBOL_MAP.get(s, SYMBOL_OTHER)


def encode_regime(raw: Any) -> int:
    s = _norm_upper(raw).lower()
    if not s or s in _EMPTY_TOKENS:
        return REGIME_UNKNOWN
    return REGIME_MAP.get(s, REGIME_OTHER)


def encode_session(raw: Any) -> int:
    s = _norm_upper(raw)
    if not s or s.lower() in _EMPTY_TOKENS:
        return SESSION_UNKNOWN
    return SESSION_MAP.get(s, SESSION_OTHER)


def encode_direction(raw: Any) -> int:
    s = _norm_upper(raw)
    if s in ("LONG", "BUY", "B", "L"):
        return DIRECTION_LONG
    if s in ("SHORT", "SELL", "S"):
        return DIRECTION_SHORT
    return DIRECTION_UNKNOWN


# --- Symmetric extractors --------------------------------------------------


def encode_categorical_from_record(d: dict[str, Any], inds: dict[str, Any]) -> dict[str, int]:
    """Train side: pull categorical features from an edge_live JSONL record.

    `d`    — the parsed JSONL row (top-level fields).
    `inds` — `d["indicators"]` (already validated dict).
    """
    return {
        "_cat_symbol_idx": encode_symbol(d.get("symbol")),
        "_cat_regime_idx": encode_regime(
            inds.get("regime") if inds.get("regime") not in (None, "") else inds.get("market_regime")
        ),
        "_cat_session_idx": encode_session(inds.get("session")),
        "_cat_direction_idx": encode_direction(d.get("direction") or d.get("side")),
    }


def encode_categorical_from_ctx(ctx: Any) -> dict[str, int]:
    """Serve side: pull categorical features from an OrderflowSignalContext.

    Mirrors `encode_categorical_from_record` so that train/serve stay aligned.
    """
    # regime: prefer regime_label, fall back to regime, then market_regime.
    regime_raw = getattr(ctx, "regime_label", None)
    if not regime_raw or str(regime_raw).strip().lower() in _EMPTY_TOKENS:
        regime_raw = getattr(ctx, "regime", None)
    if not regime_raw or str(regime_raw).strip().lower() in _EMPTY_TOKENS:
        regime_raw = getattr(ctx, "market_regime", None)

    # direction: prefer side (LONG/SHORT) over side_int.
    side_raw = getattr(ctx, "side", None)
    if not side_raw:
        side_int = getattr(ctx, "side_int", None)
        if side_int is not None:
            side_raw = "LONG" if int(side_int) > 0 else ("SHORT" if int(side_int) < 0 else "")

    return {
        "_cat_symbol_idx": encode_symbol(getattr(ctx, "symbol", None)),
        "_cat_regime_idx": encode_regime(regime_raw),
        "_cat_session_idx": encode_session(getattr(ctx, "session", None)),
        "_cat_direction_idx": encode_direction(side_raw),
    }


def is_categorical_feature_name(name: str) -> bool:
    """True if a feature name should be routed through the categorical encoder."""
    return name.startswith("_cat_")
