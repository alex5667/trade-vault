"""common.market_mode

Single source of truth for regime / market-mode label normalisation.

Canonical names
---------------
- Regime label (market state): ``"range"``
- Signal type  (trading action): ``"mean_reversion"``

Every boundary that ingests a raw string (Redis, ENV, legacy payload)
must funnel through :func:`normalize_regime` before passing downstream.
"""

from __future__ import annotations

__all__ = [
    "REGIME_RANGE",
    "REGIME_TREND",
    "REGIME_MIXED",
    "REGIME_UNKNOWN",
    "normalize_regime",
    "is_range_regime",
    "is_trend_regime",
]

# ── canonical labels ─────────────────────────────────────────────────
REGIME_RANGE: str = "range"
REGIME_TREND: str = "trend"
REGIME_MIXED: str = "mixed"
REGIME_UNKNOWN: str = "unknown"

# ── alias tables (frozen for O(1) lookup) ────────────────────────────
_RANGE_ALIASES: frozenset[str] = frozenset({
    "range",
    "ranging",
    "meanrev",
    "mean_reversion",
    "mr",
    "chop",
    "sideways",
    "range_bound",
    "range_bullish",
    "range_bearish",
    "range_meanrev",
})

_TREND_DIRECTIONAL: frozenset[str] = frozenset({
    "trending_bull",
    "trending_bear",
    "expansion_bull",
    "expansion_bear",
})

_TREND_ALIASES: frozenset[str] = frozenset({
    "trend",
    "trending",
    "momentum",
    "breakout",
}) | _TREND_DIRECTIONAL


# ── public API ───────────────────────────────────────────────────────

def normalize_regime(raw: str) -> str:
    """Normalise any regime / market-mode string to a canonical label.

    Returns one of: ``"range"``, ``"trend"``, ``"trending_bull"``,
    ``"trending_bear"``, ``"mixed"``, ``"unknown"``.

    Directional trend labels (``trending_bull`` / ``trending_bear``) are
    preserved because downstream routing (SMT / OF policy) uses them.
    """
    s = (raw or "").strip().lower()
    if not s:
        return REGIME_UNKNOWN
    if s in _RANGE_ALIASES:
        return REGIME_RANGE
    if s in _TREND_DIRECTIONAL:
        return s                      # preserve direction
    if s in _TREND_ALIASES:
        return REGIME_TREND
    if s == "mixed":
        return REGIME_MIXED
    if s in ("unknown", "na", "none"):
        return REGIME_UNKNOWN
    # squeeze family → range (conservative)
    if s.startswith("squeeze"):
        return REGIME_RANGE
    return REGIME_UNKNOWN


def is_range_regime(raw: str) -> bool:
    """Return *True* if *raw* normalises to the range regime."""
    return normalize_regime(raw) == REGIME_RANGE


def is_trend_regime(raw: str) -> bool:
    """Return *True* if *raw* normalises to any trend regime."""
    n = normalize_regime(raw)
    return n == REGIME_TREND or n in _TREND_DIRECTIONAL
