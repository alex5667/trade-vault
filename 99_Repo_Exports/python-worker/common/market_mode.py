from __future__ import annotations

"""common.market_mode

Single source of truth for regime / market-mode label normalisation.

Canonical names
---------------
- Regime label (market state): ``"range"``
- Signal type  (trading action): ``"mean_reversion"``

Every boundary that ingests a raw string (Redis, ENV, legacy payload)
must funnel through :func:`normalize_regime` before passing downstream.
"""


__all__ = [
    "REGIME_RANGE",
    "REGIME_TREND",
    "REGIME_MIXED",
    "REGIME_UNKNOWN",
    "REGIME_EXPANSION_BULL",
    "REGIME_EXPANSION_BEAR",
    "normalize_regime",
    "is_range_regime",
    "is_trend_regime",
    "regime_to_id",
]

# ── canonical labels ─────────────────────────────────────────────────
REGIME_RANGE: str = "range"
REGIME_TREND: str = "trend"
REGIME_MIXED: str = "mixed"
REGIME_UNKNOWN: str = "unknown"
REGIME_EXPANSION_BULL: str = "expansion_bull"
REGIME_EXPANSION_BEAR: str = "expansion_bear"

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


REGIME_ID: dict[str, float] = {
    "unknown": -1.0,
    "mixed": 0.0,
    "range": 1.0,
    "trend": 2.0,
    "trending_bull": 2.1,
    "trending_bear": 2.2,
    # expansion_bull/bear: higher ATR/volatility than trending_bull/bear →
    # distinct ML signal for wider spread/slippage/entry policy.
    "expansion_bull": 3.1,
    "expansion_bear": 3.2,
}


def regime_to_id(raw: str) -> float:
    """Map regime string to numeric ID.

    expansion_bull/bear → 3.1/3.2 (ML-distinct from trend 2.x).
    unknown → -1.0 (never equals range=1.0).
    """
    normalized = normalize_regime(raw)
    return REGIME_ID.get(normalized, -1.0)

