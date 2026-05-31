from __future__ import annotations

"""path_based_tp_cdf.py — distribution-aware TP1 recommender (Plan 3.3).

Idea (López de Prado / Triple-Barrier path-based exit):
  Historical winners have an MFE distribution. The classic geometric
  TP1 = k×ATR or k×R is arbitrary — it ignores how far past winners
  actually travel. Path-based TP picks `TP1_target_R = Pq(MFE_R | winner)`,
  so by construction at least (1-q) fraction of historical winners would
  have hit TP1 at that level.

Bucketing:
  `(symbol, regime, direction)` — distributions vary materially by:
    - symbol (BTC ≠ PEPE in tail behavior),
    - regime (range/squeeze/expansion/trend),
    - direction (LONG vs SHORT asymmetry, esp. counter-trend).

This module is pure compute (no Redis / IO). Used by the autocal service
to build the published bundle, and by tests.
"""

from dataclasses import dataclass, field
from typing import Any, Iterable


# Sentinel for "all" in fallback hierarchy.
ALL = "*"


@dataclass(frozen=True)
class BucketKey:
    symbol: str
    regime: str
    direction: str

    def encode(self) -> str:
        return f"{self.symbol}|{self.regime}|{self.direction}"

    @classmethod
    def decode(cls, s: str) -> "BucketKey":
        parts = s.split("|", 2)
        while len(parts) < 3:
            parts.append(ALL)
        return cls(parts[0], parts[1], parts[2])


@dataclass
class BucketCDF:
    key: BucketKey
    # Sorted (ascending) MFE in R-units among winners only.
    mfe_r_sorted: list[float] = field(default_factory=list)
    n_total: int = 0   # all eligible trades in bucket (winners + losers)
    n_winners: int = 0
    # Optional: store unsorted samples too for downstream analytics.
    @property
    def winner_rate(self) -> float:
        return self.n_winners / self.n_total if self.n_total > 0 else 0.0

    def percentile(self, q: float) -> float:
        """Linear-interp percentile (NumPy-style) of MFE_R among winners.

        q in [0.0, 1.0]. Returns 0.0 when sample empty.
        """
        n = len(self.mfe_r_sorted)
        if n == 0:
            return 0.0
        q = max(0.0, min(1.0, q))
        if n == 1:
            return float(self.mfe_r_sorted[0])
        pos = q * (n - 1)
        lo = int(pos)
        hi = min(lo + 1, n - 1)
        frac = pos - lo
        return float(self.mfe_r_sorted[lo] * (1.0 - frac) + self.mfe_r_sorted[hi] * frac)


@dataclass(frozen=True)
class TpRecommendation:
    """Per-bucket TP1_R recommendation derived from the CDF."""
    key: BucketKey
    tp1_r: float
    p25: float
    p50: float
    p75: float
    n_winners: int
    n_total: int
    winner_rate: float
    passes: int  # 1 if bucket has enough winners AND tp1_r within sanity bounds

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key.encode(),
            "tp1_r": round(self.tp1_r, 4),
            "p25": round(self.p25, 4),
            "p50": round(self.p50, 4),
            "p75": round(self.p75, 4),
            "n_winners": self.n_winners,
            "n_total": self.n_total,
            "winner_rate": round(self.winner_rate, 4),
            "passes": int(self.passes),
        }


# -------------------------------------------------------------------------- #
# Parsing & bucketing
# -------------------------------------------------------------------------- #

def parse_trade_for_cdf(fields: dict[str, Any]) -> dict[str, Any] | None:
    """Extract bucket dims + mfe_r + winner-flag from a trades:closed entry.

    Returns None when the trade is unusable (missing entry/risk/mfe).
    Winner definition (strict): r_multiple > 0 — realized positive R.
    """
    try:
        symbol = str(fields.get("symbol") or "").upper().strip() or ALL
        # direction: TradeClosed canon is `direction` (LONG/SHORT)
        d_raw = str(fields.get("direction") or fields.get("side") or "").upper().strip()
        if d_raw in ("LONG", "BUY"):
            direction = "LONG"
        elif d_raw in ("SHORT", "SELL"):
            direction = "SHORT"
        else:
            direction = ALL
        # regime: prefer entry_regime, fall back to regime / market_regime
        regime = (
            str(fields.get("entry_regime") or "").lower().strip()
            or str(fields.get("regime") or "").lower().strip()
            or str(fields.get("market_regime") or "").lower().strip()
            or ALL
        )
        # MFE_R: prefer direct field, else derive from mfe_pnl / one_r_money.
        mfe_r_raw = fields.get("mfe_r")
        if mfe_r_raw is None or mfe_r_raw == "":
            mfe_r_raw = fields.get("max_favorable_r")
        if mfe_r_raw is None or mfe_r_raw == "":
            mfe_pnl = float(fields.get("mfe_pnl") or 0.0)
            one_r = float(fields.get("one_r_money") or 0.0)
            mfe_r = (mfe_pnl / one_r) if one_r > 1e-9 else 0.0
        else:
            mfe_r = float(mfe_r_raw)
        pnl_r = float(fields.get("r_multiple") or fields.get("pnl_r") or 0.0)
        tp_hits = int(float(fields.get("tp_hits") or 0))
        is_virtual = str(fields.get("is_virtual") or "0").strip() in ("1", "true", "True")
        # Winner — strict on realized R. (Pure-MFE winners that closed via
        # SL after retracement are NOT counted; they would inflate the
        # recommendation and break the "TP would have captured" assumption.)
        is_winner = (pnl_r > 0.0) or (tp_hits >= 1 and mfe_r > 0.0)
        return {
            "symbol": symbol,
            "regime": regime,
            "direction": direction,
            "mfe_r": mfe_r,
            "pnl_r": pnl_r,
            "tp_hits": tp_hits,
            "is_virtual": is_virtual,
            "is_winner": is_winner,
        }
    except Exception:
        return None


def build_cdf_buckets(
    trades: Iterable[dict[str, Any]],
    *,
    include_virtual: bool = True,
) -> dict[str, BucketCDF]:
    """Bucket trades into (symbol, regime, direction) and global aggregates.

    For each trade, we update FIVE buckets in the fallback hierarchy:
      1. (symbol, regime, direction)   — most specific
      2. (*,      regime, direction)
      3. (symbol, *,      direction)
      4. (*,      *,      direction)
      5. (*,      *,      *)           — global

    This guarantees a non-empty fallback for any caller key, while still
    letting the picker prefer the most specific bucket with enough winners.

    Output: dict[encoded_key, BucketCDF]. mfe_r_sorted is sorted ascending.
    """
    buckets: dict[str, BucketCDF] = {}

    def _add(bk: BucketKey, mfe_r: float, is_winner: bool) -> None:
        enc = bk.encode()
        b = buckets.get(enc)
        if b is None:
            b = BucketCDF(key=bk)
            buckets[enc] = b
        b.n_total += 1
        if is_winner:
            b.n_winners += 1
            b.mfe_r_sorted.append(float(mfe_r))

    for t in trades:
        if not include_virtual and t.get("is_virtual"):
            continue
        sym = t["symbol"]
        rg = t["regime"]
        dr = t["direction"]
        mfe = float(t["mfe_r"])
        win = bool(t["is_winner"])
        # NB: any-of-{sym, rg, dr} being '*' from parse step means we still
        # add a single sample to that slot — that's fine, it just collapses
        # specificity, not double-counts.
        _add(BucketKey(sym, rg, dr), mfe, win)
        if sym != ALL:
            _add(BucketKey(ALL, rg, dr), mfe, win)
        if rg != ALL:
            _add(BucketKey(sym, ALL, dr), mfe, win)
        if sym != ALL and rg != ALL:
            _add(BucketKey(ALL, ALL, dr), mfe, win)
        _add(BucketKey(ALL, ALL, ALL), mfe, win)

    # Sort once at the end (cheaper than insertion-sort per-add).
    for b in buckets.values():
        b.mfe_r_sorted.sort()
    return buckets


# -------------------------------------------------------------------------- #
# Recommendation
# -------------------------------------------------------------------------- #

def recommend_tp1_r(
    bucket: BucketCDF,
    *,
    quantile: float = 0.5,
    min_winners: int = 30,
    tp1_r_min: float = 0.20,
    tp1_r_max: float = 1.50,
) -> TpRecommendation:
    """Pick TP1_R as bucket.percentile(quantile) clipped to [min, max].

    `passes` = 1 iff:
      - n_winners >= min_winners (enough sample for stable estimate)
      - raw percentile is within [tp1_r_min, tp1_r_max] (post-clip equals raw)

    The clip-but-don't-pass behavior prevents pathological recommendations
    (e.g. p50 = 0.05 from a degenerate regime) from being enforced, while
    still publishing the raw stats for observability.
    """
    p25 = bucket.percentile(0.25)
    p50 = bucket.percentile(0.50)
    p75 = bucket.percentile(0.75)
    raw = bucket.percentile(quantile)
    clipped = max(tp1_r_min, min(tp1_r_max, raw))
    passes = (
        bucket.n_winners >= min_winners
        and tp1_r_min <= raw <= tp1_r_max
    )
    return TpRecommendation(
        key=bucket.key,
        tp1_r=clipped,
        p25=p25,
        p50=p50,
        p75=p75,
        n_winners=bucket.n_winners,
        n_total=bucket.n_total,
        winner_rate=bucket.winner_rate,
        passes=1 if passes else 0,
    )


def build_recommendations(
    buckets: dict[str, BucketCDF],
    *,
    quantile: float = 0.5,
    min_winners: int = 30,
    tp1_r_min: float = 0.20,
    tp1_r_max: float = 1.50,
) -> dict[str, TpRecommendation]:
    return {
        enc: recommend_tp1_r(
            b,
            quantile=quantile,
            min_winners=min_winners,
            tp1_r_min=tp1_r_min,
            tp1_r_max=tp1_r_max,
        )
        for enc, b in buckets.items()
    }


def lookup_recommendation(
    recs: dict[str, dict[str, Any]],
    *,
    symbol: str,
    regime: str,
    direction: str,
) -> dict[str, Any] | None:
    """Fallback lookup over published bucket map.

    `recs` is the publishable form (encoded_key → dict-of-fields), so this
    function works both on `build_recommendations(...).items()` (after
    `to_dict()`) and on Redis-loaded snapshots without conversion.

    Returns the first bucket dict with `passes == 1` (or None).
    """
    sym = (symbol or ALL).upper()
    rg = (regime or ALL).lower()
    dr = (direction or ALL).upper()

    candidates = [
        BucketKey(sym, rg, dr),
        BucketKey(ALL, rg, dr),
        BucketKey(sym, ALL, dr),
        BucketKey(ALL, ALL, dr),
        BucketKey(ALL, ALL, ALL),
    ]
    for bk in candidates:
        entry = recs.get(bk.encode())
        if not entry:
            continue
        if int(entry.get("passes", 0)) != 1:
            continue
        return entry
    return None
