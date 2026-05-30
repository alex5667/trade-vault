"""tp1_hit_prob_cdf.py — empirical P_hit(TP1_R) curve producer (Plan 3 Phase 2).

Producer for `ctx.tp1_hit_prob_by_rr` used by core/adaptive_tp1_policy.py.

Definition
----------
For a trade with MFE_R_i (max favourable R-multiple during life), the empirical
estimate of P(TP1@rr hits before SL) is approximated as:

    P_hit(rr) ≈ ECDF_complement(MFE_R, rr)
              = #{i : MFE_R_i >= rr} / N

This is a CONSERVATIVE upper bound. It treats every trade that reached MFE_R >= rr
as if TP1@rr fired (i.e. ignores ordering vs SL hit). For V1 this is acceptable
because:
  (a) we only USE the curve where it strictly dominates baseline (EV-delta gate
      in adaptive policy demands +0.05R safety margin);
  (b) the producer can be replaced by tick-replay path-aware version (v2) without
      changing the consumer contract.

Bucketing
---------
Fallback hierarchy walked by the reader (most specific → most general):

  1. (symbol, kind, regime, direction)
  2. (*,      kind, regime, direction)
  3. (symbol, *,    regime, direction)
  4. (symbol, kind, *,      direction)
  5. (symbol, kind, regime, *         )
  6. (*,      *,    *,      *         )

calibration_ok flag
-------------------
A bucket is marked `calibration_ok=1` iff:
  * n_total >= min_samples
  * curve is monotonically non-increasing within MONOTONE_TOL=0.02
  * max(curve) - min(curve) >= 0.05  (curve has discriminative power)
  * P_hit(min_rr) <= 1.0 - 1e-9 and P_hit(max_rr) >= 0.0 (sanity)
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

ALL = "*"
MONOTONE_TOL = 0.02


# ---------------------------------------------------------------------------
# BucketKey
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BucketKey:
    symbol: str
    kind: str
    regime: str
    direction: str

    def encode(self) -> str:
        return f"{self.symbol}|{self.kind}|{self.regime}|{self.direction}"

    @classmethod
    def decode(cls, s: str) -> "BucketKey":
        parts = (s or "").split("|")
        while len(parts) < 4:
            parts.append(ALL)
        return cls(parts[0], parts[1], parts[2], parts[3])


# ---------------------------------------------------------------------------
# Trade parsing
# ---------------------------------------------------------------------------


def parse_trade_for_phit(fields: dict[str, Any]) -> dict[str, Any] | None:
    """Extract (symbol, kind, regime, direction, mfe_r, is_virtual) from a
    trades:closed entry. Returns None if essential fields missing.

    `mfe_r` extraction order:
      1. direct field `mfe_r` / `max_favorable_r`
      2. derived from `mfe_pnl / one_r_money`
    Trades with non-positive `mfe_r` are kept (contribute to the 0-bucket).
    """
    try:
        symbol = str(fields.get("symbol") or "").upper().strip() or ALL
        kind = str(fields.get("kind") or fields.get("entry_kind") or "").lower().strip() or ALL
        regime = (
            str(fields.get("entry_regime") or "").lower().strip()
            or str(fields.get("regime") or "").lower().strip()
            or str(fields.get("market_regime") or "").lower().strip()
            or ALL
        )
        d_raw = str(fields.get("direction") or fields.get("side") or "").upper().strip()
        if d_raw in {"LONG", "BUY"}:
            direction = "LONG"
        elif d_raw in {"SHORT", "SELL"}:
            direction = "SHORT"
        else:
            direction = ALL

        mfe_r_raw = fields.get("mfe_r")
        if mfe_r_raw in (None, ""):
            mfe_r_raw = fields.get("max_favorable_r")
        if mfe_r_raw in (None, ""):
            mfe_pnl = float(fields.get("mfe_pnl") or 0.0)
            one_r = float(fields.get("one_r_money") or 0.0)
            mfe_r = (mfe_pnl / one_r) if one_r > 1e-9 else 0.0
        else:
            mfe_r = float(mfe_r_raw)

        is_virtual = str(fields.get("is_virtual") or "0").strip() in {"1", "true", "True"}
        return {
            "symbol": symbol,
            "kind": kind,
            "regime": regime,
            "direction": direction,
            "mfe_r": float(mfe_r),
            "is_virtual": is_virtual,
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Bucket aggregation
# ---------------------------------------------------------------------------


@dataclass
class PhitBucket:
    key: BucketKey
    mfe_r_samples: list[float] = field(default_factory=list)

    @property
    def n_total(self) -> int:
        return len(self.mfe_r_samples)


def build_phit_buckets(
    trades: Iterable[dict[str, Any]],
    *,
    include_virtual: bool = True,
) -> dict[str, PhitBucket]:
    """Aggregate trades into all six fallback buckets (per trade).

    For each trade, push MFE_R into:
      (sym, kind, reg, dir), (*, kind, reg, dir), (sym, *, reg, dir),
      (sym, kind, *, dir), (sym, kind, reg, *), (*, *, *, *)
    """
    out: dict[str, PhitBucket] = {}

    def _push(bk: BucketKey, mfe_r: float) -> None:
        enc = bk.encode()
        b = out.get(enc)
        if b is None:
            b = PhitBucket(key=bk)
            out[enc] = b
        b.mfe_r_samples.append(mfe_r)

    for t in trades:
        if t is None:
            continue
        if (not include_virtual) and bool(t.get("is_virtual")):
            continue
        sym = t["symbol"]
        kind = t["kind"]
        reg = t["regime"]
        d = t["direction"]
        mfe = float(t["mfe_r"])
        _push(BucketKey(sym, kind, reg, d), mfe)
        _push(BucketKey(ALL, kind, reg, d), mfe)
        _push(BucketKey(sym, ALL, reg, d), mfe)
        _push(BucketKey(sym, kind, ALL, d), mfe)
        _push(BucketKey(sym, kind, reg, ALL), mfe)
        _push(BucketKey(ALL, ALL, ALL, ALL), mfe)
    return out


# ---------------------------------------------------------------------------
# Curve computation
# ---------------------------------------------------------------------------


def compute_phit_curve(mfe_r_samples: list[float], grid: list[float]) -> dict[str, float]:
    """Return {rr_str: p_hit} for each rr in `grid`.

    p_hit(rr) = #{i: mfe_r_i >= rr} / N    (ECDF complement; conservative).

    Empty samples → empty dict.
    """
    n = len(mfe_r_samples)
    if n == 0 or not grid:
        return {}
    # Sort once; use binary search via bisect_left.
    s = sorted(mfe_r_samples)
    from bisect import bisect_left
    out: dict[str, float] = {}
    for rr in grid:
        try:
            rr_f = float(rr)
        except Exception:
            continue
        # idx = first i with s[i] >= rr  →  count = n - idx
        idx = bisect_left(s, rr_f)
        out[f"{rr_f:.2f}"] = (n - idx) / n
    return out


def is_curve_calibrated(
    curve: dict[str, float],
    *,
    monotone_tol: float = MONOTONE_TOL,
    min_spread: float = 0.05,
) -> bool:
    """True if curve passes calibration sanity:
       * monotone non-increasing within tol;
       * spread (max-min) >= min_spread;
       * each p_hit in [0, 1].
    """
    if not curve:
        return False
    try:
        items = sorted(((float(k), float(v)) for k, v in curve.items()), key=lambda x: x[0])
    except Exception:
        return False
    if len(items) < 2:
        return False
    for _, p in items:
        if not (0.0 <= p <= 1.0):
            return False
    # monotone non-increasing within tolerance
    prev = items[0][1]
    for _, p in items[1:]:
        if p > prev + monotone_tol:
            return False
        prev = min(prev, p + monotone_tol)
    spread = max(p for _, p in items) - min(p for _, p in items)
    return spread >= min_spread


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PhitRecommendation:
    key: BucketKey
    n_total: int
    curve: dict[str, float]
    calibration_ok: bool
    passes: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "n_total": int(self.n_total),
            "curve": dict(self.curve),
            "calibration_ok": 1 if self.calibration_ok else 0,
            "passes": 1 if self.passes else 0,
        }


def build_phit_recommendations(
    buckets: dict[str, PhitBucket],
    *,
    grid: list[float],
    min_samples: int,
) -> dict[str, dict[str, Any]]:
    """For each bucket compute curve and pass/calibration flags.

    Output: {encoded_key: {"n_total", "curve", "calibration_ok", "passes"}}
    `passes=1` iff n_total >= min_samples AND calibration_ok.
    """
    out: dict[str, dict[str, Any]] = {}
    for enc, b in buckets.items():
        curve = compute_phit_curve(b.mfe_r_samples, grid)
        cal_ok = is_curve_calibrated(curve)
        passes = (b.n_total >= int(min_samples)) and cal_ok
        rec = PhitRecommendation(
            key=b.key,
            n_total=b.n_total,
            curve=curve,
            calibration_ok=cal_ok,
            passes=passes,
        )
        out[enc] = rec.to_dict()
    return out


# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------


def lookup_phit_curve(
    buckets: dict[str, dict[str, Any]],
    *,
    symbol: str,
    kind: str,
    regime: str,
    direction: str,
    require_pass: bool = True,
) -> dict[str, Any] | None:
    """Walk fallback hierarchy → return first matching bucket dict.

    Caller can read `["curve"]`, `["n_total"]`, `["calibration_ok"]`, `["passes"]`.
    Returns None if nothing matches.
    """
    sym = (symbol or ALL).upper()
    kd = (kind or ALL).lower()
    rg = (regime or ALL).lower()
    dr = (direction or ALL).upper()
    candidates = [
        BucketKey(sym, kd, rg, dr),
        BucketKey(ALL, kd, rg, dr),
        BucketKey(sym, ALL, rg, dr),
        BucketKey(sym, kd, ALL, dr),
        BucketKey(sym, kd, rg, ALL),
        BucketKey(ALL, ALL, ALL, ALL),
    ]
    for bk in candidates:
        entry = buckets.get(bk.encode())
        if not entry:
            continue
        if require_pass and int(entry.get("passes", 0)) != 1:
            continue
        return entry
    return None
