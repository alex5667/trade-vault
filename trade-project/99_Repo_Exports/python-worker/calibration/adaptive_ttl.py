"""
adaptive_ttl.py — Phase 2.3 distribution-aware barrier tuning.

Reads resolved signal_outcome rows; per (symbol, regime, direction)
recommends:
  * tp_r       = p50(mfe_r | label == +1) — median MFE among winners
  * sl_r       = max(min_sl_r, -p10(mae_r))  — robust 10th percentile of MAE
  * ttl_ms     = unchanged in this phase (TTL tuning needs path data,
                 see Phase 0 path_based_tp_cdf for finer logic)

Outputs a recommendation snapshot suitable to publish to Redis
(consumers may apply it as an override; current writer reads
fallbacks via ENV).

Pure-Python module; the periodic service wrapper lives in
`orderflow_services/adaptive_ttl_publisher_v1.py`.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Iterable


_MIN_SAMPLES_DEFAULT = 50
_MIN_SL_R = 0.5
_MAX_SL_R = 3.0
_MIN_TP_R = 0.3
_MAX_TP_R = 3.0


@dataclass(frozen=True)
class BarrierRec:
    symbol: str
    regime: str
    direction: int  # +1 / -1
    n: int
    win_rate: float
    tp_r: float
    sl_r: float
    median_mfe_r: float
    p10_mae_r: float


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def recommend(
    rows: Iterable[dict],
    min_samples: int = _MIN_SAMPLES_DEFAULT,
    min_sl_r: float = _MIN_SL_R,
) -> list[BarrierRec]:
    """rows: dicts with keys symbol, regime, side, label, mfe_r, mae_r."""
    by_group: dict[tuple[str, str, int], list[dict]] = {}
    for r in rows:
        key = (
            str(r.get("symbol", "")).upper(),
            str(r.get("regime", "") or "na").lower(),
            int(r.get("side", 0)),
        )
        if not key[0] or key[2] == 0:
            continue
        by_group.setdefault(key, []).append(r)

    out: list[BarrierRec] = []
    for (sym, regime, side), group in by_group.items():
        if len(group) < min_samples:
            continue
        winners = [g for g in group if int(g.get("label") or 0) == 1]
        wr = len(winners) / len(group)
        # mfe_r over winners only — what TP could realistically capture
        mfe_winners = [
            float(g["mfe_r"]) for g in winners if g.get("mfe_r") is not None
        ]
        if mfe_winners:
            median_mfe = _percentile(mfe_winners, 50.0)
        else:
            median_mfe = 1.0
        # mae_r over all — how deep adverse moves got
        # mae_r is negative (excursion in R); we want |p10|
        mae_all = [
            float(g["mae_r"]) for g in group if g.get("mae_r") is not None
        ]
        if mae_all:
            p10_mae = _percentile(mae_all, 10.0)  # most negative tail
        else:
            p10_mae = -1.0

        tp_r = max(_MIN_TP_R, min(_MAX_TP_R, median_mfe))
        sl_r = max(min_sl_r, min(_MAX_SL_R, abs(p10_mae)))

        out.append(
            BarrierRec(
                symbol=sym,
                regime=regime,
                direction=side,
                n=len(group),
                win_rate=wr,
                tp_r=tp_r,
                sl_r=sl_r,
                median_mfe_r=median_mfe,
                p10_mae_r=p10_mae,
            )
        )
    return out


def to_redis_payload(recs: list[BarrierRec], generated_at_ms: int) -> dict:
    """Compact payload for ADAPTIVE_TTL_STATE Redis key."""
    return dict(
        v=1,
        generated_at_ms=generated_at_ms,
        n=len(recs),
        recs=[asdict(r) for r in recs],
    )
