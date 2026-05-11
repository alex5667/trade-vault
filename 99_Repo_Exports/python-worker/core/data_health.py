from __future__ import annotations

# -*- coding: utf-8 -*-
"""core.data_health

Unified Data Quality / Health layer.

Why:
  A large share of false positives in event-driven trading systems comes from
  data issues (bad time, stale / low-rate books, missing BBO, dual sources
  causing CVD jumps), not from the signal logic.

Design:
  - Fail-open for the pipeline: never crash signal generation.
  - Fail-closed for *evidences*: when health is low, disable only the evidences
    that become unreliable (book-based, time-based), while keeping price/delta
    based features available.
  - Deterministic: computed from the same tick/bar inputs as the decision.
  - Audit-friendly: returns component flags + reasons.

Score semantics:
  health in [0..1]
    1.0 = all checks OK
    0.0 = hard failure (missing timestamps / clearly broken source)
"""


from dataclasses import asdict, dataclass
from typing import Any


def _b(x: Any) -> int:
    try:
        if isinstance(x, bool):
            return 1 if x else 0
        if x is None:
            return 0
        return 1 if str(x).strip().lower() in {"1", "true", "yes", "on"} else 0
    except Exception:
        return 0


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


@dataclass
class DataHealth:
    # aggregate
    score: float = 1.0
    reasons: list[str] = None  # type: ignore

    # components (0/1)
    tick_time_ok: int = 1
    book_health_ok: int = 1
    spread_ok: int = 1
    source_consistency_ok: int = 1

    # supporting metrics (optional)
    tick_oood: int = 0
    tick_gap_ms: int = 0
    book_age_ms: int = 0
    book_rate_hz: float = 0.0
    spread_bps: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["reasons"] = list(self.reasons or [])
        return d


def compute_data_health(*, indicators: dict[str, Any], cfg: dict[str, Any]) -> DataHealth:
    """Compute data health from indicators + config.

    Expected inputs in `indicators` (best-effort):
      - tick_ts_missing (0/1)
      - ticks_out_of_order (0/1)
      - tick_gap_ms (int)
      - book_health_ok (0/1)
      - book_age_ms (int)
      - book_rate_hz (float)
      - spread_bps (float) or microbar_spread_bps
      - source_consistency_ok (0/1)

    Config keys (all optional, safe defaults):
      - data_health_spread_max_bp: float (if spread known)
      - data_health_book_age_max_ms: int
      - data_health_min_book_hz: float (if book_rate_hz known)
    """

    reasons: list[str] = []

    # --- Tick time ---
    tick_ts_missing = _b(indicators.get("tick_ts_missing", 0))
    tick_oood = _b(indicators.get("tick_oood", indicators.get("ticks_out_of_order", 0)))
    tick_gap_ms = _i(indicators.get("tick_gap_ms", 0), 0)
    tick_time_ok = 1
    if tick_ts_missing == 1:
        tick_time_ok = 0
        reasons.append("tick_ts_missing")
    if tick_oood == 1:
        # out-of-order is not always fatal, but should degrade
        reasons.append("tick_out_of_order")
    if tick_gap_ms > 0:
        # gap means missing trades (ingestion) -> degrade
        reasons.append("tick_gap")

    # --- Trade-id ordering (Binance aggTradeId continuity) ---
    # Distinct from time-based tick_gap_ms / tick_oood.
    try:
        tick_id_gap = _b(indicators.get("tick_id_gap", 0))
        tick_id_dup = _b(indicators.get("tick_id_dup", 0))
        tick_id_reorder = _b(indicators.get("tick_id_reorder", 0))
        tick_gap_count = _i(indicators.get("tick_gap_count", 0), 0)
        tick_dup_count = _i(indicators.get("tick_dup_count", 0), 0)
        tick_reorder_count = _i(indicators.get("tick_reorder_count", 0), 0)
    except Exception:
        tick_id_gap = tick_id_dup = tick_id_reorder = 0
        tick_gap_count = tick_dup_count = tick_reorder_count = 0

    if tick_id_gap == 1 or tick_gap_count > 0:
        reasons.append("tick_id_gap")
    if tick_id_reorder == 1 or tick_reorder_count > 0:
        reasons.append("tick_id_reorder")
    if tick_id_dup == 1 or tick_dup_count > 0:
        reasons.append("tick_id_dup")

    # --- Book health ---
    book_health_ok = _i(indicators.get("book_health_ok", 1), 1)
    book_age_ms = _i(indicators.get("book_age_ms", 0), 0)
    book_rate_hz = _f(indicators.get("book_rate_hz", indicators.get("book_rate_ema", 0.0)), 0.0)
    # optional hard checks
    max_age = _i(cfg.get("data_health_book_age_max_ms", cfg.get("book_age_max_ms", 0)), 0)
    if max_age > 0 and book_age_ms > max_age:
        book_health_ok = 0
        reasons.append("book_age")
    min_hz = _f(cfg.get("data_health_min_book_hz", 0.0), 0.0)
    if min_hz > 0 and book_rate_hz > 0 and book_rate_hz < min_hz:
        book_health_ok = 0
        reasons.append("book_rate_low")

    # --- Spread ---
    spread_bps = _f(indicators.get("spread_bps", indicators.get("microbar_spread_bps", 0.0)), 0.0)
    spread_ok = 1
    spr_max = _f(cfg.get("data_health_spread_max_bp", 0.0), 0.0)
    if spr_max > 0 and spread_bps > 0 and spread_bps > spr_max:
        spread_ok = 0
        reasons.append("spread_wide")

    # --- Source consistency ---
    source_consistency_ok = _i(indicators.get("source_consistency_ok", 1), 1)
    if source_consistency_ok == 0:
        reasons.append("source_inconsistent")

    # --- Aggregate score ---
    # Weighted, with a hard fail for missing tick timestamps.
    if tick_time_ok == 0:
        score = 0.0
    else:
        # degrade factors
        score = 1.0
        if tick_oood == 1:
            score *= 0.85
        if tick_gap_ms >= 2000:
            score *= 0.85
        # trade-id ordering degradation (soft by default; configurable)
        if tick_id_gap == 1 or tick_gap_count > 0:
            score *= float(cfg.get("data_health_tick_id_gap_mul", 0.85))
        if tick_id_reorder == 1 or tick_reorder_count > 0:
            score *= float(cfg.get("data_health_tick_id_reorder_mul", 0.95))
        if tick_id_dup == 1 or tick_dup_count > 0:
            score *= float(cfg.get("data_health_tick_id_dup_mul", 0.98))
        if book_health_ok == 0:
            score *= 0.60
        if spread_ok == 0:
            score *= 0.80
        if source_consistency_ok == 0:
            score *= 0.30
        # clamp
        score = max(0.0, min(1.0, float(score)))

    return DataHealth(
        score=float(score),
        reasons=list(reasons),
        tick_time_ok=int(tick_time_ok),
        book_health_ok=int(book_health_ok),
        spread_ok=int(spread_ok),
        source_consistency_ok=int(source_consistency_ok),
        tick_oood=int(tick_oood),
        tick_gap_ms=int(tick_gap_ms),
        book_age_ms=int(book_age_ms),
        book_rate_hz=float(book_rate_hz),
        spread_bps=float(spread_bps),
    )


def apply_book_evidence_policy(*, indicators: dict[str, Any], dh: DataHealth, cfg: dict[str, Any]) -> None:
    """Fail-closed for book-based evidences when data health is low.

    This helper only annotates indicators. Consumers should:
      - veto OBI/Iceberg contribution in StrongGate
      - require alternative evidences in FSM/Entry policy
    """
    try:
        min_score = float(cfg.get("data_health_min_for_book_evidence", 0.70))
    except Exception:
        min_score = 0.70

    # already unhealthy by book health
    if int(dh.book_health_ok) == 0 or float(dh.score) < float(min_score):
        indicators["book_evidence_allowed"] = 0
        indicators["book_evidence_block_reason"] = "data_health" if float(dh.score) < float(min_score) else "book_health"
    else:
        indicators["book_evidence_allowed"] = 1


def apply_shadow_only_policy(*, indicators: dict[str, Any], dh: DataHealth, cfg: dict[str, Any]) -> None:
    """Mark signal as shadow-only below threshold.

    This does NOT block the pipeline; it only signals the publisher/routers
    to keep the signal for audit, not execution.
    """
    try:
        thr = float(cfg.get("data_health_shadow_only_below", 0.40))
    except Exception:
        thr = 0.40
    if float(dh.score) < float(thr):
        indicators["data_health_shadow_only"] = 1
        indicators["data_health_shadow_reason"] = ",".join(list(dh.reasons or [])[:5])
    else:
        indicators["data_health_shadow_only"] = 0
