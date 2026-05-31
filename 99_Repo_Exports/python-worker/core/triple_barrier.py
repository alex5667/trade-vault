from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class BarrierOutcome(StrEnum):
    TP_HIT = "TP_HIT"
    SL_HIT = "SL_HIT"
    TIMEOUT = "TIMEOUT"
    NO_TICKS = "NO_TICKS"


@dataclass(frozen=True)
class BarrierSpec:
    h_ms: int
    tp_bps: float
    sl_bps: float
    # Round-trip transaction cost in bps (spread + 2·fees + slippage estimate).
    # Default 0.0 preserves backward-compatible behavior:
    #   edge_after_cost_bps == realized_close_bps, y_edge_cost_aware == (gross > 0).
    # Callers that want cost-aware labels (López de Prado best practice — barriers
    # should exceed expected round-trip costs) pass a positive value.
    cost_bps: float = 0.0


@dataclass(frozen=True)
class BarrierResult:
    outcome: BarrierOutcome  # TP_HIT | SL_HIT | TIMEOUT | NO_TICKS
    hit_ms: int
    mae_bps: float
    mfe_bps: float
    adverse_proxy: float
    # --- v14: cost-aware label fields (backward-compat defaults: 0.0 / 0) ---
    cost_bps: float = 0.0
    realized_close_bps: float = 0.0   # signed bps move at outcome point (close-side)
    edge_after_cost_bps: float = 0.0  # realized_close_bps - cost_bps
    y_edge_cost_aware: int = 0        # 1 if edge_after_cost_bps > 0, else 0
    # --- timing fields (0 when not reached / no ticks) ---
    tp_hit_first_ms: int = 0          # epoch_ms when TP barrier was first crossed
    sl_hit_first_ms: int = 0          # epoch_ms when SL barrier was first crossed
    time_to_mfe_ms: int = 0           # ms from ts0 to tick that set the MFE peak
    time_to_mae_ms: int = 0           # ms from ts0 to tick that set the MAE peak


def _bps_move(px: float, ref: float) -> float:
    if ref <= 0:
        return 0.0
    return (px - ref) / ref * 10_000.0


def pick_entry_price(path: list[tuple[int, float]]) -> float:
    return float(path[0][1]) if path else 0.0


def pick_entry_price_v2(
    *,
    entry_px_expected: object,
    path: list[tuple[int, float]],
    reason_flags: list[str] | None = None,
) -> tuple[float, str]:
    """Explicit entry-price contract — prefers caller-supplied expected fill.

    Returns (entry_px, fallback_reason). fallback_reason is the empty string
    when the explicit entry_px_expected is used; otherwise one of:

      * "entry_px_fallback_first_tick"  — entry_px_expected absent/non-positive,
                                          fell back to first tick price.
      * "entry_px_fallback_no_path"     — neither explicit nor any tick available;
                                          returns 0.0 and the caller MUST treat
                                          the sample as bad_entry_px.

    When reason_flags list is provided, the chosen flag is appended (additive,
    so multiple labelers can accumulate flags in the same list).
    """
    explicit = 0.0
    if entry_px_expected is not None:
        try:
            explicit = float(entry_px_expected)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            explicit = 0.0

    if explicit > 1e-9:
        return explicit, ""

    if path:
        try:
            px = float(path[0][1])  # type: ignore[arg-type]
        except (TypeError, ValueError):
            px = 0.0
        if px > 1e-9:
            flag = "entry_px_fallback_first_tick"
            if reason_flags is not None:
                reason_flags.append(flag)
            return px, flag

    flag = "entry_px_fallback_no_path"
    if reason_flags is not None:
        reason_flags.append(flag)
    return 0.0, flag


def label_path(
    *,
    ts0_ms: int,
    direction: str,
    entry_px: float,
    path: list[tuple[int, float]],  # ascending
    spec: BarrierSpec,
) -> BarrierResult:
    if entry_px <= 1e-9 or not path:
        return BarrierResult(
            outcome=BarrierOutcome.NO_TICKS, hit_ms=0,
            mae_bps=0.0, mfe_bps=0.0, adverse_proxy=0.0,
            cost_bps=spec.cost_bps,
            realized_close_bps=0.0,
            edge_after_cost_bps=-spec.cost_bps,  # paid cost, realized nothing
            y_edge_cost_aware=0,
        )

    assert entry_px > 1e-9, f"entry_px must be positive, got {entry_px}"

    d = (direction or "").upper()
    is_long = d == "LONG"
    tp = spec.tp_bps
    sl = spec.sl_bps
    cost = spec.cost_bps

    def signed_bps(px: float) -> float:
        b = _bps_move(px, entry_px)
        return b if is_long else -b

    mae = 0.0  # most negative (adverse)
    mfe = 0.0  # most positive (favorable)
    hit_ms = ts0_ms + spec.h_ms
    outcome = BarrierOutcome.TIMEOUT
    last_sb = 0.0  # signed_bps at last seen tick (used for TIMEOUT realized close)
    realized_close_bps = 0.0

    # Timing accumulators
    tp_hit_first_ms: int = 0
    sl_hit_first_ms: int = 0
    time_to_mfe_ts: int = ts0_ms   # epoch_ms of the tick that set MFE peak
    time_to_mae_ts: int = ts0_ms   # epoch_ms of the tick that set MAE peak

    for ts, px in path:
        ts_i = int(ts)
        sb = signed_bps(px)
        last_sb = sb

        if sb > mfe:
            mfe = sb
            time_to_mfe_ts = ts_i
        if sb < mae:
            mae = sb
            time_to_mae_ts = ts_i

        if sb >= tp:
            if tp_hit_first_ms == 0:
                tp_hit_first_ms = ts_i
            outcome = BarrierOutcome.TP_HIT
            hit_ms = ts_i
            realized_close_bps = sb
            break
        if sb <= -sl:
            if sl_hit_first_ms == 0:
                sl_hit_first_ms = ts_i
            outcome = BarrierOutcome.SL_HIT
            hit_ms = ts_i
            realized_close_bps = sb
            break
    else:
        # No break → TIMEOUT path: close at last observed tick
        realized_close_bps = last_sb

    mae_mag = abs(mae)
    mfe_mag = max(0.0, mfe)
    adverse_proxy = (mae_mag / mfe_mag) if mfe_mag > 1e-9 else mae_mag

    edge_after_cost_bps = realized_close_bps - cost
    # STRICT cost-aware label: positive ONLY if TP barrier was hit AND net edge > 0.
    # Rationale: aligns with legacy y_edge (which gates on TP_HIT), so flips reflect
    # cost effect rather than a broader outcome definition. TIMEOUT with marginal drift
    # is NOT counted as positive — closing at TIMEOUT carries assumption-of-close risk
    # that we don't want to encode as a "win".
    y_edge_cost_aware = 1 if (outcome == BarrierOutcome.TP_HIT and edge_after_cost_bps > 0.0) else 0

    return BarrierResult(
        outcome=outcome, hit_ms=hit_ms,
        mae_bps=mae_mag, mfe_bps=mfe_mag, adverse_proxy=adverse_proxy,
        cost_bps=cost,
        realized_close_bps=realized_close_bps,
        edge_after_cost_bps=edge_after_cost_bps,
        y_edge_cost_aware=y_edge_cost_aware,
        tp_hit_first_ms=tp_hit_first_ms,
        sl_hit_first_ms=sl_hit_first_ms,
        time_to_mfe_ms=max(0, time_to_mfe_ts - ts0_ms),
        time_to_mae_ms=max(0, time_to_mae_ts - ts0_ms),
    )
