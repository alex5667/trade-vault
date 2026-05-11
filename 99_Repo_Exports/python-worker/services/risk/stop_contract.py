"""
stop_contract.py — Formal Stop-Risk Contract

Computes the effective stop distance as:

    effective_stop_bps = max(
        strategy_structure_stop_bps,
        ATR * stop_atr_mult  [in bps],
        spread_bps + slippage_p95_bps + fee_bps + buffer_bps,
        exchange_min_stop_bps,
        noise_q90_bps,
    )

If the effective stop is too wide → caller should DENY the trade,
not silently tighten the stop.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StopContractResult:
    effective_stop_bps: float
    strategy_stop_bps: float
    atr_stop_bps: float          # atr_bps * stop_atr_mult
    cost_floor_bps: float        # spread + slippage_p95 + fee + buffer
    exchange_min_bps: float
    noise_floor_bps: float
    binding_component: str       # which component won max()
    ok: bool
    reason: str = ""

    @property
    def candidates(self) -> dict[str, float]:
        return {
            "strategy":      self.strategy_stop_bps,
            "atr_mult":      self.atr_stop_bps,
            "cost_floor":    self.cost_floor_bps,
            "exchange_min":  self.exchange_min_bps,
            "noise_floor":   self.noise_floor_bps,
        }


# ---------------------------------------------------------------------------
# Core contract computation
# ---------------------------------------------------------------------------

def compute_effective_stop_contract(
    *,
    strategy_stop_bps: float,
    atr_bps: float,
    stop_atr_mult: float,
    spread_bps: float,
    slippage_p95_bps: float,
    fee_bps: float,
    exchange_min_stop_bps: float,
    noise_q90_bps: float,
    buffer_bps: float = 2.0,
) -> StopContractResult:
    """
    Compute the effective (widest-safe) stop distance in basis points.

    Parameters
    ----------
    strategy_stop_bps   : structure-based stop from signal logic (bps)
    atr_bps             : current ATR expressed in bps (ATR/price * 10_000)
    stop_atr_mult       : multiplier applied to ATR (may be post-SLQ)
    spread_bps          : half-spread (or full spread) in bps
    slippage_p95_bps    : 95th-percentile slippage in bps
    fee_bps             : total round-trip fee in bps
    exchange_min_stop_bps : exchange-enforced minimum stop distance in bps
    noise_q90_bps       : micro-noise 90th-percentile in bps (price jitter)
    buffer_bps          : extra safety buffer added to cost floor (default 2)

    Returns
    -------
    StopContractResult  : effective stop + breakdown + binding component
    """
    if atr_bps < 0 or strategy_stop_bps < 0:
        return StopContractResult(
            effective_stop_bps=0.0,
            strategy_stop_bps=strategy_stop_bps,
            atr_stop_bps=0.0,
            cost_floor_bps=0.0,
            exchange_min_bps=exchange_min_stop_bps,
            noise_floor_bps=noise_q90_bps,
            binding_component="invalid",
            ok=False,
            reason="negative_input",
        )

    atr_stop = atr_bps * stop_atr_mult
    cost_floor = spread_bps + slippage_p95_bps + fee_bps + buffer_bps

    candidates: dict[str, float] = {
        "strategy":     max(0.0, strategy_stop_bps),
        "atr_mult":     max(0.0, atr_stop),
        "cost_floor":   max(0.0, cost_floor),
        "exchange_min": max(0.0, exchange_min_stop_bps),
        "noise_floor":  max(0.0, noise_q90_bps),
    }

    binding = max(candidates, key=lambda k: candidates[k])
    effective = candidates[binding]

    return StopContractResult(
        effective_stop_bps=effective,
        strategy_stop_bps=strategy_stop_bps,
        atr_stop_bps=atr_stop,
        cost_floor_bps=cost_floor,
        exchange_min_bps=exchange_min_stop_bps,
        noise_floor_bps=noise_q90_bps,
        binding_component=binding,
        ok=True,
    )


# ---------------------------------------------------------------------------
# Noise floor helper (standalone, for use in position_sizing)
# ---------------------------------------------------------------------------

def compute_stop_noise_floor_bps(
    *,
    atr_bps: float,
    spread_bps: float,
    slippage_p95_bps: float,
    fee_bps: float,
    micro_noise_q90_bps: float,
    exchange_min_stop_bps: float,
    atr_floor_mult: float = 0.80,
    buffer_bps: float = 2.0,
) -> float:
    """
    Minimum acceptable stop distance in bps.

    A planned stop below this threshold is inside market noise and
    has no statistical edge — should be denied.

    Returns the maximum of:
      - atr_floor_mult * atr_bps          (ATR-proportional floor)
      - micro_noise_q90_bps               (empirical micro-noise)
      - spread + slippage_p95 + fee + buf (cost-of-entry floor)
      - exchange_min_stop_bps             (hard exchange limit)
    """
    return max(
        atr_floor_mult * atr_bps,
        micro_noise_q90_bps,
        spread_bps + slippage_p95_bps + fee_bps + buffer_bps,
        exchange_min_stop_bps,
    )


# ---------------------------------------------------------------------------
# ENV helpers
# ---------------------------------------------------------------------------

def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "")
        if v and v.strip():
            return float(v)
    except Exception:
        pass
    return default


def _env_on(name: str, default: str = "0") -> bool:
    v = (os.getenv(name, default) or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


# ---------------------------------------------------------------------------
# Context-aware facade (used by position_sizing.py)
# ---------------------------------------------------------------------------

def evaluate_stop_noise_floor_from_ctx(
    ctx: Any,
    symbol: str,
    *,
    atr_floor_mult: float | None = None,
    buffer_bps: float | None = None,
) -> tuple[float, bool]:
    """
    Extract noise-floor inputs from ctx and compute the floor.

    Returns (noise_floor_bps, floor_enabled).
    Returns (0.0, False) if STOP_NOISE_FLOOR_ENABLE is not set.
    """
    if not _env_on("STOP_NOISE_FLOOR_ENABLE"):
        return 0.0, False

    if atr_floor_mult is None:
        atr_floor_mult = _env_float("STOP_NOISE_ATR_FLOOR_MULT", 0.80)
    if buffer_bps is None:
        buffer_bps = _env_float("STOP_NOISE_SPREAD_SLIP_FEE_BUFFER_BPS", 2.0)

    # ATR in bps
    atr_bps = float(getattr(ctx, "atr_bps", 0.0) or 0.0)
    if atr_bps <= 0:
        # Fallback: reconstruct from atr_price and entry_price
        atr_price = float(getattr(ctx, "atr", 0.0) or 0.0)
        entry = float(getattr(ctx, "entry_price", 1.0) or 1.0)
        if entry > 0 and atr_price > 0:
            atr_bps = (atr_price / entry) * 10_000.0

    spread_bps = float(getattr(ctx, "spread_bps", 0.0) or 0.0)
    slippage_p95_bps = float(getattr(ctx, "slippage_p95_bps", 0.0) or 0.0)
    fee_bps = _env_float("COST_TOTAL_BPS", 8.0)
    micro_noise_q90_bps = float(getattr(ctx, "micro_noise_q90_bps", 0.0) or 0.0)
    exchange_min_bps = _env_float("RISK_MIN_STOP_DISTANCE_BPS", 8.0)

    # Symbol-specific exchange min override
    sym_full = symbol.upper().replace("-", "").replace("/", "")
    sym_base = sym_full.replace("USDT", "").replace("USDC", "").replace("BUSD", "")

    for variant in (sym_full, sym_base):
        override = _env_float(f"RISK_MIN_STOP_DISTANCE_BPS__{variant}", -1.0)
        if override > 0:
            exchange_min_bps = override
            break

    floor = compute_stop_noise_floor_bps(
        atr_bps=atr_bps,
        spread_bps=spread_bps,
        slippage_p95_bps=slippage_p95_bps,
        fee_bps=fee_bps,
        micro_noise_q90_bps=micro_noise_q90_bps,
        exchange_min_stop_bps=exchange_min_bps,
        atr_floor_mult=atr_floor_mult,
        buffer_bps=buffer_bps,
    )
    return floor, True
