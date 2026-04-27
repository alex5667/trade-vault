from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _sym_base(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if s.endswith("USDT") and len(s) > 4:
        return s[:-4]
    return s


@dataclass(frozen=True)
class EvGateConfig:
    enabled: bool
    p_min: float
    k_cost: float
    default_costs_bps: float
    log_veto: bool

    @classmethod
    def from_env(cls) -> "EvGateConfig":
        return cls(
            enabled=_env_bool("EV_GATE_ENABLED", False),
            p_min=max(0.0, min(1.0, _env_float("EV_GATE_P_MIN", 0.55))),
            k_cost=max(0.0, _env_float("EV_GATE_K_COST", 1.0)),
            default_costs_bps=max(0.0, _env_float("EV_GATE_COSTS_BPS", 8.0)),
            log_veto=_env_bool("EV_GATE_LOG_VETO", True),
        )


@dataclass(frozen=True)
class EvGateResult:
    passed: bool
    veto_reason: str
    p_hit_tp1: float
    tp1_bps: float
    stop_bps: float
    ev_bps: float
    costs_bps: float
    required_bps: float


def _bps_move(entry: float, price: float) -> float:
    if entry <= 0:
        return 0.0
    return abs(price - entry) / entry * 10_000.0


def estimate_costs_bps(ctx: Any, *, symbol: str) -> float:
    """
    Оценка стоимости (cost estimate) для EV gate.

    Приоритет:
      1) ctx.total_costs_bps / ctx.costs_bps (если какой-то гейт выше уже вычислил это)
      2) <BASE>_EV_COSTS_BPS
      3) EV_GATE_COSTS_BPS (глобально)
    """
    for name in ("total_costs_bps", "costs_bps"):
        try:
            v = getattr(ctx, name, None)
            if v is not None:
                x = float(v)
                if x >= 0:
                    return float(x)
        except Exception:
            pass
    base = _sym_base(symbol)
    try:
        v = os.getenv(f"{base}_EV_COSTS_BPS")
        if v is not None and str(v).strip() != "":
            x = float(v)
            if x >= 0:
                return float(x)
    except Exception:
        pass
    return max(0.0, _env_float("EV_GATE_COSTS_BPS", 8.0))


def evaluate_ev_gate(
    *,
    cfg: EvGateConfig,
    entry: float,
    tp1: float,
    sl: float,
    p_hit_tp1: float,
    costs_bps: float,
) -> EvGateResult:
    """
    EV_bps = p * tp1_bps - (1-p) * stop_bps
    Требуется:
      p >= p_min
      EV_bps >= K_cost * costs_bps
    """
    tp1_bps = _bps_move(entry, tp1)
    stop_bps = _bps_move(entry, sl)
    p = max(0.0, min(1.0, float(p_hit_tp1)))
    ev_bps = p * tp1_bps - (1.0 - p) * stop_bps
    required = float(cfg.k_cost) * max(0.0, float(costs_bps))

    if p < cfg.p_min:
        return EvGateResult(
            passed=False,
            veto_reason=f"p_hit_tp1<{cfg.p_min:.2f}",
            p_hit_tp1=p,
            tp1_bps=float(tp1_bps),
            stop_bps=float(stop_bps),
            ev_bps=float(ev_bps),
            costs_bps=float(costs_bps),
            required_bps=float(required),
        )
    if ev_bps < required:
        return EvGateResult(
            passed=False,
            veto_reason=f"ev<{required:.1f}bps",
            p_hit_tp1=p,
            tp1_bps=float(tp1_bps),
            stop_bps=float(stop_bps),
            ev_bps=float(ev_bps),
            costs_bps=float(costs_bps),
            required_bps=float(required),
        )
    return EvGateResult(
        passed=True,
        veto_reason="",
        p_hit_tp1=p,
        tp1_bps=float(tp1_bps),
        stop_bps=float(stop_bps),
        ev_bps=float(ev_bps),
        costs_bps=float(costs_bps),
        required_bps=float(required),
    )
