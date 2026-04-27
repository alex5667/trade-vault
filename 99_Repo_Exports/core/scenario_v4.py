from __future__ import annotations

"""scenario_v4.py

Scenario Selection v4 (B1)
--------------------------

Goal
  Expand scenario selection beyond {reversal, continuation, none} by adding:
    - range_meanrev       : sideways/range markets (no sweep, no trend)
    - vol_shock_news_proxy: high-risk regime proxy (pressure/churn/exec risk/liquidity)
    - saw_chop_spoof_proxy: "pila"/chop proxy from cancellation spike meta

Design principles
  - Deterministic: inputs are tick_ts-bounded features and gate meta, no wall-clock.
  - Fail-open: if inputs missing, fall back to base scenario.
  - Safe rollout: controlled by cfg flags (scenario_v4_enable, per-scenario enables).

"""

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class ScenarioV4:
    id: str          # scenario id
    base: str        # coarse group: reversal/continuation/range
    reason: str      # compact human-readable reason
    flags: Dict[str, Any]


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


def classify_v4(
    *,
    sweep_recent: bool,
    trend_dir: Optional[str],
    pressure_hi: bool,
    churn_hi: bool,
    exec_risk_bps: float,
    liq_regime: str,
    liq_score: float,
    cancel_meta: Dict[str, Any],
    cfg: Dict[str, Any],
) -> ScenarioV4:
    """Classify scenario using deterministic proxies."""

    # --- base ---
    if sweep_recent:
        base = "reversal"
        base_id = "reversal_sweep"
        base_reason = "sweep_recent"
    elif trend_dir is not None:
        base = "continuation"
        base_id = "continuation_trend"
        base_reason = "trend_dir"
    else:
        base = "range"
        base_id = "range_meanrev"
        base_reason = "no_sweep_no_trend"

    # --- saw / chop proxy (anti-spoof) ---
    saw_en = bool(int(cfg.get("scenario_v4_saw_chop_enable", 1) or 1))
    ready = int((cancel_meta or {}).get("ready", 0) or 0)
    veto_kind = _s((cancel_meta or {}).get("veto_kind", ""), "")
    is_saw = saw_en and ready == 1 and veto_kind in (
        "pull_without_aggr",
        "support_pulled",
        "opp_pulled",
    )

    # --- vol shock / news proxy ---
    vol_en = bool(int(cfg.get("scenario_v4_vol_shock_enable", 1) or 1))
    risk_min = float(cfg.get("vol_shock_exec_risk_min_bps", 8.0) or 8.0)
    liq_min = float(cfg.get("vol_shock_liq_score_min", 0.35) or 0.35)

    is_vol = False
    if vol_en:
        # Pressure+Churn is the strongest proxy for "eventful" regime.
        if pressure_hi and churn_hi:
            is_vol = True
        # Or: high execution risk + any instability signal.
        elif (pressure_hi or churn_hi) and exec_risk_bps >= risk_min:
            is_vol = True
        # Or: liquidity explicitly flagged as news/illiquid.
        elif str(liq_regime).lower() in ("news", "illiquid"):
            is_vol = True
        # Or: liquidity score is very low while risk is high.
        elif liq_score > 0 and liq_score < liq_min and exec_risk_bps >= risk_min:
            is_vol = True

    if is_vol:
        return ScenarioV4(
            id="vol_shock_news_proxy",
            base=base,
            reason=(
                f"pressure_hi={int(pressure_hi)} churn_hi={int(churn_hi)} "
                f"exec_risk_bps={exec_risk_bps:.2f} liq={liq_regime}:{liq_score:.2f}"
            ),
            flags={
                "pressure_hi": pressure_hi,
                "churn_hi": churn_hi,
                "exec_risk_bps": exec_risk_bps,
                "liq_regime": liq_regime,
                "liq_score": liq_score,
            },
        )

    if is_saw:
        return ScenarioV4(
            id="saw_chop_spoof_proxy",
            base=base,
            reason=f"cancel_ready=1 veto_kind={veto_kind}",
            flags={"ready": ready, "veto_kind": veto_kind},
        )

    return ScenarioV4(id=base_id, base=base, reason=base_reason, flags={})
