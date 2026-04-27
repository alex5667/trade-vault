from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class ScenarioV4:
    """
    Scenario v4 decision.
    id: public scenario id used for explainability and policy selection.
    base: coarse class for backward compatibility (reversal/continuation/range).
    reason: compact string why scenario was chosen.
    flags: extra diagnostics (kept small).
    """
    id: str
    base: str
    reason: str
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
    cancel_meta: Dict[str, Any],
    cfg: Dict[str, Any],
) -> ScenarioV4:
    """
    Deterministic scenario classifier.
    Priority:
      1) vol_shock_news_proxy (risk regime)
      2) saw_chop_spoof_proxy (cancel-pull without aggression)
      3) base scenario: reversal_sweep / continuation_trend / range_meanrev

    Notes:
      - This is a proxy-based detector (no external news feed required).
      - We keep behavior gated via cfg flags so rollout can be safe.
    """
    # ----- base scenario -----
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

    # ----- saw / chop proxy -----
    saw_en = bool(int(cfg.get("scenario_v4_saw_chop_enable", 1) or 1))
    ready = int(cancel_meta.get("ready", 0) or 0)
    veto_kind = _s(cancel_meta.get("veto_kind", ""), "")
    is_saw = saw_en and ready == 1 and veto_kind in ("pull_without_aggr", "support_pulled", "opp_pulled")

    # ----- vol shock / news proxy -----
    vol_en = bool(int(cfg.get("scenario_v4_vol_shock_enable", 1) or 1))
    risk_min = float(cfg.get("vol_shock_exec_risk_min_bps", 8.0) or 8.0)
    # triggers:
    #   - pressure_hi + churn_hi (burst + churn)
    #   - or (pressure|churn) + high exec_risk (spread+slippage)
    #   - or explicit liq_regime tagged as news/illiquid
    is_vol = vol_en and (
        (pressure_hi and churn_hi)
        or ((pressure_hi or churn_hi) and exec_risk_bps >= risk_min)
        or (str(liq_regime).lower() in ("news", "illiquid"))
    )

    if is_vol:
        return ScenarioV4(
            id="vol_shock_news_proxy",
            base=base,
            reason=f"pressure_hi={int(pressure_hi)} churn_hi={int(churn_hi)} exec_risk_bps={exec_risk_bps:.2f} liq_regime={liq_regime}",
            flags={"pressure_hi": pressure_hi, "churn_hi": churn_hi, "exec_risk_bps": exec_risk_bps, "liq_regime": liq_regime},
        )
    if is_saw:
        return ScenarioV4(
            id="saw_chop_spoof_proxy",
            base=base,
            reason=f"cancel_ready=1 veto_kind={veto_kind} dir_taker={cancel_meta.get('dir_taker', 0.0)}",
            flags={"ready": ready, "veto_kind": veto_kind, "dir_taker": cancel_meta.get("dir_taker", 0.0)},
        )

    return ScenarioV4(id=base_id, base=base, reason=base_reason, flags={})

