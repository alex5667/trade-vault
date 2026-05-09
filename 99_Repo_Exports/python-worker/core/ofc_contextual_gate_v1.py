from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ContextualGateDecision:
    allow: bool
    reason: str
    edge_net_p50_bps: float
    edge_net_p90_bps: float
    p_rule_cal: float
    cost_p50_bps: float
    cost_p90_bps: float
    score_min_ctx: float
    fallback_level: str
    mode: str


class ContextualGateV1:
    def __init__(self, gate_cfg: dict[str, Any]) -> None:
        self.gate_cfg = dict(gate_cfg or {})

    def evaluate(
        self,
        *,
        raw_score: float,
        ctx_features: dict[str, float],
        exec_cost_pred: Any,
        rule_pred: Any,
        tp_bps: float,
        sl_bps: float,
        mode: str,
    ) -> ContextualGateDecision:
        p_rule_cal = float(getattr(rule_pred, "p_rule_cal", raw_score) or raw_score)
        score_min_ctx = float(
            getattr(rule_pred, "score_min_ctx", self.gate_cfg.get("p_min_default", ctx_features.get("legacy_of_score_min", 0.60)))
            or ctx_features.get("legacy_of_score_min", 0.60)
        )
        cost_p50 = float(getattr(exec_cost_pred, "cost_p50_bps", ctx_features.get("expected_slippage_bps", 0.0)) or 0.0)
        cost_p90 = float(getattr(exec_cost_pred, "cost_p90_bps", (ctx_features.get("spread_bps", 0.0) + ctx_features.get("expected_slippage_bps", 0.0))) or 0.0)
        edge_p50 = (p_rule_cal * float(tp_bps)) - ((1.0 - p_rule_cal) * float(sl_bps)) - cost_p50
        edge_p90 = (p_rule_cal * float(tp_bps)) - ((1.0 - p_rule_cal) * float(sl_bps)) - cost_p90
        floor_p50 = float(self.gate_cfg.get("edge_floor_p50_bps", 0.0) or 0.0)
        floor_p90 = float(self.gate_cfg.get("edge_floor_p90_bps", -2.0) or -2.0)
        allow = bool((p_rule_cal >= score_min_ctx) and (edge_p50 >= floor_p50) and (edge_p90 >= floor_p90))
        reason = "allow" if allow else "ctx_edge_or_prob_veto"
        return ContextualGateDecision(
            allow=allow,
            reason=reason,
            edge_net_p50_bps=float(edge_p50),
            edge_net_p90_bps=float(edge_p90),
            p_rule_cal=float(p_rule_cal),
            cost_p50_bps=float(cost_p50),
            cost_p90_bps=float(cost_p90),
            score_min_ctx=float(score_min_ctx),
            fallback_level=str(getattr(rule_pred, "fallback_level", getattr(exec_cost_pred, "fallback_level", ""))),
            mode=(mode or "off"),
        )
