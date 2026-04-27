from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(frozen=True)
class ExecCostPrediction:
    cost_p50_bps: float
    cost_p90_bps: float
    exec_risk_ref_bps_ctx: float
    fallback_level: str
    model_version: str
    artifact_version: str


class ExecCostModelV1:
    """
    Minimal deterministic runtime.
    payload example:
      {
        "model_version": "ecm_v1",
        "artifact_version": "20260314_...",
        "groups": {
          "symbol=BTCUSDT|session=eu|liq=normal|vol=normal|scenario=continuation": {
              "cost_mult": 1.00,
              "cost_add_bps": 0.20,
              "cost_p90_mult": 1.50,
              "exec_risk_ref_mult": 1.00
          }
        },
        "global": {"cost_mult": 1.0, "cost_add_bps": 0.0, "cost_p90_mult": 1.5, "exec_risk_ref_mult": 1.0}
      }
    """
    def __init__(self, payload: Dict[str, Any]) -> None:
        self.payload = dict(payload or {})
        self.groups = dict(self.payload.get("groups", {}) or {})
        self.global_group = dict(self.payload.get("global", {}) or {})
        self.model_version = str(self.payload.get("model_version", "ecm_v1") or "ecm_v1")
        self.artifact_version = str(self.payload.get("artifact_version", "") or "")

    def predict(self, *, features: Dict[str, float], ctx_key: str, fallback_keys: List[str]) -> ExecCostPrediction:
        grp = None
        level = "global"
        for key in [ctx_key] + list(fallback_keys or []):
            if key in self.groups:
                grp = self.groups[key]
                level = key
                break
        if grp is None:
            grp = self.global_group
        spread_bps = float(features.get("spread_bps", 0.0) or 0.0)
        slip_bps = float(features.get("expected_slippage_bps", 0.0) or 0.0)
        legacy_ref = float(features.get("exec_risk_ref_bps", 10.0) or 10.0)
        cost_mult = float(grp.get("cost_mult", 1.0) or 1.0)
        cost_add = float(grp.get("cost_add_bps", 0.0) or 0.0)
        cost_p90_mult = float(grp.get("cost_p90_mult", 1.5) or 1.5)
        ref_mult = float(grp.get("exec_risk_ref_mult", 1.0) or 1.0)
        p50 = max(0.0, (slip_bps * cost_mult) + cost_add)
        p90 = max(p50, ((max(0.0, spread_bps) + max(0.0, slip_bps)) * cost_p90_mult) + cost_add)
        return ExecCostPrediction(
            cost_p50_bps=float(p50),
            cost_p90_bps=float(p90),
            exec_risk_ref_bps_ctx=float(max(1e-9, legacy_ref * ref_mult)),
            fallback_level=str(level),
            model_version=self.model_version,
            artifact_version=self.artifact_version,
        )
