from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _clamp01(x: float) -> float:
    return 0.0 if x <= 0.0 else 1.0 if x >= 1.0 else float(x)


@dataclass(frozen=True)
class RuleSuccessPrediction:
    p_rule_raw: float
    p_rule_cal: float
    score_min_ctx: float
    fallback_level: str
    model_version: str
    artifact_version: str


class RuleSuccessModelV1:
    """
    Minimal deterministic runtime.
    payload example:
      {
        "model_version": "rsm_v1",
        "artifact_version": "20260314_...",
        "groups": {
          "symbol=BTCUSDT|session=eu|liq=normal|vol=normal|scenario=continuation": {
              "score_mult": 1.0,
              "score_add": 0.0,
              "score_min_ctx": 0.58
          }
        },
        "global": {"score_mult": 1.0, "score_add": 0.0, "score_min_ctx": 0.60}
      }
    """
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = dict(payload or {})
        self.groups = dict(self.payload.get("groups", {}) or {})
        self.global_group = dict(self.payload.get("global", {}) or {})
        self.model_version = str(self.payload.get("model_version", "rsm_v1") or "rsm_v1")
        self.artifact_version = str(self.payload.get("artifact_version", "") or "")

    def predict(self, *, features: dict[str, float], ctx_key: str, fallback_keys: list[str]) -> RuleSuccessPrediction:
        grp = None
        level = "global"
        for key in [ctx_key] + list(fallback_keys or []):
            if key in self.groups:
                grp = self.groups[key]
                level = key
                break
        if grp is None:
            grp = self.global_group
        score = float(features.get("of_score_final", features.get("raw_score", 0.0)) or 0.0)
        score_mult = float(grp.get("score_mult", 1.0) or 1.0)
        score_add = float(grp.get("score_add", 0.0) or 0.0)
        p_raw = _clamp01(score)
        p_cal = _clamp01((p_raw * score_mult) + score_add)
        score_min_ctx = float(grp.get("score_min_ctx", features.get("legacy_of_score_min", 0.60)) or 0.60)
        return RuleSuccessPrediction(
            p_rule_raw=float(p_raw),
            p_rule_cal=float(p_cal),
            score_min_ctx=float(score_min_ctx),
            fallback_level=str(level),
            model_version=self.model_version,
            artifact_version=self.artifact_version,
        )
