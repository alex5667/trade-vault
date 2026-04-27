from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple
import math

from handlers.quality.quality_gate import QualityGate
from handlers.scoring.score_model import ScoreModel, ScoreResult


@dataclass
class ScoreParts:
    parts: Dict[str, float]


class CryptoConfidenceScorer:
    """
    Возвращает conf_factor ∈ [0..1] + parts (единая ось).
    Delegates to the expert ConfidenceScorer from services.signal_confidence.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        from services.signal_confidence import ConfidenceScorer
        self._impl = ConfidenceScorer()
        self._qg = QualityGate()

    def score(self, *, kind: str, side: int, ctx: Any) -> Tuple[float, Dict[str, float]]:
        """
        kind: breakout/absorption/extreme/obi_spike/...
        side: int (+1/-1)
        """
        # Convert side int -> str for Compatibility
        side_str = "LONG" if side > 0 else "SHORT" if side < 0 else "NEUTRAL"
        
        # Expert Scorer
        conf_factor01, parts = self._impl.score(kind=kind, side=side_str, ctx=ctx)
        
        # Quality Gate (keep existing logic as secondary veto/multiplier if needed, or just merge parts)
        # Assuming ConfidenceScorer handles most checks, but QualityGate might deal with external L2/L3 latencies specifically.
        # For now, we mix them or just return expert score. 
        # The prompt implies "adapt and integrate", so we should use the expert score as base.
        
        # Compatibility with existing ScoreModel expectations
        parts["base01"] = conf_factor01
        
        # Apply strict QualityGate if not already covered
        l2 = getattr(ctx, "l2_snapshot", None) or getattr(ctx, "l2", None)
        qa = self._qg.assess_kind(kind=kind, ctx=ctx, l2=l2)
        parts.update({f"q_{kk}": float(vv) for kk, vv in qa.parts.items()})
        
        # If QualityGate vetoes, force zero
        if qa.veto:
             conf_factor01 = 0.0
             parts["quality_veto"] = 1.0
        
        parts["conf01"] = conf_factor01
        return conf_factor01, parts


@dataclass
class ScoreModelCfg:
    """
    Конфигурация для CryptoScoreModel.
    """
    conf_floor: float = 0.05
    conf_cap: float = 1.00
    regime_w: float = 0.25
    geometry_w: float = 0.25
    liquidity_w: float = 0.25
    l3_w: float = 0.15
    micro_quality_w: float = 0.10
    veto_to_zero: bool = True


class CryptoScoreModel:
    """
    Крипто-специфичная модель скоринга, объединяющая confidence-скоринг с базовой ScoreModel.
    """

    def __init__(self, cfg: ScoreModelCfg) -> None:
        self.cfg = cfg
        self.conf_scorer = CryptoConfidenceScorer()
        self.base_model = ScoreModel()

    def score(self, *, ctx: Any, kind: str, side: int, raw_score: float, quality_flags: Dict[str, Any]) -> ScoreResult:
        """
        Расчет скора с использованием крипто-специфичного confidence-скорера.
        """
        # Получаем коэффициент уверенности (confidence factor) от крипто-скорера
        conf_factor01, parts = self.conf_scorer.score(kind=kind, side=side, ctx=ctx)

        # Применяем логику вето
        veto = quality_flags.get("veto", False) if self.cfg.veto_to_zero else False
        if veto:
            conf_factor01 = 0.0

        # Ограничиваем коэффициент уверенности (clamping)
        conf_factor01 = max(self.cfg.conf_floor, min(self.cfg.conf_cap, conf_factor01))

        # Используем базовую модель для финального скоринга
        return self.base_model.score(raw_score=raw_score, conf_factor01=conf_factor01)
