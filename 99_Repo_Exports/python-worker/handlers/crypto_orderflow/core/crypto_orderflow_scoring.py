from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any

from handlers.quality.quality_gate import QualityGate
from handlers.scoring.score_model import ScoreModel, ScoreResult

from core.score_component_weight_calibrator import (
    DEFAULT_WEIGHTS,
    COMPONENTS,
    extract_component_scores,
)

logger = logging.getLogger("crypto_score_model")

# ---------------------------------------------------------------------------
# Score weight reader (Redis TTL cache)
# ---------------------------------------------------------------------------

_WEIGHT_CACHE_TTL_SEC = 30.0
_ENFORCE_ENV = "SCORE_W_CAL_ENFORCE"
_ENABLED_ENV = "SCORE_W_CAL_ENABLED"
_SHADOW_BLEND_ALPHA = float(os.getenv("SCORE_W_CAL_SHADOW_BLEND_ALPHA", "0.0"))


class _WeightCache:
    """
    Thread-unsafe TTL cache for per-(symbol, regime) component weights.
    One instance per CryptoScoreModel (singleton pattern via class var).
    """

    def __init__(self) -> None:
        self._cache: dict[tuple[str, str], tuple[dict[str, float], float]] = {}
        self._r: Any = None
        self._r_init_tried = False

    def _redis(self) -> Any | None:
        if self._r_init_tried:
            return self._r
        self._r_init_tried = True
        try:
            from core.redis_client import get_redis
            url = os.getenv("SCORE_W_CAL_REDIS_URL") or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            self._r = get_redis(url)
        except Exception:
            self._r = None
        return self._r

    def get(self, symbol: str, regime: str) -> dict[str, float]:
        key = (symbol, regime)
        entry = self._cache.get(key)
        if entry:
            weights, exp = entry
            if time.monotonic() < exp:
                return weights

        weights = self._fetch(symbol, regime)
        self._cache[key] = (weights, time.monotonic() + _WEIGHT_CACHE_TTL_SEC)
        return weights

    def _fetch(self, symbol: str, regime: str) -> dict[str, float]:
        r = self._redis()
        if r is None:
            return dict(DEFAULT_WEIGHTS)
        try:
            redis_key = f"autocal:score_weights:{symbol}:{regime}"
            raw = r.hgetall(redis_key)
            if raw:
                w: dict[str, float] = {}
                for comp in COMPONENTS:
                    v = raw.get(comp) or raw.get(comp.encode(), b"")
                    if isinstance(v, bytes):
                        v = v.decode()
                    if v:
                        try:
                            w[comp] = float(v)
                        except ValueError:
                            w[comp] = DEFAULT_WEIGHTS[comp]
                    else:
                        w[comp] = DEFAULT_WEIGHTS[comp]
                return w
        except Exception:
            pass
        return dict(DEFAULT_WEIGHTS)


_WEIGHT_CACHE = _WeightCache()


def _blend_conf(
    component_scores: dict[str, float],
    weights: dict[str, float],
) -> float:
    """Compute calibrated conf from component scores × IR weights."""
    blend = sum(weights.get(c, DEFAULT_WEIGHTS[c]) * component_scores.get(c, 0.0) for c in COMPONENTS)
    return max(0.0, min(1.0, blend))


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ConfidenceResult:
    conf_factor: float
    parts: dict[str, float]


@dataclass
class ScoreParts:
    parts: dict[str, float]


# ---------------------------------------------------------------------------
# Confidence scorer (delegates to expert ConfidenceScorer)
# ---------------------------------------------------------------------------


class CryptoConfidenceScorer:
    """
    Возвращает conf_factor ∈ [0..1] + parts (единая ось).
    Delegates to the expert ConfidenceScorer from services.signal_confidence.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        from services.signal_confidence import ConfidenceScorer
        self._impl = ConfidenceScorer()
        self._qg = QualityGate()

    def score(self, *, kind: str, side: int, ctx: Any) -> ConfidenceResult:
        """
        kind: breakout/absorption/extreme/obi_spike/...
        side: int (+1/-1)
        """
        side_str = "LONG" if side > 0 else "SHORT" if side < 0 else "NEUTRAL"

        conf_factor01, parts = self._impl.score(kind=kind, side=side_str, ctx=ctx)  # type: ignore

        parts["base01"] = conf_factor01

        l2 = getattr(ctx, "l2_snapshot", None) or getattr(ctx, "l2", None)
        qa = self._qg.assess_kind(kind=kind, ctx=ctx, l2=l2)
        parts.update({f"q_{kk}": float(vv) for kk, vv in qa.parts.items()})

        if qa.veto:
            conf_factor01 = 0.0
            parts["quality_veto"] = 1.0

        parts["conf01"] = conf_factor01
        return ConfidenceResult(conf_factor=conf_factor01, parts=parts)


# ---------------------------------------------------------------------------
# ScoreModelCfg
# ---------------------------------------------------------------------------


@dataclass
class ScoreModelCfg:
    """
    Конфигурация для CryptoScoreModel.

    Component weights are *starting defaults* — at runtime they are replaced by
    walk-forward IR weights from ScoreComponentWeightCalibrator when
    SCORE_W_CAL_ENFORCE=1 (or logged-only in shadow when SCORE_W_CAL_ENABLED=1).
    """
    conf_floor: float = 0.05
    conf_cap: float = 1.00
    regime_w: float = 0.25
    geometry_w: float = 0.25
    liquidity_w: float = 0.25
    l3_w: float = 0.15
    micro_quality_w: float = 0.10
    veto_to_zero: bool = True


# ---------------------------------------------------------------------------
# CryptoScoreModel
# ---------------------------------------------------------------------------


class CryptoScoreModel:
    """
    Крипто-специфичная модель скоринга.

    When SCORE_W_CAL_ENABLED=1: extracts 5 component scores from ConfidenceScorer
    parts and logs/applies calibrated IR weights per (symbol × regime).

    SCORE_W_CAL_ENFORCE=0 (default): shadow — logs calibrated conf, uses original.
    SCORE_W_CAL_ENFORCE=1           : enforce — replaces conf_factor01 with calibrated blend.
    """

    def __init__(self, cfg: ScoreModelCfg) -> None:
        self.cfg = cfg
        self.conf_scorer = CryptoConfidenceScorer()
        self.base_model = ScoreModel()
        self._cal_enabled = os.getenv(_ENABLED_ENV, "0").strip() not in ("0", "false", "no")
        self._cal_enforce = os.getenv(_ENFORCE_ENV, "0").strip() not in ("0", "false", "no")

    def score(
        self,
        *,
        ctx: Any,
        kind: str,
        side: int,
        raw_score: float,
        quality_flags: dict[str, Any],
    ) -> ScoreResult:
        res = self.conf_scorer.score(kind=kind, side=side, ctx=ctx)
        conf_factor01 = res.conf_factor
        parts = res.parts

        veto = quality_flags.get("veto", False) if self.cfg.veto_to_zero else False
        if veto:
            conf_factor01 = 0.0

        conf_factor01 = max(self.cfg.conf_floor, min(self.cfg.conf_cap, conf_factor01))

        # IR-calibrated component weighting
        if self._cal_enabled and conf_factor01 > 0.0:
            symbol = str(getattr(ctx, "symbol", "") or "")
            regime = str(getattr(ctx, "market_regime", "") or parts.get("regime_class_raw", "") or "unknown").lower()

            comp_scores = extract_component_scores(parts)

            # Store in parts so they propagate to indicators → trades:closed
            for cmp, val in comp_scores.items():
                parts[f"cmp_{cmp}"] = val

            if self._cal_enforce and symbol:
                weights = _WEIGHT_CACHE.get(symbol, regime)
                conf_calibrated = _blend_conf(comp_scores, weights)
                conf_calibrated = max(self.cfg.conf_floor, min(self.cfg.conf_cap, conf_calibrated))
                parts["conf_calibrated"] = conf_calibrated
                parts["conf_original"] = conf_factor01
                conf_factor01 = conf_calibrated
            else:
                # Shadow: log calibrated conf without applying it
                if symbol:
                    weights = _WEIGHT_CACHE.get(symbol, regime)
                    conf_calibrated = _blend_conf(comp_scores, weights)
                    parts["conf_calibrated_shadow"] = conf_calibrated

        return self.base_model.score(raw_score=raw_score, conf_factor01=conf_factor01)
