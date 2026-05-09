from __future__ import annotations

import math
import os
from typing import Any

from signals.types import OrderflowContext, SignalContext

# NOTE:
#   Confidence != final_score.
#   - final_score: "signal strength" on a single axis after quality/regime/liquidity scaling (can be signed/unsigned).
#   - confidence: 0..100 calibrated (rolling percentile / probability-like) representation used downstream.
#   We want ONE source of truth for confidence: UnifiedSignalPipeline (not publishers/formatters).




class UnifiedSignalPipeline:
    def __init__(
        self,
        scoring_engine,  # SignalScoringEngine
        regime_service,  # MarketRegimeService
        golden_logic: Any,
        exec_filters: Any,
        publisher: Any,  # SignalPublisher
        calibrator: Any | None = None,
    ):
        self._scoring_engine = scoring_engine
        self._regime_service = regime_service
        self._golden_logic = golden_logic
        self._exec_filters = exec_filters
        self._publisher = publisher
        # RollingPercentileCalibrator (или совместимый) для калибровки уверенности.
        # ВАЖНО: Pipeline - ЕДИНСТВЕННЫЙ компонент, который пишет payload["confidence"].
        self._confidence_calibrator = calibrator

        # "1/1024 micro-downgrade": строгий assert единственного писателя для confidence.
        # В dev/test вы хотите жесткое падение, если любой компонент ниже по потоку пытается установить confidence.
        # В prod вы обычно держите это ВЫКЛЮЧЕННЫМ, чтобы оставаться fail-open (пайплайн перезапишет вместо этого).
        self._strict_conf_single_writer = self._env_flag("STRICT_CONFIDENCE_SINGLE_WRITER", default=False)

    @staticmethod
    def _env_flag(name: str, default: bool = False) -> bool:
        v = os.getenv(name)
        if v is None:
            return bool(default)
        s = str(v).strip().lower()
        return s not in {"0", "false", "no", "off", ""}

    def _clamp01(self, x: float) -> float:
        try:
            v = float(x)
        except Exception:
            return 0.0
        if not math.isfinite(v):
            return 0.0
        return max(0.0, min(1.0, v))

    def _ensure_confidence_pct(self, *, payload: dict[str, Any], symbol: str, kind: str, final_score: float) -> float:
        """
        Принудительное правило единственного писателя для confidence:
          - Publisher НЕ ДОЛЖЕН устанавливать confidence.
          - Pipeline ВСЕГДА устанавливает payload["confidence"] (0..100).
        """
        conf_pct: float
        if self._confidence_calibrator is not None:
            # Ожидаемый интерфейс: calibrate(symbol=..., kind=..., final_score=...) -> float (0..100).
            conf_pct = float(self._confidence_calibrator.calibrate(symbol=symbol, kind=kind, final_score=final_score))
        else:
            # Консервативный fallback: маппинг |final_score| в [0..95], оставляем место для "очень уверен".
            conf_pct = min(95.0, max(0.0, abs(float(final_score))))
        if not math.isfinite(conf_pct):
            conf_pct = 0.0
        conf_pct = max(0.0, min(100.0, conf_pct))
        payload["confidence"] = conf_pct
        return conf_pct


    # === 1. OrderflowContext -> SignalContext ===
    def build_ctx(self, of_ctx: OrderflowContext) -> SignalContext:
        """
        Создает SignalContext из OrderflowContext.
        """
        ctx = SignalContext(
            symbol=of_ctx.symbol,
            ts_event_ms=of_ctx.ts,
            of=of_ctx,
            session="",  # будет заполнено в attach_regime
            tags=[],
        )
        return ctx



    # === Высокоуровневый entry-point ===
    def process(self, ctx) -> None:
        # 1) Score (это уже должно произвести final_score = raw_score * conf_factor)
        res = self._scoring_engine.score(ctx)

        # res.should_emit - это первичный гейт после фильтров scoring/quality
        if not getattr(res, "should_emit", False):
            return

        # 2) Publisher строит payload (НЕ ДОЛЖЕН устанавливать "confidence")
        payload = self._publisher.build_payload(ctx=ctx, result=res)
        if not isinstance(payload, dict):
            return

        # Правило единственного писателя для confidence:
        #   - STRICT (dev/test): вызывать ошибку немедленно, чтобы регрессии отлавливались мгновенно.
        #   - default (prod): fail-open удаляя + перезаписывая позже.
        if "confidence" in payload:
            if self._strict_conf_single_writer:
                raise ValueError(
                    "Publisher attempted to set payload['confidence'], but confidence is a pipeline-only field. "
                    "Fix SignalPublisher.build_payload() to not compute/write confidence."
                )
            else:
                try:
                    del payload["confidence"]
                except Exception:
                    # fail-open: если payload это странный маппинг, просто перезаписать позже
                    pass

        # 3) Pipeline устанавливает confidence на основе final_score используя калибратор.
        kind = str(payload.get("kind") or getattr(res, "kind", "") or "")
        symbol = str(payload.get("symbol") or getattr(ctx, "symbol", "") or "")
        try:
            final_score = float(payload.get("final_score", getattr(res, "final_score", 0.0)) or 0.0)
        except Exception:
            final_score = 0.0
        if not math.isfinite(final_score):
            final_score = 0.0
        payload["final_score"] = final_score  # нормализация передачи
        self._ensure_confidence_pct(payload=payload, symbol=symbol, kind=kind, final_score=final_score)

        # 4) Publish
        self._publisher.publish(payload)
