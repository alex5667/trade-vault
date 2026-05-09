from __future__ import annotations

"""
Signal generation logic for CryptoOrderFlowHandler.

This module contains candidate detection, validation, scoring, and signal generation.
"""

import os
from dataclasses import dataclass
from typing import Any

# from orderflow.candidates import ScoredCandidate  <-- REMOVED due to shadowing collision
from common.math_safe import clamp01, finite_or
from common.u16_pack import pack_u16_list
from handlers.crypto_orderflow.logging.logging_utils import log_signal_one_json_unified
from signal_scoring.reason_codes import ReasonCode
from signal_scoring.reason_registry import reason_code_to_u16
from signal_scoring.wire_u16 import pack_u16
from utils.time_utils import get_ny_time_millis

from ..types.crypto_orderflow_pipeline_types import Candidate as CandidatePipeline
from ..types.crypto_orderflow_pipeline_types import SignalDTO
import contextlib


@dataclass(frozen=True)
class ScoredCandidate:
    candidate: CandidatePipeline
    conf_factor: float          # [0..1]
    final_score: float          # raw_score * conf_factor
    confidence_pct: float       # [0..100] calibrated display metric
    score_parts: dict[str, Any] # breakdown for debug/audit


class CryptoOrderFlowGenerateMixin:
    """
    Mixin class containing signal generation logic for CryptoOrderFlowHandler.
    """

    def _detect_candidates(self, ctx: Any) -> list[CandidatePipeline]:
        """
        Detector-only: create Candidate objects.
        Keep this method as a thin wrapper around existing detection logic (z/levels/cross).
        """
        out: list[CandidatePipeline] = []

        # Minimal mechanical adapter:
        # - If your existing code already produces something like (signal_kind, side, raw_score, reasons),
        #   map it here. If not available, keep empty list.
        #
        # You should replace `_legacy_detect_signals(ctx)` with your current detection block.
        legacy = getattr(self, "_legacy_detect_signals", None)
        if callable(legacy):
            for item in legacy(ctx) or []:
                try:
                    kind, side, raw_score, level_key, reasons = item
                except Exception:
                    # tolerate legacy shapes (kind, side, raw_score)
                    kind = item[0]
                    side = item[1]
                    raw_score = item[2] if len(item) > 2 else 0.0
                    level_key = None
                    reasons = []
                out.append(CandidatePipeline(kind=kind, side=int(side), raw_score=float(raw_score), level_key=level_key, reasons=list(reasons)))
        return out

    def _validate_candidate(self, ctx: Any, cand: CandidatePipeline) -> CandidatePipeline:
        """
        Add quality_flags + allow veto.
        """
        q: dict[str, Any] = dict(cand.quality_flags or {})
        veto = False
        veto_reason = ""

        snap = getattr(ctx, "l2", None) or getattr(ctx, "l2_snapshot", None)
        price = getattr(ctx, "last_price", None) or getattr(ctx, "price", None) or 0.0

        # L2 confirmations (breakout vs absorption selection can be kind-based)
        kind_name = str(getattr(cand.kind, "value", cand.kind))
        is_abs = "absorp" in kind_name.lower()

        # Метрика staleness должна считаться именно здесь: это место, где L2 реально влияет на решение.
        # Поведение торговли НЕ меняем этим блоком — только отмечаем stale/missing в ctx и метриках.
        with contextlib.suppress(Exception):
            self._mark_l2_staleness(ctx=ctx, kind=kind_name)

        if is_abs:
            res = self._l2_confirm_absorption_engine.check(snap, side=int(cand.side), price=float(price or 0.0))
        else:
            res = self._l2_confirm_breakout_engine.check(snap, side=int(cand.side), price=float(price or 0.0))

        q["l2_ok"] = bool(res.ok)
        q["l2_reason"] = str(res.reason_code)
        q["l2_parts"] = dict(res.parts)

        # veto policy: stale L2 is hard veto; other L2 fails are soft (handled by conf_factor)
        if q["l2_reason"] == "stale_l2":
            veto = True
            veto_reason = "stale_l2"

        q["veto"] = veto
        q["veto_reason"] = veto_reason

        cand.quality_flags = q
        cand.veto = veto
        cand.veto_reason = veto_reason
        return cand

    def _score_candidate(self, ctx: Any, cand: CandidatePipeline) -> ScoredCandidate:
        out = self._score_model.score(
            ctx=ctx,
            kind=cand.kind,
            side=int(cand.side),
            raw_score=float(cand.raw_score),
            quality_flags=dict(cand.quality_flags or {}),
        )
        return ScoredCandidate(
            candidate=cand,
            conf_factor=out.conf_factor,
            final_score=out.final_score,
            confidence_pct=out.confidence_pct,
            score_parts=dict(out.parts),
        )

    def _generate_signals(self, ctx: Any) -> bool:
        """
        Генерация сигналов (legacy path).
        ВАЖНО:
          - candidates_total должен считаться ДО validate/emit (чтобы включать veto).
          - signals_veto{reason} должен отражать реальную причину из ConfirmationsEngine.validate().
          - conf_factor_hist/final_score_hist должны наблюдаться на "ok" ветке.
        """
        any_sent = False
        pack_rc16 = os.getenv("RC_PACK_U16", "1").lower() not in {"0", "false", "no"}
        keep_reason_str = os.getenv("RC_KEEP_STR", "1").lower() not in {"0", "false", "no"}

        # fail-open доступ к SignalMetrics, созданному в BaseOrderflowHandler
        sigm = getattr(self, "_sigm", None)

        # PERF: bind frequently used attributes locally (hot path)
        cfg = self._cfg
        emit = self._emitter.emit
        logger = self.logger

        # PERF: resolve common ctx fields once
        # (ctx is a dataclass-like object with attribute access; getattr is not free)
        ctx_symbol = getattr(ctx, "symbol", None)
        ctx_ts = getattr(ctx, "ts", None)
        ctx_price = getattr(ctx, "price", None)

        # ---- "самый финальный микродожим" ----
        # Подключаем Top-N veto reporter ОДИН раз, лениво (без правок __init__),
        # потому что emitter гарантированно существует уже на момент генерации сигналов.
        #
        # Это пишет 1 агрегированное сообщение в outbox_labels (kind=label_update),
        # downstream TG/WS может отправлять в Telegram без спама.
        try:
            if sigm is not None and getattr(sigm, "_veto_reporter", None) is None:
                em = getattr(self, "_emitter", None)
                if em is not None:
                    from common.veto_reason_reporter import VetoTopNReporter
                    reporter = VetoTopNReporter(emitter=em, logger=getattr(self, "logger", None) or self.logger)
                    sigm.attach_veto_reporter(reporter)
        except Exception:
            pass

        any_sent = False
        candidates = self._detect_candidates(ctx)  # условно: где у вас формируются cand'ы
        if not candidates:
            return False

        for cand in candidates:
            # -----------------------------
            # 5.1 signals: candidates_total{kind}
            # -----------------------------
            try:
                if sigm is not None:
                    sigm.candidate(ctx=ctx, kind=str(cand.kind))
            except Exception:
                pass

            # 9.2 regime gate is a single function now
            allowed, gate_reason = self._apply_regime_gate(signal_kind=cand.kind, ctx=ctx)
            if not allowed:
                self._emit_veto_metric(kind=cand.kind, ctx=ctx, reason_code=gate_reason)
                continue

            # ---- Candidate path logging (SAMPLED) ----
            # Purpose:
            #   - See detector output (Candidate) even when veto happens later
            #   - Keep logs bounded (sampling + optional regime-change forcing)
            #
            # Contract:
            #   event="candidate" with kind/side/level_key/raw_score and top ctx features.
            try:
                now_ms = get_ny_time_millis()
                cur_regime = str(getattr(ctx, "market_regime", None) or getattr(ctx, "regime", None) or "")
                force = False
                if self._candidate_log_on_regime_change and cur_regime and cur_regime != self._last_regime_for_candidate_log:
                    force = True
                    self._last_regime_for_candidate_log = cur_regime

                if self._cand_log_gate.should_log(now_ms, force=force):
                    log_signal_one_json_unified(
                        self.logger,
                        payload={
                            "signal_id": None,
                            "kind": getattr(cand, "kind", None),
                            "side": getattr(cand, "side", None),
                            "symbol": getattr(ctx, "symbol", None),
                            "ts": getattr(ctx, "ts", None),
                            "level_key": getattr(cand, "level_key", None) or getattr(cand, "level_price", None),
                            "raw_score": getattr(cand, "raw_score", None),
                            # Candidate stage: no final_score/confidence yet.
                            "final_score": None,
                            "confidence": None,
                        },
                        ctx=ctx,
                        parts={"candidate_reasons": list(getattr(cand, "reasons", None) or [])[:8]},
                        veto=False,
                        conf_factor=None,
                        event="candidate",
                    )
            except Exception:
                # Fail-open: logging must never break generation.
                pass

            # validate (detector/validator separation уже есть через ConfirmationsEngine)
            # 1) level_price: передавать None вместо 0.0 для корректного контракта Optional[float]
            lp = getattr(cand, "level_price", None)
            level_price = float(lp) if lp is not None else None

            res = self._confirmations.validate(
                k=str(cand.kind),
                ctx=ctx,
                side=str(cand.side),
                level_price=level_price,
                l2=self._last_l2_snapshot,
            )

            # -----------------------------
            # 5.1 signals: signals_veto{kind,reason}
            # -----------------------------
            if res.veto:
                try:
                    if sigm is not None:
                        sigm.veto(ctx=ctx, kind=str(cand.kind), reason=str(res.reason_code))
                except Exception:
                    pass

                # log veto event
                with contextlib.suppress(Exception):
                    log_signal_one_json_unified(
                        self.logger,
                        payload={
                            "kind": getattr(cand, "kind", None),
                            "side": getattr(cand, "side", None),
                            "symbol": ctx_symbol,
                            "ts": ctx_ts,
                            "level_price": getattr(cand, "level_price", None),
                            "raw_score": finite_or(getattr(cand, "raw_score", None), 0.0),
                            "final_score": None,
                            "confidence": None,
                        },
                        ctx=ctx,
                        parts=dict(res.parts),
                        veto=True,
                        veto_reason_code=str(getattr(res, "reason_code", "")),
                        veto_reason_u16=int(getattr(res, "reason_u16", 0)),
                        conf_factor=None,
                        event="veto",
                    )
                continue

            raw_score = float(cand.raw_score)
            conf_factor01 = clamp01(float(getattr(res, "conf_factor01", 1.0) or 1.0))
            # SINGLE AXIS:
            #   final_score = raw_score * conf_factor01
            final_score = raw_score * conf_factor01

            # -------------------------------------------------------------------------
            # Confidence calibration (real "effect"):
            #   - stable across time
            #   - comparable across kinds/symbols
            #   - still 0..100 for formatter/publisher
            # -------------------------------------------------------------------------
            confidence_pct = self._confidence_pct(kind=cand.kind, ctx=ctx, final_score=final_score)

            # Minimal "effect visibility" metrics (optional sink):
            sym = str(getattr(ctx, "symbol", "") or "")
            self._metrics_observe("conf_factor_hist", conf_factor01, tags={"kind": str(cand.kind or ""), "symbol": sym})
            self._metrics_observe("final_score_hist", float(final_score), tags={"kind": str(cand.kind or ""), "symbol": sym})
            self._metrics_observe("confidence_pct_hist", float(confidence_pct), tags={"kind": str(cand.kind or ""), "symbol": sym})

            # parts может содержать breakdown conf_factor / L2/L3/geo/regime компонентов.
            # В лог (5.3) мы кладём parts как "parts-lite": только числа/флаги. Большие структуры запрещены.
            parts = dict(getattr(res, "parts", None) or {})

            # Сохраняем в parts для downstream дебага/калибровки
            # (унифицировано: formatter/publisher может показать это как "confidence breakdown")
            parts.setdefault("conf_factor", conf_factor01)
            parts.setdefault("raw_score", raw_score)
            parts.setdefault("final_score", final_score)

            # PERF: build payload using DTO
            signal_dto = SignalDTO(
                kind=getattr(cand, "kind", None),
                side=getattr(cand, "side", None),
                symbol=ctx_symbol,
                ts=ctx_ts,
                price=ctx_price,
                raw_score=finite_or(getattr(cand, "raw_score", None), 0.0),
                final_score=finite_or(final_score, 0.0),
                confidence=float(confidence_pct),  # 0..100 (calibrated)
                level_price=getattr(cand, "level_price", None),
                reasons=getattr(cand, "reasons", None) or [],
                parts=parts,
                signal_id=str(getattr(cand, "signal_id", "") or ""),
                conf_factor=finite_or(conf_factor01, 0.0),  # 0..1, явная шкала
                decision_code=str(getattr(res, "decision_code", "") or ""),
                decision_u16=int(getattr(res, "decision_u16", 0) or 0),
                level_key=getattr(cand, "level_key", None) or getattr(cand, "level_price", None),
                spread_bps=finite_or(getattr(ctx, "spread_bps", None), -1.0),
                taker_rate=finite_or(getattr(ctx, "taker_rate_ema", None), -1.0),
                geometry_score=finite_or(getattr(ctx, "geometry_score", None), -1.0),
                atr_pct=finite_or(getattr(ctx, "atr_5m", getattr(ctx, "atr", 0.0)) / getattr(ctx, "price", 1.0) if getattr(ctx, "price", 0.0) > 0.0 else 0.0, 0.0),
            )

            # Compact, stable reason code for wire + analytics.
            pack_rc16 = os.getenv("PACK_RC16", "1").lower() not in {"0", "false", "no"}
            keep_reason_str = os.getenv("KEEP_REASON_STR", "0").lower() in {"1", "true", "yes"}

            rc_u16 = int(getattr(res, "reason_u16", 0) or reason_code_to_u16(getattr(res, "reason_code", ReasonCode.OK.value)))
            signal_dto.rc = rc_u16
            if pack_rc16:
                signal_dto.rc16 = pack_u16(rc_u16)
            if keep_reason_str:
                signal_dto.reason_code = str(getattr(res, "reason_code", ReasonCode.OK.value))

            # FINAL: store compact uint16 codes in payload
            qf_codes = getattr(res, "quality_codes", None) or ()
            if qf_codes and not isinstance(qf_codes, list):
                qf_codes = list(qf_codes)
            if qf_codes:
                signal_dto.qf = qf_codes
                if cfg.qf_pack_u16:
                    signal_dto.qf16 = pack_u16_list(qf_codes)

            payload = signal_dto.to_dict()

            # -----------------------------
            # 5.2 signals: signals_sent{kind}
            # -----------------------------
            try:
                if sigm is not None:
                    sigm.sent(ctx=ctx, kind=str(cand.kind))
            except Exception:
                pass

            # EMIT: this is the hot path
            try:
                emit(payload)
                any_sent = True
            except Exception as e:
                logger.exception("Failed to emit signal", extra={"error": str(e), "payload": payload})
                continue

            # LOG: wire format (full payload + parts)
            try:
                log_signal_one_json_unified(
                    logger,
                    payload=payload,
                    ctx=ctx,
                    parts=parts,
                    veto=False,
                    conf_factor=conf_factor01,
                    event="emit",
                )
            except Exception:
                # fail-open: never break signal path
                pass

        return any_sent
