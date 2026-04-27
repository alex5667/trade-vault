from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
import math
import uuid
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)

# --- Common Infrastructure ---
from common.runtime_snapshot import RuntimeSnapshot
from common.safe_numbers import safe_float
from common.math_safe import clamp01
from common.dq_flags import append_dq_flag as _append_dq_flag
from common.cost_edge_adapter import decision_to_legacy_tuple

# --- Services ---
from services.ev_tp1_stats import get_tp1_hit_prob

# --- Handler Components ---
from handlers.base_orderflow_handler import BaseOrderFlowHandler, ensure_levels



# Additional typing imports
from typing import Callable
from common.cost_edge_codes import cost_edge_reason_codes as _cost_edge_reason_codes
# decision_to_legacy_tuple already imported above
from common.decision_trace import ensure_trace, trace_gate, serialize_trace_from_ctx, trace_enabled
from common.gate_cache import cached_call_exc
from common.risk_cfg_cache import resolve_risk_cfg_cached

# NOTE: diagnostics stream (НЕ outbox). Никаких tradeable действий отсюда.

# Additional handler imports
import hashlib
from typing import Tuple
from collections import deque, defaultdict

_CDBG_LAST: dict[str, float] = {}

def _c_sampled_debug(logger: Any, key: str, msg: str, *args: Any) -> None:
    try:
        interval = float(os.getenv("SAMPLED_DEBUG_INTERVAL_SEC", "30") or "30")
    except Exception:
        interval = 30.0
    try:
        now = float(time.time())
        last = float(_CDBG_LAST.get(key, 0.0))
        if (now - last) < interval:
            return
        _CDBG_LAST[key] = now
        if logger is not None:
            logger.debug(msg, *args)
    except Exception:
        return

def _env_on(name: str, default: str = "0") -> bool:
    v = (os.getenv(name, default) or "").strip().lower()
    return v in {"1", "true", "yes", "on"}

def finite_or(x: Any, default: float) -> float:
    """
    Безопасная замена `math.isfinite(x)` когда x может быть None/str/etc.
    """
    try:
        if isinstance(x, (int, float)):
            return float(x) if math.isfinite(float(x)) else float(default)
        # allow numeric strings
        f = float(x)
        return float(f) if math.isfinite(f) else float(default)
    except Exception:
        return float(default)



# --- Core / Common ---
from core.confidence_utils import normalize_confidence_pct
from core.dependency_policy import dependency_decision_for_kind
from core.instrument_config import SymbolSpecs, OrderFlowConfig, get_specs, get_config
from core.unified_signal_formatter import Signal
from core.htf_levels import RedisHTFLevelsProvider
from core.signal_context import SignalContext

from common.deque_utils import ensure_bounded_deque
from common.veto_reason_reporter import VetoTopNReporter
from common.u16_pack import pack_u16_list
from common.log_sampling import TimeSampler
from common.math_safe import safe_float, clamp01, clamp, finite_or
from common.json_fast import dumps1
from common.qf_codes import pack_qf_u16

# --- Handlers / Mixins / Components ---
from handlers.pipeline.pipeline import SignalPipeline
from handlers.pipeline.candidate import Candidate as CandidateHandler
from handlers.crypto_orderflow.utils.entry_policy_gate import EntryPolicyGate
from handlers.crypto_orderflow.utils.entry_policy_gate import write_entry_policy_diag
from handlers.crypto_orderflow.utils.log_sampler import LogSamplerFactory, sampled_info, sampled_warning
from handlers.detector.detector import Detector
from handlers.emitter.unified_signal_emitter import UnifiedSignalEmitter
from handlers.emitter.label_schema import sys_labels
from handlers.confidence_pct_provider import build_confidence_pct_fn
from handlers.scoring.score_model import ScoreModel as ScoreModelHandler

from handlers.crypto_orderflow.logging.logging_utils import (
    _safe_float,
    _ctx_quality_flags,
    log_signal_one_json_unified,
    log_signal_one_json as log_signal_one_json_local,
    _log_veto_one_json
)
from handlers.crypto_orderflow.config.runtime_config import _RuntimeCfg
from handlers.crypto_orderflow.mixins.crypto_orderflow_init import CryptoOrderFlowInitMixin
from handlers.crypto_orderflow.mixins.crypto_orderflow_l2_staleness import CryptoOrderFlowL2StalenessMixin
from handlers.crypto_orderflow.mixins.crypto_orderflow_generate import CryptoOrderFlowGenerateMixin
from handlers.crypto_orderflow.mixins.crypto_orderflow_geometry import CryptoOrderFlowGeometryMixin

# --- Models & Types ---
from handlers.crypto_orderflow.models.data_models import (
    RegimeConfig, RegimeSample, RegimeFeatures, HTFLevels,
    GeometryConfig, LiquidityConfig, ConfScoreConfig, SignalTypeConf, GoldenThresholds
)
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import (
    HTFLevel, GeoZoneHit, LiquidityContext, BarSample, L2Snapshot,
    L2Level, ClusterVol, ZoneType
)
from handlers.crypto_orderflow.types.crypto_orderflow_pipeline_types import (
    SignalKind, Candidate as CandidatePipeline, QualityState
)

# --- Utils & Gates ---
from handlers.crypto_orderflow.utils.helpers import (
    _to_str, _f, _b, _depth_sum, _is_trade_tick, _parse_bool
)
from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate
from handlers.crypto_orderflow.utils.risk_cfg_resolver import RiskCfgResolver
from handlers.crypto_orderflow.utils.pre_publish_gates import (
    HardDataQualityGate, RegimeSessionGate, ConsistencyGate
)
from handlers.crypto_orderflow.utils.smt_coherence_gate import GateDecision as SmtGateDecision
from handlers.crypto_orderflow.utils.quality_gates import (
    DataQualityGateDecision, QualityGateDecision
)
from handlers.crypto_orderflow.utils.trail_conditional import apply_trailing_policy_to_payload
from services.ev_tp1_stats import (
    attach_tp1_hit_prob_to_ctx,
    extract_regime_label_from_ctx,
    EvTp1StatsConfig,
    get_tp1_hit_prob,
)
from signals.ev_gate import EvGateConfig, evaluate_ev_gate, estimate_costs_bps
from signals.empirical_levels_dyn import EmpiricalLevelsConfig, RedisEmpiricalLevelsProvider, apply_empirical_levels_to_ctx

# --- Signal Logic ---
from signals.level_enricher import attach_trade_levels_to_ctx
from signal_scoring.reason_registry import normalize_reason, reason_code_to_u16
from signal_scoring.wire_u16 import pack_u16
from orderflow.candidates import ScoredCandidate
from .base_orderflow_handler import BaseOrderFlowHandler, Tick, PublishResult, SimpleL2Snapshot









class CryptoOrderFlowHandler(CryptoOrderFlowInitMixin, CryptoOrderFlowL2StalenessMixin, CryptoOrderFlowGenerateMixin, CryptoOrderFlowGeometryMixin, BaseOrderFlowHandler):
    # ---------- identity ----------
    def _get_source_name(self) -> str:
        return "CryptoOrderFlow"

    def _get_strategy_key(self) -> str:
        return "cryptoorderflow"

    def _get_signal_stream(self) -> str:
        return os.getenv("CRYPTO_ORDERFLOW_SIGNAL_STREAM") or f"signals:cryptoorderflow:{self.symbol}"

    # ------------------------------------------------------------------
    # Hot-path env cache (min_conf / min_conf_factor), shared with pipeline.
    # ------------------------------------------------------------------


    def _get_symbol_specs(self) -> SymbolSpecs:
        """Возвращает спецификацию для криптовалютного символа"""
        return get_specs(self.symbol)

    @staticmethod
    def _now_ms() -> int:
        return get_ny_time_millis()

    # ---------------------------------------------------------------------
    # Safety helpers (prod hardening)
    # ---------------------------------------------------------------------
    @staticmethod
    def _safe_str(v: Any) -> str:
        """Fast & Safe string conversion for Enums/None."""
        if v is None: return ""
        try:
            if isinstance(v, Enum): return str(v.value)
            return str(v)
        except Exception:
            return ""

    @staticmethod
    def _safe_int(v: Any, default: int = 0) -> int:
        try:
            return int(v)
        except Exception:
            return default

    @staticmethod
    def _safe_lower(v: Any) -> str:
        """
        Нормализация в нижний регистр, которая никогда не вызывает ошибок.

        Исправляет prod-риск:
          - '.lower()' на Enum/объекте вызвал бы AttributeError.
        """
        return CryptoOrderFlowHandler._safe_str(v).strip().lower()

    @staticmethod
    def _safe_int(v: Any, default: int = 0) -> int:
        """
        int(v) с fail-open фоллбэком.

        Зачем:
          - decision_u16 / reason_u16 могут быть None/str/object в редких случаях.
          - мы не должны крешить путь эмиссии из-за плохого значения u16.
        """
        try:
            return int(v)
        except Exception:
            return int(default)

    @staticmethod
    def _safe_reason_u16(code: Any, *, default: int) -> int:
        """
        Маппинг reason/decision code -> uint16 (fail-open).

        Исправляет prod-риск:
          - reason_code_to_u16(dc) может вызвать ошибку в строгом режиме или на неизвестных кодах.
          - downstream протокол требует ненулевой (или хотя бы стабильный) числовой код.

        Контракт:
          - возвращает >0 если возможно
          - иначе возвращает предоставленный default
        """
        rc = normalize_reason(CryptoOrderFlowHandler._safe_str(code) or "")
        if not rc:
            return int(default)
        try:
            u = reason_code_to_u16(rc, strict=False)
            u16 = CryptoOrderFlowHandler._safe_int(u, default=0)
        except Exception:
            u16 = 0
        return u16 if u16 > 0 else int(default)

    # ------------------------------------------------------------
    # Compute-once wrappers ("ещё выше")
    # ------------------------------------------------------------
    @staticmethod
    def _cfg_hash(cfg: dict) -> str:
        """
        Stable cfg hash for cache keys.
        sort_keys=True makes it deterministic across dict ordering.
        """
        try:
            s = json.dumps(cfg or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()
        except Exception:
            return "cfg:err"

    def _ctx_cache(self, ctx):
        try:
            c = getattr(ctx, "_gate_cache", None)
            if isinstance(c, dict):
                return c
            c = {}
            setattr(ctx, "_gate_cache", c)
            return c
        except Exception:
            return None


    def _risk_cfg_cached(self, *, ctx, symbol: str, cand=None):
        """
        Resolve risk cfg ONCE and store on ctx for reuse across:
          - attach_trade_levels_to_ctx
          - scoring / edge-cost gate
        """
        try:
            existing = getattr(ctx, "risk_cfg", None)
            if existing is not None:
                return existing
        except Exception:
            pass  # risk cfg missing or invalid

        cfg = None
        try:
            fn = getattr(self, "resolve_risk_cfg", None)
            if callable(fn):
                cfg = fn(symbol=str(symbol), ctx=ctx, cand=cand)
            else:
                # fallback to your existing resolver (seen in snippets)
                r = getattr(self, "_risk_cfg", None)
                if r is not None and callable(getattr(r, "resolve", None)):
                    cfg = r.resolve(str(symbol))
                else:
                    cfg = None
        except Exception:
            cfg = None

        try:
            setattr(ctx, "risk_cfg", cfg)
        except Exception:
            pass  # cache set failed
        return cfg



    def _resolve_risk_cfg_cached(self, symbol: str) -> dict:
        """
        Resolve SL/TP config once per symbol (process cache).
        Fail-open: returns {} on errors.
        """
        try:
            cache = getattr(self, "_risk_cfg_cache", None)
            if not isinstance(cache, dict):
                cache = {}
                try:
                    setattr(self, "_risk_cfg_cache", cache)
                except Exception:
                    pass  # cache set failed
            cache_ts = getattr(self, "_risk_cfg_cache_ts", None)
            if not isinstance(cache_ts, dict):
                cache_ts = {}
                try:
                    setattr(self, "_risk_cfg_cache_ts", cache_ts)
                except Exception:
                    cache_ts = None
            ttl = float(getattr(self, "_risk_cfg_cache_ttl_sec", 0.0) or 0.0)
            cfg = resolve_risk_cfg_cached(
                resolver=getattr(self, "_risk_cfg", None),
                symbol=str(symbol),
                cache=cache,
                cache_ts=cache_ts if isinstance(cache_ts, dict) else None,
                ttl_sec=ttl,
            )
            return dict(cfg) if isinstance(cfg, dict) else {}
        except Exception:
            return {}

    def _risk_cfg_for_ctx_once(self, *, ctx, symbol: str, cand=None) -> dict:
        """
        Resolve risk cfg ONCE and store on ctx for reuse across:
          - attach_trade_levels_to_ctx
          - scoring / edge-cost gate

        Priority:
          1) handler.resolve_risk_cfg(symbol, ctx, cand) if present (can be per-candidate)
          2) process cache: _resolve_risk_cfg_cached(symbol)

        Fail-open: returns {} on errors.
        """
        try:
            existing = getattr(ctx, "risk_cfg", None)
            if isinstance(existing, dict):
                return existing
        except Exception:
            pass  # ctx invalid

        cfg = None
        try:
            fn = getattr(self, "resolve_risk_cfg", None)
            if callable(fn):
                cfg = fn(symbol=str(symbol), ctx=ctx, cand=cand)
        except Exception:
            cfg = None

        if not isinstance(cfg, dict):
            cfg = self._resolve_risk_cfg_cached(str(symbol))

        try:
            cfgd = dict(cfg or {})
        except Exception:
            cfgd = {}

        try:
            setattr(ctx, "risk_cfg", cfgd)
        except Exception:
            pass  # ctx set failed

        return cfgd

    @staticmethod
    def ensure_levels_once(ctx: Any, *, side: Any) -> bool:
        """
        Calls ensure_levels at most once per (side_int) for this ctx.
        Returns True if we attempted to ensure.
        """
        from handlers.base_orderflow_handler import ensure_levels  # или ваш реальный импорт

        # normalize side to int if possible (so cache key is stable)
        side_key = None
        try:
            side_key = int(side)
        except Exception:
            side_key = str(side)

        ckey = ("ensure_levels", side_key)

        def _do() -> bool:
            try:
                ensure_levels(ctx, side=side)
                return True
            except Exception:
                return True  # attempted (fail-open)

        return bool(cached_call(ctx, ckey, _do))

    @staticmethod
    def attach_trade_levels_once(
        ctx: Any,
        *,
        side: str,
        symbol: str,
        kind: str,
        cfg: dict,
        regime: Any = None,
        empirical: Any = None,
        logger: Any = None,
    ) -> bool:
        from signals.level_enricher import attach_trade_levels_to_ctx

        ckey = ("attach_levels", str(symbol).upper(), str(kind).lower(), str(side).upper())

        def _do() -> bool:
            try:
                attach_trade_levels_to_ctx(
                    ctx,
                    side=str(side),
                    symbol=str(symbol),
                    cfg=dict(cfg),
                    kind=str(kind),
                    regime=regime,
                    empirical=empirical,
                    overwrite=False,
                    logger=logger,
                )
            except Exception:
                if logger: logger.debug("attach_trade_levels_once failed", exc_info=True)
            # success heuristic: tp1_price exists
            return getattr(ctx, "tp1_price", None) is not None

        return bool(cached_call(ctx, ckey, _do))

    @staticmethod
    def _sanitize_u16_list(xs: Any) -> list[int]:
        """
        Best-effort очистка произвольного списка -> list[int] со значениями в [0..65535].

        Зачем:
          - res.flags / quality flags могут содержать не-int значения.
          - pack_qf_u16 ожидает чистый список u16; мы не должны крешить сигналы на плохих данных.
        """
        out: list[int] = []
        try:
            it = list(xs or [])
        except Exception:
            return out
        for x in it:
            try:
                xi = int(x)
            except Exception:
                continue
            if 0 <= xi <= 0xFFFF:
                out.append(xi)
        return out

    @staticmethod
    def _jsonify(v: Any) -> Any:
        """Безопасное приведение типов для JSON сериализации."""
        if v is None or isinstance(v, (str, int, float, bool)):
            return v
        if isinstance(v, (list, tuple)):
            return [CryptoOrderFlowHandler._jsonify(x) for x in v]
        if isinstance(v, dict):
            return {str(k): CryptoOrderFlowHandler._jsonify(val) for k, val in v.items()}
        # Enum/Decimal/np types/etc
        try:
            return float(v)
        except Exception:
            return str(v)

    @staticmethod
    def _build_config_params_from_cfg(cfg: Any) -> dict[str, Any]:
        """
        Формируем config_params для отправки в sidecar meta (НЕ в payload).

        Почему так:
          - payload проходит через пайплайн и хранится в stream одним большим JSON;
          - крупные вложенные dict увеличивают стоимость сериализации/дедуп/IO;
          - но downstream (бот/аналитика) хочет видеть "на каких настройках" был сигнал.

        Поэтому config_params отправляем как meta, которая:
          - хранится отдельным ключом по signal_id,
          - подтягивается консьюмером/ботом по signal_id,
          - не влияет на детект/скоринг/дедуп.
        """
        if cfg is None:
            return {}
        raw = {
            "delta_window_ticks": getattr(cfg, "delta_window_ticks", None),
            "delta_z_threshold": getattr(cfg, "delta_z_threshold", None),
            "weak_progress_atr": getattr(cfg, "weak_progress_atr", None),
            "obi_threshold": getattr(cfg, "obi_threshold", None),
            "obi_min_duration": getattr(cfg, "obi_min_duration", None),
            "iceberg_refresh_count": getattr(cfg, "iceberg_refresh_count", None),
            "iceberg_min_duration": getattr(cfg, "iceberg_min_duration", None),
            "iceberg_refresh_min_abs": getattr(cfg, "iceberg_refresh_min_abs", None),
            "dist_atr_threshold": getattr(cfg, "dist_atr_threshold", None),
            "min_signal_interval_sec": getattr(cfg, "min_signal_interval_sec", None),
            "stop_mode": getattr(cfg, "stop_mode", None),
            "stop_atr_mult": getattr(cfg, "stop_atr_mult", None),
            "stop_pct": getattr(cfg, "stop_pct", None),
            "stop_points": getattr(cfg, "stop_points", None),
            "tp_mode": getattr(cfg, "tp_mode", None),
            "tp_rr": getattr(cfg, "tp_rr", None),
            "tp_atr_mults": getattr(cfg, "tp_atr_mults", None),
            # Dynamic SL Meta
            "slq_used": cfg.get("slq_used"),
            "slq_bump_atr": cfg.get("slq_bump_atr"),
            "slq_n": cfg.get("slq_n"),
            "slq_q90": cfg.get("slq_q90"),
            "slq_tp1_prob": cfg.get("slq_tp1_prob"),
            "slq_original_mult": cfg.get("slq_original_mult"),
        }
        # минимизация размера: выкидываем None
        return {k: v for k, v in raw.items() if v is not None}

    def _stable_signal_id(self, payload: dict[str, Any]) -> str:
        """
        Стабильный signal_id для replay/golden тестов.

        ВАЖНО:
          - выключено по умолчанию (UUID остаётся как раньше)
          - включать только для record&replay/regression тестов:
              REPLAY_STABLE_SIGNAL_ID=1

        Ключ:
          symbol|kind|side|ts_bucket|level_price_rounded|venue|timeframe
        """
        ts = int(payload.get("ts", 0) or 0)
        bucket_ms = int(os.getenv("OUTBOX_SEM_DEDUP_BUCKET_MS", "1000") or 1000)
        ts_bucket = (ts // max(bucket_ms, 1)) * max(bucket_ms, 1)
        lvl = payload.get("level_price", None)
        try:
            lvl_f = float(lvl) if lvl is not None else 0.0
        except Exception:
            lvl_f = 0.0
        lvl_dec = int(os.getenv("OUTBOX_SEM_DEDUP_LEVEL_DECIMALS", "8") or 8)
        lvl_r = round(lvl_f, max(0, lvl_dec))

        sym = str(payload.get("symbol", "") or "")
        kind = str(payload.get("kind", "") or "")
        side = str(payload.get("side", "") or "")
        venue = str(payload.get("venue", "") or payload.get("exchange", "") or "")
        tf = str(payload.get("timeframe", "") or "")
        base = f"{sym}|{kind}|{side}|{ts_bucket}|{lvl_r}|{venue}|{tf}"
        return hashlib.sha1(base.encode("utf-8")).hexdigest()



        # fail-open доступ к SignalMetrics, созданному в BaseOrderflowHandler
    def _emit_candidate_signal(self, ctx: Any, scored: ScoredCandidate) -> bool:
        """
        Main orchestration pipeline.
        Delegated to SignalOrchestrator.
        """
        return self._orchestrator.process(
            ctx=ctx,
            detect_fn=lambda _: [scored],
        )






    # === L3-Lite integration ===

    def on_l3_event(self, ev) -> None:
        """
        Обработчик L3-Lite событий.
        ev должен быть объектом с полями: ts_ms, kind, side, price, qty
        """
        from regime.l3_lite_models import L3LiteEvent
        l3_ev = L3LiteEvent(
            ts_ms=ev.ts_ms,
            kind=ev.kind,
            side=ev.side,
            price=ev.price,
            qty=ev.qty,
        )
        self.l3_agg.on_l3_event(l3_ev)

    def on_book_update(self, snap) -> None:
        """
        Обработчик обновлений книги ордеров.
        snap должен быть объектом с полями: ts_ms, bids, asks
        """
        from regime.l3_lite_models import BookSnapshot
        book_snap = BookSnapshot(
            ts_ms=snap.ts_ms,
            bids=snap.bids,
            asks=snap.asks,
        )
        self.l3_agg.on_book_update(book_snap)

    def on_signal_candidate(self, ctx: Any, signal_kind: str, side: int, raw_score: float, **kwargs: Any) -> None:
        """
        Entry point for detected signals.
        Refactored to delegate entirely to SignalOrchestrator.
        """
        # Form basic candidate object
        cand = CandidatePipeline(
            kind=str(signal_kind or "custom"),
            side=side,
            raw_score=float(raw_score),
            level_price=None, # will be attached by orchestrator logic if needed
            level_key=None,
            reasons=[],
            meta=kwargs
        )
        
        # Delegate to orchestrator
        # detect_fn wraps the single candidate payload we received
        self._orchestrator.process(
            ctx=ctx,
            detect_fn=lambda _: [cand]
        )

    # ----------------------------------------------------------------------
    # Обертки для обратной совместимости (методы физически "перемещены", вызывающие продолжают работать)
    # ----------------------------------------------------------------------
    def _get_regime_hist(self, symbol: str) -> deque:
        ml = int(getattr(self._regime_cfg, "regime_window_size", 240) or 240)
        d = self._regime_history.get(symbol)
        d2 = ensure_bounded_deque(d, ml)
        if d2 is not d:
            self._regime_history[symbol] = d2
        return d2

    def _get_bar_hist(self, symbol: str) -> deque:
        # защита истории баров (отдельный maxlen, чтобы избежать случайного неограниченного роста)
        ml = int(getattr(self, "_bar_history_maxlen", 512) or 512)
        d = self._bar_history.get(symbol)
        d2 = ensure_bounded_deque(d, ml)
        if d2 is not d:
            self._bar_history[symbol] = d2
        return d2

    def _compute_regime_features(self, ctx: Any):
        return self._regime_detector.compute_features(ctx)

    def _update_regime_history(self, ctx: Any) -> None:
        self._regime_detector.update_history(ctx)

    def _maybe_log_candidate(self, *, ctx: Any, cand: Any, parts: dict[str, Any], now_ms: Optional[int] = None) -> None:
        """
        Систематическое логирование кандидатов:
          - сэмплирование по времени (каждые N мс)
          - форсирование при смене режима
          - форсирование при исключениях (вызывающий может вызвать sampler.force()).
        """
        try:
            reg = str(getattr(ctx, "regime", "") or "")
            if reg and reg != self._last_regime_label:
                self._last_regime_label = reg
                self._candidate_log_sampler.force()
            if not self._candidate_log_sampler.maybe(now_ms):
                return
            # Строим дешевую структурированную сводку (без гигантских блобов).
            obj = {
                "type": "candidate_sample",
                "ts": int(getattr(ctx, "ts", 0) or 0),
                "symbol": getattr(ctx, "symbol", None),
                "kind": self._safe_str(getattr(cand, "kind", "") or ""),
                "side": self._safe_str(getattr(cand, "side", "") or ""),
                "raw_score": finite_or(getattr(cand, "raw_score", None), 0.0),
                "regime": reg,
                # только топовые скалярные части (избегаем дампа полного ctx)
                "spread_bps": finite_or(getattr(ctx, "spread_bps", None), -1.0),
                "taker_rate": finite_or(getattr(ctx, "taker_rate_ema", None), -1.0),
                "geometry_score": finite_or(getattr(ctx, "geometry_score", None), -1.0),
            }
            # PERF: централизованный компактный json
            self.logger.info(dumps1(obj))
        except Exception:
            # fail-open: никогда не ломать путь сигнала
            if hasattr(self, "logger"):
                self.logger.debug("maybe_log_candidate failed", exc_info=True)
            return

    def _publish_trace_diag_best_effort(self, ctx: Any, *, reason: str) -> None:
        """
        Публикация DecisionTrace в diagnostics stream (НЕ outbox).
        Это нужно, чтобы veto-сигналы тоже были дебажимы end-to-end.
        Fail-open: любые ошибки подавляются.
        """
        if not trace_enabled():
            return
        try:
            # VETO — публикуем всегда (без sampling), но можно ограничить по env.
            if not os.getenv("DECISION_TRACE_DIAG_STREAM"):
                return
            redis_client = getattr(self, "redis", None) or getattr(ctx, "redis", None)
            if redis_client is None:
                return
            tr = serialize_trace_from_ctx(ctx)
            if not isinstance(tr, dict):
                return
            payload = {
                "type": "diagnostic",
                "tradeable": False,
                "reason": str(reason or ""),
                "trace_id": str(getattr(ctx, "trace_id", "") or tr.get("trace_id") or ""),
                "sid": str(tr.get("sid") or getattr(ctx, "sid", "") or ""),
                "symbol": str(tr.get("symbol") or getattr(ctx, "symbol", "") or ""),
                "kind": str(tr.get("kind") or ""),
                "trace": tr,
                "ts_ms": get_ny_time_millis(),
            }
            stream = str(os.getenv("DECISION_TRACE_DIAG_STREAM") or "stream:signals:diagnostics")
            redis_client.xadd(stream, {"data": json.dumps(payload, ensure_ascii=False)}, maxlen=50000, approximate=True)
        except Exception:
            logger.debug("publish_trace_diag failed", exc_info=True)
            return

    def _emit_veto_metric(self, *, kind: str, ctx: Any, reason_code: str) -> None:
        """
        9.6 Минимальные метрики:
          - signals_veto_total{reason_code,kind,symbol}
        Нет жесткой зависимости от Prom/StatsD; мы просто вызываем опциональный sink.
        """
        m = self._metrics
        if not m:
            return
        try:
            sym = str(getattr(ctx, "symbol", "") or "")
            rc = normalize_reason(reason_code or "VETO_UNKNOWN")
            # Ожидаемый контракт sink: inc(name, value=1, tags={...})
            m.inc("signals_veto_total", 1, tags={"reason": rc, "kind": str(kind or ""), "symbol": sym})
        except Exception:
            # fail-open: метрики никогда не ломают торговый пайплайн
            logger.debug("emit_veto_metric failed", exc_info=True)
            return
