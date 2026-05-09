from __future__ import annotations

"""
Initialization logic for CryptoOrderFlowHandler.

This module contains all initialization-related code including __init__ method,
configuration setup, and initialization helpers.
"""


import os
from typing import Any

from common.log_sampling import TimeSampler
from core.instrument_config import get_config
from handlers.base_orderflow_handler import OrderFlowConfig
from handlers.confidence_pct_provider import build_confidence_pct_fn
from handlers.crypto_orderflow.components.gates import CryptoSignalGates
from handlers.crypto_orderflow.components.liquidity import CryptoLiquidity
from handlers.crypto_orderflow.components.market_state import CryptoMarketState
from handlers.crypto_orderflow.components.observability import CryptoObservability
from handlers.crypto_orderflow.components.sampler import _SampleEveryMs

# New Components
from handlers.crypto_orderflow.config.handler_config import CryptoOrderFlowConfigManager

# NOTE (важно):
# В проекте есть две реализации cost-edge gate:
#   1) handlers.crypto_orderflow.utils.edge_cost_gate.EdgeCostGate (детерминированная, decision-object)
#   2) handlers.crypto_orderflow.core.cost_edge_gate.CostEdgeGate  (альтернативная, другой интерфейс)
#
# Раньше был рассинхрон:
#   - в handler импортировался utils.EdgeCostGate
#   - в init создавался core.CostEdgeGate
# Это ломает контракт (evaluate/config/result) и приводит к нерабочему гейту или runtime-ошибкам.
#
# Фикс: использовать ОДИН источник правды -> utils.EdgeCostGate.
from handlers.crypto_orderflow.config.runtime_config import _RuntimeCfg
from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdFilter
from handlers.crypto_orderflow.pipeline.orchestrator import SignalOrchestrator

# Import new filters for cost edge gate and confidence thresholds
# Cost-edge gate: ДОЛЖЕН быть единым по коду - используем utils.EdgeCostGate
# Он читает ctx.* поля (entry/tp1/sl/atr) и возвращает детерминированный decision
from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate
from handlers.crypto_orderflow.utils.entry_policy_gate import EntryPolicyGate

# NEW: Quality gates (regime/session/liquidity + consistency + data-quality)
from handlers.crypto_orderflow.utils.pre_publish_gates import ConsistencyGate, HardDataQualityGate, RegimeSessionGate
from handlers.crypto_orderflow.utils.quality_gates import (
    DataQualityGate,
    RegimeSessionLiquidityGate,
    SignalConsistencyGate,
)
from handlers.crypto_orderflow.utils.risk_cfg_resolver import RiskCfgResolver
from handlers.crypto_orderflow.utils.smt_coherence_gate import SmtLeaderCoherenceGate
from handlers.crypto_orderflow.utils.trail_conditional import TrailConditionalEvaluator
from handlers.handler_dependencies import HandlerDependencies
from services.feature_drift_alarm import FeatureDriftAlarm
from signal_scoring.reason_registry import normalize_reason
from signals.empirical_levels import EmpiricalLevels, RedisEmpiricalStatsProvider
from signals.empirical_levels_dyn import EmpiricalLevelsConfig, RedisEmpiricalLevelsProvider


class CryptoOrderFlowInitMixin:
    """
    Mixin class containing initialization logic for CryptoOrderFlowHandler.
    This separates initialization concerns from the main handler logic.
    """

    def __init__(
        self,
        symbol: str,
        config: OrderFlowConfig | None = None,
        *,
        health_metrics: object | None = None,
        calibrator: Any = None,
        dependencies: HandlerDependencies | None = None,
        **kwargs: Any,
    ):
        config = config or get_config(symbol, use_env=True)
        # Create HTF provider (redis available after super().__init__)
        # We'll set it after super().__init__ since redis is initialized there
        super().__init__(
            symbol,
            config,
            source_name="CryptoOrderFlow",
            signal_stream_prefix="signals:cryptoorderflow",
            health_metrics=health_metrics,
            dependencies=dependencies,
        )

        # NOTE: avoid os.getenv() in hot paths; keep all flags/limits in fields.
        self._cfg = _RuntimeCfg.from_env()

        # ---------------------------------------------------------------------
        # Confidence calibration (hot-path optimized)
        # ---------------------------------------------------------------------
        # IMPORTANT:
        #  - в проде вы хотите RollingPercentileCalibrator
        #    (handlers/crypto_orderflow_calibration.py), т.к. он держит rolling history
        #    по (symbol, kind) и возвращает percentile rank (0..100).
        #  - CalibrationService по проекту чаще про калибровку метрик/порогов,
        #    но мы поддерживаем его shape, если вдруг используется для confidence.
        #
        # PERF:
        #  - все "угадай сигнатуру" / hasattr / TypeError делаем 1 раз в __init__,
        #    а в hot path остаётся один вызов self._conf_pct_fn(...).
        self._calibrator = calibrator if calibrator is not None else kwargs.get("calibrator")  # RollingPercentileCalibrator | CalibrationService | None
        self._use_calibrator = os.getenv("USE_SCORE_CALIBRATOR", "1").lower() not in {"0", "false", "no"}
        # Hard clamp for any returned confidence (defensive programming)
        self._confidence_cap = float(os.getenv("CONFIDENCE_CAP_PCT", "95.0"))

        # Pre-bind hot-path fn. If disabled => returns fallback.
        self._conf_pct_fn = build_confidence_pct_fn(
            self._calibrator if self._use_calibrator else None,
            cap_pct=self._confidence_cap,
        )

        # 9.7 Perf: cache env/config ONCE (no getenv in hot paths)
        self._strict_reason_codes = os.getenv("STRICT_REASON_CODES", "0").lower() in {"1", "true", "yes"}
        self._geometry_missing_score = float(os.getenv("GEOMETRY_MISSING_SCORE01", "0.10"))  # neutral (no veto)
        self._geometry_enabled = os.getenv("GEOMETRY_ENABLED", "1").lower() not in {"0", "false", "no"}

        # System-wide log samplers:
        # - candidate detail logs are expensive: sample 1/N seconds (or forced on regime change/error).
        self._candidate_log_sampler = TimeSampler(every_ms=self._cfg.candidate_log_every_ms)
        self._signal_log_sampler = TimeSampler(every_ms=self._cfg.signal_log_every_ms)

        # ---- 7) Perf: never call os.getenv in hot paths ----
        # Candidate logging is sampled (default: once per 15s) and can be forced on regime changes.
        self._candidate_log_every_ms = int(os.getenv("CANDIDATE_LOG_EVERY_MS", "15000"))
        self._candidate_log_on_regime_change = os.getenv("CANDIDATE_LOG_ON_REGIME_CHANGE", "1").lower() not in {"0", "false", "no"}
        self._cand_log_gate = _SampleEveryMs(every_ms=self._candidate_log_every_ms)
        self._last_regime_for_candidate_log: str = ""

        # Track last regime label to force log on changes (debug value without spam).
        self._last_regime_label: str = ""

        # ============================================================================
        # NEW: Cost Edge Gate and Enhanced Confidence Thresholds
        # ============================================================================
        # Initialize cost edge gate filter (prevents trading below costs).
        #
        # IMPORTANT:
        #   В репо есть ДВЕ реализации:
        #     1) handlers.crypto_orderflow.utils.edge_cost_gate.EdgeCostGate  (детерминированный, fail-open, умеет kind)
        #     2) handlers.crypto_orderflow.core.cost_edge_gate.CostEdgeGate   (другая сигнатура/поля)
        #
        #   В crypto_orderflow_handler.py у вас используется utils.EdgeCostGate.
        #   Поэтому здесь тоже используем utils.EdgeCostGate, чтобы исключить runtime mismatch.
        self._cost_edge_gate = EdgeCostGate.from_env()
        self._cost_edge_enabled = bool(self._cost_edge_gate.enabled)

        # Логирование veto (так как у utils.EdgeCostGate нет config.log_veto).
        # Поддерживаем два имени ENV:
        #   EDGE_COST_LOG_VETO=1/0 (предпочтительно)
        #   LOG_EDGE_VETO=1/0      (legacy)
        def _env_bool(name: str, default: bool) -> bool:
            v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
            return v in {"1", "true", "yes", "on"}
        self._cost_edge_log_veto = _env_bool("EDGE_COST_LOG_VETO", _env_bool("LOG_EDGE_VETO", True))

        # ----------------------------------------------------------------------
        # Pre-publish quality gates (strong quality uplift + churn reduction)
        # ----------------------------------------------------------------------
        self._hard_quality_gate = HardDataQualityGate.from_env()
        self._regime_session_gate = RegimeSessionGate.from_env()
        self._consistency_gate = ConsistencyGate.from_env()
        # --------------------------------------------------------------------
        # SMT leader/coherence gate (observe/veto).
        # Fail-open: missing Redis state never blocks signals.
        # NOTE: attach ctx.smt_* for audit + reliability post-calibration.
        # --------------------------------------------------------------------
        try:
            self._smt_leader_gate = SmtLeaderCoherenceGate.from_env(redis_client=getattr(self, "redis", None))
        except Exception:
            self._smt_leader_gate = None

        # NEW: entry-policy gate (spread shock / burst flip / cancel_to_trade)
        # EntryPolicyGate is stateful (cooldowns/delays), so we create it ONCE at init.
        # Hot-path must NEVER call from_env() again; it should reuse this instance.
        self._entry_policy_gate = EntryPolicyGate.from_env()

        # NEW: feature drift alarm (writes drift:state / drift:active in Redis)
        self._feature_drift_alarm = FeatureDriftAlarm.from_env()

        # counters (optional)
        self._veto_entry_policy_total = 0

        # Monitoring counters
        self._veto_hard_quality_total = 0
        self._veto_regime_session_total = 0
        self._veto_consistency_total = 0
        self._veto_smt_total = 0

        # Initialize confidence threshold filter (symbol-specific confidence gates)
        self._confidence_threshold_filter = ConfidenceThresholdFilter.from_env()

        # --------------------------------------------------------------------
        # NEW: conditional trailing evaluator (publisher side).
        #
        # This computes ctx.trail_after_tp1 + ctx.trail_after_tp1_reason
        # and must be propagated into payload -> TradeMonitor -> PositionState.
        #
        # Fail-open: evaluator exists even if Redis missing; it will allow or veto
        # depending on ENV (TRAIL_VETO_IF_NO_STATS).
        # --------------------------------------------------------------------
        self._trail_cond = TrailConditionalEvaluator.from_env(redis=getattr(self, "redis", None))

        # Initialize risk config resolver for compute_levels (SL/TP calculation)
        # This resolves symbol-specific ENV (BTC_STOP_MODE, ETH_TP_RR, etc.)
        self._risk_cfg: RiskCfgResolver = RiskCfgResolver(
            redis_client=getattr(self, "redis", None)
        )

        # ------------------------------------------------------------------
        # 3.3 "ещё выше": cache RiskCfgResolver.resolve(symbol)
        #
        # Why:
        #   resolve() pulls many ENV keys and normalizes values.
        #   In publish paths we call it multiple times per signal.
        #
        # TTL:
        #   RISK_CFG_CACHE_TTL_SEC=0   => cache forever (default).
        #   RISK_CFG_CACHE_TTL_SEC>0   => periodic refresh (for live tuning).
        # ------------------------------------------------------------------

        self._risk_cfg_cache = {}          # type: ignore[attr-defined]
        self._risk_cfg_cache_ts = {}       # type: ignore[attr-defined]
        try:
            self._risk_cfg_cache_ttl_sec = float(os.getenv("RISK_CFG_CACHE_TTL_SEC", "0") or "0")  # type: ignore[attr-defined]
        except Exception:
            self._risk_cfg_cache_ttl_sec = 0.0  # type: ignore[attr-defined]

        # ---------------------------------------------------------------------
        # Dynamic empirical levels (TP1/SL) from recent MFE/MAE buffers.
        #
        # This is the strongest quality upgrade because it adapts SL/TP to the
        # actual distribution of outcomes per {kind, symbol, tf, regime}.
        #
        # Fail-open: if Redis buffers not available or not enough samples,
        # nothing changes.
        # ---------------------------------------------------------------------
        try:
            self._emp_levels_cfg = EmpiricalLevelsConfig.from_env()
        except Exception:
            self._emp_levels_cfg = EmpiricalLevelsConfig(
                enabled=False, min_n=60, q_tp1=0.60, q_sl=0.80, q_ttd=0.50,
                use_regime_dim=True, fallback_to_na_regime=True, cache_ms=2000,
                max_bps=2500.0, min_bps=5.0, max_ttd_ms=6 * 60 * 60 * 1000,
            )

        # Provider needs redis client; if handler uses a different attribute name,
        # we’ll also lazy-create in handler (safe).
        try:
            r = getattr(self, "redis", None) or getattr(self, "_redis", None) or getattr(self, "redis_client", None)
            self._emp_levels_provider = RedisEmpiricalLevelsProvider(r, self._emp_levels_cfg) if r is not None else None
        except Exception:
            self._emp_levels_provider = None

        # ------------------------------------------------------------------
        # Empirical adaptive levels (MFE/MAE/TTD autocalibration)
        # ------------------------------------------------------------------
        try:
            tf = (os.getenv("LEVELS_EMPIRICAL_TF") or os.getenv("ATR_TF") or "1m").strip()
            buf_max = int(os.getenv("LEVELS_EMPIRICAL_BUF_MAX", "300"))
            use_regime_dim = (os.getenv("LEVELS_EMPIRICAL_USE_REGIME_DIM", "1").strip().lower() in {"1","true","yes","on"})
            provider = RedisEmpiricalStatsProvider(
                getattr(self, "redis", None),
                tf=tf,
                buf_max=buf_max,
                use_regime_dim=use_regime_dim,
            )
            self._empirical_levels = EmpiricalLevels.from_env(provider=provider)
        except Exception:
            self._empirical_levels = EmpiricalLevels.from_env(provider=None)

        # ------------------------------------------------------------------
        # NEW: Conditional trailing evaluator.
        #
        # Decision is computed in crypto_orderflow_handler._publish_signal()
        # and persisted into ctx (and later into PositionState via signal payload).
        # Fail-open: if redis is missing -> evaluator still works using momentum-only.
        # ------------------------------------------------------------------
        try:
            from services.trailing_condition import TrailingConditionEvaluator
            self._trail_cond = TrailingConditionEvaluator(getattr(self, "redis", None))
        except Exception:
            self._trail_cond = None

        # Veto counters for monitoring
        self._veto_cost_edge_total = 0
        self._veto_confidence_threshold_total = 0

        # ============================================================================
        # NEW: Quality gates (increase trade quality BEFORE publishing / execution)
        # ============================================================================
        # 1) Data quality gate: epoch sanity, lag, out-of-order, quarantine, ATR staleness (if timestamps available).
        # 2) Regime/session gating + liquidity gating: forbid trades in wrong regimes or poor liquidity.
        # 3) Consistency gate: feature agreement rules per signal kind.
        #
        # These gates are designed as FAIL-OPEN on missing optional metrics by default,
        # but can be made strict via ENV if you want harder filtering.
        self._data_quality_gate: DataQualityGate = DataQualityGate.from_env()
        self._regime_liquidity_gate: RegimeSessionLiquidityGate = RegimeSessionLiquidityGate.from_env()
        self._consistency_gate: SignalConsistencyGate = SignalConsistencyGate.from_env()

        # State for out-of-order / time sanity checks (per handler instance).
        # NOTE: if you run multiple containers/instances per symbol, each has its own state.
        self._last_event_ts_ms: int | None = None

        # Counters for observability (so you can see where quality is lost).
        self._veto_quality_total: int = 0
        self._veto_quality_by_reason: dict[str, int] = {}

        # ----------------------------------------------------------------------
        # Refactored Components Initialization
        # ----------------------------------------------------------------------
        # 1. Config Manager
        self._cfg_manager = CryptoOrderFlowConfigManager(self, symbol, config)

        # 2. Market State
        self._market_state = CryptoMarketState()

        # 3. Liquidity Logic
        self._liquidity_comp = CryptoLiquidity()

        # 4. Observability (Wraps logging/metrics)
        self._observability = CryptoObservability(self.logger, health_metrics)
        # Link samplers existing in mixin
        if hasattr(self, "_candidate_log_sampler"):
             self._observability.set_sampler(self._candidate_log_sampler)

        # 5. Signal Gates (Wraps entry/cost/consistency)
        self._gates = CryptoSignalGates(
            entry_policy=self._entry_policy_gate,
            cost_gate=self._cost_edge_gate,
            consistency_gate=self._consistency_gate,
            regime_liquidity_gate=self._regime_liquidity_gate,
            smt_gate=getattr(self, "_smt_leader_gate", None)
        )

        # 6. Orchestrator
        # Note: emitter and confirmations might be lazy or init in base.
        # We pass self (acting as legacy container) if needed,
        # but better to pass specific attributes if they exist.
        # Assuming self._emitter and self._confirmations exist after super init.
        self._orchestrator = SignalOrchestrator(
            config=self._cfg_manager,
            gates=self._gates,
            liquidity=self._liquidity_comp,
            observability=self._observability,
            confirmations_engine=getattr(self, "_confirmations", None),
            emitter=getattr(self, "_emitter", None),
        )

    # ------------------------------------------------------------
    # "ещё выше": cached wrapper for consistency gate
    # Safe for multi-candidate-per-ctx: cache key includes (kind, side).
    # FAIL-OPEN: cache failures never break trading.
    # ------------------------------------------------------------
    def _consistency_gate_cached(self, *, ctx: Any, symbol: str, kind: str, side: str):
        gate = getattr(self, "_consistency_gate", None)
        if gate is None or not callable(getattr(gate, "evaluate", None)):
            return None
        k = ("consistency", (kind or ""), (side or ""))
        try:
            cache = getattr(ctx, "_gate_cache", None)
            if not isinstance(cache, dict):
                cache = {}
                try:
                    ctx._gate_cache = cache
                except Exception:
                    cache = None
            if isinstance(cache, dict) and k in cache:
                return cache[k]
        except Exception:
            cache = None
        try:
            d = gate.evaluate(ctx=ctx, symbol=symbol, kind=str(kind), side=side)
        except Exception:
            d = None
        try:
            if isinstance(cache, dict):
                cache[k] = d
        except Exception:
            pass
        return d

    def _bump_quality_veto(self, reason_code: str) -> None:
        """Small helper to keep veto accounting uniform and side-effect safe."""
        try:
            self._veto_quality_total += 1
            self._veto_quality_by_reason[reason_code] = self._veto_quality_by_reason.get(reason_code, 0) + 1
        except Exception:
            pass

    def _confidence_pct(self, *, kind: str, ctx: Any, final_score: float) -> float:
        """
        Single hot-path call. No probing/hasattr here.
        """
        sym = str(getattr(ctx, "symbol", "") or "")
        ts_ms = getattr(ctx, "ts", None) or getattr(ctx, "ts_ms", None)
        try:
            ts_ms_i = int(ts_ms) if ts_ms is not None else 0
        except Exception:
            ts_ms_i = 0
        return float(self._conf_pct_fn((kind or ""), sym, float(final_score), int(ts_ms_i)))

    def _metrics_observe(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """
        Optional histogram/summary hook.
        Supports different sinks:
          - observe(name, value, tags)
          - hist(name, value, tags)
        """
        m = self._metrics
        if not m:
            return
        try:
            if hasattr(m, "observe"):
                m.observe(name, float(value), tags=tags or {})
            elif hasattr(m, "hist"):
                m.hist(name, float(value), tags=tags or {})
        except Exception:
            return

    def _get_ctx_l2_snapshot(self, ctx: Any) -> Any | None:
        # Мягкая совместимость: разные участки кода могли назвать поле по-разному.
        return (
            getattr(ctx, "l2_snapshot", None)
            or getattr(ctx, "l2", None)
            or getattr(ctx, "book", None)
        )

    def _emit_veto_metric(self, *, kind: str, ctx: Any, reason_code: str) -> None:
        """
        9.6 Minimal metrics:
          - signals_veto_total{reason_code,kind,symbol}
        No hard dependency on Prom/StatsD; we just call an optional sink.
        """
        m = self._metrics
        if not m:
            return
        try:
            sym = str(getattr(ctx, "symbol", "") or "")
            rc = normalize_reason(reason_code or "VETO_UNKNOWN")
            # gauge-style metric: current total vetoes by reason/kind/symbol
            if hasattr(m, "gauge"):
                m.gauge("signals_veto_total", 1, tags={"reason_code": rc, "kind": (kind or ""), "symbol": sym})
            elif hasattr(m, "inc"):
                # if gauge not available, use counter-style
                m.inc("signals_veto_total", tags={"reason_code": rc, "kind": (kind or ""), "symbol": sym})
        except Exception:
            return
