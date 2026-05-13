from __future__ import annotations

import json
import logging
import math
import os
import time
from collections.abc import Sequence
from types import SimpleNamespace
from typing import Any

from common.contracts.registry import OrderIntentV1, SignalV1
from common.decision_trace import ensure_trace, trace_enabled, trace_gate
from common.normalization import generate_signal_id, normalize_side_3_safe
from core.crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter
from core.dyn_cfg_keys import DynCfgKeys as DK
from core.instrument_config import get_specs
from core.of_confirm_engine import OFConfirmEngine
from core.redis_keys import RedisStreams as RS
from core.signal_payload import SignalPayload, StrongGateDecision
from core.gates.decision import GateDecisionV1
from handlers.crypto_orderflow.components.gates import GateOrchestrator
from handlers.crypto_orderflow.utils.log_sampler import LogSamplerFactory
from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate
from handlers.crypto_orderflow.utils.pre_publish_gates import (
    AtrFloorGate,
    BreadthGate,
    HardDataQualityGate,
    RegimeSessionGate,
    ConsistencyGate,
    SmtCoherenceGate,
)
from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate
from handlers.crypto_orderflow.utils.portfolio_exposure_gate import PortfolioExposureGate
from services.async_signal_publisher import AsyncSignalPublisher, StreamSink

# P4 latency contract: stamp emit time and observe feature_to_emit + end_to_end_event
from services.observability.latency_contract import stamp_emit_and_observe_async
from services.observability.latency_semconv import (
    FIELD_TS_EMIT_MS,
    FIELD_TS_EVENT_MS,
    FIELD_TS_FEATURE_MS,
    ensure_epoch_ms_fields,
)

# P5: book sanity + stream integrity gates (pre-publish, fail-open)
from services.orderflow.book_sanity_gate import BookSanityGate
from services.orderflow.metrics_sentiment_context import (
    sentiment_ctx_gate_monitor_hit_total,
    sentiment_ctx_gate_tighten_total,
    sentiment_ctx_missing_total,
    sentiment_ctx_stale_total,
    sentiment_risk_multiplier,
)
from services.orderflow.runtime import SymbolRuntime
from services.orderflow.stream_integrity_gate import StreamIntegrityGate
from services.orderflow.stream_integrity_gate import StreamIntegrityGate
from services.orderflow.utils import session_utc
from services.outbox.atomic_outbox import atomic_xadd_async
from services.outbox.envelope_builder import build_outbox_envelope, build_trace_sidecar_meta_from_ctx, dumps_env

# Imports for publishing logic
from services.pnl_math import calculate_position_size
from services.signal_preprocess import preprocess_signal_for_publish
from services.tp_config import parse_tp_ratio
from utils.task_manager import safe_create_task
from utils.time_utils import get_ny_time_millis
import contextlib
import redis

# Trade Profile Router (Phase 1 — shadow)
from services.trade_profile_router import (
    TradeProfileRouter,
    build_signal_profile_meta,
)
from services.abc_router import regime_group as _regime_group
from services.orderflow.decision_snapshot import build_decision_snapshot, publish_decision_snapshot
from services.orderflow.metrics import (
    of_inputs_publish_error_total,
    strong_gate_veto_total,
    of_session_outcome_total,
    signals_total,
)
try:
    from prometheus_client import Histogram, Counter
    _FEATURE_TO_DECISION_MS = Histogram(
        "trading_feature_to_decision_ms",
        "Latency from feature calc to gate decision",
        buckets=[5, 10, 20, 50, 100]
    )
    _DECISION_TO_OUTBOX_MS = Histogram(
        "trading_decision_to_outbox_ms",
        "Latency from gate decision to Redis XADD",
        buckets=[1, 5, 10, 20, 50]
    )
    _PRE_PUBLISH_VETO_TOTAL = Counter(
        "pre_publish_veto_total",
        "Total pre-publish vetos",
        ["gate", "reason_code", "symbol", "kind"]
    )
    _INVALID_ENVELOPE_TOTAL = Counter(
        "outbox_invalid_envelope_total",
        "Total outbox envelopes rejected due to malformed structure",
        ["symbol"]
    )
except ImportError:
    _FEATURE_TO_DECISION_MS = None
    _DECISION_TO_OUTBOX_MS = None
    _PRE_PUBLISH_VETO_TOTAL = None
    _INVALID_ENVELOPE_TOTAL = None

# Signal Pipeline Logger
logger = logging.getLogger("of_signal_pipeline")

class SignalPipeline:
    def __init__(self, publisher: AsyncSignalPublisher, atr_cache: Any):
        """
        :param publisher: AsyncSignalPublisher instance for broadcasting messages.
        :param atr_cache: ATRCache instance (typically shared/global) for level calc.
        """
        self.publisher = publisher
        self.atr_cache = atr_cache
        # OFConfirm engine for validation simulation
        self.of_engine = OFConfirmEngine()
        # Tickers & Streams
        self.cryptoorderflow_signal_stream_template = os.getenv("CRYPTO_ORDERFLOW_SIGNAL_STREAM", RS.CRYPTO_ORDERFLOW_TPL)
        self.raw_signal_stream = os.getenv("CRYPTO_RAW_SIGNAL_STREAM", RS.CRYPTO_RAW)
        self.notify_stream = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
        self.notify_maxlen = int(os.getenv("CRYPTO_NOTIFY_MAXLEN", "20000"))

        # Confidence score telemetry stream (high-frequency; keep off by default)
        self.conf_scores_publish_enabled = bool(int(os.getenv("CONF_SCORES_PUBLISH_ENABLED", "0") or 0))
        self.conf_scores_stream = os.getenv("CONF_SCORES_STREAM", RS.CONF_SCORES)
        # 200k ≈ 13s buffer -> 1_000_000 ≈ 65s buffer
        self.conf_scores_stream_maxlen = int(os.getenv("CONF_SCORES_STREAM_MAXLEN", "1000000") or 1000000)
        self.conf_scores_schema_version = int(os.getenv("CONF_SCORES_SCHEMA_VERSION", "1") or 1)
        self.conf_scores_include_evidence_json = bool(int(os.getenv("CONF_SCORES_INCLUDE_EVIDENCE_JSON", "0") or 0))
        self.conf_scores_quarantine_stream = os.getenv("CONF_EVIDENCE_QUARANTINE_STREAM", RS.CONF_QUARANTINE)
        self.conf_scores_quarantine_maxlen = int(os.getenv("CONF_EVIDENCE_QUARANTINE_MAXLEN", "20000") or 20000)

        # Initialize log samplers for signal messages (every 10000th message)
        LogSamplerFactory.get_sampler("SIGNAL_RAW_STREAM", 10000)
        LogSamplerFactory.get_sampler("SIGNAL_PUBLISHED", 10000)

        # Pre-publish gates (fail-open unless enabled by ENV)
        # of_inputs stream: controls for RAM pressure
        self.of_inputs_stream = os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS)
        self.of_inputs_publish_enabled = os.getenv("PUBLISH_OF_INPUTS", os.getenv("OF_INPUTS_PUBLISH_ENABLED", "1")).lower() in {"1", "true", "yes", "on"}
        self.of_inputs_publish_strict = bool(int(os.getenv("OF_INPUTS_PUBLISH_STRICT", "0") or 0))
        # P1 fix: 100000 → 5000 (при 40KB/entry: 5k * 40KB = 200MB max)
        self.of_inputs_stream_maxlen = int(os.getenv("OF_INPUTS_STREAM_MAXLEN", "5000") or 5000)

        self._rejected_signal_stream = os.getenv("CRYPTO_REJECTED_SIGNAL_STREAM", RS.CRYPTO_REJECTED)
        
        # Unified Orchestrator (P1)
        self.orchestrator = GateOrchestrator(
            entry_policy=None, # Loaded separately if needed
            cost_gate=EdgeCostGate.from_env(),
            portfolio_gate=PortfolioExposureGate(r=getattr(publisher, "r", None)),
            consistency_gate=ConsistencyGate.from_env(),
            regime_liquidity_gate=RegimeSessionGate.from_env(),
            smt_gate=SmtCoherenceGate.from_env(),
            dq_gate=HardDataQualityGate.from_env(),
            book_sanity_gate=BookSanityGate.from_env(),
            stream_integrity_gate=StreamIntegrityGate.from_env(),
            atr_floor_gate=AtrFloorGate.from_env(),
            breadth_gate=BreadthGate.from_env(),
        )

        # ------------------------------------------------------------------
        # Decision Snapshot (A2)
        # ------------------------------------------------------------------
        # Purpose: store a compact, joinable decision snapshot for post-trade TCA.
        # - This is NOT a trading signal stream. It is a warm-path input.
        # - Must be fail-open: never block main signal publishing.
        #
        # Contract: Redis Stream `events:decision_snapshot` with field `payload` (JSON).
        # Keys must include: sid, symbol, venue, decision_ts_ms, decision_mid (+ bid/ask).
        self.decision_snapshot_publish_enabled = bool(int(os.getenv("DECISION_SNAPSHOT_PUBLISH_ENABLED", "1") or 1))
        self.decision_snapshot_stream = os.getenv("DECISION_SNAPSHOT_STREAM", RS.DECISION_SNAPSHOT)
        # 200k ≈ 13s buffer -> 1_000_000 ≈ 65s buffer
        self.decision_snapshot_stream_maxlen = int(os.getenv("DECISION_SNAPSHOT_STREAM_MAXLEN", "1000000") or 1000000)
        self.decision_snapshot_schema_version = int(os.getenv("DECISION_SNAPSHOT_SCHEMA_VERSION", "1") or 1)

        # ------------------------------------------------------------------
        # Phase C (P2): Liquidity geometry telemetry (optional)
        # ------------------------------------------------------------------
        # To avoid high cardinality, we only emit per-symbol metrics for a small
        # allowlist. All other symbols are aggregated into symbol="__all__".
        raw_syms = (os.getenv("LIQ_GEOM_METRICS_SYMBOLS", "") or "").strip()
        self._liq_geom_syms_allow = {s.strip().upper() for s in raw_syms.split(",") if s.strip()} if raw_syms else set()

        raw_syms2 = os.getenv("FLOW_TOX_METRICS_SYMBOLS", os.getenv("LIQ_GEOM_METRICS_SYMBOLS", "") or "").strip()
        self._flow_tox_syms_allow = {s.strip().upper() for s in raw_syms2.split(",") if s.strip()} if raw_syms2 else set()
        raw_syms3 = os.getenv("DERIV_CTX_METRICS_SYMBOLS", os.getenv("FLOW_TOX_METRICS_SYMBOLS", "") or "").strip()
        self._deriv_ctx_syms_allow = {s.strip().upper() for s in raw_syms3.split(",") if s.strip()} if raw_syms3 else set()

        # Virtual routing flags (1 source of truth at startup)
        self.binance_virtual_mirror_all = os.getenv("BINANCE_VIRTUAL_MIRROR_ALL", "0").lower() in {"1", "true", "yes", "on"}
        self.binance_virtual_orders_enabled = os.getenv("BINANCE_VIRTUAL_ORDERS_ENABLED", "0").lower() in {"1", "true", "yes", "on"}

        # P0 FIX: Cache hot path variables to eliminate syscalls on signal publish
        self._cached_service_name = os.getenv("SERVICE_NAME", "python-worker")
        self._cached_dq_book_stale_flag_ms = int(os.getenv("DQ_BOOK_STALE_FLAG_MS", "1500") or 1500)
        self._cached_dq_spread_wide_flag_bps = float(os.getenv("DQ_SPREAD_WIDE_FLAG_BPS", "12") or 12.0)

        self._cached_use_outbox = (
            os.getenv("CRYPTO_USE_OUTBOX_DISPATCHER", "0").lower() in {"1","true","yes","on"}
            or os.getenv("USE_SIGNAL_OUTBOX", "0").lower() in {"1","true","yes","on"}
        )
        self._cached_shadow_outbox = os.getenv("CRYPTO_SHADOW_OUTBOX", "0").lower() in {"1","true","yes","on"}
        self._cached_outbox_stream = os.getenv("SIGNAL_OUTBOX_STREAM", RS.SIGNAL_OUTBOX)
        self._cached_gate_mode = os.getenv("ATR_GATE_MODE", os.getenv("FEES_AWARE_GATE_MODE", "ENFORCE")).upper()

        self._cached_deriv_ctx_enabled = os.getenv("DERIV_CTX_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
        self._cached_deriv_ctx_profile = os.getenv("DERIV_CTX_PROFILE", os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
        self._cached_deriv_ctx_funding_z = float(os.getenv("DERIV_CTX_FUNDING_Z_MAX", "3.0") or 3.0)
        self._cached_deriv_ctx_basis_bps = float(os.getenv("DERIV_CTX_BASIS_BPS_MAX", "10.0") or 10.0)
        self._cached_deriv_ctx_require_oi = bool(int(os.getenv("DERIV_CTX_REQUIRE_OI_FOR_VETO", "1") or 1))
        self._cached_deriv_ctx_tighten_mult = float(os.getenv("DERIV_CTX_TIGHTEN_ADD_MULT", "1.0") or 1.0)
        self._cached_deriv_ctx_tighten_cap = float(os.getenv("DERIV_CTX_TIGHTEN_ADD_CAP_BPS", "8.0") or 8.0)
        self._cached_min_conf_pct = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70"))

        self._cached_fees_bps_rt = float(os.getenv("FEES_BPS_RT", "10"))
        self._cached_tp_bps_buffer = float(os.getenv("TP_BPS_BUFFER", "4"))

        # P0 FIX: Cache LIQ_GATE, FLOW_TOXIC, MANIP and others
        self._cached_liq_geom_enabled = os.getenv("LIQ_GEOM_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
        self._cached_liq_geom_profile = os.getenv("LIQ_GATE_PROFILE", os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
        self._cached_liq_min_book_slope = float(os.getenv("LIQ_MIN_BOOK_SLOPE", "0") or 0.0)
        self._cached_liq_max_dws_bps = float(os.getenv("LIQ_MAX_DWS_BPS", "0") or 0.0)
        self._cached_liq_max_recovery_ms = int(os.getenv("LIQ_MAX_RECOVERY_TIME_MS", "0") or 0)
        self._cached_liq_tighten_cap = float(os.getenv("LIQ_GEOM_TIGHTEN_ADD_CAP_BPS", "10.0") or 10.0)
        self._cached_liq_tighten_mult = float(os.getenv("LIQ_GEOM_TIGHTEN_ADD_MULT", "1.0") or 1.0)

        self._cached_flow_tox_enabled = os.getenv("FLOW_TOX_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
        self._cached_flow_tox_profile = os.getenv("FLOW_GATE_PROFILE", os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
        self._cached_flow_mode_override = os.getenv("FLOW_TOXIC_MODE", os.getenv("FLOW_TOX_MODE", "") or "").strip().lower()
        self._cached_flow_thr_z = float(os.getenv("FLOW_OFI_NORM_Z_MAX", "0") or 0.0)
        self._cached_flow_thr_vpin = float(os.getenv("FLOW_VPIN_CDF_MAX", "0") or 0.0)
        self._cached_flow_cap = float(os.getenv("FLOW_TOX_TIGHTEN_ADD_CAP_BPS", "6.0") or 6.0)
        self._cached_flow_mult = float(os.getenv("FLOW_TOX_TIGHTEN_ADD_MULT", "1.0") or 1.0)
        self._cached_flow_veto_wo_tca = bool(int(os.getenv("FLOW_TOX_VETO_WITHOUT_TCA", "0") or 0))
        self._cached_flow_thr_is = float(os.getenv("EXEC_MAX_IS_P95_BPS", "0") or 0.0)
        self._cached_flow_thr_imp = float(os.getenv("EXEC_MAX_PERM_IMPACT_P95_BPS", "0") or 0.0)

        self._cached_manip_enabled = os.getenv("MANIP_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
        self._cached_manip_profile = os.getenv("MANIP_GATE_PROFILE", os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
        self._cached_manip_mode_override = (os.getenv("MANIP_MODE", "") or "").strip().lower()
        self._cached_manip_thr_qs = float(os.getenv("MANIP_QUOTE_STUFF_SCORE_MAX", "0") or 0.0)
        self._cached_manip_thr_lay = float(os.getenv("MANIP_LAYERING_SCORE_MAX", "0") or 0.0)
        self._cached_manip_thr_otr_z = float(os.getenv("MANIP_OTR_Z_MAX", "0") or 0.0)
        self._cached_manip_tighten_cap = float(os.getenv("MANIP_TIGHTEN_ADD_CAP_BPS", "6.0") or 6.0)
        self._cached_manip_tighten_mult = float(os.getenv("MANIP_TIGHTEN_ADD_MULT", "1.0") or 1.0)

        # DefiLlama slow-context gate config
        self._cached_defillama_ctx_enabled = os.getenv("DEFILLAMA_CTX_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
        self._cached_defillama_ctx_profile = (os.getenv("DEFILLAMA_CTX_PROFILE", "monitor") or "monitor").strip().lower()
        self._cached_defillama_ctx_tighten_mult = float(os.getenv("DEFILLAMA_CTX_TIGHTEN_ADD_MULT", "1.0") or 1.0)
        self._cached_defillama_ctx_tighten_cap = float(os.getenv("DEFILLAMA_CTX_TIGHTEN_ADD_CAP_BPS", "4.0") or 4.0)
        self._cached_defillama_ctx_max_age_ms = int(os.getenv("DEFILLAMA_CTX_MAX_AGE_MS", "7200000") or 7200000)

        # Sentiment context config (Fear & Greed)
        self._cached_sentiment_ctx_enabled = os.getenv("SENTIMENT_CTX_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
        self._cached_sentiment_ctx_profile = (os.getenv("SENTIMENT_CTX_PROFILE", "monitor") or "monitor").strip().lower()
        self._cached_sentiment_ctx_max_age_ms = int(os.getenv("SENTIMENT_CTX_MAX_AGE_MS", "172800000") or 172800000)
        self._cached_sentiment_ctx_tighten_cap = float(os.getenv("SENTIMENT_CTX_TIGHTEN_CAP_BPS", "2.0") or 2.0)

        # Cross-venue context gate config (Phase 0: disabled by default)
        self._cached_crossvenue_ctx_enabled = os.getenv("CROSSVENUE_CTX_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
        self._cached_crossvenue_ctx_profile = (os.getenv("CROSSVENUE_CTX_PROFILE", "monitor") or "monitor").strip().lower()
        self._cv_profile_cache_val = self._cached_crossvenue_ctx_profile
        self._cv_profile_cache_ts_ms = 0
        self._cached_crossvenue_ctx_max_age_ms = int(os.getenv("CROSSVENUE_CTX_MAX_AGE_MS", "5000") or 5000)
        self._cached_crossvenue_ctx_min_agree = float(os.getenv("CROSSVENUE_CTX_MIN_AGREE", "0.67") or 0.67)
        self._cached_crossvenue_ctx_max_dislocation_z = float(os.getenv("CROSSVENUE_CTX_MAX_DISLOCATION_Z", "3.0") or 3.0)
        self._cached_crossvenue_ctx_max_mid_spread_bps = float(os.getenv("CROSSVENUE_CTX_MAX_MID_SPREAD_BPS", "8.0") or 8.0)
        self._cached_crossvenue_ctx_max_stale_count = int(os.getenv("CROSSVENUE_CTX_MAX_STALE_COUNT", "1") or 1)
        self._cached_crossvenue_ctx_tighten_mult = float(os.getenv("CROSSVENUE_CTX_TIGHTEN_ADD_MULT", "1.0") or 1.0)
        self._cached_crossvenue_ctx_tighten_cap = float(os.getenv("CROSSVENUE_CTX_TIGHTEN_ADD_CAP_BPS", "6.0") or 6.0)

        self._cached_binance_trail_atr_mult = os.getenv("BINANCE_TRAIL_ATR_MULT", "1.0")
        self._cached_range_tp_rr = os.getenv("RANGE_TP_RR", "1.0,1.5")

        # Exec Health
        self._cached_exec_health_auto_freeze_enabled = os.getenv("EXEC_HEALTH_AUTO_FREEZE", "0").lower() in {"1", "true", "yes", "on"}

        # ── Hot-path ENV cache: RISK / NOTIFY / GATES ──────────────────────
        # These are read on every signal emit — caching eliminates syscall pressure.
        self._cached_risk_percent = float(os.getenv("RISK_PERCENT", "5.0"))
        self._cached_notify_every_n = int(os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", "1"))
        self._cached_exec_budget_advisory = os.getenv("ATR_POLICY_EXEC_BUDGET_ADVISORY_ONLY", "false").lower() in ("true", "1", "yes")
        self._cached_exec_budget_fail_policy = os.getenv("ATR_POLICY_EXEC_BUDGET_FAIL_POLICY", "CLOSED").upper()
        self._cached_portfolio_advisory = int(os.getenv("ATR_PORTFOLIO_GATE_ADVISORY_ONLY", "1")) == 1
        self._cached_portfolio_enable = int(os.getenv("ATR_PORTFOLIO_GATE_ENABLE", "1")) == 1
        self._cached_portfolio_fail_policy = os.getenv("ATR_PORTFOLIO_GATE_FAIL_POLICY", "CLOSED").upper()
        self._cached_regime_stress_advisory = int(os.getenv("ATR_POLICY_REGIME_STRESS_ADVISORY_ONLY", "1")) == 1
        self._cached_regime_stress_enable = int(os.getenv("ATR_POLICY_REGIME_STRESS_ENABLE", "1")) == 1
        self._cached_regime_stress_fail_policy = os.getenv("ATR_POLICY_REGIME_STRESS_FAIL_POLICY", "CLOSED").upper()
        self._cached_orders_mirror_queue = os.getenv("ORDERS_QUEUE_BINANCE_MIRROR", RS.ORDERS_QUEUE_BINANCE_MIRROR)
        self._cached_orders_intent_queue = os.getenv("ORDERS_INTENT_BINANCE", RS.ORDERS_INTENT_BINANCE)
        self._cached_analytics_db_dsn = os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN") or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
        self._cached_exec_profile = (os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
        self._cached_exec_health_tf = (os.getenv("EXEC_HEALTH_TF", "all") or "all").strip().lower()
        self._cached_exec_health_venue = (os.getenv("EXEC_HEALTH_VENUE", "binance") or "binance").strip().lower()
        self._cached_force_trail = os.getenv("FORCE_TRAIL_AFTER_TP1", "0").lower() in ("1", "true", "yes", "on")
        self._cached_max_notional_usd = float(os.getenv("MAX_NOTIONAL_USD", "0") or "0")
        self._cached_account_deposit_usd = float(os.getenv("ACCOUNT_DEPOSIT_USD", "100") or "100")
        self._cached_notional_leverage_cap = float(os.getenv("NOTIONAL_LEVERAGE_CAP", "100") or "100")
        self._cached_risk_max_qty = float(os.getenv("RISK_MAX_QTY", "0") or "0")
        self._cached_liqmap_sl_widen_cap = float(os.getenv("LIQMAP_SL_WIDEN_CAP", "1.25") or 1.25)
        self._cached_sl_atr_mult_floor = float(os.getenv("SL_ATR_MULT_FLOOR", "0.78") or 0.78)
        # Liqmap TP/SL levels overlay: OFF / SHADOW / ENFORCE
        self._cached_liqmap_levels_mode = (os.getenv("LIQMAP_LEVELS_MODE", "SHADOW") or "SHADOW").upper().strip()
        self._cached_liqmap_levels_min_usd = float(os.getenv("LIQMAP_LEVELS_MIN_USD", "250000") or 250000)
        self._cached_liqmap_levels_buffer_bps = float(os.getenv("LIQMAP_LEVELS_BUFFER_BPS", "5.0") or 5.0)
        self._cached_liqmap_levels_max_sl_widen_bps = float(os.getenv("LIQMAP_LEVELS_MAX_SL_WIDEN_BPS", "25.0") or 25.0)

        # ------------------------------------------------------------------
        # Trade Profile Router (Phase 1 — shadow, fail-open)
        # Controls: trailing_profile, execution_policy, risk_multiplier, net_edge gate
        # ------------------------------------------------------------------
        self._profile_router = TradeProfileRouter()
        self._profile_router_audit_stream = os.getenv("TRADE_PROFILE_AUDIT_STREAM", "stream:trade_profile_audit")
        self._profile_router_audit_maxlen = int(os.getenv("TRADE_PROFILE_AUDIT_MAXLEN", "50000") or 50000)
        # net_edge gate: min net edge after fees/spread/slippage before order allowed in ENFORCE mode
        self._cached_profile_net_edge_enforce = os.getenv("TRADE_PROFILE_NET_EDGE_ENFORCE", "0").lower() in {"1", "true", "yes", "on"}

    def _record_of_inputs_publish_error(self, *, symbol: str, path: str, stream: str, exc: Exception) -> None:
        with contextlib.suppress(Exception):
            of_inputs_publish_error_total.labels(
                symbol=symbol,
                stream=stream,
                path=path,
            ).inc()
        logger.error(
            "❌ (%s) Failed to publish to %s via %s path: %s",
            symbol,
            stream,
            path,
            exc,
        )

    async def _publish_of_inputs(self, *, publisher: AsyncSignalPublisher, enriched_signal: dict[str, Any], symbol: str, path: str) -> None:
        try:
            await publisher.xadd_json(
                sink=StreamSink(name=self.of_inputs_stream, field="payload", maxlen=self.of_inputs_stream_maxlen),
                payload=enriched_signal,
                symbol=symbol,
            )
        except Exception as exc:
            self._record_of_inputs_publish_error(
                symbol=symbol,
                path=path,
                stream=self.of_inputs_stream,
                exc=exc,
            )
            if self.of_inputs_publish_strict:
                raise

    @property
    def FEES_BPS_RT(self) -> float:
        return self._cached_fees_bps_rt

    def _record_veto(self, symbol: str, scenario: str, reason: str, mode: str = "ENFORCE") -> None:
        """Helper to record veto metrics."""
        with contextlib.suppress(Exception):
            strong_gate_veto_total.labels(
                symbol=symbol,
                scenario=scenario,
                reason=reason,
                mode=mode
            ).inc()

    def _handle_pipeline_veto(
        self,
        dec: GateDecisionV1,
        symbol: str,
        direction: str,
        entry: float,
        sl: float,
        tp_levels: list[float],
        lot: float,
        confidence: float,
        ts_ms: int,
        indicators: dict[str, Any],
        signal: dict[str, Any],
    ) -> None:
        """Unified rejection path for all signal gates."""
        try:
            # 1. Rejected Stream (Triage)
            if self.publisher and self.publisher.r:
                safe_create_task(
                    self.publisher.r.xadd(
                        self._rejected_signal_stream,
                        fields={
                            "symbol": symbol,
                            "gate": dec.gate,
                            "reason": dec.reason_code,
                            "ts_ms": str(ts_ms),
                            "payload": json.dumps(signal, ensure_ascii=False),
                        },
                        maxlen=1000000,
                    ),
                    name=f"reject_{symbol}_{ts_ms}"
                )

            # 2. Audit Payload
            try:
                audit_payload = {
                    "v": 1,
                    "is_virtual": 1,
                    "shadow": bool(indicators.get("of_gate_mode") == "SHADOW"),
                    "virtual": True,
                    "tradeable": False,
                    "sid": signal.get("signal_id") or signal.get("sid") or "",
                    "signal_id": signal.get("signal_id") or "",
                    "symbol": symbol,
                    "side": direction,
                    "entry": entry,
                    "sl": sl,
                    "tp_levels": tp_levels,
                    "lot": lot,
                    "qty": lot,
                    "quantity": lot,
                    "source": "CryptoOrderFlow",
                    "reason": signal.get("reason") or "delta_spike",
                    "confidence": confidence,
                    "confidence01": confidence,
                    "confidence_pct": confidence * 100.0,
                    "atr": indicators.get("atr", 0.0),
                    "ts": ts_ms,
                    "ts_ms": ts_ms,
                    FIELD_TS_EVENT_MS: ts_ms,
                    FIELD_TS_FEATURE_MS: ts_ms,
                    FIELD_TS_EMIT_MS: int(time.time() * 1000),
                    "pre_publish_veto": True,
                    "pre_publish_gate": dec.gate,
                    "pre_publish_reason": dec.reason_code,
                    "indicators": indicators,
                    "strategy": "cryptoorderflow",
                    "tf": "tick",
                }
                preprocess_signal_for_publish(audit_payload, symbol=symbol, source="CryptoOrderFlow", logger=logger)
                safe_create_task(
                    self.publisher.xadd_json(
                        sink=StreamSink(name=self.cryptoorderflow_signal_stream_template.format(symbol=symbol), field="data", maxlen=1000),
                        payload=audit_payload,
                        symbol=symbol,
                    ),
                    name=f"audit_veto_{symbol}_{ts_ms}"
                )
            except Exception:
                pass

            # 3. Virtual Trade (Backtest/ML parity)
            safe_create_task(
                self._push_virtual_to_binance_queue(
                    sid=signal.get("signal_id") or signal.get("sid") or "",
                    symbol=symbol,
                    direction=direction,
                    entry=entry,
                    sl=sl,
                    tp_levels=tp_levels,
                    lot=lot,
                    ts_ms=ts_ms,
                    confidence=confidence,
                    enriched_signal=signal,
                    indicators=indicators,
                    is_rejected_signal=True,
                    rejection_reason=dec.reason_code
                ),
                name=f"virtual_veto_{symbol}_{ts_ms}"
            )
        except Exception as e:
            logger.debug("⚠️ _handle_pipeline_veto error: %s", e)

    @property
    def TP_BPS_BUFFER(self) -> float:
        return self._cached_tp_bps_buffer

    def _conf_scores_enabled(self) -> bool:
        return self.conf_scores_publish_enabled

    def _safe_num(self, v: object) -> float | None:
        try:
            f = float(v)  # type: ignore[arg-type]
        except Exception:
            return None
        if not math.isfinite(f):
            return None
        return f

    def _build_conf_evidence_map(self, *, confirmations: list, indicators: dict) -> dict[str, float]:
        """Best-effort numeric evidence_map for `signals:confidence:scores` contract.

        Rules:
        - only numeric values (floats)
        - boolean flags -> 0.0/1.0
        - alias mapping for legacy keys
        """
        out: dict[str, float] = {}

        # Parse confirmations list like ["rsi_agree=1", "div_strength=0.7", ...]
        for item in confirmations or []:
            try:
                s = str(item)
                if "=" not in s:
                    continue
                k, v = s.split("=", 1)
                k = k.strip()
                v = v.strip()
                if not k:
                    continue
                fv = self._safe_num(v)
                if fv is None:
                    continue
                out[k] = fv
            except Exception:
                continue

        # Allow-list a few high-value numeric evidence keys from indicators
        allow = {
            "rsi_agree",
            "div_match",
            "div_strength",
            "sweep",
            "sweep_eqh",
            "sweep_eql",
            "iceberg_strict",
            "ice_strict",
            "reclaim",
            "obi_stable",
            "data_health",
            "spread_bps",
            "book_stale_ms",
        }
        for k in list(allow):
            if k in indicators:
                fv = self._safe_num(indicators.get(k))
                if fv is not None:
                    out[k] = fv

        # market_mode is often a string; encode trend/range as numeric for the evidence_map
        mm = indicators.get("market_mode") or indicators.get("regime")
        if isinstance(mm, str):
            mml = mm.strip().lower()
            if mml in {"trend", "momentum", "breakout"}:
                out.setdefault("market_mode", 1.0)
            else:
                from common.market_mode import is_range_regime
                if is_range_regime(mml):
                    out.setdefault("market_mode", 0.0)

        # Aliases / backward-compat
        if "ice_strict" in out and "iceberg_strict" not in out:
            out["iceberg_strict"] = out["ice_strict"]
        if "sweep" in out and ("sweep_eqh" not in out and "sweep_eql" not in out and "sweep_any" not in out):
            out["sweep_any"] = out["sweep"]

        # Drop legacy alias keys from canonical map
        out.pop("ice_strict", None)
        return out

    def _get_cv_profile(self, r: redis.Redis | None) -> str:
        now_ms = get_ny_time_millis()
        if now_ms - self._cv_profile_cache_ts_ms < 10000:
            return self._cv_profile_cache_val

        try:
            if r:
                raw = r.get("cfg:crypto_of:crossvenue_ctx_profile")
                if raw:
                    self._cv_profile_cache_val = str(raw).strip().lower()
                    self._cv_profile_cache_ts_ms = now_ms
                    return self._cv_profile_cache_val
        except Exception:
            pass

        self._cv_profile_cache_val = self._cached_crossvenue_ctx_profile
        self._cv_profile_cache_ts_ms = now_ms
        return self._cv_profile_cache_val

    def _extract_conf_scores(self, *, signal: dict, indicators: dict) -> tuple[float, float | None]:
        """Return (raw, final)."""
        raw = (
            indicators.get("confidence_raw")
            or indicators.get("confidence_v1")
            or signal.get("confidence_raw")
            or signal.get("confidence")
            or 0.0
        )
        raw_f = self._safe_num(raw) or 0.0

        final = (
            indicators.get("confidence_cal")
            or indicators.get("confidence_final")
            or signal.get("confidence_final")
            or signal.get("confidence")
        )
        final_f = self._safe_num(final) if final is not None else None
        return raw_f, final_f if final_f is not None else None

    async def _maybe_publish_confidence_scores(
        self,
        *,
        symbol: str,
        sid: str,
        ts_event_ms: int,
        signal: dict,
        confirmations: list,
        indicators: dict,
        evidence_dict: dict,
    ) -> None:
        if not self._conf_scores_enabled():
            return

        try:
            evidence_map = self._build_conf_evidence_map(confirmations=confirmations, indicators=indicators)
            raw, final = self._extract_conf_scores(signal=signal, indicators=indicators)

            evt = {
                "schema_version": self.conf_scores_schema_version,
                "producer": self._cached_service_name,
                "sid": sid,
                "symbol": symbol,
                "ts_event_ms": ts_event_ms,
                "confidence_raw": raw,
                "confidence_final": final if final is not None else None,
                "evidence_map": evidence_map,
            }
            if self.conf_scores_include_evidence_json:
                # Full evidence (heavy) - keep disabled unless needed.
                evt["evidence_json"] = evidence_dict

            await self.publisher.xadd_json(
                sink=StreamSink(name=self.conf_scores_stream, field="payload", maxlen=self.conf_scores_stream_maxlen),
                payload=evt,
                symbol=symbol,
            )

        except Exception as e:
            # Best-effort quarantine - never block signal publishing.
            try:
                q = {
                    "ts_event_ms": ts_event_ms,
                    "sid": sid,
                    "symbol": symbol,
                    "error": str(e),
                }
                await self.publisher.xadd_json(
                    sink=StreamSink(name=self.conf_scores_quarantine_stream, field="payload", maxlen=self.conf_scores_quarantine_maxlen),
                    payload=q,
                    symbol=symbol,
                )
            except Exception:
                pass



    def _build_gate_ctx(self, runtime: SymbolRuntime, signal: dict[str, Any], sig_ts_ms: int) -> SimpleNamespace:
        """Build a minimal ctx object compatible with pre_publish_gates.* (fail-open)."""
        micro_val = signal.get("micro")
        micro = micro_val if isinstance(micro_val, dict) else {}
        ind_val = signal.get("indicators")
        indicators = ind_val if isinstance(ind_val, dict) else {}

        # data-quality flags (prepared by preprocess_signal_for_publish + additional cheap hints)
        flags = []
        dq_flags_val = signal.get("data_quality_flags")
        if isinstance(dq_flags_val, list):
            flags.extend([str(x) for x in dq_flags_val if x is not None])

        # Additional hints available at publish time
        try:
            book_stale_ms = int(micro.get("book_stale_ms") or 0)
            dq_book_stale_flag_ms = self._cached_dq_book_stale_flag_ms
            if book_stale_ms > dq_book_stale_flag_ms:
                flags.append("stale_l2")
        except Exception:
            pass

        try:
            spread_bps = float(micro.get("spread_bps") or 0.0)
            dq_spread_wide_flag_bps = self._cached_dq_spread_wide_flag_bps
            if spread_bps > dq_spread_wide_flag_bps:
                flags.append("wide_spread")
        except Exception:
            spread_bps = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)

        # Normalize + dedup
        seen = set()
        dq_flags = []
        for x in flags:
            s = (x or "").strip().lower()
            if not s or s in seen:
                continue
            seen.add(s)
            dq_flags.append(s)

        # Surface P5 integrity fields to indicators (P5 DQ-First support)
        def _si_ema(tracker) -> float:
            try:
                gap = getattr(tracker, "gap_ema", None)
                return float(gap.ema or 0.0) if gap else 0.0
            except Exception: return 0.0
        def _si_gmax(tracker) -> int:
            try: return int(getattr(tracker, "gap_max_window", 0) or 0)
            except Exception: return 0
        def _si_schema(tracker) -> int:
            try: return int(getattr(tracker, "schema_changed_last", 0) or 0)
            except Exception: return 0

        ti = getattr(runtime, "tick_integrity", None)
        bi = getattr(runtime, "book_integrity", None)
        indicators.setdefault("tick_seq_gap_rate_ema", _si_ema(ti))
        indicators.setdefault("tick_seq_max_gap_window", _si_gmax(ti))
        indicators.setdefault("tick_schema_changed", _si_schema(ti))
        indicators.setdefault("book_seq_gap_rate_ema", _si_ema(bi))
        indicators.setdefault("book_seq_max_gap_window", _si_gmax(bi))

        # Book sanity flags
        bsf = micro.get("book_sanity_flags") or indicators.get("book_sanity_flags")
        if bsf:
            indicators.setdefault("book_sanity_flags", bsf)

        # Minimal OF-like object: expose depth_bid_5/ask_5 if runtime has them
        of = SimpleNamespace(
            depth_bid_5=float(getattr(runtime, "last_depth_bid_5", 0.0) or 0.0),
            depth_ask_5=float(getattr(runtime, "last_depth_ask_5", 0.0) or 0.0),
            atr_ts_ms=int(indicators.get("atr_ts_ms") or signal.get("atr_ts_ms") or 0),
            regime=str(indicators.get("regime") or signal.get("regime") or "unknown"),
            spread_bps=float(micro.get("spread_bps") or 0.0),
        )

        # Main ctx expected by gates
        ctx = SimpleNamespace(
            symbol=str(getattr(runtime, "symbol", "") or ""),
            ts_event_ms=sig_ts_ms,
            ts_ms=sig_ts_ms,
            ts=sig_ts_ms,
            spread_bps=float(micro.get("spread_bps") or getattr(runtime, "last_spread_bps", 0.0) or 0.0),
            regime=str(indicators.get("regime") or signal.get("regime") or "unknown"),
            session=str(signal.get("session") or indicators.get("session") or "na"),
            tf=str(signal.get("tf") or indicators.get("tf") or "na"),
            venue=str(signal.get("venue") or indicators.get("venue") or "binance"),
            touch_is_stale=bool(signal.get("touch_is_stale") or indicators.get("touch_is_stale") or False),
            data_quality_flags=dq_flags,
            indicators=indicators,
            of=of,
            redis=getattr(runtime, "redis_client", None),
        )
        return ctx

    async def publish_signal(self, runtime: SymbolRuntime, signal: dict[str, Any]) -> None:
        """
        Публикация сигнала в необходимые каналы.
        """
        symbol = runtime.symbol
        side_norm = normalize_side_3_safe(signal.get("direction") or signal.get("side") or "")
        if side_norm is None:
            logger.warning("⚠️ (%s) publish_signal: unknown direction=%r side=%r (skip)",
                           symbol, signal.get("direction"), signal.get("side"))
            return
        direction = side_norm.direction
        cfg = runtime.config

        use_outbox = self._cached_use_outbox
        shadow_outbox = self._cached_shadow_outbox
        outbox_stream = self._cached_outbox_stream
        gate_mode = self._cached_gate_mode

        passed = True
        reason = "ok"
        gate_meta: dict[str, Any] = {}

        # ------------------------------------------------------------------
        # DQ-FIRST: Early integrity check via Orchestrator
        # ------------------------------------------------------------------
        # We attribute signal by sig_ts as early as possible.
        sig_ts = int(signal.get("tick_ts") or signal.get("ts_ms") or getattr(runtime, "last_ts_ms", 0) or get_ny_time_millis())
        
        # Monitor Skew
        local_now = get_ny_time_millis()
        skew_ms = local_now - sig_ts
        if skew_ms < -3000 or skew_ms > 15000:
             logger.warning(f"🚨 [TIME_SYNC] Large Clock Skew detected for {symbol}: local={local_now}, signal_ts={sig_ts}, skew={skew_ms}ms")

        with contextlib.suppress(Exception):
            of_session_outcome_total.labels(symbol, session_utc(sig_ts), "emit").inc()

        # FIX BUG-4: ensure indicators is the live signal dict reference BEFORE
        # kind resolution — avoids split-reference between _build_gate_ctx copy and signal["indicators"].
        indicators = signal.setdefault("indicators", {})

        # FIX #4: kind must come from entry_tag (=primary_reason: weak_progress/breakout/absorption etc.)
        kind = str(
            indicators.get("kind")
            or signal.get("kind")
            or signal.get("entry_tag")   # canonical kind for CryptoOrderFlow
            or ""
        ).strip().lower()

        ctx = self._build_gate_ctx(runtime, signal, sig_ts)
        # indicators already bound above — do not re-bind here

        # Sentinel values for levels — populated after Stage-1/2 gates pass.
        # Required so _apply_decision closure can reference them safely even
        # when a DENY fires before _calculate_levels() is called.
        # NOTE: Using a mutable dict container to avoid CPython 3.12+ NameError
        # with closure capture of type-annotated local variables.
        _levels = {"entry": 0.0, "sl": 0.0, "tp_levels": [], "lot": 0.0}
        confidence: float = float(signal.get("confidence", 0.0) or 0.0)

        def _apply_decision(dec: Any) -> bool:
            """Standardized decision handling for orchestrated gates."""
            # Only increment veto metric if decision is not ALLOW
            if _PRE_PUBLISH_VETO_TOTAL is not None and getattr(dec, "decision", "") != "ALLOW":
                with contextlib.suppress(Exception):
                    _PRE_PUBLISH_VETO_TOTAL.labels(
                        gate=dec.gate,
                        reason_code=getattr(dec, "reason_code", "UNKNOWN"),
                        symbol=symbol,
                        kind=kind
                    ).inc()
            signal.setdefault("gate_decisions", []).append(dec.to_dict())
            if dec.decision == "DENY":
                logger.info("🛡️ [%s] VETO (%s): %s | reason=%s notes=%s", 
                            dec.stage.upper(), symbol, dec.gate, dec.reason_code, dec.notes)
                self._record_veto(symbol, dec.stage, dec.reason_code)
                
                # Unified rejection path
                self._handle_pipeline_veto(
                    dec=dec,
                    symbol=symbol,
                    direction=direction,
                    entry=_levels["entry"],
                    sl=_levels["sl"],
                    tp_levels=_levels["tp_levels"],
                    lot=_levels["lot"],
                    confidence=confidence,
                    ts_ms=sig_ts,
                    indicators=indicators,
                    signal=signal
                )
                return True
            if dec.decision == "TIGHTEN":
                tadd = float(dec.notes.get("tighten_add_bps", 0.0))
                if tadd > 0:
                    exp0 = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                    indicators["expected_slippage_bps"] = exp0 + tadd
                    logger.info("⚡ [%s] TIGHTEN (%s): +%.2f bps | reason=%s", 
                                dec.stage.upper(), symbol, tadd, dec.reason_code)
            return False

        # ------------------------------------------------------------------
        # STAGE 1: DQ-FIRST INTEGRITY (Hard Stop)
        # ------------------------------------------------------------------
        if _apply_decision(self.orchestrator.check_dq_integrity(ctx, kind)): return  # type: ignore

        # 2. Liquidity Integrity (Spread, Staleness)
        if _apply_decision(self.orchestrator.check_liquidity_integrity(ctx)): return  # type: ignore
        
        # 3. Quality / Floor
        if _apply_decision(self.orchestrator.check_quality(ctx, kind)): return  # type: ignore
        if _apply_decision(self.orchestrator.check_atr_floor(ctx, kind)): return  # type: ignore

        # ------------------------------------------------------------------
        # STAGE 2: CONTEXT & MARKET GATES (Fail-Open / Tighten / Veto)
        # ------------------------------------------------------------------
        
        # Async Context Gates
        if _apply_decision(self.orchestrator.check_breadth(ctx, kind, direction)): return  # type: ignore
        
        if self._cached_deriv_ctx_enabled:
            if _apply_decision(await self.orchestrator.check_derivatives_context(  # type: ignore
                ctx, kind, direction,
                profile=self._cached_deriv_ctx_profile,
                thr_funding_z=self._cached_deriv_ctx_funding_z,
                thr_basis_bps=self._cached_deriv_ctx_basis_bps,
                require_oi_for_veto=self._cached_deriv_ctx_require_oi,
                tighten_mult=self._cached_deriv_ctx_tighten_mult,
                tighten_cap_bps=self._cached_deriv_ctx_tighten_cap,
            )): return

        if self._cached_defillama_ctx_enabled:
            if _apply_decision(await self.orchestrator.check_defillama_context(  # type: ignore
                ctx, kind, side=direction,
                profile=self._cached_defillama_ctx_profile,
                tighten_mult=self._cached_defillama_ctx_tighten_mult,
                tighten_cap_bps=self._cached_defillama_ctx_tighten_cap,
                max_age_ms=self._cached_defillama_ctx_max_age_ms,
            )): return

        if self._cached_sentiment_ctx_enabled:
            if _apply_decision(await self.orchestrator.check_sentiment_context(  # type: ignore
                ctx, kind, direction,
                profile=self._cached_sentiment_ctx_profile,
                max_age_ms=self._cached_sentiment_ctx_max_age_ms,
                tighten_cap_bps=self._cached_sentiment_ctx_tighten_cap,
            )): return

        if self._cached_crossvenue_ctx_enabled:
            if _apply_decision(await self.orchestrator.check_crossvenue_context(  # type: ignore
                ctx, kind, direction,
                profile=self._cached_crossvenue_ctx_profile,
                max_age_ms=self._cached_crossvenue_ctx_max_age_ms,
                min_agree=self._cached_crossvenue_ctx_min_agree,
                max_dislocation_z=self._cached_crossvenue_ctx_max_dislocation_z,
                max_mid_spread_bps=self._cached_crossvenue_ctx_max_mid_spread_bps,
                max_stale_count=self._cached_crossvenue_ctx_max_stale_count,
                tighten_mult=self._cached_crossvenue_ctx_tighten_mult,
                tighten_cap_bps=self._cached_crossvenue_ctx_tighten_cap,
            )): return

        if self._cached_exec_health_auto_freeze_enabled:
            if _apply_decision(await self.orchestrator.check_exec_health_gate(ctx, kind)): return  # type: ignore

        if _apply_decision(self.orchestrator.check_smt(ctx, kind, direction)): return  # type: ignore

        # Sync Risk Gates
        if self._cached_liq_geom_enabled:
            if _apply_decision(self.orchestrator.check_liquidity_geometry(  # type: ignore
                ctx, kind,
                profile=self._cached_liq_geom_profile,
                thr_slope=self._cached_liq_min_book_slope,
                thr_dws=self._cached_liq_max_dws_bps,
                thr_recovery_ms=self._cached_liq_max_recovery_ms,
                tighten_cap_bps=self._cached_liq_tighten_cap,
                tighten_mult=self._cached_liq_tighten_mult,
            )): return

        if self._cached_flow_tox_enabled:
            if _apply_decision(self.orchestrator.check_flow_toxicity(  # type: ignore
                ctx, kind,
                profile=self._cached_flow_tox_profile,
                thr_z=self._cached_flow_thr_z,
                thr_vpin=self._cached_flow_thr_vpin,
                thr_is=self._cached_flow_thr_is,
                thr_imp=self._cached_flow_thr_imp,
                tighten_mult=self._cached_flow_mult,
                tighten_cap_bps=self._cached_flow_cap,
                veto_without_tca=self._cached_flow_veto_wo_tca,
            )): return

        if self._cached_manip_enabled:
            if _apply_decision(self.orchestrator.check_manipulation_gate(  # type: ignore
                ctx, kind,
                profile=self._cached_manip_profile,
                thr_qs=self._cached_manip_thr_qs,
                thr_lay=self._cached_manip_thr_lay,
                thr_otr_z=self._cached_manip_thr_otr_z,
                tighten_mult=self._cached_manip_tighten_mult,
                tighten_cap_bps=self._cached_manip_tighten_cap,
            )): return

        if _apply_decision(self.orchestrator.consistency_once(ctx=ctx, symbol=symbol, kind=kind, side=direction)): return  # type: ignore
        if _apply_decision(self.orchestrator.edge_cost_cached(ctx=ctx, kind=kind, symbol=symbol, side=direction)): return  # type: ignore

        # ------------------------------------------------------------------
        # Pipeline: Continue with levels and enrichment
        # ------------------------------------------------------------------
        signals_total.labels(symbol=symbol, handler="crypto_orderflow").inc()

        try:
            entry = float(signal["entry"])
        except Exception:
            logger.warning("⚠️ (%s) publish_signal: invalid entry=%r (skip)", symbol, signal.get("entry"))
            return
        confirmations = signal.get("confirmations", [])
        indicators = signal.get("indicators") or {}  # sync to ensure mutations from gate ctx are reflected
        # Extract delta values from indicators (where they're actually stored)
        delta = float(indicators.get("delta", 0.0))
        delta_z = float(indicators.get("delta_z", 0.0))
        # Ensure they're also available at top level for backward compatibility
        signal.setdefault("delta", delta)
        signal.setdefault("delta_z", delta_z)
        indicators.setdefault("tick_qty", signal.get("tick_qty"))

        # --- CONTRACT VALIDATION (SignalV1) ---
        try:
            # Signal ID generation (P0)
            signal_id = generate_signal_id(
                kind=(signal.get("kind") or "of"),
                symbol=symbol,
                ts_ms=sig_ts,
                direction=side_norm.direction,
            )

            # ✅ FIX (2026-04-28): Preserve original confidence from strategy.
            # SignalV1.confidence defaults to 0.0.  Without passing the real value,
            # model_dump() exports confidence=0.0, and signal.update() overwrites
            # the real confidence computed by _compute_confidence() in strategy.py.
            # This caused 100% trade suppression via Confidence VETO (0.00 < 0.70).
            _original_confidence = float(signal.get("confidence", 0.0) or 0.0)

            sig_v1 = SignalV1(
                signal_id=signal_id,
                symbol=symbol,
                venue=(signal.get("venue") or "binance_usdm"),
                ts_event_ms=sig_ts,
                ts_publish_ms=get_ny_time_millis(),
                direction=side_norm.direction,
                side=side_norm.side,
                side_int=side_norm.side_int,
                entry_price=entry,
                sl_price=float(signal.get("sl") or 0.0),
                tp_levels=signal.get("tp_levels") or [],
                confidence=_original_confidence,
                ok=int(indicators.get("of_confirm_ok", 0) or 0),
                ok_soft=int(indicators.get("ok_soft", 0) or 0),
                reason=(signal.get("reason", reason or "delta_spike")),
                scenario=(indicators.get("scenario", "")),
                indicators=indicators,
                meta=signal.get("metadata") or {}
            )
            # Standardize signal dict from v1
            # ✅ FIX: Use selective update to prevent SignalV1 defaults from
            # overwriting critical fields that were already set by strategy.
            _v1_dump = sig_v1.model_dump()
            # Fields that must NEVER be overwritten with SignalV1 defaults
            _protected_keys = {"confidence"}
            for _k, _v in _v1_dump.items():
                if _k in _protected_keys and _k in signal:
                    continue  # keep original value from strategy
                signal[_k] = _v
        except Exception as e:
            logger.warning("⚠️ (%s) SignalV1 validation failed: %s", symbol, e)

        # ------------------------------------------------------------------
        # P1 Inline Execution Health hardening
        # ------------------------------------------------------------------
        # We freeze a few explicit aliases at EMIT-time so warm-path workers do
        # not need to guess which field carried the decision anchor.
        # These aliases are intentionally redundant and backward-compatible.
        sid0 = str(signal.get("signal_id") or signal.get("sid") or "").strip()
        if sid0:
            signal.setdefault("signal_id", sid0)
            signal.setdefault("sid", sid0)
        signal.setdefault("ts_emit_ms", sig_ts)
        signal.setdefault(
            "decision_mid_at_emit",
            signal.get("decision_mid")
            or signal.get("decision_price")
            or indicators.get("decision_mid")
            or indicators.get("mid")
            or signal.get("entry"),
        )
        signal.setdefault(
            "expected_slippage_bps_at_emit",
            signal.get("decision_expected_slippage_bps")
            or indicators.get("expected_slippage_bps")
            or signal.get("expected_slippage_bps"),
        )

        # A1 Decision Context Enrichment (hot-path, fail-open)
        #
        # We freeze a minimal "decision snapshot" inside ctx at publish time.
        # This is required for later post-trade TCA joins:
        #   decision_snapshot (sid, decision_ts_ms, decision_mid/bid/ask, ...)  +  fills
        #
        # Important invariants:
        # - decision_ts_ms MUST be epoch-ms and should be deterministic (prefer tick_ts/ts_emit_ms).
        # - Never use time.time() inside the enrichment unless there is no other choice.
        try:
            from services.orderflow.decision_ctx_fields import ensure_decision_ctx_fields
            ensure_decision_ctx_fields(signal, indicators=indicators, runtime=runtime, now_ms=sig_ts)
        except Exception as e:
            logger.warning("⚠️ (%s) Failed to enrich A1 decision ctx fields: %s", symbol, e)




        # P5 gates (Book Sanity, Stream Integrity) are now handled by DQ-FIRST orchestrator at the start of the pipeline.
        # This section remains for signal enrichment only.
        pass

        # Log Strong Gate outcome if present (sample every 10000th message)
        if indicators.get("strong_gate_scn"):
             strong_gate_sampler = LogSamplerFactory.get_sampler("STRONG_GATE", 10000)
             if strong_gate_sampler.should_log(f"strong_gate_{symbol}"):
                 logger.info(
                     "🛡️ [GATE] Strong Gate: scn=%s have=%s need=%s",
                     indicators.get("strong_gate_scn"),
                     indicators.get("strong_gate_have"),
                     indicators.get("strong_gate_need"),
                 )

        # ------------------------------------------------------------------
        # TRADE PROFILE ROUTER (Phase 1 — shadow + net_edge gate)
        # Fail-open: any exception → continue with defaults.
        # Writes ProfileDecision into signal["meta"]["trade_profile"] for audit.
        # In SHADOW mode: does NOT block, just logs.
        # In ENFORCE mode: checks net_edge_bps gate; DENY → veto + return.
        # ------------------------------------------------------------------
        _profile_decision = None
        _profile_regime_bucket = "mixed"
        try:
            _raw_regime = str(
                indicators.get("regime")
                or signal.get("regime")
                or getattr(runtime, "last_regime", "")
                or "na"
            ).strip().lower()
            _profile_regime_bucket = _regime_group(_raw_regime)
            # Bear trend gets its own profile bucket so the router picks bear_trend_follow_v1
            if "trending_bear" in _raw_regime:
                _profile_regime_bucket = "trending_bear"

            _profile_decision = self._profile_router.route(
                symbol=symbol,
                regime_bucket=_profile_regime_bucket,
                kind=kind,
            )

            # --- Phase 2: execution_policy binding ---
            _exec_policy_from_profile = _profile_decision.profile.execution_policy
            indicators.setdefault("execution_policy_profile", _exec_policy_from_profile)

            # --- net_edge gate (fail-open unless ENFORCE enabled) ---
            if self._cached_profile_net_edge_enforce and _profile_decision.mode == "LIVE":
                _spread_bps = float(indicators.get("spread_bps", 0.0) or 0.0)
                _slip_bps = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                _fee_bps = self._cached_fees_bps_rt
                _ev_bps = float(indicators.get("expected_edge_bps", 0.0) or 0.0)
                _net_edge = _ev_bps - _fee_bps - _spread_bps / 2.0 - _slip_bps
                _min_net_edge = _profile_decision.profile.min_net_edge_bps
                indicators["profile_net_edge_bps"] = round(_net_edge, 2)
                indicators["profile_min_net_edge_bps"] = _min_net_edge
                if _net_edge < _min_net_edge:
                    _ts_dec_ms = int(time.time() * 1000)
                    _ne_dec = GateDecisionV1(
                        stage="profile",
                        gate="trade_profile_net_edge",
                        decision="DENY",
                        reason_code="profile_negative_net_edge",
                        severity="WARN",
                        profile=_profile_decision.profile.name,
                        fail_policy="OPEN",
                        ts_event_ms=sig_ts,
                        ts_decision_ms=_ts_dec_ms,
                        latency_us=0,
                        inputs_hash="",
                        notes={"net_edge_bps": _net_edge, "min_net_edge_bps": _min_net_edge,
                               "profile": _profile_decision.profile.name},
                    )
                    if _apply_decision(_ne_dec):
                        return

            # Write profile meta into signal for downstream consumers
            _sym_tier = str(indicators.get("symbol_tier", "B") or "B").upper()
            # Map tier → symbol_class for per-class stop/zone parameters
            _sym_class = {"A": "majors", "B": "alts", "C": "memes"}.get(_sym_tier, "alts")
            _profile_meta = build_signal_profile_meta(
                _profile_decision,
                symbol_tier=_sym_tier,
                symbol_class=_sym_class,
                realized_vol_bps=float(indicators.get("realized_vol_bps", 0.0) or 0.0),
                target_vol_bps=float(indicators.get("target_vol_bps", 0.0) or 0.0),
            )
            enriched_signal_meta = signal.setdefault("meta", {})
            enriched_signal_meta.update(_profile_meta)

            # Inject profile-computed per-class params into indicators
            # so that _calculate_levels() and gates can use them.
            indicators["profile_stop_atr_mult"] = _profile_meta.get("stop_atr_mult")
            indicators["profile_max_zone_bp"] = _profile_meta.get("max_zone_bp")
            indicators["profile_tp_rr"] = _profile_meta.get("tp_rr")
            indicators["profile_tp1_atr_mult"] = _profile_meta.get("tp1_atr_mult")

            # Audit to Redis stream (fail-open background task)
            if self.publisher and self.publisher.r:
                _audit_payload = {
                    "ts_ms": sig_ts,
                    "signal_id": str(signal.get("signal_id") or ""),
                    "symbol": symbol,
                    "regime": _raw_regime,
                    "regime_bucket": _profile_regime_bucket,
                    "kind": kind,
                    "side": str(direction),
                    "profile": _profile_decision.profile.name,
                    "decision": "ALLOW" if _profile_decision.allowed else "DENY",
                    "reason_code": _profile_decision.reason_code,
                    "profile_mode": _profile_decision.mode,
                    "is_canary": int(_profile_decision.is_canary),
                    "risk_multiplier": _profile_meta.get("risk_multiplier", 1.0),
                    "execution_policy": _profile_meta.get("execution_policy", "SAFETY_FIRST"),
                    "net_edge_bps": float(indicators.get("profile_net_edge_bps", 0.0) or 0.0),
                }
                safe_create_task(
                    self.publisher.r.xadd(
                        self._profile_router_audit_stream,
                        {"payload": json.dumps(_audit_payload, ensure_ascii=False)},
                        maxlen=self._profile_router_audit_maxlen,
                        approximate=True,
                    ),
                    name=f"tpr_audit_{symbol}_{sig_ts}"
                )
        except Exception as _tpr_err:
            logger.debug("⚠️ [TPR] trade_profile_router error (fail-open): %s", _tpr_err)

        # ---- trail_profile ----
        # cfg already initialized
        trail_profile = signal.get("trail_profile") or cfg.get("trail_profile") or "protective_only"

        # ---- ts normalization (epoch ms best-effort) ----
        # We keep multiple mirrors because older downstream components differ:
        #   - some read `ts`, others `timestamp`, newer contract expects `ts_ms`.
        def _ts_ms() -> int:
            v = signal.get("tick_ts") or signal.get("generated_at") or signal.get("ts_ms") or signal.get("ts")
            try:
                iv = int(float(v or 0))
                return iv if iv > 0 else get_ny_time_millis()
            except Exception:
                return get_ny_time_millis()

        ts_ms = _ts_ms()

        # Calculate actual ATR that will be used (including fallbacks)
        sl, tp_levels, lot, atr, atr_meta = self._calculate_levels(runtime, entry, direction, indicators, trail_profile=trail_profile)
        # Sync _levels container for any downstream _apply_decision calls (e.g. squeeze gate)
        _levels["entry"] = entry
        _levels["sl"] = sl
        _levels["tp_levels"] = tp_levels
        _levels["lot"] = lot

        # ---- DYNAMIC TRAILING CALLBACK ----
        trail_callback_pct = None
        trail_calib_source = "default"
        if atr and entry and atr > 0 and entry > 0:
            try:
                # Priority 1: Trail Calibrator enforced params (per-symbol, from trail:calib:{sym}:{regime})
                _tcp = getattr(runtime, "trail_calib_params", None)
                if _tcp and _tcp.get("mode") == "enforce":
                    calib_cb_mult = float(_tcp["callback_atr_mult"])
                    trail_calib_source = "trail_calib_enforce"
                else:
                    # Priority 2: Explicit calibrator overrides from symbol_specs
                    trail_calib = getattr(runtime, "calibrated_specs", {}).get("trailing", {})
                    calib_cb_mult = float(trail_calib.get("trail_atr_mult") or self._cached_binance_trail_atr_mult)
                    trail_calib_source = "symbol_specs" if trail_calib.get("trail_atr_mult") else "env_default"
                cb_pct_raw = (atr * calib_cb_mult / entry) * 100.0
                trail_callback_pct = round(cb_pct_raw, 2)
                indicators["trail_calib_source"] = trail_calib_source
                indicators["trail_calib_cb_mult"] = calib_cb_mult
            except Exception:
                pass

        # ---- OVERRIDE FOR RANGE REGIME ----
        # In range: 2 TPs only, recomputed via RANGE_TP_RR env (default "1.0,1.5").
        # TP levels are RECOMPUTED from stop_dist (not just truncated) to achieve correct R:R.
        # ENV: RANGE_TP_RR — comma-separated RR multipliers for range (e.g. "1.0,1.5")
        # Result: TP1 = 1.0×SL_dist, TP2 = 1.5×SL_dist.
        rg_for_overrides = str(getattr(runtime, "last_regime", "na") or "na").lower()
        if rg_for_overrides == "na":
             rg_for_overrides = (indicators.get("regime", "na") or "na").lower()

        is_range_regime_flag = ("range" in rg_for_overrides)
        is_expansion_regime_flag = ("expansion" in rg_for_overrides)
        is_trending_bear_flag = ("trending_bear" in rg_for_overrides)
        is_trending_regime_flag = ("trend" in rg_for_overrides) and not is_trending_bear_flag and not is_expansion_regime_flag
        is_squeeze_regime_flag = ("squeeze" in rg_for_overrides)

        if is_range_regime_flag:
            try:
                _range_rr_str = self._cached_range_tp_rr
                _range_rr = [float(x.strip()) for x in _range_rr_str.split(",") if x.strip()][:2]
                _stop_dist = abs(entry - sl)
                if _stop_dist > 0 and len(_range_rr) >= 1:
                    if getattr(direction, 'value', (direction or '')).upper() == "LONG":
                        tp_levels = [entry + _stop_dist * r for r in _range_rr]
                    else:
                        tp_levels = [entry - _stop_dist * r for r in _range_rr]
                    indicators["range_tp_rr_applied"] = _range_rr_str
                    indicators["range_stop_dist_atr"] = round(_stop_dist / atr, 3) if atr > 0 else 0.0

                    # ⚠️ FIX (2026-04-25): Enforce minimum TP distance in bps to guarantee
                    # profitability after exchange fees. Without this, BTC range TPs were
                    # only 1.8 bps (< 8 bps fee floor) → systematic losses.
                    _tp_bps_floor = self._cached_fees_bps_rt + self._cached_tp_bps_buffer
                    _min_tp_dist = entry * _tp_bps_floor / 10_000.0
                    _tp_expanded = False
                    for i, tp in enumerate(tp_levels):
                        _tp_dist = abs(tp - entry)
                        if _tp_dist < _min_tp_dist * (i + 1):
                            if getattr(direction, 'value', (direction or '')).upper() == "LONG":
                                tp_levels[i] = entry + _min_tp_dist * (i + 1)
                            else:
                                tp_levels[i] = entry - _min_tp_dist * (i + 1)
                            _tp_expanded = True
                    if _tp_expanded:
                        indicators["range_tp_bps_floor_expanded"] = 1
                        indicators["range_tp_bps_floor"] = round(_tp_bps_floor, 1)
                else:
                    # fallback: truncate only
                    if len(tp_levels) > 2:
                        tp_levels = tp_levels[:2]
            except Exception:
                # safe fallback
                if len(tp_levels) > 2:
                    tp_levels = tp_levels[:2]
            trail_profile = "range_protective"
            signal["trail_after_tp1"] = True

        elif is_expansion_regime_flag:
            trail_profile = "expansion_v1"
            signal["trail_after_tp1"] = True

        elif is_trending_bear_flag:
            # ── Bear Trend Quality Gate ──────────────────────────────────────
            # Цель: rocket_v1_bear только для качественных SHORT-входов.
            # Всё остальное (лонги, слабый OF, плохая ликвидность) → protective_only.
            _dir_str = str(getattr(direction, "value", direction) or "").upper()
            _of_ok = int(indicators.get("of_confirm_ok", 0) or 0)
            _of_score = float(indicators.get("of_confirm_score", 0.0) or 0.0)
            _exec_risk = float(indicators.get("exec_risk_norm", 1.0) or 1.0)
            _spread = float(indicators.get("spread_bps", 999.0) or 999.0)
            _book_age = float(indicators.get("obi_age_ms", 99999.0) or 99999.0)
            _delta_z = float(indicators.get("delta_z", 0.0) or 0.0)
            _obi = float(indicators.get("obi", 0.0) or 0.0)
            _sweep_age = float(indicators.get("sweep_age_ms", -1.0) or -1.0)
            _reclaim_age = float(indicators.get("reclaim_age_ms", -1.0) or -1.0)
            _cancel_bid = float(indicators.get("cancel_bid_rate_ema", 0.0) or 0.0)
            _cancel_ask = float(indicators.get("cancel_ask_rate_ema", 0.0) or 0.0)

            # p_edge из of_confirm.evidence.ml
            _p_edge = 0.0
            try:
                _oc_raw = indicators.get("of_confirm") or {}
                _oc = json.loads(_oc_raw) if isinstance(_oc_raw, str) else _oc_raw
                _ev = _oc.get("evidence") or {} if isinstance(_oc, dict) else {}
                _ml_raw = _ev.get("ml") or {} if isinstance(_ev, dict) else {}
                _ml = json.loads(_ml_raw) if isinstance(_ml_raw, str) else _ml_raw
                _p_edge = float(_ml.get("p_edge", 0.0) or 0.0) if isinstance(_ml, dict) else 0.0
            except Exception:
                pass

            _max_spread = float(runtime.config.get(
                "bear_trend_max_spread_bps", os.getenv("BEAR_TREND_MAX_SPREAD_BPS", "20.0")
            ))
            _max_book_age = float(runtime.config.get(
                "bear_trend_max_book_age_ms", os.getenv("BEAR_TREND_MAX_BOOK_AGE_MS", "3000.0")
            ))

            # Недавний sweep или reclaim (≤ 10 s) подтверждает давление продавцов
            _sweep_reclaim_ok = (0.0 <= _sweep_age <= 10000.0) or (0.0 <= _reclaim_age <= 10000.0)

            # Cancel spike: bid-отмены >> ask-отмены → признак манипуляции
            _cancel_spike_veto = _cancel_bid > (_cancel_ask * 3.0) and _cancel_bid > 5000.0

            bear_trend_quality_ok = (
                _dir_str in ("SELL", "SHORT")
                and kind in ("breakout", "continuation", "extreme", "obi_spike")
                and _of_ok == 1
                and _of_score >= 0.60
                and _p_edge >= 0.58
                and _exec_risk <= 0.45
                and _spread <= _max_spread
                and _book_age <= _max_book_age
                and _delta_z < 0.0
                and _obi < 0.0
                and _sweep_reclaim_ok
                and not _cancel_spike_veto
            )

            indicators["bear_trend_quality_ok"] = int(bear_trend_quality_ok)

            if bear_trend_quality_ok:
                trail_profile = "rocket_v1_bear"
                signal["trail_after_tp1"] = True
                indicators["regime_trail_override"] = "trending_bear_short_trend_follow"
            else:
                trail_profile = "protective_only"
                signal["trail_after_tp1"] = True
                indicators["regime_trail_override"] = "trending_bear_to_protective_only"

        elif is_trending_regime_flag:
            trail_profile = "rocket_v1"
            signal["trail_after_tp1"] = True

        elif is_squeeze_regime_flag:
            if _apply_decision(self.orchestrator.check_squeeze_regime(ctx, is_squeeze=True)): return  # type: ignore

        # ---- FIX (2026-05-11): unknown/mixed/na regime → range_protective ----
        # Root cause: unknown regime fell through to rocket_v1 default (WR 29.4%,
        # PnL -23.78$ vs range_protective WR 54.5%, PnL -1.90$).
        # 78.6% of TP1-hit trades reversed into losses with rocket_v1 trailing.
        # Conservative approach: use breakeven-only exit for unclassified regimes.
        elif rg_for_overrides in ("unknown", "mixed", "na", ""):
            trail_profile = "range_protective"
            signal["trail_after_tp1"] = True
            indicators["regime_trail_override"] = "unknown_to_range_protective"

        # Phase 2: override trail_profile from TradeProfile if router is in LIVE/canary mode
        # Only override when the regime-based selection above did NOT explicitly set a profile
        # (i.e., we are still on the default "protective_only" from cfg).
        try:
            if (
                _profile_decision is not None
                and _profile_decision.allowed
                and _profile_decision.mode == "LIVE"
                and _profile_decision.is_canary
                and _profile_decision.profile.trailing_profile
                and trail_profile in ("protective_only", None, "")
            ):
                trail_profile = _profile_decision.profile.trailing_profile
                signal.setdefault("trail_after_tp1", True)
                indicators["trail_profile_from_router"] = trail_profile
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Construction Phase using Typed SignalPayload
        # ------------------------------------------------------------------

        # 1. Gate Decision (if available in indicators)
        gate_decision = None
        if indicators.get("strong_gate_ok") is not None:
             gate_decision = StrongGateDecision(
                 ok=bool(int(indicators.get("strong_gate_ok", 0))),
                 scenario=(indicators.get("strong_gate_scn", "na")),
                 need=int(indicators.get("strong_gate_need", 0)),
                 have=int(indicators.get("strong_gate_have", 0)),
                 a=int(indicators.get("strong_gate_legs", {}).get("A", 0) if isinstance(indicators.get("strong_gate_legs"), dict) else 0),
                 b=int(indicators.get("strong_gate_legs", {}).get("B", 0) if isinstance(indicators.get("strong_gate_legs"), dict) else 0),
                 c=int(indicators.get("strong_gate_legs", {}).get("C", 0) if isinstance(indicators.get("strong_gate_legs"), dict) else 0),
                 reason="gate_decision",
                 legs=indicators.get("strong_gate_legs") if isinstance(indicators.get("strong_gate_legs"), dict) else None
             )

        # 2. Confidence Parts
        conf_parts = indicators.get("confidence_breakdown")

        # 3. Create Typed Payload
        sig_payload = SignalPayload(
            confirmations={c.split("=")[0]: c.split("=")[1] for c in confirmations if "=" in c},
            indicators=indicators,
            gate=gate_decision,
            confidence_parts=conf_parts,
            rejection_reason=reason,
            ts_ms=ts_ms,
            symbol=symbol,
            signal_id=(signal.get("signal_id", "")),
        )

        # Export back to dict for legacy compatibility
        # (This essentially enriches the raw payload with structured data)
        evidence_dict = sig_payload.to_dict()

        # Build final stream payload (legacy structure + new evidence)
        payload = {
            "signal_id": (signal.get("signal_id", "")),
            "symbol": runtime.symbol,
            "direction": direction,
            "entry": entry,
            "sl": sl,
            "tp_levels": [x for x in tp_levels],
            "lot": lot,
            "qty": lot,
            "quantity": lot,
            "atr": atr,
            "confidence": float(signal.get("confidence", 0.0) or 0.0), # Will be re-calculated or passed
            "reason": (signal.get("reason", "unknown")),
            "ts_ms": ts_ms,
            "generated_at": ts_ms,
            "written_at": get_ny_time_millis(),
            "evidence": evidence_dict, # <--- NEW FIELD
            "config_params": signal.get("config_params") or {"strong_gate_ok": indicators.get("strong_gate_ok", 0)},

            # --- Fields kept for backward compatibility with raw consumers ---
            "delta": delta,
            "delta_z": delta_z,
            "tick_qty": indicators.get("tick_qty"),
            "confirmations": confirmations,
            "indicators": indicators,
        }

        # TP ratio: driven by TradeProfile (no hardcoded regime fallback)
        # Profile meta is populated by build_signal_profile_meta() upstream
        _profile_meta = signal.get("meta", {})
        _profile_tp_ratios = _profile_meta.get("tp_ratios")
        if _profile_tp_ratios and isinstance(_profile_tp_ratios, (list, tuple)):
            payload["tp_ratio"] = list(_profile_tp_ratios)
        elif is_range_regime_flag:
            payload["tp_ratio"] = [0.80, 0.20]
        elif is_expansion_regime_flag:
            payload["tp_ratio"] = [0.40, 0.30, 0.30]

        # Optional: publish compact confidence score event to high-frequency stream
        safe_create_task(
            self._maybe_publish_confidence_scores(
                symbol=symbol,
                sid=str(payload.get("signal_id", "")),
                ts_event_ms=ts_ms,
                signal=signal,
                confirmations=confirmations,
                indicators=indicators,
                evidence_dict=evidence_dict,
            ),
            name=f"conf_scores_{symbol}_{ts_ms}"
        )
        # ------------------------------------------------------------------
        # ✅ FIX "BROKEN CHAIN": expose ATR-floor tier selection into indicators
        # so raw stream audit + unified gate see correct atr_floor_th_bps.
        # ------------------------------------------------------------------
        try:
            from core.atr_floor_policy import compute_atr_bps_threshold

            rg = str(getattr(runtime, "last_regime", "na") or "na").lower()

            # Current executed ATR in bps (always useful for audits)
            atr_bps_exec = 0.0
            try:
                if entry > 0 and atr > 0:
                    atr_bps_exec = 10000.0 * (atr / entry)
            except Exception:
                atr_bps_exec = 0.0
            indicators["atr_bps_exec"] = atr_bps_exec

            # Pull floors (prefer calibrated/dynamic; fallback to config)
            t0 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T0_BPS, cfg.get("atr_floor_t0_bps", 0.0)) or 0.0)
            t1 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T1_BPS, cfg.get("atr_floor_t1_bps", 0.0)) or 0.0)
            t2 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T2_BPS, cfg.get("atr_floor_t2_bps", 0.0)) or 0.0)

            tier, picked, floor_th = compute_atr_bps_threshold(regime=rg, cfg=cfg, t0=t0, t1=t1, t2=t2)

            indicators["atr_floor_t0_bps"] = t0
            indicators["atr_floor_t1_bps"] = t1
            indicators["atr_floor_t2_bps"] = t2
            indicators["atr_floor_tier"] = tier
            indicators["atr_floor_picked_bps"] = float(picked)
            indicators["atr_floor_th_bps"] = floor_th
            indicators["atr_floor_rg"] = rg
            indicators["atr_floor_ready"] = int(runtime.dynamic_cfg.get(DK.ATR_CALIB_READY, 0) or 0)
            indicators["atr_floor_src"] = str(runtime.dynamic_cfg.get(DK.ATR_BPS_SRC, "na") or "na")
            indicators["atr_floor_n"] = int(runtime.dynamic_cfg.get(DK.ATR_BPS_N, 0) or 0)

            # Keep legacy mirror used by some earlier logic
            indicators["atr_bps_th"] = floor_th
        except Exception:
            pass

        # Optional: also expose fees-aware threshold for audits even if gate not enforced
        try:
            from core.fees_aware_policy import fees_aware_min_atr_bps

            # tp1_share derived from TP_RATIO (env) or config snapshot
            tp_ratios = parse_tp_ratio(str(cfg.get("tp_ratio", "")))
            tp1_share_actual = tp_ratios[0] if tp_ratios else 0.5
            rocket_mult = self._get_rocket_multiplier(runtime.symbol) or 0.0
            fees_th, fees_meta = fees_aware_min_atr_bps(
                fees_bps_rt=self.FEES_BPS_RT,
                tp_bps_buffer=self.TP_BPS_BUFFER,
                tp1_share=tp1_share_actual,
                rocket_mult=rocket_mult,
            )
            indicators["atr_fees_th_bps"] = fees_th
            indicators["atr_fees_tp1_share"] = tp1_share_actual
            indicators["atr_fees_rocket_mult"] = rocket_mult
        except Exception:
            pass

        # Unified threshold numbers into indicators (debug-only; gate uses same values below)
        try:
            floor_th = float(indicators.get("atr_floor_th_bps", 0.0) or 0.0)
            fees_th = float(indicators.get("atr_fees_th_bps", 0.0) or 0.0)
            unified_th = max(floor_th, fees_th)
            indicators["atr_unified_th_bps"] = unified_th
            indicators["atr_gate_dominant"] = ("fees" if fees_th >= floor_th else "floor") if unified_th > 0 else "na"
        except Exception:
            pass


        # ------------------------------------------------------------------

        # Логируем для отладки с правильным ATR (sample every 10000th message)
        calc_levels_sampler = LogSamplerFactory.get_sampler("CALC_LEVELS", 10000)
        if calc_levels_sampler.should_log(f"calc_levels_{symbol}"):
            logger.info("🔍 Calculating levels for %s: trail_profile=%s, entry=%.8f, atr=%.8f", symbol, trail_profile, entry, atr)

        # ------------------------------------------------------------------
        # TP1 calculation using parametrizable mult
        # ------------------------------------------------------------------
        if trail_profile == "rocket_v1" and tp_levels:
            # Safe mult resolution: meta > symbol-override > global-default
            rocket_mult = float(gate_meta.get("mult") or self._get_rocket_multiplier(symbol))

            expected_tp1 = entry + (atr * rocket_mult) if getattr(direction, 'value', (direction or '')).upper() == "LONG" else entry - (atr * rocket_mult)
            actual_tp1 = tp_levels[0]
            diff = abs(expected_tp1 - actual_tp1)

            # tick-aware tolerance
            ts = getattr(runtime, "tick_size", None)
            if ts is None:
                ts = getattr(getattr(runtime, "ctx", None), "tick_size", None)
            if ts is None:
                ts = getattr(getattr(runtime, "spec", None), "tick_size", None)
            tick_size = float(ts) if ts and float(ts) > 0 else 0.01  # last-resort

            tol = max(2.0 * tick_size, 1e-12)
            hard_tol = 3.0 * tol

            if diff > hard_tol:
                 logger.warning(
                     "⚠️ TP1 mismatch: calc=%.4f vs levels=%.4f (diff=%.6f > %.6f) symbol=%s",
                     expected_tp1, actual_tp1, diff, hard_tol, symbol
                 )
                 # Force correct TP1 if deviation is significant
                 if getattr(direction, 'value', (direction or '')).upper() == "LONG":
                     tp_levels[0] = entry + atr * rocket_mult
                 else:
                     tp_levels[0] = entry - atr * rocket_mult

        mix_dict = self._build_mix_dict(delta, delta_z, indicators, confirmations)

        _conf_raw = signal.get("confidence")
        if _conf_raw is None:
            _conf_raw = indicators.get("confidence")
        if _conf_raw is None:
            _conf_raw = 0.3
        confidence = max(0.0, min(1.0, float(_conf_raw)))

        # --- Hard Confidence Gate (User request: drop signal if confidence too low) ---
        if not bool(runtime.config.get("disable_confidence_filter", False)):
            # Per-symbol override: MIN_CONF_{SYMBOL} env var (e.g. MIN_CONF_DOGEUSDT=35)
            _sym_key = f"MIN_CONF_{symbol.upper().replace('-','')}"
            _sym_min = os.getenv(_sym_key)
            if _sym_min is not None:
                try:
                    min_conf_pct = float(_sym_min)
                except (TypeError, ValueError):
                    min_conf_pct = self._cached_min_conf_pct
            else:
                min_conf_pct = self._cached_min_conf_pct
            if 0 < min_conf_pct <= 1:
                min_conf_pct *= 100.0
            min_conf = min_conf_pct / 100.0

            if _apply_decision(self.orchestrator.check_confidence(ctx, confidence=confidence, min_conf=min_conf)): return  # type: ignore
        # ---

        # ------------------------------------------------------------------
        # Stage: build enriched_signal (raw stream payload)
        # ------------------------------------------------------------------
        enriched_signal = dict(signal)
        enriched_signal.update(
            {
                "strategy": enriched_signal.get("strategy", "cryptoorderflow"),
                "source": "CryptoOrderFlow",
                "tf": enriched_signal.get("tf", "tick"),
                "symbol": symbol,
                "direction": direction,         # legacy
                "side": direction,              # normalized mirror for contract
                "entry": entry,
                "sl": sl,
                "tp_levels": tp_levels,
                "lot": lot,
                "qty": lot * float(getattr(get_specs(symbol), "contract_size", 1.0)),
                "quantity": lot * float(getattr(get_specs(symbol), "contract_size", 1.0)),
                "atr": atr,
                "timestamp": ts_ms,             # legacy mirror
                "ts": ts_ms,                    # common downstream
                "ts_ms": ts_ms,                 # contract
                "trail_after_tp1": self._normalize_trailing_flag(enriched_signal.get("trail_after_tp1"), symbol),
                "trail_profile": trail_profile,
                "trail_callback_pct": trail_callback_pct,
                # Full configuration snapshot for reproducibility
                "config_snapshot": {
                    "config": cfg,
                    "calibrated_specs": getattr(runtime, "calibrated_specs", {}),
                    "indicators": indicators,
                    "runtime_meta": {
                        "spec_update_ts_ms": getattr(runtime, "spec_update_ts_ms", 0),
                        "gate_meta": enriched_signal.get("gate_meta", {}),
                    }
                },
                "runtime": OFConfirmEngine.export_runtime_snapshot(runtime, indicators),
                # Evidence package for downstream analysis
                "evidence": {
                    "obi_stable_secs": float(indicators.get("obi_stable_secs", 0.0) or 0.0),
                    "obi_stability_score": float(indicators.get("obi_stability_score", 0.0) or 0.0),
                    "strong_gate_legs": int(indicators.get("strong_gate_legs", 0) or 0),
                    "strong_gate_scn": (indicators.get("strong_gate_scn", "") or ""),
                    "weak_recent_cnt": int(indicators.get("weak_recent_cnt", 0) or 0),
                    "weak_recent_frac": float(indicators.get("weak_recent_frac", 0.0) or 0.0),
                },
                # TTL / Orphan Housekeeping
                "max_lifetime_bars_after_entry": int(os.getenv("ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY", "180")),
                "orphan_ttl_ms": int(os.getenv("ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY", "0") or 0) or None,
                # FIX #2/#5: Propagate regime + scenario into signal payload
                # Previously computed for gate decisions but never written to payload →
                # all trades had regime=na, scenario=na in trades:closed.
                "regime": rg_for_overrides if rg_for_overrides != "na" else (indicators.get("regime") or "na"),
                "scenario": (indicators.get("strong_gate_scn") or "na"),
                "entry_tag": str(signal.get("entry_tag") or indicators.get("entry_tag") or "na"),
            }
        )

        # Phase 4: ATR Selector Profile for TradeMonitor Trailing
        if atr_meta:
            enriched_signal.setdefault("meta", {})
            enriched_signal["meta"]["atr_profile"] = {
                "atr_value": float(atr_meta.get("atr_value") or atr),
                "atr_tf_ms": int(atr_meta.get("atr_tf_ms") or 0),
                "atr_tf": str(atr_meta.get("atr_tf") or indicators.get("atr_tf_used") or ""),
                "ts_ms": int(atr_meta.get("ts_ms") or 0),
                "src": str(atr_meta.get("src") or atr_meta.get("source") or ""),
            }
            # Phase 6: Expose atr_tf_ms and atr_stop_pct in indicators for ML v5 feature vector.
            # setdefault — does not overwrite if already present (e.g. set by tick_processor).
            try:
                indicators.setdefault("atr_tf_ms", int(atr_meta.get("atr_tf_ms") or 0))
                _atr_val = float(atr_meta.get("atr_value") or atr or 0.0)
                _entry_px = float(indicators.get("price") or enriched_signal.get("entry") or 0.0)
                if _entry_px > 0.0 and _atr_val > 0.0:
                    indicators.setdefault("atr_stop_pct", _atr_val / _entry_px * 100.0)
                else:
                    indicators.setdefault("atr_stop_pct", 0.0)
            except Exception:
                pass

        # Phase 6: Forward horizon contract fields into indicators for ML v5 feature bridge.
        # Values come from enriched_signal (set by signal_pipeline before this point).
        try:
            for _hz_key in ("hold_target_ms", "alpha_half_life_ms", "max_signal_age_ms", "vol_ratio_fast_slow", "vol_ratio_z"):
                _hz_val = enriched_signal.get(_hz_key)
                if _hz_val is not None:
                    indicators.setdefault(_hz_key, _hz_val)
            # Also record signal_ts_ms so ML gate can compute max_signal_age_ratio
            indicators.setdefault("signal_ts_ms", int(enriched_signal.get("ts_ms") or enriched_signal.get("ts") or 0))
        except Exception:
            pass

        # TP ratio for enriched_signal: prefer TradeProfile, fallback to regime heuristics
        _es_meta = enriched_signal.get("meta", {})
        _es_tp_ratios = _es_meta.get("tp_ratios")
        _es_trail_after = _es_meta.get("trail_after_tp_level")
        _es_trail_enabled = _es_meta.get("trail_enabled")
        if _es_tp_ratios and isinstance(_es_tp_ratios, (list, tuple)):
            enriched_signal["tp_ratio"] = list(_es_tp_ratios)
            if _es_trail_after and isinstance(_es_trail_after, int) and _es_trail_after > 0:
                enriched_signal["trail_activate_tp_level_requested"] = _es_trail_after
        elif is_range_regime_flag:
            enriched_signal["tp_ratio"] = [0.80, 0.20]
        elif is_expansion_regime_flag:
            enriched_signal["tp_ratio"] = [0.40, 0.30, 0.30]

        # Override: lock_and_trail profile has its own special distribution
        if trail_profile == "lock_and_trail":
            n_tps = len(tp_levels)
            if n_tps == 2:
                enriched_signal["tp_ratio"] = [0.5, 0.5]
                enriched_signal["trail_activate_tp_level_requested"] = 2
            elif n_tps >= 3:
                rem = round(0.3 / max(1, n_tps - 2), 3)
                enriched_signal["tp_ratio"] = [0.5, 0.2] + [rem] * (n_tps - 2)
                enriched_signal["trail_activate_tp_level_requested"] = 2

        # ------------------------------------------------------------------
        # ✅ FIX net_edge_bps: lift cost/edge fields from indicators → enriched_signal
        # _build_portfolio_risk_input reads from the top-level signal dict.
        # Without this block spread/slippage/edge/fee all arrive as 0.0 →
        # risk engine is blind to real transaction costs → net_edge_bps = 0.0.
        # ------------------------------------------------------------------
        try:
            # 1. spread_bps — prefer exec-health tightened value, fallback to indicators
            if not enriched_signal.get("spread_bps"):
                _spr = float(indicators.get("spread_bps", 0.0) or 0.0)
                if _spr > 0.0:
                    enriched_signal["spread_bps"] = _spr

            # 2. expected_slippage_bps — may already be set by exec-health gate
            if not enriched_signal.get("expected_slippage_bps"):
                _slip = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                if _slip > 0.0:
                    enriched_signal["expected_slippage_bps"] = _slip

            # 3. expected_edge_bps — approximate from ATR-based TP1 distance.
            #    Formula: gross_edge ≈ tp1_dist_bps * tp1_share - stop_bps
            if not enriched_signal.get("expected_edge_bps"):
                try:
                    _entry_p = entry or 0.0
                    _atr_p = atr or 0.0
                    if _entry_p > 0 and _atr_p > 0:
                        _tp_r = parse_tp_ratio(str(cfg.get("tp_ratio", "")))
                        _tp1_share_p = _tp_r[0] if _tp_r else 0.5
                        _rocket_m = self._get_rocket_multiplier(symbol) or 1.5
                        _tp1_bps = (_atr_p * _rocket_m / _entry_p) * 10_000.0 * _tp1_share_p
                        _stop_bps = (_atr_p / _entry_p) * 10_000.0

                        # ✅ Fee-aware edge: subtract round-trip fees
                        _fees_bps = self.FEES_BPS_RT
                        _gross_edge = max(0.0, _tp1_bps - _stop_bps - _fees_bps)

                        if _gross_edge > 0.0:
                            enriched_signal["expected_edge_bps"] = _gross_edge
                except Exception:
                    pass

            # 4. fee_bps — round-trip fee from FEES_BPS_RT ENV (default 10 bps = 0.05%/side)
            if not enriched_signal.get("fee_bps"):
                enriched_signal["fee_bps"] = self.FEES_BPS_RT
        except Exception:
            pass  # fail-open: never block signal publish on cost-field enrichment

        # IDs: Crypto pipeline uses signal["signal_id"] as primary id. Mirror into sid for other consumers.
        try:
            sid = str(signal.get("signal_id") or enriched_signal.get("signal_id") or "").strip()
            if not sid:
                sid = f"signal:{symbol}:cryptoorderflow:{ts_ms}"
                enriched_signal["signal_id"] = sid
            enriched_signal["sid"] = sid
        except Exception:
            sid = f"signal:{symbol}:cryptoorderflow:{ts_ms}"
            enriched_signal["sid"] = sid

        # Contract normalization (FAIL-OPEN)
        preprocess_signal_for_publish(enriched_signal, symbol=symbol, source="CryptoOrderFlow", logger=logger)

        # ------------------------------------------------------------------
        # FINAL ENRICHMENT
        # ------------------------------------------------------------------
        indicators["final_expected_slippage_bps"] = float(indicators.get("expected_slippage_bps", 0.0))

        # ==== Пересчитываем размер позиции по риску (гарантия лимита 5% депозита) ====
        # P2: Liquidity Scaling for Risk
        # 0.5 <= scale <= 2.0. We penalize if scale < 0.8.
        liq_scale = float(indicators.get("liquidity_scale", 1.0) or 1.0)
        risk_factor = 1.0
        if liq_scale < 0.8:
            # Linear ramp: scale=0.5 -> factor=0.5; scale=0.8 -> factor=1.0
            # factor = 0.5 + (scale - 0.5) * (0.5/0.3) = 0.5 + (scale-0.5)*1.666
            # Simplified: just clamp(scale, 0.5, 1.0) effectively?
            # User rec: "decrease lot/leverage if scale < 0.8".
            # Let's map 0.5..0.8 to 0.5..1.0
            risk_factor = max(0.5, min(1.0, liq_scale / 0.8))
            logger.info("⚠️ [RISK] (%s) Liquidity Scale %.2f < 0.8 -> Risk Factor %.2f", symbol, liq_scale, risk_factor)

        # Pull base risk from env or indicators
        base_risk_pct = self._cached_risk_percent
        if 0 < base_risk_pct < 0.5: base_risk_pct *= 100.0 # Sanity handle 0.05

        effective_risk_pct = base_risk_pct * risk_factor

        lot_risk, position_size_usd, deposit, leverage = calculate_position_size(
            symbol=symbol,
            entry_price=entry,
            sl_price=sl,
            side=getattr(direction, 'value', (direction or '')).upper(),
            risk_percent=effective_risk_pct,
            tp_price=tp_levels[0] if tp_levels else 0.0, # ✅ Pass TP1 for profitability floor check
        )

        # ✅ REJECTION CHECK: If profitability floor (min_tp_dist_bps) fails, lot comes back as 0.
        # We must NOT proceed to open a trade. We treat it as a hard veto for quality.
        if lot_risk <= 0:
            logger.warning("🚫 [VETO] (%s) Signal rejected: Profitability floor not met at TP1 (lot=0).", symbol)

            # Calculate what the lot WOULD have been to preserve tracking accuracy for ML
            virtual_lot, _, _, _ = calculate_position_size(
                symbol=symbol,
                entry_price=entry,
                sl_price=sl,
                side=getattr(direction, 'value', (direction or '')).upper(),
                risk_percent=effective_risk_pct,
                tp_price=0.0, # Bypass TP1 floor check to get the mathematical risk lot
            )
            if virtual_lot <= 0:
                virtual_lot = 0.001

            # Push virtual trade for tracking
            safe_create_task(
                self._push_virtual_to_binance_queue(
                    sid=enriched_signal.get("sid") or enriched_signal.get("signal_id") or signal.get("signal_id") or "",
                    symbol=symbol,
                    direction=direction,
                    entry=entry,
                    sl=sl,
                    tp_levels=tp_levels,
                    lot=virtual_lot,
                    ts_ms=ts_ms,
                    confidence=confidence,
                    enriched_signal=enriched_signal,
                    indicators=indicators,
                    is_rejected_signal=True, # This marks it for analytics
                    rejection_reason="low_tp1_dist"
                ),
                name=f"rejected_low_tp_{symbol}_{ts_ms}"
            )
            return

        lot = lot_risk

        # ------------------------------------------------------------------
        # STAGE: Portfolio / Exposure Gate  (BUG-2 fix — gate was never called)
        # Placed AFTER lot_risk so intent_notional = lot × entry is exact.
        # Fail-open: if portfolio_gate is None or Redis down → ALLOW + warn.
        # ------------------------------------------------------------------
        _intent_notional = lot * entry if lot > 0 and entry > 0 else 0.0
        if _apply_decision(await self.orchestrator.check_portfolio(  # type: ignore
            ctx,
            source="CryptoOrderFlow",
            side=direction,
            intent_notional=_intent_notional,
            symbol=symbol,
            kind=kind,
            profile=self._cached_exec_profile,
        )): return

        # ✅ Correct enriched_signal with risk-based lot and margin params
        enriched_signal["v"] = 1  # DTO version stamp — required by TradeMonitorService
        enriched_signal["lot"] = lot
        enriched_signal["qty"] = lot * float(getattr(get_specs(symbol), "contract_size", 1.0))
        enriched_signal["quantity"] = lot * float(getattr(get_specs(symbol), "contract_size", 1.0))
        enriched_signal["position_size_usd"] = position_size_usd
        enriched_signal["deposit"] = deposit
        enriched_signal["leverage"] = leverage

        # Determine validation status based on OFConfirm result
        # OFConfirm result is stored in indicators["of_confirm_ok"] from strategy.py
        of_confirm_ok = indicators.get("of_confirm_ok")
        of_confirm_reason = indicators.get("strong_gate_reason", "unknown")

        # ── DIAGNOSTIC (temporary): trace of_confirm_ok flow ──
        if of_confirm_ok is None:
            _diag_keys = [k for k in indicators.keys() if "of_" in k or "strong_" in k or "gate" in k]
            logger.warning(
                "🔍 [DIAG-OFC] of_confirm_ok=None for %s | id(indicators)=%d | "
                "signal.indicators is same=%s | gate_keys=%s | n_indicators=%d",
                symbol,
                id(indicators),
                str(indicators is signal.get("indicators")),
                str(_diag_keys[:10]),
                len(indicators),
            )

        gate_mode = (indicators.get("of_gate_mode") or "").upper()

        if of_confirm_ok == 1:
            validation_status = "passed"
            validation_reason = f"OFConfirm passed ({of_confirm_reason})"
        elif of_confirm_ok == 0:
            is_virtual = bool(int(enriched_signal.get("is_virtual", 0) or signal.get("is_virtual", 0)))
            if is_virtual:
                validation_status = "passed"
                validation_reason = f"OFConfirm shadowed (virtual trade): {indicators.get('of_confirm', {}).get('reason', of_confirm_reason)}"
            else:
                validation_status = "failed"
                validation_reason = f"OFConfirm failed: {indicators.get('of_confirm', {}).get('reason', of_confirm_reason)}"
        else:
            # of_confirm_ok not set or OFConfirm was not evaluated
            validation_status = "bypassed"
            validation_reason = "OFConfirm not evaluated"

        enriched_signal["validation_status"] = validation_status
        enriched_signal["validation_reason"] = validation_reason

        # --- SHADOW MODE: mark main signal as virtual if validation failed or shadowed
        gate_mode = (indicators.get("of_gate_mode") or "").upper()
        # Defensive: if validation failed, it's virtual regardless of gate mode string
        if validation_status == "failed" or indicators.get("gate_shadow_veto") or (gate_mode == "SHADOW"):
            enriched_signal["is_virtual"] = 1
            if indicators.get("gate_shadow_veto"):
                enriched_signal["validation_status"] = "failed"
                enriched_signal["validation_reason"] = indicators.get("gate_reason", "SHADOW_VETO")
        
        # Recommendation 3: Explicitly segregate execution modes
        # shadow: indicative only (SHADOW mode)
        # virtual: no real capital (either rejected or shadow)
        # tradeable: real capital intended (ENFORCE + passed)
        is_virtual_val = bool(int(enriched_signal.get("is_virtual", 0) or 0))
        enriched_signal["shadow"] = (gate_mode == "SHADOW")
        enriched_signal["virtual"] = is_virtual_val or enriched_signal["shadow"]
        enriched_signal["tradeable"] = not enriched_signal["virtual"]
        # ---



        crypto_signal = CryptoSignal(
            sid=signal["signal_id"],
            symbol=symbol,
            side=getattr(direction, 'value', (direction or '')).upper(),
            entry=entry,
            sl=sl,
            tp_levels=tp_levels,
            lot=lot,
            position_size_usd=position_size_usd,
            deposit=deposit,
            leverage=leverage,
            atr=atr,
            confidence=confidence,
            ts=int(signal.get("tick_ts") or signal.get("generated_at") or ts_ms or 0),
            source="CryptoOrderFlow",
            reason_mix=mix_dict,
            confirmations=confirmations,
            indicators=indicators,
            trail_profile=trail_profile,
            trail_after_tp1=self._normalize_trailing_flag(enriched_signal.get("trail_after_tp1"), symbol),
            config_params=signal.get("config_params") or {"strong_gate_ok": signal.get("indicators", {}).get("strong_gate_ok", 0)},
            validation_status=enriched_signal.get("validation_status"),
            validation_reason=enriched_signal.get("validation_reason"),
            atr_sel_tf=indicators.get("atr_tf_used"),
            atr_sel_age=indicators.get("atr_age_ms"),
        )

        # Определяем, является ли сигнал слабым (Не проходит Gate и уверенность < 70%)
        strong_gate_ok = crypto_signal.config_params.get("strong_gate_ok", 0) if crypto_signal.config_params else 0
        is_weak = (int(strong_gate_ok) != 1 and confidence < 0.85)

        validation_status = enriched_signal.get("validation_status")

        if is_weak:
            telegram_payload = None
            logger.info("🚫 [TELEGRAM] (%s) Signal is WEAK (strong_ok=%s, conf=%.2f). Skipping notify.", symbol, strong_gate_ok, confidence)
        elif validation_status == "failed":
            telegram_payload = None
            logger.info(
                "🚫 [TELEGRAM] (%s) Signal validation FAILED (%s). Skipping Telegram notify.",
                symbol,
                enriched_signal.get("validation_reason", "unknown"),
            )
        elif validation_status == "bypassed":
            telegram_payload = None
            logger.info(
                "🚫 [TELEGRAM] (%s) Signal validation BYPASSED. Skipping Telegram notify.",
                symbol,
            )
        else:
            telegram_payload = {
                "text": CryptoSignalFormatter.format_telegram_message(crypto_signal),
                "symbol": symbol,
                "direction": crypto_signal.side,
                "entry": f"{entry:.2f}",
                "stop": f"{sl:.2f}",
                "tp": ",".join(f"{tp:.2f}" for tp in tp_levels),
                "source": crypto_signal.source,
                "reason": signal.get("reason") or "delta_spike",
                "timestamp": str(crypto_signal.ts),
            }

        # P4 latency contract: ensure canonical timestamp fields are present, then stamp emit time.
        # This is the true publish boundary – done once, immediately before the outbox write.
        try:
            ensure_epoch_ms_fields(
                enriched_signal,
                default_event_ms=int(enriched_signal.get("ts_ms") or ts_ms),
                default_feature_ms=int(enriched_signal.get("ts_feature_ms") or enriched_signal.get("ts_ms") or ts_ms),
            )
            redis_client_lc = getattr(self.publisher, "r", None) if self.publisher is not None else None
            await stamp_emit_and_observe_async(
                enriched_signal,
                redis_client=redis_client_lc,
                service="python_worker",
                symbol=symbol,
            )
        except Exception as _lc_err:
            logger.debug("(%s) latency contract emit stamp failed: %s", symbol, _lc_err)

        # A2 Decision Snapshot publication (enriched output, fail-open)
        # This MUST contain joinable keys, is_virtual flag, and be stable across retries.
        if self.decision_snapshot_publish_enabled:
            try:
                snap = build_decision_snapshot(
                    enriched_signal,  # pass enriched to capture validation fields
                    runtime=runtime,
                    indicators=indicators,
                    schema_version=self.decision_snapshot_schema_version,
                )
                safe_create_task(
                    publish_decision_snapshot(
                        publisher=self.publisher,
                        snapshot=snap,
                        stream=self.decision_snapshot_stream,
                        maxlen=self.decision_snapshot_stream_maxlen,
                        symbol=symbol,
                    ),
                    name=f"snap_{symbol}_{ts_ms}"
                )
            except Exception as e:
                logger.warning("⚠️ (%s) decision_snapshot publish failed: %s", symbol, e)

        audit_payload = {}
        signal_stream = self.cryptoorderflow_signal_stream_template.format(symbol=symbol)
        env = {}
        env_json = ""
        try:
            audit_payload = {
                "v": 1,
                "is_virtual": int(enriched_signal.get("is_virtual", 0)),
                "sid": crypto_signal.sid,
                "signal_id": crypto_signal.sid,   # canonical mirror
                "symbol": symbol,
                "side": crypto_signal.side,
                "entry": entry,
                "sl": sl,
                "tp_levels": tp_levels,
                "lot": lot,
                "qty": lot,
                "quantity": lot,
                "source": "CryptoOrderFlow",
                "reason": signal.get("reason") or "delta_spike",
                "confidence": confidence,
                "confidence01": confidence,
                "confidence_pct": confidence * 100.0,
                "atr": atr,
                "ts": ts_ms,
                "ts_ms": ts_ms,
                # P4 latency contract canonical timestamp copies
                FIELD_TS_EVENT_MS: int(enriched_signal.get(FIELD_TS_EVENT_MS) or ts_ms),
                FIELD_TS_FEATURE_MS: int(enriched_signal.get(FIELD_TS_FEATURE_MS) or ts_ms),
                FIELD_TS_EMIT_MS: int(enriched_signal.get(FIELD_TS_EMIT_MS) or int(time.time() * 1000)),
                "trail_after_tp1": self._normalize_trailing_flag(enriched_signal.get("trail_after_tp1"), symbol),
                "trail_profile": (enriched_signal.get("trail_profile") or "range_protective"),
                "indicators": indicators,
                "strategy": "cryptoorderflow",
                "tf": "tick",
            }

            env = build_outbox_envelope(
                sid=crypto_signal.sid,
                symbol=symbol,
                kind="crypto_orderflow",
                notify_payload=telegram_payload,
                audit_payload=enriched_signal,
                signal_stream_payload=audit_payload,
                audit_stream=self.raw_signal_stream,
                signal_stream=signal_stream,
                meta={
                    "is_virtual": int(enriched_signal.get("is_virtual", 0)),
                }
            )

            logger.debug("DEBUG_OUTBOX_ENV: keys=%s targets=%s", list(env.keys()), list(env.get('targets', {}).keys()) if 'targets' in env else 'MISSING')

            # ✅ VALIDATION: Ensure envelope structure is correct (audit_payload/meta must not be on top level)
            if "audit_payload" in env or "meta" not in env or "targets" not in env:
                logger.error(f"❌ ({symbol}) Invalid envelope structure: audit_payload on top level or missing required fields")
                logger.error(f"   env keys: {list(env.keys())}")
                logger.error(f"   targets keys: {list(env.get('targets', {}).keys()) if 'targets' in env else 'MISSING'}")
                if _INVALID_ENVELOPE_TOTAL is not None:
                    with contextlib.suppress(Exception):
                        _INVALID_ENVELOPE_TOTAL.labels(symbol=symbol).inc()
                return

            env_json = dumps_env(env)
        except Exception as err:
            logger.error(f"❌ ({symbol}) Error building outbox envelope: {err}", exc_info=True)
            env_json = ""
            env = {}

        # Outbox path:
        #   - outbox-only: no direct publishing (dispatcher does it)
        #   - shadow: publish legacy + outbox (audit/compare during rollout)
        if env_json and (use_outbox or shadow_outbox):
            # Atomic outbox write + meta sidecar (DecisionTrace full)
            meta_obj = None
            try:
                # минимальный ctx только для trace (чтобы не тянуть весь объект)
                ctx_min = SimpleNamespace()
                ctx_min.ts_ms = get_ny_time_millis()
                # если symbol/kind доступны в этой области — проставьте:
                try:
                    ctx_min.symbol = str(env.get("symbol") or env.get("sym") or "")
                    ctx_min.kind = (env.get("kind") or "")
                except Exception:
                    pass
                if trace_enabled():
                    ensure_trace(ctx_min, sid=sid)
                    # detector stage timing (минимально)
                    trace_gate(ctx_min, stage="detector", name="service_emit", passed=True, veto=False, reason_code="OK", duration_ms=0.0)
                    
                    # --- METRICS: DECISION TO OUTBOX ---
                    if _DECISION_TO_OUTBOX_MS is not None:
                        outbox_ts_ms = time.time() * 1000
                        decision_ts_ms = indicators.get("ts_feature_ms") or indicators.get("end_ts_ms") or ts_ms
                        if decision_ts_ms and outbox_ts_ms > float(decision_ts_ms):
                            _DECISION_TO_OUTBOX_MS.observe(outbox_ts_ms - float(decision_ts_ms))
                            
                    meta_obj = build_trace_sidecar_meta_from_ctx(ctx=ctx_min, sid=sid)
            except Exception:
                meta_obj = None

            # env_json уже готов (как раньше). Пишем его как payload_obj.
            payload_obj = env  # dict envelope
            # CRITICAL PATH: Keep outbox write SYNCHRONOUS for delivery guarantee to execution engine
            await atomic_xadd_async(
                self.publisher.r,
                stream_key=outbox_stream,
                signal_id=sid,
                payload_obj=payload_obj,
                kind=(env.get("kind") or ""),
                symbol=(env.get("symbol") or ""),
                ts=(env.get("ts_ms") or ""),
                meta_obj=meta_obj,
            )

            # NOTE: Direct Telegram notification REMOVED — dispatcher handles notify delivery
            # via outbox envelope targets["notify"]. Sending directly here caused:
            #   1. Double notifications (pipeline + dispatcher)
            #   2. Broken rate-limit: ts_ms % every_n is almost always 0 (ts_ms is divisible)
            #   3. Bypassed dispatcher's NotifyGate (CRYPTO_NOTIFY_SIGNAL_EVERY_N ignored)
            # The outbox path above already carries notify_payload in env["targets"]["notify"].
            if not telegram_payload:
                logger.debug("⏭️ [TELEGRAM] (%s) Skipped notify: payload=None (failed validation or weak signal)", symbol)


            # Also send to raw stream for ExecutionGateService compatibility
            # NOTE: Fire-and-forget to keep outbox XADD as the sole synchronous critical path.
            pub = self.publisher
            safe_create_task(
                pub.xadd_json(
                    sink=StreamSink(name=self.raw_signal_stream, field="payload", maxlen=100000),
                    payload=enriched_signal,
                    symbol=symbol,
                ),
                name=f"raw_stream_{symbol}_{ts_ms}"
            )

            # Feed TB Labeler (P45 fix) - Outbox Path
            # await: signals:of:inputs is consumed by ML dataset builder; drop = training data corruption.
            if self.of_inputs_publish_enabled and self.of_inputs_stream:
                await self._publish_of_inputs(
                    publisher=pub,
                    enriched_signal=enriched_signal,
                    symbol=symbol,
                    path="outbox",
                )

            # Skip execution queue for virtual signals (tracked by trade-monitor only)
            if not enriched_signal.get("is_virtual"):
                safe_create_task(
                    self._push_virtual_to_binance_queue(
                        sid=sid, symbol=symbol, direction=direction,
                        entry=entry, sl=sl, tp_levels=tp_levels, lot=lot,
                        ts_ms=ts_ms, confidence=confidence,
                        enriched_signal=enriched_signal, indicators=indicators,
                    ),
                    name=f"virtual_push_{symbol}_{ts_ms}"
                )
            else:
                logger.info("ℹ️ (%s) Paper trade: skipping binance execution queue (is_virtual=1)", symbol)
            return

        # ------------------------------------------------------------------
        # DIRECT PUBLISHING (Failback / Mixed Mode)
        # ------------------------------------------------------------------

        # 1) Telegram Notify
        notify_enabled = True  # Send all signals but with validation status
        if notify_enabled and self.publisher.r and telegram_payload:
             # Rate limiting implemented via modulo check (simple but effective for flood control)
             notify_signal_every_n = self._cached_notify_every_n
             msg_id = ts_ms
             counter_value = msg_id # Proxy monotonic

             try:
                # P99 FIX: fire-and-forget for notify XADD to avoid blocking hot path
                 if counter_value % notify_signal_every_n == 0:
                     safe_create_task(
                         self.publisher.r.xadd(
                             self.notify_stream,
                             fields=telegram_payload,
                             maxlen=20000,
                         ),
                         name=f"notify_direct_{symbol}_{ts_ms}"
                     )
             except Exception as exc:
                 logger.warning("⚠️ (%s) Не удалось опубликовать в %s: %s", symbol, self.notify_stream, exc)

        # 2) Raw Stream via AsyncSignalPublisher
        # P99 FIX: Use safe_create_task instead of await to avoid head-of-line
        # blocking compounding when Redis is under pressure from heavy XREVRANGE
        # commands (of-timers-worker). Raw stream is fire-and-forget safe.
        pub = self.publisher
        with contextlib.suppress(Exception):
             safe_create_task(
                 pub.xadd_json(
                     sink=StreamSink(name=self.raw_signal_stream, field="payload", maxlen=100000),
                     payload=enriched_signal,
                     symbol=symbol,
                 ),
                 name=f"raw_direct_{symbol}_{ts_ms}"
             )

        # Feed TB Labeler (P45 fix) - Direct Path
        # signals:of:inputs is consumed by ML dataset builder — await to guarantee delivery.
        # Fire-and-forget here causes join failures in dataset_report (ML training data corruption).
        if self.of_inputs_publish_enabled and self.of_inputs_stream:
            await self._publish_of_inputs(
                publisher=pub,
                enriched_signal=enriched_signal,
                symbol=symbol,
                path="direct",
            )


        # 3) Audit Payload via AsyncSignalPublisher
        # P99 FIX: fire-and-forget (audit is non-critical, maxlen=1000)
        preprocess_signal_for_publish(audit_payload, symbol=symbol, source="CryptoOrderFlow", logger=logger)
        safe_create_task(
            pub.xadd_json(
                sink=StreamSink(name=signal_stream, field="data", maxlen=1000),
                payload=audit_payload,
                symbol=symbol,
            ),
            name=f"audit_{symbol}_{ts_ms}"
        )

        # ------------------------------------------------------------------
        # 4) Bookkeeping (Deterministic signal audit/cooldown)
        # Moved to the end to ensure it only updates on actual successful emit path.
        # ------------------------------------------------------------------
        try:
            # Update last emission state
            runtime.last_signal_ts = ts_ms
            runtime.last_emit_ts_ms = ts_ms
            runtime.last_emit_dir = direction

            # Record pressure event
            runtime.pressure.record_emit(ts_ms)

            # Backward compat for SMT logic: update last_of_strong_ts_ms if strong confirmation passed
            ok = int(indicators.get("of_confirm_ok", 0) or indicators.get("strong_gate_ok", 0) or 0)
            if ok == 1:
                runtime.last_of_strong_ts_ms = ts_ms
                runtime.last_of_dir = direction

            # Persist last strong metadata (useful for audits)
            runtime.last_of_confirm_score = float(indicators.get("of_confirm_score", 0.0) or 0.0)
            runtime.last_strong_gate_have = int(indicators.get("strong_gate_have", 0) or 0)
            runtime.last_strong_gate_need = int(indicators.get("strong_gate_need", 0) or 0)
            runtime.last_strong_gate_scn = (indicators.get("strong_gate_scn", "") or "")

        except Exception as e:
            logger.debug("⚠️ publish_signal final bookkeeping error: %s", e)

        # Push to Binance executor (direct publish path, skip if virtual)
        if not enriched_signal.get("is_virtual"):
            await self._push_virtual_to_binance_queue(
                sid=sid, symbol=symbol, direction=direction,
                entry=entry, sl=sl, tp_levels=tp_levels, lot=lot,
                ts_ms=ts_ms, confidence=confidence,
                enriched_signal=enriched_signal, indicators=indicators,
            )
        else:
            logger.info("ℹ️ (%s) Paper trade: skipping binance execution queue in direct path (is_virtual=1)", symbol)

    async def _push_virtual_to_binance_queue(
        self,
        *,
        sid: str,
        symbol: str,
        direction: str,
        entry: float,
        sl: float,
        tp_levels: list[float],
        lot: float,
        ts_ms: int,
        confidence: float,
        enriched_signal: dict[str, Any],
        indicators: dict[str, Any],
        is_rejected_signal: bool = False,
        rejection_reason: str = "",
    ) -> None:
        """Push orderflow signals to Binance demo queue as is_virtual=1.

        Two modes (controlled by ENV):

        BINANCE_VIRTUAL_MIRROR_ALL=1  (full-mirror mode)
            ALL confirmed signals — regardless of validation_status — are
            mirrored to ORDERS_QUEUE_BINANCE_MIRROR (default: orders:queue:binance:mirror).
            A dedicated binance-executor-demo service consumes this queue with
            BINANCE_CLIENT_MODE=demo. Confidence filter is bypassed.

        BINANCE_VIRTUAL_ORDERS_ENABLED=1  (shadow-only mode, legacy)
            Only rejected / shadow-vetoed signals are pushed to
            ORDERS_QUEUE_BINANCE (default: orders:queue:binance).
            Requires confidence >= CRYPTO_SIGNAL_MIN_CONF threshold.

        BinanceExecutor reads is_virtual=1 and routes to demo/testnet client.
        """
        mirror_all = self.binance_virtual_mirror_all
        shadow_only = self.binance_virtual_orders_enabled

        if not mirror_all and not shadow_only:
            return  # both modes disabled — nothing to do

        if not self.publisher or not self.publisher.r:
            return

        validation_status = (enriched_signal.get("validation_status") or "").lower()
        is_virtual_flag = bool(int(enriched_signal.get("is_virtual", 0) or indicators.get("is_virtual", 0) or 0))

        min_conf_pct = self._cached_min_conf_pct
        if 0 < min_conf_pct <= 1:
            min_conf_pct *= 100.0
        min_conf = min_conf_pct / 100.0

        if mirror_all:
            # ONLY mirror real trades (passed) or virtual trades (ok_soft) that meet confidence threshold
            is_passed = (validation_status == "passed")
            meets_conf = (confidence >= min_conf)
            if not is_passed and not (is_virtual_flag and meets_conf):
                return
        else:
            # Shadow-only mode: push only rejected/shadow-vetoed signals
            if not is_rejected_signal and validation_status != "failed" and not is_virtual_flag:
                return

        if not mirror_all:
            # Confidence filter only in shadow-only mode (mirror_all logic handled above)
            if confidence < min_conf:
                return

        try:
            # mirror_all uses a dedicated queue so prod queue stays clean
            if mirror_all:
                binance_queue = self._cached_orders_mirror_queue
            else:
                # Variant B: route through ExecutionRouter intent queue for scale-in support
                binance_queue = self._cached_orders_intent_queue

            # --- PRE-TRADE RISK: EDGE COST GATE (Recommendation 5) ---
            edge_cost_enabled = int(os.getenv("EDGE_COST_GATE_ENABLED", "0")) == 1
            if edge_cost_enabled and not is_rejected_signal:
                edge_cost_mode = os.getenv("EDGE_COST_GATE_MODE", "ENFORCE").upper()
                edge_margin_bps = float(os.getenv("EDGE_COST_MARGIN_BPS", "2.0"))
                exec_max_slippage_bps = float(os.getenv("EXEC_MAX_SLIPPAGE_BPS", "12.0"))
                exec_max_impact_bps = float(os.getenv("EXEC_MAX_IMPACT_BPS", "8.0"))

                # Derive metrics
                fee_bps = self.FEES_BPS_RT
                spread_bps = float(enriched_signal.get("spread_bps", indicators.get("spread_bps", 0.0)) or 0.0)
                slippage_ema_bps = float(indicators.get("expected_slippage_bps", exec_max_slippage_bps) or 0.0)
                impact_bps = float(indicators.get("perm_impact_p95_bps", exec_max_impact_bps) or 0.0)
                expected_edge_bps = float(enriched_signal.get("expected_edge_bps", indicators.get("expected_edge_bps", 0.0)) or 0.0)

                cost_bps = fee_bps + (spread_bps / 2.0) + slippage_ema_bps + impact_bps
                edge_bps = expected_edge_bps

                if edge_bps <= cost_bps + edge_margin_bps:
                    if edge_cost_mode == "ENFORCE":
                        # Fail-closed enforce mode
                        is_rejected_signal = True
                        rejection_reason = "EDGE_COST_NEGATIVE"
                        validation_status = "failed"
                        enriched_signal["validation_status"] = validation_status
                        logger.warning("🚫 [%s] EDGE_COST_NEGATIVE: edge_bps=%.1f <= cost_bps=%.1f + margin=%.1f", symbol, edge_bps, cost_bps, edge_margin_bps)
                    else:
                        logger.info("ℹ️ [%s] SHADOW EDGE_COST_NEGATIVE: edge_bps=%.1f <= cost_bps=%.1f + margin=%.1f", symbol, edge_bps, cost_bps, edge_margin_bps)

            # --- PRE-TRADE RISK: PORTFOLIO EXPOSURE GATE (Recommendation 6) ---
            if not is_rejected_signal and self.orchestrator.portfolio_gate:
                decision = await self.orchestrator.portfolio_gate.evaluate(
                    symbol=symbol,
                    source=(enriched_signal.get("source") or "CryptoOrderFlow"),
                    side=direction,
                    intent_notional=float(lot * entry if entry > 0 else 0)
                )
                if decision.decision != "ALLOW":
                    is_rejected_signal = True
                    rejection_reason = decision.reason_code
                    validation_status = "failed"
                    enriched_signal["validation_status"] = validation_status
                    logger.warning("🚫 [%s] PORTFOLIO_GATE_REJECTED: %s", symbol, decision.reason_code)

            # --- METRICS: FEATURE TO DECISION ---
            if _FEATURE_TO_DECISION_MS is not None:
                decision_ts_ms = time.time() * 1000
                feature_ts_ms = indicators.get("ts_feature_ms") or indicators.get("end_ts_ms") or ts_ms
                if feature_ts_ms and decision_ts_ms > float(feature_ts_ms):
                    _FEATURE_TO_DECISION_MS.observe(decision_ts_ms - float(feature_ts_ms))

            # --- STRICT CONTRACT VALIDATION (SignalV1Strict) ---
            try:
                from core.contracts import SignalV1Strict
                signal_strict = SignalV1Strict(
                    symbol=symbol,
                    ts_ms=int(ts_ms),
                    direction=str(direction),
                    scenario=(indicators.get("strong_gate_scn", "none")),
                    confidence=float(confidence),
                    indicators=indicators,
                    entry=entry,
                    sl=sl,
                    lot=lot,
                )
            except Exception as e:
                logger.error("🚫 STRICT CONTRACT REJECTED: Signal validation failed before orders: %s", e)
                return

            # --- CONTRACT VALIDATION (OrderIntentV1) ---
            order_payload: dict[str, Any] = {}
            try:
                # 1) Standardize side for Execution
                side_norm = normalize_side_3_safe(direction)

                # 2) Build extra meta
                meta = {
                    "is_virtual": mirror_all or is_virtual_flag,
                    "mirror_all": mirror_all,
                    "source": (enriched_signal.get("source") or "CryptoOrderFlow"),
                    "strategy": (enriched_signal.get("strategy") or "cryptoorderflow"),
                    "confidence": confidence,
                    "confidence_pct": confidence * 100.0,
                    "trail_after_tp1": bool(enriched_signal.get("trail_after_tp1", False)),
                    "trail_profile": (enriched_signal.get("trail_profile") or "range_protective"),
                    "regime": (indicators.get("regime", "na")),
                    "atr": float(indicators.get("atr_used_for_levels") or indicators.get("atr", 0.0) or 0.0),
                    "atr_used_for_levels": float(indicators.get("atr_used_for_levels", 0.0) or 0.0),
                    "atr_tf_used": (indicators.get("atr_tf_used", "")),
                    "sl_atr_mult": float(indicators.get("sl_atr_mult", 0.0) or 0.0),
                    "tp1_atr_mult": float(indicators.get("tp1_atr_mult", 0.0) or 0.0),
                    "sl_atr": float(indicators.get("sl_atr", 0.0) or 0.0),
                    "tp1_atr": float(indicators.get("tp1_atr", 0.0) or 0.0),
                    "validation_status": "failed" if is_rejected_signal else (enriched_signal.get("validation_status") or ""),
                    "is_rejected_signal": 1 if is_rejected_signal else 0,
                    "rejection_reason": (rejection_reason or ""),
                    "sl_price": sl,
                    "tp_levels": [x for x in (tp_levels or [])],
                }
                if "tp_ratio" in enriched_signal:
                    meta["tp_ratio"] = enriched_signal["tp_ratio"]
                if "trail_activate_tp_level_requested" in enriched_signal:
                    meta["trail_activate_tp_level_requested"] = enriched_signal["trail_activate_tp_level_requested"]

                intent_v1 = OrderIntentV1(
                    intent_id=f"int:{sid}:{int(time.time()*1000)}",
                    signal_id=sid,
                    symbol=symbol,
                    ts_ms=ts_ms,
                    side=side_norm,
                    order_type="MARKET",
                    price=entry,
                    qty=lot,
                    meta=meta
                )
                order_payload = intent_v1.model_dump()
            except Exception as e:
                logger.warning("⚠️ (%s) OrderIntentV1 validation failed: %s", symbol, e)
                # Fallback to legacy dict if validation fails to prevent order loss
                order_payload = {
                    "action": "open",
                    "sid": sid,
                    "symbol": symbol,
                    "side": direction,
                    "qty": lot,
                    "type": "MARKET",
                    "entry": entry,
                    "sl": sl,
                    "tp_levels": [x for x in (tp_levels or [])],
                    "is_virtual": 1 if mirror_all else (1 if is_virtual_flag else 0),
                    "mirror_all": mirror_all,
                    "source": (enriched_signal.get("source") or "CryptoOrderFlow"),
                    "strategy": (enriched_signal.get("strategy") or "cryptoorderflow"),
                    "confidence": confidence,
                    "confidence_pct": confidence * 100.0,
                    "ts_ms": ts_ms,
                    "trail_after_tp1": bool(enriched_signal.get("trail_after_tp1", False)),
                    "trail_profile": (enriched_signal.get("trail_profile") or "range_protective"),
                    "regime": (indicators.get("regime", "na")),
                    "atr": float(indicators.get("atr_used_for_levels") or indicators.get("atr", 0.0) or 0.0),
                    "atr_used_for_levels": float(indicators.get("atr_used_for_levels", 0.0) or 0.0),
                    "atr_tf_used": (indicators.get("atr_tf_used", "")),
                    "sl_atr_mult": float(indicators.get("sl_atr_mult", 0.0) or 0.0),
                    "tp1_atr_mult": float(indicators.get("tp1_atr_mult", 0.0) or 0.0),
                    "sl_atr": float(indicators.get("sl_atr", 0.0) or 0.0),
                    "tp1_atr": float(indicators.get("tp1_atr", 0.0) or 0.0),
                    "validation_status": "failed" if is_rejected_signal else (enriched_signal.get("validation_status") or ""),
                    "is_rejected_signal": 1 if is_rejected_signal else 0,
                    "rejection_reason": (rejection_reason or ""),
                }
                if "tp_ratio" in enriched_signal:
                    order_payload["tp_ratio"] = enriched_signal["tp_ratio"]
                if "trail_activate_tp_level_requested" in enriched_signal:
                    order_payload["trail_activate_tp_level_requested"] = enriched_signal["trail_activate_tp_level_requested"]

            # --- Calibration / shadow metadata passthrough ---
            # Guarantees calib fields reach executor → orders:state → trades:closed
            try:
                from services.shadow_calib_meta import extract_calib_fields, stamp_virtual_if_calib
                calib_from_signal = extract_calib_fields(enriched_signal)
                calib_from_indicators = extract_calib_fields(indicators)
                # Signal wins over indicators
                for k, v in calib_from_indicators.items():
                    calib_from_signal.setdefault(k, v)
                order_payload.update(calib_from_signal)
                stamp_virtual_if_calib(order_payload)
            except Exception:
                pass  # fail-open

            # Phase 5.4: hard execution budget gate
            budget_allow = True
            budget_reason = "ATR_POLICY_BUDGET_NOT_CHECKED"
            budget_diag = {}

            # Advisory vs Hard toggle via ENV
            is_advisory = self._cached_exec_budget_advisory

            if not is_virtual_flag or mirror_all:
                try:
                    from services.atr_policy_execution_budget_gate import PolicyExecutionBudgetGate
                    gate = PolicyExecutionBudgetGate(redis_client=self.publisher.r)
                    budget_allow, budget_reason, budget_diag = gate.validate(enriched_signal)
                except Exception as e:
                    logger.error("Budget gate error sid=%s: %s", sid, e)
                    # Master fail-closed toggle (failsafe)
                    fail_policy = self._cached_exec_budget_fail_policy
                    budget_allow = (fail_policy == "OPEN")
                    budget_reason = f"ATR_POLICY_BUDGET_GATE_ERROR:{e}"

            enriched_signal.setdefault("meta", {})
            enriched_signal["meta"]["execution_budget_gate"] = {
                "allow": budget_allow,
                "reason_code": budget_reason,
                "diag": budget_diag,
                "advisory": is_advisory,
            }
            if isinstance(order_payload, dict):
                order_payload.setdefault("meta", {})
                order_payload["meta"]["execution_budget_gate"] = enriched_signal["meta"]["execution_budget_gate"]

            if not budget_allow and not is_advisory:
                logger.warning(
                    "🛡️ [BUDGET-GATE] Signal blocked sid=%s symbol=%s reason=%s",
                    sid, symbol, budget_reason
                )
                # Ensure we still send the telegram digest if it was blocked
                await self.send_telegram_report(
                    f"🛡️ <b>[BUDGET-GATE] DENIED</b>\nSymbol: {symbol}\nReason: {budget_reason}",
                    source="budget_gate",
                    symbol=symbol,
                )
                return  # Block actual order route

            # Phase 5.6: hard portfolio correlation gate
            portfolio_allow = True
            portfolio_reason = "ATR_PORTFOLIO_NOT_CHECKED"
            portfolio_diag = {}

            # Advisory vs Hard toggle via ENV
            port_is_advisory = self._cached_portfolio_advisory
            port_is_enabled = self._cached_portfolio_enable

            if port_is_enabled and (not is_virtual_flag or mirror_all):
                try:
                    from services.atr_policy_portfolio_gate import PolicyPortfolioGate
                    pgate = PolicyPortfolioGate(redis_client=self.publisher.r)
                    portfolio_allow, portfolio_reason, portfolio_diag = pgate.validate(enriched_signal)
                except Exception as e:
                    logger.error("Portfolio gate error sid=%s: %s", sid, e)
                    # Master fail-closed toggle (failsafe)
                    fail_policy = self._cached_portfolio_fail_policy
                    portfolio_allow = (fail_policy == "OPEN")
                    portfolio_reason = f"ATR_PORTFOLIO_GATE_ERROR:{e}"

                if not portfolio_allow:
                    # Write event unconditionally to DB if denied
                    try:
                        import psycopg2
                        import psycopg2.extras

                        # Avoid establishing connection inline if possible, but for isolation we'll do it or use a background submitter.
                        # It's better to log and let an out-of-band aggregator or background task harvest it if we shouldn't block.
                        # Since we blocked, it's safer to just log and alert for now, but the task requires `atr_policy_portfolio_events`.
                        # Let's write the event via a background task so we don't block the hot path too heavily on DB connect.
                        from utils.task_manager import safe_create_task

                        async def _record_portfolio_deny(dsn, src, ven, sym, fc, scen, reg, p_horiz, p_layer, p_ver, act, reason, ev_json):
                            try:
                                # Run blocking sync inserts via run_in_executor in production or similar, actually psycopg2 is sync.
                                # But let's just do a quick sync insert. This is a cold path (deny).
                                _conn = psycopg2.connect(dsn, connect_timeout=2)
                                with _conn, _conn.cursor() as _cur:
                                    _cur.execute("""
                                        INSERT INTO atr_policy_portfolio_events(
                                            source, venue, symbol, factor_cluster, scenario, regime, risk_horizon_bucket, layer, policy_ver, action, reason_code, event_json
                                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                                    """, (src, ven, sym, fc, scen, reg, p_horiz, p_layer, p_ver, act, reason, json.dumps(ev_json)))
                                _conn.close()
                            except Exception as ex:
                                logger.error("Failed to write to atr_policy_portfolio_events: %s", ex)

                        fc = portfolio_diag.get("factor_cluster", "unknown")
                        pg_dsn = self._cached_analytics_db_dsn
                        meta = enriched_signal.get("meta", {})
                        prov = meta.get("policy_provenance", {})

                        safe_create_task(_record_portfolio_deny(
                            pg_dsn,
                            (enriched_signal.get("source") or "CryptoOrderFlow"),
                            (enriched_signal.get("venue") or "unknown"),
                            symbol,
                            fc,
                            str(prov.get("scenario") or enriched_signal.get("kind") or "unknown").lower(),
                            str(prov.get("regime") or meta.get("regime") or "na").lower(),
                            (prov.get("risk_horizon_bucket") or "unknown").lower(),
                            (enriched_signal.get("atr_policy_layer") or "stop_ttl"),
                            int(prov.get("policy_ver") or 0),
                            "deny",
                            portfolio_reason,
                            portfolio_diag
                        ))
                    except Exception as e2:
                        logger.error("Failed scheduling portfolio event record: %s", e2)

            enriched_signal.setdefault("meta", {})
            enriched_signal["meta"]["portfolio_gate"] = {
                "allow": portfolio_allow,
                "reason_code": portfolio_reason,
                "diag": portfolio_diag,
                "advisory": port_is_advisory,
            }
            order_payload.setdefault("meta", {})
            order_payload["meta"]["portfolio_gate"] = enriched_signal["meta"]["portfolio_gate"]

            if not portfolio_allow and not port_is_advisory:
                logger.warning(
                    "🛡️ [PORTFOLIO-GATE] Signal blocked sid=%s symbol=%s reason=%s diag=%s",
                    sid, symbol, portfolio_reason, portfolio_diag
                )
                await self.send_telegram_report(
                    f"🛡️ <b>[PORTFOLIO-GATE] DENIED</b>\nSymbol: {symbol}\nReason: {portfolio_reason}",
                    source="portfolio_gate",
                    symbol=symbol,
                )
                return  # Block actual order route

            # Phase 5.7: hard regime/stress gate
            regime_stress_allow = True
            regime_stress_reason = "ATR_REGIME_STRESS_NOT_CHECKED"
            regime_stress_diag = {}
            rs_is_advisory = self._cached_regime_stress_advisory
            rs_is_enabled = self._cached_regime_stress_enable

            if rs_is_enabled and (not is_virtual_flag or mirror_all):
                try:
                    from services.atr_policy_regime_stress_gate import PolicyRegimeStressGate
                    rs_gate = PolicyRegimeStressGate(redis_client=self.publisher.r)
                    regime_stress_allow, regime_stress_reason, regime_stress_diag = rs_gate.validate(enriched_signal, {})
                except Exception as e:
                    logger.error("Regime stress gate error sid=%s: %s", sid, e)
                    # Master fail-closed toggle
                    fail_policy = self._cached_regime_stress_fail_policy
                    regime_stress_allow = (fail_policy == "OPEN")
                    regime_stress_reason = f"ATR_POLICY_REGIME_STRESS_GATE_ERROR:{e}"

            enriched_signal.setdefault("meta", {})
            enriched_signal["meta"]["regime_stress_gate"] = {
                "allow": regime_stress_allow,
                "reason_code": regime_stress_reason,
                "diag": regime_stress_diag,
                "advisory": rs_is_advisory,
            }
            order_payload.setdefault("meta", {})
            order_payload["meta"]["regime_stress_gate"] = enriched_signal["meta"]["regime_stress_gate"]
            # Carry over effective_risk_pct if clipped
            if "effective_risk_pct" in enriched_signal:
                order_payload["effective_risk_pct"] = enriched_signal["effective_risk_pct"]

            if not regime_stress_allow and not rs_is_advisory:
                logger.warning(
                    "🛡️ [REGIME-STRESS-GATE] Signal blocked sid=%s symbol=%s reason=%s diag=%s",
                    sid, symbol, regime_stress_reason, regime_stress_diag
                )
                await self.send_telegram_report(
                    f"🛡️ <b>[REGIME-STRESS-GATE] DENIED</b>\nSymbol: {symbol}\nReason: {regime_stress_reason}",
                    source="regime_stress_gate",
                    symbol=symbol,
                )
                return

            # Phase 7: Formal Invariants Engine (Runtime)
            try:
                from services.atr_invariant_runtime_engine import get_runtime_engine
                inv_engine = get_runtime_engine()
                # Run invariant checks
                inv_allow, inv_violations = inv_engine.validate_signal(order_payload)

                # Attach meta for tracing
                enriched_signal.setdefault("meta", {})
                enriched_signal["meta"]["invariant_gate"] = {
                    "allow": inv_allow,
                    "violations": inv_violations,
                }
                order_payload.setdefault("meta", {})
                order_payload["meta"]["invariant_gate"] = enriched_signal["meta"]["invariant_gate"]

                if not inv_allow:
                    logger.warning(
                        "🛑 [INVARIANT-GATE] Signal blocked sid=%s symbol=%s violations=%s",
                        sid, symbol, inv_violations
                    )
                    await self.send_telegram_report(
                        f"🛑 <b>[INVARIANT-GATE] CRITICAL DENIED</b>\nSymbol: {symbol}\nViolations: {len(inv_violations)}",
                        source="invariant_gate",
                        symbol=symbol,
                    )
                    return
            except Exception as e:
                logger.error("Invariant engine error sid=%s: %s", sid, e)

            await self.publisher.r.rpush(
                binance_queue,
                json.dumps(order_payload, ensure_ascii=False),
            )
            mode_tag = "MIRROR-ALL" if mirror_all else "VIRTUAL"
            logger.info(
                "🚀 [BINANCE-%s] (%s) Order pushed to %s sid=%s side=%s entry=%.2f conf=%.0f%% status=%s",
                mode_tag, symbol, binance_queue, sid, direction, entry, confidence * 100.0, validation_status,
            )
        except Exception as exc:
            logger.warning("⚠️ [BINANCE-VIRTUAL] (%s) Failed to push to binance queue: %s", symbol, exc)



    async def send_telegram_report(self, text: str, source: str = "report", symbol: str = "") -> None:
        """Send arbitrary report text to Telegram via notify stream (type=report)."""
        try:
            ts_ms = str(get_ny_time_millis())
            fields = {
                "type": "report",
                "text": (text or ""),
                "source": (source or "report"),
                "symbol": (symbol or ""),
                "ts_ms": ts_ms,
            }
            # notify_worker.py handles type=report with priority
            await self.publisher.r.xadd(
                self.notify_stream,
                fields=fields,
                maxlen=int(getattr(self, "notify_maxlen", 20000) or 20000),
                approximate=True,
            )
            logger.info("📱 [TELEGRAM-REPORT] sent: source=%s symbol=%s", fields["source"], fields["symbol"])
        except Exception as exc:
            logger.warning("⚠️ [TELEGRAM-REPORT] failed: %s", exc)

    def _get_rocket_multiplier(self, symbol: str) -> float:
        """
        Возвращает множитель для TP1 в профиле rocket_v1.
        Ищет в ENV: ROCKET_TP1_ATR_MULT_{SYMBOL} (напр. ROCKET_TP1_ATR_MULT_BTCUSDT)
        Fallback: ROCKET_TP1_ATR_MULT (дефолт 1.2)
        
        ✅ NEW: Добавлен clamp(0.5..10.0) и логгирование некорректных значений.
        """
        env_var = f"ROCKET_TP1_ATR_MULT_{symbol.upper()}"
        val = os.getenv(env_var)
        source = env_var

        if not val:
            val = os.getenv("ROCKET_TP1_ATR_MULT", "1.2")
            source = "ROCKET_TP1_ATR_MULT"

        try:
            m = float(val)
        except (ValueError, TypeError):
            logger.warning("⚠️ Некорректное значение множителя %s=%r. Используем дефолт 1.2", source, val)
            return 1.2

        # Clamp 0.5 .. 10.0
        if m < 0.5 or m > 10.0:
            logger.warning("⚠️ Множитель %s=%.2f вне диапазона (0.5..10.0). Применяем clamp.", source, m)
            m = max(0.5, min(10.0, m))

        return m

    def _sigmoid_abs(self, x: float, k: float = 1.0) -> float:
        """
        Sigmoid activation for absolute value:
        Maps [0, inf) -> [0, 1)
        """
        ax = abs(x)
        return 1.0 - (1.0 / (1.0 + k * ax))

    def _build_mix_dict(
        self,
        delta: float,
        delta_z: float,
        indicators: dict[str, Any] | None,
        confirmations: Sequence[str],
    ) -> dict[str, float]:
        mix: dict[str, float] = {}

        # ✅ FIX: delta_z can be 0.0 (valid z-score), don't check for None only
        if delta_z is not None:
            mix["p_delta"] = self._sigmoid_abs(delta_z, k=0.5)
            mix["p_speed"] = abs(delta_z)
        # ✅ FIX: delta can be 0.0 (valid delta), don't check for None only
        if delta is not None:
            mix["delta"] = abs(delta)
        if indicators:
            if "obi" in indicators:
                mix["p_cluster"] = float(indicators.get("obi") or 0.0)
            if "confidence" in indicators:
                mix["confidence"] = float(indicators.get("confidence") or 0.0)

        if confirmations:
            mix["confirmations_count"] = float(len(confirmations))

        return mix

    def _normalize_trailing_flag(self, value: Any, symbol: str | None = None) -> bool:
        """
        Возвращает финальный флаг трейлинга.
        """
        explicit_flag: bool | None = None
        if value is not None:
            try:
                explicit_flag = value.lower() in ("1", "true", "yes", "on") if isinstance(value, str) else bool(value)
            except Exception:
                explicit_flag = False

        # Глобальный флаг FORCE_TRAIL_AFTER_TP1 мы берем из env
        # Но в SignalPipeline у нас нет прямого доступа к self.force_trail_after_tp1 как в сервисе
        # Предполагаем, что он передается или читаем из env каждый раз (это не критично)
        force_trail = self._cached_force_trail

        # Spec trailing is harder without runtime.spec access easily, but let's assume default false for now
        # or rely on runtime.calibrated_specs if available.
        # Actually logic in Strategy used `self._env_bool` which reads env.

        if explicit_flag is not None:
            return explicit_flag

        return force_trail

    def _calculate_levels(
        self,
        runtime: SymbolRuntime,
        entry: float,
        side: str,
        indicators: dict[str, Any],
        trail_profile: str | None = None,
    ) -> tuple[float, list[float], float, float, dict[str, Any]]:
        cfg = runtime.config
        # Profile overlay: if TradeProfileRouter injected per-class params,
        # merge them into an effective cfg so all downstream cfg.get() calls
        # automatically pick up profile-based stop_atr_mult, tp_rr, tp1_atr_mult.
        _p_stop = indicators.get("profile_stop_atr_mult")
        _p_tp_rr = indicators.get("profile_tp_rr")
        _p_tp1 = indicators.get("profile_tp1_atr_mult")
        if _p_stop is not None or _p_tp_rr is not None or _p_tp1 is not None:
            cfg = {**cfg}  # shallow copy to avoid mutating runtime.config
            if _p_stop is not None:
                cfg["stop_atr_mult"] = float(_p_stop)
            if _p_tp_rr is not None:
                cfg["tp_rr"] = str(_p_tp_rr)
            if _p_tp1 is not None:
                cfg["tp1_atr_mult"] = float(_p_tp1)
        atr = float(indicators.get("atr", 0.0) or 0.0)
        atr_meta: dict[str, Any] = {}
        atr_ts_ms = 0
        # Use canonical TF resolver (single source of truth)
        atr_tf = runtime.get_atr_tf_selected()
        indicators["atr_tf_used"] = atr_tf
        # Prefer cache + sanity selection when atr not provided by signal
        if atr <= 0:
            try:
                # Deterministic-ish "now" for age calculation:
                # prefer signal ts_ms if present; else wall time.
                nm = 0
                try:
                    nm = int(indicators.get("ts_ms", 0) or indicators.get("tick_ts", 0) or indicators.get("generated_at", 0) or 0)
                except Exception:
                    nm = 0
                prefer_src = ""
                try:
                    # check if we have a robust enough preference
                    if int(runtime.dynamic_cfg.get(DK.ATR_SRC_READY, 0) or 0) == 1:
                        prefer_src = str(runtime.dynamic_cfg.get(DK.ATR_SRC_PREF, "") or "")
                except Exception:
                    prefer_src = ""

                # Use injected ATR cache
                if self.atr_cache:
                    atr, atr_meta = self.atr_cache.get_with_meta(symbol=runtime.symbol, timeframe=atr_tf, now_ms=(nm if nm > 0 else None), prefer_src=prefer_src)
                    atr = float(atr or 0.0)
                    if isinstance(atr_meta, dict):
                        indicators["atr_src"] = str(atr_meta.get("src") or atr_meta.get("source") or "na")
                        indicators["atr_ts_ms"] = int(atr_meta.get("ts_ms", 0) or 0)
                        indicators["atr_age_ms"] = int(atr_meta.get("age_ms", 0) or 0)
                        indicators["atr_consistency"] = float(atr_meta.get("consistency", 1.0) or 1.0)
                        indicators["atr_cons_ok"] = int(atr_meta.get("cons_ok", 1) or 1)
                        indicators["atr_candidates_n"] = int(atr_meta.get("candidates_n", 0) or 0)
                        if prefer_src:
                            indicators["atr_src_prefer"] = prefer_src
            except Exception:

                atr = 0.0

        # Always expose atr_bps_exec for unified gates/debug
        try:
            if entry > 0 and atr > 0:
                indicators["atr_bps_exec"] = 10000.0 * (atr / entry)
        except Exception:
            pass

        # Final ATR fallback (absolute last resort)
        if atr <= 0:
            symbol_fallbacks = {
                "BTCUSDT": 30.0,
                "ETHUSDT": 4.0,
                "BNBUSDT": 0.5,
                "SOLUSDT": 0.3,
            }
            atr = symbol_fallbacks.get(runtime.symbol, entry * 0.0003)
            indicators["atr_src"] = "fallback-symbol"
            indicators["atr_sanity_reason"] = "no_valid_atr_found"
            indicators["atr_sanity_ok"] = 1

        # Persist the final ATR used for SL/TP level calculation.
        # This is the ATR TF value (e.g. 5m/15m), NOT the 1m ATR from tick stream.
        # trade_metrics_service uses this for correct ATR normalization in reports.
        indicators["atr_used_for_levels"] = atr

        lot = indicators.get("lot")
        if lot is None:
            lot = indicators.get("tick_qty") or indicators.get("delta") or 1.0
            lot = max(lot, cfg.get("min_lot", 0.01))

        # ⚡ HARD NOTIONAL CAP: prevent delta/tick_qty from inflating lot to market volume
        try:
            _max_notional = self._cached_max_notional_usd
            if _max_notional <= 0:
                _deposit = self._cached_account_deposit_usd
                _risk_pct = self._cached_risk_percent
                if 0 < _risk_pct < 0.5:
                    _risk_pct *= 100.0
                _notional_cap = self._cached_notional_leverage_cap
                _max_notional = _deposit * (_risk_pct / 100.0) * _notional_cap  # e.g. 100 * 5% * 100 = 500
            if entry > 0 and _max_notional > 0:
                _max_lot_by_notional = _max_notional / entry
                if lot > _max_lot_by_notional:
                    lot = _max_lot_by_notional
        except Exception:
            pass

        # RISK_MAX_QTY hard cap
        try:
            _risk_max_qty = self._cached_risk_max_qty
            if _risk_max_qty > 0 and lot > _risk_max_qty:
                lot = _risk_max_qty
        except Exception:
            pass

        def rr_levels(rr_str: str) -> list[float]:
            try:
                return [float(x.strip()) for x in rr_str.split(",") if x.strip()]
            except Exception:
                return [1.3, 2.0, 2.7]

        # Проверяем профиль трейлинга до расчета SL
        if not trail_profile:
            trail_profile = cfg.get("trail_profile") or indicators.get("trail_profile") or cfg.get("default_trail_profile", "protective_only")

        if (cfg.get("stop_mode", "ATR")).upper() == "ATR":
            base_stop_mult = cfg.get("stop_atr_mult", 1.2)
            if trail_profile == "expansion_v1":
                base_stop_mult = max(base_stop_mult, 2.5)
                indicators["expansion_sl_widened"] = 1
                indicators["expansion_sl_mult_used"] = round(base_stop_mult, 2)
            stop_dist = atr * base_stop_mult
        elif (cfg.get("stop_mode", "ATR")).upper() == "PCT":
            stop_dist = entry * cfg.get("stop_pct", 0.2) / 100
        else:
            stop_dist = cfg.get("stop_points", 1.0)

        # ⚠️ REMOVED: tp1_offset_atr was incorrectly used as SL multiplier.
        # tp1_offset_atr is a TP1 trailing offset (0.1-0.17), NOT an SL mult.
        # Using it as SL produced stop_dist = ATR * 0.111 ≈ 1.2 bps — pure noise.
        # See: implementation_plan.md (2026-04-25) Root Cause #1.

        # SL ATR mult floor: never less than SL_ATR_MULT_FLOOR (industry minimum)
        _sl_atr_floor = self._cached_sl_atr_mult_floor
        if atr > 0 and stop_dist > 0:
            _actual_sl_mult = stop_dist / atr
            if _actual_sl_mult < _sl_atr_floor:
                indicators["sl_atr_mult_floored"] = 1
                indicators["sl_atr_mult_original"] = round(_actual_sl_mult, 4)
                stop_dist = atr * _sl_atr_floor

        # ------------------------------------------------------------------
        # LIQMAP Adaptive SL: widen stop_dist if an adverse liquidation cluster
        # sits INSIDE the current SL band.
        #
        # Philosophy: "never place SL inside a liq cluster — clusters act as magnets."
        # SL is NEVER tightened. Only widened. Fail-open if data missing.
        #
        # Modes (ADAPTIVE_SL_MODE env):
        #   OFF     — diagnostics only, SL unchanged (legacy behaviour)
        #   SHADOW  — diagnostics + log what would happen, SL unchanged
        #   ENFORCE — diagnostics + actually widen stop_dist behind cluster
        # ------------------------------------------------------------------
        _baseline_stop_dist = stop_dist  # snapshot before any adaptive change
        try:
            _adaptive_mode = (os.environ.get('ADAPTIVE_SL_MODE', 'OFF') or 'OFF').upper()
            _buffer_bps    = float(os.environ.get('ADAPTIVE_SL_BUFFER_BPS', '5.0') or 5.0)
            _max_widen_x   = float(os.environ.get('ADAPTIVE_SL_MAX_WIDEN_X', '2.0') or 2.0)

            base_sl_bps = (stop_dist / entry) * 10000.0 if entry > 0.0 else 0.0
            indicators['liqmap_sl_base_bps'] = float(base_sl_bps)

            side_u = side.upper()
            if side_u == 'LONG':
                reco_bps = float(indicators.get('liqmap_sl_reco_bps_long') or indicators.get('liqmap_sl_reco_bps') or 0.0)
            else:
                reco_bps = float(indicators.get('liqmap_sl_reco_bps_short') or indicators.get('liqmap_sl_reco_bps') or 0.0)

            indicators['liqmap_sl_reco_bps'] = reco_bps

            if base_sl_bps > 0.0 and reco_bps > 0.0:
                ratio = reco_bps / float(base_sl_bps)
                indicators['liqmap_sl_widen_ratio'] = ratio
                cap = self._cached_liqmap_sl_widen_cap
                indicators['liqmap_sl_widen_needed'] = 1 if ratio > cap else 0
            else:
                indicators['liqmap_sl_widen_ratio'] = 0.0
                indicators['liqmap_sl_widen_needed'] = 0

            # Adaptive SL logic: cluster is inside SL band → push SL past it
            indicators['adaptive_sl_applied'] = 0
            indicators['adaptive_sl_reco_bps'] = 0.0
            indicators['adaptive_sl_widen_bps'] = 0.0

            if _adaptive_mode in ('SHADOW', 'ENFORCE') and reco_bps > 0.0 and base_sl_bps > 0.0 and reco_bps < base_sl_bps:
                # Target: just past the cluster + buffer, capped at max_widen_x × baseline
                    adjusted_bps = reco_bps + _buffer_bps
                    adjusted_bps = min(adjusted_bps, base_sl_bps * _max_widen_x)
                    adjusted_dist = (adjusted_bps / 10000.0) * entry if entry > 0 else 0.0

                    indicators['adaptive_sl_reco_bps'] = round(adjusted_bps, 2)
                    indicators['adaptive_sl_widen_bps'] = round(adjusted_bps - base_sl_bps, 2)

                    if adjusted_dist > stop_dist:
                        indicators['adaptive_sl_applied'] = 1
                        if _adaptive_mode == 'ENFORCE':
                            stop_dist = adjusted_dist  # widen — never tighten (max guaranteed below)

                    indicators['adaptive_sl_mode'] = _adaptive_mode

                    # SHADOW/ENFORCE: log to info so it's visible in prod
                    logger.info(
                        "🎯 [ADAPTIVE-SL] mode=%s symbol=%s side=%s "
                        "base_bps=%.1f cluster_bps=%.1f adjusted_bps=%.1f applied=%d",
                        _adaptive_mode, runtime.symbol, side_u,
                        base_sl_bps, reco_bps, adjusted_bps,
                        indicators['adaptive_sl_applied']
                    )

            # Safety invariant: stop_dist must never be smaller than baseline
            stop_dist = max(stop_dist, _baseline_stop_dist)

        except Exception:
            # Fail-open: restore baseline and clear adaptive indicators
            stop_dist = _baseline_stop_dist
            indicators['adaptive_sl_applied'] = 0


        # Для rocket_v1 и expansion_v1: TP1 = MULT * ATR, остальные TP через RR
        rocket_mult_raw = self._get_rocket_multiplier(runtime.symbol)

        if trail_profile == "expansion_v1":
            rocket_mult = max(2.5, rocket_mult_raw)
            indicators["expansion_tp_widened"] = 1
            indicators["expansion_tp_mult_used"] = round(rocket_mult, 2)
        else:
            # ✅ ENFORCE: TP1 floor is 0.6 ATR to guarantee profitability after fees (User Req 1)
            rocket_mult = max(0.6, rocket_mult_raw)

        is_rocket_v1 = (trail_profile in ["rocket_v1", "expansion_v1"])

        # Логируем для отладки (sample every 10000th message)
        if is_rocket_v1:
            tp1_dist = atr * rocket_mult
            rocket_v1_sampler = LogSamplerFactory.get_sampler("ROCKET_V1", 10000)
            if rocket_v1_sampler.should_log(f"rocket_v1_{runtime.symbol}"):
                logger.info("🎯 %s detected in _calculate_levels: symbol=%s, atr=%.2f, mult=%.2f, tp1_dist=%.2f",
                           trail_profile, runtime.symbol, atr, rocket_mult, tp1_dist)
        else:
            logger.debug("_calculate_levels: trail_profile=%s (not rocket_v1), will use RR method", trail_profile)

        if side.upper() == "LONG":
            sl = entry - stop_dist
            tp1_dist = atr * rocket_mult if is_rocket_v1 else stop_dist * rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))[0]

            # Base calculation
            if is_rocket_v1:
                # TP1 = mult ATR
                tp1 = entry + tp1_dist
                rr_list = rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))

                # Default RR-based potential TPs
                tp2_potential = entry + stop_dist * (rr_list[1] if len(rr_list) > 1 else 2.0)
                tp3_potential = entry + stop_dist * (rr_list[2] if len(rr_list) > 2 else 2.7)

                # Enforce monotonicity: TP2/TP3 must be significantly further than TP1
                # If TP1 is very far (high rocket_mult), scale TP2/TP3 relative to TP1
                # Strategy: TP2 >= max(RR_based, TP1_dist * 1.5)
                #           TP3 >= max(RR_based, TP1_dist * 2.0)
                tp2_dist = max(tp2_potential - entry, tp1_dist * 1.5)
                tp3_dist = max(tp3_potential - entry, tp1_dist * 2.0)

                tp2 = entry + tp2_dist
                tp3 = entry + tp3_dist

                tps = [tp1, tp2, tp3]
                logger.debug(
                    "✅ rocket_v1 LONG adjusted: TP1=%.2f (%.2f ATR), TP2=%.2f (%.2f ATR), TP3=%.2f (%.2f ATR)",
                    tp1, tp1_dist/atr, tp2, tp2_dist/atr, tp3, tp3_dist/atr
                )
            else:
                # Standard RR logic
                tps = [entry + stop_dist * rr for rr in rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))]

        else: # SHORT
            sl = entry + stop_dist
            tp1_dist = atr * rocket_mult if is_rocket_v1 else stop_dist * rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))[0]

            if is_rocket_v1:
                # TP1 = mult ATR
                tp1 = entry - tp1_dist
                rr_list = rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))

                # Default RR-based potential TPs
                tp2_potential = entry - stop_dist * (rr_list[1] if len(rr_list) > 1 else 2.0)
                tp3_potential = entry - stop_dist * (rr_list[2] if len(rr_list) > 2 else 2.7)

                # Enforce monotonicity for SHORT (distances are positive)
                # Strategy: TP2_dist >= max(RR_dist, TP1_dist * 1.5)
                tp2_dist = max(entry - tp2_potential, tp1_dist * 1.5)
                tp3_dist = max(entry - tp3_potential, tp1_dist * 2.0)

                tp2 = entry - tp2_dist
                tp3 = entry - tp3_dist

                tps = [tp1, tp2, tp3]
                logger.debug(
                    "✅ rocket_v1 SHORT adjusted: TP1=%.2f (%.2f ATR), TP2=%.2f (%.2f ATR), TP3=%.2f (%.2f ATR)",
                    tp1, tp1_dist/atr, tp2, tp2_dist/atr, tp3, tp3_dist/atr
                )
            else:
                # Standard RR logic
                tps = [entry - stop_dist * rr for rr in rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))]

        # ------------------------------------------------------------------
        # LIQMAP TP/SL Levels Overlay (Phase D3)
        # Uses injected liqmap features to adjust SL/TP based on liquidation
        # cluster positions. Modes: OFF / SHADOW / ENFORCE.
        # ------------------------------------------------------------------
        try:
            _levels_mode = self._cached_liqmap_levels_mode
            if _levels_mode in ('SHADOW', 'ENFORCE') and float(indicators.get('liqmap_ok', 0)) > 0:
                from services.orderflow.liqmap_features import apply_liqmap_tp_sl_adjustment
                _new_sl, _new_tp1, _liqmap_patch = apply_liqmap_tp_sl_adjustment(
                    side=side,
                    entry=entry,
                    base_sl=sl,
                    base_tp1=float(tps[0]) if tps else entry,
                    indicators=indicators,
                    window="1h",
                    min_usd=self._cached_liqmap_levels_min_usd,
                    buffer_bps=self._cached_liqmap_levels_buffer_bps,
                    max_sl_widen_bps=self._cached_liqmap_levels_max_sl_widen_bps,
                    enable_tp1=True,
                    enable_sl=True,
                )
                # Always inject diagnostic patch into indicators
                indicators.update(_liqmap_patch)

                if _levels_mode == 'ENFORCE':
                    # Apply adjusted SL (only widen, never tighten)
                    if side.upper() == 'LONG' and _new_sl < sl:
                        sl = _new_sl
                        stop_dist = abs(entry - sl)
                    elif side.upper() == 'SHORT' and _new_sl > sl:
                        sl = _new_sl
                        stop_dist = abs(entry - sl)

                    # Apply adjusted TP1 (replace first TP, keep rest)
                    if tps and abs(_new_tp1 - float(tps[0])) > 1e-12:
                        tps[0] = _new_tp1

                # Log for observability
                if float(_liqmap_patch.get('liqmap_levels_applied', 0)) > 0:
                    logger.info(
                        "🗺️ [LIQMAP-LEVELS] mode=%s symbol=%s side=%s "
                        "sl_adj_bps=%.1f tp1_adj_bps=%.1f reason=%s",
                        _levels_mode, runtime.symbol, side,
                        float(_liqmap_patch.get('liqmap_sl_adj_bps', 0)),
                        float(_liqmap_patch.get('liqmap_tp1_adj_bps', 0)),
                        _liqmap_patch.get('liqmap_levels_reason', 'unknown'),
                    )
        except Exception as _liqmap_exc:
            # Fail-open: leave SL/TP unchanged
            indicators['liqmap_levels_error'] = str(_liqmap_exc)[:200]

        # FINAL SAFETY: Sort TPs by distance from entry to guarantee order 1 < 2 < 3
        # abs(tp - entry) makes it direction-agnostic
        tps.sort(key=lambda x: abs(x - entry))

        if atr > 0 and entry > 0:
            indicators["sl_atr"] = abs(entry - sl) / atr
            indicators["tp1_atr"] = abs(float(tps[0]) - entry) / atr
            indicators["sl_atr_mult"] = stop_dist / atr
            indicators["tp1_atr_mult"] = abs(float(tps[0]) - entry) / atr

        return sl, tps, lot, atr, atr_meta


