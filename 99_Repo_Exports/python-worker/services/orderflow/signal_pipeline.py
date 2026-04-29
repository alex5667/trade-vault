from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import logging
import os
import time
import json
import math
from typing import Any, Dict, List, Optional, Tuple, Sequence, Union
from types import SimpleNamespace

from common.contracts.registry import SignalV1, OrderIntentV1
from common.normalization import normalize_side_3, normalize_side_3_safe, generate_signal_id, Direction
from common.enums.trading import Direction, Side

from services.orderflow.runtime import SymbolRuntime
from services.async_signal_publisher import AsyncSignalPublisher, StreamSink
from services.tp_config import parse_tp_ratio
from core.signal_payload import SignalPayload, StrongGateDecision
from core.of_confirm_engine import OFConfirmEngine

# Imports for publishing logic
from services.pnl_math import calculate_position_size
from services.signal_preprocess import preprocess_signal_for_publish
from core.crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter
from core.dyn_cfg_keys import DynCfgKeys as DK
from core.redis_keys import RedisStreams as RS
from services.outbox.envelope_builder import build_outbox_envelope, dumps_env, build_trace_sidecar_meta_from_ctx
from services.outbox.atomic_outbox import atomic_xadd_async
from common.decision_trace import ensure_trace, trace_gate, trace_enabled
from core.instrument_config import get_specs
from utils.task_manager import safe_create_task
from services.orderflow.decision_snapshot import build_decision_snapshot, publish_decision_snapshot

# Metrics
from services.orderflow.metrics import (
    signals_total, strong_gate_veto_total, pre_publish_veto_total, of_session_outcome_total
    , liq_geom_monitor_hit_total, liq_geom_tighten_total, liq_geom_veto_total
    , liq_geom_dws_bps, liq_geom_book_slope_min_usd_per_bps, liq_geom_recovery_time_ms
    , flow_toxic_monitor_hit_total, flow_toxic_tighten_total, flow_toxic_veto_total
    , flow_toxic_ofi_norm_z, flow_toxic_vpin_cdf
    , manip_gate_events_total, of_inputs_publish_error_total
    , breadth_gate_veto_total
    , breadth_gate_shadow_veto_total
)
from services.orderflow.utils import session_utc
from handlers.crypto_orderflow.utils.log_sampler import LogSamplerFactory, sampled_info
from handlers.crypto_orderflow.utils.pre_publish_gates import HardDataQualityGate, RegimeSessionGate, AtrFloorGate, BreadthGate
from services.orderflow.breadth_context import aread_breadth_context
from services.orderflow.liquidation_context_worker import aread_liq_context
# P5: book sanity + stream integrity gates (pre-publish, fail-open)
from services.orderflow.book_sanity_gate import BookSanityGate
from services.orderflow.stream_integrity_gate import StreamIntegrityGate
from services.orderflow.exec_health_rollups import aread_exec_health_rollups, decide_exec_health_from_env
from services.orderflow.exec_health_observability import record_exec_health_observability, record_exec_health_reader_error
# P4: SLO contract state writer (fail-open, rate-limited flush)
from services.orderflow.exec_health_slo_contract import (
    record_exec_health_contract_state,
    record_exec_health_contract_reader_error as _contract_reader_err_pipeline,
    flush_exec_health_contract_state_async as _flush_contract_pipeline,
)
# P4 latency contract: stamp emit time and observe feature_to_emit + end_to_end_event
from services.observability.latency_contract import stamp_emit_and_observe_async
from services.observability.latency_semconv import ensure_epoch_ms_fields, FIELD_TS_EVENT_MS, FIELD_TS_FEATURE_MS, FIELD_TS_EMIT_MS
# P6: hard consumer hook — convert P5 autoguard freeze key into real publish stop
from services.orderflow.exec_health_freeze_hook import (
    aread_exec_health_auto_freeze,
    build_exec_health_auto_freeze_decision,
)

from services.orderflow.derivatives_context import aread_derivatives_context
from services.orderflow.derivatives_context_gate import evaluate_derivatives_context_v2
from services.orderflow.metrics_derivatives_context import (
    deriv_ctx_snapshot_age_ms,
    deriv_ctx_funding_rate_z,
    deriv_ctx_basis_bps,
    deriv_ctx_oi_notional_usd,
    deriv_ctx_gate_monitor_hit_total,
    deriv_ctx_gate_tighten_total,
    deriv_ctx_gate_veto_total,
    deriv_ctx_tighten_add_bps,
    deriv_ctx_missing_total,
)

from services.orderflow.defillama_context import aread_defillama_context
from services.orderflow.defillama_context_gate import evaluate_defillama_context
from services.orderflow.metrics_defillama_context import (
    defillama_ctx_snapshot_age_ms,
    defillama_ctx_missing_total,
    defillama_ctx_gate_monitor_hit_total,
    defillama_ctx_gate_tighten_total,
    defillama_ctx_gate_veto_total,
    defillama_ctx_dex_volume_spike_z,
    defillama_ctx_chain_tvl_delta_1d_pct,
)

from services.orderflow.crossvenue_context import aread_crossvenue_context
from services.orderflow.crossvenue_context_gate import evaluate_crossvenue_context
from services.orderflow.metrics_crossvenue_context import (
    crossvenue_ctx_missing_total,
    crossvenue_ctx_stale_total,
    crossvenue_ctx_gate_monitor_hit_total,
    crossvenue_ctx_gate_tighten_total,
    crossvenue_ctx_gate_veto_total,
    crossvenue_ctx_mid_spread_bps as _cv_mid_spread_bps_gauge,
    crossvenue_ctx_direction_agree as _cv_direction_agree_gauge,
    crossvenue_ctx_dislocation_z as _cv_dislocation_z_gauge,
    crossvenue_ctx_snapshot_age_ms as _cv_snapshot_age_ms_hist,
)

from services.orderflow.sentiment_context import aread_sentiment_context
from services.orderflow.sentiment_context_gate import evaluate_sentiment_context
from services.orderflow.metrics_sentiment_context import (
    sentiment_ctx_missing_total,
    sentiment_ctx_stale_total,
    sentiment_ctx_gate_monitor_hit_total,
    sentiment_ctx_gate_tighten_total,
    sentiment_risk_multiplier,
)


_book_sanity_gate = BookSanityGate.from_env()
_stream_integrity_gate = StreamIntegrityGate.from_env()

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
        self.of_inputs_publish_enabled = bool(int(os.getenv("OF_INPUTS_PUBLISH_ENABLED", "1") or 1))
        self.of_inputs_publish_strict = bool(int(os.getenv("OF_INPUTS_PUBLISH_STRICT", "0") or 0))
        # P1 fix: 100000 → 5000 (при 40KB/entry: 5k * 40KB = 200MB max)
        self.of_inputs_stream_maxlen = int(os.getenv("OF_INPUTS_STREAM_MAXLEN", "5000") or 5000)
        self._hard_dq_gate = HardDataQualityGate.from_env()
        self._rs_gate = RegimeSessionGate.from_env()
        self._atr_floor_gate = AtrFloorGate.from_env()
        self._breadth_gate = BreadthGate.from_env()
        self._rejected_signal_stream = os.getenv("CRYPTO_REJECTED_SIGNAL_STREAM", RS.CRYPTO_REJECTED)

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
        raw_syms = str(os.getenv("LIQ_GEOM_METRICS_SYMBOLS", "") or "").strip()
        self._liq_geom_syms_allow = {s.strip().upper() for s in raw_syms.split(",") if s.strip()} if raw_syms else set()

        raw_syms2 = str(os.getenv("FLOW_TOX_METRICS_SYMBOLS", os.getenv("LIQ_GEOM_METRICS_SYMBOLS", "")) or "").strip()
        self._flow_tox_syms_allow = {s.strip().upper() for s in raw_syms2.split(",") if s.strip()} if raw_syms2 else set()
        raw_syms3 = str(os.getenv("DERIV_CTX_METRICS_SYMBOLS", os.getenv("FLOW_TOX_METRICS_SYMBOLS", "")) or "").strip()
        self._deriv_ctx_syms_allow = {s.strip().upper() for s in raw_syms3.split(",") if s.strip()} if raw_syms3 else set()

        # TB Labeler Feed (P45 fix): explicitly feed signals:of:inputs
        self.publish_of_inputs = os.getenv("PUBLISH_OF_INPUTS", "1").lower() in {"1", "true", "yes", "on"}
        self.of_inputs_stream = os.getenv("OF_INPUTS_STREAM", RS.OF_INPUTS)

        # Virtual routing flags (1 source of truth at startup)
        self.binance_virtual_mirror_all = os.getenv("BINANCE_VIRTUAL_MIRROR_ALL", "0").lower() in {"1", "true", "yes", "on"}
        self.binance_virtual_orders_enabled = os.getenv("BINANCE_VIRTUAL_ORDERS_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
        
        # P0 FIX: Cache hot path variables to eliminate syscalls on signal publish
        self._cached_service_name = str(os.getenv("SERVICE_NAME", "python-worker"))
        self._cached_dq_book_stale_flag_ms = int(os.getenv("DQ_BOOK_STALE_FLAG_MS", "1500") or 1500)
        self._cached_dq_spread_wide_flag_bps = float(os.getenv("DQ_SPREAD_WIDE_FLAG_BPS", "12") or 12.0)
        
        self._cached_use_outbox = (
            os.getenv("CRYPTO_USE_OUTBOX_DISPATCHER", "0").lower() in {"1","true","yes","on"}
            or os.getenv("USE_SIGNAL_OUTBOX", "0").lower() in {"1","true","yes","on"}
        )
        self._cached_shadow_outbox = os.getenv("CRYPTO_SHADOW_OUTBOX", "0").lower() in {"1","true","yes","on"}
        self._cached_outbox_stream = os.getenv("SIGNAL_OUTBOX_STREAM", RS.SIGNAL_OUTBOX)
        self._cached_gate_mode = os.getenv("ATR_GATE_MODE", os.getenv("FEES_AWARE_GATE_MODE", "ENFORCE")).upper()
        
        self._cached_deriv_profile = str(os.getenv("DERIV_CTX_PROFILE", os.getenv("GATE_PROFILE", "default")) or "default").strip().lower()
        self._cached_deriv_ctx_funding_z = float(os.getenv("DERIV_CTX_FUNDING_Z_MAX", "3.0") or 3.0)
        self._cached_deriv_ctx_basis_bps = float(os.getenv("DERIV_CTX_BASIS_BPS_MAX", "10.0") or 10.0)
        self._cached_deriv_ctx_require_oi = bool(int(os.getenv("DERIV_CTX_REQUIRE_OI_FOR_VETO", "1") or 1))
        self._cached_deriv_ctx_tighten_mult = float(os.getenv("DERIV_CTX_TIGHTEN_ADD_MULT", "1.0") or 1.0)
        self._cached_deriv_ctx_tighten_cap = float(os.getenv("DERIV_CTX_TIGHTEN_ADD_CAP_BPS", "8.0") or 8.0)
        self._cached_min_conf_pct = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70"))
        
        self._cached_fees_bps_rt = float(os.getenv("FEES_BPS_RT", "10"))
        self._cached_tp_bps_buffer = float(os.getenv("TP_BPS_BUFFER", "4"))

        # P0 FIX: Cache LIQ_GATE, FLOW_TOXIC, MANIP and others
        self._cached_liq_profile = str(os.getenv("LIQ_GATE_PROFILE", os.getenv("GATE_PROFILE", "default")) or "default").strip().lower()
        self._cached_liq_min_book_slope = float(os.getenv("LIQ_MIN_BOOK_SLOPE", "0") or 0.0)
        self._cached_liq_max_dws_bps = float(os.getenv("LIQ_MAX_DWS_BPS", "0") or 0.0)
        self._cached_liq_max_recovery_ms = int(os.getenv("LIQ_MAX_RECOVERY_TIME_MS", "0") or 0)
        self._cached_liq_tighten_cap = float(os.getenv("LIQ_GEOM_TIGHTEN_ADD_CAP_BPS", "10.0") or 10.0)
        self._cached_liq_tighten_mult = float(os.getenv("LIQ_GEOM_TIGHTEN_ADD_MULT", "1.0") or 1.0)
        
        self._cached_flow_profile = str(os.getenv("FLOW_GATE_PROFILE", os.getenv("GATE_PROFILE", "default")) or "default").strip().lower()
        self._cached_flow_mode_override = str(os.getenv("FLOW_TOXIC_MODE", os.getenv("FLOW_TOX_MODE", "")) or "").strip().lower()
        self._cached_flow_thr_z = float(os.getenv("FLOW_OFI_NORM_Z_MAX", "0") or 0.0)
        self._cached_flow_thr_vpin = float(os.getenv("FLOW_VPIN_CDF_MAX", "0") or 0.0)
        self._cached_flow_cap = float(os.getenv("FLOW_TOX_TIGHTEN_ADD_CAP_BPS", "6.0") or 6.0)
        self._cached_flow_mult = float(os.getenv("FLOW_TOX_TIGHTEN_ADD_MULT", "1.0") or 1.0)
        self._cached_flow_veto_wo_tca = bool(int(os.getenv("FLOW_TOX_VETO_WITHOUT_TCA", "0") or 0))
        self._cached_flow_thr_is = float(os.getenv("EXEC_MAX_IS_P95_BPS", "0") or 0.0)
        self._cached_flow_thr_imp = float(os.getenv("EXEC_MAX_PERM_IMPACT_P95_BPS", "0") or 0.0)
        
        self._cached_manip_profile = str(os.getenv("MANIP_GATE_PROFILE", os.getenv("GATE_PROFILE", "default")) or "default").strip().lower()
        self._cached_manip_mode_override = str(os.getenv("MANIP_MODE", "") or "").strip().lower()
        self._cached_manip_thr_qs = float(os.getenv("MANIP_QUOTE_STUFF_SCORE_MAX", "0") or 0.0)
        self._cached_manip_thr_lay = float(os.getenv("MANIP_LAYERING_SCORE_MAX", "0") or 0.0)
        self._cached_manip_thr_otr_z = float(os.getenv("MANIP_OTR_Z_MAX", "0") or 0.0)
        self._cached_manip_tighten_cap = float(os.getenv("MANIP_TIGHTEN_ADD_CAP_BPS", "6.0") or 6.0)
        self._cached_manip_tighten_mult = float(os.getenv("MANIP_TIGHTEN_ADD_MULT", "1.0") or 1.0)

        # DefiLlama slow-context gate config
        self._cached_defillama_ctx_enabled = os.getenv("DEFILLAMA_CTX_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
        self._cached_defillama_ctx_profile = str(os.getenv("DEFILLAMA_CTX_PROFILE", "monitor") or "monitor").strip().lower()
        self._cached_defillama_ctx_tighten_mult = float(os.getenv("DEFILLAMA_CTX_TIGHTEN_ADD_MULT", "1.0") or 1.0)
        self._cached_defillama_ctx_tighten_cap = float(os.getenv("DEFILLAMA_CTX_TIGHTEN_ADD_CAP_BPS", "4.0") or 4.0)
        self._cached_defillama_ctx_max_age_ms = int(os.getenv("DEFILLAMA_CTX_MAX_AGE_MS", "7200000") or 7200000)

        # Sentiment context config (Fear & Greed)
        self._cached_sentiment_ctx_enabled = os.getenv("SENTIMENT_CTX_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
        self._cached_sentiment_profile = str(os.getenv("SENTIMENT_CTX_PROFILE", "monitor") or "monitor").strip().lower()
        self._cached_sentiment_max_age_ms = int(os.getenv("SENTIMENT_CTX_MAX_AGE_MS", "172800000") or 172800000)
        self._cached_sentiment_tighten_cap_bps = float(os.getenv("SENTIMENT_CTX_TIGHTEN_CAP_BPS", "2.0") or 2.0)

        # Cross-venue context gate config (Phase 0: disabled by default)
        self._cached_crossvenue_ctx_enabled = os.getenv("CROSSVENUE_CTX_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
        self._cached_crossvenue_profile = str(os.getenv("CROSSVENUE_CTX_PROFILE", "monitor") or "monitor").strip().lower()
        self._cv_profile_cache_val = self._cached_crossvenue_profile
        self._cv_profile_cache_ts_ms = 0
        self._cached_crossvenue_max_age_ms = int(os.getenv("CROSSVENUE_CTX_MAX_AGE_MS", "5000") or 5000)
        self._cached_crossvenue_min_agree = float(os.getenv("CROSSVENUE_CTX_MIN_AGREE", "0.67") or 0.67)
        self._cached_crossvenue_max_dislocation_z = float(os.getenv("CROSSVENUE_CTX_MAX_DISLOCATION_Z", "3.0") or 3.0)
        self._cached_crossvenue_max_mid_spread_bps = float(os.getenv("CROSSVENUE_CTX_MAX_MID_SPREAD_BPS", "8.0") or 8.0)
        self._cached_crossvenue_max_stale_count = int(os.getenv("CROSSVENUE_CTX_MAX_STALE_COUNT", "1") or 1)
        self._cached_crossvenue_tighten_mult = float(os.getenv("CROSSVENUE_CTX_TIGHTEN_ADD_MULT", "1.0") or 1.0)
        self._cached_crossvenue_tighten_cap = float(os.getenv("CROSSVENUE_CTX_TIGHTEN_ADD_CAP_BPS", "6.0") or 6.0)
        
        self._cached_binance_trail_atr_mult = os.getenv("BINANCE_TRAIL_ATR_MULT", "1.0")
        self._cached_range_tp_rr = os.getenv("RANGE_TP_RR", "1.0,1.5")

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
        self._cached_orders_mirror_queue = os.getenv("ORDERS_QUEUE_BINANCE_MIRROR", "orders:queue:binance:mirror")
        self._cached_orders_intent_queue = os.getenv("ORDERS_INTENT_BINANCE", "orders:intent:binance")
        self._cached_analytics_db_dsn = os.getenv("ANALYTICS_DB_DSN") or os.getenv("TRADES_DB_DSN") or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
        self._cached_exec_profile = str(os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
        self._cached_exec_health_tf = str(os.getenv("EXEC_HEALTH_TF", "all") or "all").strip().lower()
        self._cached_exec_health_venue = str(os.getenv("EXEC_HEALTH_VENUE", "binance") or "binance").strip().lower()
        self._cached_force_trail = os.getenv("FORCE_TRAIL_AFTER_TP1", "0").lower() in ("1", "true", "yes", "on")
        self._cached_max_notional_usd = float(os.getenv("MAX_NOTIONAL_USD", "0") or "0")
        self._cached_account_deposit_usd = float(os.getenv("ACCOUNT_DEPOSIT_USD", "100") or "100")
        self._cached_notional_leverage_cap = float(os.getenv("NOTIONAL_LEVERAGE_CAP", "100") or "100")
        self._cached_risk_max_qty = float(os.getenv("RISK_MAX_QTY", "0") or "0")
        self._cached_liqmap_sl_widen_cap = float(os.getenv("LIQMAP_SL_WIDEN_CAP", "1.25") or 1.25)
        self._cached_sl_atr_mult_floor = float(os.getenv("SL_ATR_MULT_FLOOR", "0.78") or 0.78)

    def _record_of_inputs_publish_error(self, *, symbol: str, path: str, stream: str, exc: Exception) -> None:
        try:
            of_inputs_publish_error_total.labels(
                symbol=str(symbol),
                stream=str(stream),
                path=str(path),
            ).inc()
        except Exception:
            pass
        logger.error(
            "❌ (%s) Failed to publish to %s via %s path: %s",
            symbol,
            stream,
            path,
            exc,
        )

    async def _publish_of_inputs(self, *, publisher: AsyncSignalPublisher, enriched_signal: Dict[str, Any], symbol: str, path: str) -> None:
        try:
            await publisher.xadd_json(
                sink=StreamSink(name=str(self.of_inputs_stream), field="payload", maxlen=self.of_inputs_stream_maxlen),
                payload=enriched_signal,
                symbol=str(symbol),
            )
        except Exception as exc:
            self._record_of_inputs_publish_error(
                symbol=str(symbol),
                path=str(path),
                stream=str(self.of_inputs_stream),
                exc=exc,
            )
            if self.of_inputs_publish_strict:
                raise

    @property
    def FEES_BPS_RT(self) -> float:
        return self._cached_fees_bps_rt
    
    @property
    def TP_BPS_BUFFER(self) -> float:
        return self._cached_tp_bps_buffer

    def _conf_scores_enabled(self) -> bool:
        return bool(self.conf_scores_publish_enabled)

    def _safe_num(self, v: object) -> float | None:
        try:
            f = float(v)  # type: ignore[arg-type]
        except Exception:
            return None
        if not math.isfinite(f):
            return None
        return float(f)

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
                out[k] = float(fv)
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
                    out[k] = float(fv)

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

        self._cv_profile_cache_val = self._cached_crossvenue_profile
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
        return float(raw_f), float(final_f) if final_f is not None else None

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
                "schema_version": int(self.conf_scores_schema_version),
                "producer": self._cached_service_name,
                "sid": str(sid),
                "symbol": str(symbol),
                "ts_event_ms": int(ts_event_ms),
                "confidence_raw": float(raw),
                "confidence_final": float(final) if final is not None else None,
                "evidence_map": evidence_map,
            }
            if self.conf_scores_include_evidence_json:
                # Full evidence (heavy) - keep disabled unless needed.
                evt["evidence_json"] = evidence_dict

            await self.publisher.xadd_json(
                sink=StreamSink(name=self.conf_scores_stream, field="payload", maxlen=self.conf_scores_stream_maxlen),
                payload=evt,
                symbol=str(symbol),
            )

        except Exception as e:
            # Best-effort quarantine - never block signal publishing.
            try:
                q = {
                    "ts_event_ms": int(ts_event_ms),
                    "sid": str(sid),
                    "symbol": str(symbol),
                    "error": str(e),
                }
                await self.publisher.xadd_json(
                    sink=StreamSink(name=self.conf_scores_quarantine_stream, field="payload", maxlen=self.conf_scores_quarantine_maxlen),
                    payload=q,
                    symbol=str(symbol),
                )
            except Exception:
                pass



    def _build_gate_ctx(self, runtime: SymbolRuntime, signal: Dict[str, Any], sig_ts_ms: int) -> SimpleNamespace:
        """Build a minimal ctx object compatible with pre_publish_gates.* (fail-open)."""
        micro = signal.get("micro") if isinstance(signal.get("micro"), dict) else {}
        indicators = signal.get("indicators") if isinstance(signal.get("indicators"), dict) else {}

        # data-quality flags (prepared by preprocess_signal_for_publish + additional cheap hints)
        flags = []
        if isinstance(signal.get("data_quality_flags"), list):
            flags.extend([str(x) for x in signal.get("data_quality_flags") if x is not None])

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
            s = str(x or "").strip().lower()
            if not s or s in seen:
                continue
            seen.add(s)
            dq_flags.append(s)

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
            ts_event_ms=int(sig_ts_ms),
            ts_ms=int(sig_ts_ms),
            ts=int(sig_ts_ms),
            spread_bps=float(micro.get("spread_bps") or getattr(runtime, "last_spread_bps", 0.0) or 0.0),
            regime=str(indicators.get("regime") or signal.get("regime") or "unknown"),
            session=str(signal.get("session") or indicators.get("session") or "na"),
            tf=str(signal.get("tf") or indicators.get("tf") or "na"),
            venue=str(signal.get("venue") or indicators.get("venue") or "binance"),
            touch_is_stale=bool(signal.get("touch_is_stale") or indicators.get("touch_is_stale") or False),
            data_quality_flags=dq_flags,
            of=of,
            redis=getattr(runtime, "redis_client", None),
        )
        return ctx

    async def publish_signal(self, runtime: SymbolRuntime, signal: Dict[str, Any]) -> None:
        """
        Публикация сигнала в необходимые каналы.
        """
        from services.orderflow.metrics import strong_gate_veto_total

        # Next level:
        #   Optionally route ALL publications through SignalDispatcher outbox
        #   to unify idempotency, per-target retries, and DLQ policy.
        #
        # Safe rollout flags:
        #   CRYPTO_USE_OUTBOX_DISPATCHER=1   -> outbox-only (no direct xadd to notify/raw/audit)
        #   USE_SIGNAL_OUTBOX=1              -> unified shared flag
        #   CRYPTO_SHADOW_OUTBOX=1           -> keep legacy direct publishing + also write to outbox
        use_outbox = self._cached_use_outbox
        shadow_outbox = self._cached_shadow_outbox
        outbox_stream = self._cached_outbox_stream
        
        # FEES AWARE GATE CONFIG
        # Modes: "SHADOW" (log only), "ENFORCE" (block signal), "OFF" (disable)
        gate_mode = self._cached_gate_mode

        # ------------------------------------------------------------------
        # Pipeline (explicit stages):
        #   1) extract + normalize primitive inputs (direction/entry/ts/conf)
        #   2) compute levels (sl/tp/lot/atr)
        #   3) build payloads:
        #        - enriched_signal: raw stream (payload)
        #        - audit_payload:   signals:cryptoorderflow:{symbol} (data)
        #        - telegram_payload: notify stream (fields)
        # ------------------------------------------------------------------
        # 1) Extract + normalize primitive inputs (direction/symbol/cfg)
        # ------------------------------------------------------------------
        symbol = runtime.symbol
        side_norm = normalize_side_3_safe(signal.get("direction") or signal.get("side") or "")
        if side_norm is None:
            logger.warning("⚠️ (%s) publish_signal: unknown direction=%r side=%r (skip)",
                           symbol, signal.get("direction"), signal.get("side"))
            return
        direction = side_norm.direction  # LONG/SHORT (str enum)
        cfg = runtime.config

        # State initialization (avoid reliance on locals() introspection)
        passed = True
        reason = "ok"
        gate_meta: Dict[str, Any] = {}

        if direction not in {"LONG", "SHORT"}:
            # FAIL-OPEN: invalid direction should not crash the service.
            logger.warning("⚠️ (%s) publish_signal: invalid direction=%r (skip)", symbol, signal.get("direction"))
            return
            
        # Record total signals
        signals_total.labels(symbol=symbol, handler="crypto_orderflow").inc()
        
        # Outcome: emit (attributed by sig_ts)
        sig_ts = int(signal.get("tick_ts") or signal.get("ts_ms") or get_ny_time_millis())  # safe fallback
        try:
            # ------------------------------------------------------------------
            # CLOCK SKEW ELIMINATION (Expert Implementation)
            # Priority:
            #   1. Explicit tick_ts from signal payload (Triggering Event Time)
            #   2. Explicit ts_ms from signal payload
            #   3. runtime.last_ts_ms (Exchange Reference Time from last processed tick)
            #   4. ONLY THEN wall-clock time.time()
            # ------------------------------------------------------------------
            exch_ref_ts = int(getattr(runtime, "last_ts_ms", 0) or 0)
            sig_ts = int(signal.get("tick_ts") or signal.get("ts_ms") or exch_ref_ts or get_ny_time_millis())

            # Monitor Skew: observe difference between signal time and system time
            # Important for SRE to detect if Go/Python servers differ too much.
            local_now = get_ny_time_millis()
            skew_ms = local_now - sig_ts
            if skew_ms < -3000 or skew_ms > 15000:  # Skew < -3s (from future) or Lag > 15s
                 logger.warning(f"🚨 [TIME_SYNC] Large Clock Skew detected for {symbol}: local={local_now}, signal_ts={sig_ts}, skew={skew_ms}ms (could be lag if >0)")

            of_session_outcome_total.labels(symbol, session_utc(sig_ts), "emit").inc()
        except Exception:
            pass
        
        try:
            entry = float(signal["entry"])
        except Exception:
            logger.warning("⚠️ (%s) publish_signal: invalid entry=%r (skip)", symbol, signal.get("entry"))
            return
        confirmations = signal.get("confirmations", [])
        indicators = signal.get("indicators") or {}
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
                kind=str(signal.get("kind") or "of"),
                symbol=symbol,
                ts_ms=int(sig_ts),
                direction=side_norm.direction
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
                venue=str(signal.get("venue") or "binance_usdm"),
                ts_event_ms=int(sig_ts),
                ts_publish_ms=get_ny_time_millis(),
                direction=side_norm.direction,
                side=side_norm.side,
                side_int=side_norm.side_int,
                entry_price=float(entry),
                sl_price=float(signal.get("sl") or 0.0),
                tp_levels=signal.get("tp_levels") or [],
                confidence=_original_confidence,
                ok=int(indicators.get("of_confirm_ok", 0) or 0),
                ok_soft=int(indicators.get("ok_soft", 0) or 0),
                reason=str(signal.get("reason", reason or "delta_spike")),
                scenario=str(indicators.get("scenario", "")),
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
        signal.setdefault("ts_emit_ms", int(sig_ts))
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
            ensure_decision_ctx_fields(signal, indicators=indicators, runtime=runtime, now_ms=int(sig_ts))
        except Exception as e:
            logger.warning("⚠️ (%s) Failed to enrich A1 decision ctx fields: %s", symbol, e)



        # --- BREADTH GATE ---
        ctx = self._build_gate_ctx(runtime, signal, sig_ts)
        kind = str(signal.get("kind") or "of")
        b_dec = self._breadth_gate.evaluate(ctx=ctx, symbol=symbol, kind=kind, side=direction)
        
        if b_dec.apply:
            indicators.setdefault("gate_flags", []).append(f"breadth_gate:{b_dec.reason_code}")
            if b_dec.veto:
                try:
                    breadth_gate_veto_total.labels(symbol=symbol, reason=b_dec.reason_code).inc()
                except Exception:
                    pass
                logger.info("🛡️ [GATE] BreadthGate VETO (%s): %s | %s", symbol, b_dec.reason_code, b_dec.notes)
                strong_gate_veto_total.labels(symbol=symbol, scenario="breadth_gate", reason=b_dec.reason_code, mode="ENFORCE").inc()
                passed = False
                reason = f"BREADTH_GATE_VETO: {b_dec.reason_code}"
                return
            elif b_dec.reason_code.startswith("SHADOW_VETO_"):
                try:
                    breadth_gate_shadow_veto_total.labels(symbol=symbol, reason=b_dec.reason_code).inc()
                except Exception:
                    pass

        # --- SHADOW REQUIRE_STRONG_CONFIRMATION (SignalPipeline) ---
        # Note: Enforcement is handled in strategy.py early gate.
        if runtime.config.get("require_strong_confirmation"):
            gate_ok = bool(int(indicators.get("strong_gate_ok", 0) or indicators.get("of_confirm_ok", 0) or 0))
            if not gate_ok:
                # Log veto (sampled)
                logger.info(
                    "🛡️ [GATE-PIPELINE-SHADOW] RequireStrongConfirmation: signal=%s vetoed by strong gate. need=%s have=%s legs=%s",
                    signal.get("signal_id"),
                    indicators.get("strong_gate_need"),
                    indicators.get("strong_gate_have"),
                    indicators.get("strong_gate_legs"),
                )
                strong_gate_veto_total.labels(symbol=runtime.symbol, scenario="unknown", reason="require_strong", mode="SHADOW").inc()
                # DO NOT return here - bookkeeping and publishing continue (handled by strategy ENFORCE)

        # ------------------------------------------------------------------
        # 🧩 SPREAD / ILLIQUIDITY GATE (P0)
        # ------------------------------------------------------------------
        # Vetos checks (spread_bps, spread_z, book_stale_ms)
        # 1. Spread BPS
        max_spread_bps = float(cfg.get("gate_spread_max_bps", 0.0) or 0.0)
        curr_spread_bps = float(indicators.get("liq_spread_bps", 0.0) or indicators.get("spread_bps", 0.0) or 0.0)
        
        # 2. Spread Z-Score
        max_spread_z = float(cfg.get("gate_spread_max_z", 0.0) or 0.0)
        curr_spread_z = float(indicators.get("spread_z", 0.0) or 0.0)

        # 3. Book Staleness
        max_stale_ms = int(cfg.get("gate_book_stale_ms", 0) or 0)
        curr_stale_ms = int(indicators.get("book_ts_gap_ms", 0) or 0)
        if curr_stale_ms <= 0:
            curr_stale_ms = int(indicators.get("liq_book_stale_ms", 0) or 0)

        # ------------------------------------------------------------------
        # P0: Derivatives context gate (funding / basis / OI crowding)
        # ------------------------------------------------------------------
        # Context is normalized by a separate low-frequency collector and stored
        # under Redis key `ctx:deriv:<SYMBOL>`.
        #
        # Profiles:
        #   - default/soft: annotate only
        #   - strict: tighten expected_slippage_bps
        #   - hard: tighten + optional veto on multi-flag crowding
        #
        # This code is fail-open: missing Redis / snapshot => no action.
        try:
            deriv_profile = self._cached_deriv_profile
            if deriv_profile not in {"default", "soft", "strict", "hard", "monitor", "tighten", "veto"}:
                deriv_profile = "default"

            snap = await aread_derivatives_context(getattr(runtime, "redis_client", None), symbol=symbol)
            if snap is None:
                # FIX-1: Observability for missing derivatives context (fail-open path)
                # Discovered via failure drill 2026-03-31: system was completely silent
                # when collector is down and ctx:deriv:* keys expire.
                indicators["deriv_ctx_missing"] = 1
                indicators["deriv_ctx_snapshot_age_ms"] = -1
                try:
                    if deriv_ctx_missing_total is not None:
                        sym_lbl = str(symbol).upper()
                        if self._deriv_ctx_syms_allow and sym_lbl not in self._deriv_ctx_syms_allow:
                            sym_lbl = "__all__"
                        deriv_ctx_missing_total.labels(symbol=sym_lbl).inc()
                except Exception:
                    pass
                logger.warning(
                    "⚠️ [GATE] DerivativesContext MISSING (%s): no snapshot in Redis "
                    "(TTL expired or collector down, profile=%s)",
                    symbol, deriv_profile,
                )
            if snap is not None:
                now_ms = int(signal.get("tick_ts") or signal.get("ts_ms") or get_ny_time_millis())
                indicators["deriv_ctx_profile"] = deriv_profile
                indicators["funding_rate"] = float(snap.funding_rate)
                indicators["funding_rate_z"] = float(snap.funding_rate_z)
                indicators["premium_index"] = float(snap.premium_index)
                indicators["basis_bps"] = float(snap.basis_bps)
                indicators["open_interest"] = float(snap.open_interest)
                indicators["delta_oi_5m"] = float(snap.delta_oi_5m)
                indicators["oi_notional_usd"] = float(snap.oi_notional_usd)
                indicators["funding_extreme"] = int(snap.funding_extreme)
                indicators["basis_extreme"] = int(snap.basis_extreme)
                indicators["oi_accel"] = int(snap.oi_accel)

                # Fetch additional contexts (fail-open)
                breadth_ctx = await aread_breadth_context(getattr(runtime, "redis_client", None))
                liq_ctx = await aread_liq_context(getattr(runtime, "redis_client", None), symbol=symbol)

                # Merge V2 fields into snapshot
                if breadth_ctx:
                    snap = __import__("dataclasses").replace(
                        snap,
                        market_breadth_ret_24h=float(breadth_ctx.get("ret_24h", 0.0)),
                        market_breadth_volume_z=float(breadth_ctx.get("vol_z", 0.0)),
                        leader_btc_eth_confirm=float(breadth_ctx.get("leader_confirm", 0.0))
                    )
                if liq_ctx:
                    snap = __import__("dataclasses").replace(
                        snap,
                        liq_buy_notional_1m=float(liq_ctx.get("liq_buy_notional_1m", 0.0)),
                        liq_sell_notional_1m=float(liq_ctx.get("liq_sell_notional_1m", 0.0)),
                        liq_imbalance_z=float(liq_ctx.get("liq_imbalance_z", 0.0))
                    )

                decd = evaluate_derivatives_context_v2(
                    profile=deriv_profile,
                    side=direction,
                    funding_rate_z=float(snap.funding_rate_z),
                    basis_bps=float(snap.basis_bps),
                    oi_accel=int(snap.oi_accel),
                    long_short_ratio_z=float(snap.long_short_ratio_z),
                    taker_buy_sell_imbalance=float(snap.taker_buy_sell_imbalance),
                    liq_imbalance_z=float(snap.liq_imbalance_z),
                    market_breadth_ret_24h=float(snap.market_breadth_ret_24h),
                    leader_btc_eth_confirm=float(snap.leader_btc_eth_confirm),
                    thr_funding_z=self._cached_deriv_ctx_funding_z,
                    thr_basis_bps=self._cached_deriv_ctx_basis_bps,
                    require_oi_for_veto=self._cached_deriv_ctx_require_oi,
                    tighten_mult=self._cached_deriv_ctx_tighten_mult,
                    tighten_cap_bps=self._cached_deriv_ctx_tighten_cap,
                )

                indicators["deriv_ctx_flags"] = ",".join(decd.flags) if decd.flags else ""
                indicators["deriv_ctx_hit"] = 1 if decd.hit else 0
                indicators["deriv_ctx_crowding_score"] = float(decd.crowding_score)
                indicators["deriv_ctx_snapshot_age_ms"] = int(max(0, now_ms - int(snap.ts_ms)))

                try:
                    sym_label = str(symbol).upper()
                    if self._deriv_ctx_syms_allow and sym_label not in self._deriv_ctx_syms_allow:
                        sym_label = "__all__"
                    deriv_ctx_snapshot_age_ms.labels(symbol=sym_label).observe(float(max(0, now_ms - int(snap.ts_ms))))
                    deriv_ctx_funding_rate_z.labels(symbol=sym_label).observe(abs(float(snap.funding_rate_z)))
                    deriv_ctx_basis_bps.labels(symbol=sym_label).observe(abs(float(snap.basis_bps)))
                    deriv_ctx_oi_notional_usd.labels(symbol=sym_label).observe(float(max(0.0, snap.oi_notional_usd)))
                    if decd.hit:
                        deriv_ctx_gate_monitor_hit_total.labels(symbol=sym_label, profile=deriv_profile).inc()
                except Exception:
                    pass

                if decd.tighten_add_bps > 0.0:
                    exp0 = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                    indicators["deriv_ctx_tighten_add_bps"] = float(decd.tighten_add_bps)
                    indicators["expected_slippage_bps"] = float(exp0 + float(decd.tighten_add_bps))
                    try:
                        sym_label2 = str(symbol).upper()
                        if self._deriv_ctx_syms_allow and sym_label2 not in self._deriv_ctx_syms_allow:
                            sym_label2 = "__all__"
                        deriv_ctx_gate_tighten_total.labels(symbol=sym_label2, profile=deriv_profile).inc()
                        deriv_ctx_tighten_add_bps.labels(symbol=sym_label2).observe(float(decd.tighten_add_bps))
                    except Exception:
                        pass

                if decd.veto:
                    deriv_reason = str(decd.veto_reason)
                    try:
                        sym_label3 = str(symbol).upper()
                        if self._deriv_ctx_syms_allow and sym_label3 not in self._deriv_ctx_syms_allow:
                            sym_label3 = "__all__"
                        deriv_ctx_gate_veto_total.labels(symbol=sym_label3, reason=deriv_reason).inc()
                    except Exception:
                        pass
                    logger.info(
                        "🛡️ [GATE] DerivativesContext VETO (%s): %s | funding_z=%.2f basis_bps=%.2f oi_accel=%s",
                        symbol,
                        deriv_reason,
                        float(snap.funding_rate_z),
                        float(snap.basis_bps),
                        int(snap.oi_accel),
                    )
                    strong_gate_veto_total.labels(symbol=symbol, scenario="deriv_ctx", reason=deriv_reason, mode="ENFORCE").inc()
                    passed = False
                    reason = f"DERIV_CTX_VETO: {deriv_reason}"
                    return
        except Exception:
            pass

        # ------------------------------------------------------------------
        # P0.1: DefiLlama macro/liquidity context gate (slow context, fail-open)
        # ------------------------------------------------------------------
        # NOT a tick trigger. Provides regime context (TVL, DEX volume, stablecoins).
        # Profiles: monitor → tighten → veto
        # Missing DefiLlama data = no action (fail-open).
        if self._cached_defillama_ctx_enabled:
            try:
                dl_profile = self._cached_defillama_ctx_profile
                dl_snap = await aread_defillama_context(getattr(runtime, "redis_client", None), symbol=symbol)
                if dl_snap is None:
                    indicators["defillama_ctx_missing"] = 1
                    indicators["defillama_ctx_snapshot_age_ms"] = -1
                    try:
                        if defillama_ctx_missing_total is not None:
                            defillama_ctx_missing_total.labels(symbol=str(symbol).upper()).inc()
                    except Exception:
                        pass
                if dl_snap is not None:
                    now_ms = int(signal.get("tick_ts") or signal.get("ts_ms") or get_ny_time_millis())
                    snap_age_ms = int(max(0, now_ms - int(dl_snap.ts_ms)))
                    indicators["defillama_ctx_profile"] = dl_profile
                    indicators["defillama_ctx_chain"] = str(dl_snap.chain)
                    indicators["defillama_ctx_stablecoin_regime"] = str(dl_snap.stablecoin_risk_regime)
                    indicators["defillama_ctx_chain_tvl_delta_1d_pct"] = float(dl_snap.chain_tvl_delta_1d_pct)
                    indicators["defillama_ctx_dex_volume_spike_z"] = float(dl_snap.dex_volume_spike_z)
                    indicators["defillama_ctx_fees_momentum"] = float(dl_snap.fees_revenue_momentum)
                    indicators["defillama_ctx_snapshot_age_ms"] = snap_age_ms

                    # Skip stale snapshots
                    if snap_age_ms > self._cached_defillama_ctx_max_age_ms:
                        indicators["defillama_ctx_stale"] = 1
                    else:
                        dl_dec = evaluate_defillama_context(
                            profile=dl_profile,
                            side=direction,
                            stablecoin_mcap_delta_1d=float(dl_snap.stablecoin_mcap_delta_1d),
                            stablecoin_mcap_delta_7d=float(dl_snap.stablecoin_mcap_delta_7d),
                            btc_dominance_momentum=0.0,  # Phase 2: wire from CoinGecko
                            chain_tvl_delta_1d_pct=float(dl_snap.chain_tvl_delta_1d_pct),
                            dex_volume_spike_z=float(dl_snap.dex_volume_spike_z),
                            fees_revenue_momentum=float(dl_snap.fees_revenue_momentum),
                            tighten_mult=self._cached_defillama_ctx_tighten_mult,
                            tighten_cap_bps=self._cached_defillama_ctx_tighten_cap,
                        )
                        indicators["defillama_ctx_flags"] = ",".join(dl_dec.flags) if dl_dec.flags else ""
                        indicators["defillama_ctx_hit"] = 1 if dl_dec.hit else 0
                        indicators["defillama_ctx_risk_score"] = float(dl_dec.risk_score)

                        # Prometheus telemetry
                        try:
                            sym_lbl = str(symbol).upper()
                            if defillama_ctx_snapshot_age_ms is not None:
                                defillama_ctx_snapshot_age_ms.labels(symbol=sym_lbl).observe(float(snap_age_ms))
                            if dl_snap.dex_volume_spike_z > 0 and defillama_ctx_dex_volume_spike_z is not None:
                                defillama_ctx_dex_volume_spike_z.labels(symbol=sym_lbl).observe(float(dl_snap.dex_volume_spike_z))
                            if defillama_ctx_chain_tvl_delta_1d_pct is not None:
                                defillama_ctx_chain_tvl_delta_1d_pct.labels(symbol=sym_lbl).observe(float(dl_snap.chain_tvl_delta_1d_pct))
                            if dl_dec.hit and defillama_ctx_gate_monitor_hit_total is not None:
                                defillama_ctx_gate_monitor_hit_total.labels(symbol=sym_lbl, profile=dl_profile).inc()
                        except Exception:
                            pass

                        if dl_dec.tighten_add_bps > 0.0:
                            exp0 = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                            indicators["defillama_ctx_tighten_add_bps"] = float(dl_dec.tighten_add_bps)
                            indicators["expected_slippage_bps"] = float(exp0 + float(dl_dec.tighten_add_bps))
                            try:
                                if defillama_ctx_gate_tighten_total is not None:
                                    defillama_ctx_gate_tighten_total.labels(symbol=str(symbol).upper(), reason="tighten").inc()
                            except Exception:
                                pass

                        if dl_dec.veto:
                            dl_reason = str(dl_dec.veto_reason)
                            try:
                                if defillama_ctx_gate_veto_total is not None:
                                    defillama_ctx_gate_veto_total.labels(symbol=str(symbol).upper(), reason=dl_reason).inc()
                            except Exception:
                                pass
                            logger.info(
                                "🛡️ [GATE] DefiLlama VETO (%s): %s | chain=%s regime=%s tvl_delta=%.2f dex_z=%.2f",
                                symbol, dl_reason, dl_snap.chain, dl_snap.stablecoin_risk_regime,
                                float(dl_snap.chain_tvl_delta_1d_pct), float(dl_snap.dex_volume_spike_z),
                            )
                            strong_gate_veto_total.labels(symbol=symbol, scenario="defillama_ctx", reason=dl_reason, mode="ENFORCE").inc()
                            passed = False
                            reason = f"DEFILLAMA_CTX_VETO: {dl_reason}"
                            return
            except Exception:
                pass

        # ------------------------------------------------------------------
        # P0.2: Sentiment context (Fear & Greed index)
        # ------------------------------------------------------------------
        if self._cached_sentiment_ctx_enabled:
            try:
                sent = await aread_sentiment_context(getattr(runtime, "redis_client", None))
                if sent is None:
                    indicators["sentiment_ctx_missing"] = 1
                    try:
                        sentiment_ctx_missing_total.inc()
                    except Exception:
                        pass
                else:
                    now_ms = int(signal.get("tick_ts") or signal.get("ts_ms") or get_ny_time_millis())
                    age_ms = max(0, now_ms - int(sent.ts_ms or 0))

                    if age_ms <= self._cached_sentiment_max_age_ms and sent.quality_status == "OK":
                        indicators["fear_greed_value"] = sent.fear_greed_value
                        indicators["fear_greed_delta_1d"] = sent.fear_greed_delta_1d
                        indicators["fear_greed_delta_7d"] = sent.fear_greed_delta_7d
                        indicators["sentiment_regime"] = sent.sentiment_regime
                        indicators["sentiment_risk_multiplier"] = sent.sentiment_risk_multiplier

                        side = "BUY" if direction == "LONG" else "SELL"

                        dec = evaluate_sentiment_context(
                            profile=self._cached_sentiment_profile,
                            side=side,
                            sentiment_regime=sent.sentiment_regime,
                            fear_greed_value=sent.fear_greed_value,
                            fear_greed_delta_1d=sent.fear_greed_delta_1d,
                            fear_greed_delta_7d=sent.fear_greed_delta_7d,
                            base_risk_multiplier=sent.sentiment_risk_multiplier,
                            tighten_cap_bps=self._cached_sentiment_tighten_cap_bps,
                        )

                        if dec.flags:
                            indicators["sentiment_flags"] = ",".join(dec.flags)
                        
                        if dec.tighten_add_bps > 0.0:
                            indicators["sentiment_tighten_add_bps"] = dec.tighten_add_bps
                            exp0 = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                            indicators["expected_slippage_bps"] = float(exp0 + float(dec.tighten_add_bps))
                            try:
                                sentiment_ctx_gate_tighten_total.labels(reason="tighten").inc()
                            except Exception:
                                pass

                        # Reduce risk (never increase)
                        signal["risk_multiplier"] = min(
                            float(signal.get("risk_multiplier", 1.0) or 1.0),
                            float(dec.risk_multiplier),
                        )
                        
                        try:
                            sentiment_risk_multiplier.set(float(dec.risk_multiplier))
                            if dec.hit:
                                sentiment_ctx_gate_monitor_hit_total.labels(profile=self._cached_sentiment_profile).inc()
                        except Exception:
                            pass
                    else:
                        indicators["sentiment_ctx_stale"] = 1
                        try:
                            sentiment_ctx_stale_total.inc()
                        except Exception:
                            pass
            except Exception as e:
                logger.warning("⚠️ (%s) Failed to evaluate sentiment context: %s", symbol, e)

        # ------------------------------------------------------------------
        # Cross-venue context gate (Coinbase/Kraken/OKX validation)
        # ------------------------------------------------------------------
        # Reads ctx:crossvenue:{SYMBOL} written by Go CrossVenueAggregator.
        # Fail-open: missing/stale context -> skip gate, no indicators emitted.
        # Phase 0: CROSSVENUE_CTX_ENABLED=0 (annotate-only via PROFILE=monitor).
        if self._cached_crossvenue_ctx_enabled:
            try:
                cv = await aread_crossvenue_context(
                    getattr(runtime, "redis_client", None), symbol=symbol
                )
                if cv is None:
                    indicators["crossvenue_ctx_missing"] = 1
                    try:
                        crossvenue_ctx_missing_total.labels(symbol=str(symbol).upper()).inc()
                    except Exception:
                        pass
                else:
                    now_ms_cv = int(signal.get("tick_ts") or signal.get("ts_ms") or get_ny_time_millis())
                    cv_age_ms = int(max(0, now_ms_cv - int(cv.ts_ms or 0)))
                    indicators["crossvenue_ctx_snapshot_age_ms"] = cv_age_ms

                    if cv_age_ms > self._cached_crossvenue_max_age_ms:
                        indicators["crossvenue_ctx_stale"] = 1
                        try:
                            crossvenue_ctx_stale_total.labels(symbol=str(symbol).upper()).inc()
                        except Exception:
                            pass
                    elif cv.quality_status == "OK":
                        # Enrich indicators with cross-venue features
                        indicators["cross_venue_mid_spread_bps"] = float(cv.cross_venue_mid_spread_bps)
                        indicators["binance_vs_coinbase_mid_bps"] = float(cv.binance_vs_coinbase_mid_bps)
                        indicators["binance_vs_kraken_mid_bps"] = float(cv.binance_vs_kraken_mid_bps)
                        indicators["binance_vs_okx_mid_bps"] = float(cv.binance_vs_okx_mid_bps)
                        indicators["cross_venue_direction_agree"] = float(cv.cross_venue_direction_agree)
                        indicators["cross_venue_trade_imbalance"] = float(cv.cross_venue_trade_imbalance)
                        indicators["venue_dislocation_z"] = float(cv.venue_dislocation_z)
                        indicators["venue_stale_count"] = int(cv.venue_stale_count)

                        cv_side = "BUY" if direction == "LONG" else "SELL"
                        active_cv_profile = self._get_cv_profile(getattr(runtime, "redis_client", None))
                        cv_dec = evaluate_crossvenue_context(
                            profile=active_cv_profile,
                            side=cv_side,
                            direction_agree=float(cv.cross_venue_direction_agree),
                            trade_imbalance=float(cv.cross_venue_trade_imbalance),
                            dislocation_z=float(cv.venue_dislocation_z),
                            mid_spread_bps=float(cv.cross_venue_mid_spread_bps),
                            stale_count=int(cv.venue_stale_count),
                            min_agree=self._cached_crossvenue_min_agree,
                            max_dislocation_z=self._cached_crossvenue_max_dislocation_z,
                            max_mid_spread_bps=self._cached_crossvenue_max_mid_spread_bps,
                            max_stale_count=self._cached_crossvenue_max_stale_count,
                            tighten_mult=self._cached_crossvenue_tighten_mult,
                            tighten_cap_bps=self._cached_crossvenue_tighten_cap,
                        )

                        indicators["crossvenue_flags"] = ",".join(cv_dec.flags) if cv_dec.flags else ""
                        indicators["crossvenue_mode"] = cv_dec.mode
                        indicators["crossvenue_tighten_add_bps"] = float(cv_dec.tighten_add_bps)

                        # Prometheus telemetry
                        try:
                            sym_cv = str(symbol).upper()
                            _cv_snapshot_age_ms_hist.labels(symbol=sym_cv).observe(float(cv_age_ms))
                            _cv_mid_spread_bps_gauge.labels(symbol=sym_cv).set(float(cv.cross_venue_mid_spread_bps))
                            _cv_direction_agree_gauge.labels(symbol=sym_cv).set(float(cv.cross_venue_direction_agree))
                            _cv_dislocation_z_gauge.labels(symbol=sym_cv).set(float(cv.venue_dislocation_z))
                            if cv_dec.hit:
                                crossvenue_ctx_gate_monitor_hit_total.labels(
                                    symbol=sym_cv, profile=self._cached_crossvenue_profile
                                ).inc()
                        except Exception:
                            pass

                        if cv_dec.tighten_add_bps > 0.0:
                            exp_slip = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                            indicators["expected_slippage_bps"] = float(exp_slip + cv_dec.tighten_add_bps)
                            try:
                                tighten_reason_cv = ",".join(cv_dec.flags[:2]) if cv_dec.flags else "unknown"
                                crossvenue_ctx_gate_tighten_total.labels(
                                    symbol=str(symbol).upper(), reason=tighten_reason_cv
                                ).inc()
                            except Exception:
                                pass

                        if cv_dec.veto:
                            cv_veto_reason = str(cv_dec.veto_reason)
                            try:
                                crossvenue_ctx_gate_veto_total.labels(
                                    symbol=str(symbol).upper(), reason=cv_veto_reason
                                ).inc()
                            except Exception:
                                pass
                            logger.info(
                                "shield [GATE] CrossVenue VETO (%s): %s | agree=%.2f disloc_z=%.2f spread_bps=%.2f stale=%d",
                                symbol, cv_veto_reason,
                                float(cv.cross_venue_direction_agree),
                                float(cv.venue_dislocation_z),
                                float(cv.cross_venue_mid_spread_bps),
                                int(cv.venue_stale_count),
                            )
                            strong_gate_veto_total.labels(
                                symbol=symbol, scenario="crossvenue_ctx",
                                reason=cv_veto_reason, mode="ENFORCE"
                            ).inc()
                            passed = False
                            reason = f"CROSSVENUE_CTX_VETO: {cv_veto_reason}"
                            return
            except Exception:
                pass

        # ------------------------------------------------------------------
        # Phase C (P2): Liquidity geometry & resiliency gate (monitor/tighten/veto)
        # ------------------------------------------------------------------
        # Inputs must already be present in indicators (computed in strategy.py):
        #   - book_slope_bid/ask (USD/bps)
        #   - dws_bps (bps)
        #   - liq_recovery_time_ms (ms)
        #
        # Profiles (ENV: LIQ_GATE_PROFILE, fallback GATE_PROFILE):
        #   - default/soft: annotate only (flags in indicators)
        #   - strict: tighten execution cost proxy (expected_slippage_bps add)
        #   - hard: tighten + veto when thresholds breached
        #
        # Thresholds (0 = disabled):
        #   - LIQ_MIN_BOOK_SLOPE
        #   - LIQ_MAX_DWS_BPS
        #   - LIQ_MAX_RECOVERY_TIME_MS
        liq_profile = self._cached_liq_profile
        if liq_profile not in {"default", "soft", "strict", "hard"}:
            liq_profile = "default"

        slope_bid = float(indicators.get("book_slope_bid", 0.0) or 0.0)
        slope_ask = float(indicators.get("book_slope_ask", 0.0) or 0.0)
        dws_bps_val = float(indicators.get("dws_bps", 0.0) or 0.0)
        rec_ms = int(indicators.get("liq_recovery_time_ms", 0) or 0)

        thr_slope = self._cached_liq_min_book_slope
        thr_dws = self._cached_liq_max_dws_bps
        thr_rec = self._cached_liq_max_recovery_ms

        cap = self._cached_liq_tighten_cap
        mult = self._cached_liq_tighten_mult

        try:
            from services.orderflow.liquidity_geom_policy import evaluate_liq_geom
            decg = evaluate_liq_geom(
                profile=liq_profile,
                slope_bid=slope_bid,
                slope_ask=slope_ask,
                dws_bps=dws_bps_val,
                recovery_ms=rec_ms,
                thr_slope=thr_slope,
                thr_dws=thr_dws,
                thr_recovery_ms=thr_rec,
                tighten_cap_bps=cap,
                tighten_mult=mult,
            )

            # Always annotate for observability/debugging
            if decg.flags:
                indicators["liq_geom_monitor_hit"] = 1
                indicators["liq_geom_flags"] = ",".join(decg.flags)
            else:
                indicators.setdefault("liq_geom_monitor_hit", 0)
                indicators.setdefault("liq_geom_flags", "")
            indicators["liq_geom_profile"] = liq_profile

            # Optional Prometheus telemetry (bounded symbol cardinality)
            try:
                sym_label = str(symbol).upper()
                if self._liq_geom_syms_allow and sym_label not in self._liq_geom_syms_allow:
                    sym_label = "__all__"
                if dws_bps_val > 0:
                    liq_geom_dws_bps.labels(symbol=sym_label).observe(float(dws_bps_val))
                if decg.slope_min > 0:
                    liq_geom_book_slope_min_usd_per_bps.labels(symbol=sym_label).observe(float(decg.slope_min))
                liq_geom_recovery_time_ms.labels(symbol=sym_label).observe(float(max(0, rec_ms)))
                if decg.flags:
                    liq_geom_monitor_hit_total.labels(symbol=sym_label, profile=liq_profile).inc()
            except Exception:
                pass

            # strict/hard: tighten execution cost proxy (expected_slippage_bps)
            if decg.tighten_add_bps > 0.0:
                exp0 = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                indicators["liq_geom_tighten_add_bps"] = float(decg.tighten_add_bps)
                indicators["expected_slippage_bps"] = float(exp0 + float(decg.tighten_add_bps))
                try:
                    sym_label2 = str(symbol).upper()
                    if self._liq_geom_syms_allow and sym_label2 not in self._liq_geom_syms_allow:
                        sym_label2 = "__all__"
                    liq_geom_tighten_total.labels(symbol=sym_label2, profile=liq_profile).inc()
                except Exception:
                    pass

            # hard: veto on breach
            if decg.veto:
                geom_reason = str(decg.veto_reason)
                try:
                    sym_label3 = str(symbol).upper()
                    if self._liq_geom_syms_allow and sym_label3 not in self._liq_geom_syms_allow:
                        sym_label3 = "__all__"
                    liq_geom_veto_total.labels(symbol=sym_label3, reason=geom_reason).inc()
                except Exception:
                    pass
                logger.info(
                    "🛡️ [GATE] Liquidity-Geometry VETO (%s): %s | slope_min=%.1f thr=%.1f dws=%.2f thr=%.2f rec_ms=%d thr=%d",
                    symbol,
                    geom_reason,
                    float(decg.slope_min),
                    thr_slope,
                    dws_bps_val,
                    thr_dws,
                    rec_ms,
                    thr_rec,
                )
                strong_gate_veto_total.labels(symbol=symbol, scenario="liq_geom", reason=geom_reason, mode="ENFORCE").inc()
                passed = False
                reason = f"LIQ_GEOM_VETO: {geom_reason}"
                return
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Phase D (P3): Flow toxicity gate (OFI normalized by depth + optional VPIN)
        # ------------------------------------------------------------------
        # Inputs (computed in strategy.py, fail-open):
        #   - ofi_norm_z : robust z-score of (ofi_best_qty*mid)/(notional_1bp_bid+ask)
        #   - vpin_cdf   : optional VPIN-like toxicity proxy in [0..1]
        #
        # Profiles:
        #   - default/soft: annotate only
        #   - strict: tighten expected_slippage_bps
        #   - hard: veto ONLY when flow_toxic AND (TCA-bad OR FLOW_TOX_VETO_WITHOUT_TCA=1)
        #
        # Thresholds:
        #   - FLOW_OFI_NORM_Z_MAX (0 disables)
        #   - FLOW_VPIN_CDF_MAX   (0 disables)
        try:
            from services.orderflow.flow_toxicity import evaluate_flow_toxicity

            flow_profile = self._cached_flow_profile
            # Allow explicit mode override
            mode_override = self._cached_flow_mode_override
            if mode_override in {"monitor", "tighten", "veto"}:
                flow_profile = mode_override
            if flow_profile not in {"default", "soft", "strict", "hard", "monitor", "tighten", "veto"}:
                flow_profile = "default"

            thr_z = self._cached_flow_thr_z
            thr_vpin = self._cached_flow_thr_vpin

            cap = self._cached_flow_cap
            mult = self._cached_flow_mult
            veto_wo_tca = self._cached_flow_veto_wo_tca

            ofi_z = float(indicators.get("ofi_norm_z", 0.0) or 0.0)
            vpin_cdf = float(indicators.get("vpin_cdf", 0.0) or 0.0)

            # Optional: TCA health inputs (if Phase B is enabled). If missing -> 0.
            tca_is = float(indicators.get("tca_is_p95_bps", indicators.get("is_p95_bps", 0.0)) or 0.0)
            tca_imp = float(indicators.get("tca_perm_impact_p95_bps", indicators.get("perm_impact_p95_bps", 0.0)) or 0.0)
            thr_is = self._cached_flow_thr_is
            thr_imp = self._cached_flow_thr_imp

            decf = evaluate_flow_toxicity(
                profile=flow_profile,
                ofi_norm_z=ofi_z,
                thr_ofi_norm_z=thr_z,
                vpin_cdf=vpin_cdf,
                thr_vpin_cdf=thr_vpin,
                tca_is_p95_bps=tca_is,
                tca_perm_impact_p95_bps=tca_imp,
                thr_is_p95_bps=thr_is,
                thr_perm_impact_p95_bps=thr_imp,
                tighten_mult=mult,
                tighten_cap_bps=cap,
                veto_without_tca=veto_wo_tca,
            )

            # annotate always
            indicators["flow_toxic_profile"] = str(flow_profile)
            indicators["flow_toxic_flags"] = ",".join(decf.flags) if decf.flags else ""
            indicators["flow_toxic_hit"] = 1 if decf.hit else 0

            # Optional Prometheus telemetry (bounded symbol cardinality)
            try:
                sym_label = str(symbol).upper()
                if self._flow_tox_syms_allow and sym_label not in self._flow_tox_syms_allow:
                    sym_label = "__all__"
                flow_toxic_ofi_norm_z.labels(symbol=sym_label).observe(float(ofi_z))
                flow_toxic_vpin_cdf.labels(symbol=sym_label).observe(float(vpin_cdf))
                if decf.hit:
                    flow_toxic_monitor_hit_total.labels(symbol=sym_label, profile=str(flow_profile)).inc()
            except Exception:
                pass

            if decf.tighten_add_bps > 0.0:
                exp0 = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                indicators["flow_toxic_tighten_add_bps"] = float(decf.tighten_add_bps)
                indicators["expected_slippage_bps"] = float(exp0 + float(decf.tighten_add_bps))
                try:
                    sym_label2 = str(symbol).upper()
                    if self._flow_tox_syms_allow and sym_label2 not in self._flow_tox_syms_allow:
                        sym_label2 = "__all__"
                    flow_toxic_tighten_total.labels(symbol=sym_label2, profile=str(flow_profile)).inc()
                except Exception:
                    pass

            if decf.veto:
                try:
                    sym_label3 = str(symbol).upper()
                    if self._flow_tox_syms_allow and sym_label3 not in self._flow_tox_syms_allow:
                        sym_label3 = "__all__"
                    flow_toxic_veto_total.labels(symbol=sym_label3, reason=str(decf.veto_reason or "flow_toxic")).inc()
                except Exception:
                    pass
                logger.info(
                    "🛡️ [GATE] FlowToxicity VETO (%s): flags=%s ofi_z=%.2f thr=%.2f vpin_cdf=%.3f thr=%.3f tca_is_p95=%.2f thr=%.2f tca_imp_p95=%.2f thr=%.2f",
                    symbol,
                    indicators.get("flow_toxic_flags", ""),
                    ofi_z,
                    thr_z,
                    vpin_cdf,
                    thr_vpin,
                    tca_is,
                    thr_is,
                    tca_imp,
                    thr_imp,
                )
                strong_gate_veto_total.labels(symbol=symbol, scenario="flow_toxic", reason="VETO_FLOW_TOXIC", mode="ENFORCE").inc()
                passed = False
                reason = "VETO_FLOW_TOXIC"
                return
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Phase E / P4: MANIPULATION / MICROSTRUCTURE ABUSE GATE
        # ------------------------------------------------------------------
        # Inputs (injected by strategy.py from runtime.manip/msg_rate):
        #   - quote_stuffing_score  float 0..1
        #   - layering_score        float 0..1
        #   - otr_z                 robust z-score of OTR ratio
        #   - manip_flags           comma-separated code string
        #
        # Mode (ENV: MANIP_GATE_PROFILE, fallback GATE_PROFILE):
        #   - default/soft/monitor: annotate only
        #   - strict/tighten: tighten expected_slippage_bps + annotate
        #   - hard/veto: tighten + veto
        #
        # Thresholds (0 = disabled):
        #   - MANIP_QUOTE_STUFF_SCORE_MAX  (0 = disabled)
        #   - MANIP_LAYERING_SCORE_MAX     (0 = disabled)
        #   - MANIP_OTR_Z_MAX             (0 = disabled)
        try:
            manip_profile = self._cached_manip_profile
            # Explicit mode override (matches flow_toxicity pattern)
            manip_mode_ov = self._cached_manip_mode_override
            if manip_mode_ov in {"monitor", "tighten", "veto"}:
                manip_profile = manip_mode_ov
            if manip_profile not in {"default", "soft", "strict", "hard", "monitor", "tighten", "veto"}:
                manip_profile = "default"

            thr_qs = self._cached_manip_thr_qs
            thr_lay = self._cached_manip_thr_lay
            thr_otr_z = self._cached_manip_thr_otr_z

            tighten_cap = self._cached_manip_tighten_cap
            tighten_mult = self._cached_manip_tighten_mult

            qs_score = float(indicators.get("quote_stuffing_score", 0.0) or 0.0)
            lay_score = float(indicators.get("layering_score", 0.0) or 0.0)
            otr_z_val = float(indicators.get("otr_z", 0.0) or 0.0)
            manip_flags_val = str(indicators.get("manip_flags", "") or "")

            # Evaluate gate flags
            manip_hit_flags = list(manip_flags_val.split(",")) if manip_flags_val else []
            hit_qs = thr_qs > 0.0 and qs_score >= thr_qs
            hit_lay = thr_lay > 0.0 and lay_score >= thr_lay
            hit_otr = thr_otr_z > 0.0 and otr_z_val >= thr_otr_z
            hit_any = hit_qs or hit_lay or hit_otr

            # Annotate always
            indicators["manip_gate_profile"] = manip_profile
            indicators["manip_gate_hit"] = 1 if hit_any else 0

            # Telemetry: monitor hit
            if hit_any and manip_profile in {"default", "soft", "monitor"}:
                reason_parts = []
                if hit_qs: reason_parts.append(f"qs={qs_score:.2f}")
                if hit_lay: reason_parts.append(f"lay={lay_score:.2f}")
                if hit_otr: reason_parts.append(f"otr_z={otr_z_val:.2f}")
                logger.info("🔍 [GATE-MANIP] MONITOR (%s): %s | flags=%s", symbol, " ".join(reason_parts), manip_flags_val)
                try:
                    manip_gate_events_total.labels(symbol=symbol, mode="monitor", reason="ANNOTATE").inc()
                except Exception:
                    pass

            # Tighten in strict/tighten/hard/veto profiles
            if hit_any and manip_profile in {"strict", "tighten", "hard", "veto"}:
                manip_score = max(qs_score, lay_score)
                if manip_score <= 0.0 and hit_otr:
                    manip_score = min(1.0, max(0.1, (otr_z_val - thr_otr_z) / max(thr_otr_z, 1.0)))
                add_bps = float(min(tighten_cap, manip_score * tighten_mult * 3.0))
                if add_bps > 0.0:
                    exp0 = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                    indicators["expected_slippage_bps"] = float(exp0 + add_bps)
                    indicators["manip_tighten_add_bps"] = float(add_bps)
                    try:
                        manip_gate_events_total.labels(symbol=symbol, mode="tighten", reason="TIGHTEN").inc()
                    except Exception:
                        pass

            # Veto in hard/veto profiles
            if hit_any and manip_profile in {"hard", "veto"}:
                veto_reason = "VETO_QUOTE_STUFFING" if hit_qs else ("VETO_LAYERING" if hit_lay else "VETO_OTR_SPIKE")
                logger.info(
                    "🛡️ [GATE] Manipulation VETO (%s): %s | qs_score=%.3f lay_score=%.3f otr_z=%.2f flags=%s",
                    symbol, veto_reason, qs_score, lay_score, otr_z_val, manip_flags_val,
                )
                try:
                    manip_gate_events_total.labels(symbol=symbol, mode="veto", reason=veto_reason).inc()
                except Exception:
                    pass
                strong_gate_veto_total.labels(symbol=symbol, scenario="manip", reason=veto_reason, mode="ENFORCE").inc()
                passed = False
                reason = f"MANIP_VETO: {veto_reason}"
                return
        except Exception:
            pass

        # ------------------------------------------------------------------
        # P5: Book sanity gate (crossed BBO / NaN depth / negative qty)
        # Default mode: monitor (annotate indicators, never veto unless BOOK_SANITY_MODE=veto)
        # ------------------------------------------------------------------
        try:
            # Populate book_sanity_flags indicator from runtime (set by BookProcessor)
            indicators.setdefault("book_sanity_ok", int(getattr(runtime, "book_sanity_ok", 1) or 1))
            bsf = str(getattr(runtime, "book_sanity_flags", "") or "")
            if bsf:
                indicators.setdefault("book_sanity_flags", bsf)
                # Parse into a list for the gate
                indicators["book_sanity_flags_list"] = [s.strip() for s in bsf.split(",") if s.strip()]
            bsg_dec = _book_sanity_gate.evaluate(indicators=indicators, symbol=str(symbol))
            indicators["book_sanity_gate_veto"] = int(1 if bsg_dec.veto else 0)
            indicators["book_sanity_gate_reason"] = str(bsg_dec.reason_code or "")
            if bsg_dec.veto:
                logger.info("🛡️ [GATE-P5-BOOK-SANITY] VETO (%s): %s | flags=%s", symbol, bsg_dec.reason_code, bsg_dec.flags)
                strong_gate_veto_total.labels(symbol=symbol, scenario="book_sanity", reason=bsg_dec.reason_code, mode="ENFORCE").inc()
                passed = False
                reason = f"P5_BOOK_SANITY: {bsg_dec.reason_code}"
                return
        except Exception:
            pass

        # ------------------------------------------------------------------
        # P5: Stream integrity gate (seq gaps / dup burst / schema drift)
        # Default mode: monitor (annotate, never veto unless STREAM_INTEGRITY_MODE=veto + thresholds set)
        # ------------------------------------------------------------------
        try:
            # Surface P5 integrity fields to indicators (set by TickProcessor / BookProcessor)
            def _si_ema(tracker) -> float:
                try: return float(getattr(tracker, "gap_ema", None).ema or 0.0)
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

            sig_dec = _stream_integrity_gate.evaluate(indicators=indicators, symbol=str(symbol))
            indicators["stream_integrity_gate_veto"] = int(1 if sig_dec.veto else 0)
            indicators["stream_integrity_gate_reason"] = str(sig_dec.reason_code or "")
            if sig_dec.veto:
                logger.info("🛡️ [GATE-P5-STREAM-INTEGRITY] VETO (%s): %s | flags=%s", symbol, sig_dec.reason_code, sig_dec.flags)
                strong_gate_veto_total.labels(symbol=symbol, scenario="stream_integrity", reason=sig_dec.reason_code, mode="ENFORCE").inc()
                passed = False
                reason = f"P5_STREAM_INTEGRITY: {sig_dec.reason_code}"
                return
        except Exception:
            pass

        spread_veto = False
        spread_reason = "ok"

        if max_spread_bps > 0 and curr_spread_bps > max_spread_bps:
            spread_veto = True
            spread_reason = f"spread_bps={curr_spread_bps:.2f} > {max_spread_bps}"
        elif max_spread_z > 0 and curr_spread_z > max_spread_z:
            spread_veto = True
            spread_reason = f"spread_z={curr_spread_z:.2f} > {max_spread_z}"
        elif max_stale_ms > 0 and curr_stale_ms > max_stale_ms:
            spread_veto = True
            spread_reason = f"book_stale_ms={curr_stale_ms} > {max_stale_ms}"

        if spread_veto:
            logger.info("🛡️ [GATE] Spread/Liquidity VETO (%s): %s", symbol, spread_reason)
            strong_gate_veto_total.labels(symbol=symbol, scenario="spread", reason=spread_reason, mode="ENFORCE").inc()
            passed = False
            reason = f"SPREAD_VETO: {spread_reason}"
            # STRICT ENFORCEMENT
            return

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


        # ---- trail_profile ----
        # cfg already initialized
        trail_profile = signal.get("trail_profile") or cfg.get("trail_profile") or "protective_only"

        # ---- ts normalization (epoch ms best-effort) ----
        # We keep multiple mirrors because older downstream components differ:
        #   - some read `ts`, others `timestamp`, newer contract expects `ts_ms`.
        def _ts_ms() -> int:
            v = signal.get("tick_ts") or signal.get("generated_at") or signal.get("ts_ms") or signal.get("ts")
            try:
                iv = int(float(v))
                return iv if iv > 0 else get_ny_time_millis()
            except Exception:
                return get_ny_time_millis()

        ts_ms = _ts_ms()

        # Calculate actual ATR that will be used (including fallbacks)
        sl, tp_levels, lot, atr, atr_meta = self._calculate_levels(runtime, entry, direction, indicators, trail_profile=trail_profile)

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
                cb_pct_raw = (atr * calib_cb_mult / float(entry)) * 100.0
                trail_callback_pct = round(cb_pct_raw, 2)
                indicators["trail_calib_source"] = trail_calib_source
                indicators["trail_calib_cb_mult"] = float(calib_cb_mult)
            except Exception:
                pass

        # ---- OVERRIDE FOR RANGE REGIME ----
        # In range: 2 TPs only, recomputed via RANGE_TP_RR env (default "1.0,1.5").
        # TP levels are RECOMPUTED from stop_dist (not just truncated) to achieve correct R:R.
        # ENV: RANGE_TP_RR — comma-separated RR multipliers for range (e.g. "1.0,1.5")
        # Result: TP1 = 1.0×SL_dist, TP2 = 1.5×SL_dist.
        rg_for_overrides = str(getattr(runtime, "last_regime", "na") or "na").lower()
        if rg_for_overrides == "na":
             rg_for_overrides = str(indicators.get("regime", "na") or "na").lower()
             
        is_range_regime_flag = ("range" in rg_for_overrides)
        is_expansion_regime_flag = ("expansion" in rg_for_overrides)
        is_trending_regime_flag = ("trending" in rg_for_overrides) and not is_expansion_regime_flag
        is_squeeze_regime_flag = ("squeeze" in rg_for_overrides)
        
        if is_range_regime_flag:
            try:
                _range_rr_str = self._cached_range_tp_rr
                _range_rr = [float(x.strip()) for x in _range_rr_str.split(",") if x.strip()][:2]
                _stop_dist = abs(float(entry) - float(sl))
                if _stop_dist > 0 and len(_range_rr) >= 1:
                    if getattr(direction, 'value', str(direction or '')).upper() == "LONG":
                        tp_levels = [entry + _stop_dist * r for r in _range_rr]
                    else:
                        tp_levels = [entry - _stop_dist * r for r in _range_rr]
                    indicators["range_tp_rr_applied"] = _range_rr_str
                    indicators["range_stop_dist_atr"] = round(_stop_dist / float(atr), 3) if float(atr) > 0 else 0.0

                    # ⚠️ FIX (2026-04-25): Enforce minimum TP distance in bps to guarantee
                    # profitability after exchange fees. Without this, BTC range TPs were
                    # only 1.8 bps (< 8 bps fee floor) → systematic losses.
                    _tp_bps_floor = self._cached_fees_bps_rt + self._cached_tp_bps_buffer
                    _min_tp_dist = float(entry) * _tp_bps_floor / 10_000.0
                    _tp_expanded = False
                    for i, tp in enumerate(tp_levels):
                        _tp_dist = abs(float(tp) - float(entry))
                        if _tp_dist < _min_tp_dist * (i + 1):
                            if getattr(direction, 'value', str(direction or '')).upper() == "LONG":
                                tp_levels[i] = float(entry) + _min_tp_dist * (i + 1)
                            else:
                                tp_levels[i] = float(entry) - _min_tp_dist * (i + 1)
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

        elif is_trending_regime_flag:
            trail_profile = "rocket_v1"
            signal["trail_after_tp1"] = True

        elif is_squeeze_regime_flag:
            logger.info("🛡️ [GATE] Regime VETO (%s): Trading disabled in Squeeze regime", symbol)
            try:
                strong_gate_veto_total.labels(symbol=symbol, scenario="regime", reason="veto_squeeze", mode="ENFORCE").inc()
            except Exception:
                pass
            passed = False
            reason = "REGIME_VETO: squeeze"
            return

        # ------------------------------------------------------------------
        # Construction Phase using Typed SignalPayload
        # ------------------------------------------------------------------
        
        # 1. Gate Decision (if available in indicators)
        gate_decision = None
        if indicators.get("strong_gate_ok") is not None:
             gate_decision = StrongGateDecision(
                 ok=bool(int(indicators.get("strong_gate_ok", 0))),
                 scenario=str(indicators.get("strong_gate_scn", "na")),
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
            signal_id=str(signal.get("signal_id", "")),
        )
        
        # Export back to dict for legacy compatibility
        # (This essentially enriches the raw payload with structured data)
        evidence_dict = sig_payload.to_dict()

        # Build final stream payload (legacy structure + new evidence)
        payload = {
            "signal_id": str(signal.get("signal_id", "")),
            "symbol": runtime.symbol,
            "direction": direction,
            "entry": float(entry),
            "sl": float(sl),
            "tp_levels": [float(x) for x in tp_levels],
            "lot": float(lot),
            "qty": float(lot),
            "quantity": float(lot),
            "atr": float(atr),
            "confidence": float(signal.get("confidence", 0.0) or 0.0), # Will be re-calculated or passed
            "reason": str(signal.get("reason", "unknown")),
            "ts_ms": int(ts_ms),
            "generated_at": int(ts_ms),
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
        
        if is_range_regime_flag:
            payload["tp_ratio"] = [0.80, 0.20]
        elif is_expansion_regime_flag:
            payload["tp_ratio"] = [0.70, 0.20, 0.10]
        
        # Optional: publish compact confidence score event to high-frequency stream
        safe_create_task(
            self._maybe_publish_confidence_scores(
                symbol=str(symbol),
                sid=str(payload.get("signal_id", "")),
                ts_event_ms=int(payload.get("ts_ms") or 0),
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
                if float(entry) > 0 and float(atr) > 0:
                    atr_bps_exec = float(10000.0 * (float(atr) / float(entry)))
            except Exception:
                atr_bps_exec = 0.0
            indicators["atr_bps_exec"] = float(atr_bps_exec)

            # Pull floors (prefer calibrated/dynamic; fallback to config)
            t0 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T0_BPS, cfg.get("atr_floor_t0_bps", 0.0)) or 0.0)
            t1 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T1_BPS, cfg.get("atr_floor_t1_bps", 0.0)) or 0.0)
            t2 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T2_BPS, cfg.get("atr_floor_t2_bps", 0.0)) or 0.0)

            tier, picked, floor_th = compute_atr_bps_threshold(regime=rg, cfg=cfg, t0=t0, t1=t1, t2=t2)

            indicators["atr_floor_t0_bps"] = float(t0)
            indicators["atr_floor_t1_bps"] = float(t1)
            indicators["atr_floor_t2_bps"] = float(t2)
            indicators["atr_floor_tier"] = int(tier)
            indicators["atr_floor_picked_bps"] = float(picked)
            indicators["atr_floor_th_bps"] = float(floor_th)
            indicators["atr_floor_rg"] = str(rg)
            indicators["atr_floor_ready"] = int(runtime.dynamic_cfg.get(DK.ATR_CALIB_READY, 0) or 0)
            indicators["atr_floor_src"] = str(runtime.dynamic_cfg.get(DK.ATR_BPS_SRC, "na") or "na")
            indicators["atr_floor_n"] = int(runtime.dynamic_cfg.get(DK.ATR_BPS_N, 0) or 0)

            # Keep legacy mirror used by some earlier logic
            indicators["atr_bps_th"] = float(floor_th)
        except Exception:
            pass

        # Optional: also expose fees-aware threshold for audits even if gate not enforced
        try:
            from core.fees_aware_policy import fees_aware_min_atr_bps

            # tp1_share derived from TP_RATIO (env) or config snapshot
            tp_ratios = parse_tp_ratio(cfg.get("tp_ratio"))
            tp1_share_actual = float(tp_ratios[0] if tp_ratios else 0.5)
            rocket_mult = float(self._get_rocket_multiplier(runtime.symbol) or 0.0)
            fees_th, fees_meta = fees_aware_min_atr_bps(
                fees_bps_rt=float(self.FEES_BPS_RT),
                tp_bps_buffer=float(self.TP_BPS_BUFFER),
                tp1_share=tp1_share_actual,
                rocket_mult=rocket_mult,
            )
            indicators["atr_fees_th_bps"] = float(fees_th)
            indicators["atr_fees_tp1_share"] = float(tp1_share_actual)
            indicators["atr_fees_rocket_mult"] = float(rocket_mult)
        except Exception:
            pass

        # Unified threshold numbers into indicators (debug-only; gate uses same values below)
        try:
            floor_th = float(indicators.get("atr_floor_th_bps", 0.0) or 0.0)
            fees_th = float(indicators.get("atr_fees_th_bps", 0.0) or 0.0)
            unified_th = float(max(floor_th, fees_th))
            indicators["atr_unified_th_bps"] = float(unified_th)
            indicators["atr_gate_dominant"] = ("fees" if fees_th >= floor_th else "floor") if unified_th > 0 else "na"
        except Exception:
            pass


        # ------------------------------------------------------------------
        # 🚦 UNIFIED ATR GATE (ATR-floor tiers + fees-aware margin for rocket_v1)
        # ------------------------------------------------------------------
        if gate_mode in {"SHADOW", "ENFORCE"} and trail_profile == "rocket_v1":
            try:
                from core.atr_floor_policy import compute_atr_bps_threshold
                from core.fees_aware_policy import fees_aware_min_atr_bps
                
                atr_bps_exec = (float(atr) / float(entry)) * 10000.0 if entry > 0 else 0.0
                
                # 1) ATR-floor threshold (tier-by-regime)
                rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                # Prefer computed floor_th_bps (fixed chain); fallback to runtime.dynamic_cfg if needed
                atr_floor_th = float(
                    indicators.get("atr_floor_th_bps", 0.0)
                    or indicators.get("atr_bps_th", 0.0)
                    or runtime.dynamic_cfg.get(DK.ATR_BPS_TH, 0.0)
                    or 0.0
                )
                
                if not (atr_floor_th > 0):
                    # fallback: recompute from floors
                    t0 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T0_BPS, runtime.config.get("atr_floor_t0_bps", 0.0)) or 0.0)
                    t1 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T1_BPS, runtime.config.get("atr_floor_t1_bps", 0.0)) or 0.0)
                    t2 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T2_BPS, runtime.config.get("atr_floor_t2_bps", 0.0)) or 0.0)
                    _, _, _th = compute_atr_bps_threshold(regime=rg, cfg=runtime.config, t0=t0, t1=t1, t2=t2)
                    atr_floor_th = float(_th)
                
                # 2) Fees-aware threshold
                # cfg already initialized at the top of publish_signal
                tp_ratios = parse_tp_ratio(cfg.get("tp_ratio"))
                tp1_share_actual = float(tp_ratios[0] if tp_ratios else 0.5)
                rocket_mult = float(self._get_rocket_multiplier(runtime.symbol) or 0.0)
                fees_th, _ = fees_aware_min_atr_bps(
                    fees_bps_rt=float(self.FEES_BPS_RT),
                    tp_bps_buffer=float(self.TP_BPS_BUFFER),
                    tp1_share=tp1_share_actual,
                    rocket_mult=rocket_mult,
                )
                
                unified_th = float(max(atr_floor_th, fees_th))
                dominant = "fees" if fees_th >= atr_floor_th else "floor"
                
                # EXPERT RELAXATION (2026-01-30):
                # For meme coins, we relax the ATR gate significantly (5% floor)
                # to ensure they enter the virtual tracking system for calibration reports.
                from core.instrument_config import symbol_env_prefix
                is_meme = symbol_env_prefix(symbol) in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF")
                
                effective_th = unified_th
                if is_meme:
                    effective_th *= 0.05 # 95% discount for calibration
                
                veto = bool(effective_th > 0 and atr_bps_exec < effective_th)
                if is_meme and not veto and effective_th < unified_th:
                     logger.info("✅ [ATR-GATE] (%s) RELAXED PASS: atr_bps=%.2f passed via relaxation (th=%.2f -> relaxed_th=%.2f)", 
                                 symbol, atr_bps_exec, unified_th, effective_th)

                if veto:
                    # [AUDIT-ONLY] Unified Pipeline Gate is now strictly passive.
                    # Substituted by "Early Gate" in strategy.py (load shedder).
                    msg_mode = "[AUDIT-ONLY]" if gate_mode == "SHADOW" else "[GATE-ENFORCE]"
                    
                    if gate_mode in {"SHADOW", "ENFORCE"}:
                         logger.info("ℹ️ %s ATR unified VETO triggered (%s): atr_bps=%.2f < th=%.2f (relaxed_th=%.2f) | %s",
                                      msg_mode, dominant, atr_bps_exec, unified_th, effective_th, symbol)

                    indicators["gate_shadow_veto"] = True
                    indicators["gate_reason"] = "LOW_ATR_UNIFIED"
                    # Keep numeric trail for downstream audit
                    indicators["atr_floor_th_bps"] = float(atr_floor_th)
                    indicators["atr_fees_th_bps"] = float(fees_th)
                    indicators["atr_unified_th_bps"] = float(unified_th)
                    indicators["atr_gate_dominant"] = str(dominant)
                    
                    if gate_mode == "ENFORCE":
                        strong_gate_veto_total.labels(symbol=symbol, scenario="atr_unified", reason="low_atr", mode="ENFORCE").inc()
                        # Strict enforcement
                        passed = False
                        reason = f"ATR_VETO: {dominant} {atr_bps_exec:.2f} < {unified_th:.2f}"
                        return
            except Exception:
                pass

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
            
            expected_tp1 = entry + (atr * rocket_mult) if getattr(direction, 'value', str(direction or '')).upper() == "LONG" else entry - (atr * rocket_mult)
            actual_tp1 = float(tp_levels[0])
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
                 if getattr(direction, 'value', str(direction or '')).upper() == "LONG":
                     tp_levels[0] = float(entry + atr * rocket_mult)
                 else:
                     tp_levels[0] = float(entry - atr * rocket_mult)
        
        mix_dict = self._build_mix_dict(delta, delta_z, indicators, confirmations)

        confidence = signal.get("confidence")
        if confidence is None:
            confidence = indicators.get("confidence")
        if confidence is None:
            confidence = 0.3
        try:
            confidence = float(confidence)
        except (TypeError, ValueError):
            confidence = 0.3
        confidence = max(0.0, min(1.0, confidence))

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
            
            if confidence < min_conf:
                logger.info(
                    "🚫 [GATE] Confidence VETO (%s): %.2f < %.2f (Signal DROPPED)",
                    symbol, confidence, min_conf
                )
                strong_gate_veto_total.labels(symbol=symbol, scenario="confidence", reason="low_confidence", mode="ENFORCE").inc()
                return # DROP SIGNAL
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
                    "strong_gate_scn": str(indicators.get("strong_gate_scn", "") or ""),
                    "weak_recent_cnt": int(indicators.get("weak_recent_cnt", 0) or 0),
                    "weak_recent_frac": float(indicators.get("weak_recent_frac", 0.0) or 0.0),
                },
                # TTL / Orphan Housekeeping
                "max_lifetime_bars_after_entry": int(os.getenv("ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY", "180")),
                "orphan_ttl_ms": int(os.getenv("ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY", "0") or 0) or None,
                # FIX #2/#5: Propagate regime + scenario into signal payload
                # Previously computed for gate decisions but never written to payload →
                # all trades had regime=na, scenario=na in trades:closed.
                "regime": rg_for_overrides if rg_for_overrides != "na" else str(indicators.get("regime") or "na"),
                "scenario": str(indicators.get("strong_gate_scn") or "na"),
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

        if is_range_regime_flag:
            enriched_signal["tp_ratio"] = [0.80, 0.20]
        elif is_expansion_regime_flag:
            enriched_signal["tp_ratio"] = [0.70, 0.20, 0.10]

        # --- Request: lock_and_trail -> TP1=50%, TP2=20%, Trail after TP2 ---
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
                    _entry_p = float(entry or 0.0)
                    _atr_p = float(atr or 0.0)
                    if _entry_p > 0 and _atr_p > 0:
                        _tp_r = parse_tp_ratio(cfg.get("tp_ratio"))
                        _tp1_share_p = float(_tp_r[0] if _tp_r else 0.5)
                        _rocket_m = float(self._get_rocket_multiplier(symbol) or 1.5)
                        _tp1_bps = (_atr_p * _rocket_m / _entry_p) * 10_000.0 * _tp1_share_p
                        _stop_bps = (_atr_p / _entry_p) * 10_000.0
                        
                        # ✅ Fee-aware edge: subtract round-trip fees
                        _fees_bps = float(self.FEES_BPS_RT)
                        _gross_edge = max(0.0, _tp1_bps - _stop_bps - _fees_bps)
                        
                        if _gross_edge > 0.0:
                            enriched_signal["expected_edge_bps"] = float(_gross_edge)
                except Exception:
                    pass

            # 4. fee_bps — round-trip fee from FEES_BPS_RT ENV (default 10 bps = 0.05%/side)
            if not enriched_signal.get("fee_bps"):
                enriched_signal["fee_bps"] = float(self.FEES_BPS_RT)
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
            pass

        # Contract normalization (FAIL-OPEN)
        preprocess_signal_for_publish(enriched_signal, symbol=str(symbol), source="CryptoOrderFlow", logger=logger)

        # ------------------------------------------------------------------
        # PRE-PUBLISH GATES (Hard Data Quality + Regime/Session)
        # Эти гейты должны отсечь «отравленные» сигналы (stale touch/ATR, gaps, low depth, burst flip, drift).
        # По умолчанию они отключены (enabled=False в from_env), так что безопасно внедрять wiring.
        # ------------------------------------------------------------------
        veto_dec = None
        try:
            # FIX #4: kind must come from entry_tag (=primary_reason: weak_progress/breakout/absorption etc.)
            # Previously indicators["kind"] and signal["kind"] were both empty for CryptoOrderFlow,
            # so all RS_DENY rules were silently bypassed because kind="" never matched anything.
            kind = str(
                indicators.get("kind")
                or signal.get("kind")
                or enriched_signal.get("entry_tag")   # canonical kind for CryptoOrderFlow
                or signal.get("entry_tag")
                or ""
            ).strip().lower()
            # FIX: regime for pp_ctx should use the canonical market regime, not liq_regime.
            # liq_regime is a liquidity-specific label; the actual market regime is in enriched_signal["regime"].
            _regime_for_ctx = str(
                enriched_signal.get("regime")
                or indicators.get("regime")
                or indicators.get("liq_regime")
                or signal.get("liq_regime")
                or ""
            ).strip().lower()
            pp_ctx = SimpleNamespace(
                ts_event_ms=int(enriched_signal.get("ts_ms") or 0),
                ts=int(enriched_signal.get("ts_ms") or 0),
                data_quality_flags=enriched_signal.get("data_quality_flags") or {},
                atr_ts_ms=int(enriched_signal.get("atr_ts_ms") or indicators.get("atr_ts_ms") or 0),
                touch_is_stale=bool(enriched_signal.get("touch_is_stale") or False),
                l2_is_stale=bool(enriched_signal.get("l2_is_stale") or False),
                spread_bps=float(enriched_signal.get("spread_bps") or indicators.get("spread_bps") or 0.0),
                depth_bid_20=float(indicators.get("depth_bid_20") or 0.0),
                depth_ask_20=float(indicators.get("depth_ask_20") or 0.0),
                regime=_regime_for_ctx,
                indicators=indicators,
                of=SimpleNamespace(
                    depth_bid_5=float(indicators.get("depth_bid_5") or 0.0),
                    depth_ask_5=float(indicators.get("depth_ask_5") or 0.0),
                    burst_flip_ratio=float(indicators.get("burst_flip_ratio") or indicators.get("burst_flip_ratio_60s") or 0.0),
                ),
            )


            for _gate in (self._hard_dq_gate, self._rs_gate, self._atr_floor_gate):
                if _gate is None:
                    continue
                try:
                    dec = _gate.evaluate(ctx=pp_ctx, symbol=str(symbol), kind=kind)
                except Exception:
                    continue
                if getattr(dec, "apply", False) and getattr(dec, "veto", False):
                    veto_dec = dec
                    break
        except Exception:
            veto_dec = None

        # ------------------------------------------------------------------
        # PRE-PUBLISH HARD AUTO-FREEZE HOOK (P6)
        # The P5 autoguard key must become a real publish stop, not just telemetry.
        # Reuse a short in-process TTL cache to avoid one Redis GET per signal.
        # ------------------------------------------------------------------
        if veto_dec is None:
            try:
                redis_client = getattr(self.publisher, "r", None) if self.publisher is not None else None
                freeze_state = await aread_exec_health_auto_freeze(
                    redis=redis_client,
                    scope="pipeline",
                    now_ms=int(enriched_signal.get("ts_ms") or 0),
                )
                indicators["exec_health_auto_freeze_active"] = int(1 if freeze_state.active else 0)
                indicators["exec_health_auto_freeze_until_ts_ms"] = int(freeze_state.freeze_until_ts_ms or 0)
                indicators["exec_health_auto_freeze_reason"] = str(freeze_state.freeze_reason or "")
                enriched_signal["exec_health_auto_freeze_active"] = int(1 if freeze_state.active else 0)
                enriched_signal["exec_health_auto_freeze_until_ts_ms"] = int(freeze_state.freeze_until_ts_ms or 0)
                enriched_signal["exec_health_auto_freeze_reason"] = str(freeze_state.freeze_reason or "")
                if freeze_state.active:
                    # Build decision and wire into the existing veto path:
                    # rejected stream + audit stay active for triage.
                    fr_dec = build_exec_health_auto_freeze_decision(scope="pipeline", state=freeze_state)
                    veto_dec = SimpleNamespace(
                        apply=True,
                        veto=True,
                        gate=str(fr_dec.gate),
                        reason_code=str(fr_dec.reason_code),
                        notes=str(fr_dec.notes),
                    )
            except Exception:
                pass

        # ------------------------------------------------------------------
        # PRE-PUBLISH EXECUTION HEALTH (single source of truth with EntryPolicy/EdgeCost)
        # ------------------------------------------------------------------
        if veto_dec is None:
            try:
                redis_client = getattr(self.publisher, "r", None) if self.publisher is not None else None
                exec_profile = self._cached_exec_profile
                exec_session = str(session_utc(int(enriched_signal.get("ts_ms") or 0)))
                exec_tf = str(indicators.get("tf") or enriched_signal.get("tf") or signal.get("tf") or self._cached_exec_health_tf).strip().lower()
                exec_kind = str(kind or indicators.get("kind") or signal.get("kind") or "all").strip().lower()
                exec_side = str(enriched_signal.get("side") or signal.get("side") or direction or "NA").strip().upper()
                exec_venue = str(indicators.get("venue") or enriched_signal.get("venue") or signal.get("venue") or self._cached_exec_health_venue).strip().lower()
                exec_roll = {}
                if redis_client is not None:
                    try:
                        exec_roll = await aread_exec_health_rollups(
                            redis=redis_client,
                            sym=str(symbol),
                            venue=exec_venue,
                            session=exec_session,
                            tf=exec_tf,
                            kind=exec_kind,
                            side=exec_side,
                        )
                    except Exception:
                        record_exec_health_reader_error(scope="pipeline", where="read_rollups")
                        _contract_reader_err_pipeline(scope="pipeline")
                        raise
                exec_dec = decide_exec_health_from_env(profile=exec_profile, rollups=exec_roll, scope="pipeline") if exec_roll else None
                record_exec_health_observability(
                    symbol=str(symbol),
                    scope="pipeline",
                    profile=exec_profile,
                    rollups=exec_roll,
                    decision=exec_dec,
                    now_ms=int(enriched_signal.get("ts_ms") or 0),
                )
                # P4 SLO contract: record decision outcome (fail-open)
                try:
                    record_exec_health_contract_state(
                        scope="pipeline",
                        profile=str(exec_profile),
                        symbol=str(symbol),
                        decision=exec_dec,
                        now_ms=int(enriched_signal.get("ts_ms") or 0),
                    )
                    await _flush_contract_pipeline(
                        redis_client=redis_client,
                        scope="pipeline",
                    )
                except Exception:
                    pass  # never block signal publishing on contract write failure
                if exec_roll and exec_dec is not None:
                    indicators["tca_is_p95_bps"] = float(exec_roll.get("is_p95_bps", 0.0) or 0.0)
                    indicators["tca_perm_impact_p95_bps"] = float(exec_roll.get("perm_impact_p95_bps", 0.0) or 0.0)
                    indicators["tca_realized_spread_p50_bps"] = float(exec_roll.get("realized_spread_p50_bps", 0.0) or 0.0)
                    indicators["exec_health_apply"] = int(1 if exec_dec.apply else 0)
                    indicators["exec_health_veto"] = int(1 if exec_dec.veto else 0)
                    indicators["exec_health_mode"] = str(exec_dec.mode)
                    indicators["exec_health_flags"] = ",".join(exec_dec.flags)
                    indicators["exec_health_reason"] = str(exec_dec.reason_code or "")
                    indicators["exec_health_tighten_add_bps"] = float(exec_dec.tighten_add_bps or 0.0)
                    indicators["exec_health_tighten_k"] = float(exec_dec.tighten_k_mult or 1.0)
                    # propagate to enriched_signal for downstream consumers
                    enriched_signal["exec_health_apply"] = int(1 if exec_dec.apply else 0)
                    enriched_signal["exec_health_veto"] = int(1 if exec_dec.veto else 0)
                    enriched_signal["exec_health_mode"] = str(exec_dec.mode)
                    enriched_signal["exec_health_flags"] = ",".join(exec_dec.flags)
                    enriched_signal["exec_health_reason"] = str(exec_dec.reason_code or "")
                    # strict/tighten: raise expected_slippage_bps
                    if float(exec_dec.tighten_add_bps or 0.0) > 0.0:
                        exp0 = float(enriched_signal.get("expected_slippage_bps") or indicators.get("expected_slippage_bps") or 0.0)
                        exp1 = float(exp0 + float(exec_dec.tighten_add_bps))
                        enriched_signal["expected_slippage_bps"] = exp1
                        indicators["expected_slippage_bps"] = exp1
                    # hard/veto: create veto decision object
                    if exec_dec.veto:
                        veto_dec = SimpleNamespace(
                            apply=True,
                            veto=True,
                            gate="ExecutionHealthGate",
                            reason_code=str(exec_dec.reason_code or "VETO_EXEC_HEALTH"),
                            notes=f"flags={','.join(exec_dec.flags)} is_p95={exec_roll.get('is_p95_bps', 0.0)} perm_impact_p95={exec_roll.get('perm_impact_p95_bps', 0.0)} realized_spread_p50={exec_roll.get('realized_spread_p50_bps', 0.0)}",
                        )
            except Exception:
                pass

        if veto_dec is not None:
            # Metrics
            try:
                pre_publish_veto_total.labels(
                    symbol=str(symbol), gate=str(getattr(veto_dec, "gate", "UnknownGate")), reason=str(veto_dec.reason_code)
                ).inc()
            except Exception:
                pass

            # Annotate payloads
            enriched_signal["pre_publish_veto"] = True
            enriched_signal["pre_publish_gate"] = str(getattr(veto_dec, "gate", "UnknownGate"))
            enriched_signal["pre_publish_reason"] = str(veto_dec.reason_code)
            if getattr(veto_dec, "notes", None):
                enriched_signal["pre_publish_notes"] = veto_dec.notes

            # Send to rejected stream for triage (ASYNCHRONOUS)
            if self.publisher and self.publisher.r:
                safe_create_task(
                    self.publisher.r.xadd(
                        self._rejected_signal_stream,
                        fields={
                            "symbol": str(symbol),
                            "gate": str(getattr(veto_dec, "gate", "UnknownGate")),
                            "reason": str(veto_dec.reason_code),
                            "ts_ms": str(int(enriched_signal.get("ts_ms") or 0)),
                            "payload": json.dumps(enriched_signal, ensure_ascii=False),
                        },
                        maxlen=1000000,
                    ),
                    name=f"reject_{symbol}_{ts_ms}"
                )

            # Still emit audit record (deterministic) but stop before trade/notify sinks
            try:
                signal_stream = self.cryptoorderflow_signal_stream_template.format(symbol=symbol)
                audit_payload = {
                    "v": 1,
                    "is_virtual": int(enriched_signal.get("is_virtual", 0)),
                    "sid": enriched_signal.get("sid") or enriched_signal.get("signal_id") or "",
                    "signal_id": enriched_signal.get("signal_id") or "",
                    "symbol": symbol,
                    "side": enriched_signal.get("side") or direction,
                    "entry": entry,
                    "sl": sl,
                    "tp_levels": tp_levels,
                    "lot": lot,
                    "qty": float(lot),
                    "quantity": float(lot),
                    "source": "CryptoOrderFlow",
                    "reason": signal.get("reason") or "delta_spike",
                    "confidence": confidence,
                    "confidence01": confidence,
                    "confidence_pct": confidence * 100.0,
                    "atr": atr,
                    "ts": ts_ms,
                    "ts_ms": ts_ms,
                    FIELD_TS_EVENT_MS: int(enriched_signal.get(FIELD_TS_EVENT_MS) or ts_ms),
                    FIELD_TS_FEATURE_MS: int(enriched_signal.get(FIELD_TS_FEATURE_MS) or ts_ms),
                    FIELD_TS_EMIT_MS: int(enriched_signal.get(FIELD_TS_EMIT_MS) or ts_ms),
                    "trail_after_tp1": self._normalize_trailing_flag(enriched_signal.get("trail_after_tp1"), symbol),
                    "trail_profile": enriched_signal.get("trail_profile", "rocket_v1"),
                    "pre_publish_veto": True,
                    "pre_publish_gate": str(getattr(veto_dec, "gate", "UnknownGate")),
                    "pre_publish_reason": str(veto_dec.reason_code),
                    "indicators": indicators,
                    "strategy": "cryptoorderflow",
                    "tf": "tick",
                }
                preprocess_signal_for_publish(audit_payload, symbol=str(symbol), source="CryptoOrderFlow", logger=logger)
                safe_create_task(
                    self.publisher.xadd_json(
                        sink=StreamSink(name=str(signal_stream), field="data", maxlen=1000),
                        payload=audit_payload,
                        symbol=str(symbol),
                    ),
                    name=f"audit_veto_{symbol}_{ts_ms}"
                )
            except Exception:
                pass

            safe_create_task(
                self._push_virtual_to_binance_queue(
                    sid=enriched_signal.get("sid") or enriched_signal.get("signal_id") or signal.get("signal_id") or "",
                    symbol=symbol,
                    direction=direction,
                    entry=entry,
                    sl=sl,
                    tp_levels=tp_levels,
                    lot=lot,
                    ts_ms=ts_ms,
                    confidence=confidence,
                    enriched_signal=enriched_signal,
                    indicators=indicators,
                    is_rejected_signal=True,
                    rejection_reason="early_veto"
                ),
                name=f"virtual_veto_{symbol}_{ts_ms}"
            )
            return

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
            side=getattr(direction, 'value', str(direction or '')).upper(),
            risk_percent=effective_risk_pct,
            tp_price=tp_levels[0] if tp_levels else None, # ✅ Pass TP1 for profitability floor check
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
                side=getattr(direction, 'value', str(direction or '')).upper(),
                risk_percent=effective_risk_pct,
                tp_price=None, # Bypass TP1 floor check to get the mathematical risk lot
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

        gate_mode = str(indicators.get("of_gate_mode") or "").upper()

        if of_confirm_ok == 1:
            validation_status = "passed"
            validation_reason = f"OFConfirm passed ({of_confirm_reason})"
        elif of_confirm_ok == 0:
            validation_status = "failed"
            validation_reason = f"OFConfirm failed: {indicators.get('of_confirm', {}).get('reason', of_confirm_reason)}"
        else:
            # of_confirm_ok not set or OFConfirm was not evaluated
            validation_status = "bypassed"
            validation_reason = "OFConfirm not evaluated"

        enriched_signal["validation_status"] = validation_status
        enriched_signal["validation_reason"] = validation_reason

        # --- SHADOW MODE: mark main signal as virtual if validation failed or shadowed
        gate_mode = str(indicators.get("of_gate_mode") or "").upper()
        # Defensive: if validation failed, it's virtual regardless of gate mode string
        if validation_status == "failed" or indicators.get("gate_shadow_veto") or (gate_mode == "SHADOW" and validation_status == "failed"):
            enriched_signal["is_virtual"] = 1
            if indicators.get("gate_shadow_veto"):
                enriched_signal["validation_status"] = "failed"
                enriched_signal["validation_reason"] = indicators.get("gate_reason", "SHADOW_VETO")
        # ---



        crypto_signal = CryptoSignal(
            sid=signal["signal_id"],
            symbol=symbol,
            side=getattr(direction, 'value', str(direction or '')).upper(),
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
                symbol=str(symbol),
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
                    schema_version=int(self.decision_snapshot_schema_version),
                )
                safe_create_task(
                    publish_decision_snapshot(
                        publisher=self.publisher,
                        snapshot=snap,
                        stream=self.decision_snapshot_stream,
                        maxlen=int(self.decision_snapshot_stream_maxlen),
                        symbol=str(symbol),
                    ),
                    name=f"snap_{symbol}_{ts_ms}"
                )
            except Exception as e:
                logger.warning("⚠️ (%s) decision_snapshot publish failed: %s", symbol, e)

        # Build outbox envelope (dispatcher will apply notify gating itself).
        try:
            signal_stream = self.cryptoorderflow_signal_stream_template.format(symbol=symbol)
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
                "qty": float(lot),
                "quantity": float(lot),
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
                FIELD_TS_EMIT_MS: int(enriched_signal.get(FIELD_TS_EMIT_MS) or ts_ms),
                "trail_after_tp1": self._normalize_trailing_flag(enriched_signal.get("trail_after_tp1"), symbol),
                "trail_profile": enriched_signal.get("trail_profile", "rocket_v1"),
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
                # Don't publish malformed envelope
                return

            env_json = dumps_env(env)
        except Exception as err:
            logger.error(f"❌ ({symbol}) Error building outbox envelope: {err}", exc_info=True)
            env_json = ""

        # Outbox path:
        #   - outbox-only: no direct publishing (dispatcher does it)
        #   - shadow: publish legacy + outbox (audit/compare during rollout)
        if env_json and (use_outbox or shadow_outbox):
            # Atomic outbox write + meta sidecar (DecisionTrace full)
            meta_obj = None
            try:
                # минимальный ctx только для trace (чтобы не тянуть весь объект)
                ctx_min = SimpleNamespace()
                setattr(ctx_min, "ts_ms", get_ny_time_millis())
                # если symbol/kind доступны в этой области — проставьте:
                try:
                    setattr(ctx_min, "symbol", str(env.get("symbol") or env.get("sym") or ""))
                    setattr(ctx_min, "kind", str(env.get("kind") or ""))
                except Exception:
                    pass
                if trace_enabled():
                    ensure_trace(ctx_min, sid=str(sid))
                    # detector stage timing (минимально)
                    trace_gate(ctx_min, stage="detector", name="service_emit", passed=True, veto=False, reason_code="OK", duration_ms=0.0)
                    meta_obj = build_trace_sidecar_meta_from_ctx(ctx=ctx_min, sid=str(sid))
            except Exception:
                meta_obj = None

            # env_json уже готов (как раньше). Пишем его как payload_obj.
            payload_obj = env  # dict envelope
            # CRITICAL PATH: Keep outbox write SYNCHRONOUS for delivery guarantee to execution engine
            await atomic_xadd_async(
                self.publisher.r,
                stream_key=str(outbox_stream),
                signal_id=str(sid),
                payload_obj=payload_obj,
                kind=str(env.get("kind") or ""),
                symbol=str(env.get("symbol") or ""),
                ts=str(env.get("ts_ms") or ""),
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
                    sink=StreamSink(name=str(self.raw_signal_stream), field="payload", maxlen=100000),
                    payload=enriched_signal,
                    symbol=str(symbol),
                ),
                name=f"raw_stream_{symbol}_{ts_ms}"
            )

            # Feed TB Labeler (P45 fix) - Outbox Path
            # await: signals:of:inputs is consumed by ML dataset builder; drop = training data corruption.
            if self.publish_of_inputs and self.of_inputs_stream and self.of_inputs_publish_enabled:
                await self._publish_of_inputs(
                    publisher=pub,
                    enriched_signal=enriched_signal,
                    symbol=str(symbol),
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
             msg_id = int(ts_ms)
             counter_value = int(msg_id) # Proxy monotonic
             
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
        try:
             safe_create_task(
                 pub.xadd_json(
                     sink=StreamSink(name=str(self.raw_signal_stream), field="payload", maxlen=100000),
                     payload=enriched_signal,
                     symbol=str(symbol),
                 ),
                 name=f"raw_direct_{symbol}_{ts_ms}"
             )
        except Exception:
             pass

        # Feed TB Labeler (P45 fix) - Direct Path
        # signals:of:inputs is consumed by ML dataset builder — await to guarantee delivery.
        # Fire-and-forget here causes join failures in dataset_report (ML training data corruption).
        if self.publish_of_inputs and self.of_inputs_stream and self.of_inputs_publish_enabled:
            await self._publish_of_inputs(
                publisher=pub,
                enriched_signal=enriched_signal,
                symbol=str(symbol),
                path="direct",
            )


        # 3) Audit Payload via AsyncSignalPublisher
        # P99 FIX: fire-and-forget (audit is non-critical, maxlen=1000)
        preprocess_signal_for_publish(audit_payload, symbol=str(symbol), source="CryptoOrderFlow", logger=logger)
        safe_create_task(
            pub.xadd_json(
                sink=StreamSink(name=str(signal_stream), field="data", maxlen=1000),
                payload=audit_payload,
                symbol=str(symbol),
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
            runtime.last_strong_gate_scn = str(indicators.get("strong_gate_scn", "") or "")

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
        tp_levels: List[float],
        lot: float,
        ts_ms: int,
        confidence: float,
        enriched_signal: Dict[str, Any],
        indicators: Dict[str, Any],
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

        validation_status = str(enriched_signal.get("validation_status") or "").lower()
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

            # --- CONTRACT VALIDATION (OrderIntentV1) ---
            try:
                # 1) Standardize side for Execution
                side_norm = normalize_side(direction)

                # 2) Build extra meta
                meta = {
                    "is_virtual": bool(mirror_all or is_virtual_flag),
                    "mirror_all": bool(mirror_all),
                    "source": str(enriched_signal.get("source") or "CryptoOrderFlow"),
                    "strategy": str(enriched_signal.get("strategy") or "cryptoorderflow"),
                    "confidence": float(confidence),
                    "confidence_pct": float(confidence) * 100.0,
                    "trail_after_tp1": bool(enriched_signal.get("trail_after_tp1", False)),
                    "trail_profile": str(enriched_signal.get("trail_profile") or "rocket_v1"),
                    "regime": str(indicators.get("regime", "na")),
                    "atr": float(indicators.get("atr_used_for_levels") or indicators.get("atr", 0.0) or 0.0),
                    "atr_used_for_levels": float(indicators.get("atr_used_for_levels", 0.0) or 0.0),
                    "atr_tf_used": str(indicators.get("atr_tf_used", "")),
                    "sl_atr_mult": float(indicators.get("sl_atr_mult", 0.0) or 0.0),
                    "tp1_atr_mult": float(indicators.get("tp1_atr_mult", 0.0) or 0.0),
                    "sl_atr": float(indicators.get("sl_atr", 0.0) or 0.0),
                    "tp1_atr": float(indicators.get("tp1_atr", 0.0) or 0.0),
                    "validation_status": "failed" if is_rejected_signal else str(enriched_signal.get("validation_status") or ""),
                    "is_rejected_signal": 1 if is_rejected_signal else 0,
                    "rejection_reason": str(rejection_reason or ""),
                    "sl_price": float(sl),
                    "tp_levels": [float(x) for x in (tp_levels or [])]
                }
                if "tp_ratio" in enriched_signal:
                    meta["tp_ratio"] = enriched_signal["tp_ratio"]
                if "trail_activate_tp_level_requested" in enriched_signal:
                    meta["trail_activate_tp_level_requested"] = enriched_signal["trail_activate_tp_level_requested"]

                intent_v1 = OrderIntentV1(
                    intent_id=f"int:{sid}:{int(time.time()*1000)}",
                    signal_id=str(sid),
                    symbol=str(symbol),
                    ts_ms=int(ts_ms),
                    side=side_norm,
                    order_type="MARKET",
                    price=float(entry),
                    qty=float(lot),
                    meta=meta
                )
                order_payload = intent_v1.model_dump()
            except Exception as e:
                logger.warning("⚠️ (%s) OrderIntentV1 validation failed: %s", symbol, e)
                # Fallback to legacy dict if validation fails to prevent order loss
                order_payload = {
                    "action": "open",
                    "sid": str(sid),
                    "symbol": str(symbol),
                    "side": str(direction),
                    "qty": float(lot),
                    "type": "MARKET",
                    "entry": float(entry),
                    "sl": float(sl),
                    "tp_levels": [float(x) for x in (tp_levels or [])],
                    "is_virtual": 1 if mirror_all else (1 if is_virtual_flag else 0),
                    "mirror_all": bool(mirror_all),
                    "source": str(enriched_signal.get("source") or "CryptoOrderFlow"),
                    "strategy": str(enriched_signal.get("strategy") or "cryptoorderflow"),
                    "confidence": float(confidence),
                    "confidence_pct": float(confidence) * 100.0,
                    "ts_ms": int(ts_ms),
                    "trail_after_tp1": bool(enriched_signal.get("trail_after_tp1", False)),
                    "trail_profile": str(enriched_signal.get("trail_profile") or "rocket_v1"),
                    "regime": str(indicators.get("regime", "na")),
                    "atr": float(indicators.get("atr_used_for_levels") or indicators.get("atr", 0.0) or 0.0),
                    "atr_used_for_levels": float(indicators.get("atr_used_for_levels", 0.0) or 0.0),
                    "atr_tf_used": str(indicators.get("atr_tf_used", "")),
                    "sl_atr_mult": float(indicators.get("sl_atr_mult", 0.0) or 0.0),
                    "tp1_atr_mult": float(indicators.get("tp1_atr_mult", 0.0) or 0.0),
                    "sl_atr": float(indicators.get("sl_atr", 0.0) or 0.0),
                    "tp1_atr": float(indicators.get("tp1_atr", 0.0) or 0.0),
                    "validation_status": "failed" if is_rejected_signal else str(enriched_signal.get("validation_status") or ""),
                    "is_rejected_signal": 1 if is_rejected_signal else 0,
                    "rejection_reason": str(rejection_reason or ""),
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
                "allow": bool(budget_allow),
                "reason_code": str(budget_reason),
                "diag": budget_diag,
                "advisory": is_advisory,
            }
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
                            str(enriched_signal.get("source") or "CryptoOrderFlow"),
                            str(enriched_signal.get("venue") or "unknown"),
                            symbol,
                            fc,
                            str(prov.get("scenario") or enriched_signal.get("kind") or "unknown").lower(),
                            str(prov.get("regime") or meta.get("regime") or "na").lower(),
                            str(prov.get("risk_horizon_bucket") or "unknown").lower(),
                            str(enriched_signal.get("atr_policy_layer") or "stop_ttl"),
                            int(prov.get("policy_ver") or 0),
                            "deny",
                            portfolio_reason,
                            portfolio_diag
                        ))
                    except Exception as e2:
                        logger.error("Failed scheduling portfolio event record: %s", e2)

            enriched_signal.setdefault("meta", {})
            enriched_signal["meta"]["portfolio_gate"] = {
                "allow": bool(portfolio_allow),
                "reason_code": str(portfolio_reason),
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
                "allow": bool(regime_stress_allow),
                "reason_code": str(regime_stress_reason),
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
                    "allow": bool(inv_allow),
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
                "text": str(text or ""),
                "source": str(source or "report"),
                "symbol": str(symbol or ""),
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
        indicators: Optional[Dict[str, Any]],
        confirmations: Sequence[str],
    ) -> Dict[str, float]:
        mix: Dict[str, float] = {}

        # ✅ FIX: delta_z can be 0.0 (valid z-score), don't check for None only
        if delta_z is not None:
            mix["p_delta"] = self._sigmoid_abs(delta_z, k=0.5)
            mix["p_speed"] = abs(delta_z)
        # ✅ FIX: delta can be 0.0 (valid delta), don't check for None only
        if delta is not None:
            mix["delta"] = abs(delta)
        if indicators:
            if "obi" in indicators:
                mix["p_cluster"] = float(indicators.get("obi"))
            if "confidence" in indicators:
                mix["confidence"] = float(indicators.get("confidence"))

        if confirmations:
            mix["confirmations_count"] = float(len(confirmations))

        return mix

    def _normalize_trailing_flag(self, value: Any, symbol: Optional[str] = None) -> bool:
        """
        Возвращает финальный флаг трейлинга.
        """
        explicit_flag: Optional[bool] = None
        if value is not None:
            try:
                if isinstance(value, str):
                    explicit_flag = value.lower() in ("1", "true", "yes", "on")
                else:
                    explicit_flag = bool(value)
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
        indicators: Dict[str, Any],
        trail_profile: Optional[str] = None,
    ) -> Tuple[float, List[float], float, float, Dict[str, Any]]:
        cfg = runtime.config
        atr = float(indicators.get("atr", 0.0) or 0.0)
        atr_meta: Dict[str, Any] = {}
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
                        atr_ts_ms = int(atr_meta.get("ts_ms", 0) or 0)
                        if prefer_src:
                            indicators["atr_src_prefer"] = str(prefer_src)
            except Exception:

                atr = 0.0

        # Always expose atr_bps_exec for unified gates/debug
        try:
            if float(entry) > 0 and float(atr) > 0:
                indicators["atr_bps_exec"] = float(10000.0 * (float(atr) / float(entry)))
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
        indicators["atr_used_for_levels"] = float(atr)

        lot = indicators.get("lot")
        if lot is None:
            lot = indicators.get("tick_qty") or indicators.get("delta") or 1.0
            lot = max(float(lot), cfg.get("min_lot", 0.01))

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
                if float(lot) > _max_lot_by_notional:
                    lot = _max_lot_by_notional
        except Exception:
            pass

        # RISK_MAX_QTY hard cap
        try:
            _risk_max_qty = self._cached_risk_max_qty
            if _risk_max_qty > 0 and float(lot) > _risk_max_qty:
                lot = _risk_max_qty
        except Exception:
            pass

        def rr_levels(rr_str: str) -> List[float]:
            try:
                return [float(x.strip()) for x in rr_str.split(",") if x.strip()]
            except Exception:
                return [1.3, 2.0, 2.7]

        # Проверяем профиль трейлинга до расчета SL
        if not trail_profile:
            trail_profile = cfg.get("trail_profile") or indicators.get("trail_profile") or cfg.get("default_trail_profile", "protective_only")

        if str(cfg.get("stop_mode", "ATR")).upper() == "ATR":
            base_stop_mult = cfg.get("stop_atr_mult", 1.2)  # was 1.0
            if trail_profile == "expansion_v1":
                base_stop_mult = max(base_stop_mult, 2.5)
                indicators["expansion_sl_widened"] = 1
                indicators["expansion_sl_mult_used"] = round(base_stop_mult, 2)
            stop_dist = atr * base_stop_mult
        elif str(cfg.get("stop_mode", "ATR")).upper() == "PCT":
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
        # LIQMAP risk diagnostic: would we need to WIDEN SL to be beyond an
        # adverse liquidation cluster? (Risk discipline: widening => hard veto.)
        #
        # We do NOT change SL here. We only compute diagnostics/flags so that
        # publish_signal() can optionally hard-veto the trade before publishing.
        # ------------------------------------------------------------------
        try:
            base_sl_bps = (float(stop_dist) / float(entry)) * 10000.0 if float(entry) > 0.0 else 0.0
            indicators['liqmap_sl_base_bps'] = float(base_sl_bps)

            side_u = str(side).upper()
            if side_u == 'LONG':
                reco_bps = float(indicators.get('liqmap_sl_reco_bps_long') or indicators.get('liqmap_sl_reco_bps') or 0.0)
            else:
                reco_bps = float(indicators.get('liqmap_sl_reco_bps_short') or indicators.get('liqmap_sl_reco_bps') or 0.0)

            indicators['liqmap_sl_reco_bps'] = float(reco_bps)

            if base_sl_bps > 0.0 and reco_bps > 0.0:
                ratio = float(reco_bps) / float(base_sl_bps)
                indicators['liqmap_sl_widen_ratio'] = float(ratio)
                cap = self._cached_liqmap_sl_widen_cap
                indicators['liqmap_sl_widen_needed'] = 1 if ratio > cap else 0
            else:
                indicators['liqmap_sl_widen_ratio'] = 0.0
                indicators['liqmap_sl_widen_needed'] = 0
        except Exception:
            pass

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
        
        # FINAL SAFETY: Sort TPs by distance from entry to guarantee order 1 < 2 < 3
        # abs(tp - entry) makes it direction-agnostic
        tps.sort(key=lambda x: abs(x - entry))

        if float(atr) > 0 and float(entry) > 0:
            indicators["sl_atr"] = abs(float(entry) - float(sl)) / float(atr)
            indicators["tp1_atr"] = abs(float(tps[0]) - float(entry)) / float(atr)
            indicators["sl_atr_mult"] = float(stop_dist) / float(atr)
            indicators["tp1_atr_mult"] = abs(float(tps[0]) - float(entry)) / float(atr)

        return sl, tps, float(lot), float(atr), atr_meta

    async def send_telegram_report(self, text: str, source: str, symbol: str) -> None:
        """
        Отправляет телеграм отчет в notify stream.

        :param text: Текст отчета
        :param source: Источник отчета
        :param symbol: Символ/инструмент
        """
        try:
            await self.publisher.r.xadd(
                self.notify_stream,
                fields={"type": "report", "text": text, "source": source, "symbol": symbol, "ts_ms": str(get_ny_time_millis())},
                maxlen=self.notify_maxlen,
                approximate=True,
            )
            logger.info("📱 [TELEGRAM-REPORT] Sent report for %s from %s", symbol, source)
        except Exception as exc:
            logger.warning("⚠️ [TELEGRAM-REPORT] Failed to send report for %s: %s", symbol, exc)
