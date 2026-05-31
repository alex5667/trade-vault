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
from handlers.crypto_orderflow.utils.entry_policy_gate import EntryPolicyGate
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
from services.ev_tp1_stats import EvTp1StatsConfig, attach_tp1_hit_prob_to_ctx
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
    from prometheus_client import Histogram, Counter, Gauge
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
    _PRE_PUBLISH_GATE_EVAL_TOTAL = Counter(
        "pre_publish_gate_eval_total",
        "Total pre-publish gate evaluations (all decisions incl. ALLOW/ABSTAIN/DENY)",
        ["gate", "decision", "symbol", "kind"]
    )
    _INVALID_ENVELOPE_TOTAL = Counter(
        "outbox_invalid_envelope_total",
        "Total outbox envelopes rejected due to malformed structure",
        ["symbol"]
    )
    _G9_CONFIDENCE_MISSING_TOTAL = Counter(
        "g9_confidence_missing_total",
        "Signals where confidence was absent (fallback to 0.3); likely a contract bug upstream",
        ["symbol"]
    )
    _BARRIER_DECISION_TOTAL = Counter(
        "confirmation_barrier_total",
        "Confirmation barrier decisions (ALLOW/DROP/SHADOW_ALLOW/SHADOW_DROP)",
        ["symbol", "decision", "reason_code"],
    )
    _BARRIER_PENDING = Gauge(
        "confirmation_barrier_pending",
        "Signals currently held in confirmation barrier awaiting deadline",
    )
    _VIRTUAL_THRESHOLD_CANDIDATE_TOTAL = Counter(
        "virtual_threshold_candidate_total",
        "Signals that entered virtual order routing logic",
        ["symbol", "meets_virtual_threshold"],
    )
    _VIRTUAL_ORDER_SKIPPED_BAD_DQ_TOTAL = Counter(
        "virtual_order_skipped_bad_dq_total",
        "Virtual order pushes suppressed due to DQ/time/integrity veto",
        ["reason"],
    )
    _VIRTUAL_ORDER_INVALID_LEVELS_TOTAL = Counter(
        "virtual_order_invalid_levels_total",
        "Virtual order pushes suppressed due to zero/missing entry/sl/lot",
        ["reason"],
    )
    # Policy-aware exploration metrics (plan §6)
    _DEEP_EXPLORE_SAMPLE_TOTAL = Counter(
        "deep_explore_sample_total",
        "Signals evaluated for deep_explore_20_35 bucket (accepted=1 passed cap+rate, 0=dropped)",
        ["symbol", "accepted"],
    )
    _VIRTUAL_SAMPLE_COUNT_BY_POLICY = Counter(
        "virtual_sample_count_total",
        "Virtual/exploration samples recorded by policy bucket",
        ["sample_policy", "symbol", "regime", "session"],
    )
    _TP_PROFILE_ENFORCE_TOTAL = Counter(
        "trade_profile_tp_enforce_total",
        "Signals where TradeProfile TP geometry was enforced (applied to _calculate_levels cfg)",
        ["profile", "symbol"],
    )
    _TP_PROFILE_SHADOW_TOTAL = Counter(
        "trade_profile_tp_shadow_total",
        "Signals where TradeProfile TP geometry was shadow-only (indicators only, not applied)",
        ["profile", "symbol"],
    )
    _TP_PROFILE_LEVEL_DELTA_BPS = Histogram(
        "trade_profile_tp_level_delta_bps",
        "Absolute difference in TP1 price level between profile and default (basis points)",
        ["profile", "symbol"],
        buckets=[0, 5, 10, 20, 50, 100, 200, 500],
    )
    _REGIME_RESOLVED_TOTAL = Counter(
        "signals_of_inputs_regime_resolved_total",
        "Source of regime value at signals:of:inputs publish time (P1.1)",
        ["kind", "source"],
    )
    # 2026-05-27 WR stop-bleed: drop counter for virtual signals with hard
    # veto / failed validation. Gated by VIRTUAL_GATE_HARD_DROP_ENABLED.
    _VIRTUAL_HARD_DROP_TOTAL = Counter(
        "virtual_hard_drop_total",
        "Virtual signals dropped pre-publish due to hard veto / failed validation",
        ["symbol", "reason", "mode"],
    )
    # 2026-05-30 gate_value_autocal applied-delta wiring.
    # Counts and value of min_conf overrides taken from
    # `cfg:gate_value_autocal:applied:{kind}|{symbol}|{horizon_ms}`.
    _APPLIED_DELTA_OVERRIDE_TOTAL = Counter(
        "applied_delta_override_total",
        "min_conf overrides from gate_value_autocal applied_delta",
        ["kind", "symbol", "phase", "delta_direction"],
    )
    _APPLIED_DELTA_OVERRIDE_VALUE = Gauge(
        "applied_delta_override_value",
        "Last applied min_conf delta (signed, fraction)",
        ["kind", "symbol", "phase"],
    )
except ImportError:
    _FEATURE_TO_DECISION_MS = None
    _DECISION_TO_OUTBOX_MS = None
    _PRE_PUBLISH_VETO_TOTAL = None
    _PRE_PUBLISH_GATE_EVAL_TOTAL = None
    _INVALID_ENVELOPE_TOTAL = None
    _G9_CONFIDENCE_MISSING_TOTAL = None
    _BARRIER_DECISION_TOTAL = None
    _BARRIER_PENDING = None
    _VIRTUAL_THRESHOLD_CANDIDATE_TOTAL = None
    _VIRTUAL_ORDER_SKIPPED_BAD_DQ_TOTAL = None
    _VIRTUAL_ORDER_INVALID_LEVELS_TOTAL = None
    _DEEP_EXPLORE_SAMPLE_TOTAL = None
    _VIRTUAL_SAMPLE_COUNT_BY_POLICY = None
    _TP_PROFILE_ENFORCE_TOTAL = None
    _TP_PROFILE_SHADOW_TOTAL = None
    _TP_PROFILE_LEVEL_DELTA_BPS = None
    _REGIME_RESOLVED_TOTAL = None
    _VIRTUAL_HARD_DROP_TOTAL = None
    _APPLIED_DELTA_OVERRIDE_TOTAL = None
    _APPLIED_DELTA_OVERRIDE_VALUE = None

# Gates whose vetoes must never route to virtual order queue.
# Checked via rejection_gate parameter (dec.gate passed by _handle_pipeline_veto).
_VIRTUAL_ORDER_SKIP_GATES: frozenset[str] = frozenset({
    "HardDataQualityGate",
    "StreamIntegrityGate",
    "BookSanityGate",
})

# Reason codes whose vetoes must never route to virtual order queue.
# DQ/time/integrity/book-sanity failures mean the signal is structurally invalid,
# not just weak. Coverage: Stage-1 gates (HardDataQualityGate / StreamIntegrityGate)
# fire BEFORE _calculate_levels, so entry/sl/lot are still zero — Guard 2
# (invalid-levels check) provides independent defence-in-depth.
# BookSanityGate reasons added so malformed book signals are also blocked.
# The audit stream (signals:cryptoorderflow:{symbol}) still records them.
_VIRTUAL_ORDER_SKIP_REASONS: frozenset[str] = frozenset({
    # HardDataQualityGate / StreamIntegrityGate
    "VETO_BAD_TS_NOT_EPOCH",
    "VETO_ATR_TS_MISSING",
    "VETO_ATR_STALE",
    "VETO_TOUCH_STALE",
    "VETO_QUALITY_FLAG",
    "VETO_STREAM_INTEGRITY",
    "VETO_SEQ_GAP_WINDOW",
    "VETO_SEQ_GAP_RATE",
    # BookSanityGate
    "VETO_BOOK_SANITY",
    "VETO_BOOK_CROSS",
    "VETO_BOOK_NAN",
    "VETO_BOOK_NEG_QTY",
    "VETO_TRADE_OUTSIDE_BBO",
})

# 2026-05-27 WR stop-bleed: virtual hard-drop gate.
# Trade report showed 147/198 signals "bypassed" (OFConfirm not evaluated for
# virtual) yet 140 became open positions with WR=1.6%. This helper decides
# whether a virtual signal should be dropped BEFORE outbox envelope build.
#
# Drop conditions (any of):
#   - validation_status == "failed" (OFConfirm explicitly failed OR real signal
#     missing confirmation got marked failed → virtual)
#   - validation_status == "bypassed" (virtual, OFConfirm not evaluated)
#   - hard_veto reason in v_gate_reason / validation_reason
#
# ENV:
#   VIRTUAL_GATE_HARD_DROP_ENABLED ("0"/"1", default "0") — master switch
#   VIRTUAL_GATE_HARD_DROP_SHADOW  ("0"/"1", default "1") — when 1, telemetry
#                                   only (counter mode="shadow"); when 0, drop
def should_drop_virtual_signal(enriched_signal: dict[str, Any]) -> tuple[bool, str]:
    """Return (should_drop, reason_label).

    Decision is pure on payload — env-aware caller controls enforce vs shadow.
    Reads `is_virtual`, `validation_status`, `validation_reason`, `v_gate_reason`.
    Non-virtual signals are never dropped (reason=""). Returns (False, "") if
    the signal does not match any drop condition.
    """
    try:
        is_virtual = bool(int(enriched_signal.get("is_virtual", 0) or 0))
    except Exception:
        is_virtual = False
    if not is_virtual:
        return False, ""
    vstatus = str(enriched_signal.get("validation_status") or "").lower()
    if vstatus == "failed":
        return True, "validation_failed"
    if vstatus == "bypassed":
        return True, "validation_bypassed"
    reason_blob = " ".join(
        str(enriched_signal.get(k) or "")
        for k in ("v_gate_reason", "validation_reason")
    ).upper()
    if "VETO_" in reason_blob or "HARD_VETO" in reason_blob:
        return True, "hard_veto"
    return False, ""


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

        # Shadow recording: confidence-gated-out signals → Redis stream for
        # later outcome pairing. Stage-1 of «is the confidence gate worth keeping?»
        # investigation. See docs/PHASE2_SCORER_MULTIPLIERS_REDESIGN.md and
        # docs/PHASE3_ML_FUSION_REDESIGN.md.
        self.gated_out_shadow_enabled = bool(int(os.getenv("SHADOW_GATED_OUT_ENABLE", "1") or 1))
        self.gated_out_shadow_stream = os.getenv("SIGNAL_GATED_OUT_STREAM", RS.SIGNAL_GATED_OUT)
        self.gated_out_shadow_maxlen = int(os.getenv("SIGNAL_GATED_OUT_MAXLEN", "100000") or 100000)

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
        # P0 join recovery: keep a materially longer hot buffer so trades:closed
        # can still find their originating signals during live dataset builds.
        # Historical retention should still come from archive/DB, but the Redis
        # default must not collapse coverage to minutes/hours.
        self.of_inputs_stream_maxlen = int(os.getenv("OF_INPUTS_STREAM_MAXLEN", "1000000") or 1000000)
        # ML training metadata — injected into every of:inputs record for dataset filtering
        self.ml_feature_schema_version = int(os.getenv("ML_FEATURE_SCHEMA_VERSION", "14") or 14)

        self._rejected_signal_stream = os.getenv("CRYPTO_REJECTED_SIGNAL_STREAM", RS.CRYPTO_REJECTED)
        self._rejected_signal_maxlen = int(os.getenv("CRYPTO_REJECTED_MAXLEN", "5000"))

        # Unified Orchestrator (P1)
        # EntryPolicyGate: enabled by default — drives autocal:adverse_cross / spread_staleness /
        # burst_c2t accumulation and freeze enforcement. Toggle via ENTRY_POLICY_ENABLED=0 env.
        _cost_gate = EdgeCostGate.from_env()
        # Wire calibrated slippage reader: q75(adverse_bps_t) per (symbol × session).
        # Fail-open: if key missing or Redis unavailable, gate falls back to EDGE_SLIPPAGE_BPS_DEFAULT.
        if os.getenv("EDGE_SLIPPAGE_CAL_ENABLED", "1").lower() in {"1", "true", "yes", "on"}:
            try:
                from core.slippage_cal_store import SlippageCalReader
                _slip_redis_url = (
                    os.getenv("SLIP_CAL_REDIS_URL")
                    or os.getenv("REDIS_WORKER_1_URL")
                    or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
                )
                from core.redis_client import get_redis as _get_redis
                _cost_gate.set_slippage_store(SlippageCalReader(_get_redis(_slip_redis_url)))
            except Exception as _e:
                import logging as _log
                _log.getLogger(__name__).warning("SlippageCalReader init failed (fail-open): %s", _e)

        self.orchestrator = GateOrchestrator(
            entry_policy=EntryPolicyGate.from_env(),
            cost_gate=_cost_gate,
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
        self._ev_tp1_cfg = EvTp1StatsConfig.from_env()

        # ------------------------------------------------------------------
        # Pipeline-level calibrators (observed on emit, snapshotted to Redis)
        # ------------------------------------------------------------------
        # These calibrators were historically wired in the legacy SignalOrchestrator
        # (multi-symbol-orderflow service, now disabled). Wired here so the new
        # pipeline path (used by scanner-crypto-orderflow*) also persists them.
        from core.cooldown_calibrator import CooldownCalibrator
        from core.vol_z_thr_calibrator import VolZThrCalibrator
        from core.htf_proximity_calibrator import HtfProximityCalibrator
        from core.liquidity_wall_calibrator import LiquidityWallCalibrator
        from core.dq_microstructure_calibrator import DqMicrostructureCalibrator
        from core.confirmation_barrier_calibrator import ConfirmationBarrierCalibrator

        self._cooldown_calib = CooldownCalibrator(
            min_signals=int(os.getenv("COOLDOWN_CAL_MIN_SIGNALS", "100") or "100"),
            enforce=(os.getenv("COOLDOWN_CAL_ENFORCE", "0") or "0").strip().lower() in {"1", "true", "yes", "on"},
        )
        self._vol_z_calib = VolZThrCalibrator()
        self._htf_prox_calib = HtfProximityCalibrator()
        self._liq_wall_calib = LiquidityWallCalibrator()
        # 2026-05-27 P2.4: env-tunable min_samples (default 30 → 10 для cold-start).
        # Audit 2026-05-26: bins={} пусто — observations не накапливались. Снижение
        # позволяет per-(sym,kind) bin промотироваться быстрее, пока fix observation
        # producer ещё не приехал. Не уменьшать ниже 5.
        try:
            _cb_min_samples = max(5, int(os.getenv("CONFIRMATION_BARRIER_CAL_MIN_SAMPLES", "10") or 10))
        except Exception:
            _cb_min_samples = 10
        self._confirm_barrier_cal = ConfirmationBarrierCalibrator(min_samples=_cb_min_samples)
        # Cache hot-path DQ thresholds here (also re-assigned below with the rest of
        # the _cached_* block — that's fine, same value from the same env var).
        self._cached_dq_book_stale_flag_ms = int(os.getenv("DQ_BOOK_STALE_FLAG_MS", "1500") or 1500)
        self._cached_dq_spread_wide_flag_bps = float(os.getenv("DQ_SPREAD_WIDE_FLAG_BPS", "12") or 12.0)

        self._dq_micro_cal = DqMicrostructureCalibrator(
            min_samples=int(os.getenv("DQ_MICRO_CAL_MIN_SAMPLES", "200") or 200),
            enforce=os.getenv("DQ_MICRO_CAL_ENFORCE", "0").lower() in {"1", "true", "yes", "on"},
            auto_promote=os.getenv("DQ_MICRO_CAL_AUTO_PROMOTE", "1").lower() not in {"0", "false", "no", "off"},
            auto_promote_min_hours=float(os.getenv("DQ_MICRO_CAL_AUTO_PROMOTE_MIN_HOURS", "0.5") or 0.5),
            default_stale_ms=self._cached_dq_book_stale_flag_ms,
            default_spread_bps=self._cached_dq_spread_wide_flag_bps,
        )

        self._calib_loaded: bool = False
        self._calib_last_snap_ms: int = 0
        self._calib_snap_interval_ms: int = int(os.getenv("PIPELINE_CALIB_SNAP_SEC", "60") or "60") * 1000

        # ------------------------------------------------------------------
        # Confirmation Barrier (audit 2026-05-18 fix #E)
        # ------------------------------------------------------------------
        # Defers publish of just-emitted signals by N ms (or until next-bar)
        # to require directional follow-through. Default mode=off — explicit
        # opt-in via env CONFIRMATION_BARRIER_MODE=shadow|enforce.
        from core.confirmation_barrier import ConfirmationBarrier
        self._barrier = ConfirmationBarrier()
        # When a signal is re-published from barrier.poll(), we mark it so
        # publish_signal does NOT submit it again (infinite-loop guard).
        self._BARRIER_RESOLVED_KEY = "_barrier_resolved"

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
        # Separate lower threshold for virtual/ML data collection only.
        # Live gate stays at CRYPTO_SIGNAL_MIN_CONF. Setting this below
        # live threshold widens the training distribution without relaxing
        # the real execution path.
        self._cached_virtual_min_conf_pct = float(
            os.getenv("VIRTUAL_SIGNAL_MIN_CONF", str(self._cached_min_conf_pct))
        )
        # Autocalibrator reader for adaptive per-(kind × regime) confidence threshold.
        # Enabled via AUTOCAL_SIGNAL_CONF_READ_ENABLED=1 after calibrator warms up.
        # Fail-open: cold/stale/disabled → None → ENV fallback to CRYPTO_SIGNAL_MIN_CONF.
        self._signal_conf_autocal_enabled = os.getenv("AUTOCAL_SIGNAL_CONF_READ_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
        self._signal_conf_reader: Any = None
        try:
            if self._signal_conf_autocal_enabled:
                from services.signal_min_conf_runtime_overrides import get_reader as _get_sc_reader
                self._signal_conf_reader = _get_sc_reader()
        except Exception:
            self._signal_conf_reader = None

        # gate_value_autocal applied-delta reader (Stage 6 ENFORCED writes).
        # Reads `cfg:gate_value_autocal:applied:{kind}|{symbol}|{horizon_ms}`
        # and applies `min_conf_delta` after all other min_conf calculations.
        # Disabled by default (AUTOCAL_APPLIED_DELTA_READ_ENABLED=0); rollout
        # governor's STAGE_6_ENFORCED state controls the upstream WRITE side
        # via cfg:gva:enforce — this reader controls the READ side.
        self._applied_delta_reader: Any = None
        try:
            if os.getenv("AUTOCAL_APPLIED_DELTA_READ_ENABLED", "0").lower() in {"1", "true", "yes", "on"}:
                from services.signal_min_conf_applied_delta_reader import get_reader as _get_ad_reader
                self._applied_delta_reader = _get_ad_reader()
        except Exception:
            self._applied_delta_reader = None

        # W4: funding_z / basis_bps autocal reader (AUTOCAL_FUNDING_Z_READ_ENABLED=0 default)
        self._funding_z_reader: Any = None
        try:
            if os.getenv("AUTOCAL_FUNDING_Z_READ_ENABLED", "0").lower() in {"1", "true", "yes", "on"}:
                from services.funding_basis_z_runtime_overrides import get_reader as _get_fz_reader
                self._funding_z_reader = _get_fz_reader()
        except Exception:
            self._funding_z_reader = None

        # W4: sl_atr_floor autocal reader (AUTOCAL_SL_ATR_FLOOR_READ_ENABLED=0 default)
        self._sl_atr_floor_reader: Any = None
        try:
            if os.getenv("AUTOCAL_SL_ATR_FLOOR_READ_ENABLED", "0").lower() in {"1", "true", "yes", "on"}:
                from services.sl_atr_floor_runtime_overrides import get_reader as _get_slf_reader
                self._sl_atr_floor_reader = _get_slf_reader()
        except Exception:
            self._sl_atr_floor_reader = None

        # W5: DQ soft-flag autocal reader (AUTOCAL_DQ_SOFT_FLAG_READ_ENABLED=0 default)
        self._dq_softflag_reader: Any = None
        try:
            if os.getenv("AUTOCAL_DQ_SOFT_FLAG_READ_ENABLED", "0").lower() in {"1", "true", "yes", "on"}:
                from services.dq_softflag_runtime_overrides import get_reader as _get_dqsf_reader
                self._dq_softflag_reader = _get_dqsf_reader()
        except Exception:
            self._dq_softflag_reader = None

        # W5: TP size fraction autocal reader (AUTOCAL_TP_SIZE_FRAC_READ_ENABLED=0 default)
        self._tp_size_frac_reader: Any = None
        try:
            if os.getenv("AUTOCAL_TP_SIZE_FRAC_READ_ENABLED", "0").lower() in {"1", "true", "yes", "on"}:
                from services.tp_size_fraction_runtime_overrides import get_reader as _get_tpsf_reader
                self._tp_size_frac_reader = _get_tpsf_reader()
        except Exception:
            self._tp_size_frac_reader = None

        # ------------------------------------------------------------------
        # Deep Exploration Sampling (plan §7, Phase 2)
        # ------------------------------------------------------------------
        # Signals in [DEEP_EXPLORATION_MIN_CONF, VIRTUAL_SIGNAL_MIN_CONF) are
        # sampled at a low rate for ML outcome tracking ONLY.
        # NEVER routes to virtual order queue (DEEP_EXPLORATION_TO_ORDER_QUEUE=0).
        # Rollback: set DEEP_EXPLORATION_SAMPLE_RATE=0
        self._cached_deep_explore_min_conf_pct = float(
            os.getenv("DEEP_EXPLORATION_MIN_CONF", "20")
        )
        self._cached_deep_explore_sample_rate = float(
            os.getenv("DEEP_EXPLORATION_SAMPLE_RATE", "0.0")
        )
        # Safety: deep_explore NEVER goes to order queue by default.
        self._cached_deep_explore_to_queue = os.getenv(
            "DEEP_EXPLORATION_TO_ORDER_QUEUE", "0"
        ).lower() in {"1", "true", "yes", "on"}
        # Cap: max deep_explore samples per (symbol, rounded_hour, regime)
        self._cached_deep_explore_cap_per_slot = int(
            os.getenv("DEEP_EXPLORATION_CAP_PER_SYMBOL_SESSION_HOUR", "50")
        )
        # In-memory cap counters: key=(symbol, hour_bucket, regime) → count
        # Rotated by hour; bounded by number of active symbols × regimes.
        self._deep_explore_cap_counters: dict[tuple[str, int, str], int] = {}

        self._cached_fees_bps_rt = float(os.getenv("FEES_BPS_RT", "10"))
        self._cached_tp_bps_buffer = float(os.getenv("TP_BPS_BUFFER", "4"))

        # cfg:trade_profile:* Redis overrides — refreshed every 60 s
        self._profile_overrides: dict[str, Any] = {}
        self._profile_overrides_ts: float = 0.0
        self._profile_overrides_ttl: float = float(os.getenv("TRADE_PROFILE_OVERRIDES_TTL_S", "60"))

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
        # Autocalibrator reader (AUTOCAL_FLOW_TOX_READ_ENABLED=1 для включения).
        # Per-symbol пороги из autocal:flow_toxicity:state перекрывают ENV 0.0 после прогрева.
        # fail-open: cold/stale → (0.0, 0.0) → gate pass-through.
        self._flow_tox_autocal_enabled = os.getenv("AUTOCAL_FLOW_TOX_READ_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
        try:
            if self._flow_tox_autocal_enabled:
                from services.flow_toxicity_runtime_overrides import get_reader as _ftox_reader
                self._flow_tox_reader = _ftox_reader()
            else:
                self._flow_tox_reader = None
        except Exception:
            self._flow_tox_reader = None
        self._cached_flow_cap = float(os.getenv("FLOW_TOX_TIGHTEN_ADD_CAP_BPS", "6.0") or 6.0)
        self._cached_flow_mult = float(os.getenv("FLOW_TOX_TIGHTEN_ADD_MULT", "1.0") or 1.0)
        self._cached_flow_veto_wo_tca = bool(int(os.getenv("FLOW_TOX_VETO_WITHOUT_TCA", "0") or 0))
        self._cached_flow_thr_is = float(os.getenv("EXEC_MAX_IS_P95_BPS", "0") or 0.0)
        self._cached_flow_thr_imp = float(os.getenv("EXEC_MAX_PERM_IMPACT_P95_BPS", "0") or 0.0)

        self._cached_manip_enabled = os.getenv("MANIP_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
        _manip_mode_override = (os.getenv("MANIP_MODE", "") or "").strip().lower()
        if _manip_mode_override == "auto":
            _manip_mode_override = ""
        self._cached_manip_profile = _manip_mode_override or os.getenv("MANIP_GATE_PROFILE", os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
        self._cached_manip_thr_qs = float(os.getenv("MANIP_QUOTE_STUFF_SCORE_MAX", "0") or 0.0)
        self._cached_manip_thr_lay = float(os.getenv("MANIP_LAYERING_SCORE_MAX", "0") or 0.0)
        self._cached_manip_thr_otr_z = float(os.getenv("MANIP_OTR_Z_MAX", "0") or 0.0)
        self._cached_manip_tighten_cap = float(os.getenv("MANIP_TIGHTEN_ADD_CAP_BPS", "6.0") or 6.0)
        self._cached_manip_tighten_mult = float(os.getenv("MANIP_TIGHTEN_ADD_MULT", "1.0") or 1.0)
        
        self._manip_autocal_enabled = os.getenv("AUTOCAL_MANIP_READ_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
        self._manip_reader: Any = None
        try:
            if self._manip_autocal_enabled:
                from services.manip_gate_runtime_overrides import get_reader as _get_manip_reader
                self._manip_reader = _get_manip_reader()
        except Exception:
            pass

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

        # P2 ctx_tighten autocalibrator reader — tri-state:
        #   "off"  → disabled entirely (AUTOCAL_CTX_TIGHTEN_READ_ENABLED=0)
        #   "auto" → apply caps only when calibrator sets auto_promoted=True in snapshot
        #   "on"   → always apply caps (AUTOCAL_CTX_TIGHTEN_READ_ENABLED=1)
        # Default: "auto" — picks up enforce automatically once n_tightened≥50.
        _actr_raw = os.getenv("AUTOCAL_CTX_TIGHTEN_READ_ENABLED", "auto").strip().lower()
        if _actr_raw in {"0", "false", "no", "off"}:
            self._ctx_tighten_autocal_mode = "off"
        elif _actr_raw in {"1", "true", "yes", "on"}:
            self._ctx_tighten_autocal_mode = "on"
        else:
            self._ctx_tighten_autocal_mode = "auto"
        self._ctx_tighten_autocal_enabled = self._ctx_tighten_autocal_mode != "off"
        self._ctx_tighten_autocal_last_ms: int = 0
        self._ctx_tighten_autocal_ttl_ms: int = 300_000  # refresh every 5 min

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

        # Cross-venue autocalibrator reader (adaptive disloc_z / min_agree)
        # Enabled via AUTOCAL_CROSSVENUE_READ_ENABLED=1 after feed service warms up.
        try:
            from core.cross_venue_calib_reader import get_reader as _get_cv_calib_reader
            self._cv_calib_reader = _get_cv_calib_reader()
        except Exception:
            self._cv_calib_reader = None

        self._cached_binance_trail_atr_mult = os.getenv("BINANCE_TRAIL_ATR_MULT", "1.0")
        self._cached_range_tp_rr = os.getenv("RANGE_TP_RR", "1.0,1.5")
        self._cached_tp1_min_rr_floor = float(os.getenv("TP1_MIN_RR_FLOOR", "1.0") or 1.0)
        # ── TP1 target R override (2026-05-19) ────────────────────────────
        # Цель: TP1 на 0.5R при avg_MFE/avg_SL≈0.78 (вместо текущего ~1.0R).
        # Поведение:
        #   TP1_TARGET_R=0.0 (default)         — disabled (текущее поведение).
        #   TP1_TARGET_R>0 + ENFORCE=0 (SHADOW) — пишет counterfactual поля
        #                                        tp1_target_r_dist/price в
        #                                        indicators, payload НЕ меняется.
        #   TP1_TARGET_R>0 + ENFORCE=1         — применяет как первый TP-level
        #                                        в range (через RANGE_TP_RR prepend)
        #                                        и через TP1_MIN_RR_FLOOR override.
        # Quantify через replay перед ENFORCE.
        # autocal runtime override > ENV (`tp1_target_r`, `tp1_target_r_enforce`).
        _env_tp1_target_r = float(os.getenv("TP1_TARGET_R", "0.0") or 0.0)
        _env_tp1_target_r_enforce = os.getenv(
            "TP1_TARGET_R_ENFORCE", "0"
        ).lower() in ("1", "true", "yes", "on")
        try:
            from services.tp_sl_trailing_runtime_overrides import get_override
            self._cached_tp1_target_r = float(
                get_override("tp1_target_r", _env_tp1_target_r)
            )
            self._cached_tp1_target_r_enforce = bool(
                get_override("tp1_target_r_enforce", _env_tp1_target_r_enforce)
            )
        except Exception:
            self._cached_tp1_target_r = _env_tp1_target_r
            self._cached_tp1_target_r_enforce = _env_tp1_target_r_enforce

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

    async def _publish_of_inputs(
        self,
        *,
        publisher: AsyncSignalPublisher,
        enriched_signal: dict[str, Any],
        symbol: str,
        path: str,
        runtime: SymbolRuntime | None = None,
    ) -> None:
        try:
            # Inject ML training metadata so dataset builder can filter/join correctly.
            # All setdefault — never overwrites values already present in the signal.
            enriched_signal.setdefault("feature_schema_version", self.ml_feature_schema_version)
            _inds = enriched_signal.get("indicators")
            if isinstance(_inds, dict):
                # tick_qty — bridge from top-level signal field (set in publish_signal),
                # fallback to 0.0 (also applies for veto-path signals that bypass publish_signal).
                # Use direct assignment (not setdefault) so that an existing None is overridden —
                # setdefault(key, 0.0) silently skips when key is present but value is None.
                if _inds.get("tick_qty") is None:
                    _tq_raw = enriched_signal.get("tick_qty")
                    try:
                        _inds["tick_qty"] = float(_tq_raw) if _tq_raw is not None else 0.0
                    except (TypeError, ValueError):
                        _inds["tick_qty"] = 0.0
                # strong_gate_ok — 0.0 = gate not run / not applicable for non-tick-decision paths.
                # tick_decision_engine sets it explicitly to 0 or 1 when it runs; all other paths
                # get 0.0. Same None-override logic: setdefault skips None.
                if _inds.get("strong_gate_ok") is None:
                    _inds["strong_gate_ok"] = 0.0
                if runtime is not None:
                    try:
                        dcfg = getattr(runtime, "dynamic_cfg", {}) or {}
                        _vr = float(dcfg.get(DK.VOL_RATIO, _inds.get("vol_ratio", 0.0)) or 0.0)
                        _vz = float(dcfg.get(DK.VOL_RATIO_Z, _inds.get("vol_ratio_z", 0.0)) or 0.0)
                        if _vr != 0.0:
                            _inds.setdefault("vol_ratio", _vr)
                        if _vz != 0.0:
                            _inds.setdefault("vol_ratio_z", _vz)
                        # spread_bps_z: ML Feature Bridge in publish_signal() only runs for
                        # pass-path signals (~0% of training data at current gate thresholds).
                        # Bridge here covers veto-path signals (99%+).
                        # NB: write unconditionally — robust z-score of 0.0 is a valid value
                        # (spread is at its EMA mean), not a "missing" sentinel. The earlier
                        # `if _sz != 0.0` guard caused coverage dropouts whenever last_spread_z
                        # was exactly 0 on cold restart (v15_of audit 2026-05-28).
                        _inds.setdefault("spread_bps_z", float(getattr(runtime, "last_spread_z", 0.0) or 0.0))
                        # trend_score / range_score: ML Feature Bridge in publish_signal() only.
                        # Veto-path signals bypass it entirely, leaving both always 0.0 in
                        # training data. Bridge from runtime._last_regime_score (updated every
                        # microbar by strategy.update_regime) so all signal paths get the value.
                        _rg = float(getattr(runtime, "_last_regime_score", 0.0) or 0.0)
                        if _rg != 0.0:
                            _inds.setdefault("trend_score", max(0.0, _rg))
                            _inds.setdefault("range_score", max(0.0, -_rg))
                        # regime_id / regime_score / regime_confidence / regime_age_ms / regime_stale:
                        # Strategy sets these on indicators only at microbar close → only ~1% of
                        # signals in signals:of:inputs have them. Bridge from runtime attrs so
                        # ALL tick-path and veto-path signals carry the full regime feature set.
                        _rid = getattr(runtime, "_last_regime_id", None)
                        if _rid is not None and _rid != 0.0:
                            _inds.setdefault("regime_id", float(_rid))
                        if _rg != 0.0:
                            _inds.setdefault("regime_score", _rg)
                            _inds.setdefault("regime_confidence", abs(_rg))
                        _rts = getattr(runtime, "_last_regime_ts_ms", 0) or 0
                        if _rts > 0:
                            _now_ms_r = int(
                                _inds.get("ts_ms", 0) or enriched_signal.get("ts_ms", 0) or 0
                            )
                            if _now_ms_r > 0:
                                _inds.setdefault("regime_age_ms", int(_now_ms_r - _rts))
                        _inds.setdefault("regime_stale", 0)
                        # regime_micro_1m / regime_micro_age_ms: fast 5-bar micro-regime,
                        # computed inline by bar_processor._update_regime_micro on every 1m close.
                        # Bridges ALL signal paths (veto + tick) so training data is complete.
                        _rm = str(getattr(runtime, "last_regime_micro", "") or "").strip().lower()
                        _rm_ts = int(getattr(runtime, "last_regime_micro_ts_ms", 0) or 0)
                        if _rm and _rm not in ("na", "none", ""):
                            _inds.setdefault("regime_micro_1m", _rm)
                            _now_ms_rm = int(
                                _inds.get("ts_ms", 0)
                                or enriched_signal.get("ts_ms", 0)
                                or 0
                            )
                            if _now_ms_rm > 0 and _rm_ts > 0:
                                _inds.setdefault("regime_micro_age_ms", max(0, _now_ms_rm - _rm_ts))
                        # trade_msg_rate_hz/z: strategy.py sets these from runtime attrs, but
                        # iceberg/delta_spike service instances may not run that strategy block.
                        # Bridge as fallback so all veto-path signals carry the rate features.
                        _tmr = float(getattr(runtime, "trade_msg_rate_hz", 0.0) or 0.0)
                        _tmz = float(getattr(runtime, "trade_msg_rate_z", 0.0) or 0.0)
                        if _tmr != 0.0:
                            _inds.setdefault("trade_msg_rate_hz", _tmr)
                        if _tmz != 0.0:
                            _inds.setdefault("trade_msg_rate_z", _tmz)
                        # cancel_rate_z: book_processor sets runtime.cancel_rate_z from
                        # MessageRateTracker but never bridges to indicators; bridge here
                        # so veto/tick signals both carry the L3 cancel-rate z-score.
                        _crz = float(getattr(runtime, "cancel_rate_z", 0.0) or 0.0)
                        if _crz != 0.0:
                            _inds.setdefault("cancel_rate_z", _crz)
                        # Only bridge non-zero values: enricher (_enrich_vol_features) produces
                        # vol_fast_bps/vol_slow_bps from microstruct:ctx; setdefault with 0.0
                        # would block the enricher's real value via setdefault at merge time.
                        _vf = float(dcfg.get(DK.VOL_FAST_BPS, 0.0) or 0.0)
                        _vs = float(dcfg.get(DK.VOL_SLOW_BPS, 0.0) or 0.0)
                        if _vf != 0.0:
                            _inds.setdefault("vol_fast_bps", _vf)
                        if _vs != 0.0:
                            _inds.setdefault("vol_slow_bps", _vs)
                        if _vr == 0.0 and _vf > 0.0 and _vs > 0.0:
                            _inds.setdefault("vol_ratio", _vf / max(_vs, 1e-9))
                        _vol_label = str(dcfg.get(DK.VOL_REGIME_LABEL, _inds.get("vol_regime_label", "na")) or "na").strip().lower()
                        if _vol_label:
                            _inds.setdefault("vol_regime_label", _vol_label)
                        if _vf > 0.0 and "vol_regime_code" not in _inds:
                            try:
                                from core.v11_of_computers.regime_computers import compute_vol_regime_code
                                _inds["vol_regime_code"] = float(compute_vol_regime_code(_vf, _vs))
                            except Exception:
                                pass
                    except Exception:
                        pass
                    try:
                        _v13 = getattr(runtime, "v13_tracker", None)
                        _v13_snap = _v13.snapshot() if _v13 is not None else {}
                        if isinstance(_v13_snap, dict):
                            for _v13_k, _v13_raw in _v13_snap.items():
                                # Bridge ALL v13_tracker snapshot keys (NA/NB/NC/NE/NF groups).
                                # Only non-zero values: 0.0 = "not computed yet" and would
                                # freeze enricher-produced values via setdefault.
                                _v13_val = float(_v13_raw or 0.0)
                                if _v13_val != 0.0:
                                    _inds.setdefault(_v13_k, _v13_val)
                            # NX interaction: depth_resil_x_sweep not in snapshot() —
                            # computed here from bridged NB/NC values.
                            _resil = float(_v13_snap.get("depth_resilience_half_life", 0.0) or 0.0)
                            _sweep = float(_v13_snap.get("aggressive_sweep_ratio", 0.0) or 0.0)
                            _drxs = _resil * _sweep
                            if _drxs != 0.0:
                                _inds.setdefault("depth_resil_x_sweep", _drxs)
                    except Exception:
                        pass
                # depth_migration_bps: runtime EMA first (in-memory, no I/O) →
                # book_stats_reader (Redis call) only when EMA unavailable.
                # Must be here (not only in _enrich_signal) because signals:of:inputs is
                # written before the outbox enrichment step.
                if not _inds.get("depth_migration_bps"):
                    try:
                        _ema = float(getattr(runtime, "depth_migration_bps_ema", 0.0) or 0.0) if runtime is not None else 0.0
                        if _ema != 0.0:
                            _inds["depth_migration_bps"] = _ema
                        else:
                            import asyncio as _aio
                            from core.book_stats_reader import get_book_stats as _gbs
                            _sym = enriched_signal.get("symbol") or ""
                            _dm, _ = await _aio.to_thread(_gbs, _sym)
                            if _dm not in (None, 0, 0.0):
                                _inds["depth_migration_bps"] = _dm
                    except Exception:
                        pass
                # signal_age_ms: of_confirm_engine.build() computes this only for pass-path signals.
                # Veto-path bypasses of_confirm_engine entirely → always 0. Compute trivially so
                # ML training data has this feature for all paths.
                if not _inds.get("signal_age_ms"):
                    _sig_ts_ms = int(
                        enriched_signal.get("ts_ms") or enriched_signal.get("tick_ts") or 0
                    )
                    if _sig_ts_ms > 0:
                        _inds["signal_age_ms"] = max(0.0, float(int(time.time() * 1000) - _sig_ts_ms))
                # vol_ratio bridge: v14_of schema key vol_ratio maps to vol_ratio_fast_slow
                _inds.setdefault("vol_ratio", float(_inds.get("vol_ratio_fast_slow") or 0.0))
                _inds.setdefault("vol_ratio_z", float(_inds.get("sc_vol_ratio_z") or 0.0))
                # obi bridge: v13/v14_of schema key "obi" maps to payload key "obi_avg"
                _obi = float(_inds.get("obi_avg") or 0.0)
                _inds.setdefault("obi", _obi)
                # pressure bridge: v13/v14_of "pressure" maps to pressure_per_min_ema
                _pressure = float(_inds.get("pressure_per_min_ema") or 0.0)
                _inds.setdefault("pressure", _pressure)
                # pressure_x_obi: interaction term (aggressive flow × book structure alignment)
                _inds.setdefault("pressure_x_obi", _pressure * _obi)
                # regime bridge: resolve from runtime.last_regime → enriched_signal["regime"]
                # → indicators["regime"]. Veto paths (`_handle_pipeline_veto`) publish the raw
                # signal before regime resolution at L2594, so without this fallback ~87% of
                # `signals:of:inputs` records have regime=None and ML scorer cannot condition
                # on the regime feature (`_cat_regime_idx` becomes useless).
                _NA_TOKENS = ("na", "NA", "None", "unknown", "?", "")
                _need_inds_regime = (
                    not _inds.get("regime")
                    or str(_inds.get("regime")) in _NA_TOKENS
                )
                _regime_source = "indicators" if not _need_inds_regime else "none"
                if _need_inds_regime:
                    # Try top-level on the signal first.
                    _top_regime_raw = (str(enriched_signal.get("regime") or "")).lower().strip()
                    _top_regime = _top_regime_raw if (_top_regime_raw and _top_regime_raw not in _NA_TOKENS) else ""
                    if _top_regime:
                        _regime_source = "top_level"
                    elif runtime is not None:
                        # Fall back to runtime.last_regime (set by bar_processor regime svc).
                        _rt_regime = (str(getattr(runtime, "last_regime", "") or "")).lower().strip()
                        if _rt_regime and _rt_regime not in _NA_TOKENS:
                            _top_regime = _rt_regime
                            _regime_source = "runtime"
                    # 2026-05-27 P0.E-ext: universal Redis fallback. Audit showed
                    # regime fill rate в trades_closed = 5.33% (Lane B baseline 26%).
                    # runtime.last_regime может быть пустой для kinds которые не
                    # обновляют runtime (iceberg/delta_spike subscribers). Reader
                    # читает regime:{SYMBOL} string из worker-1 (fail-open).
                    if not _top_regime:
                        try:
                            from services.iceberg_long_gate_inline import (
                                get_regime_for_symbol as _get_regime,
                            )
                            _sym_for_regime = (
                                enriched_signal.get("symbol")
                                or symbol
                                or ""
                            )
                            if _sym_for_regime:
                                _redis_regime = _get_regime(str(_sym_for_regime))
                                if _redis_regime:
                                    _top_regime = str(_redis_regime).lower().strip()
                                    _regime_source = "redis_fallback"
                        except Exception:
                            pass
                    if _top_regime:
                        _inds["regime"] = _top_regime
                        enriched_signal.setdefault("regime", _top_regime)
                if _REGIME_RESOLVED_TOTAL is not None:
                    try:
                        _kind_lbl = str(enriched_signal.get("kind") or "unknown")
                        _REGIME_RESOLVED_TOTAL.labels(kind=_kind_lbl, source=_regime_source).inc()
                    except Exception:
                        pass
                # confidence_v1 bridge: strategy.py sets indicators["confidence"] not indicators["confidence_v1"]
                # Calibration trainer expects confidence_v1 as the raw pre-calibration confidence key.
                _inds.setdefault("confidence_v1", _inds.get("confidence"))

                # ── v14_of schema completeness invariant ──────────────────
                # `of_confirm_engine.build()` invokes `build_external_features_payload`
                # but mutates a LOCAL `indicators` dict; whether that dict survives
                # into the published `enriched_signal["indicators"]` depends on the
                # signal kind path (of/iceberg/delta_spike). Coverage audit (2026-05-24)
                # confirmed 77/359 v14_of features missing in `signals:of:inputs`
                # uniformly across kinds — meaning the bridge enrichment is LOST
                # before publish.
                #
                # Step 1 — run the per-group feature enricher (Redis HSET lookups +
                #          cheap computables from already-published keys). This
                #          backfills funding_rate_bps, book ratios, microbar, etc.
                try:
                    from core.feature_enricher_v1 import enrich_indicators
                    _sync_redis = getattr(self, "_sync_redis_client", None)
                    if _sync_redis is None:
                        # Best-effort sync client for HASH/GET lookups
                        try:
                            import redis as _redis_mod
                            _sync_redis = _redis_mod.from_url(
                                getattr(self, "redis_url", os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")),
                                decode_responses=True, socket_connect_timeout=1,
                                socket_timeout=0.5,
                            )
                            self._sync_redis_client = _sync_redis  # cache on pipeline
                        except Exception:
                            _sync_redis = None
                    # Bridge direction into indicators so _enrich_derived can compute
                    # conf_rsi_agree (direction is top-level in enriched_signal, not in _inds).
                    _sig_direction = enriched_signal.get("direction") or enriched_signal.get("side")
                    if _sig_direction:
                        _inds.setdefault("direction", str(_sig_direction).upper())
                    _enriched = enrich_indicators(
                        indicators=_inds,
                        symbol=symbol,
                        redis_client=_sync_redis,
                    )
                    # of_confirm_engine calls indicators.update(v13_snap) which freezes
                    # garman/parkinson/yang_zhang/etc to 0.0 before _publish_of_inputs.
                    # setdefault would then silently skip the enricher's real value.
                    # For these microstruct:ctx keys, override the frozen 0.0 directly.
                    _MICROSTRUCT_OVERRIDE = frozenset({
                        "garman_klass_vol", "parkinson_vol", "yang_zhang_vol",
                        "vol_of_vol", "amihud_illiquidity", "pin_estimate",
                    })
                    for _ek, _ev in _enriched.items():
                        if _ev is None:
                            continue  # enricher returns None when Redis key absent; skip to stay absent
                        if _ek in _MICROSTRUCT_OVERRIDE and _ev != 0.0:
                            _inds[_ek] = _ev  # override frozen 0.0 from v13_snap.update()
                        else:
                            _inds.setdefault(_ek, _ev)
                except Exception as _enr_exc:
                    logger.debug("⚠️ _publish_of_inputs feature_enricher fail-open: %s", _enr_exc)

                # Step 2 — re-invoke the bridge as a final invariant. `setdefault`
                # keeps already-populated values intact; missing _NUM_KEYS default
                # to 0.0 (train/serve consistency — model was trained on padded vectors).
                try:
                    from core.external_features_payload_v1 import (
                        build_external_features_payload,
                    )
                    _bridge_out = build_external_features_payload(
                        indicators_with_v4=None,
                        runtime_indicators=_inds,
                    )
                    for _bk, _bv in _bridge_out.items():
                        # `setdefault` semantics — never overwrite existing populated value.
                        # Skip None: _pick() returns None when neither source has the key;
                        # storing None would corrupt .get(key, 0.0) callers (returns None not 0.0).
                        if _bv is not None:
                            _inds.setdefault(_bk, _bv)
                except Exception as _ext_exc:
                    # Fail-open: bridge unavailable should not block publish.
                    logger.debug("⚠️ _publish_of_inputs ext-bridge fail-open: %s", _ext_exc)

                # eth_btc_corr_5m: inject_v12_of_features pre-sets 0.0 via setdefault, and the
                # cross_asset_corr_reader call in publish_signal() only runs on the pass-path
                # (~0% of training data at OF_SOFT_SCORE_MIN=0.85). Re-read here for all paths.
                # Direct assignment (not setdefault) overrides the 0.0 from inject_v12_of_features.
                if not _inds.get("eth_btc_corr_5m"):
                    try:
                        import asyncio as _aio_corr
                        from core.cross_asset_corr_reader import get_eth_btc_corr_5m as _get_corr
                        _corr = await _aio_corr.to_thread(_get_corr)
                        if _corr not in (None, 0, 0.0):
                            _inds["eth_btc_corr_5m"] = _corr
                    except Exception:
                        pass

                # fp_edge_absorb: computed in of_confirm_engine.build() for pass-path only.
                # For veto-path, call compute_fp_edge_absorb directly using runtime.last_fp_edge
                # (same source the confirm engine uses). Sets fp_edge_absorb + ancillary keys
                # (fp_edge_age_ms, fp_edge_strength, fp_edge_bias) in _inds as a side-effect.
                if "fp_edge_absorb" not in _inds and runtime is not None:
                    try:
                        from core.fp_edge_evidence import compute_fp_edge_absorb as _fp_absorb
                        _last_edge = getattr(runtime, "last_fp_edge", None)
                        _fp_dir = str(enriched_signal.get("direction") or "").upper()
                        _fp_cfg = dict(getattr(runtime, "config", {}) or {}) if hasattr(runtime, "config") else {}
                        _fp_ok, _, _, _ = _fp_absorb(
                            direction=_fp_dir,
                            now_ts_ms=int(time.time() * 1000),
                            last_edge=_last_edge,
                            cfg=_fp_cfg,
                            indicators=_inds,
                        )
                        _inds.setdefault("fp_edge_absorb", 1 if _fp_ok else 0)
                    except Exception:
                        _inds.setdefault("fp_edge_absorb", 0)

                # dq_score / dq_flag_count / dq_level / dq_pen:
                # of_confirm_engine.build() sets these on pass-path only.
                # For veto-path, bridge from data_health / data_health_reasons
                # (the pre-gate data quality proxy already in indicators from strategy.py).
                # dq_score ≈ data_health (0–1 range, 1.0 = fully healthy)
                # dq_flag_count = comma-separated reasons count
                # dq_level = 0 (veto-path DQ check not run; signal passed DQ-FIRST if reached)
                # dq_pen = 0.0 (no penalty without full confirm engine run)
                if "dq_score" not in _inds:
                    try:
                        _dh = float(_inds.get("data_health", 1.0) or 1.0)
                        _inds["dq_score"] = _dh
                        _dr = str(_inds.get("data_health_reasons") or "")
                        _inds["dq_flag_count"] = float(
                            len([r for r in _dr.split(",") if r.strip()])
                        )
                        _inds.setdefault("dq_level", 0)
                        _inds.setdefault("dq_pen", 0.0)
                    except Exception:
                        _inds.setdefault("dq_score", 1.0)
                        _inds.setdefault("dq_flag_count", 0.0)
                        _inds.setdefault("dq_level", 0)
                        _inds.setdefault("dq_pen", 0.0)

                # Ensure atr_5m is populated for ML features — covers both veto and outbox paths.
                # _calculate_levels sets it for executed signals; here we cover vetoed signals.
                if not _inds.get("atr_5m"):
                    try:
                        if self.atr_cache:
                            _nm5: int | None = int(
                                _inds.get("ts_ms", 0) or enriched_signal.get("ts_ms", 0) or 0
                            ) or None
                            _atr_5m_v, _ = self.atr_cache.get_with_meta(
                                symbol=symbol, timeframe="5m", now_ms=_nm5
                            )
                            if _atr_5m_v:
                                _inds["atr_5m"] = float(_atr_5m_v)
                    except Exception:
                        pass

            # Plan 3 / Step 2: TCA lifecycle DECISION stage — fired BEFORE the
            # XADD so it carries the decision-time epoch_ms even if publish fails.
            # Master switch ORDER_EXEC_EVENTS_ENABLED=0 → no-op (SHADOW default).
            try:
                from core.order_execution_events import Stage as _OEEStage
                from core.order_execution_events import async_emit as _oee_async_emit
                _sid_dec = str(enriched_signal.get("signal_id") or enriched_signal.get("sid") or "")
                _dir_dec = str(enriched_signal.get("direction") or enriched_signal.get("side") or "LONG").upper()
                _side_dec = 1 if _dir_dec != "SHORT" else -1
                if _sid_dec:
                    await _oee_async_emit(
                        getattr(publisher, "r", None),
                        sid=_sid_dec, stage=_OEEStage.DECISION,
                        symbol=symbol, side=_side_dec, status="ok",
                        ts_ms=int(enriched_signal.get("ts_ms") or 0) or None,
                        payload={"path": path, "kind": enriched_signal.get("kind") or ""},
                    )
            except Exception:
                pass  # fail-open, never block publish

            await publisher.xadd_json(
                sink=StreamSink(name=self.of_inputs_stream, field="payload", maxlen=self.of_inputs_stream_maxlen),
                payload=enriched_signal,
                symbol=symbol,
            )

            # Plan 3 / Step 2: SIGNAL_PUBLISHED stage — fired only after successful
            # XADD so latency_ms (vs DECISION) reflects real serialization cost.
            try:
                from core.order_execution_events import Stage as _OEEStage
                from core.order_execution_events import async_emit as _oee_async_emit
                _sid_pub = str(enriched_signal.get("signal_id") or enriched_signal.get("sid") or "")
                _dir_pub = str(enriched_signal.get("direction") or enriched_signal.get("side") or "LONG").upper()
                _side_pub = 1 if _dir_pub != "SHORT" else -1
                if _sid_pub:
                    await _oee_async_emit(
                        getattr(publisher, "r", None),
                        sid=_sid_pub, stage=_OEEStage.SIGNAL_PUBLISHED,
                        symbol=symbol, side=_side_pub, status="ok",
                        payload={"stream": self.of_inputs_stream},
                    )
            except Exception:
                pass

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

    def _get_profile_overrides(self) -> dict[str, Any]:
        """Load cfg:trade_profile:* keys from Redis with TTL cache."""
        import time as _time
        now = _time.monotonic()
        if now - self._profile_overrides_ts < self._profile_overrides_ttl:
            return self._profile_overrides
        try:
            r = self._pipeline_calib_sync_redis()
            if r is None:
                return self._profile_overrides
            overrides: dict[str, Any] = {}
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor, match="cfg:trade_profile:*", count=200)
                for raw_key in keys:
                    key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
                    raw = r.get(key)
                    if raw is None:
                        continue
                    try:
                        import json as _json
                        overrides[key] = _json.loads(raw.decode() if isinstance(raw, bytes) else raw)
                    except Exception:
                        pass
                if cursor == 0:
                    break
            self._profile_overrides = overrides
            self._profile_overrides_ts = now
        except Exception:
            pass
        return self._profile_overrides

    def _enrich_atr_floor_indicators(
        self,
        *,
        indicators: dict[str, Any],
        runtime: Any,
        cfg: dict[str, Any],
        entry: float,
        atr: float,
    ) -> None:
        """Write atr_floor / atr_fees / atr_unified threshold keys into indicators.

        Idempotent — safe to call multiple times. Pure write-only enrichment
        (no gate logic, no side effects beyond dict mutation).
        """
        try:
            from core.atr_floor_policy import compute_atr_bps_threshold

            rg = str(getattr(runtime, "last_regime", "na") or "na").lower()

            atr_bps_exec = 0.0
            try:
                if entry > 0 and atr > 0:
                    atr_bps_exec = 10000.0 * (atr / entry)
            except Exception:
                atr_bps_exec = 0.0
            indicators["atr_bps_exec"] = atr_bps_exec

            t0 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T0_BPS, cfg.get("atr_floor_t0_bps", 0.0)) or 0.0)
            t1 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T1_BPS, cfg.get("atr_floor_t1_bps", 0.0)) or 0.0)
            t2 = float(runtime.dynamic_cfg.get(DK.ATR_FLOOR_T2_BPS, cfg.get("atr_floor_t2_bps", 0.0)) or 0.0)

            tier, _rg_norm, floor_th = compute_atr_bps_threshold(regime=rg, cfg=cfg, t0=t0, t1=t1, t2=t2)

            indicators["atr_floor_t0_bps"] = t0
            indicators["atr_floor_t1_bps"] = t1
            indicators["atr_floor_t2_bps"] = t2
            indicators["atr_floor_tier"] = tier
            indicators["atr_floor_picked_bps"] = floor_th
            indicators["atr_floor_th_bps"] = floor_th
            indicators["atr_floor_rg"] = rg
            indicators["atr_floor_ready"] = int(runtime.dynamic_cfg.get(DK.ATR_CALIB_READY, 0) or 0)
            indicators["atr_floor_src"] = str(runtime.dynamic_cfg.get(DK.ATR_BPS_SRC, "na") or "na")
            indicators["atr_floor_n"] = int(runtime.dynamic_cfg.get(DK.ATR_BPS_N, 0) or 0)
            indicators["atr_bps_th"] = floor_th
        except Exception:
            pass

        try:
            from core.fees_aware_policy import fees_aware_min_atr_bps

            tp_ratios = parse_tp_ratio(str(cfg.get("tp_ratio", "")))
            # W5: override tp_ratios from calibrator when enabled + warm
            if self._tp_size_frac_reader is not None:
                try:
                    _regime_for_tpsf = (
                        indicators.get("regime")
                        or getattr(runtime, "last_regime", None)
                        or "*"
                    )
                    _cal_fracs = self._tp_size_frac_reader.get_fractions(_regime_for_tpsf)
                    if _cal_fracs:
                        indicators["tp_size_frac_shadow"] = list(_cal_fracs)
                        tp_ratios = list(_cal_fracs)
                except Exception:
                    pass
            tp1_share_actual = tp_ratios[0] if tp_ratios else 0.5
            rocket_mult = self._get_rocket_multiplier(runtime.symbol) or 0.0
            fees_th, _fees_meta = fees_aware_min_atr_bps(
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

        try:
            floor_th_v = float(indicators.get("atr_floor_th_bps", 0.0) or 0.0)
            fees_th_v = float(indicators.get("atr_fees_th_bps", 0.0) or 0.0)
            unified_th = max(floor_th_v, fees_th_v)
            indicators["atr_unified_th_bps"] = unified_th
            indicators["atr_gate_dominant"] = ("fees" if fees_th_v >= floor_th_v else "floor") if unified_th > 0 else "na"
        except Exception:
            pass

    def _record_veto(self, symbol: str, scenario: str, reason: str, mode: str = "ENFORCE") -> None:
        """Helper to record veto metrics."""
        with contextlib.suppress(Exception):
            strong_gate_veto_total.labels(
                symbol=symbol,
                scenario=scenario,
                reason=reason,
                mode=mode
            ).inc()

    def _record_gated_out_shadow(
        self,
        *,
        signal: dict[str, Any],
        indicators: dict[str, Any],
        confirmations: list[Any],
        symbol: str,
        direction: str,
        ts_ms: int,
        confidence: float,
        min_conf: float,
        entry: float = 0.0,
        sl: float = 0.0,
        tp_levels: list[float] | None = None,
        regime: str = "na",
    ) -> None:
        """Fire-and-forget XADD of a confidence-gated-out signal to a shadow stream.

        Stage-1 investigation: «is the confidence gate worth keeping?» — we need
        outcomes (virtual or real) for signals BELOW the threshold to answer.
        Without this, the gate is permanent unfalsifiable assumption.
        See docs/PHASE2_SCORER_MULTIPLIERS_REDESIGN.md.

        Off-path; failures are swallowed (fail-open).
        """
        if not getattr(self, "gated_out_shadow_enabled", False):
            return
        if not (self.publisher and getattr(self.publisher, "r", None)):
            return
        try:
            _virtual_min_conf_pct = getattr(self, "_cached_virtual_min_conf_pct", min_conf)
            if 0 < _virtual_min_conf_pct <= 1:
                _virtual_min_conf_pct *= 100.0
            _virtual_min_conf = _virtual_min_conf_pct / 100.0
            _session = session_utc(ts_ms)
            payload = {
                "v": 2,
                "ts_ms": ts_ms,
                "symbol": symbol,
                "direction": direction,
                "side": direction.lower(),
                "signal_id": str(signal.get("signal_id") or ""),
                "confidence": confidence,
                "min_conf": min_conf,
                "entry": entry,
                "sl": sl,
                "tp_levels": list(tp_levels) if tp_levels else [],
                "gated_out": 1,
                "gate_reason": "low_confidence",
                "virtual": True,
                "tradeable": False,
                "is_counterfactual": True,
                # ML training metadata: policy-aware split requires these fields.
                # sample_policy drives selection_weight in training; conformal
                # calibration must be run on strict_live_passed holdout only.
                "sample_policy": "confidence_gated_out",
                "selection_policy_version": "v1",
                "selection_prob": confidence / min_conf if min_conf > 0 else 0.0,
                "selection_weight": confidence / min_conf if min_conf > 0 else 0.0,
                # Whether this signal clears the relaxed virtual threshold,
                # i.e. it would be routed to virtual order queue if enabled.
                "meets_virtual_threshold": confidence >= _virtual_min_conf,
                "virtual_min_conf": _virtual_min_conf,
                "regime": regime,
                "session": _session,
                "confirmations": list(confirmations) if isinstance(confirmations, (list, tuple)) else [],
                "indicators": indicators,
            }
            safe_create_task(
                self.publisher.r.xadd(
                    self.gated_out_shadow_stream,
                    {"payload": json.dumps(payload, ensure_ascii=False, default=str)},
                    maxlen=self.gated_out_shadow_maxlen,
                    approximate=True,
                ),
                name=f"gated_out_shadow_{symbol}_{ts_ms}",
            )
            with contextlib.suppress(Exception):
                if _VIRTUAL_SAMPLE_COUNT_BY_POLICY is not None:
                    _VIRTUAL_SAMPLE_COUNT_BY_POLICY.labels(
                        sample_policy="confidence_gated_out",
                        symbol=symbol,
                        regime=regime,
                        session=_session,
                    ).inc()
        except Exception:
            # Fail-open: shadow recording must never break the pipeline.
            pass

    def _maybe_record_deep_explore(
        self,
        *,
        signal: dict[str, Any],
        indicators: dict[str, Any],
        confirmations: list[Any],
        symbol: str,
        direction: str,
        ts_ms: int,
        confidence: float,
        entry: float = 0.0,
        sl: float = 0.0,
        tp_levels: list[float] | None = None,
        regime: str = "na",
    ) -> None:
        """Sampled outcome-only recording for signals in [deep_min, virtual_min) range.

        Policy: deep_explore_20_35_sampled
        Rules:
        - NEVER routes to virtual order queue (tradeable=False, gated_out=1).
        - Controlled by DEEP_EXPLORATION_SAMPLE_RATE (probabilistic gate, default=0).
        - Capped by DEEP_EXPLORATION_CAP_PER_SYMBOL_SESSION_HOUR per (symbol×hour×regime).
        - Off-path, fail-open.

        Rollback: set DEEP_EXPLORATION_SAMPLE_RATE=0.0 (default).
        """
        if not getattr(self, "gated_out_shadow_enabled", False):
            return
        if not (self.publisher and getattr(self.publisher, "r", None)):
            return

        sample_rate = getattr(self, "_cached_deep_explore_sample_rate", 0.0)
        if sample_rate <= 0.0:
            return  # disabled by default

        try:
            import hashlib
            signal_id = str(signal.get("signal_id") or "")
            
            # --- Probabilistic gate (Deterministic Hash) ---
            hash_hex = hashlib.sha256(f"{signal_id}:{symbol}:deep_explore".encode("utf-8")).hexdigest()
            bucket = int(hash_hex, 16) % 10_000
            accept = bucket < (sample_rate * 10_000)
            
            if not accept:
                with contextlib.suppress(Exception):
                    if _DEEP_EXPLORE_SAMPLE_TOTAL is not None:
                        _DEEP_EXPLORE_SAMPLE_TOTAL.labels(symbol=symbol, accepted="0").inc()
                return

            # --- Cap gate: per (symbol, hour_bucket, regime) via Redis ---
            cap = getattr(self, "_cached_deep_explore_cap_per_slot", 50)
            hour_bucket = ts_ms // 3_600_000  # rounded to hour epoch
            cap_key = f"deep_explore:cap:{symbol}:{hour_bucket}:{regime}"
            
            current_count = 0
            try:
                if self.publisher and self.publisher.r:
                    pipe = self.publisher.r.pipeline()
                    pipe.incr(cap_key)
                    pipe.expire(cap_key, 7200)  # 2 hours TTL
                    res = pipe.execute()
                    current_count = res[0]
            except Exception as e:
                logger.debug("⚠️ Redis cap counter fail-open for %s: %s", cap_key, e)
                # Fail-open: if Redis is down, we allow the sample.
                
            if current_count > cap:
                with contextlib.suppress(Exception):
                    if _DEEP_EXPLORE_SAMPLE_TOTAL is not None:
                        _DEEP_EXPLORE_SAMPLE_TOTAL.labels(symbol=symbol, accepted="0").inc()
                return

            # --- Build payload ---
            _virt_min_conf_pct = getattr(self, "_cached_virtual_min_conf_pct", 35.0)
            _virt_min_conf = _virt_min_conf_pct / 100.0 if _virt_min_conf_pct > 1 else _virt_min_conf_pct
            _deep_min_pct = getattr(self, "_cached_deep_explore_min_conf_pct", 20.0)
            _deep_min_conf = _deep_min_pct / 100.0 if _deep_min_pct > 1 else _deep_min_pct
            _session = session_utc(ts_ms)

            payload = {
                "v": 2,
                "ts_ms": ts_ms,
                "symbol": symbol,
                "direction": direction,
                "side": direction.lower(),
                "signal_id": str(signal.get("signal_id") or ""),
                "confidence": confidence,
                "min_conf": _virt_min_conf,
                "entry": entry,
                "sl": sl,
                "tp_levels": list(tp_levels) if tp_levels else [],
                "gated_out": 1,
                "gate_reason": "low_confidence",
                "virtual": True,
                # SAFETY: deep_explore samples are NEVER tradeable.
                # They are for outcome tracking and ML dataset expansion only.
                "tradeable": False,
                "is_counterfactual": True,
                # Policy labels for dataset filtering and propensity weighting.
                "sample_policy": "deep_explore_20_35_sampled",
                "selection_policy_version": "v1",
                "selection_prob": sample_rate,
                # Propensity weight: inverse of sample_rate for off-policy correction.
                # Capped at 20x to avoid extreme weights.
                "selection_weight": min(1.0 / sample_rate if sample_rate > 0 else 1.0, 20.0),
                "meets_virtual_threshold": False,  # always False for deep explore
                "virtual_min_conf": _virt_min_conf,
                "deep_explore_min_conf": _deep_min_conf,
                "regime": regime,
                "session": _session,
                "confirmations": list(confirmations) if isinstance(confirmations, (list, tuple)) else [],
                "indicators": indicators,
            }

            safe_create_task(
                self.publisher.r.xadd(
                    self.gated_out_shadow_stream,
                    {"payload": json.dumps(payload, ensure_ascii=False, default=str)},
                    maxlen=self.gated_out_shadow_maxlen,
                    approximate=True,
                ),
                name=f"deep_explore_{symbol}_{ts_ms}",
            )
            with contextlib.suppress(Exception):
                if _DEEP_EXPLORE_SAMPLE_TOTAL is not None:
                    _DEEP_EXPLORE_SAMPLE_TOTAL.labels(symbol=symbol, accepted="1").inc()
                if _VIRTUAL_SAMPLE_COUNT_BY_POLICY is not None:
                    _VIRTUAL_SAMPLE_COUNT_BY_POLICY.labels(
                        sample_policy="deep_explore_20_35_sampled",
                        symbol=symbol,
                        regime=regime,
                        session=_session,
                    ).inc()
            logger.debug(
                "🔬 [%s] deep_explore sample recorded conf=%.3f bucket=%s",
                symbol, confidence, cap_key,
            )
        except Exception:
            # Fail-open: exploration sampling must never break the pipeline.
            pass

    def _confidence_meta_gate_decide(
        self,
        *,
        legacy_dec: "GateDecisionV1",
        signal: dict[str, Any],
        indicators: dict[str, Any],
        symbol: str,
        kind: str,
        direction: str,
        sig_ts: int,
        confidence: float,
        min_conf: float,
    ) -> "GateDecisionV1":
        """Plan 1: calibrated meta-gate that can replace the legacy confidence
        DENY/ALLOW. SHADOW by default — returns `legacy_dec` unchanged unless
        mode=CANARY/ENFORCE selects this sample. Hard safety gates upstream
        and the risk engine downstream are unaffected.

        Fail-open: any exception is swallowed and `legacy_dec` is returned.
        """
        try:
            from services.confidence_meta_gate import (
                ConfidenceMetaGateInput,
                MetaGateMode,
                decide_meta_gate,
                emit_decision,
                get_runtime,
            )
            from services.confidence_meta_gate.metrics import set_model_health_gauges
        except Exception:
            return legacy_dec

        try:
            rt = get_runtime()
            cfg = rt.cfg
            if not cfg.enabled or cfg.mode is MetaGateMode.OFF:
                return legacy_dec

            artifact = rt.ensure_loaded()
            if artifact is not None:
                age_hours = rt._slot.age_hours() if hasattr(rt, "_slot") else None
                set_model_health_gauges(
                    artifact.model_ver, age_hours,
                    artifact.calibrator.ece,
                )

            legacy_kind = "DENY" if getattr(legacy_dec, "decision", "") == "DENY" else "ALLOW"
            sid = str(signal.get("signal_id") or signal.get("sid") or "")
            now_ms = int(time.time() * 1000)
            session = session_utc(sig_ts)
            regime = str(indicators.get("regime") or "na")
            schema_hash = str(indicators.get("feature_schema_version") or "")
            feature_cols_hash = (
                artifact.feature_cols_hash if artifact is not None else ""
            )

            spread_bps = float(indicators.get("spread_bps", 0.0) or 0.0)
            slippage_bps = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
            expected_edge_bps = float(
                signal.get("expected_edge_bps", indicators.get("expected_edge_bps", 0.0)) or 0.0
            )

            features: dict[str, float] = {}
            if artifact is not None:
                # Populate only the columns the model declared, with safe fallbacks.
                _sources = (signal, indicators)
                for col in artifact.feature_cols:
                    val: Any = None
                    for src in _sources:
                        if col in src:
                            val = src.get(col)
                            break
                    if val is None:
                        continue
                    try:
                        v = float(val)
                    except (TypeError, ValueError):
                        continue
                    if v != v:  # NaN guard
                        continue
                    features[col] = v
                # Inject canonical SL-bps key the gate expects.
                if "sl_bps" not in features:
                    sl_bps_hint = float(
                        indicators.get("sl_atr_th_bps", 0.0)
                        or indicators.get("gate_risk_bps", 0.0)
                        or 0.0
                    )
                    if sl_bps_hint > 0:
                        features["sl_bps"] = sl_bps_hint

            inp = ConfidenceMetaGateInput(
                sid=sid,
                symbol=symbol,
                kind=kind,
                side=direction,
                ts_ms=sig_ts,
                now_ms=now_ms,
                legacy_confidence=confidence,
                legacy_min_confidence=min_conf,
                legacy_decision=legacy_kind,
                p_edge_raw=float(indicators.get("p_edge_raw", 0.0) or 0.0),
                p_edge_cal=(
                    float(indicators["p_edge_cal"])
                    if indicators.get("p_edge_cal") is not None else None
                ),
                rule_score=float(indicators.get("rule_score", 0.0) or 0.0),
                have=int(indicators.get("strong_gate_legs", 0) or 0),
                need=int(indicators.get("strong_gate_need", 0) or 0),
                spread_bps=spread_bps,
                expected_slippage_bps=slippage_bps,
                fee_bps=self.FEES_BPS_RT,
                expected_edge_bps=expected_edge_bps,
                exec_risk_norm=float(indicators.get("exec_risk_norm", 0.0) or 0.0),
                dq_score=float(indicators.get("dq_score", 1.0) or 1.0),
                dq_flag_count=int(indicators.get("dq_flag_count", 0) or 0),
                regime=regime,
                session=session,
                schema_hash=schema_hash,
                feature_cols_hash=feature_cols_hash,
                features=features,
            )

            # Resolve effective mode: cfg.mode unless the auto-demote watcher
            # has written `cfg:conf_meta_gate.mode=SHADOW` to Redis (TTL-cached
            # inside the runtime so this is at most one HGET / 30 s).
            redis_for_override = (
                self.publisher.r
                if (self.publisher and getattr(self.publisher, "r", None)) else None
            )
            effective_mode = rt.effective_mode(redis_for_override)
            out = decide_meta_gate(
                inp, cfg, artifact, now_ms=now_ms, mode_override=effective_mode,
            )

            # Compute the active decision: SHADOW/non-selected CANARY ⇒ legacy.
            if out.active and out.decision in ("ALLOW", "ALLOW_TIGHTENED"):
                active_decision = "ALLOW"
            elif out.active and out.decision == "DENY_SOFT":
                active_decision = "DENY"
            else:
                active_decision = legacy_kind

            redis_client = (
                self.publisher.r
                if (self.publisher and getattr(self.publisher, "r", None)) else None
            )
            emit_decision(
                inp, out, cfg,
                active_decision=active_decision,
                redis_client=redis_client,
            )

            # Only patch the legacy decision when meta-gate is authoritative.
            if not out.active:
                return legacy_dec

            # Synthesize a GateDecisionV1 carrying the meta-gate verdict so the
            # rest of the pipeline (_apply_decision / _handle_pipeline_veto) keeps
            # working with a single decision type.
            from core.gates.decision import GateDecisionV1 as _GD
            ts_dec_ms = int(time.time() * 1000)
            if active_decision == "ALLOW":
                return _GD(
                    stage="confidence",
                    gate="ConfidenceMetaGate",
                    decision="ALLOW",
                    reason_code=out.reason_codes[-1] if out.reason_codes else "META_ALLOW",
                    severity="INFO",
                    profile="meta",
                    fail_policy="CLOSED",
                    ts_event_ms=sig_ts,
                    ts_decision_ms=ts_dec_ms,
                    latency_us=int(out.latency_ms * 1000),
                    inputs_hash="",
                    notes={
                        "p_win_cal": out.p_win_calibrated,
                        "expected_r": out.expected_r,
                        "model_ver": out.model_ver,
                        "canary_bucket": out.canary_bucket,
                        "legacy_decision": legacy_kind,
                        "meta_decision": out.decision,
                    },
                )
            return _GD(
                stage="confidence",
                gate="ConfidenceMetaGate",
                decision="DENY",
                reason_code=out.reason_codes[0] if out.reason_codes else "META_DENY",
                severity="WARN",
                profile="meta",
                fail_policy="CLOSED",
                ts_event_ms=sig_ts,
                ts_decision_ms=ts_dec_ms,
                latency_us=int(out.latency_ms * 1000),
                inputs_hash="",
                notes={
                    "p_win_cal": out.p_win_calibrated,
                    "expected_r": out.expected_r,
                    "model_ver": out.model_ver,
                    "canary_bucket": out.canary_bucket,
                    "legacy_decision": legacy_kind,
                    "meta_decision": out.decision,
                    "reasons": list(out.reason_codes),
                },
            )
        except Exception as e:  # noqa: BLE001
            logger.debug("⚠️ conf_meta_gate evaluate fail-open: %s", e)
            return legacy_dec

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
        runtime: SymbolRuntime | None = None,
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
                        maxlen=self._rejected_signal_maxlen,
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
                    rejection_reason=dec.reason_code,
                    rejection_gate=dec.gate,
                ),
                name=f"virtual_veto_{symbol}_{ts_ms}"
            )

            # 4. Feed TB Labeler — pre_publish vetoed signals still need training data
            # Calibration requires (confidence, outcome) pairs for ALL signals that
            # passed portfolio risk gate, regardless of execution-level veto reason.
            # Pass runtime so _publish_of_inputs can resolve regime from runtime.last_regime
            # (the raw `signal` here has not yet been through the regime-enrichment step).
            if self.of_inputs_publish_enabled and self.of_inputs_stream:
                safe_create_task(
                    self._publish_of_inputs(
                        publisher=self.publisher,
                        enriched_signal=signal,
                        symbol=symbol,
                        path="veto",
                        runtime=runtime,
                    ),
                    name=f"of_inputs_veto_{symbol}_{ts_ms}",
                )
        except Exception as e:
            logger.debug("⚠️ _handle_pipeline_veto error: %s", e)

    @staticmethod
    def _resolve_vol_regime_label(
        *,
        indicators: dict[str, Any],
        runtime: Any | None = None,
    ) -> str:
        """Resolve canonical vol regime label for regime-exec engine.

        Source priority:
          1. ``indicators["vol_regime_label"]`` / ``["vol_regime"]`` if present.
          2. ``runtime.dynamic_cfg[DK.VOL_REGIME_LABEL]`` (set by bar_processor
             on every closed bar via ``VolRegimeTracker.update``).
          3. Fallback ``"na"``.

        Side effect: copies resolved value into ``indicators["vol_regime_label"]``
        so downstream consumers (audit stream, ML feature emitters) see it too.
        Also mirrors ``vol_ratio_z`` and ``vol_ratio`` when available.
        """
        label = (
            indicators.get("vol_regime_label")
            or indicators.get("vol_regime")
        )
        if runtime is not None:
            try:
                dcfg = getattr(runtime, "dynamic_cfg", {}) or {}
                if not label:
                    label = dcfg.get(DK.VOL_REGIME_LABEL)
                # Mirror vol magnitudes unconditionally — tick_decision_engine sets
                # vol_regime_label but not vol_fast_bps/vol_slow_bps/vol_ratio,
                # so the label-presence check must not gate the magnitude mirror.
                indicators.setdefault(
                    "vol_ratio_z",
                    float(dcfg.get(DK.VOL_RATIO_Z, 0.0) or 0.0),
                )
                indicators.setdefault(
                    "vol_ratio",
                    float(dcfg.get(DK.VOL_RATIO, 0.0) or 0.0),
                )
                indicators.setdefault(
                    "vol_fast_bps",
                    float(dcfg.get(DK.VOL_FAST_BPS, 0.0) or 0.0),
                )
                indicators.setdefault(
                    "vol_slow_bps",
                    float(dcfg.get(DK.VOL_SLOW_BPS, 0.0) or 0.0),
                )
            except Exception:
                pass
        norm = str(label or "na").strip().lower() or "na"
        indicators["vol_regime_label"] = norm
        return norm

    def _evaluate_regime_exec_engine(
        self,
        *,
        indicators: dict[str, Any],
        signal: dict[str, Any],
        symbol: str,
        kind: str,
        runtime: Any,
        trend_regime: str,
        current_trail_profile: str | None,
        sig_ts: int,
    ) -> dict[str, Any]:
        """Run regime-conditional execution engine (Task 3.1).

        Returns a dict with applied overrides:
          - ``trail_profile`` (str | None)
          - ``tp_ratios`` (list[float] | None)
          - ``veto_decision`` (GateDecisionV1 | None)

        Writes ``regime_exec_*`` audit keys into ``indicators``. Always
        fail-open: any exception leaves caller state untouched.
        """
        result: dict[str, Any] = {
            "trail_profile": None,
            "tp_ratios": None,
            "veto_decision": None,
        }
        try:
            from core.regime_conditional_execution import (
                get_engine as _get_regime_exec_engine,
                record_shadow_diff as _regime_exec_record_diff,
                emit_veto_metric as _regime_exec_emit_veto,
            )
        except Exception as e:
            logger.debug("regime_exec_engine: import fail (fail-open): %s", e)
            return result
        try:
            engine = _get_regime_exec_engine(
                redis_client=getattr(self.publisher, "r", None)
            )
            if engine is None:
                return result
            vol_reg = self._resolve_vol_regime_label(
                indicators=indicators, runtime=runtime
            )
            trend_reg = trend_regime or "na"
            policy = engine.select_policy(
                vol_regime=vol_reg,
                trend_regime=trend_reg,
                symbol=getattr(runtime, "symbol", "") or symbol,
            )
            enforce_effective = engine.effective_enforce(policy)

            indicators["regime_exec_bucket"] = policy.bucket
            indicators["regime_exec_vol"] = vol_reg
            indicators["regime_exec_trend"] = trend_reg
            indicators["regime_exec_mode"] = (
                "enforce" if enforce_effective else "shadow"
            )
            indicators["regime_exec_skip_proposed"] = int(policy.skip)
            indicators["regime_exec_reason"] = policy.reason
            if policy.trail_profile:
                indicators["regime_exec_trail_profile_proposed"] = policy.trail_profile
            if policy.tp1_target_r is not None:
                indicators["regime_exec_tp1_target_r_proposed"] = policy.tp1_target_r
            if policy.tp_ratios:
                indicators["regime_exec_tp_ratios_proposed"] = list(policy.tp_ratios)

            shadow_diff = _regime_exec_record_diff(
                policy,
                actual_trail_profile=current_trail_profile,
                actual_tp_ratios=None,
                actual_tp1_target_r=self._cached_tp1_target_r,
            )
            if shadow_diff:
                indicators["regime_exec_shadow_diff"] = shadow_diff

            if not enforce_effective:
                return result

            if engine.should_skip(policy):
                indicators["regime_exec_skip_applied"] = 1
                _regime_exec_emit_veto(policy, symbol=symbol, kind=kind)
                result["veto_decision"] = GateDecisionV1(
                    stage="regime_exec",
                    gate="regime_conditional_execution",
                    decision="DENY",
                    reason_code="VETO_REGIME_CHOPPY",
                    severity="INFO",
                    profile=policy.bucket,
                    fail_policy="OPEN",
                    ts_event_ms=sig_ts,
                    ts_decision_ms=int(time.time() * 1000),
                    latency_us=0,
                    inputs_hash="",
                    notes={
                        "vol_regime": vol_reg,
                        "trend_regime": trend_reg,
                        "bucket": policy.bucket,
                        "reason": policy.reason,
                    },
                )
                return result

            if policy.trail_profile:
                result["trail_profile"] = policy.trail_profile
                indicators["regime_exec_trail_profile_applied"] = policy.trail_profile
                signal.setdefault("trail_after_tp1", True)
            if policy.tp1_target_r is not None and policy.tp1_target_r > 0:
                self._cached_tp1_target_r = policy.tp1_target_r
                self._cached_tp1_target_r_enforce = True
                indicators["regime_exec_tp1_target_r_applied"] = (
                    self._cached_tp1_target_r
                )
            if policy.tp_ratios:
                result["tp_ratios"] = list(policy.tp_ratios)
                indicators["regime_exec_tp_ratios_applied"] = result["tp_ratios"]
        except Exception as e:
            logger.debug("regime_exec_engine: fail-open: %s", e)
        return result

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

        # 2026-05-27 P0.E-final: universal regime resolution into ctx.indicators
        # ДО любых gate evaluation. Audit 2026-05-27 (Lane B): regime=NULL у 74%
        # LONG-trades + downstream EntryPolicyGate.indicators.regime=None даже
        # при наличии payload bridge от iceberg detector.
        #
        # Resolution hierarchy (longest wins; preserves existing value):
        #   1. indicators.regime (already set by upstream — iceberg P0.E bridge,
        #      of_confirm_engine, etc.)
        #   2. signal["regime"] top-level field
        #   3. runtime.last_regime (set by bar_processor regime svc)
        #   4. Redis fallback: regime:{SYMBOL} string on worker-1
        #      (writer: regime_engine service)
        _NA_TOKENS = ("", "na", "none", "null", "unknown", "?")
        _cur_ind_reg = str(indicators.get("regime") or "").strip().lower()
        _need_resolve = (
            indicators.get("regime") is None
            or _cur_ind_reg in _NA_TOKENS
        )
        if _need_resolve:
            _resolved_regime: str | None = None
            # signal["regime"] top-level
            _top = str(signal.get("regime") or "").strip().lower()
            if _top and _top not in _NA_TOKENS:
                _resolved_regime = _top
            # runtime.last_regime
            if _resolved_regime is None and runtime is not None:
                _rt = str(getattr(runtime, "last_regime", "") or "").strip().lower()
                if _rt and _rt not in _NA_TOKENS:
                    _resolved_regime = _rt
            # Redis fallback (regime:{SYMBOL})
            if _resolved_regime is None:
                try:
                    from services.iceberg_long_gate_inline import (
                        get_regime_for_symbol as _get_regime,
                    )
                    _sym = str(getattr(runtime, "symbol", "") or signal.get("symbol") or "")
                    if _sym:
                        _rr = _get_regime(_sym)
                        if _rr:
                            _resolved_regime = str(_rr).strip().lower()
                except Exception:
                    pass
            if _resolved_regime:
                indicators["regime"] = _resolved_regime
                # mirror to top-level signal for downstream consumers
                signal.setdefault("regime", _resolved_regime)

        # data-quality flags (prepared by preprocess_signal_for_publish + additional cheap hints)
        flags = []
        dq_flags_val = signal.get("data_quality_flags")
        if isinstance(dq_flags_val, list):
            flags.extend([str(x) for x in dq_flags_val if x is not None])

        # Additional hints available at publish time.
        # Observe unconditionally for DQ micro calibration (ALL signals, not emit-only).
        _dq_sym = getattr(runtime, "symbol", "") or ""
        try:
            book_stale_ms = int(micro.get("book_stale_ms") or 0)
            # Fallback: compute stale from runtime book timestamp when micro field absent
            if book_stale_ms <= 0 and runtime is not None:
                _last_book_ts = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
                if _last_book_ts > 0:
                    book_stale_ms = max(0, sig_ts_ms - _last_book_ts)
            _dq_spread_raw = float(micro.get("spread_bps") or 0.0)
            # Fallback: use runtime spread when micro field absent
            if _dq_spread_raw <= 0.0 and runtime is not None:
                _dq_spread_raw = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
            if _dq_sym:
                self._dq_micro_cal.observe(
                    symbol=_dq_sym, book_stale_ms=book_stale_ms, spread_bps=_dq_spread_raw
                )
            dq_book_stale_flag_ms = (
                self._dq_micro_cal.stale_threshold(_dq_sym)
                if _dq_sym else self._cached_dq_book_stale_flag_ms
            )
            # W5: DQ soft-flag autocal override for ENV fallback path
            if not _dq_sym and self._dq_softflag_reader is not None:
                try:
                    _sf = self._dq_softflag_reader.get_thresholds(runtime.symbol)
                    if _sf is not None:
                        dq_book_stale_flag_ms = _sf[0]
                except Exception:
                    pass
            if book_stale_ms > dq_book_stale_flag_ms:
                flags.append("stale_l2")
        except Exception:
            pass

        try:
            spread_bps = float(micro.get("spread_bps") or 0.0)
            dq_spread_wide_flag_bps = (
                self._dq_micro_cal.spread_threshold(_dq_sym)
                if _dq_sym else self._cached_dq_spread_wide_flag_bps
            )
            if not _dq_sym and self._dq_softflag_reader is not None:
                try:
                    _sf = self._dq_softflag_reader.get_thresholds(runtime.symbol)
                    if _sf is not None:
                        dq_spread_wide_flag_bps = _sf[1]
                except Exception:
                    pass
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

        # entry_price is available from strategy payload; sl/tp1 are computed later
        # by _calculate_levels() — gate position must move AFTER that call for EV mode
        # to evaluate correctly (see TODO below at edge_cost_cached call site).
        _entry_raw = signal.get("entry")
        _entry_price = float(_entry_raw) if _entry_raw is not None else None

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
            entry_price=_entry_price,
            sl_price=None,   # populated after _calculate_levels(); gate fails-open until then
            tp1_price=None,  # populated after _calculate_levels(); gate fails-open until then
        )
        return ctx

    def _pipeline_calib_sync_redis(self) -> Any:
        """Lazy-cached sync Redis client for pipeline calibrator persist/restore."""
        rc = getattr(self, "_pipeline_calib_redis", None)
        if rc is not None:
            return rc
        try:
            from handlers.crypto_orderflow.config.handler_config import _get_sync_redis
            self._pipeline_calib_redis = _get_sync_redis()
            return self._pipeline_calib_redis
        except Exception:
            return None

    async def _calib_snap_bg_loop(self) -> None:
        """Periodic pipeline calibrator snapshot — fires every PIPELINE_CALIB_SNAP_SEC
        regardless of whether any signal was emitted. Ensures dq_micro / confirm_barrier
        state is persisted to Redis even in periods of all-SHADOW_DROP or no-emit."""
        import asyncio as _asyncio
        import time as _time
        interval_sec = max(10.0, self._calib_snap_interval_ms / 1000.0)
        while True:
            await _asyncio.sleep(interval_sec)
            try:
                self._pipeline_calib_snap(int(_time.time() * 1000))
            except Exception:
                pass

    def _pipeline_calib_load(self) -> None:
        """Warm-start pipeline calibrators from Redis (best-effort, once)."""
        if self._calib_loaded:
            return
        self._calib_loaded = True
        rc = self._pipeline_calib_sync_redis()
        if rc is None:
            return
        import json as _json
        from core.redis_keys import RK
        for key, calib, loader_attr in (
            (RK.AUTOCAL_COOLDOWN,      self._cooldown_calib,  "load_symbol_state"),
            (RK.AUTOCAL_VOL_Z_THR,     self._vol_z_calib,     "load_regime_state"),
            (RK.AUTOCAL_HTF_PROXIMITY, self._htf_prox_calib,  "load_symbol_state"),
            (RK.AUTOCAL_LIQ_WALL,      self._liq_wall_calib,  "load_symbol_state"),
            (RK.AUTOCAL_DQ_MICRO,      self._dq_micro_cal,    "load_symbol_state"),
        ):
            try:
                raw_map = rc.hgetall(key)
                if not raw_map:
                    continue
                loader = getattr(calib, loader_attr)
                for v in raw_map.values():
                    s = v.decode("utf-8", "ignore") if isinstance(v, (bytes, bytearray)) else v
                    loader(_json.loads(s))
            except Exception:
                pass
        # confirm_barrier uses a single JSON blob (STRING key, not HASH)
        try:
            raw_cb = rc.get(RK.AUTOCAL_CONFIRM_BARRIER)
            if raw_cb:
                s = raw_cb.decode("utf-8", "ignore") if isinstance(raw_cb, (bytes, bytearray)) else raw_cb
                self._confirm_barrier_cal.load_state(_json.loads(s))
        except Exception:
            pass

    def _pipeline_calib_snap(self, now_ms: int) -> None:
        """Throttled snapshot of pipeline calibrators to Redis (best-effort)."""
        if (now_ms - self._calib_last_snap_ms) < self._calib_snap_interval_ms:
            return
        self._calib_last_snap_ms = now_ms
        rc = self._pipeline_calib_sync_redis()
        if rc is None:
            return
        import json as _json
        from core.redis_keys import RK
        # cooldown (per symbol)
        try:
            for sym_key in list(self._cooldown_calib._n.keys()):
                state = self._cooldown_calib.dump_symbol_state(symbol=sym_key, updated_ts_ms=now_ms)
                rc.hset(RK.AUTOCAL_COOLDOWN, sym_key, _json.dumps(state))
        except Exception:
            pass
        # vol_z_thr (per regime)
        try:
            for regime_key in list(self._vol_z_calib._n.keys()):
                sym_part = regime_key.split(":")[0].upper()
                state = self._vol_z_calib.dump_regime_state(symbol=sym_part, regime=regime_key, updated_ts_ms=now_ms)
                rc.hset(RK.AUTOCAL_VOL_Z_THR, regime_key, _json.dumps(state))
        except Exception:
            pass
        # htf_proximity (per symbol)
        try:
            for sym_key in list(self._htf_prox_calib._n.keys()):
                state = self._htf_prox_calib.dump_symbol_state(symbol=sym_key, updated_ts_ms=now_ms)
                rc.hset(RK.AUTOCAL_HTF_PROXIMITY, sym_key, _json.dumps(state))
        except Exception:
            pass
        # liq_wall (per symbol)
        try:
            for sym_key in list(self._liq_wall_calib._n.keys()):
                state = self._liq_wall_calib.dump_symbol_state(symbol=sym_key, updated_ts_ms=now_ms)
                rc.hset(RK.AUTOCAL_LIQ_WALL, sym_key, _json.dumps(state))
        except Exception:
            pass
        # dq_micro (per symbol)
        try:
            for sym_key in list(self._dq_micro_cal._n.keys()):
                state = self._dq_micro_cal.dump_symbol_state(symbol=sym_key, updated_ts_ms=now_ms)
                rc.hset(RK.AUTOCAL_DQ_MICRO, sym_key, _json.dumps(state))
        except Exception:
            pass
        # confirm_barrier (global snapshot, STRING key, TTL 14 days)
        try:
            snapshot_cb = self._confirm_barrier_cal.snapshot()
            rc.set(RK.AUTOCAL_CONFIRM_BARRIER, _json.dumps(snapshot_cb), ex=14 * 24 * 3600)
        except Exception:
            pass

    # Map from signal kind → calibration bin name used in ConfirmationBarrierCalibrator.
    # Original breakout/absorption keep their names; all other OF signal kinds get
    # mapped to "of" so the calibrator accumulates a cross-kind OBI distribution.
    _CB_KIND_MAP: dict[str, str] = {
        "breakout": "breakout", "bo": "breakout",
        "absorption": "absorption", "abs": "absorption",
        "iceberg": "iceberg",
        "delta_spike": "delta_spike",
    }

    def _confirm_barrier_observe(
        self,
        *,
        symbol: str,
        signal: dict[str, Any],
        runtime: Any,
        now_ms: int,
        indicators: dict[str, Any] | None = None,
    ) -> None:
        """Observe OBI ratio for ConfirmationBarrierCalibrator on every signal emit.

        Priority for OBI source:
          1. lob_obi_5 / depth_imbalance_5 from indicators (direction-agnostic raw ratio)
          2. depth_5_bid_vol / depth_5_ask_vol from runtime.last_book (direction-aware)

        All signal kinds are observed (iceberg, delta_spike, breakout, absorption, …),
        each mapped to a calibration bin via _CB_KIND_MAP (unknown → "of").
        This ensures enough sample volume to reach min_samples=30 within days.
        """
        try:
            kind_raw = str(signal.get("kind") or "").lower().strip()
            cal_kind = self._CB_KIND_MAP.get(kind_raw, "of")

            ind = indicators or {}
            side_raw = str(signal.get("direction") or signal.get("side") or "").lower()
            dir_up = side_raw in ("buy", "long", "up", "bull", "1")

            obi_ratio: float | None = None

            # Source 1: indicators lob_obi_5 (already direction-weighted imbalance >0)
            _raw_obi = float(ind.get("lob_obi_5") or ind.get("depth_imbalance_5") or 0.0)
            if _raw_obi > 0.0:
                obi_ratio = _raw_obi
            else:
                # Source 2: runtime book depth volumes (direction-aware)
                if runtime is None:
                    return
                book = getattr(runtime, "last_book", None)
                if book is None:
                    return
                bid_vol = float(getattr(book, "depth_5_bid_vol", 0.0) or 0.0)
                ask_vol = float(getattr(book, "depth_5_ask_vol", 0.0) or 0.0)
                if bid_vol <= 0.0 or ask_vol <= 0.0:
                    return
                if cal_kind == "absorption":
                    # absorption: counter-side must dominate
                    obi_ratio = ask_vol / bid_vol if dir_up else bid_vol / ask_vol
                else:
                    obi_ratio = bid_vol / ask_vol if dir_up else ask_vol / bid_vol

            if obi_ratio is None or obi_ratio <= 0.0:
                return
            self._confirm_barrier_cal.observe((symbol or "").upper(), cal_kind, obi_ratio, now_ms)
        except Exception:
            pass

    def _pipeline_calib_observe_on_emit(self, *, symbol: str, signal: dict[str, Any], indicators: dict[str, Any], now_ms: int) -> None:
        """Observe pipeline calibrators on a successful emit. Fail-open."""
        try:
            self._pipeline_calib_load()
        except Exception:
            pass
        sym = (symbol or "").upper()
        if not sym:
            return
        # cooldown: just needs (symbol, emit_ts_ms)
        try:
            self._cooldown_calib.observe(symbol=sym, emit_ts_ms=float(now_ms))
        except Exception:
            pass
        # vol_z_thr: regime = "{sym}:{session}"; needs a z-score
        try:
            session = str(signal.get("session") or indicators.get("session") or "na").lower()
            vol_z = indicators.get("vol_z") or indicators.get("volume_z") or indicators.get("vol_z_score")
            if vol_z is not None:
                self._vol_z_calib.observe(regime=f"{sym.lower()}:{session}", vol_z=float(vol_z))
        except Exception:
            pass
        # htf_proximity: needs (symbol, dist_bps, daily_atr_bps)
        try:
            dist_bps = indicators.get("htf_dist_bps") or indicators.get("htf_proximity_bps")
            daily_atr_bps = indicators.get("atr_daily_bps") or indicators.get("daily_atr_bps") or indicators.get("atr_1d_bps")
            if dist_bps is not None and daily_atr_bps is not None:
                self._htf_prox_calib.observe(
                    symbol=sym, dist_bps=float(dist_bps), daily_atr_bps=float(daily_atr_bps),
                )
        except Exception:
            pass
        # liq_wall: needs (symbol, size_z, dist_bps)
        try:
            wall_size_z = indicators.get("liq_wall_size_z") or indicators.get("wall_size_z")
            wall_dist_bps = indicators.get("liq_wall_dist_bps") or indicators.get("wall_dist_bps")
            if wall_size_z is not None and wall_dist_bps is not None:
                self._liq_wall_calib.observe(symbol=sym, size_z=float(wall_size_z), dist_bps=float(wall_dist_bps))
        except Exception:
            pass
        # throttled snap – offload Redis hset to thread pool so event loop is not blocked
        try:
            import asyncio
            safe_create_task(
                asyncio.to_thread(self._pipeline_calib_snap, now_ms),
                name="calib_snap",
            )
        except Exception:
            pass

    # ----------------------------------------------------------------------
    # Confirmation Barrier integration helpers (audit 2026-05-18 fix #E)
    # ----------------------------------------------------------------------

    def _barrier_submit(self, runtime: "SymbolRuntime", signal: dict[str, Any]) -> "str | None":
        """Submit a signal to the confirmation barrier.

        Returns:
            ``"ALLOW"`` immediately when the barrier is in ``off`` mode or the
            signal cannot be queued (unknown side, bad price, no sid). In
            that case the caller MUST continue publishing inline.

            ``None`` when the signal was queued. In that case the caller MUST
            return without publishing — :meth:`barrier_poll_and_publish` will
            re-inject it later (or drop it).
        """
        try:
            barrier = self._barrier
            if barrier.mode == "off":
                return "ALLOW"
            sid = str(signal.get("sid") or signal.get("signal_id") or "").strip()
            if not sid:
                return "ALLOW"  # cannot track without an id → fail-open
            side = signal.get("direction") or signal.get("side") or ""
            entry = signal.get("entry") or signal.get("price") or signal.get("entry_price")
            trigger_ts = (
                signal.get("tick_ts")
                or signal.get("ts_ms")
                or getattr(runtime, "last_ts_ms", None)
                or get_ny_time_millis()
            )
            try:
                trigger_price = float(entry or 0.0)
            except (TypeError, ValueError):
                trigger_price = 0.0
            try:
                trigger_ts_ms = int(trigger_ts)
            except (TypeError, ValueError):
                trigger_ts_ms = int(get_ny_time_millis())
            dec = barrier.submit(
                signal_id=sid,
                symbol=runtime.symbol,
                side=str(side),
                trigger_price=trigger_price,
                trigger_ts_ms=trigger_ts_ms,
                payload=signal,
            )
            # In shadow mode: barrier returns None (pending), but caller is
            # ALLOWED to publish immediately. We still want the SHADOW_DROP
            # telemetry to fire on poll → keep the entry, but return ALLOW.
            if barrier.mode == "shadow":
                return "ALLOW"
            return dec  # None → defer; "ALLOW" → publish inline
        except Exception:
            logger.exception("barrier_submit failed — failing open")
            return "ALLOW"

    def barrier_observe_tick(self, symbol: str, ts_ms: int, price: float) -> None:
        """Feed an observation (tick or bar close) to the barrier.

        Wire this from the tick handler in the strategy. Fail-open on errors.
        """
        try:
            self._barrier.observe(symbol=symbol, ts_ms=int(ts_ms), price=float(price))
        except Exception:
            pass

    async def _maybe_refresh_ctx_tighten_caps(self, ctx: Any) -> None:
        """TTL-cached read of autocal:ctx_tighten:state → update sentiment/defillama cap.

        Tri-state behaviour (self._ctx_tighten_autocal_mode):
          "on"   → always apply calibrated caps
          "auto" → apply only when calibrator has set auto_promoted=True per gate
          "off"  → method never called (guarded by caller)
        """
        import time as _t
        now_ms = int(_t.time() * 1000)
        if now_ms - self._ctx_tighten_autocal_last_ms < self._ctx_tighten_autocal_ttl_ms:
            return
        try:
            import json as _json
            from core.redis_keys import RK as _RK
            _redis = getattr(ctx, "redis", None)
            if _redis is None:
                return
            raw = await _redis.get(_RK.AUTOCAL_CTX_TIGHTEN_STATE)
            if not raw:
                return
            state = _json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
            sv = int(state.get("schema_version", 0) or 0)
            if sv < 1:
                return
            _s = state.get("sentiment") or {}
            _d = state.get("defillama") or {}
            _mode = self._ctx_tighten_autocal_mode
            _s_apply = _mode == "on" or (_mode == "auto" and bool(_s.get("auto_promoted", False)))
            _d_apply = _mode == "on" or (_mode == "auto" and bool(_d.get("auto_promoted", False)))
            if _s_apply:
                _sc = float(_s.get("cap_bps", 0.0) or 0.0)
                if _sc > 0.0:
                    self._cached_sentiment_ctx_tighten_cap = _sc
            if _d_apply:
                _dc = float(_d.get("cap_bps", 0.0) or 0.0)
                if _dc > 0.0:
                    self._cached_defillama_ctx_tighten_cap = _dc
        except Exception:
            pass
        finally:
            self._ctx_tighten_autocal_last_ms = now_ms

    async def barrier_poll_and_publish(
        self,
        now_ms: int,
        runtime_resolver,
    ) -> int:
        """Resolve barrier-pending signals and publish ALLOWED ones.

        Args:
            now_ms: current wall-clock in ms.
            runtime_resolver: callable ``(symbol) -> SymbolRuntime | None`` used
                to fetch the runtime for re-publish.

        Returns:
            Number of signals processed (allowed + dropped).
        """
        try:
            resolved = self._barrier.poll(int(now_ms))
        except Exception:
            logger.exception("barrier_poll failed")
            return 0
        if not resolved:
            if _BARRIER_PENDING is not None:
                try:
                    _BARRIER_PENDING.set(len(self._barrier))
                except Exception:
                    pass
            return 0
        processed = 0
        for sid, decision, reason, payload in resolved:
            processed += 1
            sym = str((payload or {}).get("symbol") or "") if isinstance(payload, dict) else ""
            # Normalise reason → reason_code for Prometheus (strip numeric suffix).
            reason_code = str(reason or "unknown").split("=")[0].split("<")[0].rstrip("_")
            if _BARRIER_DECISION_TOTAL is not None:
                try:
                    _BARRIER_DECISION_TOTAL.labels(
                        symbol=sym or "unknown",
                        decision=decision,
                        reason_code=reason_code,
                    ).inc()
                except Exception:
                    pass
            if decision in ("ALLOW",):
                if not isinstance(payload, dict):
                    logger.warning("barrier_poll: signal %s ALLOW but payload is not dict — skip", sid)
                    continue
                if not sym:
                    logger.warning("barrier_poll: signal %s ALLOW but runtime for %s not found — skip", sid, sym)
                    continue
                runtime = runtime_resolver(sym)
                if runtime is None:
                    logger.warning("barrier_poll: signal %s ALLOW but runtime for %s not found — skip", sid, sym)
                    continue
                payload[self._BARRIER_RESOLVED_KEY] = True
                payload.setdefault("indicators", {})["barrier_resolution"] = reason
                try:
                    await self.publish_signal(runtime, payload)
                except Exception:
                    logger.exception("barrier_poll: re-publish failed for %s", sid)
            else:
                # DROP / SHADOW_DROP / SHADOW_ALLOW → log only, do not republish.
                logger.info(
                    "🪤 [BARRIER] sid=%s decision=%s reason=%s — not republished",
                    sid, decision, reason,
                )
        if _BARRIER_PENDING is not None:
            try:
                _BARRIER_PENDING.set(len(self._barrier))
            except Exception:
                pass
        return processed

    async def publish_signal(self, runtime: SymbolRuntime, signal: dict[str, Any]) -> None:
        """
        Публикация сигнала в необходимые каналы.
        """
        symbol = runtime.symbol

        # last_trade_outcome_raw via trades:closed Redis read (audit 2026-05-19
        # Phase 4). TTL-cached per symbol (60s) to keep cost negligible.
        # Wrapped in asyncio.to_thread to avoid blocking the event loop on
        # cache miss (sync redis.Redis.xrevrange, up to 2s socket timeout).
        try:
            _ind = signal.get("indicators")
            if isinstance(_ind, dict):
                import asyncio
                from core.last_trade_outcome_reader import get_last_trade_outcome_bps
                v = await asyncio.to_thread(get_last_trade_outcome_bps, symbol)
                if v not in (None, 0, 0.0):
                    _ind["last_trade_outcome_raw"] = float(v)
        except Exception:
            pass

        # eth_btc_corr_5m via Python tick-stream poller (audit 2026-05-19
        # Phase 5). go-worker REST polling into runtime:crossasset doesn't
        # exist in prod — compute directly from stream:tick_BTCUSDT/ETHUSDT
        # on redis-ticks. TTL 30s; cost ≈ 1ms warm. asyncio.to_thread to keep
        # event loop free on cache miss (redis xrevrange + math).
        try:
            _ind = signal.get("indicators")
            if isinstance(_ind, dict):
                import asyncio
                from core.cross_asset_corr_reader import get_eth_btc_corr_5m
                v = await asyncio.to_thread(get_eth_btc_corr_5m)
                if v not in (None, 0, 0.0):
                    _ind["eth_btc_corr_5m"] = float(v)
        except Exception:
            pass

        # Group MA trade stats (large_trade_ratio, trade_size_entropy) —
        # TickProcessor in reference/ would maintain these rolling. Reader
        # computes from stream:tick_{SYMBOL} on redis-ticks instead.
        # TTL 10s + 5min rolling window; ~1ms warm via asyncio.to_thread.
        try:
            _ind = signal.get("indicators")
            if isinstance(_ind, dict):
                import asyncio
                from core.trade_stats_reader import get_trade_stats
                large_ratio, entropy = await asyncio.to_thread(get_trade_stats, symbol)
                if large_ratio not in (None, 0, 0.0):
                    _ind["large_trade_ratio"] = float(large_ratio)
                if entropy not in (None, 0, 0.0):
                    _ind["trade_size_entropy"] = float(entropy)
        except Exception:
            pass

        # Group MB book stats — depth_migration_bps only (audit 2026-05-19
        # Phase 6 + rollback). quote_stuffing_score path REMOVED: reader
        # produced OOD values ~65-90 (intended scale [0,1] as cancel/quote
        # ratio); fed ManipulationGate VETO_QUOTE_STUFFING triggers in prod.
        # depth_migration_bps stays — small bps values, safe distribution.
        # Fallback chain: book_stats_reader (L2 snapshots on redis-ticks) →
        # runtime.depth_migration_bps_ema (Go crossasset EMA, always available).
        try:
            _ind = signal.get("indicators")
            if isinstance(_ind, dict):
                import asyncio
                from core.book_stats_reader import get_book_stats
                dm, _qs = await asyncio.to_thread(get_book_stats, symbol)
                if dm not in (None, 0, 0.0):
                    _ind["depth_migration_bps"] = float(dm)
                elif runtime is not None:
                    _ema = float(getattr(runtime, "depth_migration_bps_ema", 0.0) or 0.0)
                    if _ema != 0.0:
                        _ind["depth_migration_bps"] = _ema
                # quote_stuffing_score intentionally NOT written — wait for
                # proper cancel/quote split tracker (BookProcessor migration).
        except Exception:
            pass

        # tick_direction_run per-symbol direction-sign counter (audit 2026-05-19
        # Phase 7). Original schema semantic: max consecutive same-sign TICK
        # runs in rolling tick window (TickProcessor in reference/). Defensible
        # proxy at signal granularity: count consecutive same-direction signals
        # per symbol. Cap at 50 to avoid OOD outliers in long streaks. Natural
        # distribution typically 1-5 → safely inside expected schema range [1,20].
        try:
            _ind = signal.get("indicators")
            _direction = str(signal.get("direction") or signal.get("side") or "").upper()
            if isinstance(_ind, dict) and _direction in ("LONG", "SHORT", "BUY", "SELL"):
                if not hasattr(self, "_tick_dir_run_state"):
                    # symbol -> (last_norm_dir, current_run_length)
                    self._tick_dir_run_state = {}  # type: ignore[attr-defined]
                tdr_state = self._tick_dir_run_state  # type: ignore[attr-defined]
                _norm_dir = "LONG" if _direction in ("LONG", "BUY") else "SHORT"
                prev = tdr_state.get(symbol)
                if prev is None or prev[0] != _norm_dir:
                    run = 1
                else:
                    run = min(50, int(prev[1]) + 1)
                tdr_state[symbol] = (_norm_dir, run)
                _ind["tick_direction_run"] = float(run)
        except Exception:
            pass

        # signal_frequency_1h per-symbol counter (audit 2026-05-19 Phase 3).
        # Runtime trackers live in reference/ — runtime.signal_count_1h never
        # populates. Maintain in-instance deque of recent publish timestamps,
        # prune > 1h. Runs BEFORE the v12_of inject below so compute_group_me
        # sees the populated indicators[signal_frequency_1h] value.
        try:
            _ind = signal.get("indicators")
            _ts = int(signal.get("ts_ms", 0) or 0)
            if isinstance(_ind, dict) and _ts > 0:
                if not hasattr(self, "_signal_freq_1h_state"):
                    self._signal_freq_1h_state = {}  # type: ignore[attr-defined]
                state = self._signal_freq_1h_state  # type: ignore[attr-defined]
                from collections import deque as _deque
                q = state.get(symbol)
                if q is None:
                    q = _deque(maxlen=10000)
                    state[symbol] = q
                cutoff = _ts - 3_600_000
                while q and q[0] < cutoff:
                    q.popleft()
                q.append(_ts)
                _ind["signal_frequency_1h"] = float(len(q))
        except Exception:
            pass

        # v12_of new groups (MA/MB/MC/MD/ME/MX, 21 keys): inject_v12_of_features
        # is dead code in active pipeline (only reference/tick_processor.py imports
        # it). Without this wiring those 21 v13_of base keys are ABSENT in the
        # outbound signals:of:inputs payload — verified empirically on golden
        # fixture 2026-05-19 (`audit_v12_of_inject_dead_code_2026_05_19`).
        # signal["indicators"] is the dict that ships in enriched_signal →
        # _publish_of_inputs → signals:of:inputs.
        # Defense-in-depth: of_confirm_engine.build() already injects 21 v12_of
        # MA/MB/MC/MD/ME/MX keys into signal["indicators"]. This second inject
        # covers any code path that bypasses build() (barrier replay, future
        # alternative producers). setdefault semantics — never overwrites
        # populated values. See audit_v12_of_inject_dead_code_2026_05_19.
        try:
            _ind = signal.get("indicators")
            if isinstance(_ind, dict):
                from core.v12_of_features import inject_v12_of_features
                inject_v12_of_features(
                    runtime=runtime,
                    now_ms=int(signal.get("ts_ms", 0) or 0),
                    indicators=_ind,
                )
        except Exception:
            pass

        # ML Feature Bridge: regime + L2 microstructure features that are computed
        # in the active pipeline but never written to indicators. All setdefault —
        # never overwrites existing values. Fail-open per field.
        try:
            _ind = signal.get("indicators")
            if isinstance(_ind, dict):
                # Regime features (stored on runtime by kline handler after update_regime)
                _ind.setdefault("atr_q", float(getattr(runtime, "_last_atr_q", 0.5) or 0.5))
                _ind.setdefault("delta_ema", float(getattr(runtime, "_regime_delta_ema", 0.0) or 0.0))
                _rg_score = float(getattr(runtime, "_last_regime_score", 0.0) or 0.0)
                _ind.setdefault("trend_score", max(0.0, _rg_score))
                _ind.setdefault("range_score", max(0.0, -_rg_score))
                # L2 book microstructure (LOBPressureTracker runtime attrs as proxies)
                _ind.setdefault("spread_bps_z", float(getattr(runtime, "last_spread_z", 0.0) or 0.0))
                _ind.setdefault("obi_avg_20", float(getattr(runtime, "lob_dw_obi", 0.0) or 0.0))
                _ind.setdefault("microprice_shift_bps_20", float(getattr(runtime, "lob_micro_shift_bps", 0.0) or 0.0))
                _ind.setdefault("depth_bid_20", float(getattr(runtime, "last_depth_bid_5", 0.0) or 0.0))
                _ind.setdefault("depth_ask_20", float(getattr(runtime, "last_depth_ask_5", 0.0) or 0.0))
                _ind.setdefault("slope_bid_20", float(getattr(runtime, "lob_depth_slope_bid", 0.0) or 0.0))
                _ind.setdefault("slope_ask_20", float(getattr(runtime, "lob_depth_slope_ask", 0.0) or 0.0))
                # wall_bid/ask_dist_bps: GPU wall price → distance from mid in bps
                _mid = float(getattr(runtime, "last_book_mid", 0.0) or 0.0)
                _wall_bid_px = float(getattr(runtime, "last_gpu_wall_bid_price", 0.0) or 0.0)
                _wall_ask_px = float(getattr(runtime, "last_gpu_wall_ask_price", 0.0) or 0.0)
                _ind.setdefault(
                    "wall_bid_dist_bps",
                    abs(_mid - _wall_bid_px) / _mid * 10_000.0 if _mid > 0.0 and _wall_bid_px > 0.0 else 0.0,
                )
                _ind.setdefault(
                    "wall_ask_dist_bps",
                    abs(_wall_ask_px - _mid) / _mid * 10_000.0 if _mid > 0.0 and _wall_ask_px > 0.0 else 0.0,
                )
                # obi_local_q: OBI robust z-score as local-quantile proxy
                _ind.setdefault("obi_local_q", float(getattr(runtime, "dw_obi_z", 0.0) or 0.0))
                # delta_spike_z: reuse delta_z as proxy (same underlying delta series)
                _ind.setdefault("delta_spike_z", float(_ind.get("delta_z", 0.0) or 0.0))
                _ind.setdefault("delta_spike_z_local_q", 0.5)
                # atr_local_q: same as atr_q (best available ATR quantile in active pipeline)
                _ind.setdefault("atr_local_q", float(_ind.get("atr_q", 0.5) or 0.5))
                # weak_ratio: range_vs_atr proxy (neutral 1.0 if unavailable)
                _ind.setdefault("weak_ratio", float(_ind.get("range_vs_atr", 1.0) or 1.0))
        except Exception:
            pass

        side_norm = normalize_side_3_safe(signal.get("direction") or signal.get("side") or "")
        if side_norm is None:
            logger.warning("⚠️ (%s) publish_signal: unknown direction=%r side=%r (skip)",
                           symbol, signal.get("direction"), signal.get("side"))
            return
        direction = side_norm.direction
        cfg = runtime.config

        # ------------------------------------------------------------------
        # Confirmation Barrier (audit 2026-05-18): defer publish until
        # follow-through confirms. In enforce mode the signal is held in
        # _barrier and re-injected by barrier_poll_and_publish() after the
        # deadline; here we just submit and return. In shadow mode we still
        # publish immediately — barrier logs would-have-dropped for telemetry.
        # In off mode we are a no-op.
        if not signal.get(self._BARRIER_RESOLVED_KEY):
            barrier_dec = self._barrier_submit(runtime, signal)
            if barrier_dec is None:
                # Pending — deferred publish; caller continues with other work.
                return
            # When mode == "off" or submit is bypassed → continue inline.

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
            dec_str = getattr(dec, "decision", "UNKNOWN")
            with contextlib.suppress(Exception):
                if _PRE_PUBLISH_GATE_EVAL_TOTAL is not None:
                    _PRE_PUBLISH_GATE_EVAL_TOTAL.labels(
                        gate=dec.gate,
                        decision=dec_str,
                        symbol=symbol,
                        kind=kind,
                    ).inc()
            if _PRE_PUBLISH_VETO_TOTAL is not None and dec_str in ("DENY", "VETO"):
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

                # Propagate SMT fields from ctx → indicators before veto so the
                # ML training record has SMT coherence features for all signal paths.
                # pass-path does the same at L2897 after all gates; here we mirror it
                # at veto time with whatever ctx state is current.
                with contextlib.suppress(Exception):
                    for _smt_k in (
                        "smt_leader_dir", "smt_leader_confirm", "smt_coh",
                        "smt_align", "smt_state_stale", "smt_bundle_id", "smt_blocked",
                    ):
                        if _smt_k not in indicators:
                            _sv = getattr(ctx, _smt_k, None)
                            if _sv is not None:
                                indicators[_smt_k] = _sv

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
                    signal=signal,
                    runtime=runtime,
                )
                return True
            if dec.decision == "TIGHTEN":
                tadd = float(dec.notes.get("tighten_add_bps", 0.0))
                if tadd > 0:
                    exp0 = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                    indicators["expected_slippage_bps"] = exp0 + tadd
                    # Per-gate attribution for ctx_tighten calibrator (P2)
                    _gate = dec.gate
                    if _gate == "SentimentContextGate":
                        indicators["ctx_sentiment_tighten_bps"] = (
                            indicators.get("ctx_sentiment_tighten_bps", 0.0) or 0.0
                        ) + tadd
                    elif _gate == "DefiLlamaContextGate":
                        indicators["ctx_defillama_tighten_bps"] = (
                            indicators.get("ctx_defillama_tighten_bps", 0.0) or 0.0
                        ) + tadd
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

        # 4. Entry Policy (spread shock / burst flip / c2t / freeze + adaptive calibrators
        # + HTF LONG bias gate). Drives autocal:adverse_cross / spread_staleness / burst_c2t.
        if _apply_decision(self.orchestrator.check_entry_policy(ctx, kind, side=direction)): return  # type: ignore

        # ------------------------------------------------------------------
        # STAGE 2: CONTEXT & MARKET GATES (Fail-Open / Tighten / Veto)
        # ------------------------------------------------------------------
        
        # Async Context Gates
        if _apply_decision(self.orchestrator.check_breadth(ctx, kind, direction)): return  # type: ignore
        
        if self._cached_deriv_ctx_enabled:
            _fz = self._cached_deriv_ctx_funding_z
            _bb = self._cached_deriv_ctx_basis_bps
            if self._funding_z_reader is not None:
                try:
                    _vol_reg = str(indicators.get("vol_regime") or indicators.get("regime") or "*")
                    _fz_cal = self._funding_z_reader.get_thresholds(symbol, _vol_reg)
                    if _fz_cal is not None:
                        _fz, _bb = _fz_cal
                except Exception:
                    pass
            if _apply_decision(await self.orchestrator.check_derivatives_context(  # type: ignore
                ctx, kind, direction,
                profile=self._cached_deriv_ctx_profile,
                thr_funding_z=_fz,
                thr_basis_bps=_bb,
                require_oi_for_veto=self._cached_deriv_ctx_require_oi,
                tighten_mult=self._cached_deriv_ctx_tighten_mult,
                tighten_cap_bps=self._cached_deriv_ctx_tighten_cap,
            )): return

        # Refresh calibrated caps (TTL-cached, fail-open)
        if self._ctx_tighten_autocal_enabled:
            await self._maybe_refresh_ctx_tighten_caps(ctx)

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
            # Resolve adaptive thresholds from autocalibrator (fail-open to ENV)
            _cv_sym = str(getattr(ctx, "symbol", "") or "")
            if self._cv_calib_reader is not None:
                _cv_disloc_z, _cv_min_agree = self._cv_calib_reader.thresholds_for(
                    _cv_sym,
                    default_disloc_z=self._cached_crossvenue_ctx_max_dislocation_z,
                    default_min_agree=self._cached_crossvenue_ctx_min_agree,
                )
            else:
                _cv_disloc_z  = self._cached_crossvenue_ctx_max_dislocation_z
                _cv_min_agree = self._cached_crossvenue_ctx_min_agree
            if _apply_decision(await self.orchestrator.check_crossvenue_context(  # type: ignore
                ctx, kind, direction,
                profile=self._cached_crossvenue_ctx_profile,
                max_age_ms=self._cached_crossvenue_ctx_max_age_ms,
                min_agree=_cv_min_agree,
                max_dislocation_z=_cv_disloc_z,
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
            # Per-symbol автокалибровка: читаем committed p95-пороги из Redis снапшота.
            # Если reader disabled/cold/stale → возвращает (0.0, 0.0) → gate проходит.
            # ENV-дефолты (0.0) остаются fallback при отключённом reader'е.
            _ftox_thr_z = self._cached_flow_thr_z
            _ftox_thr_vpin = self._cached_flow_thr_vpin
            if self._flow_tox_reader is not None:
                try:
                    _cal_z, _cal_vpin = self._flow_tox_reader.get_thresholds(symbol)
                    if _cal_z > 0.0:
                        _ftox_thr_z = _cal_z
                    if _cal_vpin > 0.0:
                        _ftox_thr_vpin = _cal_vpin
                except Exception:
                    pass
            if _apply_decision(self.orchestrator.check_flow_toxicity(  # type: ignore
                ctx, kind,
                profile=self._cached_flow_tox_profile,
                thr_z=_ftox_thr_z,
                thr_vpin=_ftox_thr_vpin,
                thr_is=self._cached_flow_thr_is,
                thr_imp=self._cached_flow_thr_imp,
                tighten_mult=self._cached_flow_mult,
                tighten_cap_bps=self._cached_flow_cap,
                veto_without_tca=self._cached_flow_veto_wo_tca,
            )): return

        if self._cached_manip_enabled:
            _manip_thr_qs = self._cached_manip_thr_qs
            _manip_thr_lay = self._cached_manip_thr_lay
            _manip_tighten_cap = self._cached_manip_tighten_cap
            
            if self._manip_reader is not None:
                try:
                    _cal_manip = self._manip_reader.get_thresholds(symbol)
                    if _cal_manip:
                        if _cal_manip.get("layering_score_max", 0) > 0:
                            _manip_thr_lay = _cal_manip["layering_score_max"]
                        if _cal_manip.get("qs_score_max", 0) > 0:
                            _manip_thr_qs = _cal_manip["qs_score_max"]
                        if _cal_manip.get("tighten_bps", 0) > 0:
                            _manip_tighten_cap = _cal_manip["tighten_bps"]
                except Exception:
                    pass

            if _apply_decision(self.orchestrator.check_manipulation_gate(  # type: ignore
                ctx, kind,
                profile=self._cached_manip_profile,
                thr_qs=_manip_thr_qs,
                thr_lay=_manip_thr_lay,
                thr_otr_z=self._cached_manip_thr_otr_z,
                tighten_mult=self._cached_manip_tighten_mult,
                tighten_cap_bps=_manip_tighten_cap,
            )): return

        if _apply_decision(self.orchestrator.consistency_once(ctx=ctx, symbol=symbol, kind=kind, side=direction)): return  # type: ignore

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

        # Propagate SMT audit fields from ctx → indicators so they survive into
        # order:{id}.signal_payload.indicators (consumed by reporter SMT VETO sim
        # and reliability calibrators). pre_publish_gates.SmtCoherenceGate writes
        # them onto ctx; without this copy they never reach the payload.
        for _k, _default in (
            ("smt_leader_dir", "NA"),
            ("smt_leader_confirm", 0),
            ("smt_coh", float("nan")),
            ("smt_align", 0),
            ("smt_state_stale", 1),
            ("smt_bundle_id", ""),
            ("smt_blocked", 0),
            ("smt_leader_conf_score", None),
        ):
            if _k in indicators:
                continue
            _v = getattr(ctx, _k, None)
            if _v is None:
                continue
            indicators[_k] = _v

        # Extract delta values from indicators (where they're actually stored)
        delta = float(indicators.get("delta", 0.0))
        delta_z = float(indicators.get("delta_z", 0.0))
        # Ensure they're also available at top level for backward compatibility
        signal.setdefault("delta", delta)
        signal.setdefault("delta_z", delta_z)
        indicators.setdefault("tick_qty", float(signal.get("tick_qty") or 0.0))

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
            # Write resolved regime back so it appears in the published payload
            if _raw_regime and _raw_regime != "na":
                indicators["regime"] = _raw_regime
            _profile_regime_bucket = _regime_group(_raw_regime)
            # Bear trend gets its own profile bucket so the router picks bear_trend_follow_v1
            if "trending_bear" in _raw_regime:
                _profile_regime_bucket = "trending_bear"

            _profile_decision = self._profile_router.route(
                symbol=symbol,
                regime_bucket=_profile_regime_bucket,
                kind=kind,
                overrides=self._get_profile_overrides() or None,
            )

            # --- Phase 2: execution_policy binding ---
            _exec_policy_from_profile = _profile_decision.profile.execution_policy
            indicators.setdefault("execution_policy_profile", _exec_policy_from_profile)

            # --- net_edge gate (fail-open unless ENFORCE enabled) ---
            if self._cached_profile_net_edge_enforce and _profile_decision.mode == "LIVE":
                _spread_bps = float(indicators.get("spread_bps", 0.0) or 0.0)
                _slip_bps = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                _fee_bps = self._cached_fees_bps_rt
                # expected_edge_bps: prefer upstream indicator, then actual TP1/SL
                # from signal, then ATR-based approximation.
                # NOTE: the field is only written to enriched_signal ~800 lines
                # later, so indicators never has it at this point in the pipeline.
                _ev_bps = float(indicators.get("expected_edge_bps", 0.0) or 0.0)
                if _ev_bps == 0.0:
                    try:
                        _tp_r_gate = parse_tp_ratio(str(cfg.get("tp_ratio", "")))
                        _tp1_share_gate = _tp_r_gate[0] if _tp_r_gate else 0.5
                        _sl_gate = float(signal.get("sl", 0.0) or 0.0)
                        _tp1_gate = float(signal.get("tp1", 0.0) or 0.0)
                        if entry > 0 and _tp1_gate > 0 and _sl_gate > 0:
                            # Use actual TP1/SL levels — most accurate estimate
                            _tp1_dist_bps = abs(_tp1_gate - entry) / entry * 10_000.0
                            _sl_dist_bps = abs(entry - _sl_gate) / entry * 10_000.0
                            _ev_bps = max(0.0, _tp1_dist_bps * _tp1_share_gate - _sl_dist_bps * (1.0 - _tp1_share_gate))
                        else:
                            # Fallback: ATR-based approximation (less accurate)
                            _atr_gate = float(indicators.get("atr", 0.0) or 0.0)
                            if entry > 0 and _atr_gate > 0:
                                _rocket_gate = self._get_rocket_multiplier(symbol) or 1.5
                                _tp1_bps_gate = (_atr_gate * _rocket_gate / entry) * 10_000.0 * _tp1_share_gate
                                _stop_bps_gate = (_atr_gate / entry) * 10_000.0
                                _ev_bps = max(0.0, _tp1_bps_gate - _stop_bps_gate)
                    except Exception:
                        pass
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
            # Resolution order:
            #   1) indicators["symbol_tier"]  (already enriched upstream)
            #   2) signal["symbol_tier"] / signal["risk_tier"]  (set by signal_gate after risk eval)
            #   3) infer_symbol_tier(symbol)  (deterministic from symbol pattern, e.g. 1000PEPE → C)
            #   4) hard default "B"  (legacy fallback only when import fails)
            #
            # Fix 2026-05-14: previously fell straight to "B", which mis-classified
            # 1000PEPEUSDT and other memes as alts, causing per-class memes-overlay
            # (stop_atr_mult_memes / max_zone_bp_memes) to never apply.
            _sym_tier_raw = (
                indicators.get("symbol_tier")
                or signal.get("symbol_tier")
                or signal.get("risk_tier")
                or signal.get("tier")
            )
            _sym_tier = str(_sym_tier_raw or "").strip().upper()
            if _sym_tier not in {"A", "B", "C"}:
                try:
                    from services.risk.risk_policy_engine import infer_symbol_tier as _infer_tier
                    _sym_tier = (_infer_tier(symbol) or "B").upper()
                except Exception:
                    _sym_tier = "B"
            if _sym_tier not in {"A", "B", "C"}:
                _sym_tier = "B"
            indicators["symbol_tier"] = _sym_tier
            indicators["symbol_tier_source"] = "indicators" if indicators.get("symbol_tier") and _sym_tier_raw else (
                "signal" if signal.get("symbol_tier") or signal.get("risk_tier") or signal.get("tier") else "inferred"
            )
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

        # Phase 3 fix: apply TradeProfileRouter trail_profile BEFORE _calculate_levels.
        # TP ratios/RR: TRADE_PROFILE_TP_ENFORCE=1 → applied to cfg via indicator keys
        # that _calculate_levels reads (profile_tp_rr, profile_tp_ratio).
        # Default shadow: written as _shadow indicators only, not applied to cfg.
        _tp_enforce = os.getenv("TRADE_PROFILE_TP_ENFORCE", "0") == "1"
        try:
            if (
                _profile_decision is not None
                and _profile_decision.allowed
                and _profile_decision.mode == "LIVE"
                and _profile_decision.is_canary
            ):
                _pd_profile = _profile_decision.profile
                if _pd_profile.trailing_profile and trail_profile in ("protective_only", None, ""):
                    trail_profile = _pd_profile.trailing_profile
                    signal["trail_profile"] = trail_profile
                    if _pd_profile.trail_enabled:
                        signal.setdefault("trail_after_tp1", True)
                    if _pd_profile.trail_after_tp_level:
                        signal.setdefault("trail_after_tp_level", _pd_profile.trail_after_tp_level)
                    indicators["trail_profile_pre_calc"] = trail_profile
                _pd_tp_rr = getattr(_pd_profile, "tp_rr", None)
                _pd_tp_ratios = getattr(_pd_profile, "tp_ratios", None)
                _pd_has_geometry = _pd_tp_rr is not None or bool(_pd_tp_ratios)
                if _pd_tp_rr is not None:
                    if _tp_enforce:
                        indicators["profile_tp_rr"] = _pd_tp_rr          # → _calculate_levels applies to cfg
                        indicators["profile_tp_rr_enforced"] = _pd_tp_rr
                    else:
                        indicators["profile_tp_rr_shadow"] = _pd_tp_rr
                if _pd_tp_ratios:
                    if _tp_enforce:
                        indicators["profile_tp_ratio"] = ",".join(str(x) for x in _pd_tp_ratios)  # → _calculate_levels
                        indicators["profile_tp_ratios_enforced"] = list(_pd_tp_ratios)
                    else:
                        indicators["profile_tp_ratios_shadow"] = list(_pd_tp_ratios)
                if _pd_has_geometry:
                    _pname = getattr(_pd_profile, "name", "unknown")
                    _sym = symbol or "unknown"
                    if _tp_enforce:
                        if _TP_PROFILE_ENFORCE_TOTAL is not None:
                            _TP_PROFILE_ENFORCE_TOTAL.labels(profile=_pname, symbol=_sym).inc()
                    else:
                        if _TP_PROFILE_SHADOW_TOTAL is not None:
                            _TP_PROFILE_SHADOW_TOTAL.labels(profile=_pname, symbol=_sym).inc()
        except Exception:
            pass

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

        # Emit TP1 delta bps vs default when profile was enforced
        try:
            if (
                _TP_PROFILE_LEVEL_DELTA_BPS is not None
                and _tp_enforce
                and indicators.get("profile_tp_rr") is not None
                and tp_levels
                and entry
                and entry > 0
            ):
                _default_rr_str = cfg.get("tp_rr", "1.3,2.0,2.7")
                _default_tp1_rr = float(str(_default_rr_str).split(",")[0])
                _sl_dist = abs(entry - (sl or entry))
                _default_tp1 = entry + _sl_dist * _default_tp1_rr if direction == "LONG" else entry - _sl_dist * _default_tp1_rr
                _profile_tp1 = tp_levels[0]
                _delta_bps = abs(_profile_tp1 - _default_tp1) / entry * 10_000
                _pname = indicators.get("trail_profile_pre_calc") or "unknown"
                _sym = symbol or "unknown"
                _TP_PROFILE_LEVEL_DELTA_BPS.labels(profile=_pname, symbol=_sym).observe(_delta_bps)
        except Exception:
            pass

        # ATR-floor enrichment (early): ensures virtual veto signals emitted by
        # _handle_pipeline_veto also carry atr_*_th_bps keys in indicators.
        # Gated by ATR_FLOOR_ENRICHMENT_EARLY (default 0 = legacy post-gate position).
        # See investigation notes 2026-05-22: ind_atr_th_bps fill 17%→11% root cause.
        if os.getenv("ATR_FLOOR_ENRICHMENT_EARLY", "0") == "1":
            self._enrich_atr_floor_indicators(
                indicators=indicators, runtime=runtime, cfg=cfg, entry=entry, atr=atr,
            )

        # ----------------------------------------------------------------
        # G8 · Edge-Cost Gate (EV mode) — runs AFTER levels are computed
        # sl_price / tp1_price are now available; attach online EV stats from Redis
        # then evaluate. Fail-open on Redis errors (no veto if stats absent).
        # ----------------------------------------------------------------
        ctx.sl_price = sl
        ctx.tp1_price = tp_levels[0] if tp_levels else None
        with contextlib.suppress(Exception):
            # ev_tp1_stats uses a sync Redis client; run in thread pool to avoid blocking event loop.
            _ev_rc = getattr(self, "_ev_sync_redis", None)
            if _ev_rc is None:
                from handlers.crypto_orderflow.config.handler_config import _get_sync_redis
                self._ev_sync_redis = _ev_rc = _get_sync_redis()
            import asyncio
            await asyncio.to_thread(
                attach_tp1_hit_prob_to_ctx,
                ctx,
                redis_client=_ev_rc,
                kind=kind,
                symbol=symbol,
                tf=str(signal.get("tf") or indicators.get("tf") or "na"),
                cfg=self._ev_tp1_cfg,
            )
        if _apply_decision(self.orchestrator.edge_cost_cached(ctx=ctx, kind=kind, symbol=symbol, side=direction)): return  # type: ignore

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
                    # ── TP1_TARGET_R override (2026-05-19) ────────────────
                    # SHADOW (ENFORCE=0): только counterfactual индикаторы.
                    # ENFORCE=1: prepend как первый TP, существующие RR сдвигаем.
                    _tp1_tgt = self._cached_tp1_target_r
                    # ── Plan 3.3: Path-based TP via Triple-Barrier CDF ────
                    # Per-(symbol×regime×direction) median MFE among past
                    # winners — distribution-aware override over the static
                    # `_cached_tp1_target_r`. Always emit shadow indicator
                    # for replay; only swap `_tp1_tgt` when reader returns
                    # an enforced bucket value.
                    try:
                        from services.path_based_tp_runtime_overrides import (
                            get_bucket_for_inspection,
                            get_path_based_tp1_r,
                        )
                        _sym_pb = str(getattr(signal, "symbol", "") or "").upper()
                        _dir_pb = str(getattr(direction, "value", direction) or "").upper()
                        _rg_pb = rg_for_overrides or "na"
                        _shadow_bucket = get_bucket_for_inspection(
                            _sym_pb, _rg_pb, _dir_pb,
                        )
                        if _shadow_bucket:
                            indicators["tp1_target_r_path_shadow"] = round(
                                float(_shadow_bucket.get("tp1_r") or 0.0), 4
                            )
                            indicators["tp1_target_r_path_p50"] = round(
                                float(_shadow_bucket.get("p50") or 0.0), 4
                            )
                            indicators["tp1_target_r_path_n_winners"] = int(
                                _shadow_bucket.get("n_winners") or 0
                            )
                            indicators["tp1_target_r_path_bucket"] = (
                                _shadow_bucket.get("key") or ""
                            )
                        _path_tp1 = get_path_based_tp1_r(
                            _sym_pb, _rg_pb, _dir_pb,
                            default=(_tp1_tgt or 0.0),
                            require_enforce=True,
                        )
                        if _path_tp1 > 0 and _path_tp1 != _tp1_tgt:
                            _tp1_tgt = _path_tp1
                            indicators["tp1_target_r_path_enforce_applied"] = 1
                    except Exception:
                        pass
                    if _tp1_tgt > 0:
                        _shadow_tp1_dist = _stop_dist * _tp1_tgt
                        if getattr(direction, 'value', (direction or '')).upper() == "LONG":
                            indicators["tp1_target_r_shadow_price"] = round(
                                entry + _shadow_tp1_dist, 6
                            )
                        else:
                            indicators["tp1_target_r_shadow_price"] = round(
                                entry - _shadow_tp1_dist, 6
                            )
                        indicators["tp1_target_r_shadow_r"] = round(_tp1_tgt, 3)
                        if self._cached_tp1_target_r_enforce:
                            _range_rr = [_tp1_tgt] + [
                                r for r in _range_rr if r > _tp1_tgt
                            ]
                            _range_rr = _range_rr[:2]
                            indicators["tp1_target_r_enforce_applied"] = 1

                    # Apply TP1_MIN_RR_FLOOR so the range override doesn't undercut the global floor
                    # (range branch bypasses compute_levels(), where the floor lives).
                    # NOTE: при TP1_TARGET_R_ENFORCE=1 — floor НЕ применяется (target R
                    # сознательно опускается ниже текущего floor).
                    _floor = self._cached_tp1_min_rr_floor
                    if _floor > 0 and _range_rr[0] < _floor and not (
                        self._cached_tp1_target_r_enforce
                        and self._cached_tp1_target_r > 0
                    ):
                        _range_rr[0] = _floor
                        indicators["range_tp1_floor_applied"] = round(_floor, 3)
                    if getattr(direction, 'value', (direction or '')).upper() == "LONG":
                        tp_levels = [entry + _stop_dist * r for r in _range_rr]
                    else:
                        tp_levels = [entry - _stop_dist * r for r in _range_rr]
                    indicators["range_tp_rr_applied"] = ",".join(str(round(r, 3)) for r in _range_rr)
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
            # p_edge floor: configurable, default 0.0 (disabled).
            # edge_stack_v1 max p_edge ≈ 0.23 — hard-coding 0.58 permanently blocks
            # rocket_v1_bear. Re-enable when a model with AUC ≥ 0.60 is in enforce.
            _min_p_edge_bear = float(runtime.config.get(
                "bear_trend_min_p_edge", os.getenv("BEAR_TREND_MIN_P_EDGE", "0.0")
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
                and (_min_p_edge_bear <= 0.0 or _p_edge >= _min_p_edge_bear)
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
        # 3.1 · Regime-Conditional Execution Engine (vol × trend buckets)
        # ------------------------------------------------------------------
        # Maps (vol_regime × trend_regime) → ExecutionPolicy with overrides for
        # tp1_target_r / tp_ratios / trail_profile / atr_mult, or veto when
        # bucket says skip (choppy/squeeze/mixed). SHADOW by default — writes
        # counterfactual `regime_exec_*` indicators without overriding execution.
        # Enable enforce via REGIME_EXEC_ENGINE_ENFORCE=1 after replay-quantify.
        _regime_exec_result = self._evaluate_regime_exec_engine(
            indicators=indicators,
            signal=signal,
            symbol=symbol,
            kind=kind,
            runtime=runtime,
            trend_regime=rg_for_overrides,
            current_trail_profile=trail_profile,
            sig_ts=sig_ts,
        )
        _regime_exec_tp_ratios: list[float] | None = _regime_exec_result.get("tp_ratios")
        if _regime_exec_result.get("trail_profile"):
            trail_profile = _regime_exec_result["trail_profile"]
        if _regime_exec_result.get("veto_decision") is not None:
            if _apply_decision(_regime_exec_result["veto_decision"]):
                return  # type: ignore

        # ------------------------------------------------------------------
        # Phase D.2 · Entry Profile Classifier (SHADOW only)
        # ------------------------------------------------------------------
        # Новый слой taxonomy поверх `kind` (microstructure pattern).
        # Не влияет на исполнение — только пишет `entry_profile_shadow` для
        # сбора bucket-метрик. Promotion в enforce — после P1/Phase C промоут-
        # сервиса (см. services/regime_exec_promotion_v1.py).
        try:
            from services.entry_profile_classifier import classify_entry_profile

            _ep_ctx = {
                "kind": kind,
                "vol_regime": (
                    indicators.get("vol_regime_label")
                    or indicators.get("vol_regime")
                    or "na"
                ),
                "trend_regime": rg_for_overrides or "na",
                "side": signal.get("side") or signal.get("direction") or "LONG",
                "og_score": indicators.get("og_score"),
                "smt_coh": indicators.get("smt_coh"),
                "news_shock": bool(indicators.get("news_shock_active")),
                "adverse_cross": bool(indicators.get("adverse_cross")),
            }
            _ep_res = classify_entry_profile(_ep_ctx)
            indicators["entry_profile_shadow"] = _ep_res.profile
            indicators["entry_profile_confidence"] = round(_ep_res.confidence, 3)
            # Список причин — для отладки в trades_closed.config_snapshot.indicators.
            indicators["entry_profile_reasons"] = ";".join(_ep_res.reasons)[:300]
        except Exception:
            # Shadow-only; никогда не должен ронять пайплайн.
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
            "confidence": confidence, # Use gated confidence (with fallback to indicators + default 0.3) for audit trail integrity
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

        # TP ratio resolution (precedence: regime_exec engine → legacy regime flag fallback).
        # TradeProfile tp_ratios written to indicators as shadow only — tp_levels are
        # already computed without profile geometry; applying ratio here without verifying
        # level consistency would create a payload/execution mismatch.
        _profile_meta = signal.get("meta", {})
        _profile_tp_ratios = _profile_meta.get("tp_ratios")
        if _profile_tp_ratios and isinstance(_profile_tp_ratios, (list, tuple)):
            indicators["profile_tp_ratios_shadow"] = list(_profile_tp_ratios)
        if _regime_exec_tp_ratios:
            payload["tp_ratio"] = list(_regime_exec_tp_ratios)
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
        # ATR-floor enrichment (legacy post-gate position).
        # Skipped when ATR_FLOOR_ENRICHMENT_EARLY=1 — already ran after _calculate_levels.
        # Idempotent helper, safe to call again, but skipped for perf.
        # ------------------------------------------------------------------
        if os.getenv("ATR_FLOOR_ENRICHMENT_EARLY", "0") != "1":
            self._enrich_atr_floor_indicators(
                indicators=indicators, runtime=runtime, cfg=cfg, entry=entry, atr=atr,
            )


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
            with contextlib.suppress(Exception):
                if _G9_CONFIDENCE_MISSING_TOTAL is not None:
                    _G9_CONFIDENCE_MISSING_TOTAL.labels(symbol=symbol).inc()
            logger.debug("G9: confidence absent for %s, fallback=0.3", symbol)
        confidence = max(0.0, min(1.0, float(_conf_raw)))

        # --- Hard Confidence Gate (User request: drop signal if confidence too low) ---
        if not bool(runtime.config.get("disable_confidence_filter", False)):
            # Per-symbol override: MIN_SIGNAL_CONFIDENCE__{SYMBOL} (documented) or MIN_CONF_{SYMBOL} (legacy).
            _sym = symbol.upper().replace("-", "")
            _sym_min_raw = os.getenv(f"MIN_SIGNAL_CONFIDENCE__{_sym}") or os.getenv(f"MIN_CONF_{_sym}")
            if _sym_min_raw is not None:
                try:
                    min_conf_pct = float(_sym_min_raw)
                except (TypeError, ValueError):
                    min_conf_pct = self._cached_min_conf_pct
            else:
                # Autocal override: per-(kind × regime) EV-grid threshold.
                # Fail-open: returns None → fall through to ENV default.
                _autocal_thr: float | None = None
                if self._signal_conf_reader is not None:
                    try:
                        _kind_for_cal = str(indicators.get("kind", "") or "")
                        _regime_for_cal = str(indicators.get("regime", "") or "")
                        _autocal_thr = self._signal_conf_reader.get_threshold(_kind_for_cal, _regime_for_cal)
                    except Exception:
                        pass
                min_conf_pct = _autocal_thr if _autocal_thr is not None else self._cached_min_conf_pct
            if 0 < min_conf_pct <= 1:
                min_conf_pct *= 100.0
            min_conf = min_conf_pct / 100.0

            # gate_value_autocal applied-delta override (Stage 6 ENFORCED).
            # Layered AFTER all other min_conf sources so it has the final
            # word. Fail-open: any error keeps base min_conf untouched.
            # Floor/ceiling from the payload clamp the result to a safe band.
            if self._applied_delta_reader is not None:
                try:
                    _delta_obj = self._applied_delta_reader.get_delta(
                        kind=str(kind or ""),
                        symbol=str(symbol or ""),
                        horizon_ms=int(indicators.get("horizon_ms") or 0),
                    )
                except Exception:
                    _delta_obj = None
                if _delta_obj is not None:
                    _new_conf = min_conf + float(_delta_obj.min_conf_delta)
                    _new_conf = max(
                        float(_delta_obj.min_conf_floor),
                        min(float(_delta_obj.min_conf_ceiling), _new_conf),
                    )
                    if _APPLIED_DELTA_OVERRIDE_TOTAL is not None:
                        with contextlib.suppress(Exception):
                            _dir = (
                                "relax" if _delta_obj.min_conf_delta < 0
                                else "tighten" if _delta_obj.min_conf_delta > 0
                                else "none"
                            )
                            _APPLIED_DELTA_OVERRIDE_TOTAL.labels(
                                kind=str(kind or "unknown"),
                                symbol=str(symbol or "unknown"),
                                phase=_delta_obj.phase or "unknown",
                                delta_direction=_dir,
                            ).inc()
                    if _APPLIED_DELTA_OVERRIDE_VALUE is not None:
                        with contextlib.suppress(Exception):
                            _APPLIED_DELTA_OVERRIDE_VALUE.labels(
                                kind=str(kind or "unknown"),
                                symbol=str(symbol or "unknown"),
                                phase=_delta_obj.phase or "unknown",
                            ).set(float(_delta_obj.min_conf_delta))
                    min_conf = _new_conf

            _conf_dec = self.orchestrator.check_confidence(ctx, confidence=confidence, min_conf=min_conf)
            # Plan 1: calibrated meta-gate (SHADOW by default). Replaces the legacy
            # confidence decision only when mode=CANARY (selected) or ENFORCE; in
            # SHADOW it logs everything and returns _conf_dec untouched.
            _conf_dec = self._confidence_meta_gate_decide(
                legacy_dec=_conf_dec,
                signal=signal,
                indicators=indicators,
                symbol=symbol,
                kind=kind,
                direction=direction,
                sig_ts=sig_ts,
                confidence=confidence,
                min_conf=min_conf,
            )
            if getattr(_conf_dec, "decision", "") == "DENY":
                _entry = float(_levels.get("entry", 0.0) or 0.0)  # type: ignore[arg-type]
                _sl = float(_levels.get("sl", 0.0) or 0.0)  # type: ignore[arg-type]
                _tp_raw = _levels.get("tp_levels", []) or []
                _tp_list: list[float] = [float(x) for x in _tp_raw] if isinstance(_tp_raw, (list, tuple)) else []
                _regime_for_explore = str(indicators.get("regime") or "na").lower().strip() or "na"
                # --- Deep exploration bucket: [DEEP_EXPLORATION_MIN_CONF, VIRTUAL_SIGNAL_MIN_CONF) ---
                # Only for structurally valid signals (entry/sl already validated by guard above).
                # These signals have passed DQ/time/book gates but are below virtual threshold.
                # sample_policy=deep_explore_20_35_sampled, tradeable=False, never to order queue.
                _deep_min_pct = getattr(self, "_cached_deep_explore_min_conf_pct", 20.0)
                _deep_min = _deep_min_pct / 100.0 if _deep_min_pct > 1 else _deep_min_pct
                _virt_min_pct = getattr(self, "_cached_virtual_min_conf_pct", self._cached_min_conf_pct)
                _virt_min = _virt_min_pct / 100.0 if _virt_min_pct > 1 else _virt_min_pct
                
                if _deep_min <= confidence < _virt_min:
                    self._maybe_record_deep_explore(
                        signal=signal, indicators=indicators, confirmations=confirmations,
                        symbol=symbol, direction=direction, ts_ms=sig_ts,
                        confidence=confidence,
                        entry=_entry, sl=_sl, tp_levels=_tp_list,
                        regime=_regime_for_explore,
                    )
                else:
                    self._record_gated_out_shadow(
                        signal=signal, indicators=indicators, confirmations=confirmations,
                        symbol=symbol, direction=direction, ts_ms=sig_ts,
                        confidence=confidence, min_conf=min_conf,
                        entry=_entry, sl=_sl, tp_levels=_tp_list,
                        regime=_regime_for_explore,
                    )
                if _PRE_PUBLISH_VETO_TOTAL is not None:
                    with contextlib.suppress(Exception):
                        _PRE_PUBLISH_VETO_TOTAL.labels(
                            gate=_conf_dec.gate,
                            reason_code=getattr(_conf_dec, "reason_code", "LOW_CONFIDENCE"),
                            symbol=symbol,
                            kind=kind
                        ).inc()
            if _apply_decision(_conf_dec): return  # type: ignore
        # ---

        # ------------------------------------------------------------------
        # Stage: build enriched_signal (raw stream payload)
        # ------------------------------------------------------------------
        enriched_signal = dict(signal)
        # trace_id propagation: ensure raw-stream payload carries trace_id so
        # downstream consumers (trade-monitor runner, ML dataset, of:inputs)
        # populate `trace=` in logs and join keys. Fallback to sid for parity
        # with envelope_builder (env.trace_id or sid).
        enriched_signal.setdefault(
            "trace_id",
            str(
                signal.get("trace_id")
                or signal.get("sid")
                or signal.get("signal_id")
                or ""
            ),
        )
        enriched_signal.update(
            {
                "strategy": enriched_signal.get("strategy", "cryptoorderflow"),
                "source": "CryptoOrderFlow",
                "source_service": self._cached_service_name,
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
                # Maker-only execution flag (item 4, 2026-05-24): propagated from
                # EntryPolicyGate (ctx.exec_maker_only_enforce). Downstream
                # OrderPayloadBuilder → OrderOpenService reads this to switch
                # entry order to LIMIT+timeInForce=GTX (Post-Only / Binance maker).
                # 0 = MARKET path; 1 = enforce maker-only (LIMIT GTX).
                "exec_maker_only": int(bool(getattr(ctx, "exec_maker_only_enforce", 0))),
                "exec_maker_only_shadow": int(bool(getattr(ctx, "exec_maker_only_required", 0))),
            }
        )

        # Phase 4: ATR Selector Profile for TradeMonitor Trailing
        if atr_meta:
            enriched_signal.setdefault("meta", {})
            # Only overwrite if the profile was not already set by a richer source
            # (e.g. select_runtime_atr_profile called via attach_phase0_contract in
            # strategy.py before publish_signal).  A pre-built profile has "mode" key;
            # the simple dict below does not, so we skip overwrite when "mode" present.
            _existing_profile = enriched_signal["meta"].get("atr_profile", {})
            if not isinstance(_existing_profile, dict) or "mode" not in _existing_profile:
                _entry_px = float(indicators.get("price") or enriched_signal.get("entry") or 0.0)
                _atr_val_p = float(atr_meta.get("atr_value") or atr_meta.get("atr") or atr or 0.0)
                enriched_signal["meta"]["atr_profile"] = {
                    "atr_value": _atr_val_p,
                    "atr_tf_ms": int(atr_meta.get("atr_tf_ms") or 0),
                    "atr_tf": str(atr_meta.get("atr_tf") or atr_meta.get("picked_tf") or indicators.get("atr_tf_used") or ""),
                    "ts_ms": int(atr_meta.get("ts_ms") or 0),
                    "src": str(atr_meta.get("src") or atr_meta.get("source") or ""),
                    "atr_age_ms": int(atr_meta.get("atr_age_ms") or atr_meta.get("age_ms") or 0),
                    "atr_pct": (_atr_val_p / _entry_px) if _entry_px > 0 and _atr_val_p > 0 else 0.0,
                }
            # Phase 6: Expose atr_tf_ms and atr_stop_pct in indicators for ML v5 feature vector.
            # setdefault — does not overwrite if already present (e.g. set by tick_processor).
            try:
                _new_atr_tf = int(atr_meta.get("atr_tf_ms") or 0)
                if _new_atr_tf > 0:
                    indicators["atr_tf_ms"] = _new_atr_tf
                else:
                    indicators.setdefault("atr_tf_ms", 0)

                _atr_val = float(atr_meta.get("atr_value") or atr or 0.0)
                _entry_px = float(indicators.get("price") or enriched_signal.get("entry") or 0.0)
                if _entry_px > 0.0 and _atr_val > 0.0:
                    indicators["atr_stop_pct"] = _atr_val / _entry_px * 100.0
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
                    # Overwrite if engine defaulted to 0.0, but keep if valid
                    if float(indicators.get(_hz_key) or 0.0) == 0.0:
                        indicators[_hz_key] = _hz_val
                    else:
                        indicators.setdefault(_hz_key, _hz_val)

            # Compute normalizations explicitly to override of_confirm_engine's 0.0 defaults
            _HZ_ONE_HOUR_MS = 3600000.0
            _ahl = indicators.get("alpha_half_life_ms")
            if _ahl is not None and float(_ahl) > 0:
                indicators["alpha_half_life_ms_norm"] = float(_ahl) / _HZ_ONE_HOUR_MS

            _htm = indicators.get("hold_target_ms")
            if _htm is not None and float(_htm) > 0:
                indicators["hold_target_ms_norm"] = float(_htm) / _HZ_ONE_HOUR_MS

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

        # P2.6 — Quarter-Kelly sizing (shadow default, enforce via KELLY_SIZING_ENABLED=1)
        try:
            from core.kelly_sizer_v2 import apply_kelly_sizing as _kelly_apply
            _kelly_enforce = os.getenv("KELLY_SIZING_ENABLED", "0") == "1"
            effective_risk_pct = _kelly_apply(
                indicators, effective_risk_pct,
                enforce=_kelly_enforce, symbol=symbol, kind=kind,
            )
        except Exception:
            pass

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
        enriched_signal["contract_size"] = float(getattr(get_specs(symbol), "contract_size", 1.0))
        enriched_signal["tick_size"] = float(getattr(get_specs(symbol), "tick_size", 0.01))
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

        # CRYPTO_OF_VIRTUAL_ENFORCE=true: block virtual trades when OF confirm fails.
        # Default false = legacy shadow behaviour (virtual trades always "passed").
        _virtual_enforce = os.getenv("CRYPTO_OF_VIRTUAL_ENFORCE", "0").lower() in ("1", "true", "yes", "on")

        if of_confirm_ok == 1:
            validation_status = "passed"
            validation_reason = f"OFConfirm passed ({of_confirm_reason})"
        elif of_confirm_ok == 0:
            is_virtual = bool(int(enriched_signal.get("is_virtual", 0) or signal.get("is_virtual", 0)))
            if is_virtual and not _virtual_enforce:
                # Legacy shadow: virtual trades pass regardless of OF confirm result
                validation_status = "passed"
                validation_reason = f"OFConfirm shadowed (virtual trade): {indicators.get('of_confirm', {}).get('reason', of_confirm_reason)}"
            else:
                # Enforce: block even virtual trades when OF confirm rejects
                validation_status = "failed"
                validation_reason = f"OFConfirm failed: {indicators.get('of_confirm', {}).get('reason', of_confirm_reason)}"
        else:
            # of_confirm_ok not set or OFConfirm was not evaluated
            is_virtual = bool(int(enriched_signal.get("is_virtual", 0) or signal.get("is_virtual", 0)))
            if not is_virtual:
                # Real signals MUST fail if confirmation is missing.
                validation_status = "failed"
                validation_reason = "OFConfirm not evaluated (real signal blocked)"
            elif is_virtual and not _virtual_enforce:
                validation_status = "bypassed"
                validation_reason = "OFConfirm not evaluated (virtual)"
            else:
                validation_status = "failed"
                validation_reason = "OFConfirm not evaluated (virtual enforce=True)"

        enriched_signal["validation_status"] = validation_status
        enriched_signal["validation_reason"] = validation_reason
        enriched_signal["v_gate_reason"] = validation_reason

        # --- SHADOW MODE: mark main signal as virtual if validation failed or shadowed
        gate_mode = (indicators.get("of_gate_mode") or "").upper()
        # Defensive: if validation failed, it's virtual regardless of gate mode string
        if validation_status == "failed" or indicators.get("gate_shadow_veto") or (gate_mode == "SHADOW"):
            enriched_signal["is_virtual"] = 1
            if indicators.get("gate_shadow_veto"):
                enriched_signal["validation_status"] = "failed"
                enriched_signal["validation_reason"] = indicators.get("gate_reason", "SHADOW_VETO")
                enriched_signal["v_gate_reason"] = indicators.get("gate_reason", "SHADOW_VETO")
        
        # Recommendation 3: Explicitly segregate execution modes
        # shadow: indicative only (SHADOW mode)
        # virtual: no real capital (either rejected or shadow)
        # tradeable: real capital intended (ENFORCE + passed)
        is_virtual_val = bool(int(enriched_signal.get("is_virtual", 0) or 0))
        enriched_signal["shadow"] = (gate_mode == "SHADOW")
        enriched_signal["virtual"] = is_virtual_val or enriched_signal["shadow"]
        enriched_signal["tradeable"] = not enriched_signal["virtual"]
        # ---

        # 2026-05-27 WR stop-bleed: hard-drop virtual signals with failed/bypassed
        # validation BEFORE outbox envelope build. Trade report showed 140/147
        # bypassed virtuals turning into open trades with WR=1.6%.
        _hard_drop_enabled = os.getenv("VIRTUAL_GATE_HARD_DROP_ENABLED", "0").lower() in ("1", "true", "yes", "on")
        if _hard_drop_enabled:
            _should_drop, _drop_reason = should_drop_virtual_signal(enriched_signal)
            if _should_drop:
                _drop_shadow = os.getenv("VIRTUAL_GATE_HARD_DROP_SHADOW", "1").lower() in ("1", "true", "yes", "on")
                _mode = "shadow" if _drop_shadow else "enforce"
                if _VIRTUAL_HARD_DROP_TOTAL is not None:
                    with contextlib.suppress(Exception):
                        _VIRTUAL_HARD_DROP_TOTAL.labels(
                            symbol=symbol, reason=_drop_reason, mode=_mode,
                        ).inc()
                if not _drop_shadow:
                    logger.info(
                        "🚫 [VIRTUAL_HARD_DROP] (%s) drop virtual signal reason=%s vstatus=%s",
                        symbol, _drop_reason, enriched_signal.get("validation_status"),
                    )
                    return
                # shadow mode: log infrequently
                logger.debug(
                    "[VIRTUAL_HARD_DROP_SHADOW] (%s) would-drop reason=%s",
                    symbol, _drop_reason,
                )

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

        # Block virtual/shadow trades from Telegram by default. Predicate is
        # extracted to core.notify_filters.should_skip_telegram_virtual so the
        # gate is unit-testable without SignalPipeline setup. Reads BOTH
        # `virtual` (bool, set at line ~2459) AND `is_virtual` (int, wire-level)
        # so external producers cannot bypass it. CRYPTO_NOTIFY_SKIP_VIRTUAL=0
        # restores legacy shadow-to-Telegram behaviour.
        from core.notify_filters import should_skip_telegram_virtual as _should_skip_virt_tg
        _skip_virtual_now = _should_skip_virt_tg(enriched_signal)

        if is_weak:
            telegram_payload = None
            logger.info("🚫 [TELEGRAM] (%s) Signal is WEAK (strong_ok=%s, conf=%.2f). Skipping notify.", symbol, strong_gate_ok, confidence)
        elif _skip_virtual_now:
            telegram_payload = None
            logger.info(
                "🚫 [TELEGRAM] (%s) Signal is VIRTUAL/SHADOW (%s). Skipping Telegram notify.",
                symbol,
                enriched_signal.get("validation_reason", "virtual"),
            )
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

        # Pipeline calibrator observation on successful emit (fail-open).
        # Feeds autocal:cooldown / vol_z_thr / htf_proximity / liq_wall.
        try:
            self._pipeline_calib_observe_on_emit(
                symbol=symbol,
                signal=enriched_signal,
                indicators=indicators,
                now_ms=ts_ms,
            )
        except Exception:
            pass

        # Confirmation barrier calibrator observation (shadow mode, fail-open).
        # Feeds autocal:confirm_barrier:state for adaptive OBI thresholds.
        try:
            self._confirm_barrier_observe(
                symbol=symbol,
                signal=enriched_signal,
                runtime=runtime,
                now_ms=ts_ms,
                indicators=indicators,
            )
        except Exception:
            pass

        # A2 Decision Snapshot publication (enriched output, fail-open)
        # This MUST contain joinable keys, is_virtual flag, and be stable across retries.
        if self.decision_snapshot_publish_enabled:
            try:
                snap = build_decision_snapshot(
                    enriched_signal,  # pass enriched to capture validation fields
                    runtime=runtime,
                    indicators=indicators,
                    schema_version=self.decision_snapshot_schema_version,
                    include_indicators=True,
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
                "v_gate_reason": enriched_signal.get("v_gate_reason") or "",
                "validation_reason": enriched_signal.get("validation_reason") or "",
                "validation_status": enriched_signal.get("validation_status") or "",
                # Forward meta sub-keys needed by _stamp_closed_trade_meta.
                # meta is set by preprocess_signal_for_publish on enriched_signal but
                # was not propagated to the signal stream payload, causing live_surface
                # and trailing A/B fields to always be 0/False in trades_closed.
                # We forward only the analytics-critical sub-keys (not atr_policy_resolution
                # which is large) to keep payload compact.
                "meta": {
                    k: v
                    for k, v in (enriched_signal.get("meta") or {}).items()
                    if k in (
                        "live_surface_baseline", "live_surface_applied",
                        "risk_surface_live_candidate", "live_surface_canary",
                        "trailing_canary_decision", "trailing_surface_diagnostic",
                        "policy_provenance",
                    )
                },
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
                    runtime=runtime,
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
                # every_n<=1 means "send every signal" (no rate limiting); guards modulo-by-zero
                 if notify_signal_every_n <= 1 or counter_value % notify_signal_every_n == 0:
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
                runtime=runtime,
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
        rejection_gate: str = "",
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
            ORDERS_QUEUE_BINANCE (default: orders:queue:binance:intent).
            Confidence threshold is VIRTUAL_SIGNAL_MIN_CONF (defaults to
            CRYPTO_SIGNAL_MIN_CONF when unset), keeping the live execution
            gate independent.

        BinanceExecutor reads is_virtual=1 and routes to demo/testnet client.
        """
        mirror_all = self.binance_virtual_mirror_all
        shadow_only = self.binance_virtual_orders_enabled

        if not mirror_all and not shadow_only:
            return  # both modes disabled — nothing to do

        if not self.publisher or not self.publisher.r:
            return

        # Guard 1: DQ/time/integrity/book-sanity vetoes must never reach the virtual queue.
        # The signal is structurally invalid (bad timestamp, stale book, crossed BBO,
        # stream gap) — not merely low-confidence. Two complementary checks:
        #   a) gate name (rejection_gate) — fast path when dec.gate is forwarded
        #   b) reason code (rejection_reason) — covers all BookSanityGate + DQ codes
        # The audit stream still records them via _handle_pipeline_veto step 2.
        _skip_by_gate = bool(rejection_gate and rejection_gate in _VIRTUAL_ORDER_SKIP_GATES)
        _skip_by_reason = bool(rejection_reason and rejection_reason in _VIRTUAL_ORDER_SKIP_REASONS)
        if _skip_by_gate or _skip_by_reason:
            _skip_label = rejection_reason or f"gate:{rejection_gate}"
            with contextlib.suppress(Exception):
                if _VIRTUAL_ORDER_SKIPPED_BAD_DQ_TOTAL is not None:
                    _VIRTUAL_ORDER_SKIPPED_BAD_DQ_TOTAL.labels(reason=_skip_label).inc()
            return

        # Guard 2: levels must be valid before any virtual order routing.
        # _handle_pipeline_veto is called at early gate stages where entry/sl/lot
        # may still be zero (pre-level-calculation veto). Routing these would
        # produce zero-price virtual orders and corrupt ML outcome labels.
        _entry_ok = entry > 0
        _sl_ok = sl > 0
        _lot_ok = lot > 0
        if not (_entry_ok and _sl_ok and _lot_ok):
            _lvl_reason = (
                "zero_entry" if not _entry_ok
                else "zero_sl" if not _sl_ok
                else "zero_lot"
            )
            with contextlib.suppress(Exception):
                if _VIRTUAL_ORDER_INVALID_LEVELS_TOTAL is not None:
                    _VIRTUAL_ORDER_INVALID_LEVELS_TOTAL.labels(reason=_lvl_reason).inc()
            return

        validation_status = (enriched_signal.get("validation_status") or "").lower()
        is_virtual_flag = bool(int(enriched_signal.get("is_virtual", 0) or indicators.get("is_virtual", 0) or 0))

        # Virtual/shadow/rejected paths use a separate lower threshold so the
        # live execution gate (CRYPTO_SIGNAL_MIN_CONF) is never relaxed.
        # VIRTUAL_SIGNAL_MIN_CONF defaults to CRYPTO_SIGNAL_MIN_CONF when unset.
        _use_virtual_threshold = mirror_all or shadow_only or is_virtual_flag or is_rejected_signal
        _raw_conf_pct = (
            self._cached_virtual_min_conf_pct if _use_virtual_threshold else self._cached_min_conf_pct
        )
        min_conf_pct = _raw_conf_pct
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

        # Telemetry: count candidates that cleared guards and confidence filter.
        # min_conf is already computed above in the correct unit (0-1 fraction).
        with contextlib.suppress(Exception):
            if _VIRTUAL_THRESHOLD_CANDIDATE_TOTAL is not None:
                _VIRTUAL_THRESHOLD_CANDIDATE_TOTAL.labels(
                    symbol=symbol,
                    meets_virtual_threshold="1" if confidence >= min_conf else "0",
                ).inc()

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



    async def send_telegram_report(self, text: str, source: str = "report", symbol: str = "", runtime: Any = None) -> None:
        """Send arbitrary report text to Telegram via notify stream (type=report)."""
        try:
            ts_ms = str(get_ny_time_millis())
            resolved_symbol = symbol or (getattr(runtime, "symbol", "") if runtime else "") or ""
            fields = {
                "type": "report",
                "text": (text or ""),
                "source": (source or "report"),
                "symbol": resolved_symbol,
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
        _p_tp_ratio = indicators.get("profile_tp_ratio")  # enforced by TRADE_PROFILE_TP_ENFORCE=1
        if _p_stop is not None or _p_tp_rr is not None or _p_tp1 is not None or _p_tp_ratio is not None:
            cfg = {**cfg}  # shallow copy to avoid mutating runtime.config
            if _p_stop is not None:
                cfg["stop_atr_mult"] = float(_p_stop)
            if _p_tp_rr is not None:
                cfg["tp_rr"] = str(_p_tp_rr)
            if _p_tp1 is not None:
                cfg["tp1_atr_mult"] = float(_p_tp1)
            if _p_tp_ratio is not None:
                cfg["tp_ratio"] = str(_p_tp_ratio)
        _raw_signal_atr = float(indicators.get("atr", 0.0) or 0.0)
        indicators["atr_signal_raw"] = _raw_signal_atr
        atr = 0.0  # Force load from canonical HTF cache
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
            if _raw_signal_atr > 0:
                atr = _raw_signal_atr
                indicators["atr_src"] = "fallback-signal-raw"
                indicators["atr_sanity_reason"] = "cache_miss_used_raw"
                indicators["atr_sanity_ok"] = 1
            else:
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

        # Explicitly fetch 5m ATR for ML features / signals table.
        # Done separately from canonical ATR so both are always available regardless of atr_tf.
        if not indicators.get("atr_5m"):
            try:
                if self.atr_cache:
                    _nm5: int | None = int(
                        indicators.get("ts_ms", 0) or indicators.get("tick_ts", 0) or 0
                    ) or None
                    _atr_5m, _ = self.atr_cache.get_with_meta(
                        symbol=runtime.symbol, timeframe="5m", now_ms=_nm5
                    )
                    indicators["atr_5m"] = float(_atr_5m or 0.0)
            except Exception:
                pass

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
        if self._sl_atr_floor_reader is not None:
            try:
                _venue = str(getattr(runtime, "config", {}).get("venue") or "binance")
                _cal_floor = self._sl_atr_floor_reader.get_floor(runtime.symbol, _venue)
                if _cal_floor is not None:
                    _sl_atr_floor = _cal_floor
            except Exception:
                pass
        if atr > 0 and stop_dist > 0:
            _actual_sl_mult = stop_dist / atr
            if _actual_sl_mult < _sl_atr_floor:
                indicators["sl_atr_mult_floored"] = 1
                indicators["sl_atr_mult_original"] = round(_actual_sl_mult, 4)
                stop_dist = atr * _sl_atr_floor
        elif atr <= 0:
            # Fix (2026-05-29): atr<=0 means SL floor can't be evaluated. Previously
            # this silently fell through, allowing arbitrarily small SL through. Now:
            #   - always emit indicator for telemetry
            #   - when SL_ATR_FLOOR_VETO_ON_ZERO_ATR=1, collapse stop_dist to 0 so the
            #     downstream profitability gate (calculate_position_size →
            #     lot_risk<=0 path at signal_pipeline.py:~4281) vetoes the signal
            #     (virtual trade pushed for ML tracking — no real fill).
            indicators["sl_floor_atr_invalid"] = 1
            indicators["sl_floor_atr_invalid_reason"] = "atr_zero_or_negative"
            indicators["sl_floor_atr_invalid_atr_raw"] = atr
            if (os.getenv("SL_ATR_FLOOR_VETO_ON_ZERO_ATR", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}:
                indicators["sl_floor_atr_invalid_enforced"] = 1
                stop_dist = 0.0

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

        # Apply bounded SL floor (pit_priors MAE-percentile) — same logic as signals/risk_levels.py.
        # Prevents ATR-stale / micro-timeframe SLs from falling below the fee+noise floor.
        # BOUNDED_SL_ENABLED=1, SHADOW=0 → enforce; fail-open on any error.
        try:
            from signals.bounded_sl import apply_bounded_sl_floor
            # 2026-05-28 fix: use canonical HTF ATR (same one used for stop_dist) for cap ratio.
            # indicators["atr"] is the raw signal ATR (1m/tick), not the level ATR — produces wrong ratio.
            _atr_for_cap = atr if atr > 0 else None
            _bsl_new, _bsl_meta = apply_bounded_sl_floor(runtime.symbol, entry, stop_dist, cfg, atr=_atr_for_cap)
            if _bsl_meta.get("applied"):
                stop_dist = _bsl_new
                indicators["bounded_sl_applied_pipeline"] = 1
                indicators["bounded_sl_floor_bps"] = round(float(_bsl_meta.get("mae_floor_bps", 0.0)), 2)
            if _bsl_meta.get("atr_cap_triggered"):
                indicators["bounded_sl_atr_cap_triggered"] = 1
                indicators["bounded_sl_atr_cap_skipped"] = int(_bsl_meta.get("atr_cap_skipped", 0))
                indicators["bounded_sl_mae_to_atr_mult"] = round(float(_bsl_meta.get("mae_floor_to_atr_mult", 0.0)), 2)
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

        # ── Shadow telemetry: rocket_tp1_actual_atr_mult (2026-05-14) ──────────
        # Records the ACTUAL TP1 multiplier (in ATR units) that this _calculate_levels
        # call will produce, plus which method was used (ROCKET vs RR). This lets
        # us verify whether ROCKET_TP1_ATR_MULT env actually reaches signal generation.
        # Reports/dashboards can group by this to see if rocket-method ever applies.
        indicators["rocket_tp1_env_mult_raw"] = round(rocket_mult_raw, 4)
        indicators["rocket_tp1_env_mult_floored"] = round(rocket_mult, 4)
        indicators["trail_profile_used"] = str(trail_profile or "unknown")
        if is_rocket_v1 and atr > 0:
            _actual_tp1_atr = rocket_mult  # is_rocket_v1 path: TP1 = atr * rocket_mult
            indicators["tp1_calc_method"] = "ROCKET"
        elif atr > 0 and stop_dist > 0:
            _rr0 = rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))[0]
            _actual_tp1_atr = (stop_dist * _rr0) / atr
            indicators["tp1_calc_method"] = "RR"
            indicators["rr_tp1_used"] = round(_rr0, 4)
        else:
            _actual_tp1_atr = 0.0
            indicators["tp1_calc_method"] = "na"
        indicators["rocket_tp1_actual_atr_mult"] = round(_actual_tp1_atr, 4)
        # ── end shadow telemetry ───────────────────────────────────────────────

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

        # ── AdaptiveTP1Policy v1 (Plan 3, 2026-05-29) ─────────────────────────
        # Evaluates argmax-EV(TP1_R) from the grid and emits shadow stream entry.
        # SHADOW by default: indicators always written, TP1 override only when
        # mode in {paper,enforce} AND _atp1.apply=True. Fail-open: never breaks levels.
        try:
            import types as _types
            _side_s = side.upper() if isinstance(side, str) else ("LONG" if side > 0 else "SHORT")
            _regime_s = str(
                indicators.get("regime") or indicators.get("entry_regime")
                or (runtime.last_regime if hasattr(runtime, "last_regime") else "")
                or ""
            )
            _kind_s = str(indicators.get("kind") or "of")
            _baseline_tp1_dist = abs(float(tps[0]) - entry) if tps else 0.0

            # Build a minimal ctx proxy for the CDF reader
            _atp1_ctx = _types.SimpleNamespace(
                tp1_hit_prob_by_rr=None,
                tp1_prob_samples=0,
                tp1_calibration_ok=None,
            )
            try:
                from services.tp1_hit_prob_reader import attach_tp1_phit_to_ctx as _attach_phit
                _attach_phit(
                    _atp1_ctx,
                    symbol=runtime.symbol,
                    kind=_kind_s,
                    regime=_regime_s,
                    direction=_side_s,
                )
            except Exception:
                pass

            from core.adaptive_tp1_policy import choose_adaptive_tp1
            _atp1 = choose_adaptive_tp1(
                ctx=_atp1_ctx,
                entry=entry,
                stop_dist=stop_dist,
                baseline_tp1_dist=_baseline_tp1_dist,
                symbol=runtime.symbol,
                kind=_kind_s,
                regime=_regime_s,
            )

            # Telemetry into indicators (always, shadow-safe)
            indicators["tp1_adaptive_reason"] = _atp1.reason
            indicators["tp1_adaptive_mode"] = _atp1.mode
            indicators["tp1_adaptive_ev_delta_r"] = float(_atp1.ev_delta_r)
            indicators["tp1_adaptive_ev_baseline_r"] = float(_atp1.ev_baseline_r)
            indicators["tp1_adaptive_ev_adaptive_r"] = float(_atp1.ev_adaptive_r)
            indicators["tp1_adaptive_samples"] = int(_atp1.samples)
            if _atp1.tp1_rr is not None:
                indicators["tp1_adaptive_rr_selected"] = float(_atp1.tp1_rr)
            if _atp1.p_hit is not None:
                indicators["tp1_adaptive_p_hit"] = float(_atp1.p_hit)
            if _atp1.p_hit_baseline is not None:
                indicators["tp1_adaptive_p_hit_baseline"] = float(_atp1.p_hit_baseline)

            # Prometheus + XADD shadow stream (fail-open)
            try:
                from core.tp1_adaptive_metrics import emit_decision as _emit_atp1
                _dir = 1 if _side_s == "LONG" else -1
                _atp1_tp1_price = (
                    entry + _dir * float(_atp1.tp1_dist)
                    if _atp1.tp1_dist is not None else None
                )
                _emit_atp1(
                    decision=_atp1,
                    symbol=runtime.symbol,
                    kind=_kind_s,
                    side=_side_s,
                    regime=_regime_s,
                    entry_price=entry,
                    sl_price=sl,
                    baseline_tp1_price=float(tps[0]) if tps else entry,
                    baseline_tp1_rr=_atp1.baseline_rr,
                    adaptive_tp1_price=_atp1_tp1_price,
                    spread_bps=float(indicators.get("spread_bps", 0.0) or 0.0),
                    slippage_bps=float(indicators.get("slippage_ema_bps", 0.0) or 0.0),
                    fee_bps=float(os.getenv("TAKER_FEE_BPS", "4.0") or 4.0),
                    ts_ms=(int(indicators.get("ts_ms") or indicators.get("tick_ts") or 0) or None),
                    sid=str(indicators.get("signal_id") or indicators.get("sid") or ""),
                )
            except Exception:
                pass

            # Apply override only in paper/enforce mode
            if _atp1.apply and _atp1.tp1_dist is not None and _atp1.tp1_dist > 0.0 and tps:
                _dir = 1 if _side_s == "LONG" else -1
                tps[0] = entry + _dir * float(_atp1.tp1_dist)
                tps.sort(key=lambda x: abs(x - entry))
                indicators["tp1_adaptive_applied"] = 1
                if atr > 0:
                    indicators["tp1_atr"] = abs(float(tps[0]) - entry) / atr
                    indicators["tp1_atr_mult"] = abs(float(tps[0]) - entry) / atr
        except Exception:
            pass
        # ── end AdaptiveTP1Policy ─────────────────────────────────────────────

        return sl, tps, lot, atr, atr_meta
