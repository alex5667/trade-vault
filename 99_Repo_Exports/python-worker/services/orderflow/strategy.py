"""
Универсальный сервис ордерфлоу для крипто‑фьючерсов Binance USDT-M.

Задачи:
- Читает тики и книги заявок из Redis Streams (`stream:tick_<symbol>` / `stream:book_<symbol>`).
- Поддерживает динамический список символов (set `crypto:symbols`) + базовые `BTCUSDT`, `ETHUSDT`.
- Берёт настройки из `config:orderflow:<symbol>` (Hash) и пресетов `OrderFlowConfig`.
- Использует готовые детекторы из `core.crypto_orderflow_detectors`.
- Публикует сигналы в `notify:telegram`, `signals:crypto:raw` и (опционально) `orders:queue`.

Сервис асинхронный, построен на redis.asyncio.
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
from common.time_utils import normalize_epoch_ms as normalize_epoch_ms_v2
from common.of_gate_metrics_contract import enrich_schema_fields, validate_of_gate_row, why_label
from services.orderflow.dq_quarantine import emit_quarantine_row
import os
import time
import asyncio
from utils.task_manager import safe_create_task
from common.normalization import generate_signal_id, normalize_side_3, normalize_side_3_safe, Direction

import logging
from typing import Any, Dict, List, Optional, Tuple
import math

from handlers.crypto_orderflow.utils.log_sampler import sampled_info, sampled_warning, sampled_debug, LogSamplerFactory


from services.tp_config import parse_tp_ratio
from services.orderflow.configuration import (
    _safe_int, _safe_float, _to_bool, 
    _ensure_list_levels
)
from services.orderflow.metrics_batcher import MetricsBatcher, PrometheusBatcher
from core.burst_gate import BurstCandidate

from core.atr_sanity import ATRSanity

# Inline regime computation (fallback for symbols not covered by handler pipeline)
try:
    from handlers.regime_service import MarketRegimeService, RegimeConfig, RegimeFeatures
except ImportError:
    try:
        from regime_service import MarketRegimeService, RegimeConfig, RegimeFeatures
    except ImportError:
        MarketRegimeService = RegimeConfig = RegimeFeatures = None  # type: ignore



from services.orderflow.metrics import (
    log_silent_error, ok_metrics_emitted_total, ok_metrics_skipped_total, ok_metrics_error_total
    fp_buckets_evicted_total
    tick_ts_backwards_total, tick_ts_clamped_total, tick_ts_quarantined_total
    burst_active_gauge, burst_window_ms_gauge, tick_gap_p50_ms_gauge
    ticks_out_of_order_total, ticks_side_unknown_total, bars_closed_total, divergence_detected_total
    sweep_detected_total, strong_gate_veto_total, ticks_pressure_filtered_total
    atr_tf_switch_total, atr_tf_candidate_diff, atr_tf_target_bps, atr_tf_candidate_score
    book_stale_ms_gauge, ptier_tier0_usd, ptier_tier1_usd, ptier_tier2_usd, dn_gate_events_total, of_session_outcome_total, veto_low_conf_total, cvd_reclaim_eval_total, cvd_reclaim_ok_total, cvd_reclaim_applied_total, cvd_reclaim_age_ms_gauge, conf_feature_seen_total, conf_feature_true_total, g10_adverse_veto_total
    # Latency audit sub-stage histograms
    process_tick_validate_time_us, process_tick_cvd_update_us
    process_tick_liqmap_us, process_tick_gates_us
    signal_emit_latency_us, worker_lag_ms_hist
    # P0/P1 audit: book observability
    book_health_state_gauge, book_ts_gap_ms_hist
)
from handlers.crypto_orderflow.utils.smt_coherence_gate import SmtLeaderCoherenceGate
from services.orderflow.utils import (
    _calc_pressure_sps, _cooldown_ms_for, _should_sample
    session_utc, hour_of_week_utc
)
from services.orderflow.runtime import SymbolRuntime
from services.orderflow.signal_pipeline import SignalPipeline
from services.signal_preprocess import preprocess_signal_for_publish
from services.orderflow.market_state import MarketStateService
from services.orderflow.liqmap_features import try_parse_liqmap_snapshot_json, compute_liqmap_features_from_snapshot

# Phase C/P2: liquidity geometry + resiliency (hot-path)
from services.orderflow.book_geometry import (
    extract_levels_from_runtime
    calc_book_slope
    calc_depth_weighted_spread
    calc_cost_to_cross
)






from core.smt_symbol_snapshot import SymbolSnapshot
from core.atr_floor_policy import compute_atr_bps_threshold

from core.weak_progress import compute_weak_progress


from core.footprint_policy import fp_confirmations_from_microbar
from core.strong_of_gate import hidden_trend_dir
from core.of_confirm_engine import OFConfirmEngine
from core.of_inputs_contract import OFInputsV1, OFInputsV2



from core.time_utils import normalize_epoch_ms
from services.observability.latency_contract import (
    LatencyStateWriter
    stamp_feature_ready
    observe_feature_ready_async
    SERVICE_PYTHON_WORKER
)

# Consolidated core imports
from core.cvd_reclaim import compute_cvd_reclaim




from services.async_signal_publisher import AsyncSignalPublisher


import redis.asyncio as aioredis
from redis.exceptions import RedisError

from common.time_norm import normalize_epoch_ms
from core.instrument_config import get_default_delta_tiers

from services.signal_confidence import ConfidenceScorer, ConfidenceConfig
from core.microbar import MicroBar
from core.data_health import compute_data_health, apply_book_evidence_policy, apply_shadow_only_policy
from core.slippage_model import expected_slippage_bps
from services.orderflow.components.book_processor import BookProcessor
from core.indicator_keys import IndicatorKeys as IK
from core.dyn_cfg_keys import DynCfgKeys as DK
from core.redis_keys import RedisStreams as RS

# Phase B1: BBO time-series publisher (hot-path safe)
from services.orderflow.bbo_store import BBOStoreCfg, maybe_publish_bbo


# ──────────────────────────────────────────────────────────────────────────────
# Настройки по умолчанию
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("crypto_orderflow_service")
# Настройка логирования
log_level = os.getenv("CRYPTO_OF_LOG_LEVEL", "INFO")
logging.basicConfig(
    level=log_level
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
# Доп. флаг: подробный DEBUG по дельте (по умолчанию выключен, чтобы не шуметь)
# Доп. флаг: подробный DEBUG по дельте (по умолчанию выключен, чтобы не шуметь)
DEBUG_DELTAS = os.getenv("CRYPTO_OF_DEBUG_DELTAS", "false").strip().lower() in ("1", "true", "yes", "on")

# SRE metrics for gate decisions (world-class: drift + latency + exec risk)
OF_GATE_METRICS_STREAM = os.getenv("OF_GATE_METRICS_STREAM", RS.OF_GATE_METRICS)
OF_GATE_METRICS_ENABLE = os.getenv("OF_GATE_METRICS_ENABLE", "1").strip() in ("1","true","yes","on")
OF_GATE_METRICS_SAMPLE = float(os.getenv("OF_GATE_METRICS_SAMPLE", "0.10") or 0.10)  # 10% кандидатов
OF_GATE_METRICS_MAXLEN = int(os.getenv("OF_GATE_METRICS_MAXLEN", "200000") or 200000)
OF_GATE_METRICS_QUARANTINE_STREAM = os.getenv("OF_GATE_METRICS_QUARANTINE_STREAM", RS.OF_GATE_METRICS_QUARANTINE)
OF_GATE_METRICS_QUARANTINE_ENABLE = os.getenv("OF_GATE_METRICS_QUARANTINE_ENABLE", "1").strip().lower() in ("1", "true", "yes", "on")
OF_GATE_METRICS_QUARANTINE_MAXLEN = int(os.getenv("OF_GATE_METRICS_QUARANTINE_MAXLEN", "200000") or 200000)
OF_GATE_METRICS_SAMPLE_SALT = os.getenv("OF_GATE_METRICS_SAMPLE_SALT", "").strip()
OF_GATE_METRICS_SAMPLE_KEY_MODE = "symbol_ts_v1"

import hashlib

def _sample_uid_symbol_ts(symbol: str, ts_ms: int) -> int:
    """
    Sampling-invariant key: make sampling stable AND de-correlated across symbols.
    Important: key must NOT depend on ok/ok_soft to avoid bias.
    """
    b = f"{OF_GATE_METRICS_SAMPLE_SALT}|{symbol}|{int(ts_ms)}".encode("utf-8", errors="replace")
    h = hashlib.sha1(b).digest()
    return int.from_bytes(h[:8], byteorder="big", signed=False)

# Fail-open defaults to avoid exec-risk penalty becoming 0 silently
SPREAD_BPS_MISSING_DEFAULT = float(os.getenv("SPREAD_BPS_MISSING_DEFAULT", "15.0") or 15.0)
SLIPPAGE_BPS_MISSING_DEFAULT = float(os.getenv("SLIPPAGE_BPS_MISSING_DEFAULT", "4.0") or 4.0)
DATA_HEALTH_ON_SPREAD_MISSING = float(os.getenv("DATA_HEALTH_ON_SPREAD_MISSING", "0.80") or 0.80)






# Счетчик для уменьшения логов добавления символов
_symbols_added_counter = 0





# ──────────────────────────────────────────────────────────────────────────────
from services.orderflow.signal_pipeline import SignalPipeline
from utils.atr_cache import get_atr_cache, ATRCache
from services.orderflow.metrics_batcher import MetricsBatcher


# ──────────────────────────────────────────────────────────────────────────────
# Runtime для одного символа
# ──────────────────────────────────────────────────────────────────────────────


# Optional microstructure metrics (prom)




class OrderFlowStrategy:
    def __init__(self, redis: aioredis.Redis, ticks: aioredis.Redis, publisher: AsyncSignalPublisher
                 of_engine: OFConfirmEngine, calib_svc=None, score_calibrator=None
                 notify_client: Optional[aioredis.Redis] = None, notify_stream: str = RS.NOTIFY_TELEGRAM
                 orders_queue_mt5: str = "", orders_queue_binance: str = ""):
        self.redis = redis
        self.ticks = ticks
        self.publisher = publisher
        self.of_engine = of_engine
        self.calib_svc = calib_svc
        self.score_calibrator = score_calibrator
        self.notify_client = notify_client
        self.notify_stream = notify_stream
        self.orders_queue_mt5 = orders_queue_mt5
        self.orders_queue_binance = orders_queue_binance
        self.logger = logging.getLogger("orderflow_strategy")
        self._latency_writer = LatencyStateWriter(service=SERVICE_PYTHON_WORKER)
        
        self.atr_cache: ATRCache = get_atr_cache()
        self.market_state = MarketStateService(redis_client=self.redis, atr_cache=self.atr_cache)
        self.signal_pipeline = SignalPipeline(publisher=self.publisher, atr_cache=self.atr_cache)
        self._smt_leader_gate = SmtLeaderCoherenceGate.from_env(redis_client=self.redis)

        # ------------------------------------------------------------------
        # Phase B1 — BBO time-series
        # ------------------------------------------------------------------
        # We publish compact BBO snapshots to Redis Stream `events:bbo_ts`.
        # Post-trade workers use this data to compute realized spread / impact.
        #
        # IMPORTANT: This is hot-path code. It MUST be:
        # - throttled (per symbol)
        # - bounded (no top-of-book arrays)
        # - fail-open (never blocks)
        self._bbo_cfg = BBOStoreCfg.from_env()
        self.low_conf_counters = {}
        self.strong_gate_counters = {}
        self.dn_gate_relaxed_counters = {}  # Counter for [DN-GATE] RELAXED messages
        self.dn_gate_proxy_relaxed_counters = {}  # Counter for [DN-GATE-PROXY] RELAXED messages
        self.conf_relax_counters = {}  # Counter for [CONF-RELAX] messages
        self.adverse_continuation_counters = {}  # Counter for [ADVERSE] Continuation Verified messages
        # Confidence scorer with injected calibrator (calibrator applied in _compute_confidence)
        self.conf_scorer = ConfidenceScorer(cfg=ConfidenceConfig())
        
        # Robust ATR sanity (last-good fallback + jump protection)
        # One instance per Strategy; per-symbol state is managed internally by ATRSanity.
        self._atr_sanity = ATRSanity(window=int(os.getenv("ATR_SANITY_WINDOW", "60")))

        # Bounded async sink для НЕкритических Redis-записей в hot path.
        # Заменяет fire-and-forget safe_create_task(self.redis.set/sadd/incr/expire...).
        # Запускать воркер через start_batcher() когда event loop уже работает.
        self._mbatch = MetricsBatcher(
            redis=redis
            maxsize=int(os.getenv("METRICS_BATCHER_MAXSIZE", "10000"))
            worker_label="orderflow_strategy"
        )
        self._mbatch_task = None  # устанавливается в start_batcher()
        self._pbatch = PrometheusBatcher(interval=1.0)
        self._pbatch_task = None

        # Book processor (handles OBI, iceberg, LOB, churn, GPU L2)
        self._book_processor = BookProcessor(
            book_churn_z_start=float(os.getenv("BOOK_CHURN_Z_START", "2.0"))
            book_churn_z_full=float(os.getenv("BOOK_CHURN_Z_FULL", "5.0"))
            book_churn_z_hi=float(os.getenv("BOOK_CHURN_Z_HI", "4.0"))
        )

        # ------------------------------------------------------------------
        # Liquidation Map (liqmap) feature enrichment (best-effort, fail-open)
        # ------------------------------------------------------------------
        # This consumes compact snapshots produced by `services/liquidation_map_service.py`
        # and stored under Redis keys like: liqmap:snapshot:<SYMBOL>:<WINDOW>.
        #
        # IMPORTANT risk discipline:
        #  - We do NOT widen SL to 'fit' a liquidation cluster (that should be a hard veto).
        #  - This layer only provides features/anchors; enforcement happens in gates/risk.
        self.liqmap_features_enable = bool(int(os.getenv("LIQMAP_FEATURES_ENABLE", "1") or 0))
        self.liqmap_snapshot_key_prefix = os.getenv("LIQMAP_SNAPSHOT_KEY_PREFIX", "liqmap:snapshot")
        self.liqmap_feature_windows = [w.strip() for w in os.getenv("LIQMAP_FEATURE_WINDOWS", "5m,1h").split(',') if w.strip()]
        self.liqmap_feature_cache_ms = int(os.getenv("LIQMAP_FEATURE_CACHE_MS", "400") or 400)
        self.liqmap_feature_redis_timeout_s = float(os.getenv("LIQMAP_FEATURE_REDIS_TIMEOUT_S", "0.02") or 0.02)
        self.liqmap_feature_max_stale_ms = int(os.getenv("LIQMAP_FEATURE_MAX_STALE_MS", "3500") or 3500)
        self.liqmap_feature_peak_range_bps = float(os.getenv("LIQMAP_FEATURE_PEAK_RANGE_BPS", "600") or 600)
        self.liqmap_feature_front_run_bps = float(os.getenv("LIQMAP_FEATURE_FRONT_RUN_BPS", "20") or 20)
        self.liqmap_feature_sl_buffer_bps = float(os.getenv("LIQMAP_FEATURE_SL_BUFFER_BPS", "15") or 15)

        # --- Hoisted hot-path constants (loaded once, avoids os.getenv on each tick) ---
        self._last_px_ttl_sec: int = int(os.getenv("LAST_PX_TTL_SEC", "600"))
        self._liq_min_slope: float = float(os.getenv("LIQ_MIN_BOOK_SLOPE", "0") or 0)
        self._liq_max_dws: float = float(os.getenv("LIQ_MAX_DWS_BPS", "0") or 0)
        self._liq_max_rec: float = float(os.getenv("LIQ_MAX_RECOVERY_TIME_MS", "0") or 0)
        self._manip_mode: str = str(os.getenv("MANIP_MODE", "auto") or "auto").strip().lower()
        self._gate_profile: str = str(os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
        self._manip_tighten_mult: float = float(os.getenv("MANIP_TIGHTEN_ADD_MULT", "1.0") or 1.0)
        self._manip_tighten_cap: float = float(os.getenv("MANIP_TIGHTEN_ADD_CAP_BPS", "6.0") or 6.0)
        self._atr_sanity_enable: bool = bool(int(os.getenv("ATR_SANITY_ENABLE", "1") or 1))
        self._atr_bad_ttl_sec: int = int(os.getenv("ATR_BAD_TTL_SEC", "600") or 600)
        self._atr_bad_symbols_set_ttl_sec: int = int(os.getenv("ATR_BAD_SYMBOLS_SET_TTL_SEC", "86400") or 86400)
        self._metrics_counter_ttl_sec: int = int(os.getenv("METRICS_COUNTER_TTL_SEC", "604800") or 604800)
        self._atr_jump_window_sec: int = int(os.getenv("ATR_JUMP_WINDOW_SEC", "3600") or 3600)
        self._atr_jump_symbols_set_ttl_sec: int = int(os.getenv("ATR_JUMP_SYMBOLS_SET_TTL_SEC", "86400") or 86400)
        self._cvd_quar_symbols_set_ttl_sec: int = int(os.getenv("CVD_QUAR_SYMBOLS_SET_TTL_SEC", "86400") or 86400)
        self._debug_deltas: bool = bool(int(os.getenv("DEBUG_DELTAS", "0") or 0))

        # ── Inline regime computation (shared across all symbols) ──
        # Single MarketRegimeService; per-symbol state lives in SymbolRuntime.
        self._regime_svc = None
        if MarketRegimeService is not None and RegimeConfig is not None:
            try:
                self._regime_svc = MarketRegimeService(RegimeConfig())
            except Exception:
                pass
        self._regime_delta_alpha: float = float(os.getenv("REGIME_DELTA_EMA_ALPHA", "0.05"))
        self._regime_hold_alpha: float = float(os.getenv("REGIME_HOLD_EMA_ALPHA", "0.10"))
        self._regime_pub_gap_ms: int = int(os.getenv("REGIME_REDIS_PUB_GAP_MS", "2000"))
        self._regime_redis_ttl_sec: int = int(os.getenv("REGIME_REDIS_TTL_SEC", "120"))
        self._runtime_refresh_tasks: Dict[Tuple[str, str], asyncio.Task] = {}

    def _schedule_runtime_refresh(self, runtime, refresh_name: str, coro_factory) -> None:
        """Keep Redis-backed runtime refreshes out of the tick loop task churn."""
        try:
            symbol = str(getattr(runtime, "symbol", "") or "")
            key = (symbol, str(refresh_name))
            task = self._runtime_refresh_tasks.get(key)
            if task is not None and not task.done():
                return
            self._runtime_refresh_tasks[key] = safe_create_task(coro_factory())
        except Exception:
            pass

    def start_batcher(self):
        """Запустить фоновые воркеры MetricsBatcher и PrometheusBatcher."""
        if self._mbatch_task is None:
            self._mbatch_task = safe_create_task(self._mbatch.run())
        if self._pbatch_task is None:
            self._pbatch_task = safe_create_task(self._pbatch.run())

    def cleanup_symbol(self, symbol: str) -> None:
        """Removes all internal tracking state for a symbol to prevent memory leaks."""
        sym = str(symbol or "").upper()
        if not sym:
            return
            
        # Cleanup primitive counters
        self.low_conf_counters.pop(sym, None)
        self.strong_gate_counters.pop(sym, None)
        self.dn_gate_relaxed_counters.pop(sym, None)
        self.dn_gate_proxy_relaxed_counters.pop(sym, None)
        self.conf_relax_counters.pop(sym, None)
        self.adverse_continuation_counters.pop(sym, None)
        
        # Cleanup ATR cache entries if the method exists
        if hasattr(self.atr_cache, 'cleanup_symbol'):
            self.atr_cache.cleanup_symbol(sym)
            
        if hasattr(self, 'market_state') and hasattr(self.market_state, 'cleanup_symbol'):
            self.market_state.cleanup_symbol(sym)

    async def process_book(self, runtime, payload, ingest_ts_ms: int) -> bool:
        """Delegate book snapshot processing to BookProcessor.

        This method exists so that the service layer can call
        `await self.strategy.process_book(runtime, payload, ingest_ts_ms)`
        without needing to know about internal component boundaries.
        BookProcessor.process_book is synchronous; we wrap it in a coroutine
        so the call-site can uniformly use `await`.
        """
        try:
            ok = self._book_processor.process_book(runtime, payload, ingest_ts_ms)

            # Hot-path best-effort: publish BBO snapshot after book is parsed.
            # We use book event time from runtime.book_state.ts_ms (deterministic).
            try:
                if ok and self._bbo_cfg.enabled:
                    bs = getattr(runtime, "book_state", None)
                    ts_ms = int(getattr(bs, "ts_ms", 0) or 0) if bs is not None else 0
                    if ts_ms > 0:
                        await maybe_publish_bbo(
                            publisher=self.publisher
                            cfg=self._bbo_cfg
                            runtime=runtime
                            book_ts_ms=ts_ms
                        )
            except Exception:
                # Never break book processing.
                pass

            return ok
        except Exception as exc:
            from services.orderflow.metrics import log_silent_error
            log_silent_error(exc, 'strategy_process_book', getattr(runtime, 'symbol', 'unknown'), 'OrderFlowStrategy.process_book')
            return False

    def _liqmap_snapshot_key(self, *, symbol: str, window: str) -> str:
        return f"{self.liqmap_snapshot_key_prefix}:{str(symbol).upper()}:{str(window)}"

    async def _fetch_liqmap_bg(self, runtime, windows_to_fetch: list, keys: list, now_ms: int):
        try:
            raw_list = await asyncio.wait_for(
                self.redis.mget(keys), 
                timeout=float(self.liqmap_feature_redis_timeout_s)
            )
            cache = getattr(runtime, 'liqmap_snapshot_cache', {})
            if raw_list and len(raw_list) == len(windows_to_fetch):
                for i, w in enumerate(windows_to_fetch):
                    raw = raw_list[i]
                    payload = try_parse_liqmap_snapshot_json(raw)
                    if payload:
                        cache[w] = {'fetch_ms': int(now_ms), 'payload': payload}
                    else:
                        if w in cache:
                            cache[w]['fetch_ms'] = int(now_ms)
                        else:
                            cache[w] = {'fetch_ms': int(now_ms), 'payload': None}
        except Exception:
            pass

    async def _maybe_add_liqmap_features(self, *, runtime, indicators: dict, mid_px: float, now_ms: int) -> None:
        """Enrich indicators with liqmap-derived features (best-effort).

        This is intentionally fail-open and short-timeout to keep the hot path safe.
        Caching is per-symbol (stored on runtime object).
        """
        if not self.liqmap_features_enable:
            return
        if not (isinstance(mid_px, (int, float)) and float(mid_px) > 0.0):
            return

        # Per-symbol cache state: { window: { 'fetch_ms': int, 'payload': dict } }
        cache = getattr(runtime, 'liqmap_snapshot_cache', None)
        if cache is None or not isinstance(cache, dict):
            cache = {}
            setattr(runtime, 'liqmap_snapshot_cache', cache)

        windows_to_fetch = []
        for w in self.liqmap_feature_windows:
            ent = cache.get(w)
            fetch_ms = int(ent.get('fetch_ms', 0) or 0) if isinstance(ent, dict) else 0
            if (now_ms - fetch_ms) >= int(self.liqmap_feature_cache_ms):
                windows_to_fetch.append(w)

        # 1. Batch fetch from Redis using background task (Optimization for hot-path)
        if windows_to_fetch:
            keys = [self._liqmap_snapshot_key(symbol=runtime.symbol, window=w) for w in windows_to_fetch]
            safe_create_task(self._fetch_liqmap_bg(runtime, windows_to_fetch, keys, now_ms))
            
            for w in windows_to_fetch:
                # Optimistically mark as fetched to avoid queueing duplicate tasks next tick
                if w not in cache:
                    cache[w] = {'fetch_ms': int(now_ms), 'payload': None}
                else:
                    cache[w]['fetch_ms'] = int(now_ms)

        # 2. Extract features from cached (old or just updated) payloads
        primary_window = None
        for w in self.liqmap_feature_windows:
            if not primary_window:
                primary_window = w
                
            ent = cache.get(w)
            payload = ent.get('payload') if isinstance(ent, dict) else None
            if payload is None:
                continue

            feats = compute_liqmap_features_from_snapshot(
                payload=payload
                mid_px=float(mid_px)
                now_ms=int(now_ms)
                max_stale_ms=int(self.liqmap_feature_max_stale_ms)
                peak_range_bps=float(self.liqmap_feature_peak_range_bps)
                front_run_bps=float(self.liqmap_feature_front_run_bps)
                sl_buffer_bps=float(self.liqmap_feature_sl_buffer_bps)
            )

            # Write per-window features
            for k, v in (feats or {}).items():
                try:
                    indicators[f'liqmap_{w}_{k}'] = float(v)
                except Exception:
                    continue

            # Promote directional anchors from the primary window to generic keys
            if primary_window and w == primary_window and feats:
                for k in (
                    'tp1_anchor_bps_long'
                    'tp1_anchor_bps_short'
                    'sl_reco_bps_long'
                    'sl_reco_bps_short'
                    'squeeze_bias'
                    'is_stale'
                    'stale_ms'
                    'levels_n'
                ):
                    if k in feats:
                        try:
                            indicators[f'liqmap_{k}'] = float(feats[k])
                        except Exception:
                            pass

        # Convenience flag: did we produce anything meaningful?
        if 'liqmap_levels_n' in indicators and float(indicators.get('liqmap_levels_n') or 0.0) > 0.0:
            indicators['liqmap_ok'] = 1
        else:
            indicators['liqmap_ok'] = 0


    async def _maybe_poll_symbol_overrides(self, runtime, now_ms: int) -> None:
        """
        Pull cfg:crypto_of:overrides:{SYMBOL} (JSON) and merge selected keys into runtime.config.
        Fail-open, throttled, deterministic by now_ms=tick_ts.
        """
        try:
            gap = int(getattr(runtime, "_ov_poll_gap_ms", 2500) or 2500)
            ts0 = int(getattr(runtime, "_ov_ts_ms", 0) or 0)
            if (now_ms - ts0) < gap:
                return
            runtime._ov_ts_ms = int(now_ms)
            key = f"cfg:crypto_of:overrides:{str(runtime.symbol).upper()}"
            raw = await self.redis.get(key)
            if not raw:
                return
            # etag to avoid repeated json loads (simple hash-like etag)
            etag = str(abs(hash(raw)))
            if etag == str(getattr(runtime, "_ov_etag", "") or ""):
                return
            runtime._ov_etag = etag
            d = json.loads(raw)
            if not isinstance(d, dict):
                return
            # allowlist of keys (avoid accidental config takeover)
            allow = {
                "cooldown_reversal_sec"
                "cooldown_continuation_sec"
                "pressure_hi_sps"
                "pressure_ema_alpha"
                "cooldown_mul_thin"
                "cooldown_spread_hi_bp"
                "cooldown_mul_wide_spread"
                "cooldown_mul_pressure_hi"
                "cooldown_min_ms"
                "cooldown_max_ms"
                "burst_audit_enable"
                "burst_audit_sample"
            }
            for k, v in d.items():
                if k in allow:
                    runtime.config[k] = v
        except Exception as exc:
            log_silent_error(exc, 'config_update_failure', runtime.symbol if runtime else "unknown", '_maybe_poll_symbol_overrides')
            return

    async def _burst_audit(self, *, runtime, now_ms: int, event: str, payload: Dict[str, Any], indicators: Dict[str, Any], extra: Dict[str, Any]) -> None:
        """
        Low-volume audit for cooldown floods and best-of-burst selection.
        Fail-open. Uses deterministic sampling.
        """
        try:
            cfg = runtime.config or {}
            if not bool(int(cfg.get("burst_audit_enable", 0))):
                return
            rate = float(cfg.get("burst_audit_sample", 0.05) or 0.05)
            if not _should_sample(int(now_ms), rate):
                return
            msg = {
                "type": "burst_audit"
                "ts_ms": str(int(now_ms))
                "symbol": str(runtime.symbol)
                "event": str(event)
                "payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
                "ind": json.dumps({
                    "scenario": indicators.get("strong_gate_scn") or ""
                    "of_score": indicators.get("of_confirm_score", 0.0)
                    "delta_z": indicators.get("delta_z", 0.0)
                    "pressure_sps": float(getattr(runtime, "pressure_sps", 0.0) or 0.0)
                    "pressure_hi": int(getattr(runtime, "pressure_hi", 0) or 0)
                    "regime": str(getattr(runtime, "last_regime", "na") or "na")
                    "spread_bp": float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
                    "obi_age_ms": indicators.get("obi_age_ms", -1)
                    "iceberg_age_ms": indicators.get("iceberg_age_ms", -1)
                }, ensure_ascii=False, separators=(",", ":"))
                "extra": json.dumps(extra or {}, ensure_ascii=False, separators=(",", ":"))
            }
            await self.redis.xadd(self.burst_audit_stream, msg, maxlen=200000, approximate=True)
        except Exception as exc:
            log_silent_error(exc, 'audit_failure', self.symbol or "unknown", '_burst_audit')
            return

    # ── Публичные методы ──────────────────────────────────────────────────────


    # ── Динамическая загрузка символов ────────────────────────────────────────










    # ── Основные рабочие циклы ────────────────────────────────────────────────

    async def process_tick(self, runtime: SymbolRuntime, tick: Dict[str, Any], worker_lag_ms: float = 0.0) -> Optional[Dict[str, Any]]:
        # Initialize variables that may not be set if exceptions occur
        ofc = None
        dec = None
        direction = None


        # Быстрый ранний выход: некорректный тик
        if not tick or not isinstance(tick, dict):
            return None
        runtime.tick_count += 1
        runtime.heartbeat_counter += 1
        # Нормализуем qty/volume, чтобы downstream не падал
        if "qty" not in tick and "volume" in tick:
            tick["qty"] = tick.get("volume")
        if tick.get("qty") is None and tick.get("volume") is None:
            tick["qty"] = 0.0
        if tick.get("price") is None:
            # Без цены не обрабатываем
            return None
        if not hasattr(self, "logger"):
            self.logger = logger
        
        # ------------------------------------------------------------------
        # Robust Time Normalization (Expert Recommendation 3, Patch 1)
        # ------------------------------------------------------------------
        _t0_validate = time.monotonic_ns()  # Latency audit: validate_time sub-stage

        if tick.get("mock_force"):
            runtime.mock_force = tick.get("mock_force")
            logger.warning("⚠️ tick mock forced: %s", runtime.mock_force)
        
        # Local cfg snapshot (avoid NameError; deterministic per tick)
        cfg = runtime.config or {}
        
        tick_ts = int(
            tick.get("ts_ms")
            or tick.get("ts")
            or tick.get("event_time")
            or tick.get("E")
            or tick.get("T")
            or tick.get("time")
            or tick.get("written_at")
            or 0
        )
        # Only fallback if 0
        if tick_ts <= 0:
            from services.orderflow.metrics import tick_ts_missing_total
            if tick_ts_missing_total:
                tick_ts_missing_total.labels(symbol=runtime.symbol).inc()
            return None

        indicators: Dict[str, Any] = {}
        
        if not self._validate_tick_time(runtime, tick_ts, cfg, indicators):
            return None

        # Latency audit: record validate_time sub-stage
        try:
            _dt_validate = (time.monotonic_ns() - _t0_validate) / 1_000  # ns -> us
            process_tick_validate_time_us.labels(symbol=runtime.symbol).observe(_dt_validate)
        except Exception:
            pass

        # 1. СРАЗУ ИЗВЛЕКАЕМ PRICE (Нужен для CVD и Notional)
        price = _safe_float(tick.get("price") or tick.get("last") or tick.get("mid"))
        if not price or price <= 0:
            return None

        # 2. Lazy Start для батчеров (если они еще не запущены)
        if self._mbatch_task is None:
            self.start_batcher()



        runtime.last_ts_ms = int(tick_ts)
        sess = session_utc(int(tick_ts))
        how = hour_of_week_utc(int(tick_ts))
        indicators["session"] = sess
        indicators["hour_of_week"] = how

        # ------------------------------------------------------------------
        # Source consistency guard (dual-source / CVD jump)
        # - detects implausible delta jumps and marks source_consistency_ok=0
        # - consumer policy: turn book evidences off, optionally shadow-only
        # ------------------------------------------------------------------
        _t0_cvd = time.monotonic_ns()  # Latency audit: cvd_update sub-stage
        try:
            px = float(tick.get("price") or 0.0)
            cvd = float(getattr(runtime, "cvd_last", 0.0) or 0.0)
            cvd_prev = float(getattr(runtime, "cvd_prev", cvd) or cvd)
            # compute jump in USD
            jump_usd = 0.0
            if px > 0:
                jump_usd = abs(cvd - cvd_prev) * px
            # thresholds: default high to avoid false triggers
            j_usd_th = float(cfg.get("source_jump_usd_th", 50_000_000.0))
            if jump_usd > j_usd_th:
                indicators["source_consistency_ok"] = 0
                indicators["source_jump_usd"] = float(jump_usd)
                # cool down period (ms) during which we keep it marked inconsistent
                until = int(tick_ts) + int(cfg.get("source_inconsistent_ttl_ms", 60_000))
                setattr(runtime, "source_inconsistent_until_ms", until)
            else:
                until = int(getattr(runtime, "source_inconsistent_until_ms", 0) or 0)
                if until > int(tick_ts):
                    indicators["source_consistency_ok"] = 0
                else:
                    indicators["source_consistency_ok"] = 1
            setattr(runtime, "cvd_prev", cvd)
            setattr(runtime, "cvd_last", cvd)
        except Exception:
            pass
        # Latency audit: record cvd_update sub-stage
        try:
            _dt_cvd = (time.monotonic_ns() - _t0_cvd) / 1_000
            process_tick_cvd_update_us.labels(symbol=runtime.symbol).observe(_dt_cvd)
        except Exception:
            pass

        # Expert Recommendation 4: Track timestamp for Gap Cap
        lt_seen = int(getattr(runtime, "last_tick_seen_ts", 0) or 0)
        if lt_seen > 0 and tick_ts > lt_seen:
             gap = tick_ts - lt_seen
             try:
                 runtime.tick_gaps_ms.append(int(gap))
             except Exception:
                 pass
        runtime.last_tick_seen_ts = int(tick_ts)

        # Runtime overrides (cooldown/pressure tuning) — throttled, fail-open
        now_ms = int(tick_ts)
        # Liquidation map feature enrichment (best-effort, fail-open, short-timeout)
        _t0_liqmap = time.monotonic_ns()  # Latency audit: liqmap sub-stage
        try:
            await self._maybe_add_liqmap_features(runtime=runtime, indicators=indicators, mid_px=float(price), now_ms=int(now_ms))
        except Exception:
            pass
        # Latency audit: record liqmap sub-stage (includes Redis GET + parse + compute)
        try:
            _dt_liqmap = (time.monotonic_ns() - _t0_liqmap) / 1_000
            process_tick_liqmap_us.labels(symbol=runtime.symbol).observe(_dt_liqmap)
        except Exception:
            pass
        # Legacy override poll (cfg:crypto_of:overrides)
        ov_gap = int(getattr(runtime, "_ov_poll_gap_ms", 2500) or 2500)
        ov_ts0 = int(getattr(runtime, "_ov_ts_ms", 0) or 0)
        if (now_ms - ov_ts0) >= ov_gap:
            self._schedule_runtime_refresh(
                runtime
                "legacy_overrides"
                lambda: self._maybe_poll_symbol_overrides(runtime, now_ms)
            )
        
        # SRE Versioned Overrides V1 (High Priority)
        ov1_gap = int(cfg.get("overrides_cache_ttl_ms", 30000))
        ov1_ts0 = int(getattr(runtime, "overrides_loaded_ts_ms", 0) or 0)
        if (now_ms - ov1_ts0) >= ov1_gap:
            self._schedule_runtime_refresh(
                runtime
                "overrides_v1"
                lambda: runtime.maybe_load_overrides(self.redis)
            )

        # v12_of: refresh cross-asset metrics from go-worker Redis Hash (5s TTL, fail-open)
        ca_gap = int(getattr(runtime, "config", {}).get("crossasset_cache_ttl_ms", 5_000))
        ca_ts0 = int(getattr(runtime, "_crossasset_last_load_ms", 0) or 0)
        if (now_ms - ca_ts0) >= ca_gap:
            self._schedule_runtime_refresh(
                runtime
                "crossasset_v12"
                lambda: runtime.maybe_load_crossasset(self.redis)
            )
            self._schedule_runtime_refresh(
                runtime
                "crossasset_v13"
                lambda: runtime.maybe_load_crossasset_v13(self.redis)
            )

        # Initialize early
        confirmations: List[str] = []
        
        # --- Apply Overrides V1 into local cfg view (deterministic per tick best-effort) ---
        # We start with runtime.config (base)
        cfg = runtime.config
        try:
            o = getattr(runtime, "overrides_obj", None)
            if o is not None and int(getattr(o, "enabled", 0) or 0) == 1:
                # Canary decision:
                #  - if canary_symbols defined -> apply only if symbol is listed
                #  - else apply by deterministic hash-share (optional)
                ro = getattr(o, "rollout", None)
                apply_ovr = True
                if ro is not None and str(getattr(ro, "mode", "full") or "full").lower() == "canary":
                    syms = set([str(x).upper() for x in (getattr(ro, "canary_symbols", []) or []) if x])
                    if syms:
                        apply_ovr = (str(runtime.symbol or "").upper() in syms)
                    else:
                        # Fallback to share logic? 
                        # Implement deterministic hash share if share < 1.0 (optional)
                        pass

                if apply_ovr:
                    cfg = o.apply_to_cfg(cfg)
                    indicators["policy_sid"] = str(getattr(runtime, "overrides_sid", "") or "")
                    indicators["policy_src"] = "overrides_v1"
        except Exception:
            cfg = runtime.config

        # Book health: check gaps and staleness
        book_ts_base = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
        book_gap = int(tick_ts - book_ts_base) if book_ts_base > 0 else 0
        book_stale_ms = int(runtime.config.get("book_stale_ms", 15000))
        book_ok = 1 if (book_ts_base > 0 and book_gap < book_stale_ms) else 0
        indicators["book_health_ok"] = int(book_ok)
        indicators["book_ts_gap_ms"] = int(book_gap)

        # ------------------------------------------------------------
        # Liquidity regime snapshot (risk overlay)
        # ------------------------------------------------------------
        self._update_liquidity_regime(runtime, tick_ts, indicators)

        # Track tick gaps (Section 5: Burst Calibrator)
        try:
            runtime.tick_gaps.record(int(tick_ts))
        except Exception:
            pass

        # Periodic calibration (every 200 ticks)
        if runtime.tick_count % 200 == 0:
            try:
                # Update window/max_age only if burst is not currently active
                # using the lock for safety although st.active check is usually okay
                async with runtime.burst_mu:
                    is_active = getattr(runtime.burst.st, "active", False)
                    if not is_active:
                        gaps = runtime.tick_gaps.snapshot()
                        p_snap = runtime.pressure.snapshot(now_ms=int(tick_ts))
                        
                        w, ma = runtime.burst_cal.compute(
                            gap_p50_ms=float(gaps.get("p50", 0.0))
                            cand_per_min=float(p_snap.per_min_ema)
                        )
                        runtime.burst.window_ms = int(w)
                        runtime.burst.max_age_ms = int(ma)
                        
                        # Metrics visibility
                        burst_window_ms_gauge.labels(symbol=runtime.symbol).set(float(w))
                        tick_gap_p50_ms_gauge.labels(symbol=runtime.symbol).set(float(gaps.get("p50", 0.0)))
            except Exception:
                pass
            
        # --- Book Health Gating (Stop Evidence) ---
        # If book is unhealthy, we cannot trust OBI or Iceberg signals.
        # We nullify them (force 0.0) so they don't contribute to the score.
        if int(indicators.get("book_health_ok", 1)) == 0:
            # We don't VETO the entire signal (maybe price action is valid)
            # but we remove microstructure evidence component.
            # (unless it's a super-strong price move > strong_z, handled elsewhere)
            # Nullify indicators for downstream
            indicators["obi"] = 0.0
            indicators["obi_z"] = 0.0
            indicators["iceberg_refresh"] = 0
            indicators["iceberg_avg_qty"] = 0.0
            # Optional: Log throttling?
            pass

        if runtime.heartbeat_counter >= 5000:
            self.logger.info(
                "💓 (%s) Heartbeat: processed 5000 ticks (total=%d) | last_price=%.2f | delta_triggers=%d"
                runtime.symbol
                runtime.tick_count
                float(tick.get("price") or 0.0)
                runtime.delta_triggers
            )
            runtime.heartbeat_counter = 0
        
        # Check side classification
        s = str(tick.get("side") or "").upper()
        if s not in ("BUY", "SELL"):
             ticks_side_unknown_total.labels(symbol=runtime.symbol).inc()

        # Tick-CVD update (Phase A) BEFORE delta_detector.push()

        try:
            if runtime.cvd_state:
                # Track previous CVD for consistency check
                prev_cvd = float(getattr(runtime.cvd_state, "cvd_tick", 0.0) or 0.0)
                runtime.cvd_state.update(tick)
                cvd_now = float(getattr(runtime.cvd_state, "cvd_tick", 0.0) or 0.0)
                
                # Compute delta_usd for CVD consistency guard
                # delta_usd = delta_qty * price (approximate)
                px = float(tick.get("price") or price or 0.0)
                delta_qty = float(getattr(runtime.cvd_state, "last_delta_tick", 0.0) or 0.0)
                delta_usd = abs(delta_qty * px) if (px > 0 and delta_qty != 0) else 0.0
                
                # CVD consistency guard (quarantine on jumps)
                if not hasattr(runtime, "_cvd_guard"):
                    from core.cvd_consistency import CVDConsistencyGuard
                    runtime._cvd_guard = CVDConsistencyGuard()
                
                ts_ms = int(tick.get("ts", 0) or 0)
                dec = runtime._cvd_guard.update(
                    sym=runtime.symbol
                    ts_ms=ts_ms
                    cvd_now=cvd_now
                    delta_usd=delta_usd
                )
                if dec.quarantine_active:
                    runtime.cvd_quarantine_active = 1
                    runtime.cvd_quarantine_until_ms = int(dec.quarantine_until_ms)
                    runtime.delta_fallback_mode = "volume"
                    # IMPORTANT: disable CVD-derived deltas/divergences
                    # 1) don't update cvd-based slope/divergence features
                    # 2) compute delta_usd from volume-based aggregation (buy_qty - sell_qty) * mid
                    # (exact computation depends on your tick payload/aggregation)
                else:
                    runtime.cvd_quarantine_active = 0
                    runtime.delta_fallback_mode = "cvd"
        except Exception:
            pass

        # MicroBar aggregation (Phase B)
        try:
            if runtime.microbar:
                cvd_val = getattr(runtime.cvd_state, "cvd_tick", 0.0)
                closed_bars = runtime.microbar.push_tick(tick, cvd_val)
                if closed_bars:
                    for b in closed_bars:
                        # === Microstructure spread robust stats (per-symbol) ===
                        try:
                            mid = float(getattr(b, "mid_last", 0.0) or 0.0)
                            spr = float(getattr(b, "spread_last", 0.0) or 0.0)
                            if mid > 0 and spr > 0:
                                spread_bps = 10000.0 * (spr / mid)
                                # DEBUG-SPREAD: removed unsampled warning (was blocking event loop on every bar)
                                runtime.last_spread_bps = float(spread_bps)
                                runtime.spread_stats.update(float(spread_bps))
                                runtime.last_spread_z = float(runtime.spread_stats.z(float(spread_bps)))
                        except Exception:
                            pass
                        
                        # Fire async microbar closed handler
                        try:
                            safe_create_task(self._on_microbar_closed(runtime, b))
                        except Exception:
                            pass
        except Exception:
            pass

        # --- L3-lite (Reconciliation metrics) ---
        self._update_l3_stats(runtime, tick_ts, tick)

        delta_event = runtime.delta_detector.push(tick)
        if delta_event:
             # DEBUG: Confirm event creation immediately (every 10000th)
             sampled_info(logger, "DELTA_EVENT", "🔍 [DELTA-EVENT] (%s) Event created: delta=%.2f z=%.2f", runtime.symbol, delta_event.get("delta", 0.0), delta_event.get("z", 0.0))

        # ------------------------------------------------------------
        # Publish last price (for ATR selector / diagnostics)
        # ------------------------------------------------------------
        try:
            if price > 0:
                sym = str(getattr(runtime, "symbol", "") or "")
                if sym:
                    # tick_ts (Event Time) — не wall clock — для детерминизма
                    self._mbatch.put("set", f"cfg:last_px:{sym}", str(price), ex=self._last_px_ttl_sec)
                    self._mbatch.put("set", f"cfg:last_px_ts_ms:{sym}", str(int(tick_ts)), ex=self._last_px_ttl_sec)
        except Exception:
            pass

        # Pressure metric: raw triggers rate (pre-cooldown)
        try:
            if delta_event:
                runtime.pressure.on_raw_trigger(ts_ms=int(tick_ts))
            ps = runtime.pressure.snapshot(now_ms=int(tick_ts))
            indicators["pressure_per_min_ema"] = float(ps.per_min_ema)
            indicators["cooldown_hit_rate_ema"] = float(ps.cd_rate_ema)
            runtime.pressure_sps = float(ps.per_min_ema) / 60.0
        except Exception:
            pass

        # [REMOVED] Duplicate DN-PREFILTER-1 (Expert Check)
        # We rely on the second prefilter block (lines ~3200) which has the same logic but better context comments.

        
        # --- Prefilter: delta_notional_usd tiers (self-calibrating via dn_calib) ---
        # [REMOVED] Duplicate DN-PREFILTER-1 (Expert Check)
        # We rely on the second prefilter block (which has the same logic but better context comments).

        
        # Check against USD threshold if present
        if delta_event:
            delta_val = float(delta_event.get("delta", 0.0))
            delta_usd = abs(delta_val) * price
            min_usd = float(runtime.config.get("delta_abs_min_usd", 0.0) or 0.0)
            if min_usd > 1.0 and delta_usd < min_usd:
                 # Vetoed by USD threshold
                 logger.warning(
                     "🛑 [MIN-USD] (%s) VETO: delta_usd=$%.2f < min=$%.2f - Signal blocked"
                     runtime.symbol, delta_usd, min_usd
                 )
                 return None

        # BURST: tick-driven flush even without new candidates (ensure signals don't get stuck)
        try:
            if bool(int(runtime.config.get("burst_enable", 1))) and getattr(runtime.burst.st, "active", False):
                # [OPT A] Strategy only considers, background loop handles flush.
                # Remove sync maybe_flush() to prevent "phantom" emissions or double-publish.
                pass
        except Exception:
            pass

        if not delta_event:
            self._log_metrics(runtime)
            return None

        _t0_gates = time.monotonic_ns()

        # Trigger Event!
        runtime.delta_triggers += 1
        of_session_outcome_total.labels(runtime.symbol, sess, "trigger_delta").inc()
        
        # --- Pressure tracking: candidate attempts (deterministic by tick_ts) ---
        try:
            runtime.signal_attempt_ts_ms.append(int(tick_ts))
            psps = _calc_pressure_sps(list(runtime.signal_attempt_ts_ms), int(tick_ts), 60_000)
            # light smoothing (EMA)
            a = float(runtime.config.get("pressure_ema_alpha", 0.20))
            if a <= 0 or a > 1: a = 0.20
            runtime.pressure_sps = float((1.0 - a) * float(getattr(runtime, "pressure_sps", 0.0) or 0.0) + a * psps)
            indicators["pressure_sps"] = float(runtime.pressure_sps)
            # pressure_hi flag
            thr = float(runtime.config.get("pressure_hi_sps", 0.12))  # ~7.2 кандидатов/мин
            runtime.pressure_hi = 1 if runtime.pressure_sps >= thr else 0
            indicators["pressure_hi"] = int(runtime.pressure_hi)
        except Exception:
            pass

        # Update indicators with trigger context
        indicators["delta_z"] = delta_event.get("z", 0.0)
        
        # Диагностика: логируем срабатывание детектора (по флагу)
        if DEBUG_DELTAS:
            # Sampled debug log for delta trigger
            if runtime.delta_log_sampler.should_log("delta_trigger"):
                logger.debug(
                    "🔍 (%s) Delta detector triggered: delta=%.2f, z=%.2f, threshold=%.2f"
                    runtime.symbol
                    delta_event.get("delta", 0.0)
                    delta_event.get("z", 0.0)
                    runtime.delta_detector.z_threshold
                )

        # Determine signal direction
        direction = "LONG" if delta_event["delta"] >= 0 else "SHORT"

        # [NEW] SMT Leader Coherence injection
        if hasattr(self, "_smt_leader_gate") and self._smt_leader_gate:
            try:
                class _Ctx: pass
                _ctx = _Ctx()
                self._smt_leader_gate.evaluate(
                    ctx=_ctx
                    symbol=runtime.symbol
                    kind="orderflow_strategy"
                    direction="UP" if direction == "LONG" else "DOWN"
                )
                if hasattr(_ctx, "smt_leader_confirm"):
                    indicators["smt_leader_confirm"] = int(getattr(_ctx, "smt_leader_confirm", 0))
                    indicators["smt_coh"] = float(getattr(_ctx, "smt_coh", 0.0))
                    indicators["smt_leader_dir"] = str(getattr(_ctx, "smt_leader_dir", "NA"))
            except Exception as e:
                self.logger.error("Failed to inject SMT Coherence state: %s", e)

        # ------------------------------------------------------------------
        # ATR floor veto (tier-by-regime) — FIX BROKEN CHAIN
        # ВАЖНО:
        #   - раньше читали atr_bps_th, но не выбирали tier -> th оставался 0.0
        #   - теперь выбираем tier прямо здесь (safety), используя runtime.dynamic_cfg + bootstrap.
        # Fail-open:
        #   - если чего-то не хватает -> не блокируем (как и было), но всё логируем в indicators.
        # ------------------------------------------------------------------
        # ------------------------------------------------------------------
        # Authoritative DeltaNotional Tier Gating (Expert Recommendation)
        passed, tier, delta_usd, dn_tiers_decision = self._eval_dn_gate(runtime, tick_ts, delta_event, price, indicators)
        if not passed:
             return None

        # Add indicators
        indicators["dn_tier"] = int(tier)
        indicators["dn_usd"] = float(delta_usd)
        indicators["dn_t1_usd"] = float(dn_tiers_decision.tier1_usd)
        indicators["dn_src"] = str(dn_tiers_decision.src)
        
        # P2: Inject Liquidity Scale (Hour-of-Week) for Risk/Conf
        indicators["liquidity_scale"] = float(dn_tiers_decision.scale)


        # Детерминированное "now" — tick_ts > 0 гарантировано (проверка в начале process_tick)
        now_ts = tick_ts

        indicators.update({
            "delta": delta_event.get("delta", 0.0)
            "delta_z": delta_event.get("z", 0.0)
        })

        # Pre-calculate absorption once for all consumers (Variant A + OFConfirm)
        absorption_feat = None
        try:
            absorption_feat = runtime.absorption_detector.push(tick, runtime.last_book, price)
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Variant A: Publish delta_spike event for decentralized OFConfirm service
        # ------------------------------------------------------------------
        try:
            spike_out = {
                "type": "delta_spike"
                "symbol": runtime.symbol
                "ts_ms": now_ts
                "price": float(price)
                "direction": direction
                "delta": float(delta_event.get("delta", 0.0))
                "delta_z": float(delta_event.get("z", 0.0))
            }
            # Optional: if we already have features from runtime
            # Optional: if we already have features from runtime
            if absorption_feat:
                spike_out["absorption"] = absorption_feat
            
            # Enrich with OBI/Iceberg (if not stale)
            now_ms = int(tick_ts) # EXPERT FIX: Use tick_ts instead of wall-time
            obi_ttl = int(runtime.config.get("obi_event_ttl_ms", 30000))
            if runtime.last_obi_event and (now_ms - runtime.last_obi_event.get("ts_ms", 0)) < obi_ttl:
                spike_out["obi"] = runtime.last_obi_event
            
            ice_ttl = int(runtime.config.get("iceberg_event_ttl_ms", 15000))
            if runtime.last_iceberg_event and (now_ms - runtime.last_iceberg_event.get("ts_ms", 0)) < ice_ttl:
                spike_out["iceberg"] = runtime.last_iceberg_event
            
            # Enrich with L3-lite stats
            if runtime.l3_stats:
                spike_out.update({
                    "cancel_bid_rate_ema": float(runtime.l3_stats.cancel_bid_rate_ema)
                    "cancel_ask_rate_ema": float(runtime.l3_stats.cancel_ask_rate_ema)
                    "taker_buy_rate_ema": float(runtime.l3_stats.taker_buy_rate_ema)
                    "taker_sell_rate_ema": float(runtime.l3_stats.taker_sell_rate_ema)
                })

            self._mbatch.put(
                "xadd"
                "events:delta_spike"
                {"payload": json.dumps(spike_out, ensure_ascii=False)}
                maxlen=20000
            )
        except Exception as e:
            logger.error(f"Failed to publish delta_spike event: {e}")

        # Attach Tick-CVD indicators
        try:
            if runtime.cvd_state:
                indicators.update(runtime.cvd_state.indicators_light())
                indicators.update(runtime.cvd_state.robust_snapshot())
        except Exception:
            pass

        # Attach Phase B structure snapshots
        try:
            if runtime.last_bar:
                b = runtime.last_bar
                indicators.update({
                    "microbar_tf_ms": int(b.tf_ms)
                    "microbar_start_ts": int(b.start_ts_ms)
                    "microbar_end_ts": int(b.end_ts_ms)
                    "microbar_open": float(b.open)
                    "microbar_high": float(b.high)
                    "microbar_low": float(b.low)
                    "microbar_close": float(b.close)
                    "microbar_vol": float(b.vol)
                    "microbar_delta_sum": float(b.delta_sum)
                    "microbar_cvd_close": float(b.cvd_close)
                    "microbar_vwap": float(b.vwap)
                    "microbar_mid": float(b.mid_last) if b.mid_last is not None else None
                    "microbar_spread": float(b.spread_last) if b.spread_last is not None else None
                    "microbar_ticks": int(b.tick_count)
                })
            
            # RSI indicators (if available)
            if hasattr(runtime, "rsi_price") and runtime.rsi_price.value is not None:
                indicators["rsi_price"] = float(runtime.rsi_price.value)
            if hasattr(runtime, "rsi_cvd") and runtime.rsi_cvd.value is not None:
                indicators["rsi_cvd"] = float(runtime.rsi_cvd.value)

            # RSI Confirmation check
            rp = float(indicators.get("rsi_price", 50.0))
            rc = float(indicators.get("rsi_cvd", 50.0))
            
            # v7 structure
            indicators.setdefault("conf_rsi_agree", 0)
            conf_feature_seen_total.labels(feature="rsi_agree", src="strategy").inc()
            
            if direction == "LONG" and rp > 50 and rc > 50:
                confirmations.append("rsi_agree=1")
                indicators["conf_rsi_agree"] = 1
                conf_feature_true_total.labels(feature="rsi_agree", src="strategy").inc()
            elif direction == "SHORT" and rp < 50 and rc < 50:
                confirmations.append("rsi_agree=1")
                indicators["conf_rsi_agree"] = 1
                conf_feature_true_total.labels(feature="rsi_agree", src="strategy").inc()

            if runtime.last_swing_high:
                sh = runtime.last_swing_high
                indicators.update({
                    "swing_high_ts": int(sh.ts_ms)
                    "swing_high_px": float(sh.price)
                    "swing_high_cvd": float(sh.cvd)
                })
            if runtime.last_swing_low:
                sl = runtime.last_swing_low
                indicators.update({
                    "swing_low_ts": int(sl.ts_ms)
                    "swing_low_px": float(sl.price)
                    "swing_low_cvd": float(sl.cvd)
                })
            if runtime.last_div:
                dv = runtime.last_div
                indicators.update({
                    "div_kind": str(dv.kind)
                    "div_ts": int(dv.ts_ms)
                    "div_strength": float(dv.strength)
                    "div_price_prev": float(dv.price_prev)
                    "div_price_curr": float(dv.price_curr)
                    "div_cvd_prev": float(dv.cvd_prev)
                    "div_cvd_curr": float(dv.cvd_curr)
                })
        except Exception:
            pass

        # Phase C/D: Metadata for Payload (Sweep, Footprint, Weak Progress)
        try:
            ev = runtime.last_sweep
            if ev is not None:
                # v7 structure: sweep metrics
                indicators.setdefault("conf_sweep_eqh", 0)
                indicators.setdefault("conf_sweep_eql", 0)
                
                kind = str(getattr(ev, "kind", "") or "").upper()
                if kind == "EQH_SWEEP":
                    indicators["conf_sweep_eqh"] = 1
                    conf_feature_true_total.labels(feature="sweep_eqh", src="strategy").inc()
                elif kind == "EQL_SWEEP":
                    indicators["conf_sweep_eql"] = 1
                    conf_feature_true_total.labels(feature="sweep_eql", src="strategy").inc()
                
                conf_feature_seen_total.labels(feature="sweep_eqh", src="strategy").inc()
                conf_feature_seen_total.labels(feature="sweep_eql", src="strategy").inc()

                div = runtime.last_div
                div_match = False
                if div is not None:
                    if ev.direction_bias == "SHORT" and str(div.kind).startswith("bearish"):
                        div_match = True
                    if ev.direction_bias == "LONG" and str(div.kind).startswith("bullish"):
                        div_match = True
                indicators["sweep_div_match"] = int(1 if div_match else 0)
                
                # v7 structure
                indicators.setdefault("conf_div_match", 0)
                conf_feature_seen_total.labels(feature="div_match", src="strategy").inc()

                if div_match:
                    confirmations.append("div_match=1")
                    indicators["conf_div_match"] = 1
                    conf_feature_true_total.labels(feature="div_match", src="strategy").inc()

            b = runtime.last_bar
            if b is not None and getattr(b, "fp_enabled", False):
                indicators.update({
                    "fp_bucket_px": float(getattr(b, "fp_bucket_px", 0.0) or 0.0)
                    "fp_max_imbalance": float(getattr(b, "fp_max_imbalance", 0.0) or 0.0)
                    "fp_absorb_score": float(getattr(b, "fp_absorb_score", 0.0) or 0.0)
                })
                fp_confs = fp_confirmations_from_microbar(b, direction, runtime.config)
                for c in fp_confs:
                    confirmations.append(c)
            
            wp = runtime.last_wp
            if wp is not None:
                indicators.update({"weak_range_atr": wp.range_atr, "weak_body_atr": wp.body_atr, "weak_eff": wp.eff})
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Unified data_health score (0..1) + policies
        # ------------------------------------------------------------------
        try:
            # Ensure basic indicators for compute_data_health
            # Sentinel 10**9 when book never arrived — otherwise tick_ts - 0 looks like a fresh tick and misleads gates.
            _last_book_ts_ms = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
            indicators["book_ts_gap_ms"] = int(tick_ts - _last_book_ts_ms) if _last_book_ts_ms > 0 else int(10**9)
            indicators["book_rate_hz"] = float(getattr(runtime, "book_rate_ema", 0.0) or 0.0)
            
            # Use most recent spread from book snapshot if MicroBar hasn't updated yet or ticks lack bid/ask
            spr = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
            if spr <= 0 and runtime.last_book:
                spr = float(runtime.last_book.spread_bps)
            indicators["spread_bps"] = spr

            # ------------------------------------------------------------------
            # Phase C/P2: Book geometry features (slope, DWS, notional bands)
            # Written to indicators so downstream gates / decision snapshot can consume.
            # ------------------------------------------------------------------
            try:
                bids, asks, mid = extract_levels_from_runtime(runtime)
                if mid > 0:
                    slope_bid, slope_ask = calc_book_slope(bids, asks, mid)
                    dws_bps = calc_depth_weighted_spread(bids, asks, mid, xbps=5.0)
                    nb1 = calc_cost_to_cross(bids, mid, xbps=1.0)
                    na1 = calc_cost_to_cross(asks, mid, xbps=1.0)
                    nb5 = calc_cost_to_cross(bids, mid, xbps=5.0)
                    na5 = calc_cost_to_cross(asks, mid, xbps=5.0)

                    indicators["book_slope_bid"] = float(slope_bid)
                    indicators["book_slope_ask"] = float(slope_ask)
                    indicators["dws_bps"] = float(dws_bps)
                    indicators["notional_within_1bp"] = float(nb1 + na1)
                    indicators["notional_within_5bp"] = float(nb5 + na5)
                    indicators["notional_bid_within_1bp"] = float(nb1)
                    indicators["notional_ask_within_1bp"] = float(na1)
                    indicators["notional_bid_within_5bp"] = float(nb5)
                    indicators["notional_ask_within_5bp"] = float(na5)

                    # Liquidity resiliency (recovery time after stress).
                    try:
                        res = getattr(runtime, "liq_resiliency", None)
                        if res is not None:
                            out = res.update(
                                ts_ms=int(tick_ts)
                                spread_bps=float(indicators.get("spread_bps", 0.0) or 0.0)
                                depth_usd=float(indicators.get("notional_within_5bp", 0.0) or 0.0)
                            )
                            indicators.update(out)
                    except Exception:
                        pass

                    # Monitor-only annotations (no veto here).
                    # LiquidityGate/EntryPolicyGate should consume these in monitor mode first.
                    try:
                        flags = []
                        smin = min(float(slope_bid), float(slope_ask))
                        if self._liq_min_slope > 0 and smin > 0 and smin < self._liq_min_slope:
                            flags.append("slope_low")
                        if self._liq_max_dws > 0 and float(dws_bps) > self._liq_max_dws:
                            flags.append("dws_high")
                        rec_ms = float(indicators.get("liq_recovery_time_ms", 0) or 0)
                        if self._liq_max_rec > 0 and rec_ms > self._liq_max_rec:
                            flags.append("recovery_slow")
                        if flags:
                            indicators["liq_geom_monitor_hit"] = 1
                            indicators["liq_geom_flags"] = ",".join(flags)
                        else:
                            indicators.setdefault("liq_geom_monitor_hit", 0)
                    except Exception:
                        pass


                # ------------------------------------------------------------
                # Phase D (P3): Flow toxicity features (OFI normalized by depth)
                # ------------------------------------------------------------
                # We compute a scale-invariant OFI proxy:
                #   ofi_norm = (ofi_best_qty * mid) / (bid_notional_1bp + ask_notional_1bp)
                # and a robust z-score using a bounded RollingRobustZ tracker (runtime.ofi_norm_stats).
                #
                # Why here (not earlier): we already computed near-touch depth notional (nb1/na1).
                # This keeps the metric stable across symbols and market regimes.
                try:
                    from services.orderflow.flow_toxicity import compute_ofi_norm_notional, normal_cdf
                    ofi_qty = float(indicators.get("ofi_best_qty", 0.0) or 0.0)
                    depth1 = float(nb1 + na1)
                    if depth1 <= 0.0:
                        depth1 = float(min(nb5, na5))
                    if ofi_qty != 0.0 and float(mid) > 0.0 and depth1 > 0.0:
                        ofi_norm = compute_ofi_norm_notional(ofi_best_qty=ofi_qty, mid=float(mid), depth_usd_near=float(depth1))
                        indicators["ofi_norm"] = float(ofi_norm)
                        indicators["ofi_norm_depth_usd_1bp"] = float(depth1)
                        try:
                            runtime.ofi_norm_stats.update(float(ofi_norm))
                            z = float(runtime.ofi_norm_stats.z(float(ofi_norm)))
                            runtime.ofi_norm_z = float(z)
                            indicators["ofi_norm_z"] = float(z)
                        except Exception:
                            indicators.setdefault("ofi_norm_z", float(getattr(runtime, "ofi_norm_z", 0.0) or 0.0))
                    else:
                        indicators.setdefault("ofi_norm", 0.0)
                        indicators.setdefault("ofi_norm_z", float(getattr(runtime, "ofi_norm_z", 0.0) or 0.0))

                    # Optional VPIN-like toxicity proxy from L3-lite tracker (if enabled)
                    # Stored as z-score + CDF in [0..1] for easy thresholding.
                    vz = 0.0
                    try:
                        l3s = getattr(runtime, "l3_stats", None)
                        if l3s is not None:
                            vz = float(getattr(l3s, "vpin_tox_z", 0.0) or 0.0)
                    except Exception:
                        vz = 0.0
                    indicators["vpin_tox_z"] = float(vz)
                    indicators["vpin_cdf"] = float(normal_cdf(float(vz)))
                except Exception:
                    pass

                # --------------------------------------------------------
                # Phase E / P4: Manipulation pattern indicators (hot-path)
                # Sourced from runtime.msg_rate + runtime.manip (updated
                # by book_processor on every book snapshot).
                # All fields default to 0 / "" on missing runtime attrs.
                # --------------------------------------------------------
                try:
                    indicators["book_update_rate_hz"] = float(getattr(runtime, "book_update_rate_hz", 0.0) or 0.0)
                    indicators["book_update_rate_z"] = float(getattr(runtime, "book_update_rate_z", 0.0) or 0.0)
                    indicators["trade_msg_rate_hz"] = float(getattr(runtime, "trade_msg_rate_hz", 0.0) or 0.0)
                    indicators["trade_msg_rate_z"] = float(getattr(runtime, "trade_msg_rate_z", 0.0) or 0.0)
                    indicators["cancel_rate_z"] = float(getattr(runtime, "cancel_rate_z", 0.0) or 0.0)
                    indicators["otr"] = float(getattr(runtime, "otr", 0.0) or 0.0)
                    indicators["otr_z"] = float(getattr(runtime, "otr_z", 0.0) or 0.0)
                    indicators["quote_stuffing_score"] = float(getattr(runtime, "quote_stuffing_score", 0.0) or 0.0)
                    indicators["layering_score"] = float(getattr(runtime, "layering_score", 0.0) or 0.0)
                    indicators["manip_flags"] = str(getattr(runtime, "manip_flags", "") or "")

                    # strict/hard tighten: increase expected_slippage_bps when manipulation detected
                    # (MANIP_MODE=auto + GATE_PROFILE: strict→tighten, hard→veto)
                    # This is the "tighten" arm; veto is done in signal_pipeline.
                    try:
                        should_tighten = (
                            (self._manip_mode == "tighten") or
                            (self._manip_mode == "auto" and self._gate_profile in ("strict", "hard"))
                        )
                        if should_tighten and indicators["manip_flags"]:
                            manip_score = max(
                                float(indicators.get("quote_stuffing_score", 0.0) or 0.0)
                                float(indicators.get("layering_score", 0.0) or 0.0)
                            )
                            if manip_score > 0.0:
                                add_bps = float(min(self._manip_tighten_cap, manip_score * self._manip_tighten_mult * 3.0))  # 3 bps/score unit
                                exp0 = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                                indicators["expected_slippage_bps"] = exp0 + add_bps
                                indicators["manip_tighten_add_bps"] = add_bps
                    except Exception:
                        pass
                except Exception:
                    pass

            except Exception:
                pass
            
            if (runtime.symbol == "ETHUSDT" or "PEPE" in runtime.symbol):
                # Sample every 10000th message to reduce log spam
                spread_debug_sampler = LogSamplerFactory.get_sampler("DEBUG_SPREAD", 10000)
                if spread_debug_sampler.should_log(f"spread_debug_{runtime.symbol}"):
                    self.logger.warning("📊 [DEBUG-SPREAD] (%s) FINAL INDICATOR: spread_bps=%.4f (src=%s)", 
                                        runtime.symbol, indicators["spread_bps"], 
                                        "microbar" if runtime.last_spread_bps > 0 else "l2_snap")
            
            dh = compute_data_health(indicators=indicators, cfg=cfg)
            indicators[IK.DATA_HEALTH] = float(dh.score)
            indicators[IK.DATA_HEALTH_REASONS] = ",".join(list(dh.reasons or [])[:5])
            indicators[IK.BOOK_HEALTH_OK] = int(dh.book_health_ok)
            apply_book_evidence_policy(indicators=indicators, dh=dh, cfg=cfg)
            apply_shadow_only_policy(indicators=indicators, dh=dh, cfg=cfg)
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Expected slippage model (bps) for adverse selection filtering
        # ------------------------------------------------------------------
        # CRITICAL: avoid missing/zero slippage when model fails
        indicators.setdefault("expected_slippage_bps", 0.0)
        indicators.setdefault("slippage_reason", "na")

        # --- OFI impact proxy from best-level book changes (Cont et al.) ---
        # Produces: ofi_best_qty, ofi_best_norm, depth_top5_qty (best-effort)
        try:
            book = getattr(runtime, 'last_book', None)
            prev = getattr(runtime, '_ofi_prev_book', None)
            if book is not None:
                def _get(obj, k, d=0.0):
                    try:
                        if obj is None: return d
                        if isinstance(obj, dict): return float(obj.get(k, d) or d)
                        return float(getattr(obj, k, d) or d)
                    except Exception:
                        return d
                # best bid/ask (supports BookSnapshot or dict)
                bbp = _get(book, 'best_bid_px', 0.0)
                bbq = _get(book, 'best_bid_qty', 0.0)
                bap = _get(book, 'best_ask_px', 0.0)
                baq = _get(book, 'best_ask_qty', 0.0)
                p_bbp = _get(prev, 'best_bid_px', 0.0)
                p_bbq = _get(prev, 'best_bid_qty', 0.0)
                p_bap = _get(prev, 'best_ask_px', 0.0)
                p_baq = _get(prev, 'best_ask_qty', 0.0)
                # OFI formula (best-level, snapshot-based approximation)
                ofi_bid = 0.0
                if bbp > p_bbp and bbp > 0: ofi_bid = bbq
                elif bbp < p_bbp and p_bbp > 0: ofi_bid = -p_bbq
                elif bbp == p_bbp and bbp > 0: ofi_bid = (bbq - p_bbq)
                ofi_ask = 0.0
                if bap < p_bap and bap > 0: ofi_ask = -baq
                elif bap > p_bap and p_bap > 0: ofi_ask = p_baq
                elif bap == p_bap and bap > 0: ofi_ask = -(baq - p_baq)
                ofi = ofi_bid + ofi_ask
                # depth (qty) from top5 if available
                d_b = _get(book, 'depth_5_bid_vol', 0.0)
                d_a = _get(book, 'depth_5_ask_vol', 0.0)
                depth = float(d_b + d_a)
                if depth <= 0:
                    try:
                        bids = book.get('bids') if isinstance(book, dict) else getattr(book, 'bids', None)
                        asks = book.get('asks') if isinstance(book, dict) else getattr(book, 'asks', None)
                        if bids: depth += sum(float(x[1]) for x in bids[:5] if x and len(x)>=2)
                        if asks: depth += sum(float(x[1]) for x in asks[:5] if x and len(x)>=2)
                    except Exception:
                        pass
                norm = float(ofi / max(depth, 1e-9))
                indicators['ofi_best_qty'] = float(ofi)
                indicators['depth_top5_qty'] = float(depth)
                indicators['ofi_best_norm'] = float(norm)
                runtime._ofi_prev_book = book
        except Exception:
            pass

        # --- ATR meta & sanity flags (fail-open trading; fail-closed evidence) ---
        # if you have atr_cache.get_with_meta() use it; otherwise keep your current atr read
        try:
            from utils.atr_cache import get_atr_cache
            atr_cache = get_atr_cache()
            atr_val, atr_meta = atr_cache.get_with_meta(symbol=runtime.symbol, timeframe=None)  # None => use cfg:atr_tf:{sym}
            if atr_val is not None and float(atr_val) > 0:
                indicators["atr"] = float(atr_val)
            # Don't set indicators["atr"] if atr_val is None or <= 0 - let sanity check handle it
            indicators["atr_src"] = str(atr_meta.get("picked_src") or atr_meta.get("src") or "na")
            indicators["atr_tf"] = str(atr_meta.get("picked_tf") or atr_meta.get("tf") or "na")
            indicators["atr_age_ms"] = int(atr_meta.get("age_ms") or 0)
            indicators["atr_ts_ms"] = int(atr_meta.get("ts_ms") or 0)
        except Exception:
            indicators.setdefault("atr_src", str(getattr(runtime, "atr_src", "na")))
            indicators.setdefault("atr_tf", str(getattr(runtime, "atr_tf", "na")))
            indicators.setdefault("atr_age_ms", int(getattr(runtime, "atr_age_ms", 0) or 0))
            indicators.setdefault("atr_ts_ms", int(getattr(runtime, "last_atr_ts_ms", 0) or 0))

        # Full robust sanity + last-good fallback (fail-open for trading)
        try:
            if self._atr_sanity_enable:
                px0 = float(price or indicators.get("price", 0.0) or 0.0)
                # Get ATR from indicators if set, otherwise from runtime.last_atr, but don't default to 0.0
                # If ATR is None or not set, use runtime.last_atr if available, otherwise 0.0 (will be caught by sanity check)
                atr_from_indicators = indicators.get("atr")
                if atr_from_indicators is not None:
                    atr0 = float(atr_from_indicators)
                else:
                    atr0 = float(getattr(runtime, "last_atr", 0.0) or 0.0)
                age0 = int(indicators.get("atr_age_ms", 0) or 0)
                now_ms = int(indicators.get("now_ts_ms", 0) or tick_ts)  # только Event Time

                res = self._atr_sanity.update(
                    symbol=str(runtime.symbol)
                    atr=float(atr0)
                    px=float(px0)
                    age_ms=int(age0)
                    now_ms=int(now_ms)
                )

                # Use sanitized ATR for downstream gates/tiers/levels
                indicators["atr"] = float(res.atr_used)
                indicators["atr_bad"] = int(res.bad)
                indicators["atr_bad_reason"] = str(res.reason or "")
                indicators["atr_used_last_good"] = int(res.used_last_good)
                indicators["atr_jump_count_window"] = int(getattr(res, "jump_count_window", 0) or 0)

                # Записываем мониторинг-ключи через MetricsBatcher
                # (неблокирующий bounded queue — безопасен в hot path)
                try:
                    if int(res.bad) == 1:
                        ttl = self._atr_bad_ttl_sec
                        reason = str(res.reason or "na")
                        # Write JSON (not bare "1") so alert worker can display the reason
                        _atr_bad_payload = json.dumps({"reason": reason, "ts_ms": int(now_ms)}, ensure_ascii=False)
                        self._mbatch.put("set", f"cfg:atr_bad:{runtime.symbol}", _atr_bad_payload, ex=ttl)
                        self._mbatch.put("sadd", "cfg:atr_bad:symbols", str(runtime.symbol))
                        self._mbatch.put("expire", "cfg:atr_bad:symbols", self._atr_bad_symbols_set_ttl_sec)
                        self._mbatch.put("hincrby", f"metrics:atr_bad_total:{runtime.symbol}", reason, 1)
                        self._mbatch.put("expire", f"metrics:atr_bad_total:{runtime.symbol}", self._metrics_counter_ttl_sec)
                    # Jump window counters
                    if int(getattr(res, "jump_event", 0) or 0) == 1:
                        win = self._atr_jump_window_sec
                        self._mbatch.put("incr", f"cfg:atr_jump_count:{runtime.symbol}")
                        self._mbatch.put("expire", f"cfg:atr_jump_count:{runtime.symbol}", win)
                        self._mbatch.put("sadd", "cfg:atr_jump:symbols", str(runtime.symbol))
                        self._mbatch.put("expire", "cfg:atr_jump:symbols", self._atr_jump_symbols_set_ttl_sec)
                        self._mbatch.put("incr", f"metrics:atr_jump_total:{runtime.symbol}")
                        self._mbatch.put("expire", f"metrics:atr_jump_total:{runtime.symbol}", self._metrics_counter_ttl_sec)
                except Exception:
                    pass
            else:
                indicators.setdefault("atr_bad", 0)
                indicators.setdefault("atr_bad_reason", "")
                indicators.setdefault("atr_used_last_good", 0)
                indicators.setdefault("atr_jump_count_window", 0)
        except Exception:
            indicators.setdefault("atr_bad", 0)
            indicators.setdefault("atr_bad_reason", "")
            indicators.setdefault("atr_used_last_good", 0)
            indicators.setdefault("atr_jump_count_window", 0)

        # CVD quarantine (0/1) + fallback mode
        indicators["cvd_quarantine_active"] = int(getattr(runtime, "cvd_quarantine_active", 0) or indicators.get("cvd_quarantine_active", 0) or 0)
        indicators.setdefault(
            "delta_fallback_mode"
            str(getattr(runtime, "delta_fallback_mode", "") or ("volume" if indicators["cvd_quarantine_active"] else "cvd"))
        )
        # Best-effort meta for reporting (reason/ttl)
        try:
            indicators.setdefault("cvd_quarantine_until_ms", int(getattr(runtime, "cvd_quarantine_until_ms", 0) or indicators.get("cvd_quarantine_until_ms", 0) or 0))
            indicators.setdefault("cvd_quarantine_reason", str(getattr(runtime, "cvd_quarantine_reason", "") or indicators.get("cvd_quarantine_reason", "") or ""))
        except Exception:
            pass

        # Persist quarantine meta for Telegram health reporter
        # Keys:
        #   cfg:cvd_quarantine_meta:{sym} = JSON {until_ms, reason, mode, ts_ms}
        #   cfg:cvd_quarantine:symbols = set of active quarantine symbols
        try:
            if int(indicators.get("cvd_quarantine_active", 0) or 0) == 1:
                now_ms = int(indicators.get("now_ts_ms", 0) or tick_ts)  # только Event Time
                until_ms = int(indicators.get("cvd_quarantine_until_ms", 0) or 0)
                reason = str(indicators.get("cvd_quarantine_reason", "") or "")
                mode = str(indicators.get("delta_fallback_mode", "") or "volume")
                ttl_sec = 900
                if until_ms > now_ms:
                    ttl_sec = max(60, int((until_ms - now_ms) / 1000))
                meta = {"until_ms": until_ms, "reason": reason, "mode": mode, "ts_ms": now_ms}
                # Пишем через MetricsBatcher (bounded, неблокирующий)
                self._mbatch.put("set", f"cfg:cvd_quarantine_meta:{runtime.symbol}", json.dumps(meta, ensure_ascii=False), ex=ttl_sec)
                self._mbatch.put("sadd", "cfg:cvd_quarantine:symbols", str(runtime.symbol))
                self._mbatch.put("expire", "cfg:cvd_quarantine:symbols", self._cvd_quar_symbols_set_ttl_sec)
                self._mbatch.put("incr", f"metrics:cvd_quarantine_activations_total:{runtime.symbol}")
                self._mbatch.put("expire", f"metrics:cvd_quarantine_activations_total:{runtime.symbol}", self._metrics_counter_ttl_sec)
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Volume-delta fallback: if CVD is quarantined, compute delta_z from signed trade volume
        # (protects against broken baselines / offset jumps). Deterministic, robust.
        # ------------------------------------------------------------------
        delta_z_used = float(delta_event.get("z", 0.0) if isinstance(delta_event, dict) else 0.0)
        try:
            if int(indicators.get("cvd_quarantine_active", 0) or 0) == 1:
                from core.delta_volume_fallback import volume_delta_z_from_tick
                dz, d_raw = volume_delta_z_from_tick(runtime, tick)
                delta_z_used = float(dz if dz is not None else delta_z_used)
                # unify downstream: override delta_event + indicators when in fallback
                if isinstance(delta_event, dict):
                    delta_event["z"] = float(delta_z_used)
                    delta_event["raw"] = float(d_raw)
                    delta_event["mode"] = "volume_fallback"
                indicators["delta_tick"] = float(d_raw)
                indicators["delta_z"] = float(delta_z_used)
                indicators["delta_fb_raw"] = float(d_raw)
                indicators["delta_fb_z"] = float(delta_z_used)
                indicators["delta_z_source"] = "volume_fallback"
            else:
                indicators["delta_z_source"] = "cvd"
        except Exception:
            indicators.setdefault("delta_z_source", "cvd")

        try:
            spr = float(indicators.get("spread_bps", 0.0) or 0.0)
            churn = float(getattr(runtime, "book_churn_score", 0.0) or 0.0)
            brz = float(getattr(runtime, "book_rate_z", 0.0) or 0.0)
            press = float(getattr(runtime, "pressure_sps", 0.0) or 0.0)
            # Fetch ATR bps if available
            px = float(price or indicators.get("price", 0.0) or 0.0)
            atr = float(indicators.get("atr", getattr(runtime, "last_atr", 0.0)) or 0.0)
            atr_bps = (atr / px * 10000.0) if (px > 0 and atr > 0) else 0.0
            indicators["atr_bps"] = float(atr_bps)
            
            est = expected_slippage_bps(
                spread_bps=spr
                churn_score=churn
                book_rate_z=brz
                pressure_sps=press
                atr_bps=atr_bps
                cfg=cfg
            )
            indicators["expected_slippage_bps"] = float(est.expected_bps)
            indicators["slippage_reason"] = str(est.reason)
            # Optional OFI add-on: convert best-level OFI into extra impact bps
            # Default k=0 => disabled. Enable via cfg['slip_ofi_k'] or env SLIP_OFI_K.
            try:
                k = float(cfg.get('slip_ofi_k', os.getenv('SLIP_OFI_K', '0.0')) or 0.0)
                if k > 0:
                    impact = float(k) * abs(float(indicators.get('ofi_best_norm', 0.0) or 0.0))
                    if impact > 0:
                        indicators['expected_slippage_bps'] = float(indicators.get('expected_slippage_bps', 0.0) or 0.0) + impact
                        indicators['slippage_reason'] = str(indicators.get('slippage_reason', 'na') or 'na') + f'|ofi+{impact:.3f}'
            except Exception:
                pass
        except Exception:
            # keep setdefault() values above
            pass

        # ------------------------------------------------------------
        # OFConfirm Engine (single source of truth for decision & score)
        # ------------------------------------------------------------
        try:
            # absorption = absorption_feat (computed earlier)
            absorption = absorption_feat
            # Robust gate using pre-computed health (lines 1728+)
            book_ok = int(indicators.get("book_health_ok", 1))
            book_health = str(indicators.get("book_health", "OK"))
            
            # Additional check: explicitly verify threshold from dynamic config (OR logic)
            try:
                # Условие прохода: book_ts_gap_ms < book_stale_ms ИЛИ book_rate_hz >= book_rate_min_hz
                br = float(indicators.get("book_rate_hz", 0.0))
                min_hz = float(runtime.dynamic_cfg.get(DK.BOOK_RATE_MIN_HZ, 0.0))
                book_gap = int(indicators.get("book_ts_gap_ms", 999999))
                book_stale_ms = int(runtime.config.get("book_stale_ms", 15000))
                has_book = int(getattr(runtime, "last_book_ts_ms", 0) or 0) > 0
                
                gap_ok = (book_gap < book_stale_ms)
                rate_ok = (min_hz > 0 and br >= min_hz)
                
                if has_book and (gap_ok or rate_ok):
                    book_ok = 1
                    indicators["book_health_ok"] = 1
                    indicators["book_health"] = "OK"
                else:
                    book_ok = 0
                    indicators["book_health_ok"] = 0
                    if not has_book:
                        indicators["book_health"] = "NO_BOOK"
                    elif not gap_ok and not rate_ok:
                        indicators["book_health"] = "STALE_AND_LOW_RATE"

                # P0 audit fix: Explicitly export back to runtime so `decision_snapshot`
                # captures the true book_health (previously it would just read the default 'OK')
                runtime.last_book_health_ok = indicators["book_health_ok"]
                runtime.last_book_health = indicators["book_health"]

                # P1 audit fix: Update observability metrics
                _st = indicators["book_health"]
                book_health_state_gauge.labels(symbol=runtime.symbol, state="OK").set(1 if _st == "OK" else 0)
                book_health_state_gauge.labels(symbol=runtime.symbol, state="NO_BOOK").set(1 if _st == "NO_BOOK" else 0)
                book_health_state_gauge.labels(symbol=runtime.symbol, state="STALE_AND_LOW_RATE").set(1 if _st == "STALE_AND_LOW_RATE" else 0)
                if has_book:
                    book_ts_gap_ms_hist.labels(symbol=runtime.symbol).observe(book_gap)

            except Exception:
                pass

            
            if book_ok == 0:
                of_session_outcome_total.labels(runtime.symbol, sess, "veto_book_stale").inc()
                # Stale or Unhealthy -> Disable Microstructure Evidence
                # We do NOT return None (fail-close for signal), but we zero-out 
                # book-dependent evidence so OFConfirmEngine sees "no evidence".
                indicators["obi"] = 0
                indicators["iceberg_refresh"] = 0
                indicators["iceberg_avg_qty"] = 0
                
                # Verify removal of any other book-dependent components if needed? 
                # Currently these are the main ones feeding score.
                
                # Check for debug logs
                if self._debug_deltas:
                     logger.debug("⚠️ (%s) Book Health Fail: %s (OBI/Iceberg disabled)", runtime.symbol, book_health)
            
            # --- PRESSURE PROXY LAYER START ---
            # 1. Update meters
            # Note: We do NOT add tick_ts to pressure here. Pressure tracks *candidates*, recorded later.
            
            # 2. Compute metrics
            p_snap = runtime.pressure.snapshot(now_ms=int(tick_ts))
            pres_per_min = float(p_snap.per_min_ema)
            cd_per_min = float(p_snap.cd_rate_ema)
            
            hit_rate = cd_per_min # It's already an EMA rate

            runtime.last_pressure_per_min = pres_per_min
            runtime.last_cd_hit_rate = hit_rate
            indicators[IK.PRESSURE_PER_MIN] = pres_per_min
            indicators[IK.COOLDOWN_HIT_RATE] = hit_rate

            # 3. Dynamic Thresholds
            p_hi = float(runtime.config.get("pressure_hi_per_min", 0.0) or 0.0)
            p_ext = float(runtime.config.get("pressure_extreme_per_min", 0.0) or 0.0)
            
            pressure_hi = int(p_hi > 0 and pres_per_min >= p_hi)
            pressure_extreme = int(p_ext > 0 and pres_per_min >= p_ext)
            
            runtime.dynamic_cfg[DK.PRESSURE_PER_MIN] = pres_per_min
            runtime.dynamic_cfg[DK.PRESSURE_HI] = pressure_hi
            runtime.dynamic_cfg[DK.PRESSURE_EXTREME] = pressure_extreme
            indicators[IK.PRESSURE_HI_FLAG] = pressure_hi
            indicators[IK.PRESSURE_EXTREME_FLAG] = pressure_extreme

            # 4. Strictness escalation (Need=3)
            # If pressure is high, increase required confirmations (reversal/continuation need -> 3)
            # Only if strong_dynamic_need_enable=1 (default)
            if bool(int(runtime.config.get("strong_dynamic_need_enable", 1))):
                # [EXPERT] Fix drift: always base on static config values instead of cumulative dynamic state
                base_r = int(runtime.config.get("strong_need_reversal", 2) or 2)
                base_c = int(runtime.config.get("strong_need_continuation", 2) or 2)
                need_r = base_r
                need_c = base_c

                if pressure_hi or pressure_extreme:
                    need_r = max(need_r, 3)
                    need_c = max(need_c, 3)
                    indicators[IK.STRONG_NEED_REASON] = "pressure"
                else:
                    indicators[IK.STRONG_NEED_REASON] = "base"

                runtime.dynamic_cfg[DK.STRONG_NEED_REVERSAL] = int(need_r)
                runtime.dynamic_cfg[DK.STRONG_NEED_CONTINUATION] = int(need_c)
            
            # --- Delta-notional tier gate (AUTHORITATIVE: dn_calib via dynamic_cfg) ---
            tiers_cfg = runtime.config.get("delta_diff_tiers") or get_default_delta_tiers(runtime.symbol)

            rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
            tier_idx = 0 if "trend" in rg else 1
            # Escalation by pressure flags (telemetry-only inputs; dn thresholds remain dn_calib)
            if int(runtime.dynamic_cfg.get(DK.PRESSURE_HI, 0) or 0) == 1:
                tier_idx = min(tier_idx + 1, 2)
            if int(runtime.dynamic_cfg.get(DK.PRESSURE_EXTREME, 0) or 0) == 1:
                tier_idx = 2

            tier_key = f"tier{tier_idx}"

            # Read ONLY canonical dn_calib keys; fallback to defaults
            th = float(runtime.dynamic_cfg.get(f"dn_tier{tier_idx}_usd", 0.0) or 0.0)
            if th <= 0:
                th = float(tiers_cfg.get(tier_key, tiers_cfg.get("tier1", 100000.0)))

            notional_usd = abs(float(delta_event.get("delta", 0.0))) * float(price)
            indicators[IK.DELTA_NOTIONAL_USD] = float(notional_usd)
            indicators[IK.DN_TIER_ACTIVE] = int(tier_idx)
            indicators[IK.DN_TIER_THRESHOLD] = float(th)

            sess = session_utc(int(tick_ts))

            if th > 1.0 and notional_usd < th:
                # EXPERT RELAXATION (2026-01-30): Consistent with main DN-GATE
                from core.instrument_config import symbol_env_prefix
                prefix = symbol_env_prefix(runtime.symbol)
                is_meme = prefix in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF")
                
                if is_meme and notional_usd >= th * 0.50:
                    # Log every 10,000th message
                    cnt = self.dn_gate_proxy_relaxed_counters.get(runtime.symbol, 0) + 1
                    self.dn_gate_proxy_relaxed_counters[runtime.symbol] = cnt
                    if cnt % 10000 == 0:
                        logger.info("✅ [DN-GATE-PROXY] (%s) RELAXED: notional_usd=$%.2f passed via 50%% tolerance (th=$%.2f) (x%d)", 
                                    runtime.symbol, notional_usd, th, cnt)
                else:
                    ticks_pressure_filtered_total.labels(symbol=runtime.symbol, reason=tier_key).inc()
                    dn_gate_events_total.labels(symbol=runtime.symbol, tier=str(tier_idx), session=sess, result="veto").inc()
                    sampled_warning(
                        logger
                        "DN_FILTERED"
                        "🛑 (%s) Notional Veto: $%.2f < threshold $%.2f (tier=%s)"
                        runtime.symbol
                        notional_usd
                        th
                        tier_key
                    )
                    return None
            dn_gate_events_total.labels(symbol=runtime.symbol, tier=str(tier_idx), session=sess, result="pass").inc()
            # --- PRESSURE PROXY LAYER END ---

            # Merge static cfg + dynamic calibrated thresholds
            cfg2 = dict(runtime.config)
            try:
                dyn = getattr(runtime, "dynamic_cfg", {}) or {}
                if bool(int(cfg2.get("abs_lvl_use_dynamic_th", 1))):
                    cfg2.update(dyn)
                else:
                    indicators["abs_lvl_dynamic_disabled"] = 1
            except Exception:
                pass

            try:
                # readiness gate
                min_samples = int(cfg2.get("eff_calib_min_samples", cfg2.get("EFF_CALIB_MIN_SAMPLES", 300)) or 300)
                calib_n = int(cfg2.get("abs_lvl_calib_n", 0) or 0)
                calib_src = str(cfg2.get("abs_lvl_calib_src", "static") or "static")
                abs_ready = int((calib_n >= min_samples) and (calib_src != "static"))
                
                # safety switch: unstable -> disable ready
                if int(cfg2.get("abs_lvl_th_unstable", 0) or 0) == 1:
                    abs_ready = 0
                    indicators["abs_lvl_disabled_by_unstable"] = 1
                    
                cfg2["abs_lvl_calib_ready"] = abs_ready
                indicators["abs_lvl_ready"] = abs_ready
            except Exception:
                pass
                
            # Continuation context update: if this spike is counter-trend + weak progress, record it.
            # This enables Bit C in eval_continuation for future trend-aligned signals.
            try:
                div_k = getattr(runtime.last_div, "kind", None) if runtime.last_div else None
                t_dir = hidden_trend_dir(div_k)
                if t_dir is None:
                    rg = str(getattr(runtime, 'last_regime', 'na') or 'na').lower()
                    if "bull" in rg: t_dir = "LONG"
                    elif "bear" in rg: t_dir = "SHORT"
                
                if t_dir is not None and direction != t_dir:
                    if runtime.last_wp and runtime.last_wp.weak_any:
                        runtime.cont_ctx_ts_ms = now_ts
                        runtime.cont_ctx_trend_dir = t_dir
            except Exception:
                pass

            # Continuation veto logic
            try:
                div_k = getattr(runtime.last_div, "kind", None) if runtime.last_div else None
                t_dir = hidden_trend_dir(div_k)
                if t_dir is None:
                    rg = str(getattr(runtime, 'last_regime', 'na') or 'na').lower()
                    if "bull" in rg: t_dir = "LONG"
                    elif "bear" in rg: t_dir = "SHORT"
                
                veto_th = float(cfg2.get("abs_lvl_cont_veto_score", 0.75))
                abs_bias = str(indicators.get("abs_lvl_bias", "NONE") or "NONE").upper()
                abs_score = float(indicators.get("abs_lvl_score", 0.0) or 0.0)
                if int(indicators.get("abs_lvl_ready", 0)) == 1 and t_dir is not None:
                    if abs_bias in ("LONG","SHORT") and abs_bias != str(t_dir).upper() and abs_score >= veto_th:
                        indicators["abs_lvl_cont_veto"] = 1
            except Exception:
                pass

            # Threshold and weighting overrides: relax 0.65 -> 0.60 (updated from 0.45)
            cfg2["of_score_min"] = float(cfg2.get("of_score_min", os.getenv("OF_SCORE_MIN", "0.60")))

            # Divergence Sensitivity
            cfg2["div_strength_min"] = float(cfg2.get("div_strength_min", 1.5))
            cfg2["div_min_price_bp"] = float(cfg2.get("div_min_price_bp", 3.0))
            if hasattr(runtime, "divergence") and runtime.divergence:
                runtime.divergence.apply_config(cfg2)

            # --- L3-lite (Cancellation rates for OFConfirm engine) ---
            if runtime.l3_stats:
                indicators["cancel_bid_rate_ema"] = float(runtime.l3_stats.cancel_bid_rate_ema)
                indicators["cancel_ask_rate_ema"] = float(runtime.l3_stats.cancel_ask_rate_ema)
                indicators["taker_buy_rate_ema"] = float(runtime.l3_stats.taker_buy_rate_ema)
                indicators["taker_sell_rate_ema"] = float(runtime.l3_stats.taker_sell_rate_ema)
                indicators["trade_intensity"] = float(runtime.l3_stats.taker_buy_rate_ema) + float(runtime.l3_stats.taker_sell_rate_ema)

            # Hawkes burst features (computed on bucket advance; fail-open if missing)
            hsnap = getattr(runtime, "hawkes_snapshot", None)
            if isinstance(hsnap, dict):
                indicators.update(hsnap)

            # --- Fail-open fix: spread/slippage must not silently be 0 ---
            # Guarantee spread_bps and expected_slippage_bps (not zeros silently).
            # Three failure modes are explicitly handled here:
            # 1. Crossed BBO → book_processor guards against 0-write; see book_processor.py.
            # 2. Stale book (go-worker frozen) → last_spread_bps_l2 keeps old value indefinitely;
            #    we skip it once book_ts_gap_ms > SPREAD_STALE_BOOK_GAP_MS.
            # 3. Cold-start race (python-worker restarted before first L2 snapshot arrives) →
            #    suppress data_health penalty for SPREAD_MISSING_COLD_START_MS.
            try:
                # Tune-able via config/ENV (defaults: 30s stale, 10s cold-start grace)
                _stale_ms = int(cfg2.get(
                    "spread_stale_book_gap_ms"
                    int(os.getenv("SPREAD_STALE_BOOK_GAP_MS", "30000"))
                ))
                _cold_start_ms = int(cfg2.get(
                    "spread_missing_cold_start_ms"
                    int(os.getenv("SPREAD_MISSING_COLD_START_MS", "10000"))
                ))

                # --- Staleness check: how long since the last book snapshot? ---
                _book_ts_gap = int(indicators.get("book_ts_gap_ms", 0) or 0)
                # sentinel 10**9 means book was never seen (cold start)
                _book_never_seen = _book_ts_gap >= int(10**8)
                _book_stale = (not _book_never_seen) and (_book_ts_gap > _stale_ms)

                # --- Cold-start check: does runtime have a first-book timestamp? ---
                _first_book_ts = int(getattr(runtime, "first_book_ts_ms", 0) or 0)
                _in_cold_start = _book_never_seen and (
                    _first_book_ts <= 0 or (int(tick_ts) - _first_book_ts) < _cold_start_ms
                )

                spr = float(indicators.get("spread_bps", 0.0) or 0.0)
                if spr <= 0:
                    # Use last_spread_bps_l2 only when book is live AND not stale
                    if not _book_stale and not _book_never_seen:
                        spr = float(getattr(runtime, "last_spread_bps_l2", 0.0) or 0.0)
                    else:
                        indicators["spread_bps_stale_book"] = 1
                if spr <= 0:
                    spr = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
                if spr <= 0:
                    # liq_spread_bps is computed from BBO (best_bid/best_ask) earlier in this tick
                    spr = float(indicators.get(IK.LIQ_SPREAD_BPS, 0.0) or 0.0)
                if spr <= 0:
                    spr = float(cfg2.get("spread_bps_missing_default", SPREAD_BPS_MISSING_DEFAULT))
                    indicators["spread_bps_missing"] = 1
                    # Suppress data_health penalty during cold-start grace period to avoid
                    # dh_bad storms right after each python-worker restart.
                    if not _in_cold_start:
                        # degrade data_health so book-evidence is vetoed downstream
                        dh = float(indicators.get(IK.DATA_HEALTH, 1.0) or 1.0)
                        indicators[IK.DATA_HEALTH] = min(dh, float(cfg2.get("data_health_on_spread_missing", DATA_HEALTH_ON_SPREAD_MISSING)))
                        r_str = str(indicators.get(IK.DATA_HEALTH_REASONS, ""))
                        indicators[IK.DATA_HEALTH_REASONS] = (r_str + ",spread_missing") if r_str else "spread_missing"
                        indicators[IK.BOOK_HEALTH_OK] = 0
                    else:
                        # Cold start: annotate with milder reason, do NOT degrade data_health
                        r_str = str(indicators.get(IK.DATA_HEALTH_REASONS, ""))
                        indicators[IK.DATA_HEALTH_REASONS] = (r_str + ",spread_cold_start") if r_str else "spread_cold_start"
                        indicators["spread_bps_cold_start"] = 1
                indicators[IK.LIQ_SPREAD_BPS] = float(spr)

                if "expected_slippage_bps" not in indicators or float(indicators.get("expected_slippage_bps", 0.0) or 0.0) <= 0:
                    indicators["expected_slippage_bps"] = float(cfg2.get("expected_slippage_bps_missing_default", SLIPPAGE_BPS_MISSING_DEFAULT))
                    indicators["expected_slippage_missing"] = 1
            except Exception:
                pass

            # Propagate sid for deterministic canary-share ENFORCE
            # Prefer stable sid from signal pipeline or generate deterministic one
            sid = str(indicators.get("sid", "") or indicators.get("stable_sid", "") or "")
            if not sid:
                # Generate deterministic sid for this signal candidate
                sid = f"{runtime.symbol}|{tick_ts}|{direction}|{scenario if 'scenario' in locals() else 'unknown'}"
            indicators["sid"] = sid

            # ------------------------------------------------------------------
            # Persist anomaly keys for reporters (best-effort, async)
            # ------------------------------------------------------------------
            try:
                ttl = int(os.getenv("REPORT_KEYS_TTL_SEC", "7200"))
                sym = str(runtime.symbol or "").upper()
                if sym:
                    # ATR bad keys
                    if int(indicators.get("atr_bad", 0) or 0) == 1:
                        o = {
                            "ts_ms": int(tick_ts or 0)
                            "atr_age_ms": int(indicators.get("atr_age_ms", 0) or 0)
                            "atr_bps": float(indicators.get("atr_bps", 0.0) or 0.0)
                            "reason": str(indicators.get("atr_bad_reason", "") or "")
                        }
                        # БАТЧ
                        self._mbatch.put("set", f"cfg:atr_bad:{sym}", json.dumps(o, ensure_ascii=False), ex=ttl)
                        sset = os.getenv("ATR_BAD_SYMBOLS_SET", "cfg:atr_bad:symbols")
                        self._mbatch.put("sadd", sset, sym)
                        self._mbatch.put("expire", sset, ttl)

                    # CVD quarantine keys
                    if int(indicators.get("cvd_quarantine_active", 0) or 0) == 1:
                        until_ms = int(indicators.get("cvd_quarantine_until_ms", 0) or getattr(runtime, "cvd_quarantine_until_ms", 0) or 0)
                        o = {
                            "ts_ms": int(tick_ts or 0)
                            "until_ms": int(until_ms)
                            "reason": str(indicators.get("cvd_quarantine_reason", "") or "")
                        }
                        # БАТЧ
                        self._mbatch.put("set", f"cfg:cvd_quarantine:{sym}", json.dumps(o, ensure_ascii=False), ex=ttl)
                        sset = os.getenv("CVD_Q_SYMBOLS_SET", "cfg:cvd_quarantine:symbols")
                        self._mbatch.put("sadd", sset, sym)
                        self._mbatch.put("expire", sset, ttl)
            except Exception:
                pass

            # Capture inputs for golden replay (fail-open, sampled)
            CAP = os.getenv("OFC_CAPTURE_ENABLE", "0") == "1"
            CAP_EVERY = int(os.getenv("OFC_CAPTURE_EVERY_N", "200"))
            CAP_PATH = os.getenv("OFC_CAPTURE_PATH", "/tmp/ofc_inputs.ndjson")
            CAP_SNAPSHOT = os.getenv("OFC_CAPTURE_SNAPSHOT", "1") == "1"
            CAP_CANCEL_STATE = os.getenv("OFC_CAPTURE_CANCEL_STATE", "0") == "1"
            if CAP and (runtime.tick_count % CAP_EVERY == 0):
                runtime_snapshot = None
                cancel_gate_state = None
                if CAP_SNAPSHOT:
                    try:
                        runtime_snapshot = self.of_engine.export_runtime_snapshot(runtime, indicators)
                    except Exception:
                        runtime_snapshot = None
                if CAP_CANCEL_STATE:
                    try:
                        cancel_gate_state = self.of_engine.export_cancel_gate_state()
                    except Exception:
                        cancel_gate_state = None

                row = {
                    "symbol": runtime.symbol
                    "tf": str(runtime.config.get("micro_tf", "1s"))
                    "direction": direction
                    "tick_ts_ms": int(tick_ts)
                    "price": float(price)
                    "delta_z": float(delta_event.get("z", 0.0))
                    "indicators": indicators
                    "absorption": absorption if isinstance(absorption, dict) else None
                    # cfg можно ограничить (чтобы файл не раздувался)
                    "cfg": {}
                    "schema_v": 1
                    "runtime_snapshot": runtime_snapshot
                    "cancel_gate_state": cancel_gate_state
                }
                try:
                    # P-LAG-FIX: Offload disk I/O to background executor to prevent event loop blocking
                    row_str = json.dumps(row, ensure_ascii=False) + "\n"
                    async def _bg_save(path: str, data: str):
                        try:
                            def _sync_write():
                                with open(path, "a", encoding="utf-8") as f:
                                    f.write(data)
                            loop = asyncio.get_running_loop()
                            await loop.run_in_executor(None, _sync_write)
                        except Exception:
                            pass
                    safe_create_task(_bg_save(CAP_PATH, row_str))
                except Exception:
                    pass

            # Measure engine build latency for SRE monitoring
            t_build_ns0 = time.perf_counter_ns()
            try:
                from services.ml_confirm_gate.concurrency import is_of_sync_build, run_bounded_of_build
            except ImportError:
                def is_of_sync_build(): return True
            
            if is_of_sync_build():
                ofc, dec = self.of_engine.build(
                    symbol=runtime.symbol
                    tf=str(runtime.config.get("micro_tf", "1s"))
                    direction=direction
                    tick_ts_ms=tick_ts
                    price=float(price)
                    delta_z=float(delta_z_used)
                    runtime=runtime
                    cfg=cfg2
                    indicators=indicators
                    absorption=absorption if isinstance(absorption, dict) else None
                    worker_lag_ms=worker_lag_ms
                )
            else:
                def _do_build():
                    return self.of_engine.build(
                        symbol=runtime.symbol
                        tf=str(runtime.config.get("micro_tf", "1s"))
                        direction=direction
                        tick_ts_ms=tick_ts
                        price=float(price)
                        delta_z=float(delta_z_used)
                        runtime=runtime
                        cfg=cfg2
                        indicators=indicators
                        absorption=absorption if isinstance(absorption, dict) else None
                        worker_lag_ms=worker_lag_ms
                    )
                result, _ = await run_bounded_of_build(_do_build, timeout_s=0.5)
                if result:
                    ofc, dec = result
                else:
                    ofc, dec = None, None

            t_build_us = int((time.perf_counter_ns() - t_build_ns0) / 1000)

            # P4.1 Latency Contract: Feature Ready (Sampled 1% for BBO flow)
            try:
                if int(tick_ts) % 100 == 0:
                    # Capture timestamps from current tick to verify SLO budget (redis_to_feature < 50ms)
                    latency_payload = {
                        "symbol": str(runtime.symbol)
                        "ts_event_ms": int(tick_ts)
                        "ts_redis_read_ms": int(tick.get("ts_redis_read_ms") or tick.get("ingest_ts_ms") or 0)
                    }
                    # Stamp current time as FEATURE_READY and publish to Redis/Prometheus
                    stamp_feature_ready(latency_payload)
                    safe_create_task(observe_feature_ready_async(
                        latency_payload
                        redis_client=self.redis
                        symbol=runtime.symbol
                        writer=self._latency_writer
                    ))
            except Exception:
                pass

            # expose calibration diagnostics
            indicators["abs_lvl_eff_quote_th"] = float(cfg2.get("abs_lvl_eff_quote_th", 0.0) or 0.0)
            indicators["abs_lvl_min_quote_delta"] = float(cfg2.get("abs_lvl_min_quote_delta", 0.0) or 0.0)
            indicators["abs_lvl_calib_n"] = int(cfg2.get("abs_lvl_calib_n", 0) or 0)
            indicators["abs_lvl_calib_src"] = str(cfg2.get("abs_lvl_calib_src", "static"))

            if ofc:
                ev = ofc.evidence
                indicators["of_confirm"] = ofc.to_dict()
                indicators["of_confirm_v3"] = ofc.to_dict()
                indicators["of_confirm_ok"] = int(ofc.ok)

                # ------------------------------------------------------------------
                # CRITICAL: Propagate meta_enforce_cov_bucket and meta_enforce_applied
                # from evidence into indicators on ALL paths (pass + veto).
                #
                # Without this, PositionState.meta_enforce_cov_bucket stays at "" and
                # meta_enforce_applied stays at -1 (defaults from models.py), because:
                #   - handlers.py:929 reads from signal payload
                #   - signal payload is built from indicators
                #   - evidence→indicators sync only happened on the veto path (tick_processor.py:2836)
                #
                # Effect: TradeClosed in trades:closed stream had empty bucket and -1 applied
                # causing meta_cov_outcome_auto_apply_v1.py to report n=0 for all enf/ctl groups.
                # ------------------------------------------------------------------
                try:
                    if isinstance(ev, dict):
                        _cov_bucket = str(ev.get("meta_enforce_cov_bucket") or "")
                        if _cov_bucket:
                            indicators["meta_enforce_cov_bucket"] = _cov_bucket
                        _enforce_applied = ev.get("meta_enforce_applied")
                        if _enforce_applied is not None:
                            indicators["meta_enforce_applied"] = int(_enforce_applied)
                except Exception:
                    pass
                
                # ------------------------------------------------------------
                # SRE metrics emission (sampled, deterministic, fail-open)
                # ------------------------------------------------------------
                try:
                    missing = []  # fail-safe: always bind before sampling branch
                    if OF_GATE_METRICS_ENABLE:
                        rate = float(cfg2.get("of_gate_metrics_sample", OF_GATE_METRICS_SAMPLE) or OF_GATE_METRICS_SAMPLE)
                        sample_uid = _sample_uid_symbol_ts(str(runtime.symbol), int(tick_ts))
                        if rate <= 0:
                            ok_metrics_skipped_total.labels("strategy", "disabled").inc()
                        elif not _should_sample(sample_uid, rate):
                            ok_metrics_skipped_total.labels("strategy", "sample").inc()
                        else:
                            ev = ofc.evidence or {}
                            scenario_v4 = str(ev.get("scenario_v4", "") or "") or str(getattr(ofc, "scenario", "") or "")
                            missing = ev.get("missing_legs", []) if isinstance(ev, dict) else []
                            if not isinstance(missing, list):
                                missing = []
                                
                            ok = 1 if getattr(ofc, "ok", False) else 0
                            ok_soft = int(ev.get("ok_soft", 0) or 0)
                            ok_src = "strategy_ofc"
                            ok_soft_src = "strategy_ev"
                            
                            ml = ev.get("ml_decision", {}) if isinstance(ev.get("ml_decision"), dict) else {}
                            ml_lat_us = float(ml.get("latency_us", 0.0) or 0.0)

                            # Add extra flags for monitoring / slicing 
                            payload = {
                                "type": "of_gate"
                                "schema": "of_gate_metrics_v1"
                                "schema_ver": "1"
                                "emit_src": "strategy"
                                "sample_rate": str(rate)
                                "sample_key_mode": OF_GATE_METRICS_SAMPLE_KEY_MODE
                                "ts_ms": str(normalize_epoch_ms_v2(tick_ts).ts_ms)
                                "symbol": str(runtime.symbol)
                                "direction": str(direction)
                                "scenario": str(getattr(ofc, "scenario", "") or "")
                                "scenario_v4": scenario_v4
                                "ok": str(ok)
                                "rule_ok": str(ok)
                                "ok_rule": str(ok)
                                "ok_soft": str(ok_soft)
                                "rule_ok_soft": str(ok_soft)
                                "ok_rule_soft": str(ok_soft)
                                "ok_src": str(ok_src)
                                "ok_soft_src": str(ok_soft_src)
                                "have": str(int(getattr(ofc, "have", 0) or 0))
                                "need": str(int(getattr(ofc, "need", 0) or 0))
                                "score": str(float(getattr(ofc, "score", 0.0) or 0.0))
                                # keep for offline debug but cap size (avoid huge cardinality strings)
                                "reason": str(getattr(ofc, "reason", "") or "")[:120]
                                "gate_bits": str(int(getattr(ofc, "gate_bits", 0) or 0))
                                "exec_risk_bps": str(float(ev.get("exec_risk_bps", 0.0) or 0.0))
                                "exec_risk_norm": str(float(ev.get("exec_risk_norm", 0.0) or 0.0))
                                "latency_us": str(int(t_build_us))
                                "meta_p": str(float(ev.get("meta_p", -1.0) or -1.0))
                                "meta_veto": str(int(ev.get("meta_veto", 0) or 0))
                                "meta_enforce_applied": str(int(ev.get("meta_enforce_applied", 0) or 0))
                                "meta_enforce_share": str(float(ev.get("meta_enforce_share", 1.0) or 1.0))
                                "meta_enforce_bucket": str(ev.get("meta_enforce_bucket", "other") or "other")
                                "meta_mode": str(ev.get("meta_mode", "") or "")
                                "meta_enable": str(int(ev.get("meta_enable", 0) or 0))
                                "meta_reason": str(ev.get("meta_reason", "") or "")[:80]
                                "meta_schema_name": str(ev.get("meta_schema_name", "") or "")
                                "meta_schema_version": str(int(ev.get("meta_schema_version", 0) or 0))
                                "meta_model_schema_name": str(ev.get("meta_model_schema_name", "") or "")
                                "meta_model_schema_version": str(int(ev.get("meta_model_schema_version", 0) or 0))
                                "meta_feature_coverage": str(float(ev.get("meta_feature_coverage", 1.0) or 1.0))
                                "meta_feature_missing_rate": str(float(ev.get("meta_feature_missing_rate", 0.0) or 0.0))
                                "meta_model_feature_total": str(int(ev.get("meta_model_feature_total", 0) or 0))
                                "meta_model_feature_missing": str(int(ev.get("meta_model_feature_missing", 0) or 0))
                                "meta_enforce_cov_bucket": str(ev.get("meta_enforce_cov_bucket", "") or "")
                                "meta_enforce_bucket_type": str(ev.get("meta_enforce_bucket_type", "") or "")
                                "data_health": str(float(indicators.get(IK.DATA_HEALTH, 1.0) or 1.0))
                                "book_health_ok": str(int(indicators.get(IK.BOOK_HEALTH_OK, 1) or 1))
                                # contract from PDF: needed for SRE monitor
                                "source_consistency_ok": str(int(indicators.get("source_consistency_ok", 1)))
                                "missing_legs": json.dumps(missing[:6], ensure_ascii=False, separators=(",", ":"))

                                # ML confirm (for p50/p95/p99 + fail rate)
                                "ml_mode": str(ml.get("mode", "") or "")
                                "ml_kind": str(ml.get("kind", "") or "")
                                "ml_allow": str(int(bool(ml.get("allow", True))))
                                "ml_bucket": str(ml.get("bucket", "") or "")
                                "ml_p_edge": str(float(ml.get("p_edge", 0.0) or 0.0))
                                "ml_p_min": str(float(ml.get("p_min", 0.0) or 0.0))
                                "ml_score": str(float(ml.get("score", 0.0) or 0.0))
                                "ml_floor": str(float(ml.get("floor", 0.0) or 0.0))
                                "ml_latency_us": str(int(ml_lat_us))
                            }
                            payload = enrich_schema_fields(payload)
                            ok_row, code = validate_of_gate_row(payload)

                            async def _emit_ok_metrics(_payload: Dict[str, Any]) -> None:
                                try:
                                    await self.redis.xadd(
                                        OF_GATE_METRICS_STREAM
                                        {k: str(v) for k, v in _payload.items()}
                                        maxlen=OF_GATE_METRICS_MAXLEN
                                        approximate=True
                                    )
                                    ok_metrics_emitted_total.labels("strategy").inc()
                                except Exception:
                                    ok_metrics_error_total.labels("strategy", "xadd").inc()

                            async def _emit_quarantine(_payload: Dict[str, Any], _why: str) -> None:
                                try:
                                    await emit_quarantine_row(
                                        self.redis
                                        stream=OF_GATE_METRICS_QUARANTINE_STREAM
                                        payload=_payload
                                        why=_why
                                        emit_src="strategy"
                                        maxlen=OF_GATE_METRICS_QUARANTINE_MAXLEN
                                    )
                                except Exception:
                                    ok_metrics_error_total.labels("strategy", "quarantine_xadd").inc()

                            if not ok_row:
                                wl = why_label(code)
                                ok_metrics_skipped_total.labels("strategy", f"dq_{wl}").inc()
                                if OF_GATE_METRICS_QUARANTINE_ENABLE:
                                    safe_create_task(_emit_quarantine(payload, code))
                            else:
                                safe_create_task(_emit_ok_metrics(payload))
                except Exception as e:
                    import traceback
                    print(f"METRICS EMISSION ERROR: {e}")
                    traceback.print_exc()
                    pass
                
                # Use dec directly from build() instead of overwriting with None
                if dec and hasattr(dec, "need") and hasattr(dec, "have"):
                    # P2: Dynamic Confirmation Need (Expert Scaler)
                    # We lower the barrier in high liquidity (liq_score >= 0.8) 
                    # and raise it if requested by regime service.
                    liq_score = float(indicators.get(IK.LIQ_SCORE, 1.0) or 1.0)
                    need_bump = 0
                    
                    if liq_score >= 0.8:
                        # Healthy market: allow 2-leg signals in Range scenario
                        if str(getattr(dec, "scenario", "")) == "range":
                             dec.need = max(2, int(dec.need) - 1)
                             dec.reason = f"{dec.reason}|liq_relax"
                    elif liq_score < 0.35:
                        need_bump = 1
                    
                    if need_bump > 0:
                        indicators["strong_gate_need_bump"] = 1
                        indicators["strong_gate_need_reason"] = "low_liquidity"
                    
                    eff_need = int(dec.need) + need_bump
                    
                    # Re-evaluate OK status
                    is_ok = int(dec.have) >= eff_need
                    # Only strictify (never relax)
                    if not is_ok:
                        indicators["strong_gate_ok"] = 0
                        indicators["of_confirm_ok"] = 0
                        ofc.ok = False # Sync object
                    
                    # IMPORTANT:
                    #   ofc.score is a continuous quality score (0..1).
                    #   have/need ratio is a different diagnostic.
                    # Keep both explicitly to avoid confusing audits/telemetry/Telegram.
                    indicators["of_confirm_score"] = float(getattr(ofc, "score", 0.0) or 0.0)
                    indicators["of_confirm_have_need_ratio"] = float(dec.have / eff_need) if eff_need > 0 else 0.0
                    
                    # Soft-fail diagnostics
                    indicators["of_confirm_ok_soft"] = int(ev.get("ok_soft", 0))
                    indicators["of_confirm_soft_reason"] = str(ev.get("soft_reason", ""))
                    
                    # Persist last strong-gate diagnostics for SMT snapshot / entry policy.
                    try:
                        indicators["strong_gate_have"] = int(dec.have)
                        indicators["strong_gate_need"] = int(eff_need)
                        indicators["strong_gate_scn"] = str(dec.scenario)
                        indicators["strong_need_reason"] = str(getattr(dec, "need_reason", "") or "")

                        runtime.last_of_confirm_score = float(indicators.get("of_confirm_score", 0.0) or 0.0)
                        setattr(runtime, "last_of_confirm_have_need_ratio", float(indicators.get("of_confirm_have_need_ratio", 0.0) or 0.0))
                        runtime.last_strong_gate_have = int(indicators.get("strong_gate_have", 0) or 0)
                        runtime.last_strong_gate_need = int(indicators.get("strong_gate_need", 0) or 0)
                        runtime.last_strong_gate_scn = str(indicators.get("strong_gate_scn", "") or "")
                    except Exception:
                        pass
                indicators["strong_gate_bits"] = int(ofc.gate_bits)
                indicators["strong_gate_reason"] = str(ofc.reason)
                # indicators["strong_gate_ok"] already updated if needed
                # If require_strong_confirmation is False, we are effectively in SHADOW/MONITOR mode
                if not bool(runtime.config.get("require_strong_confirmation", False)):
                    indicators["of_gate_mode"] = "SHADOW"
                else:
                    indicators["of_gate_mode"] = "SHADOW" if bool(runtime.config.get("strong_gate_shadow", False)) else "ENFORCE"

                # --- NEW: record last strong-pass dir/ts ONLY when gate passed (ok==1) ---
                # This is the value SMT/EntryPolicy should trust as "leader confirmed by OF".
                try:
                    if int(ofc.ok) == 1:
                        runtime.last_strong_pass_ts_ms = int(tick_ts)
                        runtime.last_strong_pass_dir = str(direction).upper()
                except Exception:
                    pass




                # Rate limit logs: only 1 in 50
                sg_cnt = self.strong_gate_counters.get(runtime.symbol, 0) + 1
                self.strong_gate_counters[runtime.symbol] = sg_cnt

                if sg_cnt % 10000 == 0:
                    self.logger.info(
                        "🔥 Signal Strong-Gate Decision: symbol=%s, scenario=%s, ok=%d, score=%.2f, have=%d, need=%d, reason=%s (x%d)"
                        runtime.symbol, ofc.scenario, ofc.ok, ofc.score, ofc.have, ofc.need, ofc.reason, sg_cnt
                    )

                # ENFORCE / SHADOW logic (+ liquidity auto-enforce on stressed + calibrator override)
                enforce = bool(runtime.config.get("require_strong_confirmation", False))
                try:
                    if str(getattr(runtime, "liq_regime", "normal") or "normal").lower() == "stressed":
                        enforce = bool(int(runtime.config.get("liq_enforce_strong_when_stressed", 1) or 1))
                except Exception:
                    pass

                # G5 Calibrator override: auto-promote from SHADOW → ENFORCE
                sg_calib_mode = ""
                try:
                    sg_calib_mode = str(getattr(runtime, "dynamic_cfg", {}).get(DK.SG_CALIB_MODE, "") or "")
                    if sg_calib_mode in ("shadow_enforce", "full_enforce"):
                        enforce = True
                        indicators["sg_calib_promoted"] = 1
                        indicators["sg_calib_mode"] = sg_calib_mode
                    if sg_calib_mode == "shadow_enforce" and not bool(runtime.config.get("strong_gate_shadow", False)):
                        # Auto-promoted: keep shadow as safety net until full_enforce approved
                        runtime.config["strong_gate_shadow"] = True
                        indicators["sg_calib_shadow_override"] = 1
                except Exception:
                    pass

                if enforce and ofc.ok == 0:
                    # Soft-Fail Bypass (Analytics Mode)
                    # If engine marked it as ok_soft=1 (high quality but missing 1 leg), we let it pass as VIRTUAL signal.
                    # This allows tracking stats via TradeMonitor/DB without risking capital.
                    is_soft_pass = int(ev.get("ok_soft", 0) or 0) == 1
                    
                    if is_soft_pass:
                        # BYPASS VETO via Soft-Fail (Virtual)
                        indicators["strong_gate_soft_pass"] = 1
                        indicators["is_virtual"] = 1  # MARKER for TradeMonitor/Payload
                        
                        # Add detailed flags for analytics (requested by user)
                        scenario_v4 = str(ev.get("scenario_v4", "") or "")
                        reason_soft = str(ev.get("soft_reason", "") or "")
                        
                        indicators["is_soft_fail"] = 1
                        # Distinct flags for scenarios
                        indicators["soft_fail_type"] = scenario_v4 
                        indicators["soft_fail_reason"] = reason_soft
                        
                        # Specific flags for easy SQL querying
                        if "range" in scenario_v4:
                            indicators["soft_fail_range"] = 1
                        elif "vol_shock" in scenario_v4:
                            indicators["soft_fail_vol_shock"] = 1
                        elif "saw" in scenario_v4:
                            indicators["soft_fail_saw_chop"] = 1
                            
                        self.logger.info(
                            "⚠️ Signal SOFT-PASSED (Virtual): symbol=%s, scenario=%s, reason=%s"
                            runtime.symbol, scenario_v4, reason_soft
                        )
                    elif bool(runtime.config.get("strong_gate_shadow", False)):
                        indicators["strong_gate_shadow_veto"] = 1
                        indicators["is_virtual"] = 1  # ❗️ MARKER for Virtual Trade (shadow mode)
                    else:
                        strong_gate_veto_total.labels(symbol=runtime.symbol, scenario=ofc.scenario, reason="engine_veto", mode="ENFORCE").inc()
                        veto_low_conf_total.labels(symbol=runtime.symbol).inc()
                        of_session_outcome_total.labels(runtime.symbol, sess, "veto_strong_gate").inc()
                        # Add explicit visibility for dropped signals
                        self.logger.warning(
                            "🚫 Signal filtered by Strong Gate (ENFORCE): symbol=%s, scenario=%s, reason=%s. "
                            "To fix, enable strong_gate_shadow=1 or disable require_strong_confirmation."
                            runtime.symbol, ofc.scenario, ofc.reason
                        )
                        return None

                # Audit Confirmations (mirror resulting evidence)
                # Note: We append these to confirmations list for Telegram/UI
                if ev.get("sweep"):
                    div_match = bool(indicators.get("sweep_div_match", 0))
                    require_div = bool(runtime.config.get("sweep_require_divergence", 0))
                    if (not require_div) or div_match:
                         kind = indicators.get("sweep_kind", "")
                         confirmations.insert(0, "sweep_eqh=1" if kind == "EQH_SWEEP" else "sweep_eql=1")
                
                if ev.get("absorption"): confirmations.append(f"absorption={ev.get('absorption_volume', 0.0):.2f}")
                if ev.get("weak_progress"): confirmations.append("weak_progress=1")
                if ev.get("abs_lvl_ok"): confirmations.append(f"abs_lvl={ev.get('abs_lvl_score', 0.0):.2f}")

                # ------------------------------------------------------------
                # Phase E: OBI quality, FP Edge Absorb, Weak Trend (Scoring/Telemetry)
                # ------------------------------------------------------------
                try:
                    now_ms_det = int(now_ms)
                    # OBI stability (quality-gated)
                    if runtime.last_obi_event:
                        age = now_ms_det - int(runtime.last_obi_event.get("ts_ms", 0) or 0)
                        ttl = int(runtime.config.get("obi_event_ttl_ms", 30000))
                        if 0 <= age <= ttl:
                            indicators["obi_event_age_ms"] = int(age)
                            indicators["obi_dir"] = str(runtime.last_obi_event.get("direction") or "")
                            indicators["obi"] = float(runtime.last_obi_event.get("obi", 0.0) or 0.0)
                            indicators["obi_z"] = float(runtime.last_obi_event.get("obi_z", 0.0) or 0.0)
                            indicators["obi_stable_secs"] = float(runtime.last_obi_event.get("stable_secs", 0.0) or 0.0)
                            indicators["obi_stability_score"] = float(runtime.last_obi_event.get("stability_score", 0.0) or 0.0)
                            indicators["obi_sustained"] = bool(int(runtime.last_obi_event.get("stable", 0) or 0) == 1)
                            if str(runtime.last_obi_event.get("direction") or "").upper() == direction:
                                if indicators["obi_sustained"]:
                                    confirmations.append(f"obi_stable={float(indicators['obi_stable_secs']):.2f}")

                    # BUGFIX: Ensure continuous OBI is recorded for ML if no valid event was found
                    if "obi" not in indicators or indicators["obi"] == 0.0:
                        indicators["obi"] = float(getattr(runtime, "lob_dw_obi", 0.0) or 0.0)
                        indicators["obi_z"] = float(getattr(runtime, "dw_obi_z", 0.0) or 0.0)

                    # Footprint edge absorb (recent, no range expansion)
                    fe = getattr(runtime, "last_fp_edge", None)
                    if fe is not None:
                        valid = int(runtime.config.get("fp_edge_valid_ms", 30000))
                        age = now_ms_det - int(getattr(fe, "ts_ms", 0) or 0)
                        if 0 <= age <= valid:
                            p90 = float(getattr(fe, "p90", 0.0) or 0.0)
                            val = float(getattr(fe, "value", 0.0) or 0.0)
                            strength = (val / p90) if p90 > 0 else 0.0
                            bias = str(getattr(fe, "bias", "") or "").upper()
                            rng = int(getattr(fe, "range_expansion", 0) or 0)
                            # Logic: LONG signal needs BUY bias edge (support?), SHORT needs SELL bias?
                            # Actually, tick-level fp_edge side "BID" means absorption on bid (support).
                            # If bias is present, use it.
                            ok = 1 if (bias == direction and rng == 0 and strength > 0) else 0
                            indicators["fp_edge_absorb"] = int(ok)
                            indicators["fp_edge_strength"] = float(strength)
                            indicators["fp_edge_range_expansion"] = int(rng)
                            indicators["fp_edge_age_ms"] = int(age)
                            if ok:
                                confirmations.append(f"fp_edge_absorb={strength:.2f}")

                    # Weak progress trend (history)
                    try:
                        wp_det = getattr(runtime, "weak_progress_det", None)
                        if wp_det is not None:
                            indicators["weak_recent_window"] = int(getattr(wp_det, "recent_window", 0) or 0)
                            indicators["weak_recent_count"] = int(wp_det.recent_weak_count())
                            w = int(indicators["weak_recent_window"] or 0)
                            c = int(indicators["weak_recent_count"] or 0)
                            ratio = float(c / w) if w > 0 else 0.0
                            indicators["weak_recent_ratio"] = ratio
                            
                            # Legacy boolean for Scorer fallback
                            min_weak = int(runtime.config.get("weak_recent_min_cnt", 3))
                            indicators["weak_progress"] = bool(ev.get("weak_progress") or (c >= min_weak))
                            if c >= min_weak:
                                confirmations.append(f"weak_recent={c}/{w}")
                    except Exception:
                        pass
                except Exception:
                    pass
                    
                # Iceberg (Strict/Recent)
                if runtime.last_iceberg_event:
                     ice_ts = int(runtime.last_iceberg_event.get("ts_ms") or 0)
                     if (tick_ts - ice_ts) < 5000:
                         confirmations.append(f"iceberg={runtime.last_iceberg_event.get('total_refresh_qty')}")
                         # strict direction check
                         ice_side = str(runtime.last_iceberg_event.get("side")).upper()
                         spike_side = "BUY" if float(delta_event.get("delta", 0)) > 0 else "SELL"
                         iceberg_side = "BUY" if ice_side == "BID" else "SELL" # iceberg is limit
                         # We want opposing iceberg for absorption
                         if spike_side != iceberg_side:
                              confirmations.append("ice_strict=1")


                # Optional Redis Publication (v3 asychronous)
                if bool(int(runtime.config.get("publish_of_confirm", 0))):
                    stream = str(runtime.config.get("of_confirm_stream", "signals:of:confirm"))
                    try:
                        safe_create_task(
                            self.ticks.xadd(
                                stream
                                fields={"payload": json.dumps(ofc.to_dict(), ensure_ascii=False)}
                                maxlen=int(runtime.config.get("of_confirm_stream_maxlen", 50000))
                                approximate=True
                            )
                        )
                    except Exception:
                        pass

                # ------------------------------------------------------------
                # Publish deterministic decision inputs for golden replay
                # ------------------------------------------------------------
                try:
                    # logger.error("DEBUG: 1. accessing OFI config")
                    pub_val = runtime.config.get("publish_of_inputs", 0)
                    should_pub = bool(int(pub_val))
                    
                    if should_pub:
                        # Deterministic time check: skip publish if tick_ts_ms <= 0
                        # This is critical for "golden replay": same ticks must produce same inputs
                        tick_ts_ms = int(tick_ts) if int(tick_ts or 0) > 0 else 0
                        if tick_ts_ms <= 0:
                            # skip publish: non-deterministic / bad tick time
                            try:
                                from services.orderflow.metrics import of_inputs_bad_time_total
                                of_inputs_bad_time_total.labels(symbol=str(runtime.symbol)).inc()
                            except Exception:
                                pass
                            should_pub = False
                        
                        if should_pub:
                            # logger.error("DEBUG: 2. Entering OFI Logic")
                            # continuation context
                            trend_dir = "NONE"
                            hidden_ctx_recent = 0
                            cont_ctx_recent = 0
                            try:
                                div = getattr(runtime, "last_div", None)
                                td = hidden_trend_dir(getattr(div, "kind", None) if div else None)
                                if td:
                                    trend_dir = str(td).upper()
                                # hidden ctx - deterministic: depends only on tick_ts
                                if div and td:
                                    now_ts = tick_ts_ms
                                    hidden_ms = int(runtime.config.get("hidden_ctx_valid_ms", 120_000))
                                    age = now_ts - int(getattr(div, "ts_ms", now_ts))
                                    hidden_ctx_recent = 1 if (0 <= age <= hidden_ms) else 0
                                # cont ctx - deterministic: depends only on tick_ts
                                now_ts = tick_ts_ms
                                cts = int(getattr(runtime, "cont_ctx_ts_ms", 0) or 0)
                                cv = int(runtime.config.get("cont_ctx_valid_ms", 120_000))
                                cont_ctx_recent = 1 if (cts > 0 and 0 <= now_ts - cts <= cv) else 0
                            except Exception as ex_ctx:
                                logger.debug(f"OFI: Context calc error: {ex_ctx}")

                        # 2. Extract evidence
                        # Helper functions for deterministic type conversion (sanitizes NaN/Inf, handles None)
                        def _i(v, d=0) -> int:
                            try:
                                return int(v)
                            except Exception:
                                try:
                                    return int(float(v))
                                except Exception:
                                    return int(d)

                        def _f(v, d=0.0) -> float:
                            try:
                                x = float(v)
                                # sanitize NaN/Inf (kills replay determinism / diffs)
                                if x != x or x == float("inf") or x == float("-inf"):
                                    return float(d)
                                return x
                            except Exception:
                                return float(d)

                        def _s(v, d="na") -> str:
                            try:
                                s = str(v) if v is not None else d
                                s = s.strip()
                                return s if s else d
                            except Exception:
                                return d

                        # Prefer evidence snapshot (deterministic), fallback to indicators
                        ev_weak       = _i(indicators.get("weak_progress", 0), 0)
                        ev_sweep      = _i(indicators.get("sweep_recent", indicators.get("sweep", 0)), 0)
                        ev_reclaim    = _i(indicators.get("reclaim_recent", indicators.get("reclaim", 0)), 0)
                        ev_obi_stable = _i(indicators.get("obi_stable", 0), 0)
                        ev_ice_strict = _i(indicators.get("iceberg_strict", indicators.get("ice_strict", 0)), 0)
                        ev_abs_lvl_ok = _i(indicators.get("abs_lvl_ok", 0), 0)

                        if ofc and hasattr(ofc, "evidence") and isinstance(ofc.evidence, dict):
                            ev = ofc.evidence
                            ev_weak       = _i(ev.get("weak_progress", ev_weak), ev_weak)
                            # evidence uses sweep/reclaim (already "recent" semantics in your pipeline)
                            ev_sweep      = _i(ev.get("sweep", ev.get("sweep_recent", ev_sweep)), ev_sweep)
                            ev_reclaim    = _i(ev.get("reclaim", ev.get("reclaim_recent", ev_reclaim)), ev_reclaim)
                            ev_obi_stable = _i(ev.get("obi_stable", ev_obi_stable), ev_obi_stable)
                            ev_ice_strict = _i(ev.get("iceberg_strict", ev_ice_strict), ev_ice_strict)
                            ev_abs_lvl_ok = _i(ev.get("abs_lvl_ok", ev_abs_lvl_ok), ev_abs_lvl_ok)
                        
                        # 4. Create Object
                        # logger.error("DEBUG: 4. Creating OFI Object")
                        
                        # Safe CFG - keep only small, JSON-safe, deterministic subset for replay
                        cfg_safe = {}
                        try:
                            for _k in (
                                "of_score_min"
                                "of_inputs_stream"
                                "of_inputs_stream_maxlen"
                                "hidden_ctx_valid_ms"
                                "cont_ctx_valid_ms"
                            ):
                                if _k in runtime.config:
                                    _v = runtime.config.get(_k)
                                    if isinstance(_v, (int, float, str, bool)) or _v is None:
                                        cfg_safe[_k] = _v
                        except Exception:
                            cfg_safe = {}

                        # Determinism: do NOT pick version by "key presence".
                        # Emit v2 unless explicitly disabled in runtime cfg/env.
                        emit_v2_cfg = runtime.config.get("of_inputs_emit_v2", 1)
                        emit_v2 = bool(_i(emit_v2_cfg, 1))

                        # Build base OFInputs fields
                        ofi_kwargs = {
                            "v": 2 if emit_v2 else 1
                            "symbol": _s(runtime.symbol)
                            "ts_ms": int(tick_ts_ms)
                            "regime": _s(getattr(runtime, "last_regime", "na"))
                            "direction": _s(direction)
                            # prefer scenario_v4 from evidence snapshot if available
                            "scenario": _s(
                                (ofc.evidence.get("scenario_v4") if (ofc and isinstance(getattr(ofc, "evidence", None), dict)) else None)
                                or (getattr(dec, "scenario_v4", None) if dec else None)
                                or (getattr(dec, "scenario", None) if dec else None)
                                or "na"
                            )
                            # determinism: use the same delta_z used in build(), not raw delta_event
                            "delta_z": _f(delta_z_used, 0.0)
                            "weak_progress": ev_weak
                            "sweep_recent": ev_sweep
                            "reclaim_recent": ev_reclaim
                            "obi_stable": ev_obi_stable
                            "iceberg_strict": ev_ice_strict
                            "abs_lvl_ok": ev_abs_lvl_ok
                            "trend_dir": _s(trend_dir, "NONE").upper()
                            "hidden_ctx_recent": _i(hidden_ctx_recent, 0)
                            "cont_ctx_recent": _i(cont_ctx_recent, 0)
                            "cfg": cfg_safe
                            "fp_eff_quote": _f(getattr(runtime.last_bar, "fp_eff_quote", 0.0) if runtime.last_bar else 0.0, 0.0)
                            "fp_quote_delta": _f(getattr(runtime.last_bar, "fp_quote_delta", 0.0) if runtime.last_bar else 0.0, 0.0)
                        }
                        
                        # Optional fields (only if contract supports them)
                        _ann = getattr(OFInputsV1, "__annotations__", {}) or {}
                        if "regime_group" in _ann:
                            ofi_kwargs["regime_group"] = str(getattr(runtime, "last_regime", "na"))
                        
                        hsnap = getattr(runtime, "hawkes_snapshot", None)
                        if isinstance(hsnap, dict):
                            if "hawkes_dt_s" in _ann:
                                ofi_kwargs["hawkes_dt_s"] = float(hsnap.get("hawkes_dt_s", 0.0) or 0.0)
                            if "hawkes_taker_lam" in _ann:
                                ofi_kwargs["hawkes_taker_lam"] = float(hsnap.get("hawkes_taker_lam", 0.0) or 0.0)
                            if "hawkes_cancel_lam" in _ann:
                                ofi_kwargs["hawkes_cancel_lam"] = float(hsnap.get("hawkes_cancel_lam", 0.0) or 0.0)
                            if "hawkes_churn_lam" in _ann:
                                ofi_kwargs["hawkes_churn_lam"] = float(hsnap.get("hawkes_churn_lam", 0.0) or 0.0)
                        
                        # Add OFI fields if using V2
                        missing_ofi = False
                        missing_fp = False
                        if emit_v2:
                            # Always include fields in v2 (deterministic schema)
                            ofi_kwargs["ofi"] = _f(indicators.get("ofi", 0.0), 0.0)
                            ofi_kwargs["ofi_z"] = _f(indicators.get("ofi_z", 0.0), 0.0)
                            ofi_kwargs["ofi_stable"] = _i(indicators.get("ofi_stable", 0), 0)
                            ofi_kwargs["ofi_dir_ok"] = _i(indicators.get("ofi_dir_ok", 0), 0)
                            ofi_kwargs["ofi_stable_secs"] = _f(indicators.get("ofi_stable_secs", 0.0), 0.0)
                            ofi_kwargs["ofi_stability_score"] = _f(indicators.get("ofi_stability_score", 0.0), 0.0)
                            ofi_kwargs["ofi_age_ms"] = _i(indicators.get("ofi_age_ms", -1), -1)

                            # FP edge fields
                            ofi_kwargs["fp_edge_absorb"] = _i(indicators.get("fp_edge_absorb", 0), 0)
                            ofi_kwargs["fp_edge_absorb_strength"] = _f(indicators.get("fp_edge_absorb_strength", indicators.get("fp_edge_strength", 0.0)), 0.0)
                            ofi_kwargs["fp_edge_age_ms"] = _i(indicators.get("fp_edge_age_ms", -1), -1)

                            # Confirmations-as-features (Stage 4, partial)
                            if "sweep_eqh" in OFInputsV2.__annotations__:
                                ofi_kwargs["sweep_eqh"] = _i(indicators.get("sweep_eqh", 0), 0)
                            if "sweep_eql" in OFInputsV2.__annotations__:
                                ofi_kwargs["sweep_eql"] = _i(indicators.get("sweep_eql", 0), 0)
                            if "rsi_agree" in OFInputsV2.__annotations__:
                                ofi_kwargs["rsi_agree"] = _i(indicators.get("rsi_agree", 0), 0)
                            if "div_match" in OFInputsV2.__annotations__:
                                ofi_kwargs["div_match"] = _i(indicators.get("div_match", 0), 0)

                            # Missing = age unknown AND values essentially default
                            if ofi_kwargs["ofi_age_ms"] < 0 and ofi_kwargs["ofi"] == 0.0 and ofi_kwargs["ofi_z"] == 0.0:
                                missing_ofi = True
                            if ofi_kwargs["fp_edge_age_ms"] < 0 and ofi_kwargs["fp_edge_absorb"] == 0:
                                missing_fp = True

                            ofi = OFInputsV2(**ofi_kwargs)
                        else:
                            ofi = OFInputsV1(**ofi_kwargs)
                            # For v1, OFI/FP are missing by definition
                            missing_ofi = True
                            missing_fp = True
                        
                        # Record metrics
                        try:
                            from services.orderflow.metrics import (
                                of_inputs_version_total
                                of_inputs_missing_ofi_total
                                of_inputs_missing_fp_total
                            )
                            version_str = "v2" if emit_v2 else "v1"
                            of_inputs_version_total.labels(symbol=str(runtime.symbol), version=version_str).inc()
                            if missing_ofi:
                                of_inputs_missing_ofi_total.labels(symbol=str(runtime.symbol)).inc()
                            if missing_fp:
                                of_inputs_missing_fp_total.labels(symbol=str(runtime.symbol)).inc()
                        except Exception:
                            pass  # Don't fail on metrics
                        
                        # logger.error("DEBUG: 5. Serializing...")
                        # Canonical JSON to make replay/topdiff deterministic
                        blob = json.dumps(ofi.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)

                        # Align default with actual usage
                        in_stream = str(runtime.config.get("of_inputs_stream", "signals:of:inputs"))

                        sampled_debug(logger, "OFI_PUBLISHING", "OFI: Publishing to Redis...")
                        safe_create_task(
                            self.ticks.xadd(
                                in_stream
                                fields={"payload": blob}
                                maxlen=int(runtime.config.get("of_inputs_stream_maxlen", 200000))
                                approximate=True
                            )
                        )
                        sampled_debug(logger, "OFI_PUBLISHED", "OFI: PublishedTask Created")

                except Exception as e_main:
                     logger.debug(f"OFI: Block error: {e_main}")
                     pass

        except Exception as ex:
            logger.error(f"OFConfirm engine error: {ex}")


        # ------------------------------------------------------------
        # min_confirmations gate (hard vs soft)
        # По умолчанию fp_imb не увеличивает hard_count, иначе pass-rate станет выше.
        # ------------------------------------------------------------
        # ------------------------------------------------------------
        from core.footprint_policy import is_soft_confirmation # Ensure import or use existing
        
        if tick.get("mock_force"):
             self.logger.warning("TRACE 3: Approaching Gate Check")

        delta_abs = abs(delta_event.get("delta", 0.0))
        min_delta = runtime.config["delta_abs_min_confirm"]
        min_confirmations = int(runtime.config.get("min_confirmations", 0))
        
        fp_imb_counts = bool(runtime.config.get("fp_imb_counts_for_min_confirmations", False))
        if fp_imb_counts:
            hard_count = len(confirmations)
        else:
            hard_count = 0
            for c in confirmations:
                if is_soft_confirmation(c):
                    continue
                hard_count += 1

        if delta_abs < min_delta and hard_count < min_confirmations:
            # FORCE LOG for diagnostics
            logger.warning(
                "🛑 [MIN-CONF] (%s) Signal filtered: delta_abs=%.2f < %.2f AND hard_confirmations=%d < %d"
                runtime.symbol
                delta_abs
                min_delta
                hard_count
                min_confirmations
            )
            return None

        # Deterministic now
        now_ms = int(tick_ts)

        signal_id = f"crypto-of:{runtime.symbol}:{now_ms}"
        primary_reason = "delta_spike"
        if confirmations:
            primary_reason = confirmations[0].split("=", 1)[0]

        # [DEDUPLICATED] Primary ATR-floor gate is handled as Early Gate (lines ~600).


        # ------------------------------------------------------------
        # Phase E: OBI stability evidence (TTL + book health)
        # ------------------------------------------------------------
        # Populate indicators so scorer/Telegram can use stability duration + quality.
        # Fail-open: if no book evidence or TTL expired, do nothing.
        try:
            if int(indicators.get("book_health_ok", 1) or 1) == 1:
                obe = getattr(runtime, "last_obi_event", None)
                if isinstance(obe, dict):
                    ots = int(obe.get("ts_ms", 0) or 0)
                    ttl = int(runtime.config.get("obi_event_ttl_ms", 30000) or 15000)
                    if ots > 0 and 0 <= (now_ms - ots) <= ttl:
                        # raw OBI values
                        indicators["obi"] = float(obe.get("obi", indicators.get("obi", 0.0) or 0.0) or 0.0)
                        indicators["obi_z"] = float(obe.get("obi_z", 0.0) or 0.0)
                        # stability
                        indicators["obi_stable_secs"] = float(obe.get("stable_secs", 0.0) or 0.0)
                        # quality score may be missing (legacy); default 1.0 if duration present
                        q = obe.get("stability_score", None)
                        if q is None:
                            q = 1.0 if float(indicators.get("obi_stable_secs", 0.0) or 0.0) > 0 else 0.0
                        indicators["obi_stability_score"] = float(q)
                        indicators["obi_stable"] = int(obe.get("stable", 0) or 0)
        except Exception:
            pass

        # ------------------------------------------------------------
        # Phase E+: OFI stability evidence (TTL + book health)
        # ------------------------------------------------------------
        try:
            if int(indicators.get("book_health_ok", 1) or 1) == 1:
                oe = getattr(runtime, "last_ofi_event", None)
                if isinstance(oe, dict):
                    ots = int(oe.get("ts_ms", 0) or 0)
                    ttl = int(runtime.config.get("ofi_event_ttl_ms", 15000) or 15000)
                    if ots > 0 and 0 <= (now_ms - ots) <= ttl:
                        indicators["ofi"] = float(oe.get("ofi", 0.0) or 0.0)
                        indicators["ofi_z"] = float(oe.get("ofi_z", 0.0) or 0.0)
                        indicators["ofi_stable_secs"] = float(oe.get("stable_secs", 0.0) or 0.0)
                        indicators["ofi_stability_score"] = float(oe.get("stability_score", 0.0) or 0.0)
                        indicators["ofi_stable"] = int(oe.get("stable", 0) or 0)
                        indicators["ofi_age_ms"] = int(now_ms - ots)

                        if int(indicators["ofi_stable"] or 0) == 1:
                            if str(oe.get("direction", "") or "").upper() == str(direction).upper():
                                confirmations.append(f"ofi_stable={float(indicators['ofi_stable_secs']):.1f}s")
        except Exception:
            pass

        # ------------------------------------------------------------
        # Phase E: CVD Reclaim (bonus-layer)
        # ------------------------------------------------------------
        # Add as SOFT confirmation after gates (won't affect min_confirmations).
        try:
            if int(runtime.config.get("cvd_reclaim_enable", 1) or 0) == 1:
                ev = runtime.last_cvd_reclaim
                if ev and (now_ms - ev.ts_ms) <= 120_000:
                    if ev.direction_bias == direction:
                        indicators["cvd_reclaim_ok"] = int(ev.ok)
                        indicators["cvd_reclaim_score"] = float(ev.score)
                        indicators["cvd_reclaim_delta"] = float(ev.cvd_delta)
                        if ev.ok:
                            confirmations.append(f"cvdR={ev.score:.2f}")
                            cvd_reclaim_applied_total.labels(symbol=runtime.symbol, bias=direction).inc()
                            cvd_reclaim_age_ms_gauge.labels(symbol=runtime.symbol, bias=direction).set(int(now_ms - ev.ts_ms))
        except Exception:
            pass

        if tick.get("mock_force"):
             self.logger.warning("TRACE 5: Computing Confidence")

        confidence = await self._compute_confidence(runtime, indicators, confirmations, side=direction, kind=primary_reason, worker_lag_ms=worker_lag_ms)
        indicators["confidence"] = confidence

        # ------------------------------------------------------------
        # Phase E+: Liquidity regime (risk overlay)
        # ------------------------------------------------------------
        # Uses:
        #  - spread from BookSnapshot (top5)
        #  - depth_usd_min_5 from top5 volumes * mid
        #  - book_rate_ema from runtime
        #  - book_stale_ms from tick_ts - last_book_ts_ms
        try:
            snap = getattr(runtime, "last_book", None)
            bts = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
            book_stale_ms = int(tick_ts - bts) if (bts > 0 and tick_ts >= bts) else int(10**9)
            if snap is not None:
                mid = 0.5 * (float(snap.best_bid_px) + float(snap.best_ask_px))
                depth_qty = float(min(snap.depth_5_bid_vol, snap.depth_5_ask_vol))
                depth_usd_min_5 = float(depth_qty * max(mid, 1e-9))
                spread_bps = float(getattr(snap, "spread_bps", 0.0) or 0.0)
            else:
                depth_usd_min_5 = 0.0
                spread_bps = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)

            # Use the service to calculate regime from raw metrics
            liq_ev = runtime.liq_service.update(
                ts_ms=int(tick_ts)
                spread_bps=float(spread_bps)
                depth_min_5_usd=float(depth_usd_min_5)
                book_rate_hz=float(getattr(runtime, "book_rate_ema", 0.0) or 0.0)
            )
            
            # Update runtime state
            runtime.last_liq_score = liq_ev.score
            runtime.last_liq_regime = liq_ev.regime
            
            # Metrics
            indicators[IK.LIQ_SCORE] = float(liq_ev.score)
            indicators[IK.LIQ_REGIME] = str(liq_ev.regime)
            
            # Export thresholds for visibility/debugging
            thr = runtime.liq_service.thresholds()
            indicators["liq_spread_warn"] = float(thr.spread_warn_bp)
            indicators["liq_spread_crit"] = float(thr.spread_crit_bp)
            indicators["liq_depth_warn"] = float(thr.depth_warn_usd)
            indicators["liq_rate_warn"] = float(thr.rate_warn_hz)
            
            # Backward compatibility for logs/other modules
            runtime.liq_score = float(liq_ev.score)
            runtime.liq_regime = str(liq_ev.regime)
            runtime.last_liq = {"score": liq_ev.score, "regime": liq_ev.regime}

            indicators[IK.LIQ_DEPTH_USD_5] = float(depth_usd_min_5)
            indicators[IK.LIQ_SPREAD_BPS] = float(spread_bps)
            indicators["liq_book_rate_hz"] = float(getattr(runtime, "book_rate_ema", 0.0) or 0.0)
            indicators["liq_book_stale_ms"] = int(book_stale_ms)
            if liq.why:
                indicators["liq_why"] = str(liq.why)
        except Exception:
            pass

        # Log the confidence for this signal
        # Log the confidence for this signal (sampled)
        if primary_reason == "weak_progress":
            if runtime.weak_signal_log_sampler.should_log("weak_progress"):
                self.logger.info("[%s] emit signal %s conf=%.1f%%", runtime.symbol, primary_reason, confidence * 100.0)
        elif primary_reason == "absorption":
            # Log every 10,000th absorption signal
            runtime.absorption_signal_count += 1
            if runtime.absorption_signal_count % 10000 == 0:
                self.logger.info("[%s] emit signal %s conf=%.1f%%", runtime.symbol, primary_reason, confidence * 100.0)
        else:
            # Log other signals sampled at 1/1000
            if runtime.signal_emit_log_sampler.should_log(primary_reason):
                self.logger.info("[%s] emit signal %s conf=%.1f%%", runtime.symbol, primary_reason, confidence * 100.0)

        # Фильтр по минимальной уверенности
        try:
            min_conf_pct = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70"))
        except Exception:
            min_conf_pct = 80.0

        # Override из config, который загрузился через OrderFlowConfigLoader
        spec_min_conf = runtime.config.get("signal_min_conf", runtime.config.get("min_conf"))
        if spec_min_conf is not None:
            try:
                min_conf_pct = float(spec_min_conf)
            except Exception:
                pass

        # EXPERT RELAXATION (2026-01-30):
        # Meme coins often have volatile confidence scores. For calibration purposes
        # we want to capture signals even with lower confidence (pushed to Virtual).
        # Standard floor for memes in Instance 2 is 30%.
        # Can be disabled via env: {PREFIX}_CONF_RELAX_DISABLE=true or CONF_RELAX_DISABLE=true
        # Can be overridden via env: {PREFIX}_CONF_RELAX_MAX=70 (sets max relaxation threshold)
        from core.instrument_config import symbol_env_prefix
        prefix = symbol_env_prefix(runtime.symbol)
        is_meme = prefix in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF")
        if is_meme:
            # Check for per-symbol disable
            symbol_disable = _to_bool(os.getenv(f"{prefix}_CONF_RELAX_DISABLE", ""))
            global_disable = _to_bool(os.getenv("CONF_RELAX_DISABLE", "false"))
            
            if symbol_disable or global_disable:
                # Relaxation disabled for this symbol
                pass
            else:
                # Check for per-symbol override of max relaxation threshold
                relax_max_str = os.getenv(f"{prefix}_CONF_RELAX_MAX", os.getenv("CONF_RELAX_MAX", "30.0"))
                try:
                    relax_max = float(relax_max_str)
                except (ValueError, TypeError):
                    relax_max = 30.0
                
                original_min_conf = min_conf_pct
                min_conf_pct = min(min_conf_pct, relax_max)
                if original_min_conf > relax_max:
                    # Log every 10,000th message
                    cnt = self.conf_relax_counters.get(runtime.symbol, 0) + 1
                    self.conf_relax_counters[runtime.symbol] = cnt
                    if cnt % 10000 == 0:
                        self.logger.info("✅ [CONF-RELAX] (%s) Relaxed min_conf: %.1f%% -> %.1f%% (meme=%s prefix=%s relax_max=%.1f%%) (x%d)", 
                                         runtime.symbol, original_min_conf, min_conf_pct, is_meme, prefix, relax_max, cnt)

        # ------------------------------------------------------------
        # Phase E: OFI stability evidence (TTL + book health)
        # ------------------------------------------------------------
        # OFI is harder to fake than snapshot OBI because it is incremental.
        # Default: SOFT confirmation (does not affect min_confirmations).
        try:
            if int(indicators.get("book_health_ok", 1) or 1) == 1:
                ev = getattr(runtime, "last_ofi_event", None)
                if isinstance(ev, dict):
                    ots = int(ev.get("ts_ms", 0) or 0)
                    ttl = int(runtime.config.get("ofi_event_ttl_ms", 15000) or 15000)
                    if ots > 0 and 0 <= (now_ms - ots) <= ttl:
                        indicators["ofi"] = float(ev.get("ofi", 0.0) or 0.0)
                        indicators["ofi_z"] = float(ev.get("ofi_z", 0.0) or 0.0)
                        indicators["ofi_stable_secs"] = float(ev.get("stable_secs", 0.0) or 0.0)
                        indicators["ofi_stability_score"] = float(ev.get("stability_score", 0.0) or 0.0)
                        indicators["ofi_stable"] = int(ev.get("stable", 0) or 0)
                        indicators["ofi_age_ms"] = int(now_ms - ots)

                        # direction match -> add confirmation
                        if int(ev.get("stable", 0) or 0) == 1:
                            bias = str(ev.get("direction", "") or "").upper()
                            if bias == str(direction).upper():
                                confirmations.append(f"ofi_stable={float(indicators['ofi_stable_secs']):.1f}s")
        except Exception:
            pass

        min_conf = min_conf_pct / 100.0

        if tick.get("mock_force"):
             self.logger.warning("TRACE 6: Confidence Check. conf=%f min=%f", confidence, min_conf)

        # ------------------------------------------------------------
        # G9 · CONFIDENCE-GATE (Calibrated Confidence Filter)
        # ------------------------------------------------------------
        cal_mode = os.getenv("CONF_CAL_MODE", "raw").lower()
        conf_gate_mode = "raw"
        conf_gate_reason = "baseline"
        cal_proof_valid = False
        
        if cal_mode == "cal_always":
            conf_gate_mode = "calibrated"
            conf_gate_reason = "forced"
        elif cal_mode == "cal_after_proof":
            proof = getattr(runtime, "conf_cal_proof", None)
            if proof and isinstance(proof, dict) and int(tick_ts) - int(proof.get("ts_ms", 0)) <= 6 * 3600 * 1000:
                conf_gate_mode = "calibrated"
                conf_gate_reason = "proven"
                cal_proof_valid = True
            else:
                conf_gate_mode = "raw"
                conf_gate_reason = "no_proof"

        # Apply canary if enabled
        try:
            canary_share = float(runtime.config.get("conf_cal_canary_share", os.getenv("CONF_CAL_CANARY_SHARE", "0.0")))
        except Exception:
            canary_share = 0.0

        if conf_gate_mode == "calibrated" and canary_share > 0.0:
            import hashlib
            # Deterministic pseudo-random based on symbol and tick session (hourly)
            hash_input = f"{runtime.symbol}:{(int(tick_ts)//3600000)}"
            h = int(hashlib.md5(hash_input.encode()).hexdigest()[:8], 16) / 0xffffffff
            if h >= canary_share:
                conf_gate_mode = "raw"
                conf_gate_reason = "canary_skip"

        ab_mode = os.getenv("CONF_CAL_AB_MODE", "off").lower()
        if ab_mode != "off":
            indicators["conf_cal_ab_mode"] = ab_mode

        # Indicators export
        indicators["confidence_gate"] = 1 if confidence >= min_conf else 0
        indicators["confidence_gate_mode"] = conf_gate_mode
        indicators["confidence_gate_reason"] = conf_gate_reason
        indicators["confidence_decision"] = "pass" if confidence >= min_conf else "fail"
        indicators["confidence_cal_proof_valid"] = int(cal_proof_valid)

        # Strict confidence filter
        if confidence < min_conf:
             disabled = _to_bool(os.getenv("DISABLE_CONFIDENCE_FILTER", os.getenv("CRYPTO_DISABLE_CONFIDENCE_FILTER", runtime.config.get("disable_confidence_filter", "false"))))
             if disabled:
                 self.logger.info("ℹ️ (%s) [LOW-CONF] Signal confidence %.2f%% < %.2f%% but filter is DISABLED.", runtime.symbol, confidence * 100.0, min_conf_pct)
             else:
                 self.low_conf_counters[runtime.symbol] = self.low_conf_counters.get(runtime.symbol, 0) + 1
                 sampled_warning(logger, "LOW_CONF"
                     "🛑 [LOW-CONF] (%s) Signal filtered: conf=%.2f%% < min_conf=%.2f%%. (x%d)"
                     runtime.symbol, confidence * 100.0, min_conf_pct, self.low_conf_counters[runtime.symbol]
                 )
                 return None
        
        # Telemetry: Hidden Divergence Usage
        if indicators.get("hidden_div_used"):
             from services.orderflow.metrics import of_hidden_divergence_signal_total
             of_hidden_divergence_signal_total.labels(symbol=runtime.symbol).inc()

        runtime.signal_count += 1
        
        # Executable Entry Pricing (P0)
        executable_entry = float(price)
        try:
            if runtime.last_book:
                bts_entry = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
                # Max staleness 2s for pricing to avoid bad fills
                if bts_entry > 0 and (tick_ts - bts_entry) < 2000:
                    if direction == "LONG":
                        asks_entry = runtime.last_book.get("asks")
                        if asks_entry and len(asks_entry) > 0:
                             executable_entry = float(asks_entry[0][0])
                    else:
                        bids_entry = runtime.last_book.get("bids")
                        if bids_entry and len(bids_entry) > 0:
                             executable_entry = float(bids_entry[0][0])
                    
                    # Sanity: if deviation > 10% from tick price, revert to tick (bad book?)
                    if abs(executable_entry - price) / (price + 1e-9) > 0.10:
                        executable_entry = float(price)
        except Exception:
            executable_entry = float(price)

        # Initialize payload early for candidate/pressure enrichment
        payload = {
            "symbol": runtime.symbol
            "ts_ms": int(tick_ts)
            "tick_ts": int(tick_ts)
            # A1: legacy alias – the actual emit time is stamped by SignalPipeline.stamp_emit_and_observe_async.
            "ts_emit_ms": int(tick_ts)
            # P4 latency contract: wall-clock at feature-computation completion.
            # feature_to_emit = ts_emit_ms - ts_feature_ms = publish_signal() duration (H4).
            # Was tick_ts (exchange time) → feature_to_emit was identical to end_to_end_event.
            "ts_feature_ms": get_ny_time_millis()

            "price": float(price)
            "entry": float(executable_entry)
            "direction": direction
            "side": direction.lower()
            "indicators": indicators
            "confirmations": list(confirmations)
            "confidence": float(confidence)
            "signal_id": str(signal_id)
            "entry_tag": str(primary_reason)
            "is_virtual": bool(int(indicators.get("is_virtual", 0) or 0))
        }
        
        self._log_metrics(runtime)


        # === Pressure snapshot attached to every candidate payload ===
        try:
            ps = runtime.pressure.snapshot(now_ms=int(tick_ts))
            payload["pressure"] = {
                "per_min_ema": float(ps.per_min_ema)
                "cd_rate_ema": float(ps.cd_rate_ema)
                "n_raw": int(ps.n_raw)
                "n_cd": int(ps.n_cd)
            }
            hi_th = float(runtime.config.get("pressure_hi_per_min", 60.0))
            payload["pressure"]["pressure_hi"] = 1 if ps.per_min_ema >= hi_th else 0
        except Exception:
            pass

        # Attach microstructure context (from last book/bar)
        try:
            payload.setdefault("micro", {})
            payload["micro"]["spread_bps"] = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
            payload["micro"]["spread_z"] = float(getattr(runtime, "last_spread_z", 0.0) or 0.0)
            # book freshness/rate
            bts = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
            book_stale_ms = int(tick_ts - bts) if (bts > 0 and tick_ts > 0 and tick_ts >= bts) else int(10**9)
            payload["micro"]["book_stale_ms"] = int(book_stale_ms)
            payload["micro"]["book_rate_ema"] = float(getattr(runtime, "book_rate_ema", 0.0) or 0.0)
            payload["micro"]["book_rate_z"] = float(getattr(runtime, "book_rate_z", 0.0) or 0.0)
            payload["micro"]["book_churn_score"] = float(getattr(runtime, "book_churn_score", 0.0) or 0.0)
            payload["micro"]["book_churn_hi"] = int(getattr(runtime, "book_churn_hi", 0) or 0)
            if book_stale_ms_gauge is not None:
                book_stale_ms_gauge.labels(symbol=runtime.symbol).set(float(book_stale_ms))
        except Exception:
            pass

        if runtime.last_book:
            payload["book_ts"] = runtime.last_book.get("ts")
            bids = runtime.last_book.get("bids") or []
            asks = runtime.last_book.get("asks") or []
            if bids:
                payload["best_bid"] = bids[0][0]
            if asks:
                payload["best_ask"] = asks[0][0]

        # ------------------------------------------------------------------
        # 🛡️ G10 ADVERSE SELECTION GATE (P0)
        # ------------------------------------------------------------------
        adverse_enabled = bool(int(runtime.config.get("adverse_check_enable", 0)))
        adv_shadow_only = False

        # G10 Calibrator override: auto-enable per-symbol from dynamic_cfg
        try:
            adv_calib_mode = str(getattr(runtime, "dynamic_cfg", {}).get(DK.ADV_CALIB_MODE, "") or "")
            if adv_calib_mode == "enforce":
                adverse_enabled = True
                indicators["adv_calib_mode"] = "enforce"
            elif adv_calib_mode == "shadow":
                adverse_enabled = True
                adv_shadow_only = True
                indicators["adv_calib_mode"] = "shadow"
        except Exception:
            pass

        if adverse_enabled:
            gate_res = self._eval_g10_adverse_gate(runtime, payload, tick_ts)
            if gate_res == "veto_reversal":
                if adv_shadow_only:
                    # Shadow mode: log the veto but don't block
                    indicators["g10_reversal_vetoed"] = 1
                    indicators["g10_shadow_only"] = 1
                else:
                    indicators["g10_reversal_vetoed"] = 1
                    return None
            elif gate_res == "wait_continuation":
                if adv_shadow_only:
                    indicators["g10_continuation_shadow"] = 1
                    # FIX: Clear pending payload so it is not double-emitted by microbar_closed
                    runtime.pending_adverse_payload = None
                else:
                    return None
            elif gate_res == "pass":
                if "reversal" in str(indicators.get("strong_gate_scn", "") or "").lower():
                    indicators["g10_reversal_passed"] = 1

        try:
            _dt_gates = (time.monotonic_ns() - _t0_gates) / 1_000
            process_tick_gates_us.labels(symbol=runtime.symbol).observe(_dt_gates)
        except Exception:
            pass

        return await self._emit_payload(runtime, payload, int(tick_ts))

    def _eval_g10_adverse_gate(self, runtime: SymbolRuntime, payload: Dict[str, Any], tick_ts: int) -> str:
        """
        Evaluate G10 Adverse Selection Gate logic.
        Returns "pass", "veto_reversal", or "wait_continuation".
        """
        indicators = payload.get("indicators", {})
        scn = str(indicators.get("strong_gate_scn", "") or "").lower()
        if not scn:
            scn = "reversal" if int(indicators.get("sweep", 0) or 0) == 1 else "continuation"
        
        # REVERSAL CHECK (Immediate Veto)
        if "reversal" in scn:
            has_reclaim = bool(indicators.get("cvd_reclaim_ok", 0))
            has_absorb = bool(indicators.get("absorption_volume", 0) > 0)
            has_obi = bool(indicators.get("obi_stable", 0))
            has_ofi = bool(indicators.get("ofi_stable", 0))
            
            if not (has_reclaim or has_absorb or has_obi or has_ofi):
                g10_adverse_veto_total.labels(gate="G10_ADVERSE_REVERSAL").inc()
                return "veto_reversal"
        
        # CONTINUATION CHECK (Wait for Bar)
        elif "continuation" in scn:
            runtime.pending_adverse_payload = payload
            runtime.pending_adverse_ts_ms = int(tick_ts)
            return "wait_continuation"
            
        return "pass"

    async def _emit_payload(self, runtime: SymbolRuntime, payload: Dict[str, Any], now_ms: int) -> Optional[Dict[str, Any]]:
        """
        Internal helper: Cooldown -> Burst -> Return/Buffer.
        Used by process_tick AND _on_microbar_closed (deferred execution).
        """
        # --- CLOCK SKEW ELIMINATION ---
        # Explicitly freeze the triggering event time (exchange clock) into the payload.
        payload["tick_ts"] = int(now_ms)
        payload["ts_ms"] = int(now_ms)
        
        indicators = payload.get("indicators", {})
        confidence = float(payload.get("confidence", 0.0))
        
        scenario = str(indicators.get("strong_gate_scn", "") or "")
        if not scenario:
            scenario = "reversal" if int(indicators.get("sweep", 0) or 0) == 1 else "continuation"
            
        cooldown_ms = _cooldown_ms_for(runtime, scenario=scenario, now_ms=now_ms
                                        new_dir=str(payload.get("direction", "") or ""))
        last_emit_ts = int(getattr(runtime, "last_signal_ts", 0) or 0)
        age = int(now_ms) - last_emit_ts if last_emit_ts > 0 else 10**9

        # define score for candidate selection (always)
        of_score = float(indicators.get("of_confirm_score", 0.0))
        # Recalculate score from payload data just in case
        score = of_score if of_score > 0 else confidence

        if age < cooldown_ms:
            # --- Pressure Proxy: record deterministic cooldown hit ---
            try:
                runtime.pressure.on_cooldown_hit(ts_ms=int(now_ms))
            except Exception:
                pass

            # Buffer into pending_payload for post-cooldown emission
            cand_score = float(score)
            if runtime.pending_payload is None or cand_score > float(getattr(runtime, "pending_score", 0.0) or 0.0):
                runtime.pending_payload = payload
                runtime.pending_score = float(cand_score)
                runtime.pending_ts_ms = int(now_ms)
                runtime.pending_replaced += 1
            
            cur_dir = str(payload.get("direction", "") or "")
            last_dir = str(getattr(runtime, "last_emit_dir", "NONE") or "NONE")
            is_reversal = cur_dir and last_dir not in ("NONE", "") and cur_dir.upper() != last_dir.upper()
            logger.warning(
                "🛑 [COOLDOWN] (%s) Signal buffered (age=%dms < %dms, dir=%s→%s%s). Pending updated=%s"
                runtime.symbol, age, cooldown_ms, last_dir, cur_dir
                " REVERSAL" if is_reversal else "", "YES"
            )
            return None

        # Cooldown window open: check if we have better pending
        if runtime.pending_payload is not None:
            pending_score = float(getattr(runtime, "pending_score", 0.0) or 0.0)
            cur_score = float(score)
            if pending_score >= cur_score:
                payload = runtime.pending_payload
                # upgrade score if pending was better
                score = pending_score
            runtime.pending_payload = None
            runtime.pending_score = 0.0

        # Burst Mode Check (Consolidated)
        force_burst = bool(indicators.get("pressure_extreme_flag", 0))
        use_burst = bool(int(os.getenv("CRYPTO_BURST_ENABLE", "0"))) or force_burst
        
        # DEBUG: Log that signal passed all filters and is about to enter burst
        # logger.info(
        #     "✅ [PRE-BURST] (%s) Signal passed all filters: dir=%s conf=%.1f%% score=%.2f"
        #     runtime.symbol, payload.get("direction"), confidence*100, score
        # )
        
        if use_burst:
            try:
                out = None
                async with runtime.burst_mu:
                    was_active = runtime.burst.st.active
                    runtime.burst.consider(
                        ts_ms=int(now_ms)
                        cand=BurstCandidate(ts_ms=int(now_ms), score=float(score), payload=payload)
                    )
                    # EXPERT FIX: Check flush immediately to prevent 'stuck' signals
                    pass # Burst flush handled by dedicated loop
                    
                    burst_active_gauge.labels(symbol=runtime.symbol).set(1 if runtime.burst.st.active else 0)

                # Do not emit now; we will flush at deadline.
                return None
            except Exception:
                pass # Bookkeeping moved to SignalPipeline
                return payload

        # No burst: emit immediately
        return payload


    async def _compute_confidence(
        self
        runtime: SymbolRuntime
        indicators: Dict[str, Any]
        confirmations: Sequence[str]
        *
        side: str
        kind: str
        worker_lag_ms: float = 0.0
    ) -> float:
        """
        Делегируем расчёт в универсальный ConfidenceScorer (services/signal_confidence.py).
        """
        from types import SimpleNamespace

        def _get(name: str, default=0.0):
            v = indicators.get(name)
            return v if v is not None else default

        ctx = SimpleNamespace(
            z_delta=_get("delta_z", _get("z", 0.0))
            delta=_get("delta", 0.0)
            obi_avg=_get("obi", 0.0)
            obi_sustained=bool(indicators.get("obi_sustained", False))
            obi_avg_20=_get("obi_20", 0.0)
            obi_sustained_20=bool(indicators.get("obi_sustained_20", False))
            microprice_shift_bps_20=_get("microprice_shift_bps_20", 0.0)
            wall_bid=bool(indicators.get("wall_bid", False))
            wall_ask=bool(indicators.get("wall_ask", False))
            wall_bid_dist_bps=_get("wall_bid_dist_bps", 0.0)
            wall_ask_dist_bps=_get("wall_ask_dist_bps", 0.0)
            depletion_score=_get("depletion_score", 0.0)
            refill_score=_get("refill_score", 0.0)
            impact_proxy=_get("impact_proxy", 0.0)
            spread_bps=_get("spread_bps", 0.0)
            realized_ema_bps=_get("realized_ema_bps", 0.0)
            adverse_ratio_ema=_get("adverse_ratio_ema", 0.0)
            market_mode=indicators.get("market_mode", "mixed") or "mixed"
            l2_age_ms=_get("l2_age_ms", 0.0)
            l2_is_stale=bool(indicators.get("l2_is_stale", False))
            taker_buy_rate_ema=_get("taker_buy_rate_ema", 0.0)
            taker_sell_rate_ema=_get("taker_sell_rate_ema", 0.0)
            cancel_to_trade_ask=_get("cancel_to_trade_ask", 0.0)
            cancel_to_trade_bid=_get("cancel_to_trade_bid", 0.0)
            eta_fill_ask_sec=_get("eta_fill_ask_sec", 0.0)
            eta_fill_bid_sec=_get("eta_fill_bid_sec", 0.0)
            weak_progress=bool(indicators.get("weak_progress", False))
            # Phase E+: weak progress trend (history-based)
            weak_recent_cnt=int((indicators.get("weak_recent_cnt") if indicators.get("weak_recent_cnt") is not None else indicators.get("weak_recent_count", 0)) or 0)
            weak_recent_window=int(indicators.get("weak_recent_window", 0) or 0)
            # Phase E+: OBI stability quality (duration + persistence score)
            obi_stable_secs=float(indicators.get("obi_stable_secs", 0.0) or 0.0)
            obi_stability_score=float(indicators.get("obi_stability_score", 0.0) or 0.0)
            # Phase E+: OFI stability quality
            ofi_stable_secs=float(indicators.get("ofi_stable_secs", 0.0) or 0.0)
            ofi_stability_score=float(indicators.get("ofi_stability_score", 0.0) or 0.0)
            # Liquidity regime (risk overlay)
            liq_score=float(indicators.get(IK.LIQ_SCORE, 0.0) or 0.0)
            liq_regime=str(indicators.get(IK.LIQ_REGIME, getattr(runtime, "liq_regime", "normal")) or "normal")
            # Phase E+: footprint edge absorb evidence
            fp_edge_absorb=bool(indicators.get("fp_edge_absorb", False))
            fp_edge_absorb_strength=float((indicators.get("fp_edge_absorb_strength") if indicators.get("fp_edge_absorb_strength") is not None else indicators.get("fp_edge_strength", 0.0)) or 0.0)
            iceberg_refresh=_get("iceberg_refresh", 0.0)
            iceberg_duration=_get("iceberg_duration", 0.0)
            absorption_volume=_get("absorption_volume", 0.0)
            # Phase D+: footprint data for scoring
            confirmations=list(confirmations or [])
            fp_absorb_min_score=float(runtime.config.get("fp_absorb_min_score", 1.0))
            fp_absorb_bonus_w=float(runtime.config.get("fp_absorb_bonus_w", 0.06))
            fp_imb_bonus_w=float(runtime.config.get("fp_imb_bonus_w", 0.03))
            fp_bonus_cap=float(runtime.config.get("fp_bonus_cap", 0.08))
            lag_ms=float(worker_lag_ms)
        )

        try:
            conf, parts = await self.conf_scorer.score(kind=kind or "custom", side=side, ctx=ctx)
            indicators["confidence_breakdown"] = {
                "base": round(float(parts.get("base", 0.0)), 4)
                "mult": round(float(parts.get("mult", 1.0)), 4)
                "pen_total": round(float(parts.get("pen_total", 0.0)), 4)
            }
            if "ml_shadow_conf01" in parts:
                indicators["confidence_breakdown"]["ml_shadow_conf01"] = round(float(parts["ml_shadow_conf01"]), 4)
            
            # Apply RollingPercentileCalibrator if configured
            if getattr(self, "score_calibrator", None) is not None:
                # pass 'update=True' to keep filling the sliding window history
                try:
                    cal_pct = self.score_calibrator.calibrate(
                         symbol=str(runtime.symbol or "")
                         kind=str(kind or "custom")
                         final_score=float(conf)
                         update=True
                    )
                    conf = cal_pct / 100.0
                    indicators["confidence_calibrated_pct"] = round(cal_pct, 2)
                except Exception as e:
                    self.logger.error("Error in RollingPercentileCalibrator: %s", e)
            
            return round(float(conf), 4)
        except Exception as exc:
            self.logger.warning("confidence scorer fallback due to error: %s", exc)
            return float(0.1)

    def _get_atr_for_symbol(self, symbol: str, cfg: Dict[str, Any], tf_override: Optional[str] = None, runtime: Optional[Any] = None) -> Optional[float]:
        """
        Delegates to MarketStateService.
        """
        try:
            # Single source of truth: atr_tf_selected (via canonical resolver)
            tf = str(tf_override or (runtime.get_atr_tf_selected() if runtime else None) or cfg.get("atr_tf") or os.getenv("ATR_TF", "5m") or "5m")
            return self.market_state.get_atr(symbol, tf)
        except Exception:
            return None


    async def publish_signal(self, runtime: SymbolRuntime, signal: Dict[str, Any]) -> None:
        """
        Delegates signal publishing to SignalPipeline.
        """
        await self.signal_pipeline.publish_signal(runtime, signal)
    async def _publish_orders_queue(self, runtime: SymbolRuntime, signal: Dict[str, Any]) -> None:
        """
        Публикует команду в очередь ордеров (MT5=Stream, Binance=List).
        Схема: order_creation.md (минимально необходимый payload).
        """
        symbol = signal.get("symbol") or runtime.symbol
        ts_value = signal.get("tick_ts") or signal.get("ts_event_ms") or signal.get("generated_at")
        if not ts_value:
            logger.warning("⚠️ (%s) Нет временной метки сигнала, пропускаем orders:queue", runtime.symbol)
            return

        # Unified side normalization (P0)
        side_norm = normalize_side_3_safe(signal.get("direction") or signal.get("side") or "")
        if side_norm is None:
            logger.warning("⚠️ (%s) _publish_orders_queue: unknown direction=%r side=%r (skip)"
                           symbol, signal.get("direction"), signal.get("side"))
            return
        direction = side_norm.execution.lower() # buy/sell
        venue = str(signal.get("venue") or "mt5").lower()

        reason = signal.get("reason") or "delta_spike"

        # Signal ID generation (P0)
        signal_id = generate_signal_id(
            kind=str(signal.get("kind") or "spike")
            symbol=symbol
            ts_ms=int(ts_value)
            direction=side_norm.internal
        )

        order_cmd = {
            "id": f"order-{symbol}-{ts_value}"
            "sid": signal_id
            "signal_id": signal_id
            "symbol": symbol
            "type": "market"
            "direction": direction
            "side": side_norm.execution
            "side_int": side_norm.numeric
            "source": "CryptoOrderFlow"
            "venue": venue
            "reason": reason
        }

        try:
            if venue == "mt5":
                if not self.orders_queue_mt5:
                    logger.warning("⚠️ (%s) orders_queue_mt5 не задан, пропуск", runtime.symbol)
                    return
                # MT5 uses Redis Stream
                await self.redis.xadd(self.orders_queue_mt5, order_cmd, maxlen=1000, approximate=True)
            else:
                # Binance uses Redis List
                queue = self.orders_queue_binance or RS.ORDERS_QUEUE_BINANCE
                await self.redis.lpush(queue, json.dumps(order_cmd))
        except RedisError as exc:
            logger.warning("⚠️ (%s) Не удалось отправить в очередь ордеров (%s): %s", runtime.symbol, venue, exc)

    # ── Парсинг сообщений ──────────────────────────────────────────────────────

    def _parse_tick_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if "data" in payload:
            try:
                nested = json.loads(payload["data"])
            except json.JSONDecodeError:
                nested = {}
        else:
            nested = {}

        merged = {**payload, **nested}
        ts_ms = normalize_epoch_ms(merged.get("ts") or merged.get("event_time"))
        tick: Dict[str, Any] = {
            "symbol": merged.get("symbol")
            "ts": int(ts_ms or 0),      # legacy epoch ms (keep)
            "ts_ms": int(ts_ms or 0),   # source of truth epoch ms
            "price": _safe_float(merged.get("price") or merged.get("last") or merged.get("mid"))
            "last": _safe_float(merged.get("last"))
            "bid": _safe_float(merged.get("bid"))
            "ask": _safe_float(merged.get("ask"))
            "qty": merged.get("qty") or merged.get("volume")
            "side": str(merged.get("side") or merged.get("trade_side") or "UNKNOWN").upper()
            "is_buyer_maker": merged.get("is_buyer_maker")
            "written_at": _safe_int(merged.get("written_at"))
        }

        # Нормализация числовых полей и buyer/maker + mid
        try:
            qty = float(tick.get("qty", 0.0) or 0.0)
        except (TypeError, ValueError):
            qty = 0.0
        tick["qty"] = qty

        side_upper = str(tick.get("side") or "").upper()
        if side_upper == "SELL":
            tick["is_buyer_maker"] = True
        elif side_upper == "BUY":
            tick["is_buyer_maker"] = False

        bid = _safe_float(tick.get("bid"))
        ask = _safe_float(tick.get("ask"))
        if bid and ask:
            tick["mid"] = (bid + ask) / 2.0
        else:
            tick["mid"] = _safe_float(tick.get("price"))

        return tick

    @staticmethod
    def _env_bool(name: str, default: Optional[bool] = None) -> Optional[bool]:
        """Читает boolean переменную окружения с fallback."""
        raw = os.getenv(name)
        if raw is None:
            return default
        return raw.lower() in ("1", "true", "yes", "on")


    def _log_metrics(self, runtime: SymbolRuntime) -> None:
        """
        Периодический сброс метрик в Prometheus.
        """
        now = time.time()
        if now - runtime.last_metrics_ts < 30:
            return
        runtime.last_metrics_ts = now
        
        # Count how many times _log_metrics has been called
        if not hasattr(runtime, '_metrics_call_count'):
            runtime._metrics_call_count = 0
        runtime._metrics_call_count += 1

        # Only log every 10000th call
        if runtime._metrics_call_count % 10000 != 0:
            return

        logger.info(
            "METRICS symbol=%s ticks=%d delta_trig=%d signals=%d"
            runtime.symbol
            runtime.tick_count
            runtime.delta_triggers
            runtime.signal_count
        )

    async def _on_microbar_closed(self, runtime: SymbolRuntime, bar: MicroBar) -> None:
        """
        In-memory обработка события bar_close.
        Здесь можно делать более тяжелые вычисления (но только на bar_close, не на каждом тике):
        - swings
        - divergences
        - RSI(price) и RSI(CVD)
        - New: CVD Snapshots & Dedicated Div Stream
        """
        try:
            await runtime.ensure_dn_loaded(self.redis)
            if bool(int(os.getenv("ATR_TF_CALIB_ENABLE", "1"))):
                await runtime.ensure_atr_tf_loaded(self.redis)
            # ATR sanity selector state (source preference)
            try:
                if bool(int(runtime.config.get("atr_sanity_enable", int(os.getenv("ATR_SANITY_ENABLE", "1"))) or 1)):
                    await runtime.ensure_atr_sanity_loaded(self.redis)
            except Exception:
                pass
            # ATR(bps) calibrator (lazy-load once)
            if bool(int(os.getenv("ATR_BPS_CALIB_ENABLE", "1"))):

                await runtime.ensure_atr_bps_loaded(self.redis)
            if bool(int(os.getenv("ATR_TF_CALIB_ENABLE", "1"))):
                await runtime.ensure_atr_tf_loaded(self.redis)
            if bool(int(os.getenv("ATR_BPS_CALIB_ENABLE", "1"))):
                await runtime.ensure_atr_bps_loaded(self.redis)
            # Load persisted ATR sanity states once (lazy)
            try:
                await runtime.ensure_atr_sanity_loaded(self.redis)
            except Exception:
                pass
        except Exception:
            pass


        # --- ATR sanity range proxy update (roll microbars into atr_tf buckets) ---
        try:
            o = float(getattr(bar, "open", 0.0) or 0.0)
            h = float(getattr(bar, "high", 0.0) or 0.0)
            l = float(getattr(bar, "low", 0.0) or 0.0)
            c = float(getattr(bar, "close", 0.0) or 0.0)
            ts = int(getattr(bar, "end_ts_ms", 0) or 0)
            if ts > 0:
                # ADVERSE Selection Check: Continuation Verify
                if runtime.pending_adverse_payload:
                    sig = runtime.pending_adverse_payload
                    # Check timeout (e.g. 2 * tf or 5s)
                    age_adv = ts - int(runtime.pending_adverse_ts_ms or 0)
                    if 0 < age_adv < 5000:
                        s_dir = str(sig.get("direction", "")).upper()
                        # Verified if bar closes in favor
                        verified = False
                        if s_dir == "LONG" and c > o: verified = True
                        elif s_dir == "SHORT" and c < o: verified = True
                        
                        if verified:
                            # Log every 10,000th message
                            cnt = self.adverse_continuation_counters.get(runtime.symbol, 0) + 1
                            self.adverse_continuation_counters[runtime.symbol] = cnt
                            if cnt % 10000 == 0:
                                logger.info("✅ [ADVERSE] Continuation Verified! Emitting buffered signal. (x%d)", cnt)
                            # inject late metrics
                            sig["adverse_wait_ms"] = age_adv
                            # EMIT
                            final_sig = await self._emit_payload(runtime, sig, ts)
                            if final_sig:
                                preprocess_signal_for_publish(final_sig, runtime.symbol, "CryptoOrderFlow", logger)
                                await self.publish_signal(runtime, final_sig)
                        else:
                            g10_adverse_veto_total.labels(gate="G10_ADVERSE_CONTINUATION").inc()
                    else:
                        g10_adverse_veto_total.labels(gate="G10_ADVERSE_TIMEOUT").inc()
                    
                    # Clear buffer after check (one-shot)
                    runtime.pending_adverse_payload = None
                    runtime.pending_adverse_ts_ms = 0

                runtime.atr_range_agg.push_microbar(end_ts_ms=ts, o=o, h=h, l=l, c=c)
                snap = runtime.atr_range_agg.snapshot()
                runtime.dynamic_cfg[DK.ATR_RANGE_TF_MS] = int(snap.tf_ms)
                runtime.dynamic_cfg[DK.ATR_RANGE_N] = int(snap.n)
                runtime.dynamic_cfg[DK.ATR_RANGE_P50_BPS] = float(snap.p50)
                runtime.dynamic_cfg[DK.ATR_RANGE_P95_BPS] = float(snap.p95)
        except Exception:
            pass

        # 0. Update Daily Tracker
        try:
             # Feed microbar to daily tracker (persists on day roll)
             runtime.daily_tracker.update(bar)
        except Exception:
             pass

        # 0) Dynamic Regime Update (Redis read + inline fallback)
        try:
             # Fast fetch, fall back to "na" (default)
             # Key convention: regime:{symbol} -> string "range"|"trend"|"thin"
             reg_key = f"regime:{runtime.symbol}"
             rg_val = await self.redis.get(reg_key)

             old_regime = str(getattr(runtime, "last_regime", "na") or "na")
             new_regime = "na"

             if rg_val:
                 new_regime = str(rg_val)

             # ── Inline fallback: compute regime when Redis key is absent ──
             # This covers Shard 3/3B symbols that have no handler pipeline
             # writing regime:{symbol} to Redis.
             if new_regime == "na" and self._regime_svc is not None and RegimeFeatures is not None:
                 try:
                     price = float(getattr(bar, "close", 0) or 0)
                     volume = float(getattr(bar, "vol", 0) or 0)
                     delta = float(getattr(bar, "delta_sum", 0) or 0)
                     ts_bar = int(getattr(bar, "end_ts_ms", 0) or 0)
                     if price > 0 and ts_bar > 0:
                         # Day reset
                         day_id = ts_bar // 86_400_000
                         if runtime._regime_day_id == 0 or day_id != runtime._regime_day_id:
                             runtime._regime_day_id = day_id
                             runtime._regime_open_day = price
                             runtime._regime_pv = 0.0
                             runtime._regime_vol = 0.0
                             runtime._regime_vwap = price
                             runtime._regime_cross_hist.clear()
                             runtime._regime_last_side = 0
                             runtime._regime_hold_ema = 0.0

                         # VWAP
                         if volume > 0:
                             runtime._regime_pv += price * volume
                             runtime._regime_vol += volume
                             runtime._regime_vwap = (
                                 runtime._regime_pv / runtime._regime_vol
                                 if runtime._regime_vol > 0
                                 else price
                             )

                         # Delta EMA
                         a = self._regime_delta_alpha
                         runtime._regime_delta_ema = a * delta + (1.0 - a) * runtime._regime_delta_ema

                         # Hold side (price vs VWAP persistence)
                         side = 0
                         if price > runtime._regime_vwap:
                             side = 1
                         elif price < runtime._regime_vwap:
                             side = -1

                         crossed = (
                             1
                             if (
                                 runtime._regime_last_side != 0
                                 and side != 0
                                 and side != runtime._regime_last_side
                             )
                             else 0
                         )
                         runtime._regime_cross_hist.append(crossed)
                         if side != 0:
                             runtime._regime_last_side = side

                         ha = self._regime_hold_alpha
                         runtime._regime_hold_ema = ha * float(side) + (1.0 - ha) * runtime._regime_hold_ema

                         cross_rate = (
                             sum(runtime._regime_cross_hist) / max(len(runtime._regime_cross_hist), 1)
                             if runtime._regime_cross_hist
                             else 0.0
                         )

                         # ATR quantile proxy
                         atr_q = 0.5
                         snap_atr = getattr(runtime, "atr_range_agg", None)
                         if snap_atr and hasattr(snap_atr, "snapshot"):
                             try:
                                 s_atr = snap_atr.snapshot()
                                 p50 = float(getattr(s_atr, "p50", 0) or 0)
                                 p95 = float(getattr(s_atr, "p95", 0) or 0)
                                 if p95 > 0 and p50 > 0:
                                     atr_q = min(1.0, max(0.0, p50 / p95))
                             except Exception:
                                 pass

                         features = RegimeFeatures(
                             atr_q=atr_q
                             adx_q=0.5
                             delta_ema=runtime._regime_delta_ema
                             hold_side_score=runtime._regime_hold_ema
                             vwap_cross_rate=cross_rate
                             vwap=runtime._regime_vwap
                             open_day=runtime._regime_open_day
                         )

                         new_regime = self._regime_svc.update_regime(features)

                         # Publish back to Redis for other consumers
                         if ts_bar - runtime._regime_last_pub_ms >= self._regime_pub_gap_ms:
                             try:
                                 sym = str(runtime.symbol).upper()
                                 safe_create_task(
                                     self.redis.set(
                                         f"regime:{sym}"
                                         str(new_regime)
                                         ex=self._regime_redis_ttl_sec
                                     )
                                     name=f"regime-pub-{sym}"
                                 )
                                 runtime._regime_last_pub_ms = ts_bar
                             except Exception:
                                 pass
                 except Exception:
                     pass  # fail-open

             runtime.last_regime = new_regime
             runtime.last_regime_ts_ms = get_ny_time_millis()
        except Exception:
             # fail-safe
             pass

        # ------------------------------------------------------------------
        # ATR TF Calibrator update (freshness + consistency)
        # Deterministic time: bar.end_ts_ms
        # ------------------------------------------------------------------
        try:
            if bool(int(os.getenv("ATR_TF_CALIB_ENABLE", "1"))):
                now_ts = int(getattr(bar, "end_ts_ms", 0) or 0)
                close_px = float(getattr(bar, "close", 0.0) or 0.0)
                rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                if now_ts > 0 and close_px > 0:
                    cand_str = str(runtime.config.get("atr_tf_candidates", os.getenv("ATR_TF_CANDIDATES", "1m,5m,15m")) or "")
                    cands = tuple([x.strip() for x in cand_str.split(",") if x.strip()])
                    if not cands:
                        cands = ("1m", "5m", "15m")

                    # floor hint: helps detect absurdly low ATR for current regime (optional)
                    hint_floor = float(runtime.dynamic_cfg.get(DK.ATR_BPS_TH, 0.0) or runtime.config.get("atr_bps_min_static", 0.0) or 0.0)

                    scores_inst: Dict[str, float] = {}
                    # score each tf from ATRCache meta
                    for tf in cands:
                        v, m = self.atr_cache.get_with_meta(symbol=runtime.symbol, timeframe=tf, now_ms=now_ts)
                        vv = float(v or 0.0)
                        if vv <= 0 or not m:
                            continue
                        age_ms = int((m or {}).get("age_ms", 0) or 0)
                        atr_bps = 10000.0 * (vv / close_px) if close_px > 0 else 0.0
                        # build inst score in [0..~1.5]
                        # freshness: decays with age
                        # consistency: penalize too-low vs hint
                        # NOTE: scoring function is mirrored in ATRTFCalibrator docs
                        fresh = float(1.0 / (1.0 + (max(0, age_ms) / float(max(1, int(os.getenv("ATR_TF_CALIB_MAX_AGE_MS", str(10 * 60_000))) ) / 2))))
                        cons = 1.0
                        if hint_floor > 0 and atr_bps > 0:
                            cons = max(0.0, min(1.5, float(atr_bps / hint_floor)))
                        sc = float(0.7 * fresh + 0.3 * min(1.0, cons))
                        # tiny bonus for tracker hash (more trustworthy)
                        src = str((m or {}).get("src", (m or {}).get("source", "")) or "")
                        if src == "tracker_hash":
                            sc *= 1.05
                        scores_inst[str(tf)] = float(sc)

                    runtime.atr_tf_calib.update(regime=rg, scores_inst=scores_inst, ts_ms=now_ts)
                    dec = runtime.atr_tf_calib.pick(regime=rg, default_tf=str(runtime.config.get("atr_tf", "5m") or "5m"), candidates=cands)
                    runtime.dynamic_cfg[DK.ATR_TF_SELECTED] = str(dec.tf)
                    runtime.dynamic_cfg[DK.ATR_TF_SRC] = str(dec.src)
                    runtime.dynamic_cfg[DK.ATR_TF_N] = int(dec.n)
                    runtime.dynamic_cfg[DK.ATR_TF_READY] = int(dec.ready)
                    runtime.dynamic_cfg[DK.ATR_TF_SCORES_EMA] = dict(dec.scores_ema or {})
                    runtime.dynamic_cfg[DK.ATR_TF_SCORES_INST] = dict(dec.last_scores_inst or {})
                    runtime.dynamic_cfg[DK.ATR_TF_PICKED_SCORE] = float(dec.picked_score or 0.0)
                    runtime.dynamic_cfg[DK.ATR_TF_SECOND_SCORE] = float(dec.second_score or 0.0)

        except Exception:
            pass

        # --------------------------------------------------------
        # ATR Sanity Calibrator (Source Selection) - User Diff Integration
        # --------------------------------------------------------
        try:
            if bool(int(runtime.config.get("atr_sanity_enable", int(os.getenv("ATR_SANITY_ENABLE", "1"))) or 1)):
                close_ts = int(now_ts)
                # ATR TF
                atr_tf = str(runtime.config.get("atr_tf", "5m") or "5m")
                # Normalize TF
                try:
                    tf_norm = self.atr_cache._normalize_tracker_tf(atr_tf)
                except Exception:
                    tf_norm = str(atr_tf).upper()

                cands_src = []
                try:
                    cands_src = self.atr_cache.get_candidates(symbol=runtime.symbol, timeframe=atr_tf, now_ms=close_ts)
                except Exception:
                    cands_src = []

                dec_src = runtime.atr_sanity.decide(tf_norm=tf_norm, candidates=cands_src)
                
                runtime.dynamic_cfg[DK.ATR_SRC_PREF] = str(dec_src.src_pref)
                runtime.dynamic_cfg[DK.ATR_SRC_READY] = int(dec_src.ok)
                runtime.dynamic_cfg[DK.ATR_SRC_REASON] = str(dec_src.reason)
                runtime.dynamic_cfg[DK.ATR_SRC_MISMATCH] = int(dec_src.mismatch)
                runtime.dynamic_cfg[DK.ATR_SRC_N] = int(dec_src.n)
                runtime.dynamic_cfg[DK.ATR_SRC_MEDIAN] = float(dec_src.median)
                runtime.dynamic_cfg[DK.ATR_SRC_PICKED] = float(dec_src.picked)
                
                # Persist state (throttled)
                try:
                    min_iv_ms = int(runtime.config.get("atr_sanity_persist_min_interval_ms", 300_000) or 300_000)
                    min_bars = int(runtime.config.get("atr_sanity_persist_min_bars", 30) or 30)
                    runtime._atr_sanity_bars_since_persist = int(getattr(runtime, "_atr_sanity_bars_since_persist", 0) or 0) + 1
                    last_p = int(getattr(runtime, "_atr_sanity_last_persist_ts_ms", 0) or 0)
                    due_by_time = (last_p <= 0) or (close_ts - last_p >= min_iv_ms)
                    due_by_bars = runtime._atr_sanity_bars_since_persist >= min_bars
                    
                    if int(dec_src.n) >= 5 and (due_by_time or due_by_bars):
                        if self.calib_svc:
                            await self.calib_svc.persist_atr_sanity(runtime, tf_norm=str(tf_norm), ts_ms=int(close_ts))
                        runtime._atr_sanity_last_persist_ts_ms = int(close_ts)
                        runtime._atr_sanity_bars_since_persist = 0
                except Exception:
                    pass
        except Exception:
            pass


        # Throttled persist per regime
        try:
            gap_ms = int(runtime.config.get("atr_tf_calib_persist_gap_ms", int(os.getenv("ATR_TF_CALIB_PERSIST_GAP_MS", "300000"))))
            last_p = int(getattr(runtime, "_atr_tf_last_persist_ts_ms", 0) or 0)
            if gap_ms > 0 and (now_ts - last_p) >= gap_ms:
                if self.calib_svc:
                    await self.calib_svc.persist_atr_tf_regime(runtime, regime=rg, ts_ms=now_ts)
                runtime._atr_tf_last_persist_ts_ms = int(now_ts)
        except Exception as exc:
            log_silent_error(exc, 'persist_failure', runtime.symbol, '_handle_tick:atr_tf_persist')
            pass

    
        # --- Dynamic calibration update (eff_quote / min_quote_delta) ---
        try:
            quote_delta = float(getattr(runtime, "last_quote_delta", 0.0) or 0.0)
            if quote_delta > 0:
                rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                runtime.eff_calib.update(regime=rg, quote_delta=float(quote_delta))
                
                # ... existing eff_calib persistence ...
                # Leaving existing EffQuote logic here as is, assumed working
                # ...
                if bool(int(runtime.config.get("calib_persist_enable", 1))):
                    runtime._calib_bars_since_persist += 1
                    min_bars = int(runtime.config.get("calib_persist_min_bars", 60))
                    if runtime._calib_bars_since_persist >= min_bars:
                        runtime._calib_bars_since_persist = 0
                        if self.calib_svc:
                            await self.calib_svc.persist_effq(runtime, regime=rg, ts_ms=int(bar.end_ts_ms))
    
        except Exception as exc:
            log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_handle_tick:eff_calib_update')
            pass

        # ------------------------------------------------------------------
        # ATR(bps) sanity floors (per-regime) -> runtime.dynamic_cfg
        # Fix "broken chain": we MUST select atr_bps_th based on regime+tier and expose it.
        # ------------------------------------------------------------------
        try:
            close_px = float(getattr(bar, "close", 0.0) or 0.0)
            atr_val = float(getattr(runtime, "last_atr", 0.0) or 0.0)
            rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
            if close_px > 0 and atr_val > 0:
                atr_bps = 10000.0 * (atr_val / close_px)
                runtime.dynamic_cfg[DK.ATR_BPS] = float(atr_bps)

                # Update calibrator (fail-open)
                if bool(int(os.getenv("ATR_BPS_CALIB_ENABLE", "1"))):
                    runtime.atr_bps_calib.update(regime=rg, atr_bps=float(atr_bps))

                # Bootstrap floors (must be >0 in config; if not, fallback to static min)
                # --- ATR Floor Policy (Tiered) ---
                # Check for overrides in local 'cfg'
                cfg = runtime.config
                d0 = float(cfg.get("atr_floor_t0_bps", 0.0) or 0.0)
                d1 = float(cfg.get("atr_floor_t1_bps", 0.0) or 0.0)
                d2 = float(cfg.get("atr_floor_t2_bps", 0.0) or 0.0)
                floors = runtime.atr_bps_calib.thresholds(
                    regime=rg
                    default_floor_t0=d0
                    default_floor_t1=d1
                    default_floor_t2=d2
                )
                runtime.dynamic_cfg[DK.ATR_FLOOR_T0_BPS] = float(floors.floor_t0)
                runtime.dynamic_cfg[DK.ATR_FLOOR_T1_BPS] = float(floors.floor_t1)
                runtime.dynamic_cfg[DK.ATR_FLOOR_T2_BPS] = float(floors.floor_t2)
                runtime.dynamic_cfg[DK.ATR_BPS_SRC] = str(floors.src)
                runtime.dynamic_cfg[DK.ATR_BPS_N] = int(floors.n)
                runtime.dynamic_cfg[DK.ATR_CALIB_READY] = int(
                    1 if floors.n >= int(runtime.config.get("atr_bps_calib_min_samples", int(os.getenv("ATR_BPS_CALIB_MIN_SAMPLES", "500")))) else 0
                )

                # SELECT threshold by regime tier (this is the missing link)
                tier, rg2, th = compute_atr_bps_threshold(
                    regime=rg
                    cfg=runtime.config
                    t0=float(floors.floor_t0)
                    t1=float(floors.floor_t1)
                    t2=float(floors.floor_t2)
                )
                runtime.dynamic_cfg[DK.ATR_FLOOR_TIER] = int(tier)
                runtime.dynamic_cfg[DK.ATR_BPS_TH] = float(th)

                # Persist (throttled)
                try:
                    gap_ms = int(runtime.config.get("atr_bps_calib_persist_gap_ms", int(os.getenv("ATR_BPS_CALIB_PERSIST_GAP_MS", "120000"))))
                    last_p = int(getattr(runtime, "_atr_bps_last_persist_ts_ms", 0) or 0)
                    if bool(int(os.getenv("ATR_BPS_CALIB_ENABLE", "1"))) and gap_ms > 0 and (int(bar.end_ts_ms) - last_p) >= gap_ms:
                        if self.calib_svc:
                            await self.calib_svc.persist_atr_bps(runtime, regime=rg, ts_ms=int(bar.end_ts_ms))
                        runtime._atr_bps_last_persist_ts_ms = int(bar.end_ts_ms)
                except Exception as exc:
                    log_silent_error(exc, 'persist_failure', runtime.symbol, '_handle_tick:atr_bps_persist')
                    pass
        except Exception as exc:
            log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_handle_tick:atr_bps_wrapper')
            pass
            
        # --- DeltaNotional tiers calibration (per regime) ---
        try:
            dn_usd = abs(float(getattr(bar, "delta_sum", 0.0) or 0.0)) * float(getattr(bar, "close", 0.0) or 0.0)
            if math.isfinite(dn_usd) and dn_usd > 0:
                rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                
                # 1. Update Calibrator (Authoritative source)
                runtime.dn_calib.update(
                    regime=rg
                    dn_usd=float(dn_usd)
                    ts_ms=int(bar.end_ts_ms)
                )

                # 2. Telemetry: Check Scale & Divergence (Throttle: 1h)
                now_ms = int(bar.end_ts_ms)
                if not hasattr(runtime, "last_dn_how_report_ts_ms"):
                     runtime.last_dn_how_report_ts_ms = 0
                
                if now_ms - runtime.last_dn_how_report_ts_ms > 3600_000:
                    tiers_cfg = runtime.config.get("delta_diff_tiers") or get_default_delta_tiers(runtime.symbol)
                    d0 = float(tiers_cfg.get("tier0", 0.0) or 0.0)
                    d1 = float(tiers_cfg.get("tier1", 0.0) or 0.0)
                    d2 = float(tiers_cfg.get("tier2", 0.0) or 0.0)
                    
                    t_telem = runtime.dn_calib.tiers(regime=rg, ts_ms=now_ms, default_t0=d0, default_t1=d1, default_t2=d2)
                    t_decis = runtime.dn_calib.tiers(regime=rg, ts_ms=0, default_t0=d0, default_t1=d1, default_t2=d2)
                    
                    # Metrics
                    from services.orderflow.metrics import of_dn_how_scale_gauge, of_dn_how_ratio_t1_gauge
                    try:
                        of_dn_how_scale_gauge.labels(symbol=runtime.symbol, regime=rg).set(t_telem.scale)
                    except Exception:
                        pass
                    
                    ratio = 1.0
                    if t_decis.tier1_usd > 0:
                        ratio = t_telem.tier1_usd / t_decis.tier1_usd
                    try:
                        of_dn_how_ratio_t1_gauge.labels(symbol=runtime.symbol, regime=rg).set(ratio)
                    except Exception:
                        pass
                    
                    # Report
                    if ratio < 0.8 or ratio > 1.2:
                        msg = (
                            f"Liquidity Divergence Report ({runtime.symbol})\n"
                            f"Regime: {rg}\n"
                            f"HourOfWeek: {t_telem.hour_of_week}\n"
                            f"Global Liq (EMA): ${t_telem.g_liq_ema:,.0f}\n"
                            f"Bucket Liq (EMA): ${t_telem.b_liq_ema:,.0f}\n"
                            f"Scale Factor: {t_telem.scale:.2f}x\n"
                            f"Tier1 (Decision): ${t_decis.tier1_usd:,.0f}\n"
                            f"Tier1 (Telemetry): ${t_telem.tier1_usd:,.0f}\n"
                            f"Ratio: {ratio:.2f}"
                        )
                        await self.signal_pipeline.send_telegram_report(runtime=runtime, text=msg)
                    runtime.last_dn_how_report_ts_ms = now_ms

                # 3. Persistence
                if bool(int(runtime.config.get("calib_persist_enable", 1))):
                    runtime._calib_bars_since_persist = int(getattr(runtime, "_calib_bars_since_persist", 0) or 0) + 1
                    min_bars = int(runtime.config.get("calib_persist_min_bars", 60))
                    if runtime._calib_bars_since_persist >= min_bars:
                        runtime._calib_bars_since_persist = 0
                        if self.calib_svc:
                            await self.calib_svc.persist_dn(runtime, regime=rg, ts_ms=int(bar.end_ts_ms))

        except Exception as exc:
             log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_on_microbar_closed:dn_calib')
             pass


        # ATR TF Selector (UNIFIED - single source of truth: atr_tf_selected)
        # Shadow mode: compute candidate, no apply. Enforce mode: apply candidate to selected.
        # ------------------------------------------------------------------
        try:
            if bool(int(os.getenv("ATR_TF_CALIB_ENABLE", "1"))):
                now_ts = int(getattr(bar, "end_ts_ms", 0) or 0)
                close_px = float(getattr(bar, "close", 0.0) or 0.0)
                rg = str(getattr(runtime, "last_regime", "na") or "na").lower()

                # Throttle: do not recompute too often (Redis reads for multiple TF)
                refresh_ms = int(runtime.config.get("atr_tf_calib_refresh_ms", 60_000))
                last = int(runtime.dynamic_cfg.get(DK.ATR_TF_CALIB_LAST_MS, 0) or 0)
                if refresh_ms < 10_000:
                    refresh_ms = 10_000
                if now_ts > 0 and (now_ts - last) >= refresh_ms and close_px > 0:
                    runtime.dynamic_cfg[DK.ATR_TF_CALIB_LAST_MS] = int(now_ts)

                    # Candidate TFs list (env-tunable)
                    tfs_raw = str(os.getenv("ATR_TF_CALIB_TFS", "1m,5m,15m,1h"))
                    tfs = [x.strip() for x in tfs_raw.split(",") if x.strip()]
                    if not tfs:
                        tfs = ["1m", "5m", "15m", "1h"]

                    # Compute target from fees-aware gate (rocket_v1) to avoid permanent veto
                    # NOTE: this is *sanity* target; unified gate still uses max(floor,fees).
                    target_bps = 0.0
                    try:
                        tp_ratios = parse_tp_ratio(runtime.config.get("tp_ratio") or runtime.config.get("tp_rr") or "")
                        tp1_share = float(tp_ratios[0] if tp_ratios else 0.5)
                        # Use signal_pipeline for rocket logic
                        rocket_mult = float(self.signal_pipeline._get_rocket_multiplier(runtime.symbol) or 0.0)
                        denom = float(tp1_share * rocket_mult)
                        if denom > 0:
                            target_bps = float((float(self.signal_pipeline.FEES_BPS_RT) + float(self.signal_pipeline.TP_BPS_BUFFER)) / denom)
                    except Exception:
                        target_bps = 0.0

                    # Collect atr_bps for each TF (best-effort; if tf missing -> skip)
                    atr_bps_by_tf: Dict[str, float] = {}
                    for tf in tfs:
                        try:
                            # Use raw cache lookup to bypass calibration logic itself
                            atr_tf = float(self.atr_cache.get(runtime.symbol, tf) or 0.0)
                            if atr_tf > 0:
                                atr_bps_by_tf[tf] = 10000.0 * (atr_tf / close_px)
                        except Exception as exc:
                            log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_handle_tick:atr_tf_update')
                            continue

                    if atr_bps_by_tf:
                        runtime.atr_tf_calib.update_many(regime=rg, atr_bps_by_tf=atr_bps_by_tf)

                        # Recommend TF (switching controlled by hold-down + hysteresis)
                        fallback_tf = str(runtime.config.get("atr_tf", os.getenv("ATR_TF", "5m")) or "5m")
                        current_tf = runtime.get_atr_tf_selected()  # Use canonical resolver
                        # ATR_TF_SELECTOR_MODE takes priority; ATR_TF_CALIB_MODE is alias for back-compat
                        mode = str(
                            os.getenv("ATR_TF_SELECTOR_MODE")
                            or os.getenv("ATR_TF_CALIB_MODE")
                            or "enforce"
                        ).lower()  # "shadow"|"enforce"
                        allow_switch = (mode == "enforce")
                        runtime.dynamic_cfg[DK.ATR_TF_MODE] = mode

                        choice = runtime.atr_tf_calib.recommend_tf(
                            regime=rg
                            target_bps=target_bps
                            fallback_tf=fallback_tf
                            now_ts_ms=now_ts
                            current_tf=current_tf
                            allow_switch=allow_switch
                        )

                        runtime.dynamic_cfg[DK.ATR_TF_TARGET_BPS] = float(choice.target_bps)
                        runtime.dynamic_cfg[DK.ATR_TF_READY] = int(1 if choice.src != "static" and choice.n >= int(os.getenv("ATR_TF_CALIB_MIN_SAMPLES", "30")) else 0)
                        runtime.dynamic_cfg[DK.ATR_TF_SRC] = str(choice.src)
                        runtime.dynamic_cfg[DK.ATR_TF_N] = int(choice.n)
                        # Telemetry: always write candidate (for observability)
                        runtime.dynamic_cfg[DK.ATR_TF_CANDIDATE] = str(choice.tf)
                        runtime.dynamic_cfg[DK.ATR_TF_CANDIDATE_SRC] = str(choice.src)
                        runtime.dynamic_cfg[DK.ATR_TF_CANDIDATE_N] = int(choice.n)
                        runtime.dynamic_cfg[DK.ATR_TF_CANDIDATE_SCORE] = float(getattr(choice, "score", 0.0) or 0.0)
                        runtime.dynamic_cfg[DK.ATR_TF_CANDIDATES_BPS] = dict(atr_bps_by_tf)
                                
                        # Update metrics
                        atr_tf_target_bps.labels(symbol=runtime.symbol).set(float(target_bps))
                        atr_tf_candidate_score.labels(symbol=runtime.symbol).set(float(getattr(choice, "score", 0.0) or 0.0))
                        candidate_diff = 1 if str(choice.tf) != current_tf else 0
                        atr_tf_candidate_diff.labels(symbol=runtime.symbol).set(candidate_diff)
                                
                        # Apply: ONLY in enforce mode
                        if allow_switch and str(choice.tf) != current_tf:
                            prev_tf = current_tf
                            new_tf = str(choice.tf)
                            runtime.dynamic_cfg[DK.ATR_TF_SELECTED] = new_tf
                            runtime.dynamic_cfg[DK.ATR_TF_LAST_SWITCH_TS_MS] = int(now_ts)
                            # Log switch (rate-limited)
                            logger.info(
                                "🔄 (%s) ATR-TF switch: %s → %s (target_bps=%.1f, src=%s, n=%d)"
                                runtime.symbol, prev_tf, new_tf, target_bps, choice.src, choice.n
                            )
                            # Increment switch counter
                            atr_tf_switch_total.labels(symbol=runtime.symbol).inc()
                        elif not allow_switch:
                            # Shadow mode: ensure selected is initialized but don't change it
                            runtime.dynamic_cfg.setdefault("atr_tf_selected", current_tf)

                        # Persist selected TF (throttled, only in enforce or on init)
                        persist_gap = int(runtime.config.get("atr_tf_calib_persist_gap_ms", 300_000))
                        if persist_gap < 60_000:
                            persist_gap = 60_000
                        last_p = int(getattr(runtime, "_atr_tf_last_persist_ts_ms", 0) or 0)
                        if now_ts > 0 and (now_ts - last_p) >= persist_gap and allow_switch:
                            runtime._atr_tf_last_persist_ts_ms = int(now_ts)
                            choice_state = {
                                "tf": runtime.get_atr_tf_selected()
                                "src": str(choice.src)
                                "updated_ts_ms": int(now_ts)
                            }
                            if self.calib_svc:
                                await self.calib_svc.persist_atr_tf_choice(runtime, choice_state=choice_state, ts_ms=now_ts)
        except Exception:
            pass


        # --- ADX quantile snapshot (deterministic by bar end ts) ---
        # We store in runtime.dynamic_cfg for later use in snapshot publisher.
        # Source of truth:
        #  - adx14 is in Redis key adx:{symbol} (float)
        #  - quantiles are in Redis key regime:q:{symbol}:1m (json)
        # Here we only read adx14 (cheap); adx_q is computed in snapshot publisher.
        try:
            # best-effort; fail-open
            adx_raw = await self.redis.get(f"adx:{runtime.symbol}")
            runtime.dynamic_cfg[DK.ADX14] = float(adx_raw) if adx_raw is not None else 0.0
        except Exception:
            pass

        # 1) RSI updates
        try:
            runtime.rsi_price.update(float(bar.close))
            runtime.rsi_cvd.update(float(bar.cvd_close))
        except Exception:
            pass
            
        # Metric: bars closed
        bars_closed_total.labels(symbol=runtime.symbol, tf=str(getattr(bar, "tf_ms", "0"))).inc()


        # ------------------------------------------------------------
        # Phase C: ATR TF selection + ATR caching for bar_close.
        # Goal:
        #  - choose best timeframe/source by freshness+consistency
        #  - store deterministic choice for later tick/execution use
        # Fail-open:
        #  - if selector fails, fall back to cfg atr_tf
        # ------------------------------------------------------------
        atr_val = float(getattr(runtime, "last_atr", 0.0) or 0.0)
        try:
            now_ts = int(bar.end_ts_ms)
            refresh_ms = int(runtime.config.get("eq_atr_refresh_ms", 15_000))
            if refresh_ms < 1_000:
                refresh_ms = 1_000

            if (now_ts - int(getattr(runtime, "last_atr_ts_ms", 0) or 0)) >= refresh_ms:
                close_px = float(getattr(bar, "close", 0.0) or 0.0)
                # 1) Use canonical TF resolver (single source of truth)
                tf_sel = runtime.get_atr_tf_selected()
                try:
                    if bool(int(os.getenv("ATR_TF_CALIB_ENABLE", "1"))) and close_px > 0:
                        choice = self.atr_tf_sel.choose(
                            symbol=str(runtime.symbol)
                            price=float(close_px)
                            now_ms=int(now_ts)
                            atr_cache=self.atr_cache
                        )
                        if choice is not None:
                            # TELEMETRY ONLY: do NOT write to atr_tf_selected (legacy path)
                            # Single source of truth is the unified selector in _on_microbar_closed
                            runtime.dynamic_cfg[DK.ATR_TF_ALT_CANDIDATE] = str(choice.tf)
                            runtime.dynamic_cfg[DK.ATR_TF_ALT_SRC] = str(choice.src)
                            runtime.dynamic_cfg[DK.ATR_TF_ALT_SCORE] = float(choice.score)
                            runtime.dynamic_cfg[DK.ATR_TF_ALT_AGE_MS] = int(choice.age_ms)
                            runtime.dynamic_cfg[DK.ATR_TF_ALT_ATR_BPS] = float(choice.atr_bps)
                            # NO persistence for legacy path
                except Exception:
                    pass

                # 2) fetch ATR using selected TF (best-effort)
                atr_tmp = 0.0
                try:
                    atr_tmp, atr_meta = self.atr_cache.get_with_meta(symbol=runtime.symbol, timeframe=tf_sel, now_ms=int(now_ts))
                    atr_tmp = float(atr_tmp or 0.0)
                    # expose meta for audit/debug
                    if isinstance(atr_meta, dict):
                        runtime.dynamic_cfg[DK.ATR_LIVE_SRC] = str(atr_meta.get("src", "na"))
                        runtime.dynamic_cfg[DK.ATR_LIVE_KEY] = str(atr_meta.get("key", ""))
                        runtime.dynamic_cfg[DK.ATR_LIVE_AGE_MS] = int(atr_meta.get("age_ms", 0) or 0)
                except Exception:
                    atr_tmp = 0.0

                if atr_tmp > 0:
                    # Sanitize live ATR too (keeps last_atr consistent across the system)
                    try:
                        px0 = float(getattr(runtime, "last_px", 0.0) or 0.0)
                        age0 = 0
                        if isinstance(atr_meta, dict):
                            age0 = int(atr_meta.get("age_ms", 0) or 0)
                        res = self._atr_sanity.update(
                            symbol=str(runtime.symbol)
                            atr=float(atr_tmp)
                            px=float(px0)
                            age_ms=int(age0)
                            now_ms=int(now_ts)
                        )
                        runtime.last_atr = float(res.atr_used)
                        runtime.last_atr_ts_ms = int(now_ts)
                        runtime.dynamic_cfg[DK.ATR_BAD] = int(res.bad)
                        runtime.dynamic_cfg[DK.ATR_BAD_REASON] = str(res.reason or "")
                    except Exception:
                        runtime.last_atr = float(atr_tmp)
                        runtime.last_atr_ts_ms = int(now_ts)
        except Exception:
            pass

        # ------------------------------------------------------------------
        # ATR floor tiers (per-symbol/per-regime) -> runtime.dynamic_cfg
        # Purpose:
        #   Fix "broken chain": ATR tiers must be selected later by tick-gate.
        # Deterministic time:
        #   uses bar.end_ts_ms and runtime.last_regime (bar-close derived).


        # 2) Swings and Divergences
        try:
            swings = runtime.swing.update(bar)
            for sp in swings:
                # Rate limit logs: only 1 in 50
                sp_cnt = self.swing_point_counters.get(runtime.symbol, 0) + 1
                self.swing_point_counters[runtime.symbol] = sp_cnt

                if sp_cnt % 50 == 0:
                     self.logger.info("📐 Swing Point detected (%s): kind=%s, price=%.2f, ts_ms=%d (x%d)", runtime.symbol, sp.kind, sp.price, sp.ts_ms, sp_cnt)
                
                if sp.kind == "high":
                    runtime.prev_swing_high = runtime.last_swing_high
                    runtime.last_swing_high = sp
                elif sp.kind == "low":
                    runtime.prev_swing_low = runtime.last_swing_low
                    runtime.last_swing_low = sp

                # Hidden divergence requires trend bias.
                bias = "none"
                if getattr(runtime, "cont_ctx_trend_dir", None):
                     td = str(runtime.cont_ctx_trend_dir).upper()
                     bias = "UP" if td == "LONG" else "DOWN" if td == "SHORT" else "none"
                else:
                     if runtime.last_swing_high and bar.close >= runtime.last_swing_high.price:
                         bias = "UP"
                     elif runtime.last_swing_low and bar.close <= runtime.last_swing_low.price:
                         bias = "DOWN"

                # Check Hidden Divergence
                divs_swing = runtime.divergence.update_swing(sp, trend_bias=bias)
                if divs_swing:
                    runtime.last_div = divs_swing[-1]
                    for d in divs_swing:
                        divergence_detected_total.labels(symbol=runtime.symbol, kind=str(d.kind)).inc()
                        self.logger.info("💎 Divergence Detected (%s): kind=%s, strength=%.2f", runtime.symbol, d.kind, d.strength)
                        
                        # --- Unified Divergence/Pools Signal Publishing ---
                        try:
                            # 1. Features
                            feats = {}
                            if "cont_ctx_recent" in OFInputsV2.__annotations__:
                                feats["cont_ctx_recent"] = _i(indicators.get("cont_ctx_recent", 0), 0)

                            if "macro_bias" in OFInputsV2.__annotations__:
                                feats["macro_bias"] = _i(indicators.get("macro_bias", 0), 0)
                            try:
                                feats["deltaSpikeZ"] = 0.0  # Not directly available in swing context
                                feats["weak_progress"] = int(getattr(runtime.last_wp, "is_weak", 0)) if runtime.last_wp else 0
                                feats["regime"] = str(getattr(runtime, "last_regime", "na"))
                                feats["atr_mult"] = 0.0  # Placeholder since ATR usually part of specific rule config
                                # Additional context if available
                                if hasattr(runtime, "last_spread_bps"):
                                    feats["spread_bps"] = float(runtime.last_spread_bps)
                            except Exception:
                                pass

                            # 2. Nearest Pool (mature only)
                            npool_info = None
                            try:
                                # Find nearest pool of ANY kind to the current price
                                pools_all = runtime.eq_pools.pools(kind=None, only_mature=True)
                                if pools_all:
                                    # Sort by distance to bar.close
                                    pools_all.sort(key=lambda p: abs(float(p.level) - float(bar.close)))
                                    np = pools_all[0]
                                    npool_info = {
                                        "id": str(getattr(np, "pool_id", ""))
                                        "kind": str(getattr(np, "kind", ""))
                                        "level": float(getattr(np, "level", 0.0))
                                        "dist_px": abs(float(np.level) - float(bar.close))
                                    }
                            except Exception:
                                pass

                            # 3. Payload
                            payload = {
                                "signal_type": "Divergence"
                                "symbol": str(runtime.symbol)
                                "tf": str(runtime.config.get("micro_tf", "1s"))
                                "ts_ms": int(d.ts_ms)
                                "side_bias": str(bias)
                                "divergence_kind": str(d.kind)
                                "strength": float(d.strength)
                                "confidence": min(0.99, float(d.strength) / 10.0),  # Simple confidence estimation
                                "features": feats
                                "nearest_pool": npool_info
                                "generated_at": get_ny_time_millis()
                                # Standard fields for compatibility
                                "reason": f"divergence_{d.kind}"
                                "entry": float(d.price_curr)
                                "price": float(d.price_curr)
                                "cvd": float(d.cvd_curr)
                            }

                            # 4. Publish to signals:crypto:raw
                            # We use xadd directly here to ensure it goes to the unified stream immediately
                            stream_key = "signals:crypto:raw"
                            pl_json = json.dumps(payload, default=str, ensure_ascii=False)
                            safe_create_task(self.ticks.xadd(stream_key, {"payload": pl_json}, maxlen=20000))

                        except Exception as ex:
                            self.logger.warning(f"⚠️ Failed to publish Divergence signal: {ex}")

                # Update EQ pools from swing points
                try:
                    runtime.eq_pools.on_swing(sp, atr=atr_val)
                except Exception:
                    pass

            divs = runtime.divergence.update(bar, runtime.swing.swings)
            for div in divs:
                runtime.last_div = div
        except Exception:
            pass

        # --- Dynamic calibration update (eff_quote / min_quote_delta) ---
        try:
            if bool(getattr(bar, "fp_enabled", False)):
                eff_q = float(getattr(bar, "fp_eff_quote", 0.0) or 0.0)
                qd = float(getattr(bar, "fp_quote_delta", 0.0) or 0.0)
                regime = str(getattr(runtime, "last_regime", "na") or "na")
                runtime.eff_calib.update(regime=regime, eff_quote=eff_q, quote_delta=qd)

                # Tier policy by regime
                tier = int(cfg.get("abs_lvl_tier_default", 1))
                if regime in ("range",):
                    tier = int(cfg.get("abs_lvl_tier_range", 1))
                elif regime in ("trend", "trending_bull", "trending_bear"):
                    tier = int(cfg.get("abs_lvl_tier_trend", 0))
                elif regime in ("thin", "news", "illiquid"):
                    tier = int(cfg.get("abs_lvl_tier_thin", 2))

                th = runtime.eff_calib.thresholds(
                    regime=regime
                    default_eff_th=float(runtime.config.get("abs_lvl_eff_quote_th", 0.0020))
                    default_min_qd=float(runtime.config.get("abs_lvl_min_quote_delta", 0.0))
                    tier=tier
                )
                runtime.dynamic_cfg[DK.ABS_LVL_EFF_QUOTE_TH] = float(th.eff_quote_th)
                runtime.dynamic_cfg[DK.ABS_LVL_MIN_QUOTE_DELTA] = float(th.min_quote_delta)
                runtime.dynamic_cfg[DK.ABS_LVL_CALIB_N] = int(th.n)
                runtime.dynamic_cfg[DK.ABS_LVL_CALIB_SRC] = str(th.src)
                runtime.dynamic_cfg[DK.ABS_LVL_TIER] = int(tier)

                stab = runtime._th_stab.update(float(th.eff_quote_th))
                runtime.dynamic_cfg[DK.ABS_LVL_TH_EMA] = float(stab.ema)
                runtime.dynamic_cfg[DK.ABS_LVL_TH_DRIFT] = float(stab.drift)
                runtime.dynamic_cfg[DK.ABS_LVL_TH_RANGE_NORM] = float(stab.range_norm)
                runtime.dynamic_cfg[DK.ABS_LVL_TH_STAB_N] = int(stab.n)

                drift_max = float(runtime.config.get("abs_lvl_th_drift_max", 0.35))
                range_max = float(runtime.config.get("abs_lvl_th_range_max", 1.20))
                unstable = int((stab.drift > drift_max) or (stab.range_norm > range_max))
                runtime.dynamic_cfg[DK.ABS_LVL_TH_UNSTABLE] = unstable

                # Dynamic strictness: if unstable or thin/news -> need=3
                if bool(int(runtime.config.get("strong_dynamic_need_enable", 1))):
                    if unstable or regime in ("thin", "news", "illiquid"):
                        runtime.dynamic_cfg[DK.STRONG_NEED_REVERSAL] = 3
                        runtime.dynamic_cfg[DK.STRONG_NEED_CONTINUATION] = 3
                    else:
                        runtime.dynamic_cfg[DK.STRONG_NEED_REVERSAL] = int(cfg.get("strong_need_reversal", 2))
                        runtime.dynamic_cfg[DK.STRONG_NEED_CONTINUATION] = int(cfg.get("strong_need_continuation", 2))
                runtime.dynamic_cfg[DK.ABS_LVL_CALIB_N] = int(th.n)
                runtime.dynamic_cfg[DK.ABS_LVL_CALIB_SRC] = str(th.src)

                # --- Persist calibration (throttled, deterministic by bar time) ---
                if bool(int(runtime.config.get("calib_persist_enable", 1))):
                    runtime._calib_bars_since_persist += 1
                    min_bars = int(runtime.config.get("calib_persist_min_bars", 120))
                    min_dt = int(runtime.config.get("calib_persist_min_interval_ms", 60_000))
                    ts_ms = int(getattr(bar, "end_ts_ms", 0) or 0)
                    last = int(getattr(runtime, "_calib_last_persist_ts_ms", 0) or 0)

                    due = (runtime._calib_bars_since_persist >= min_bars) or (ts_ms > 0 and last > 0 and (ts_ms - last) >= min_dt)
                    if due and ts_ms > 0:
                        runtime._calib_last_persist_ts_ms = ts_ms
                        runtime._calib_bars_since_persist = 0
                        # regime label should match what you used for update()
                        rg = str(getattr(runtime, "last_regime", "na") or "na")
                        if self.calib_svc:
                            safe_create_task(self.calib_svc.persist_effq(runtime, regime=rg, ts_ms=ts_ms))

                if bool(int(runtime.config.get("strong_dynamic_need_enable", 0))):
                    if regime in ("thin", "news", "illiquid"):
                        runtime.dynamic_cfg[DK.STRONG_NEED_REVERSAL] = 3
                        runtime.dynamic_cfg[DK.STRONG_NEED_CONTINUATION] = 3
                    else:
                        runtime.dynamic_cfg[DK.STRONG_NEED_REVERSAL] = int(cfg.get("strong_need_reversal", 2))
                        runtime.dynamic_cfg[DK.STRONG_NEED_CONTINUATION] = int(cfg.get("strong_need_continuation", 2))
        except Exception:
            pass
            
        # C) Rolling CVD Snapshot (for UI/QA)
        # Writes to LIST: cvd:snap:{symbol}
        if os.getenv("CVD_SNAPSHOT_ENABLE", "0") == "1":
            try:
                # Format: "{ts_ms},{cvd},{cvd_ema},{cvd_slope}"
                # For now, just cvd, others 0.0
                val_str = f"{int(bar.end_ts_ms)},{float(bar.cvd_close):.2f},0.0,0.0"
                snap_key = f"cvd:snap:{runtime.symbol}"
                
                # Use pipeline for atomicity if possible, or just gather
                # Need to verify if self.ticks supports pipeline easily (it is redis client)
                # Just sequential await is fine for now as it's fire-and-forget logic
                await self.ticks.lpush(snap_key, val_str)
                await self.ticks.ltrim(snap_key, 0, 3599) # Keep last 3600 (1 hour @ 1s)
                await self.ticks.expire(snap_key, 21600)  # TTL 6 hours
            except Exception:
                pass


        # 3) Footprint diagnostics
        if getattr(bar, "fp_evictions", 0) > 0:
            fp_buckets_evicted_total.labels(symbol=runtime.symbol).inc(bar.fp_evictions)


        # Phase C: sweep detection using mature pools.
        try:
            mature = runtime.eq_pools.pools(only_mature=True)
            sweeps = runtime.sweep.update_bar(bar, pools=mature)
            if sweeps:
                sw = sweeps[-1]
                runtime.last_sweep = sw
                # Store baseline CVD at sweep bar close
                try:
                    runtime.last_sweep_ts_ms = int(getattr(sw, "ts_ms", 0) or int(bar.end_ts_ms))
                    runtime.last_sweep_cvd = float(getattr(bar, "cvd_close", 0.0) or 0.0)
                except Exception:
                    pass
                sweep_detected_total.labels(symbol=runtime.symbol, eq_kind=str(sw.kind)).inc()
                # start reclaim FSM on sweep return
                runtime.reclaim.on_sweep_return(runtime.last_sweep)
                # FIX: prevent reclaim on same bar
                runtime.reclaim_start_ts_ms = int(getattr(sw, "ts_ms", 0))
        except Exception:
            pass
            
        # Reclaim FSM progress on each bar close
        try:
            # FIX: ignore same bar
            if int(getattr(runtime, "reclaim_start_ts_ms", 0)) == int(bar.end_ts_ms):
                pass
            else:
                ev = runtime.reclaim.on_bar_close(bar)
                if ev is not None:
                    runtime.last_reclaim = ev

                    # ------------------------------------------------------------
                    # Phase E: CVD Reclaim Evidence (bonus-evidence)
                    # ------------------------------------------------------------
                    try:
                        # Always try to compute if we have sweep baseline
                        if (int(runtime.config.get("cvd_reclaim_enable", 1) or 0) == 1 and 
                            runtime.last_sweep_ts_ms > 0):
                            
                            res = compute_cvd_reclaim(
                                ts_ms=int(ev.ts_ms)
                                sweep_ts_ms=runtime.last_sweep_ts_ms
                                cvd_sweep=float(runtime.last_sweep_cvd)
                                reclaim_ts_ms=int(ev.ts_ms)
                                cvd_reclaim=float(bar.cvd_close)
                                direction_bias=str(ev.direction_bias)
                                min_abs=float(runtime.config.get("cvd_reclaim_min_abs", 0.0))
                                sat_abs=float(runtime.config.get("cvd_reclaim_sat_abs", 0.0))
                            )
                            runtime.last_cvd_reclaim = res
                            
                            cvd_reclaim_eval_total.labels(symbol=runtime.symbol, bias=str(ev.direction_bias)).inc()
                            if res.ok:
                                cvd_reclaim_ok_total.labels(symbol=runtime.symbol, bias=str(ev.direction_bias)).inc()
                            
                            self.logger.info(
                                "CVDReclaim computed sym=%s bias=%s ok=%d score=%.3f delta=%.1f window_ms=%d"
                                runtime.symbol, ev.direction_bias, res.ok, res.score, res.cvd_delta, (int(ev.ts_ms) - runtime.last_sweep_ts_ms)
                            )
                    except Exception:
                        pass
        except Exception:
            pass

        # --- Weak progress snapshot ---
        try:
            runtime.last_wp = compute_weak_progress(bar, atr_val, runtime.config)
            # Update WeakProgressDetector history (trend-of-absorption)
            try:
                if runtime.last_wp is not None:
                    runtime.weak_progress_det.push(runtime.last_wp, ts_ms=int(bar.end_ts_ms))
            except Exception:
                pass
        except Exception:
            runtime.last_wp = None

        # --- Footprint edge absorb ---
        try:
            fe = runtime.fp_edge.update_bar(bar, runtime.config)
            if fe is not None:
                runtime.last_fp_edge = fe
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Variant A: Publish microbar_closed for decentralized services
        # ------------------------------------------------------------------
        try:
            bar_out = {
                "type": "microbar_closed"
                "symbol": runtime.symbol
                "ts_ms": int(bar.end_ts_ms)
                "open": float(bar.open)
                "high": float(bar.high)
                "low": float(bar.low)
                "close": float(bar.close)
                "vol": float(bar.vol)
                "cvd": float(bar.cvd_close)
                # Metadata needed by OFConfirmEngine
                "weak_progress": bool(runtime.last_wp.weak_any) if runtime.last_wp else False
                "sweep": {
                    "kind": str(runtime.last_sweep.kind)
                    "ts_ms": int(runtime.last_sweep.ts_ms)
                } if runtime.last_sweep else None
                "regime": str(getattr(runtime, "last_regime", "na"))
                "reclaim": {
                    "hold_bars": int(runtime.last_reclaim.hold_bars)
                    "ts_ms": int(runtime.last_reclaim.ts_ms)
                } if runtime.last_reclaim else None
                "last_div_kind": str(runtime.last_div.kind) if runtime.last_div else None
                "generated_at": get_ny_time_millis()
            }
            # Best practice: optionally split retention per symbol so minors are not evicted by majors
            from services.orderflow.microbar_publish import publish_microbar_closed
            safe_create_task(
                publish_microbar_closed(
                    redis_client=self.redis
                    symbol=runtime.symbol
                    payload_obj=bar_out
                )
            )
        except Exception as e:
            logger.error(f"Failed to publish microbar_closed event: {e}")

        # ------------------------------------------------------------------
        # Adaptive Pressure Proxy Calibration (Tick-Level)
        # ------------------------------------------------------------------
        try:
            now_ms = int(getattr(bar, "end_ts_ms", 0) or 0)
            calib_min_samples = int(os.getenv("PRESSURE_TIER_CALIB_MIN_SAMPLES", "300"))
            calib_refresh_ms = int(os.getenv("PRESSURE_TIER_CALIB_REFRESH_MS", "60000"))
            
            last_update = int(getattr(runtime, "ptier_last_update_ts_ms", 0) or 0)
            if now_ms > 0 and (now_ms - last_update) >= calib_refresh_ms:
                 # Clone deque to list for sorting
                 samples = list(runtime.ptier_samples_usd)
                 if len(samples) >= calib_min_samples:
                     samples.sort()
                     n = len(samples)
                     def _q(p): return samples[int(p * (n - 1))]
                     
                     p75 = _q(0.75)
                     p90 = _q(0.90)
                     p97 = _q(0.97)
                     
                     # Clamp (safety)
                     min_usd = float(os.getenv("PRESSURE_TIER_MIN_USD", "10000.0"))
                     max_usd = float(os.getenv("PRESSURE_TIER_MAX_USD", "5000000.0"))
                     
                     def _clamp_usd(x): return max(min_usd, min(max_usd, x))
                     
                     t0 = _clamp_usd(p75)
                     t1 = _clamp_usd(p90)
                     t2 = _clamp_usd(p97)
                     
                     runtime.dynamic_cfg[DK.PRESSURE_TIER0_USD] = t0
                     runtime.dynamic_cfg[DK.PRESSURE_TIER1_USD] = t1
                     runtime.dynamic_cfg[DK.PRESSURE_TIER2_USD] = t2
                     
                     runtime.ptier_last_update_ts_ms = int(now_ms)
                     
                     # Log calibration
                     self.logger.info(
                         "⚖️ [PTIER-CALIB] (%s) Updated thresholds (n=%d): T0=$%.0f, T1=$%.0f, T2=$%.0f"
                         runtime.symbol, n, t0, t1, t2
                     )
        except Exception as exc:
            log_silent_error(exc, 'calib_update_failure', runtime.symbol, '_on_microbar_closed:ptier_calib')

        # ------------------------------------------------------------
        # Pressure Tier Calibrator (Expert Recommendation - Production Ready)
        # Regime-aware quantile-based adaptive thresholds with hysteresis
        # ------------------------------------------------------------
        try:
            rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
            tiers = runtime.ptier_calib.maybe_recompute(now_ms=int(now_ms), regime=rg)
            
            if tiers:
                # Update telemetry-only keys in dynamic_cfg
                runtime.dynamic_cfg[DK.PTIER_TIER0_USD] = float(tiers["tier0"])
                runtime.dynamic_cfg[DK.PTIER_TIER1_USD] = float(tiers["tier1"])
                runtime.dynamic_cfg[DK.PTIER_TIER2_USD] = float(tiers["tier2"])
                
                # Update telemetry metrics
                ptier_tier0_usd.labels(symbol=runtime.symbol).set(float(tiers["tier0"]))
                ptier_tier1_usd.labels(symbol=runtime.symbol).set(float(tiers["tier1"]))
                ptier_tier2_usd.labels(symbol=runtime.symbol).set(float(tiers["tier2"]))

                # NOTE: We no longer update dn_tier*, dn_tier_active, or dn_th_usd here.
                # dn_calib (above) is now the sole authority for those keys.
                # [EXPERT] Persistence disabled for telemetry-only ptier results.
                
                # Log calibration (telemetry only)
                    
        except Exception as exc:
            log_silent_error(exc, 'ptier_calib_failure', runtime.symbol, '_on_microbar_closed:ptier_calib')

        # ------------------------------------------------------------
        # SMT V2: Publish compact snapshot (BOS proxy, swings, OF state)
        # ------------------------------------------------------------
        await self._publish_smt_snapshot(runtime, bar)

    async def _publish_smt_snapshot(self, runtime: SymbolRuntime, bar: MicroBar) -> None:
        try:
            now_ts = int(getattr(bar, "end_ts_ms", 0) or 0)
            if now_ts <= 0:
                # В детерминированной системе мы не должны падать на системное время.
                # Если end_ts_ms нет, значит бар некорректен.
                return

            snap_every_ms = int(runtime.config.get("smt_snapshot_every_ms", 1000))
            if snap_every_ms < 250:
                snap_every_ms = 250
            
            if (now_ts - int(getattr(runtime, "last_snapshot_ts_ms", 0) or 0)) >= snap_every_ms:
                runtime.last_snapshot_ts_ms = now_ts

                # --- Persist MicroBar to PostgreSQL (Redundancy) ---
                try:
                    pm = (getattr(runtime, 'pm', None) or get_persistence_manager())
                    b_dict = {
                        "ts_ms": int(bar.end_ts_ms)
                        "open": float(bar.open)
                        "high": float(bar.high)
                        "low": float(bar.low)
                        "close": float(bar.close)
                        "vol": float(bar.vol)
                        "cvd": float(bar.cvd_close)
                    }
                    safe_create_task(pm.save_microbar(runtime.symbol, b_dict))
                except Exception:
                    pass

                # 1. BOS / Structure Proxy
                close_px = float(getattr(bar, "close", 0.0) or 0.0)
                close_cross = 0
                close_cross_dir = "NONE"
                close_cross_level = 0.0
                
                if runtime.last_swing_high:
                    lvl = float(runtime.last_swing_high.price)
                    if lvl > 0 and close_px > lvl:
                        close_cross = 1
                        close_cross_dir = "UP"
                        close_cross_level = lvl
                
                if runtime.last_swing_low:
                    lvl = float(runtime.last_swing_low.price)
                    if lvl > 0 and close_px < lvl:
                        close_cross = 1
                        close_cross_dir = "DOWN"
                        close_cross_level = lvl
                
                # Trend Dir Proxy (Hidden Div > CloseCross > NONE)
                trend_dir = "NONE"
                if runtime.last_div:
                    k = str(runtime.last_div.kind)
                    if k == "bullish_hidden": trend_dir = "UP"
                    elif k == "bearish_hidden": trend_dir = "DOWN"
                
                if trend_dir == "NONE" and close_cross_dir in ("UP", "DOWN"):
                    trend_dir = close_cross_dir

                # 2. Strong OF Context
                of_valid_ms = int(runtime.config.get("smt_of_strong_valid_ms", 120000))
                of_strong = 0
                if runtime.last_of_strong_ts_ms > 0:
                     if (now_ts - runtime.last_of_strong_ts_ms) <= of_valid_ms:
                         of_strong = 1
                
                # 3. Detectors state
                wp = 1 if (runtime.last_wp and runtime.last_wp.weak_any) else 0
                
                reclaim = 0
                reclaim_dir = "NONE"
                reclaim_ts = 0
                if runtime.last_reclaim:
                    reclaim_ts = int(runtime.last_reclaim.ts_ms)
                    if now_ts - reclaim_ts <= int(runtime.config.get("smt_reclaim_valid_ms", 120000)):
                        reclaim = 1
                        reclaim_dir = str(runtime.last_reclaim.direction_bias).upper()
                
                sweep = 0
                sweep_dir = "NONE"
                sweep_ts = 0
                if runtime.last_sweep:
                    sweep_ts = int(runtime.last_sweep.ts_ms)
                    if now_ts - sweep_ts <= int(runtime.config.get("smt_sweep_valid_ms", 120000)):
                        sweep = 1
                        sweep_dir = str(runtime.last_sweep.direction_bias).upper()

                obi_stable_sec = 0.0
                if runtime.last_obi_event:
                     obi_stable_sec = float(runtime.last_obi_event.get("stable_secs", 0.0) or 0.0)
                
                iceberg_strict = 0
                if runtime.last_iceberg_event:
                    # check if recent strict criteria met
                    # Simplified: just check if refresh count is high
                    refresh = int(runtime.last_iceberg_event.get("refresh", 0) or 0)
                    dur = float(runtime.last_iceberg_event.get("duration", 0.0) or 0.0)
                    r_min = int(runtime.config.get("iceberg_strict_refresh_min", 3))
                    d_min = float(runtime.config.get("iceberg_strict_duration_min", 1.5))
                    if refresh >= r_min and dur >= d_min:
                        iceberg_strict = 1
                
                div_kind = "none"
                div_ts = 0
                if runtime.last_div:
                    div_kind = str(runtime.last_div.kind)
                    div_ts = int(runtime.last_div.ts_ms)
                
                # Ranking features
                rsi14 = float(runtime.rsi_price.value) if (hasattr(runtime, "rsi_price") and runtime.rsi_price.value is not None) else 0.0
                cvd_slope = float(getattr(runtime.cvd_state, "cvd_slope", 0.0)) if hasattr(runtime.cvd_state, "cvd_slope") else 0.0
                retrace_atr = float(runtime.config.get("smt_retrace_atr", 0.0))

                sh0 = float(runtime.last_swing_high.price) if runtime.last_swing_high else 0.0
                sh1 = float(runtime.prev_swing_high.price) if runtime.prev_swing_high else 0.0
                sl0 = float(runtime.last_swing_low.price) if runtime.last_swing_low else 0.0
                sl1 = float(runtime.prev_swing_low.price) if runtime.prev_swing_low else 0.0
                tsh0 = int(runtime.last_swing_high.ts_ms) if runtime.last_swing_high else 0
                tsh1 = int(runtime.prev_swing_high.ts_ms) if runtime.prev_swing_high else 0
                tsl0 = int(runtime.last_swing_low.ts_ms) if runtime.last_swing_low else 0
                tsl1 = int(runtime.prev_swing_low.ts_ms) if runtime.prev_swing_low else 0

                rsi14 = float(runtime.rsi_price.value) if (hasattr(runtime, "rsi_price") and runtime.rsi_price.value is not None) else 0.0
                cvd_slope = float(getattr(runtime.cvd_state, "cvd_slope", 0.0)) if hasattr(runtime.cvd_state, "cvd_slope") else 0.0
                
                # The user patch provided a different calculation for rsi14 and cvd_slope.
                # I will use the original calculation for rsi14 and cvd_slope as it seems more robust
                # (checking for hasattr and None) and the user's snippet for these two lines
                # seems to be a partial or alternative thought process.
                # The user's snippet for rsi14 and cvd_slope:
                # rsi14 = float(runtime.rsi_price.value)
                # cvd_slope = float(runtime.rsi_cvd.value) # Using rsi_cvd as proxy or separate slope?
                # This conflicts with the existing `cvd_slope` which uses `runtime.cvd_state.cvd_slope`.
                # I will keep the existing `rsi14` and `cvd_slope` calculations.

                retrace_atr = 0.0
                if runtime.last_retrace:
                     retrace_atr = float(getattr(runtime.last_retrace, "depth_atr", 0.0) or 0.0)

                # --- SMT snapshot extra fields (for SMT V2 quality/confScore/entry gating) ---
                # We compute "zone" as a proxy: use close_cross_level (last swing level crossed).
                # This is NOT FVG/OB. It is a structural proxy until zones are wired into snapshot.
                delta_z = float(getattr(runtime, "last_delta_z", 0.0) or 0.0)
                delta_eff_norm = float(getattr(runtime, "last_delta_eff_norm", 0.0) or 0.0)
                abs_lvl_ok = int(getattr(runtime, "last_abs_lvl_ok", 0) or 0)

                # --- REAL nearest zone from HTF zones cache (preferred over swing proxy) ---
                zone_id = ""
                zone_type = ""
                zone_src = ""
                zone_side = ""
                zone_px_lo = 0.0
                zone_px_hi = 0.0
                zone_ts_ms = 0
                zone_weight = 0.0
                zone_dist_bp = 0.0
                near_zone = 0
                zone_ok = 0

                try:
                    await runtime.maybe_load_htf_zones(now_ts_ms=int(now_ts), redis_client=self.redis)
                    px = float(close_px or 0.0)
                    pack = getattr(runtime, "zones_pack", None)
                    if pack is not None and px > 0:
                        z, d_bp, inside = pack.nearest(px)
                        if z is not None:
                            zone_id = str(z.id)
                            zone_type = str(z.type)
                            zone_src = str(z.src)
                            zone_side = str(z.side)
                            zone_px_lo = float(z.px_lo)
                            zone_px_hi = float(z.px_hi)
                            zone_ts_ms = int(z.ts_ms)
                            zone_weight = float(z.weight)
                            zone_dist_bp = float(d_bp)
                            near_bp = float(runtime.config.get("smt_near_zone_bp", runtime.config.get("smt_zone_max_bp", 15.0)))
                            ok_bp = float(runtime.config.get("smt_zone_max_bp", 15.0))
                            near_zone = 1 if (inside or (zone_dist_bp > 0 and zone_dist_bp <= near_bp)) else 0
                            zone_ok = 1 if (inside or (zone_dist_bp > 0 and zone_dist_bp <= ok_bp)) else 0
                except Exception:
                    pass

                # Fallback to swing proxy if HTF zones missing
                if zone_ok == 0 and (not zone_id):
                    try:
                        z_level = float(close_cross_level or 0.0)
                        z_px = float(close_px or 0.0)
                        if z_level > 0 and z_px > 0:
                            mid = 0.5 * (abs(z_px) + abs(z_level))
                            zone_dist_bp = (10000.0 * abs(z_px - z_level) / mid) if mid > 0 else 0.0
                        near_bp = float(runtime.config.get("smt_near_zone_bp", runtime.config.get("smt_zone_max_bp", 15.0)))
                        ok_bp = float(runtime.config.get("smt_zone_max_bp", 15.0))
                        near_zone = 1 if (zone_dist_bp > 0 and zone_dist_bp <= near_bp) else 0
                        zone_ok = 1 if (near_zone == 1 and int(close_cross or 0) == 1 and zone_dist_bp <= ok_bp) else 0
                        # mark proxy
                        zone_id = "SWING_PROXY"
                        zone_type = "LEVEL"
                        zone_src = "swing"
                        zone_side = "NA"
                        zone_px_lo = float(z_level)
                        zone_px_hi = float(z_level)
                        zone_ts_ms = int(now_ts)
                        zone_weight = 0.1
                    except Exception as e:
                       self.logger.warning(f"Fallback proxy error: {e}")
                       pass
                
                # abs_lvl_ok should already be present in indicators/dynamic cfg; keep best-effort:
                abs_lvl_ok = 0
                try:
                    # We can't access indicators here as they are not in scope.
                    # But we used getattr(runtime, "last_abs_lvl_ok", 0) previously.
                    abs_lvl_ok = int(getattr(runtime, "last_abs_lvl_ok", 0) or 0)
                except Exception:
                    abs_lvl_ok = 0

                # --- ADX strength quantile (deterministic in snapshot) ---
                # Source of truth:
                #   adx14: Redis adx:{symbol} (float)
                #   quantiles: Redis regime:q:{symbol}:1m (json with adx_p40/p60/p75)
                # We compute adx_q with approx_quantile_3pt; fail-open 0.5.
                adx14 = 0.0
                adx_q = 0.5
                try:
                    # now_ts is your snapshot ts_ms (bar-aligned); keep deterministic.
                    adx14 = float(await self.market_state.get_adx(symbol=runtime.symbol, now_ms=int(now_ts)))
                    rq = await self.market_state.get_regime_quantiles(symbol=runtime.symbol, tf="1m", now_ms=int(now_ts))
                    if isinstance(rq, dict):
                        p40 = float(rq.get("adx_p40") or 0.0)
                        p60 = float(rq.get("adx_p60") or 0.0)
                        p75 = float(rq.get("adx_p75") or 0.0)
                        # sanity: must be monotonic and positive
                        if p40 > 0 and p60 > 0 and p75 > 0 and (p40 <= p60 <= p75):
                            from core.regime_quantiles_store import approx_quantile_adx
                            adx_q = float(approx_quantile_adx(float(adx14), p40, p60, p75))
                except Exception:
                    pass


                # Data-quality from runtime (deterministic at now_ts)
                spread_bp = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
                book_age_ms = 10**9
                try:
                    bts = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
                    if bts > 0:
                        book_age_ms = int(max(0, now_ts - bts))
                except Exception:
                    pass
                obi_age_ms = 10**9
                try:
                    if runtime.last_obi_event:
                        ots = int(runtime.last_obi_event.get("ts_ms") or 0)
                        if ots > 0: obi_age_ms = int(max(0, now_ts - ots))
                except Exception:
                    pass
                iceberg_age_ms = 10**9
                try:
                    if runtime.last_iceberg_event:
                        its = int(runtime.last_iceberg_event.get("ts_ms") or 0)
                        if its > 0: iceberg_age_ms = int(max(0, now_ts - its))
                except Exception:
                    pass

                snap = SymbolSnapshot(
                    symbol=str(runtime.symbol)
                    ts_ms=now_ts
                    trend_dir=trend_dir
                    close_px=close_px
                    close_cross=close_cross
                    close_cross_dir=close_cross_dir
                    close_cross_level=close_cross_level
                    swing_high_0=sh0
                    swing_high_1=sh1
                    swing_low_0=sl0
                    swing_low_1=sl1
                    swing_ts_high_0=tsh0
                    swing_ts_high_1=tsh1
                    swing_ts_low_0=tsl0
                    swing_ts_low_1=tsl1
                    of_strong=of_strong
                    of_dir=str(of_dir)
                    of_ts_ms=int(runtime.last_of_strong_ts_ms)
                    weak_progress=int(wp)
                    reclaim=reclaim
                    reclaim_dir=reclaim_dir
                    reclaim_ts_ms=reclaim_ts
                    sweep=sweep
                    sweep_dir=sweep_dir
                    sweep_ts_ms=sweep_ts
                    obi_stable_sec=obi_stable_sec
                    iceberg_strict=iceberg_strict
                    div_kind=str(runtime.last_div.kind) if runtime.last_div else "none"
                    div_ts_ms=int(runtime.last_div.ts_ms) if runtime.last_div else 0
                    rsi14=rsi14
                    cvd_slope=cvd_slope
                    retrace_atr=retrace_atr
                    # SMT V2 fields
                    delta_z=float(delta_z)
                    delta_eff_norm=float(delta_eff_norm)
                    zone_dist_bp=float(zone_dist_bp)
                    zone_ok=int(zone_ok)
                    near_zone=int(near_zone)
                    abs_lvl_ok=int(abs_lvl_ok)
                    # Real zone identity (for retest FSM/UI/debug)
                    zone_id=str(zone_id)
                    zone_type=str(zone_type)
                    zone_src=str(zone_src)
                    zone_side=str(zone_side)
                    zone_px_lo=float(zone_px_lo)
                    zone_px_hi=float(zone_px_hi)
                    zone_ts_ms=int(zone_ts_ms)
                    zone_weight=float(zone_weight)
                    # Market context
                    regime=str(getattr(runtime, "last_regime", "na") or "na")
                    atr=float(getattr(runtime, "last_atr", 0.0) or 0.0)
                    # Absorption-level readiness/stability
                    abs_lvl_ready=int(1 if int(runtime.dynamic_cfg.get(DK.ABS_LVL_CALIB_N, 0) or 0) >= int(runtime.config.get("abs_lvl_calib_min_samples", 300)) else 0)
                    delta_z_window=int(runtime.config.get("delta_window_n", 60) or 60)

                    # Book health (deterministic)
                    book_rate_hz=float(getattr(runtime, "book_rate_ema", 0.0) or 0.0)
                    book_age_ms=int(max(0, int(now_ts) - int(getattr(runtime, "last_book_ts_ms", 0) or 0))) if int(getattr(runtime, "last_book_ts_ms", 0) or 0) > 0 else 10**9
                    book_rate_ok_min_hz=float(runtime.dynamic_cfg.get(DK.BOOK_RATE_OK_MIN_HZ, runtime.config.get("book_rate_min_hz", 5.0)))
                    book_rate_crit_hz=float(runtime.dynamic_cfg.get(DK.BOOK_RATE_CRIT_HZ, runtime.config.get("book_rate_crit_hz", 2.0)))
                    book_rate_ready=int(runtime.dynamic_cfg.get(DK.BOOK_RATE_READY, 0) or 0)
                    book_rate_src=str(runtime.dynamic_cfg.get(DK.BOOK_RATE_CALIB_SRC, "static") or "static")
                    
                    # Already computed in handle_tick, but we refresh for snapshot context just in case, 
                    # or use stored runtime values.
                    # Using stored runtime values is safer for consistency with what triggered signal.
                    book_health_ok=int(getattr(runtime, "last_book_health_ok", 1))
                    book_health=str(getattr(runtime, "last_book_health", "OK"))

                    abs_lvl_th_unstable=int(runtime.dynamic_cfg.get(DK.ABS_LVL_TH_UNSTABLE, 0) or 0)
                    # Strong gate diagnostics
                    of_confirm_score=float(getattr(runtime, "last_of_confirm_score", 0.0) or 0.0)
                    strong_gate_have=int(getattr(runtime, "last_strong_gate_have", 0) or 0)
                    strong_gate_need=int(getattr(runtime, "last_strong_gate_need", 0) or 0)
                    strong_gate_scn=str(getattr(runtime, "last_strong_gate_scn", "") or "")
                    # ADX-aware regime strength
                    adx_q=float(adx_q)
                    adx14=float(adx14)
                    # DQ / Pressure
                    pressure_sps=float(getattr(runtime, "pressure_sps", 0.0) or 0.0)
                    pressure_hi=int(getattr(runtime, "pressure_hi", 0) or 0)
                    spread_bp=float(spread_bp)
                    obi_age_ms=int(obi_age_ms)
                    iceberg_age_ms=int(iceberg_age_ms)
                    cooldown_sps=float(getattr(runtime, "cooldown_hits_ema", 0.0) or 0.0)
                    spread_z=float(getattr(runtime, "last_spread_z", 0.0) or 0.0)
                )

                ttl_sec = int(runtime.config.get("smt_snapshot_ttl_sec", 30))
                if ttl_sec < 5: ttl_sec = 5
                
                key = f"smt:snap:{runtime.symbol}"
                # БАТЧ: Используем MetricsBatcher для снимков SMT (без аллокаций тасок)
                self._mbatch.put("set", key, snap.to_json(), ex=ttl_sec)

                # Phase D (P3): store extra snapshot fields WITHOUT changing SymbolSnapshot schema.
                # Rationale: `core.smt_symbol_snapshot.SymbolSnapshot` has a fixed schema.
                # We keep additional (evolving) fields in a sidecar key and merge in EntryPolicy.
                try:
                    from services.orderflow.flow_toxicity import normal_cdf
                    ofi_z = float(getattr(runtime, "ofi_norm_z", 0.0) or 0.0)
                    vz = 0.0
                    try:
                        l3s = getattr(runtime, "l3_stats", None)
                        if l3s is not None:
                            vz = float(getattr(l3s, "vpin_tox_z", 0.0) or 0.0)
                    except Exception:
                        vz = 0.0
                    extra = {
                        "ofi_norm_z": float(ofi_z)
                        "vpin_tox_z": float(vz)
                        "vpin_cdf": float(normal_cdf(float(vz)))
                        "flow_tox_ts_ms": int(now_ts)
                        # Phase E / P4: manipulation pattern fields in sidecar
                        # (merged in _get_snap via MGET → snap.update(extra))
                        "book_update_rate_hz": float(getattr(runtime, "book_update_rate_hz", 0.0) or 0.0)
                        "book_update_rate_z": float(getattr(runtime, "book_update_rate_z", 0.0) or 0.0)
                        "trade_msg_rate_hz": float(getattr(runtime, "trade_msg_rate_hz", 0.0) or 0.0)
                        "trade_msg_rate_z": float(getattr(runtime, "trade_msg_rate_z", 0.0) or 0.0)
                        "cancel_rate_z": float(getattr(runtime, "cancel_rate_z", 0.0) or 0.0)
                        "otr": float(getattr(runtime, "otr", 0.0) or 0.0)
                        "otr_z": float(getattr(runtime, "otr_z", 0.0) or 0.0)
                        "quote_stuffing_score": float(getattr(runtime, "quote_stuffing_score", 0.0) or 0.0)
                        "layering_score": float(getattr(runtime, "layering_score", 0.0) or 0.0)
                        "manip_flags": str(getattr(runtime, "manip_flags", "") or "")
                        "manip_ts_ms": int(now_ts)
                    }
                    ex_key = f"smt:snap_extra:{runtime.symbol}"
                    self._mbatch.put("set", ex_key, json.dumps(extra, separators=(",", ":")), ex=ttl_sec)
                except Exception:
                    pass

        except Exception:
            pass

    def _parse_book_payload(self, payload: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        if "data" in payload:
            try:
                nested = json.loads(payload["data"])
            except json.JSONDecodeError:
                nested = {}
        else:
            nested = {}

        # Zero-allocation fallback:
        def _get(key: str) -> Any:
            return nested.get(key) if key in nested else payload.get(key)

        bids = _ensure_list_levels(_get("bids"))
        asks = _ensure_list_levels(_get("asks"))
        ts_ms = normalize_epoch_ms(_get("ts") or _get("event_time"))

        book = {
            "symbol": symbol
            "ts": int(ts_ms or 0)
            "ts_ms": int(ts_ms or 0),  # deterministic exchange timestamp (ms)
            "first_id": _safe_int(_get("first_id") or _get("firstId") or _get("U"))
            "final_id": _safe_int(_get("final_id") or _get("finalId") or _get("u"))
            "prev_final": _safe_int(_get("prev_final") or _get("pu"))
            "bids": bids
            "asks": asks
        }
        return book

    # ── Конфигурация и инфраструктура ─────────────────────────────────────────


    def _validate_tick_time(self, runtime: SymbolRuntime, tick_ts: int, cfg: dict, indicators: dict) -> bool:
        """
        Validate and sanitize tick timestamp for monotonicity.
        Returns False if tick should be quarantined (hard rollback).
        """
        MAX_BACK_MS = int(os.getenv("TIME_MAX_BACK_MS", "2000"))
        WARN_BACK_MS = int(os.getenv("TIME_WARN_BACK_MS", "500"))
        prev_ts = int(getattr(runtime, "last_ts_ms", 0) or 0)

        if prev_ts > 0 and tick_ts < prev_ts:
            back = prev_ts - tick_ts
            self._pbatch.inc(tick_ts_backwards_total, {"symbol": runtime.symbol})

            if back <= MAX_BACK_MS:
                # sanitize: clamp forward
                tick_ts = prev_ts + 1
                self._pbatch.inc(tick_ts_clamped_total, {"symbol": runtime.symbol})
                indicators["tick_quality"] = "low"
                indicators["tick_ts_back_ms"] = int(back)
                indicators["tick_time_action"] = "reorder_soft"
                indicators["tick_ts_clamped"] = 1
                
                if back > WARN_BACK_MS:
                    self._pbatch.inc(ticks_out_of_order_total, {"symbol": runtime.symbol})
                    sampled_warning(self.logger, "TIME_SKEW", "⚠️ Time skew %s: back=%d", runtime.symbol, back)
            else:
                self._pbatch.inc(tick_ts_quarantined_total, {"symbol": runtime.symbol})
                indicators["tick_quarantined"] = 1
                indicators["tick_quarantine_reason"] = "reorder_hard"
                indicators["tick_ts_back_ms"] = int(back)
                return False
        
        runtime.last_ts_ms = int(tick_ts)
        return True

    def _update_liquidity_regime(self, runtime: SymbolRuntime, tick_ts: int, indicators: dict):
        """Calculates and updates liquidity regime indicators."""
        try:
            snap = getattr(runtime, "last_book", None)
            spread_bps = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
            depth_usd_min_5 = 0.0
            
            if snap is not None:
                spread_bps = float(getattr(snap, "spread_bps", spread_bps) or spread_bps)
                bb = float(getattr(snap, "best_bid_px", 0.0) or 0.0)
                ba = float(getattr(snap, "best_ask_px", 0.0) or 0.0)
                mid = (bb + ba) / 2.0 if (bb > 0 and ba > 0) else 0.0
                depth_qty = float(min(getattr(snap, "depth_5_bid_vol", 0.0) or 0.0
                                      getattr(snap, "depth_5_ask_vol", 0.0) or 0.0))
                depth_usd_min_5 = float(depth_qty * max(mid, 1e-9)) if mid > 0 else 0.0

            book_ts_base = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
            stale = int(tick_ts - book_ts_base) if book_ts_base > 0 else int(10**9)
            
            book_rate_hz = float(getattr(runtime, "book_rate_ema", 0.0) or 0.0)
            liq = runtime.liq_service.update(
                ts_ms=int(tick_ts)
                spread_bps=float(spread_bps)
                depth_min_5_usd=float(depth_usd_min_5)
                book_rate_hz=float(book_rate_hz)
            )
            runtime.liq_score = float(liq.score)
            runtime.liq_regime = str(liq.regime)
            
            indicators.update({
                IK.LIQ_SCORE: float(liq.score)
                IK.LIQ_REGIME: str(liq.regime)
                IK.LIQ_DEPTH_USD_5: float(depth_usd_min_5)
                IK.LIQ_SPREAD_BPS: float(liq.spread_bps)
                "liq_book_rate_hz": float(liq.book_rate_hz)
                "liq_book_stale_ms": int(stale)
            })
            if liq.score < 0.60:
                indicators["liq_why"] = f"score<={liq.score:.2f}"
        except Exception as exc:
            log_silent_error(exc, "liq_update_failed", runtime.symbol, "_update_liquidity_regime")

    def _update_l3_stats(self, runtime: SymbolRuntime, tick_ts: int, tick: dict):
        """Feeds L3-lite proxy and handles bucket advancement."""
        try:
            qty_f = float(tick.get("qty") or 0.0)
            side_sign = 0
            bm = tick.get("is_buyer_maker")
            if bm is not None:
                side_sign = -1 if bool(bm) else 1
            else:
                s = str(tick.get("side") or "").upper()
                if s == "BUY": side_sign = 1
                elif s == "SELL": side_sign = -1

            if qty_f > 0.0 and side_sign != 0:
                runtime.l3_queue.on_trade(side=side_sign, qty=qty_f)
            
            bucket_ms = runtime.l3_queue.bucket_ms or 1000
            cur_bucket_id = int(tick_ts // bucket_ms)
            
            if runtime._last_l3_bucket_id is None:
                runtime._last_l3_bucket_id = cur_bucket_id
            elif cur_bucket_id > runtime._last_l3_bucket_id:
                # Меняем бакет
                runtime.l3_stats = runtime.l3_queue.on_bucket_advance(bucket_id=runtime._last_l3_bucket_id)
                runtime._last_l3_bucket_id = cur_bucket_id

                # --- Hawkes-like online intensities ---
                if runtime.l3_stats:
                    hs = getattr(runtime, "hawkes_state", None)
                    if hs is None:
                        hs = {"ts_ms": int(tick_ts), "S_taker": 0.0, "S_cancel": 0.0, "S_churn": 0.0}
                        runtime.hawkes_state = hs

                    t_now = int(tick_ts)
                    prev_ts = int(hs.get("ts_ms", t_now))
                    dt_s = max(0.0, (t_now - prev_ts) / 1000.0)
                    hs["ts_ms"] = t_now

                    tb = float(getattr(runtime.l3_stats, "taker_buy_rate_ema", 0.0) or 0.0)
                    tsell = float(getattr(runtime.l3_stats, "taker_sell_rate_ema", 0.0) or 0.0)
                    cb = float(getattr(runtime.l3_stats, "cancel_bid_rate_ema", 0.0) or 0.0)
                    ca = float(getattr(runtime.l3_stats, "cancel_ask_rate_ema", 0.0) or 0.0)

                    taker_rate = max(0.0, tb + tsell)
                    cancel_rate = max(0.0, cb + ca)
                    churn_rate = taker_rate + cancel_rate

                    cfg_l3 = getattr(runtime, "config", {}) or {}
                    beta = float(cfg_l3.get("hawkes_beta", 1.8) or 1.8)

                    if dt_s > 0.0:
                        decay = math.exp(-beta * dt_s)
                        hs["S_taker"] = decay * float(hs.get("S_taker", 0.0)) + taker_rate * dt_s
                        hs["S_cancel"] = decay * float(hs.get("S_cancel", 0.0)) + cancel_rate * dt_s
                        hs["S_churn"] = decay * float(hs.get("S_churn", 0.0)) + churn_rate * dt_s

                    runtime.hawkes_snapshot = {
                        "hawkes_dt_s": float(dt_s)
                        "hawkes_taker_lam": float(cfg_l3.get("hawkes_mu_taker", 0.1) + cfg_l3.get("hawkes_alpha_taker", 0.9) * hs["S_taker"])
                        "hawkes_cancel_lam": float(cfg_l3.get("hawkes_mu_cancel", 0.1) + cfg_l3.get("hawkes_alpha_cancel", 0.7) * hs["S_cancel"])
                        "hawkes_churn_lam": float(cfg_l3.get("hawkes_mu_churn", 0.1) + cfg_l3.get("hawkes_alpha_churn", 0.5) * hs["S_churn"])
                    }
        except Exception as exc:
            log_silent_error(exc, "l3_update_failed", runtime.symbol, "_update_l3_stats")

    def _eval_dn_gate(self, runtime: SymbolRuntime, tick_ts: int, delta_event: dict, price: float, indicators: dict) -> Tuple[bool, int, float, Any]:
        """
        Evaluates DeltaNotional tier gating. 
        Returns (passed, tier, delta_usd, decision_obj).
        """
        rg = str(getattr(runtime, "last_regime", "na"))
        dn_tiers_decision = runtime.tick_dn_calib.tiers(
            regime=rg
            ts_ms=int(tick_ts)
            default_t0=float(runtime.config.get("dn_tier0_usd", 30000.0))
            default_t1=float(runtime.config.get("dn_tier1_usd", 70000.0))
            default_t2=float(runtime.config.get("dn_tier2_usd", 150000.0))
        )
        
        runtime.dynamic_cfg.update({
            "dn_tier0_usd": float(dn_tiers_decision.tier0_usd)
            "dn_tier1_usd": float(dn_tiers_decision.tier1_usd)
            "dn_tier2_usd": float(dn_tiers_decision.tier2_usd)
            "dn_src": str(dn_tiers_decision.src)
        })
        
        delta_usd = abs(float(delta_event.get("delta", 0.0))) * price
        if delta_usd > 0:
             runtime.tick_dn_calib.update(regime=rg, dn_usd=delta_usd, ts_ms=int(tick_ts))
             
        tier = -1
        if delta_usd > dn_tiers_decision.tier2_usd: tier = 2
        elif delta_usd > dn_tiers_decision.tier1_usd: tier = 1
        elif delta_usd > dn_tiers_decision.tier0_usd: tier = 0

        min_tier = int(runtime.config.get("delta_tier_min", 0))
        passed = (tier >= min_tier)
        
        # Relax for memes
        if not passed and min_tier == 0 and tier == -1:
            from core.instrument_config import symbol_env_prefix
            prefix = symbol_env_prefix(runtime.symbol)
            if prefix in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF"):
                if delta_usd >= dn_tiers_decision.tier0_usd * 0.50:
                    passed, tier = True, 0
                    indicators["dn_gate_relaxed"] = 1

        sess = indicators.get("session", "OFF")
        runtime.dn_passrate.update(tier=tier, session=sess, passed=passed)
        
        res = "pass" if passed else "veto_tier"
        self._pbatch.inc(dn_gate_events_total, {"symbol": runtime.symbol, "tier": str(tier), "session": sess, "result": res})
        
        if not passed:
             if runtime.delta_log_sampler.should_log("dn_veto"):
                  logger.info("🛑 [DN-GATE] (%s) VETO: delta_usd=$%.0f < T%d session=%s"
                              runtime.symbol, delta_usd, min_tier, sess)
        
        indicators.update({
            "dn_tier": int(tier)
            "dn_usd": float(delta_usd)
            "dn_t1_usd": float(dn_tiers_decision.tier1_usd)
            "dn_src": str(dn_tiers_decision.src)
        })
        return passed, tier, delta_usd, dn_tiers_decision
