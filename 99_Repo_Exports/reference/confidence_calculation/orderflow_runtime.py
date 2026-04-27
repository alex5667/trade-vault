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

import json
import os
import time
import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Deque
from collections import deque
from dataclasses import dataclass, field

@dataclass
class BookSnapshot:
    best_bid_px: float
    best_bid_qty: float
    best_ask_px: float
    best_ask_qty: float
    top5_bids: List[Tuple[float, float]]  # [(px, qty), ...]
    top5_asks: List[Tuple[float, float]]
    ts_ms: int
    spread_bps: float
    depth_5_bid_vol: float
    depth_5_ask_vol: float

    @property
    def bids(self):
        return self.top5_bids

    @property
    def asks(self):
        return self.top5_asks

    def get(self, key: str, default: Any = None) -> Any:
        if key == "bids": return self.top5_bids
        if key == "asks": return self.top5_asks
        if key == "ts": return self.ts_ms
        if key == "ts_ms": return self.ts_ms
        return getattr(self, key, default)

    def __getitem__(self, key: str) -> Any:
        val = self.get(key)
        if val is None:
            raise KeyError(key)
        return val

    @staticmethod
    def from_raw(book: dict) -> "BookSnapshot":
        """
        Build snapshot from raw dict: {"bids": [[px,qty]...], "asks": [[px,qty]...], "ts_ms": ...}
        Keeps only top-5 to bound memory/CPU.
        """
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        ts_ms = _safe_int(book.get("ts_ms") or book.get("ts") or 0)

        def _top5(levels, reverse=False):
            # Ensure levels are sorted by price correctly even if provider sends unsorted/reversed
            try:
                sorted_levels = sorted(levels, key=lambda x: float(x[0]), reverse=reverse)
            except (ValueError, TypeError, IndexError):
                sorted_levels = levels
                
            out = []
            for it in sorted_levels[:5]:
                try:
                    px = float(it[0]); qty = float(it[1])
                    out.append((px, qty))
                except Exception:
                    continue
            return out

        tb = _top5(bids, reverse=True)  # Bids: highest first
        ta = _top5(asks, reverse=False) # Asks: lowest first

        best_bid_px, best_bid_qty = (tb[0][0], tb[0][1]) if tb else (0.0, 0.0)
        best_ask_px, best_ask_qty = (ta[0][0], ta[0][1]) if ta else (0.0, 0.0)

        mid = 0.5 * (best_bid_px + best_ask_px) if (best_bid_px > 0 and best_ask_px > 0) else 0.0
        spread_bps = 0.0
        if mid > 0 and best_ask_px >= best_bid_px and best_bid_px > 0:
            spread_bps = ((best_ask_px - best_bid_px) / mid) * 10_000.0

        depth_5_bid_vol = sum(q for _, q in tb)
        depth_5_ask_vol = sum(q for _, q in ta)

        return BookSnapshot(
            best_bid_px=float(best_bid_px),
            best_bid_qty=float(best_bid_qty),
            best_ask_px=float(best_ask_px),
            best_ask_qty=float(best_ask_qty),
            top5_bids=list(tb),
            top5_asks=list(ta),
            ts_ms=_safe_int(ts_ms),
            spread_bps=float(spread_bps),
            depth_5_bid_vol=float(depth_5_bid_vol),
            depth_5_ask_vol=float(depth_5_ask_vol),
        )


@dataclass(frozen=True)
class BookState:
    """Atomic snapshot of the latest order book state.

    Motivation: `consume_books()` used to update multiple `runtime.last_*` fields
    sequentially, so tick processing could observe a partially-updated book.
    With `BookState`, the producer builds the snapshot first and then assigns it
    in a single atomic write (`runtime.book_state = ...`).

    Fields:
      - raw: original decoded payload (bids/asks)
      - snap: bounded typed snapshot (top5)
      - prev_snap: previous snapshot (optional)
      - ts_ms: event time of book (epoch ms)
      - prev_ts_ms: event time of previous book (epoch ms)
      - ingest_ts_ms: wall clock time when book was ingested (epoch ms)
    """

    raw: Dict[str, Any]
    snap: BookSnapshot
    prev_snap: Optional[BookSnapshot]
    ts_ms: int
    prev_ts_ms: int
    ingest_ts_ms: int

from services.orderflow.configuration import (
    _safe_int
)
from core.pressure_tracker import PressureTracker
from core.burst_gate import BurstCandidateSelector
from core.robust_stats import RollingRobustZ

from core.eff_quote_calibrator import EffQuoteCalibrator
from core.atr_tf_calibrator import ATRTfCalibrator
from core.atr_sanity_calibrator import ATRSanityCalibrator
from core.atr_bps_calibrator import ATRBpsCalibrator




from services.orderflow.metrics import (
    log_silent_error
)
from services.orderflow.utils import (
    _dedup_seen_uid,
    LogSampler, LogSamplerFactory
)






from core.book_rate_calibrator import BookRateCalibrator
from core.delta_notional_calibrator import DeltaNotionalCalibrator
from core.daily_ohlc_tracker import DailyCandleTracker

from core.weak_progress import WeakProgressSnapshot
from common.zone_store import ZonePack
from core.reclaim_detector import ReclaimDetector, ReclaimEvent
from core.fp_edge_absorb import FPEdgeAbsorbDetector, EdgeAbsorbEvent

from core.sweep_detector import SweepDetector, SweepEvent

from core.tick_gap_tracker import TickGapTracker
from core.burst_calibrator import BurstCalibrator


# Consolidated core imports
from core.obi_stability_tracker import OBIStabilityTracker
from core.weak_progress_detector import WeakProgressDetector
from core.cvd_reclaim import CVDReclaimEvent
from core.session_telemetry import HourOfWeekScaleTracker
from core.session_telemetry import HourOfWeekScaleTracker
from services.orderflow.session_telemetry import PassRateEmaBySession
from core.ofi_tracker import OFIStabilityTracker
from core.liquidity_regime import LiquidityRegimeService






import redis.asyncio as aioredis

from core.crypto_orderflow_detectors import (
    AbsorptionDetector,
    DeltaSpikeDetector,
    IcebergDetector,
    OBIDetector,
)
from core.instrument_config import get_specs
from services.l3_queue_events_proxy import L3QueueEventsProxy
from core.atr_sanity_guard import RangeTfAggregator, tf_to_ms
from services.persistence_manager import get_persistence_manager
from core.tick_cvd import TickCVDState
from core.microbar import MicroBar, MicroBarAggregator
from core.swing_detector import SwingDetector, SwingPoint
from core.divergence_engine import DivergenceEngine, DivergenceEvent
from core.rsi import StreamingRSI
from core.eq_pools import EQPoolTracker
from core.dyn_cfg_keys import DynCfgKeys as DK


# ──────────────────────────────────────────────────────────────────────────────
# Настройки по умолчанию
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("crypto_orderflow_service")
# Настройка логирования
log_level = os.getenv("CRYPTO_OF_LOG_LEVEL", "INFO")
logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
# Доп. флаг: подробный DEBUG по дельте (по умолчанию выключен, чтобы не шуметь)
# Доп. флаг: подробный DEBUG по дельте (по умолчанию выключен, чтобы не шуметь)
DEBUG_DELTAS = os.getenv("CRYPTO_OF_DEBUG_DELTAS", "false").strip().lower() in ("1", "true", "yes", "on")






# Счетчик для уменьшения логов добавления символов
_symbols_added_counter = 0





# ──────────────────────────────────────────────────────────────────────────────
# Runtime для одного символа
# ──────────────────────────────────────────────────────────────────────────────


# Optional microstructure metrics (prom)




@dataclass
class SymbolRuntime:
    symbol: str
    config: Dict[str, Any]
    tick_stream: str = ""
    book_stream: str = ""
    tick_group: str = ""
    book_group: str = ""
    pm: Optional[Any] = field(default=None, init=False, repr=False)  # PersistenceManager (injectable for tests)
    redis_client: Optional[Any] = field(default=None, init=False, repr=False)  # Redis client for metrics (injectable)
    delta_detector: DeltaSpikeDetector = field(init=False)
    obi_detector: OBIDetector = field(init=False)
    absorption_detector: AbsorptionDetector = field(init=False)
    iceberg_detector: IcebergDetector = field(init=False)
    cvd_state: TickCVDState = field(init=False)
    l3_queue: L3QueueEventsProxy = field(init=False)
    liquidity: CryptoLiquidity = field(init=False)
    l3_stats: Optional[Any] = None  # Stores L3BucketStats
    _last_l3_bucket_id: Optional[int] = None
    
    # Structure engines (Phase B)
    microbar: MicroBarAggregator = field(init=False)
    swing: SwingDetector = field(init=False)
    divergence: DivergenceEngine = field(init=False)
    rsi_price: StreamingRSI = field(init=False)
    rsi_cvd: StreamingRSI = field(init=False)

    # Latest structure snapshots
    last_bar: Optional[MicroBar] = None
    last_swing_high: Optional[SwingPoint] = None
    last_swing_low: Optional[SwingPoint] = None
    prev_swing_high: Optional[SwingPoint] = None
    prev_swing_low: Optional[SwingPoint] = None
    last_div: Optional[DivergenceEvent] = None

    last_snapshot_ts_ms: int = 0
    last_of_strong_ts_ms: int = 0
    last_of_dir: str = "NONE"

    # NEW: dynamic regime state
    last_regime: str = "na"

    # Book Health State
    last_book_health_ok: int = 1
    last_book_health: str = "OK"
    last_book_age_ms: int = 0
    
    # Separate: last emitted signal (even if strong gate did NOT pass)
    last_emit_ts_ms: int = 0
    last_emit_dir: str = "NONE"
    # Separate: last strong-pass (of_confirm_ok==1)
    last_strong_pass_ts_ms: int = 0
    last_strong_pass_dir: str = "NONE"
    # Cache last strong-pass score (only meaningful when pass)
    last_strong_pass_score: float = 0.0

    # Phase C: liquidity pools + sweeps
    eq_pools: EQPoolTracker = field(init=False)
    sweep: SweepDetector = field(init=False)
    last_sweep: Optional[SweepEvent] = None
    # CVD baseline at sweep moment (bar close). Needed for CVD reclaim evidence.
    last_sweep_ts_ms: int = 0
    last_sweep_cvd: float = 0.0

    # NEW: weak progress snapshot from last closed microbar
    last_wp: Optional[WeakProgressSnapshot] = None

    # reclaim detector and last reclaim
    reclaim: ReclaimDetector = field(init=False)
    last_reclaim: Optional[ReclaimEvent] = None
    # CVD reclaim evidence (computed ONLY when reclaim confirmed)
    last_cvd_reclaim: Optional[CVDReclaimEvent] = None

    # OFI evidence (best bid/ask incremental flow)
    ofi_tracker: OFIStabilityTracker = field(init=False)
    last_ofi_event: Optional[Dict[str, Any]] = None

    # Liquidity regime (risk overlay)
    liq_service: LiquidityRegimeService = field(init=False)
    last_liq: Optional[Dict[str, Any]] = None
    liq_score: float = 0.0
    liq_regime: str = "normal"

    # NEW: footprint edge absorb detector and last event
    fp_edge: FPEdgeAbsorbDetector = field(init=False)
    last_fp_edge: Optional[EdgeAbsorbEvent] = None


    # NEW: continuation context (countertrend absorption observed recently)
    cont_ctx_ts_ms: int = 0
    cont_ctx_trend_dir: str = ""

    # ATR caching for bar_close (avoid Redis on every 1s bar)
    last_atr: float = 0.0
    last_atr_ts_ms: int = 0
    # ATR sanity: TF-range proxy (roll-up microbars -> atr_tf)
    atr_range_agg: object = field(init=False)
    
    # Adaptive Pressure Proxy Tier Calibration (legacy - kept for compatibility)
    ptier_samples_usd: Deque[float] = field(default_factory=lambda: deque(maxlen=4096))
    ptier_last_update_ts_ms: int = 0
    
    # Pressure Tier Calibrator (Expert Recommendation - Production Ready)
    # Adaptive DN threshold calibration with regime-awareness and hysteresis
    ptier_calib: object = field(init=False)  # PressureTierCalibrator instance

    tick_buffer: Deque[Dict[str, Any]] = field(init=False)
    # Raw (full) book dict from Redis stream (fail-open compatibility for detectors)
    last_book_raw: Optional[Dict[str, Any]] = None
    # Typed snapshot for microstructure analytics (top5 + best bid/ask)
    last_book: Optional[BookSnapshot] = None
    prev_book: Optional[BookSnapshot] = None
    # Atomic snapshot (preferred by tick path)
    book_state: Optional[BookState] = None
    last_book_ts_ms: int = 0
    prev_book_ts_ms: int = 0
    book_rate_ema: float = 0.0
    # spread robust stats (microstructure)
    spread_stats: RollingRobustZ = field(init=False)
    last_spread_bps: float = 0.0
    last_spread_z: float = 0.0

    # --- Runtime overrides (cooldown/pressure policy) ---
    _ov_ts_ms: int = 0
    _ov_etag: str = ""
    _ov_poll_gap_ms: int = 2500
    book_rate_stats: RollingRobustZ = field(init=False)
    book_rate_z: float = 0.0
    book_churn_score: float = 0.0
    book_churn_hi: int = 0
    last_obi_event: Optional[Dict[str, Any]] = None
    last_iceberg_event: Optional[Dict[str, Any]] = None
    last_signal_ts: int = 0
    # Overtrading/churn proxy: cooldown filtered rate (EMA, signals/sec)
    cooldown_hits_ema: float = 0.0
    cooldown_last_ts_ms: int = 0

    # Pressure/burst (tick-ts deterministic)
    pressure: PressureTracker = field(init=False)
    pressure_sps: float = 0.0
    cooldown_hit_rate_ema: float = 0.0
    
    # DeltaNotional calibrator
    dn_calib: DeltaNotionalCalibrator = field(init=False)
    tick_dn_calib: DeltaNotionalCalibrator = field(init=False) # Separate for Tick Triggers
    _dn_loaded: bool = False
    _dn_last_persist_ts_ms: int = 0

    burst: BurstCandidateSelector = field(init=False)
    tick_gaps: TickGapTracker = field(init=False)
    # NEW: lock for burst/flush race protection
    burst_mu: asyncio.Lock = field(default_factory=asyncio.Lock)
    # NEW: monotonic tick time tracking
    last_tick_ts_ms: int = 0
    # Tick dedup (best-effort). Helps avoid double-counting on retries/replays.
    tick_dedup_window: int = 4096
    tick_uid_ring: Deque[str] = field(default_factory=lambda: deque(maxlen=4096), repr=False)
    tick_uid_set: Set[str] = field(default_factory=set, repr=False)
    burst_cal: BurstCalibrator = field(init=False)
    # простая телеметрия
    tick_count: int = 0
    heartbeat_counter: int = 0
    delta_triggers: int = 0
    signal_count: int = 0
    
    # Counters moved from Service
    strong_gate_counter: int = 0
    low_conf_counter: int = 0
    swing_point_counter: int = 0
    
    last_metrics_ts: float = 0.0
    
    # --- Candidate pressure (cooldown flood) + best-of-burst ---
    signal_attempt_ts_ms: Deque[int] = field(default_factory=lambda: deque(maxlen=1200))  # ~20min if 1Hz
    pressure_sps: float = 0.0
    pressure_hi: int = 0

    pending_payload: Optional[Dict[str, Any]] = None
    pending_score: float = 0.0
    pending_ts_ms: int = 0
    pending_replaced: int = 0

    # Adverse Selection Verification (Continuation)
    pending_adverse_payload: Optional[Dict[str, Any]] = None
    pending_adverse_ts_ms: int = 0

    # Log samplers for high-frequency messages
    delta_log_sampler: LogSampler = field(init=False)
    weak_signal_log_sampler: LogSampler = field(init=False)
    signal_emit_log_sampler: LogSampler = field(init=False)
    loop_log_sampler: LogSampler = field(init=False)
    
    # Counter for absorption signal logging (log every 10,000th)
    absorption_signal_count: int = field(default=0)


    # Strong gate diagnostics snapshot (latest)
    last_of_confirm_score: float = 0.0
    last_strong_gate_have: int = 0
    last_strong_gate_need: int = 0
    last_strong_gate_scn: str = ""

    last_ts_ms: int = 0

    # HTF zones cache (real zones from geometry publisher)
    zones_pack: Optional[ZonePack] = None
    zones_last_load_ts_ms: int = 0
    
    # Dynamic specs from Redis (auto-calibration)
    calibrated_specs: Dict[str, Any] = field(default_factory=dict)
    dynamic_cfg: Dict[str, Any] = field(default_factory=dict)
    spec_update_ts_ms: int = 0
    _history_loaded: bool = False
    _pg_loaded: bool = False

    # --- Overrides v1 cache (versioned policy, SRE-style) ---
    overrides_sid: str = ""
    overrides_loaded_ts_ms: int = 0
    overrides_obj: Any = None

    async def maybe_load_overrides(self, r) -> None:
        """
        Load versioned overrides (orderflow) from Redis.
        Keys:
          cfg:orderflow:overrides:v1:active_sid
          cfg:orderflow:overrides:v1:meta:{sid}
        Cache TTL: 30s
        Fail-open: if anything fails -> keep previous.
        """
        try:
            now = int(time.time() * 1000)
            # Default TTL 30s
            ttl = int(self.config.get("overrides_cache_ttl_ms", 30000))
            if ttl > 0 and (now - int(self.overrides_loaded_ts_ms or 0)) < ttl:
                return
            
            # 1. Get active pointer
            active_sid = str(await r.get("cfg:orderflow:overrides:v1:active_sid") or "")
            if not active_sid:
                self.overrides_sid = ""
                self.overrides_obj = None
                self.overrides_loaded_ts_ms = now
                return

            # 2. If same as loaded, update timestamp (heartbeat)
            if active_sid == self.overrides_sid and self.overrides_obj is not None:
                self.overrides_loaded_ts_ms = now
                return

            # 3. Load meta
            raw = await r.get(f"cfg:orderflow:overrides:v1:meta:{active_sid}")
            if not raw:
                self.overrides_loaded_ts_ms = now
                return

            # 4. Parse strict schema
            from core.orderflow_overrides_v1 import OrderflowOverridesV1
            o, status = OrderflowOverridesV1.from_json(str(raw))
            if o is None:
                # Invalid schema => ignore/keep previous
                # (OR clear? Fail-open usually means ignore bad config)
                self.overrides_loaded_ts_ms = now
                return
            
            self.overrides_sid = active_sid
            self.overrides_obj = o
            self.overrides_loaded_ts_ms = now
        except Exception:
            return

    # ATR TF selection (persisted)
    _atr_tf_loaded: bool = False
    _atr_tf_last_persist_ts_ms: int = 0
    _atr_tf_bars_since_persist: int = 0

    # --- Pivot Persistence: Daily Candle Tracker ---
    last_day_date: Optional[str] = None
    daily_open: float = 0.0
    daily_high: float = 0.0
    daily_low: float = 0.0
    daily_volume: float = 0.0

    # ATR Sanity Calibrator (Source Selection)
    atr_sanity: ATRSanityCalibrator = field(init=False)
    atr_src_calib: ATRSanityCalibrator = field(init=False) # alias
    _atr_sanity_loaded: bool = False
    _atr_sanity_last_persist_ts_ms: int = 0
    _atr_sanity_bars_since_persist: int = 0


    # NEW: session/how telemetry
    dn_passrate: PassRateEmaBySession = field(init=False)
    how_scale: HourOfWeekScaleTracker = field(init=False)
    last_dn_how_alert_ts_ms: int = 0
    last_dn_how_report_ts_ms: int = 0

    # State flags
    ready: bool = False
    
    # Checksum / Sequence tracking
    last_u: int = 0
    last_id: int = 0
    
    # Last state
    last_price: float = 0.0
    
    # Book / Spread stats
    last_spread_bps_l2: float = 0.0
    last_book_mid: float = 0.0
    # Top-5 depth (from book snapshots)
    last_depth_bid_5: float = 0.0
    last_depth_ask_5: float = 0.0
    last_depth_min_5_usd: float = 0.0
    
    # Liquidity regime output
    last_liq_score: float = 0.0
    last_liq_regime: str = "na"
    liq_guard: LiquidityRegimeService = field(init=False)
    
    # OFI (incremental L1 flow)
    # ofi_tracker: OFITracker = field(init=False) # Already exists as OFIStabilityTracker
    # last_ofi_event: Optional[Dict[str, Any]] = None # Already exists
    
    # CVD reclaim (bonus-only): сохраняем только на reclaim event
    # last_sweep_cvd: float = 0.0 # Already exists
    # last_sweep_ts_ms: int = 0 # Already exists
    # last_cvd_reclaim: Optional[CVDReclaimEvent] = None # Already exists

    # Detectors (initialized in __post_init__)
    # delta_detector: DeltaSpikeDetector = field(init=False) # Already exists
    # obi_detector: OBIDetector = field(init=False) # Already exists
    # iceberg_detector: IcebergDetector = field(init=False) # Already exists
    # absorption_detector: AbsorptionDetector = field(init=False) # Already exists
    
    # Trackers
    # pressure: PressureTracker = field(init=False) # Already exists
    # burst: BurstCandidateSelector = field(init=False) # Already exists
    # spread_stats: RollingRobustZ = field(init=False) # Already exists
    
    # Pending Signal (for Cooldown/Burst)
    # pending_payload: Optional[Dict[str, Any]] = None # Already exists
    # pending_score: float = 0.0 # Already exists
    # pending_ts_ms: int = 0 # Already exists
    # pending_replaced: int = 0 # Already exists
    
    # Dynamic config overrides (from calibration etc.)
    # dynamic_cfg: Dict[str, Any] = field(default_factory=dict) # Already exists
    
    # OBI Tracker
    # obi_tracker: OBIStabilityTracker = field(init=False) # Already exists
    obi_stable: bool = False
    obi_stable_secs: float = 0.0
    obi_stability_score: float = 0.0
    # last_obi_event: Optional[Dict[str, Any]] = None # Already exists
    # last_iceberg_event: Optional[Dict[str, Any]] = None # Already exists
    liq_service: LiquidityRegimeService = field(init=False)


    def __post_init__(self) -> None:
        self.apply_config(self.config)
        # default selected tf = config atr_tf
        try:
            self.dynamic_cfg[DK.ATR_TF_SELECTED] = str(self.config.get("atr_tf", "1m") or "1m")
        except Exception:
            self.dynamic_cfg[DK.ATR_TF_SELECTED] = "1m"
            
        # Initialize Liquidity Regime Service using global config + overrides
        from core.liquidity_regime import LiquidityRegimeService
        self.liq_service = LiquidityRegimeService(symbol=self.symbol, cfg=self.config)

        # Pressure/burst defaults (config-tunable, safe)
        pw = int(self.config.get("pressure_window_ms", 60_000))
        pa = float(self.config.get("pressure_ema_alpha", 0.20))
        self.pressure = PressureTracker(window_ms=max(5_000, pw), ema_alpha=pa)

        bw = int(self.config.get("burst_window_ms", 2500))
        ba = int(self.config.get("burst_max_age_ms", 8000))
        self.burst = BurstCandidateSelector(window_ms=max(0, bw), max_age_ms=max(1000, ba))
        self.tick_gaps = TickGapTracker(window=int(self.config.get("tick_gap_window", 512)))
        self.burst_cal = BurstCalibrator(
            base_window_ms=int(self.config.get("burst_window_ms", 2500)),
            min_window_ms=int(self.config.get("burst_window_min_ms", 300)),
            max_window_ms=int(self.config.get("burst_window_max_ms", 3000)),
            base_max_age_ms=int(self.config.get("burst_max_age_ms", 8000)),
            pressure_hi_per_min=float(self.config.get("pressure_hi_per_min", 60.0)),
            pressure_extreme_per_min=float(self.config.get("pressure_extreme_per_min", 200.0)),
        )
        
        # ATR sanity range aggregator (deterministic TF roll-up)
        try:
            atr_tf = str(self.config.get("atr_tf", "1m") or "1m")
            tf_ms = tf_to_ms(atr_tf)
            min_samples = int(self.config.get("atr_sanity_min_samples", 30))
        except Exception:
            # defaults
            tf_ms = 60_000
            min_samples = 30
        self.atr_range_agg = RangeTfAggregator(tf_ms=tf_ms, min_samples=min_samples)
        
        # ATR Sanity Calibrator (Source Selector)
        # Replaced with user's specific naming from diff: atr_sanity 
        try:
            ms = int(self.config.get("atr_sanity_min_samples", int(os.getenv("ATR_SANITY_MIN_SAMPLES", "500"))) or 500)
            max_age = int(self.config.get("atr_sanity_max_age_ms", int(os.getenv("ATR_SANITY_MAX_AGE_MS", "180000"))) or 180000)
        except Exception:
            ms = 500
            max_age = 180000
        self.atr_sanity = ATRSanityCalibrator(min_samples=ms, max_age_ms=max_age)
        
        # Legacy/previous field for compat if needed (but we will switch to atr_sanity primarily)
        self.atr_src_calib = self.atr_sanity 



        # Spread robust stats
        sw = int(self.config.get("spread_stats_window", 300))
        self.spread_stats = RollingRobustZ(window=max(32, sw))
        
        # Daily Candle Tracker
        self.daily_tracker = DailyCandleTracker(self.symbol)
        


        # Book rate stats (for churn)
        rw = int(self.config.get("book_rate_stats_window", 300))
        self.book_rate_stats = RollingRobustZ(window=max(32, rw))
        # Dynamic calibration (eff_quote thresholds per regime)
        self.eff_calib = EffQuoteCalibrator(min_samples=int(os.getenv("EFF_CALIB_MIN_SAMPLES", "300")))
        # ATR TF calibration (per regime)
        # ATR TF Calibrator (per regime)
        # ATR TF selector (service-level; deterministic inputs come from bar_close)
        cand_raw = os.getenv("ATR_TF_CANDIDATES", "1m,5m,15m")
        cands = [x.strip() for x in str(cand_raw).split(",") if x.strip()]
        self.atr_tf_calib = ATRTfCalibrator(
            candidates=cands,
            min_atr_bps=float(os.getenv("ATR_TF_MIN_ATR_BPS", "0.10")),
            max_atr_bps=float(os.getenv("ATR_TF_MAX_ATR_BPS", "500.0")),
            max_jump_mult=float(os.getenv("ATR_TF_MAX_JUMP_MULT", "4.0")),
        )

        
        # --------------------------------------
        # ATR(bps) floor tiers calibrator (v1)
        # --------------------------------------
        self.atr_bps_calib = ATRBpsCalibrator(
            min_samples=int(os.getenv("ATR_BPS_CALIB_MIN_SAMPLES", "500"))
        )
        self._atr_bps_loaded: bool = False
        self._atr_bps_last_persist_ts_ms: int = 0
        # BookRate calibration: auto-tune min/warn Hz per regime (for book_health gate)
        try:
            br_ms = int(self.config.get("book_calib_min_samples", 300))
            br_dtm = int(self.config.get("book_calib_dt_max_ms", 2000))
        except Exception:
            br_ms = 300
            br_dtm = 2000
        self.br_calib = BookRateCalibrator(min_samples=br_ms, dt_max_ms=br_dtm)
        
        # Pressure Tier Calibrator (Expert Recommendation - Production Ready)
        # Adaptive DN threshold calibration with regime-awareness and hysteresis
        from core.pressure_tier_calibrator import PressureTierCalibrator
        self.ptier_calib = PressureTierCalibrator(
            min_samples=int(self.config.get("ptier_min_samples", 300)),
            window=int(self.config.get("ptier_window", 2000)),
            recompute_gap_ms=int(self.config.get("ptier_recompute_gap_ms", 10_000)),
            hold_ms=int(self.config.get("ptier_hold_ms", 60_000)),
            max_jump_mult=float(self.config.get("ptier_max_jump_mult", 2.0)),
        )

        # Delta Notional Calibrator (Classic/Legacy) - required for ensure_calibration_loaded
        self.dn_calib = DeltaNotionalCalibrator(
            min_samples=int(self.config.get("dn_calib_min_samples", 300))
        )
        # Separate Trigger DN Calibrator (P2)
        self.tick_dn_calib = DeltaNotionalCalibrator(
            min_samples=int(self.config.get("dn_calib_min_samples", 300))
        )
        
        # --------------------------------------
        # OBI Stability Tracker (Strong OF Confirmation)
        # --------------------------------------
        # from core.obi_stability_tracker import OBIStabilityTracker
        self.obi_tracker = OBIStabilityTracker(
            window_ms=int(self.config.get("obi_stable_window_ms", 3000)),
            threshold=float(self.config.get("obi_threshold", 0.25)),
            deadband=float(self.config.get("obi_deadband", 0.05)),
            grace_ms=int(self.config.get("obi_grace_ms", 250)),
        )
        self.obi_stable_secs = 0.0
        self.obi_stability_score = 0.0
        self.obi_stable = False
        
        # --------------------------------------
        # Weak Progress Detector (History) - Absorption Mode
        # --------------------------------------
        # from core.weak_progress_detector import WeakProgressDetector
        self.weak_progress_det = WeakProgressDetector(
            maxlen=int(self.config.get("weak_history_maxlen", 50)),
            recent_window=int(self.config.get("weak_recent_window", 5)),
            range_max_atr=float(self.config.get("weak_range_max_atr", 0.30)),
            body_max_atr=float(self.config.get("weak_body_max_atr", 0.35)),
            eff_max=float(self.config.get("weak_eff_max", 0.02)),
        )

        # --------------------------------------
        # OFI tracker (best bid/ask incremental flow)
        # --------------------------------------
        self.ofi_tracker = OFIStabilityTracker(
            window_ms=int(self.config.get("ofi_window_ms", 3000)),
            z_window=int(self.config.get("ofi_z_window", 256)),
        )

        # --------------------------------------
        # Liquidity regime service (risk overlay) -> Initialized at top
        # --------------------------------------
        
        # --------------------------------------
        # CVD Reclaim (bonus-layer)
        # --------------------------------------
        # Computed only when reclaim is confirmed (discrete microbar grid).
        # We store baseline at sweep, and evaluate at reclaim.
        self.last_cvd_reclaim = None
        

        


        # self._calib_loaded is for eff_quote, but it seems shared in some versions. 
        # I'll stick to the MEGA-DIFF.
        

        
        # L3-lite proxy
        l3_alpha = float(os.getenv("L3_TAKER_RATE_EMA_ALPHA", "0.12"))
        bucket_ms = int(self.config.get("delta_bucket_ms", 1000) or 1000)
        
        # Calibration loaded flags (unified or separate)
        self._calib_loaded: bool = False
        self._book_calib_loaded: bool = False
        self._dn_calib_loaded: bool = False
        
        # Separate throttles (avoid coupling lifecycles)
        self._book_calib_last_persist_ts_ms: int = 0
        self._dn_calib_last_persist_ts_ms: int = 0
        
        self._atr_tf_loaded: bool = False
        self._atr_tf_last_persist_ts_ms: int = 0
        
        self._bookrate_sample_bucket: int = -1
        self.l3_queue = L3QueueEventsProxy(bucket_ms=bucket_ms, alpha=l3_alpha)
        from handlers.crypto_orderflow.components.liquidity import CryptoLiquidity
        self.liquidity = CryptoLiquidity()

        # Signal pressure deque
        self._sig_times_ms = deque(maxlen=200)
        self.cooldown_hits_ema = 0.0

        # Log samplers initialization
        try:
             d_rate = float(self.config.get("delta_log_sample_rate", 0.05))
             d_n = int(1.0 / d_rate) if d_rate > 0 else 100
        except: d_n = 20

        try:
             w_rate = float(self.config.get("weak_signal_log_sample_rate", 0.05))
             w_n = int(1.0 / w_rate) if w_rate > 0 else 100
        except: w_n = 20

        self.delta_log_sampler = LogSampler(sample_rate=d_n)
        self.weak_signal_log_sampler = LogSampler(sample_rate=w_n)
        # Log every Nth signal emission (default: 10000, i.e., log every 10000th occurrence)
        signal_emit_rate = int(os.getenv("SIGNAL_EMIT_LOG_SAMPLE_RATE", "10000"))
        self.signal_emit_log_sampler = LogSampler(sample_rate=signal_emit_rate)

        # Loop diagnostics sampler
        loop_rate = int(os.getenv("LOG_SAMPLE_LOOP_DIAGNOSTIC_RATE", "10000"))
        self.loop_log_sampler = LogSamplerFactory.get_sampler("LOOP_DIAGNOSTIC", loop_rate)

        # General stream throttle (Processing N stream entries)
        self.throttle_log_sampler = LogSampler(sample_rate=10000)
        
        # Initialization samplers (every 10000th)
        LogSamplerFactory.get_sampler("WORKER_INIT", 10000)
        LogSamplerFactory.get_sampler("TICK_HELPER_INIT", 10000)
        
        self.cooldown_last_ts_ms = 0

        # NEW: DN Telemetry initialization
        asia_end = int(os.getenv("SESSION_ASIA_END_H", "8"))
        eu_end = int(os.getenv("SESSION_EU_END_H", "16"))

        self.dn_passrate = PassRateEmaBySession(
            alpha=float(os.getenv("DN_PASSRATE_EMA_ALPHA", "0.05"))
        )

        self.how_scale = HourOfWeekScaleTracker(
            alpha=float(os.getenv("HOW_SCALE_EMA_ALPHA", "0.02")),
            clamp_low=float(os.getenv("HOW_SCALE_CLAMP_LOW", "0.5")),
            clamp_high=float(os.getenv("HOW_SCALE_CLAMP_HIGH", "2.0")),
            min_bucket_n=int(os.getenv("HOW_SCALE_MIN_BUCKET_N", "300")),
            min_global_n=int(os.getenv("HOW_SCALE_MIN_GLOBAL_N", "2000")),
        )
        self.last_dn_how_alert_ts_ms = 0









    def get_atr_tf_selected(self) -> str:
        """
        Canonical resolver for ATR timeframe.
        Single source of truth: dynamic_cfg[DK.ATR_TF_SELECTED] -> config -> fallback.
        All ATR calculations MUST use this method.
        """
    
    def is_duplicate_tick_uid(self, uid: str) -> bool:
        """Return True if uid has been seen in recent window; otherwise record and return False."""
        return _dedup_seen_uid(uid, self.tick_uid_ring, self.tick_uid_set, int(self.tick_dedup_window or 0))
        tf = str((self.dynamic_cfg or {}).get("atr_tf_selected") or self.config.get("atr_tf") or "1m")
        tf = tf.strip()
        return tf if tf else "1m"

    async def ensure_history_loaded(self) -> None:
        """
        Loads historical microbars from PostgreSQL and "warms up" detectors.
        Restores RSI, Swing, etc.
        """
        if getattr(self, "_history_loaded", False):
            return
        
        try:
            pm = (self.pm or get_persistence_manager())
            limit = int(self.config.get("history_warmup_limit", 200))
            bars = await pm.load_microbar_history(self.symbol, limit=limit)
            
            if not bars:
                logger.info(f"ℹ️ No historical microbars found in PG for {self.symbol}")
                self._history_loaded = True
                return

            logger.info(f"🔄 Warming up {self.symbol} with {len(bars)} historical bars from PG")
            
            # Prepare MicroBar objects for replay
            # from core.microbar import MicroBar
            for b_dict in bars:
                mb = MicroBar(
                    symbol=self.symbol,
                    tf_ms=1000,
                    start_ts_ms=int(b_dict['ts_ms']) - 1000, # Approximation for warmup
                    end_ts_ms=int(b_dict['ts_ms']),
                    open=float(b_dict['open']),
                    high=float(b_dict['high']),
                    low=float(b_dict['low']),
                    close=float(b_dict['close']),
                    vol=float(b_dict['vol']),
                    cvd_close=float(b_dict['cvd_close'])
                )
                # Feed to detectors
                try:
                    if hasattr(self, "swing"): self.swing.update(mb)
                    if hasattr(self, "rsi_price"): self.rsi_price.update(mb.close)
                    if hasattr(self, "rsi_cvd"): self.rsi_cvd.update(mb.cvd_close)
                    # Update pools
                    if hasattr(self, "eq_pools"): self.eq_pools.on_bar(mb)
                except Exception:
                    pass
                
            self._history_loaded = True
        except Exception as e:
            logger.error(f"❌ Error during history warmup for {self.symbol}: {e}")
            self._history_loaded = True # Prevent infinite retry

    def apply_config(self, new_config: Dict[str, Any]) -> None:
        """
        Обновляет конфиг и перезагружает детекторы без потери истории тиков.
        """
        prev_ticks: List[Dict[str, Any]] = list(self.tick_buffer) if hasattr(self, "tick_buffer") else []
        self.config = new_config.copy()
        self.tick_buffer = deque(prev_ticks, maxlen=self.config["tick_buffer"])
        # keep pressure deque; do not reset on config reload

        # Tick dedup window (env overrides config). Keep small and bounded.
        try:
            w_env = os.getenv("TICK_DEDUP_WINDOW", "")
            w_cfg = self.config.get("tick_dedup_window", 4096)
            w = int(w_env) if w_env.strip() else int(w_cfg or 4096)
        except Exception:
            w = 4096
        self.tick_dedup_window = max(0, min(200_000, w))
        # If window shrank, trim.
        try:
            while len(self.tick_uid_ring) > self.tick_dedup_window:
                old = self.tick_uid_ring.popleft()
                if old:
                    self.tick_uid_set.discard(old)
        except Exception:
            pass

        # Tick-CVD (Phase A)
        try:
            if hasattr(self, "cvd_state") and self.cvd_state:
                self.cvd_state.apply_config(self.config)
                # Update Redis client if available
                if hasattr(self, "redis_client") and self.redis_client is not None:
                    self.cvd_state.redis = self.redis_client
            else:
                self.cvd_state = TickCVDState(
                    symbol=self.symbol,
                    reset_mode=self.config.get("cvd_reset_mode", "day"),
                    ema_period_delta=int(self.config.get("cvd_ema_period_delta", 10)),
                    ema_period_cvd=int(self.config.get("cvd_ema_period_cvd", 20)),
                    robust_window=int(self.config.get("cvd_robust_w", 500)),
                    redis_client=getattr(self, "redis_client", None),
                )
        except Exception as exc:
            # Fail-open
            log_silent_error(exc, 'config_parse_failure', self.symbol, 'apply_config:cvd_state')
            if not hasattr(self, "cvd_state"):
                self.cvd_state = TickCVDState(symbol=self.symbol, redis_client=getattr(self, "redis_client", None))

        # Structure engines (Phase B)
        try:
            if hasattr(self, "microbar") and self.microbar:
                self.microbar.apply_config(self.config)
            else:
                self.microbar = MicroBarAggregator(
                    symbol=self.symbol,
                    mode=self.config.get("microbar_mode", "time"),
                    tf_ms=int(self.config.get("microbar_tf_ms", 1000)),
                    volume_target=float(self.config.get("microbar_volume_target", 0.0)),
                    tick_size=float(self.config.get("tick_size", 0.0)),
                )
        except Exception as exc:
            log_silent_error(exc, 'config_parse_failure', self.symbol, 'apply_config:microbar')
            if not hasattr(self, "microbar"):
                self.microbar = MicroBarAggregator(symbol=self.symbol)

        try:
            if hasattr(self, "swing") and self.swing:
                self.swing.apply_config(self.config)
            else:
                self.swing = SwingDetector(
                    left=int(self.config.get("swing_left", 3)),
                    right=int(self.config.get("swing_right", 3)),
                    min_bp=float(self.config.get("swing_min_bp", 5.0)),
                    min_range_bp=float(self.config.get("swing_min_range_bp", 1.0)),
                )
        except Exception as exc:
            log_silent_error(exc, 'config_parse_failure', self.symbol, 'apply_config:swing')
            if not hasattr(self, "swing"):
                self.swing = SwingDetector()

        try:
            if hasattr(self, "divergence") and self.divergence:
                self.divergence.apply_config(self.config)
            else:
                self.divergence = DivergenceEngine(
                    min_strength=float(self.config.get("div_strength_min", 2.5)),
                    min_price_bp=float(self.config.get("div_min_price_bp", 5.0)),
                    require_bias_for_hidden=bool(self.config.get("div_require_bias_hidden", True)),
                )
        except Exception as exc:
            log_silent_error(exc, 'config_parse_failure', self.symbol, 'apply_config:divergence')
            if not hasattr(self, "divergence"):
                self.divergence = DivergenceEngine()

        # Phase A detectors (delta, OBI, absorption, iceberg)
        try:
            if hasattr(self, "delta_detector") and self.delta_detector:
                # Update existing detector config if it supports it
                pass
            else:
                z_thr = self.config.get("delta_z_threshold")
                if z_thr is None:
                    # Fallback to SymbolSpecs
                    try:
                        z_thr = get_specs(self.symbol).delta_z
                    except Exception:
                        z_thr = 3.0

                self.delta_detector = DeltaSpikeDetector(
                    window=int(self.config.get("delta_window", 120)),
                    z_threshold=float(z_thr),
                    min_abs_volume=float(self.config.get("delta_abs_min", 0.0)),
                )
        except Exception as exc:
            log_silent_error(exc, 'config_parse_failure', self.symbol, 'apply_config:delta_detector')
            if not hasattr(self, "delta_detector"):
                self.delta_detector = DeltaSpikeDetector(window=120, z_threshold=3.0, min_abs_volume=0.0)

        try:
            if hasattr(self, "obi_detector") and self.obi_detector:
                pass
            else:
                self.obi_detector = OBIDetector(
                    depth=int(self.config.get("obi_depth", 5)),
                    threshold=float(self.config.get("obi_threshold", 0.4)),
                    hold_secs=float(self.config.get("obi_hold_secs", 2.0)),
                )
        except Exception as exc:
            log_silent_error(exc, 'config_parse_failure', self.symbol, 'apply_config:obi_detector')
            if not hasattr(self, "obi_detector"):
                self.obi_detector = OBIDetector(depth=5, threshold=0.4, hold_secs=2.0)

        try:
            if hasattr(self, "absorption_detector") and self.absorption_detector:
                pass
            else:
                self.absorption_detector = AbsorptionDetector(
                    price_tolerance=self.config.get("absorption_price_tolerance", 0.0001),
                    min_volume=self.config.get("absorption_min_volume", 10.0),
                    window_sec=self.config.get("absorption_window_sec", 5.0),
                )
        except Exception as exc:
            log_silent_error(exc, 'config_parse_failure', self.symbol, 'apply_config:absorption')
            if not hasattr(self, "absorption_detector"):
                self.absorption_detector = AbsorptionDetector(price_tolerance=0.0001, min_volume=10.0, window_sec=5.0)

        try:
            if hasattr(self, "iceberg_detector") and self.iceberg_detector:
                pass
            else:
                self.iceberg_detector = IcebergDetector(
                    min_refresh=self.config.get("iceberg_refresh", 2),
                    min_duration=self.config.get("iceberg_duration", 1.5),
                )
        except Exception as exc:
            log_silent_error(exc, 'config_parse_failure', self.symbol, 'apply_config:iceberg')
            if not hasattr(self, "iceberg_detector"):
                self.iceberg_detector = IcebergDetector(min_refresh=2, min_duration=1.5)

    async def ensure_specs_fresh(self, r) -> None:
        """
        Periodically reload symbol_specs from Redis to get calibrated params (e.g. SL offset).
        """
        now = int(time.time() * 1000)
        # Cache for 60s
        if now - getattr(self, "spec_update_ts_ms", 0) < 60_000:
            return

        try:
            key = f"symbol_specs:{self.symbol}"
            raw = await r.get(key)
            if raw:
                try:
                    self.calibrated_specs = json.loads(raw)
                except Exception as exc:
                    log_silent_error(exc, 'config_parse_failure', self.symbol, 'ensure_specs_fresh:json_load')
                    self.calibrated_specs = {}
            else:
                self.calibrated_specs = {}
            self.spec_update_ts_ms = now
        except Exception as exc:
            # fail-open, retry next time
            log_silent_error(exc, 'redis_read_failure', self.symbol, 'ensure_specs_fresh:outer')
            pass

    async def maybe_load_htf_zones(self, *, now_ts_ms: int, redis_client: aioredis.Redis) -> None:
        """
        Best-effort load zones:htf:v1:<symbol> into runtime cache.
        Called on bar_close or snapshot publish, throttled.
        """
        try:
            refresh_ms = int(self.config.get("htf_zones_cache_refresh_ms", 5000))
            if refresh_ms < 500:
                refresh_ms = 500
            last = int(getattr(self, "zones_last_load_ts_ms", 0) or 0)
            if last > 0 and now_ts_ms - last < refresh_ms:
                return
            key = str(self.config.get("htf_zones_key_prefix", "zones:htf:v1:")) + str(self.symbol)
            raw = await redis_client.get(key)
            if not raw:
                self.zones_pack = None
                self.zones_last_load_ts_ms = now_ts_ms
                return
            pack = ZonePack.from_json(raw)
            self.zones_pack = pack
            self.zones_last_load_ts_ms = now_ts_ms
        except Exception as exc:
            # fail-open: keep old cache
            log_silent_error(exc, 'calib_load_failure', self.symbol, 'maybe_load_htf_zones:load_pack')
            return

        try:
            if not hasattr(self, "rsi_price"):
                self.rsi_price = StreamingRSI(period=int(self.config.get("rsi_period", 14)))
            else:
                self.rsi_price.apply_config(self.config, key="rsi_period")
                
            if not hasattr(self, "rsi_cvd"):
                self.rsi_cvd = StreamingRSI(period=int(self.config.get("rsi_period", 14)))
            else:
                self.rsi_cvd.apply_config(self.config, key="rsi_period")
        except Exception as exc:
            log_silent_error(exc, 'config_parse_failure', self.symbol, 'maybe_load_htf_zones:rsi_config')
            if not hasattr(self, "rsi_price"): self.rsi_price = StreamingRSI()
            if not hasattr(self, "rsi_cvd"): self.rsi_cvd = StreamingRSI()

        # Phase C: liquidity pools + sweeps
        try:
            if hasattr(self, "eq_pools") and self.eq_pools:
                pass
            else:
                self.eq_pools = EQPoolTracker(
                    mature_bars=int(self.config.get("pool_mature_bars", 60)),
                    expiry_bars=int(self.config.get("pool_expiry_bars", 3600)),
                )
        except Exception as exc:
            log_silent_error(exc, 'init_failure', self.symbol, 'maybe_load_htf_zones:eq_pools_init')
            self.eq_pools = EQPoolTracker()


        try:
            if hasattr(self, "eq_pools"):
                self.eq_pools.apply_config(self.config)
            else:
                self.eq_pools = EQPoolTracker(
                    symbol=self.symbol,
                    eq_tol_bp=float(self.config.get("eq_tol_bp", 6.0)),
                    eq_tol_atr_mult=float(self.config.get("eq_tol_atr_mult", 0.08)),
                    eq_min_touches=int(self.config.get("eq_min_touches", 2)),
                    eq_ttl_ms=int(self.config.get("eq_ttl_ms", 3_600_000)),
                    eq_max_pools=int(self.config.get("eq_max_pools", 64)),
                )
        except Exception as exc:
            log_silent_error(exc, 'config_parse_failure', self.symbol, 'maybe_load_htf_zones:eq_pools_config')
            if not hasattr(self, "eq_pools"):
                self.eq_pools = EQPoolTracker(symbol=self.symbol)

        try:
            if hasattr(self, "sweep") and self.sweep:
                self.sweep.apply_config(self.config)
            else:
                self.sweep = SweepDetector(
                    confirm_bars=int(self.config.get("sweep_confirm_bars", 3)),
                    cooldown_ms=int(self.config.get("sweep_cooldown_ms", 60_000)),
                    valid_ms=int(self.config.get("sweep_valid_ms", 120_000)),
                )
        except Exception:
            if not hasattr(self, "sweep"):
                self.sweep = SweepDetector()

        # Phase F: Strong OF Gate detectors
        try:
            if hasattr(self, "reclaim") and self.reclaim:
                self.reclaim.apply_config(self.config)
            else:
                self.reclaim = ReclaimDetector(
                    hold_bars=int(self.config.get("reclaim_hold_bars", 2)),
                    valid_ms=int(self.config.get("reclaim_valid_ms", 120_000)),
                )
        except Exception:
            if not hasattr(self, "reclaim"):
                self.reclaim = ReclaimDetector()

        try:
            if hasattr(self, "fp_edge") and self.fp_edge:
                self.fp_edge.apply_config(self.config)
            else:
                self.fp_edge = FPEdgeAbsorbDetector(
                    window_bars=int(self.config.get("fp_edge_window_bars", 1800)),
                    refresh_every=int(self.config.get("fp_edge_refresh_every", 5)),
                )
        except Exception:
            if not hasattr(self, "fp_edge"):
                self.fp_edge = FPEdgeAbsorbDetector()

        # Phase F: Strong OF Gate detectors
        try:
            if hasattr(self, "reclaim") and self.reclaim:
                self.reclaim.apply_config(self.config)
            else:
                self.reclaim = ReclaimDetector(
                    hold_bars=int(self.config.get("reclaim_hold_bars", 2)),
                    valid_ms=int(self.config.get("reclaim_valid_ms", 120_000)),
                )
        except Exception:
            if not hasattr(self, "reclaim"):
                self.reclaim = ReclaimDetector()

        try:
            if hasattr(self, "fp_edge") and self.fp_edge:
                self.fp_edge.apply_config(self.config)
            else:
                self.fp_edge = FPEdgeAbsorbDetector(
                    window_bars=int(self.config.get("fp_edge_window_bars", 1800)),
                    refresh_every=int(self.config.get("fp_edge_refresh_every", 5)),
                )
        except Exception:
            if not hasattr(self, "fp_edge"):
                self.fp_edge = FPEdgeAbsorbDetector()


# ──────────────────────────────────────────────────────────────────────────────
# Основной сервис
# ──────────────────────────────────────────────────────────────────────────────
