# data_processor.py
from __future__ import annotations
"""
Data processing functionality extracted from base_orderflow_handler.py
"""

from utils.time_utils import get_ny_time_millis

from typing import Optional, Dict, Any, Tuple, List, Deque, Union
from collections import deque
import time
import os
import math
import logging
from dataclasses import fields as dc_fields, is_dataclass

class CVDTracker:
    """
    Cumulative Volume Delta tracker with sliding window.
    """
    def __init__(self, window_sec: int = 300):
        self.window_sec = window_sec
        self._events: Deque[Tuple[float, float]] = deque()
        self.cvd = 0.0
        self.divergence = 0.0

    def update(self, ts_ms: int, delta: float, atr: float) -> float:
        now = ts_ms / 1000.0
        self._events.append((now, delta))
        self.cvd += delta
        
        while self._events and now - self._events[0][0] > self.window_sec:
            _, old_delta = self._events.popleft()
            self.cvd -= old_delta
            
        self.divergence = self.cvd / atr if atr > 0 else 0.0
        return self.cvd

# Imports from contexts module


def normalize_pivots_input(pivots: Union[None, Dict[str, float], Dict[str, Any]]) -> Tuple[Dict[str, float], int, str]:
    """
    Normalize pivots input to (pivots_dict, pivots_ts_ms, pivots_date).
    Supports:
      A) bundle: {"ts_ms":..., "date":..., "pivots": {k:v}}
      B) raw dict: {k:v}
    Fail-open: returns empty dict and (0,"") on errors.
    """
    try:
        raw_pivots: Optional[Dict[str, Any]] = None
        pivots_ts_ms = 0
        pivots_date = ""

        if isinstance(pivots, dict) and "pivots" in pivots and isinstance(pivots.get("pivots"), dict):
            raw_pivots = pivots.get("pivots")  # type: ignore[assignment]
            try:
                pivots_ts_ms = int(pivots.get("ts_ms") or 0)
            except Exception:
                pivots_ts_ms = 0
            try:
                pivots_date = str(pivots.get("date") or "")
            except Exception:
                pivots_date = ""
        elif isinstance(pivots, dict):
            raw_pivots = pivots

        out: Dict[str, float] = {}
        if raw_pivots:
            for k, v in raw_pivots.items():
                try:
                    fv = float(v)
                except Exception:
                    continue
                # Keep only sane positive levels
                if fv > 0.0:
                    out[str(k)] = fv
        return out, int(pivots_ts_ms), str(pivots_date)
    except Exception:
        return {}, 0, ""


def nearest_pivot(price: float, pivots_dict: Dict[str, float]) -> Tuple[str, float]:
    """
    Return (nearest_key, nearest_price). If not found -> ("", 0.0).
    Deterministic tie-break: first encountered with minimal distance (dict order in py3.7+ stable).
    """
    try:
        if not pivots_dict or price <= 0.0:
            return "", 0.0
        best_k = ""
        best_v = 0.0
        best_d = 1e100
        for k, v in pivots_dict.items():
            d = abs(price - float(v))
            if d < best_d:
                best_d = d
                best_k = str(k)
                best_v = float(v)
        return best_k, float(best_v)
    except Exception:
        return "", 0.0

# Imports from contexts module

from .data_parser import OrderFlowDataParser
try:
    # For running as part of the package
    from ..contexts import BucketState, L2Level, Tick, SimpleL2Snapshot, OrderflowSignalContext, OrderflowSignalThresholds
    from ..l2_microstructure_engine import L2MicrostructureEngine
    from ..regime_engine import BarBuilder1m, RegimeEngine
    from ..handlers.regime_service import MarketRegimeService, RegimeConfig, RegimeFeatures
    from ..core.regime_quantiles_store import RegimeQuantilesStore, approx_quantile_3pt
    from ..core.regime_quantiles_redis import parse_rq
except ImportError:
    # For direct execution/testing
    from contexts import BucketState, Tick, OrderflowSignalContext, OrderflowSignalThresholds
    from l2_microstructure_engine import L2MicrostructureEngine
    from regime_engine import BarBuilder1m, RegimeEngine
    from handlers.regime_service import MarketRegimeService, RegimeConfig, RegimeFeatures
    from core.regime_quantiles_store import RegimeQuantilesStore, approx_quantile_3pt
    from core.regime_quantiles_redis import parse_rq
    from core.regime_quantiles_redis import parse_rq
    from core.robust_stats import RollingRobustZ

try:
    from .atr_redis_publisher import AtrRedisPublisher
except Exception:
    AtrRedisPublisher = None  # type: ignore

try:
    from signals.atr import ATR
except Exception:
    ATR = None  # type: ignore


def _filter_dataclass_kwargs(cls: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Предотвращение сбоев runtime при различии версий dataclass.
    Fail-open (без TypeError в __init__):
      - неизвестные ключи НЕ передаём в dataclass (чтобы не падать),
      - но если dataclass поддерживает поле `extra: dict`, то сохраняем туда все "лишние" ключи,
      - аккуратно мержим уже переданный kwargs["extra"] (если это dict),
      - нормализуем data_quality_flags (None/iterable -> list) если поле существует.
    """
    try:
        if not is_dataclass(cls):
            return dict(kwargs)

        field_names = {f.name for f in dc_fields(cls)}
        out: Dict[str, Any] = {}

        has_extra = "extra" in field_names
        extra: Dict[str, Any] = {}

        # Seed extra with already-provided extra, if supported.
        if has_extra:
            cur_extra = kwargs.get("extra")
            if isinstance(cur_extra, dict):
                extra.update(cur_extra)
            elif cur_extra is not None:
                # Не теряем неожиданный тип (иногда прилетает SimpleNamespace/строка).
                extra["_extra_raw"] = cur_extra

        # Split known fields vs unknown fields.
        for k, v in kwargs.items():
            if k == "extra":
                continue
            if k in field_names:
                out[k] = v
            else:
                if has_extra:
                    extra[k] = v

        # Always attach extra if the dataclass supports it (even if empty).
        if has_extra:
            out["extra"] = extra

        # Normalize dq flags to list if supported by dataclass.
        if "data_quality_flags" in field_names and "data_quality_flags" in out:
            dq = out.get("data_quality_flags")
            if dq is None:
                out["data_quality_flags"] = []
            elif not isinstance(dq, list):
                try:
                    out["data_quality_flags"] = list(dq)  # type: ignore[arg-type]
                except Exception:
                    out["data_quality_flags"] = [str(dq)]

        return out
    except Exception:
        # Worst-case fail-open: вернуть исходные kwargs как раньше.
        return dict(kwargs)


def _build_regime_config_safe(cfg_cls: Any, raw: Dict[str, Any]) -> Any:
    """
    Создание RegimeConfig только с полями, существующими в текущей версии.
    Это позволяет избежать ошибок при несовпадении версий конфигурации.
    """
    try:
        if is_dataclass(cfg_cls):
            allowed = {f.name for f in dc_fields(cfg_cls)}
            filtered = {k: v for k, v in raw.items() if k in allowed}
            return cfg_cls(**filtered)
    except Exception:
        pass
    return cfg_cls()  # fallback


class OrderFlowDataProcessor:
    """
    Процессор данных orderflow (тики, стаканы, бакеты).
    """

    def __init__(
        self,
        symbol: str,
        specs: Any,
        config: Any,
        atr_publisher: Any = None,
        atr_calculator: Any = None,
        health_metrics: Any = None,
        l3_queue: Any = None,
        l2_gpu_processor: Any = None,
    ):
        self.symbol = symbol
        self.specs = specs
        self.config = config
        self.logger = logging.getLogger(f"OrderFlowDataProcessor:{symbol}")
        
        # GPU Processor
        self.l2_gpu_processor = l2_gpu_processor

        # Bucket management
        self.delta_window: Deque[float] = deque(maxlen=config.delta_window_ticks)
        self._bucket_id: Optional[int] = None
        self._bucket_sum = 0.0
        self._last_bucket_value = 0.0
        self._last_z_delta = 0.0

        # Bucket state
        self._bucket_state = BucketState.empty()

        # сигнал-триггер по "bucket closed" (для более частых сигналов, чем 1m)
        self._signal_bucket_ms = int(os.getenv("SIGNAL_BUCKET_MS", "1000"))  # 1s по умолчанию
        self._signal_bucket_id: Optional[int] = None

        # Parser
        self.parser = OrderFlowDataParser(symbol, specs)

        # ATR (injected from BaseOrderFlowHandler)
        self.atr_publisher = atr_publisher
        self.atr_calculator = atr_calculator
        # health_metrics: best-effort, не должен ломать hot-path
        self.health_metrics = health_metrics

        # L2 Microstructure Engine
        self.l2_engine = L2MicrostructureEngine(config, specs, gpu_processor=self.l2_gpu_processor)

        # L3-lite proxy
        self.l3_queue = l3_queue

        # Regime engine
        self._regime = RegimeEngine(config)

        # CVD Tracker (5 min window)
        self.cvd_tracker = CVDTracker(window_sec=300)

        # Bar builder for 1m bars
        self._bar_builder_1m = BarBuilder1m()

        # ------------------------------------------------------------
        # Bar range tracker (tick-driven, fail-open)
        # ------------------------------------------------------------
        # Default TF for bar-range is config.timeframe_s (fallback 60s).
        # You can override with:
        #   BAR_RANGE_TF_MS=60000  (highest priority)
        #   BAR_RANGE_TF_S=60
        try:
            tf_s = int(getattr(config, "timeframe_s", 60) or 60)
        except Exception:
            tf_s = 60
        tf_ms = tf_s * 1000
        try:
            env_tf_ms = int(os.getenv("BAR_RANGE_TF_MS", "0") or "0")
        except Exception:
            env_tf_ms = 0
        if env_tf_ms > 0:
            tf_ms = env_tf_ms
        else:
            try:
                env_tf_s = int(os.getenv("BAR_RANGE_TF_S", "0") or "0")
            except Exception:
                env_tf_s = 0
            if env_tf_s > 0:
                tf_ms = env_tf_s * 1000
        self._bar_tf_ms = int(max(tf_ms, 1000))  # hard floor 1s

        self._bar_id: Optional[int] = None
        self._bar_open: float = 0.0
        self._bar_high: float = 0.0
        self._bar_low: float = 0.0
        self._bar_last_ts_ms: int = 0

        # EMA for range (bps) - used as smooth baseline for “spike”-like features
        try:
            self._bar_range_alpha = float(getattr(config, "bar_range_ema_alpha", 0.05) or 0.05)
        except Exception:
            self._bar_range_alpha = 0.05
        self._bar_range_alpha = float(min(max(self._bar_range_alpha, 0.001), 0.5))
        self._bar_range_bps_ema: float = 0.0

        # Robust baseline (median/MAD) over last N closed bars (range bps)
        try:
            hist_len = int(getattr(config, "bar_range_hist_len", 120) or 120)
        except Exception:
            hist_len = 120
        try:
            env_hist = int(os.getenv("BAR_RANGE_HIST_LEN", "0") or "0")
        except Exception:
            env_hist = 0
        if env_hist > 0:
            hist_len = env_hist
        self._bar_range_stats = RollingRobustZ(window=int(max(hist_len, 10)))


        # Market Regime Service (optional; keep system robust across config variants)
        _rc_raw = {
            "score_hi": float(getattr(config, "regime_label_hi", 0.35)),
            "score_lo": float(getattr(config, "regime_label_lo", -0.35)),
            "atr_q_hi": float(getattr(config, "regime_atr_hi_q", 0.70)),
            "atr_q_lo": float(getattr(config, "regime_atr_lo_q", 0.35)),
            "adx_q_hi": float(getattr(config, "regime_adx_hi_q", 0.75)),
            "adx_q_lo": float(getattr(config, "regime_adx_lo_q", 0.40)),
            "ping_scale": float(getattr(config, "regime_ping_scale", 0.20)),
            "delta_scale": float(getattr(config, "regime_delta_scale", 1.0)),
            "w_atr": float(getattr(config, "regime_w_atr", 0.35)),
            "w_adx": float(getattr(config, "regime_w_adx", 0.20)),
            "w_delta": float(getattr(config, "regime_w_delta", 0.25)),
            "w_hold": float(getattr(config, "regime_w_hold", 0.25)),
            "w_ping": float(getattr(config, "regime_w_ping", 0.15)),
            "trend_dir_hold_min": float(getattr(config, "regime_trend_dir_hold_min", 0.10)),
            # placeholder RegimeConfig compatibility (if present)
            "window_bars": int(getattr(config, "regime_window_bars", 20)),
            "trend_threshold": float(getattr(config, "regime_trend_threshold", 0.7)),
            "range_threshold": float(getattr(config, "regime_range_threshold", 0.3)),
            "mixed_threshold": float(getattr(config, "regime_mixed_threshold", 0.5)),
        }
        self.regime_service = MarketRegimeService(_build_regime_config_safe(RegimeConfig, _rc_raw))

        # Minimal online feature builder state (tick-driven)
        self._regime_pv = 0.0
        self._regime_vol = 0.0
        self._regime_vwap = 0.0
        self._regime_open_day = 0.0
        self._regime_day_id = None

        self._regime_delta_ema = 0.0
        self._regime_delta_alpha = float(getattr(config, "regime_delta_ema_alpha", 0.05))

        self._regime_last_side = 0
        self._regime_cross_hist: Deque[int] = deque(maxlen=int(getattr(config, "regime_cross_hist", 30)))
        self._regime_hold_ema = 0.0
        self._regime_hold_alpha = float(getattr(config, "regime_hold_ema_alpha", 0.10))
        self._regime_atr_q = 0.5  # fallback, will be computed from quantiles

        # Regime quantiles store (optional, fail-open)
        try:
            self._regime_quantiles_store = RegimeQuantilesStore(refresh_ms=300_000)
        except Exception:
            self._regime_quantiles_store = None

        # Regime quantiles cache (Redis -> in-memory)
        self._rq_cache: dict = {}
        self._rq_last_fetch_ms: dict = {}
        self._rq_tf = str(getattr(config, "regime_q_timeframe", "1m") or "1m")
        self._rq_fetch_gap_ms = int(getattr(config, "regime_q_fetch_gap_ms", 60_000) or 60_000)
        self._rq_min_samples = int(getattr(config, "regime_q_min_samples", 300) or 300)

        # Redis regime publishing (optional)
        self._regime_redis_ttl_sec = int(getattr(config, "regime_redis_ttl_sec", 30) or 30)
        self._regime_last_pub_ms: int = 0
        self._regime_pub_gap_ms = int(getattr(config, "regime_redis_pub_gap_ms", 500) or 500)

        # Robust stats for P1 features
        # Spread (tick-based) - window ~300 ticks
        self._spread_stats = RollingRobustZ(window=300)
        
        # Churn (book-based) - window ~300 updates
        self._churn_stats = RollingRobustZ(window=300)
        self._last_book_ts = 0
        
        # OFI (bucket-based, or trade-based) - window ~100 buckets
        self._ofi_stats = RollingRobustZ(window=100)

    def get_obi_metrics(self) -> Dict[str, Any]:
        """
        Единый источник истины для OBI метрик: чтение из BucketState.
        Заменяет устаревший контракт _get_obi(), использовавшийся в хендлерах.
        """
        st = self._bucket_state
        return {
            "obi": float(getattr(st, "obi", 0.0) or 0.0),
            "obi_avg": float(getattr(st, "obi_avg", 0.0) or 0.0),
            "obi_sustained": bool(getattr(st, "obi_sustained", False)),

            "obi20": float(getattr(st, "obi_20", 0.0) or 0.0),
            "obi20_avg": float(getattr(st, "obi_avg_20", 0.0) or 0.0),
            "obi20_sustained": bool(getattr(st, "obi_sustained_20", False)),
            "obi20_valid": bool(getattr(st, "obi_20_valid", False)),
        }

    def _get_rq(self, symbol: str, now_ms: int) -> Optional[Any]:
        """
        Read quantiles from Redis key regime:q:{symbol}:{tf} with local caching.
        Throttled fetch (default: once per 60s) to minimize Redis calls.
        Fail-open: return None if unavailable.
        """
        sym = str(symbol or "").upper()
        if not sym:
            return None
        
        # Check cache freshness
        last = int(self._rq_last_fetch_ms.get(sym, 0) or 0)
        if now_ms - last < self._rq_fetch_gap_ms:
            return self._rq_cache.get(sym)
        
        self._rq_last_fetch_ms[sym] = now_ms
        
        try:
            # Read from Redis (requires parser.redis to be available)
            if not hasattr(self, 'parser') or not hasattr(self.parser, 'redis'):
                return self._rq_cache.get(sym)
            
            redis_client = getattr(self.parser, 'redis', None)
            if not redis_client:
                return self._rq_cache.get(sym)
            
            raw = redis_client.get(f"regime:q:{sym}:{self._rq_tf}")
            if not raw:
                return self._rq_cache.get(sym)
            
            rq = parse_rq(raw)
            if rq is None or int(getattr(rq, "sample_size", 0) or 0) < self._rq_min_samples:
                return self._rq_cache.get(sym)
            
            self._rq_cache[sym] = rq
            return rq
        except Exception:
            return self._rq_cache.get(sym)

    def _extract_top1(self, x: Any) -> Tuple[float, float]:
        """Извлечение топового уровня из данных стакана."""
        if isinstance(x, dict):
            return float(x.get("price", 0.0)), float(x.get("size", 0.0))
        elif isinstance(x, (list, tuple)) and len(x) >= 2:
            return float(x[0]), float(x[1])
        return 0.0, 0.0

    def _extract_top_levels(self, book_data: Dict[str, Any], side: str, n: int = 3) -> List[Tuple[float, float]]:
        """Извлечение топ-N уровней из данных стакана."""
        levels_data = book_data.get(side, [])
        if not levels_data:
            return []

        result = []
        for i, level in enumerate(levels_data[:n]):
            price, size = self._extract_top1(level)
            if price > 0 and size > 0:
                result.append((price, size))

        return result

    def _classify_delta(self, tick: Tick) -> float:
        """Классификация дельты тика по направлению сделки."""
        if getattr(tick, 'is_buyer_maker', None) is None:
            return 0.0
        # buyer is maker, значит taker SELL -> дельта отрицательная
        sign = -1.0 if tick.is_buyer_maker else 1.0
        vol = float(getattr(tick, "volume", 1.0) or 1.0)
        return sign * vol

    def _taker_side(self, tick: Tick) -> int:
        """Определение стороны тейкера из тика."""
        if getattr(tick, 'is_buyer_maker', None) is None:
            return 0
        # buyer is maker, значит taker SELL -> side = -1
        return -1 if tick.is_buyer_maker else 1

    def _feed_delta_bucket(self, delta: float, ts: int) -> Optional[int]:
        """Добавление дельты в бакет и возврат ID бакета, если он завершен."""
        self._bucket_sum += delta
        self.delta_window.append(delta)

        st = self._bucket_state
        # st.current_delta += delta  # REMOVED: Doubled increment (already done in update_from_tick_inplace)

        bucket_ms = int(getattr(self.config, "delta_bucket_ms", 1000) or 1000)
        if bucket_ms <= 0:
            return None

        if self._bucket_id is None:
            self._bucket_id = ts // bucket_ms

        cur = ts // bucket_ms
        if cur != self._bucket_id:
            old = self._bucket_id
            bucket_value = float(self._bucket_sum)

            # синхронизация в BucketState (single source of truth)
            st.delta_bucket = bucket_value
            st.current_delta = 0.0  # сброс на новый бакет

            # L2 totals for L3-lite reconciliation
            if self.l3_queue:
                l3_stats = self.l3_queue.on_bucket_advance(bucket_id=old)
                if l3_stats:
                    st.taker_buy_qty_bucket = float(l3_stats.taker_buy_qty)
                    st.taker_sell_qty_bucket = float(l3_stats.taker_sell_qty)
                    st.taker_buy_rate_ema = float(l3_stats.taker_buy_rate_ema)
                    st.taker_sell_rate_ema = float(l3_stats.taker_sell_rate_ema)
                    st.cancel_bid_rate_ema = float(l3_stats.cancel_bid_rate_ema)
                    st.cancel_ask_rate_ema = float(l3_stats.cancel_ask_rate_ema)
                    
                    # P1: Update OFI
                    self._update_ofi(l3_stats)

            self._bucket_id = cur
            self._bucket_sum = 0.0
            self._last_bucket_value = bucket_value
            return int(old)

        return None

    def _get_obi(self, ts: int) -> Tuple[float, float, bool, float, float, bool]:
        """
        Legacy API: вернуть OBI-метрики из BucketState (единый источник истины).
        Формат сохранён: (obi, obi_avg, obi_sustained, obi_avg_20, obi_sustained_20, invalid_flag)
        """
        st = self._bucket_state
        obi = float(getattr(st, "obi", 0.0) or 0.0)
        obi_avg = float(getattr(st, "obi_avg", 0.0) or 0.0)
        obi_sust = bool(getattr(st, "obi_sustained", False))
        obi_avg_20 = float(getattr(st, "obi_avg_20", 0.0) or 0.0)
        obi_sust_20 = bool(getattr(st, "obi_sustained_20", False))
        invalid = not bool(getattr(st, "obi_20_valid", False))
        return obi, obi_avg, obi_sust, obi_avg_20, obi_sust_20, invalid

    def _update_ofi(self, bucket_stats: Any) -> None:
        """Update OFI stats from a closed bucket/L3 stats."""
        if not hasattr(self, "_ofi_stats"):
            return
        
        # OFI = TakerBuy - TakerSell (Flow Imbalance)
        # We can use the bucket values or the EMA rates. 
        # Using raw volume imbalance per bucket seems more standard for OFI.
        buy_qty = float(getattr(bucket_stats, "taker_buy_qty", 0.0))
        sell_qty = float(getattr(bucket_stats, "taker_sell_qty", 0.0))
        
        ofi = buy_qty - sell_qty
        self._ofi_stats.update(ofi)
        
        z = self._ofi_stats.z(ofi)
        
        # Store in state (persists until next bucket update)
        st = self._bucket_state
        st.ofi_val = ofi
        st.ofi_z = float(z)


    def _update_bar_range(self, price: float, ts: int) -> None:
        """Обновление отслеживания диапазона бара."""
        # Tick-safe, fail-open bar-range tracker:
        # - tracks open/high/low for current bar (tf = self._bar_tf_ms)
        # - computes range in abs and bps
        # - maintains EMA baseline (bps)
        # - maintains robust z-score baseline (median/MAD over closed bars)
        try:
            # Basic guards
            p = float(price or 0.0)
            if p <= 0.0 or (not math.isfinite(p)):
                return
            t = int(ts or 0)
            if t <= 0:
                return

            st = self._bucket_state
            tf_ms = int(getattr(self, "_bar_tf_ms", 60_000) or 60_000)
            if tf_ms <= 0:
                tf_ms = 60_000

            # bad-time protection: do not rewind bars on older timestamps
            if self._bar_last_ts_ms > 0 and t + 500 < self._bar_last_ts_ms:
                # Out-of-order tick (older than last by > 500ms). Ignore for bar stats.
                st.bar_time_backwards_cnt += 1
                st.bar_time_backwards_ms = int(self._bar_last_ts_ms - t)
                st.bar_time_backwards_flag = True
                return
            self._bar_last_ts_ms = t

            bar_id = int(t // tf_ms)

            # init
            if self._bar_id is None:
                self._bar_id = bar_id
                self._bar_open = p
                self._bar_high = p
                self._bar_low = p
            else:
                # If we jumped forward to a new bar: finalize previous bar into history
                if bar_id != int(self._bar_id):
                    # forward gap detection
                    gap = int(bar_id - int(self._bar_id))
                    if gap > 1:
                        st.bar_gap_bars = gap - 1
                        st.bar_gap_flag = True

                    # finalize previous bar stats (closed bar)
                    prev_open = float(self._bar_open or 0.0)
                    prev_high = float(self._bar_high or 0.0)
                    prev_low = float(self._bar_low or 0.0)
                    prev_range = float(max(prev_high - prev_low, 0.0))
                    prev_bps = float((prev_range / prev_open) * 1e4) if prev_open > 0.0 else 0.0
                    if math.isfinite(prev_bps) and prev_bps >= 0.0:
                        self._bar_range_stats.update(prev_bps)

                    # expose previous bar info
                    st.prev_bar_open = prev_open
                    st.prev_bar_high = prev_high
                    st.prev_bar_low = prev_low
                    st.prev_bar_range = prev_range
                    st.prev_bar_range_bps = prev_bps

                    # reset new bar with current tick price
                    self._bar_id = bar_id
                    self._bar_open = p
                    self._bar_high = p
                    self._bar_low = p
                else:
                    # same bar: update H/L
                    if p > self._bar_high:
                        self._bar_high = p
                    if p < self._bar_low:
                        self._bar_low = p

            # current bar stats
            o = float(self._bar_open or p)
            h = float(self._bar_high or p)
            l = float(self._bar_low or p)
            rng = float(max(h - l, 0.0))
            rng_bps = float((rng / o) * 1e4) if o > 0.0 else 0.0
            if (not math.isfinite(rng_bps)) or rng_bps < 0.0:
                rng_bps = 0.0

            # EMA baseline
            a = float(getattr(self, "_bar_range_alpha", 0.05) or 0.05)
            if self._bar_range_bps_ema <= 0.0:
                self._bar_range_bps_ema = rng_bps
            else:
                self._bar_range_bps_ema = a * rng_bps + (1.0 - a) * self._bar_range_bps_ema

            # robust z-score vs history of closed bars (range bps)
            z = self._bar_range_stats.z(rng_bps)

            # publish into BucketState
            st.bar_id = int(self._bar_id or bar_id)
            st.bar_open = o
            st.bar_high = h
            st.bar_low = l
            st.bar_range = rng
            st.bar_range_bps = rng_bps
            st.bar_range_bps_ema = float(self._bar_range_bps_ema)
            st.bar_range_z = float(z)
        except Exception:
            # fail-open: never break tick processing
            return

    def _emit_health_metrics_best_effort(self) -> None:
        """
        Публикация health метрик на каждом тике.
        Вызывается после _update_l2_tick_staleness().

        Цель:
          - заполнить orderflow:{symbol}:health_snapshot полноценными L2-age/staleness метриками
          - без блокировок критического пути: try/except, минимум вычислений
        """
        hm = getattr(self, "health_metrics", None)
        if not hm:
            return
        try:
            st = self._bucket_state
            l2_age_ms = float(getattr(st, "l2_age_ms", 0.0) or 0.0)
            # Некоторые реализации BucketState имеют l2_age_ms_tick, если нет — используем l2_age_ms
            l2_age_ms_tick = float(getattr(st, "l2_age_ms_tick", l2_age_ms) or l2_age_ms)
            l2_is_stale = bool(getattr(st, "l2_is_stale", True))
            l2_is_stale_now = bool(getattr(st, "l2_is_stale_now", l2_is_stale))
            hm.on_tick(
                symbol=self.symbol,
                l2_age_ms=l2_age_ms,
                l2_age_ms_tick=l2_age_ms_tick,
                l2_is_stale=l2_is_stale,
                l2_is_stale_now=l2_is_stale_now,
                eta_fill_ms=None,
                burst_ratio=None,
                imbalance_min=None,
            )
        except Exception:
            # Не ломаем обработку тика ни при каких обстоятельствах
            return

    def _normalize_ts(self, ts: Any) -> int:
        """
        Нормализация времени в epoch ms.
        Строгая проверка на epoch (с 2001 года).
        """
        now_ms = get_ny_time_millis()
        try:
            val = int(ts)
            # epoch seconds -> ms
            if 1_000_000_000 <= val < 100_000_000_000:
                val *= 1000

            # hard epoch-ms plausibility window (>= 2001-09-09)
            if val < 1_000_000_000_000:
                return 0
            # future sanity check (+7 days max)
            if val > now_ms + 7 * 86_400_000:
                return 0

            return val
        except Exception:
            return 0

    def _process_tick(self, tick: Tick) -> Tuple[Optional[object], Optional[int]]:
        """Обработка входящего тика."""
        st = self._bucket_state
        
        # 1. Normalize TS
        now_ms = get_ny_time_millis()
        ts_ms = self._normalize_ts(tick.ts) or now_ms
        
        # Вычисление знаковой дельты ОДИН РАЗ и передача её в обновление BucketState через lambda
        delta = self._classify_delta(tick)
        st.update_from_tick_inplace(tick, ts_ms, delta_classifier=lambda _t: delta)
        
        # Обновление CVD
        atr_val = float(getattr(st, 'atr_14_raw', 0.0) or getattr(st, 'atr', 1.0))
        if atr_val <= 0:
            atr_val = 1.0
        self.cvd_tracker.update(ts_ms, delta, atr_val)
        st.cvd_5m = float(self.cvd_tracker.cvd)
        st.cvd_divergence = float(self.cvd_tracker.divergence)

        # Передача в бакет
        completed_bucket = self._feed_delta_bucket(delta, ts_ms)

        # Feed L3-lite proxy with trade data
        if self.l3_queue:
             self.l3_queue.on_trade(side=self._taker_side(tick), qty=tick.volume)

        # --- bucket closed detector (по времени тика, в ms) ---
        closed_bucket_ts_ms: Optional[int] = None
        try:
            if ts_ms > 0 and self._signal_bucket_ms > 0:
                bid = ts_ms // self._signal_bucket_ms
                if self._signal_bucket_id is None:
                    self._signal_bucket_id = int(bid)
                elif int(bid) != int(self._signal_bucket_id):
                    # закрыли предыдущий bucket; ts_end = start(next_bucket)
                    closed_bucket_ts_ms = int(bid) * int(self._signal_bucket_ms)
                    self._signal_bucket_id = int(bid)
        except Exception:
            # bucket — best-effort, не валим обработку тиков
            closed_bucket_ts_ms = None

        # Обновление устаревания L2 на каждом тике (важно для build_signal_ctx)
        self._update_l2_tick_staleness(ts_ms)

        # REMOVED: self._emit_health_metrics_best_effort() 
        # (avoiding double emit, Handler handles it)

        # Обновление диапазона бара
        self._update_bar_range(tick.last, ts_ms)
        
        # P1: Update spread stats (tick-based)
        if hasattr(self, "_spread_stats"):
             # We need valid spread_bps from bucket state. 
             # Note: st.spread_bps is updated in st.update_from_tick_inplace if we parse bid/ask there, 
             # BUT standard Ticket doesn't always have bid/ask. 
             # Let's rely on st.best_bid/best_ask if available.
             
             # Calculate spread_bps from current state
             bb = float(getattr(st, "best_bid", 0.0) or 0.0)
             ba = float(getattr(st, "best_ask", 0.0) or 0.0)
             if bb > 0 and ba > 0 and ba > bb:
                 mid = 0.5 * (bb + ba)
                 sbps = ((ba - bb) / mid) * 10000.0
                 self._spread_stats.update(sbps)
                 
                 # Store stats in BucketState
                 med, _, _ = self._spread_stats.median_mad()
                 z = self._spread_stats.z(sbps)
                 st.spread_bps = sbps
                 st.spread_bps_mean = float(med)
                 st.spread_bps_z = float(z)


        # Обновление regime engine данными тика
        price = float(getattr(tick, "last", 0.0) or 0.0)
        volume = float(getattr(tick, "volume", 0.0) or 0.0)
        if price > 0.0:
            self._regime.on_tick(ts_ms, price, volume, delta)

        # Проверка завершенных 1m баров и обновление ATR
        finished_bar = self._bar_builder_1m.update_tick(ts_ms, price, volume, delta)
        if finished_bar is not None:
            self._regime.on_bar_1m(finished_bar.ts_open, finished_bar.high, finished_bar.low, finished_bar.close)

            # Публикация ATR для таймфрейма 1m (hash + legacy)
            if self.atr_publisher and self.atr_calculator:
                try:
                    atr_value = self.atr_calculator.update(finished_bar.high, finished_bar.low, finished_bar.close)
                    if atr_value is not None and float(atr_value) > 0:
                        # Предпочтительно ts_close, fallback на ts_open + 60s, затем текущее время
                        ts_close = int(
                            getattr(finished_bar, "ts_close", 0)
                            or (int(getattr(finished_bar, "ts_open", 0)) + 60_000)
                            or get_ny_time_millis()
                        )
                        self.atr_publisher.publish("1m", float(atr_value), ts_close)
                except Exception as e:
                    self.logger.warning("Failed to publish ATR for %s: %s", self.symbol, e)

            # ВАЖНО: на тике закрытия 1m бара bucket тоже мог "закрыться".
            # Чтобы не генерить сигнал дважды — подавляем bucket event.
            closed_bucket_ts_ms = None

        # RegimeEngine предоставляет базовый score/label (всегда доступен в этом пайплайне)
        rs = self._regime.compute(ts_ms, price)
        base_score = float(getattr(rs, "score", 0.0) or 0.0)
        base_label = str(getattr(rs, "label", "mixed") or "mixed")

        # --- Обновление фич Market Regime Service (тик) ---
        ts = ts_ms # use normalized
        if ts > 0 and price > 0.0:
            # сброс дня
            day_id = ts // 86400000
            if self._regime_day_id is None or day_id != self._regime_day_id:
                self._regime_day_id = day_id
                self._regime_open_day = price
                self._regime_pv = 0.0
                self._regime_vol = 0.0
                self._regime_vwap = price
                self._regime_cross_hist.clear()
                self._regime_last_side = 0
                self._regime_hold_ema = 0.0

            if volume > 0.0:
                self._regime_pv += price * volume
                self._regime_vol += volume
                self._regime_vwap = (self._regime_pv / self._regime_vol) if self._regime_vol > 0.0 else price

            # EMA потока дельты
            a = self._regime_delta_alpha
            self._regime_delta_ema = a * float(delta) + (1.0 - a) * self._regime_delta_ema

            # сторона относительно VWAP
            side = 0
            if price > self._regime_vwap:
                side = 1
            elif price < self._regime_vwap:
                side = -1

            crossed = 1 if (self._regime_last_side != 0 and side != 0 and side != self._regime_last_side) else 0
            self._regime_cross_hist.append(crossed)
            if side != 0:
                self._regime_last_side = side

            # EMA удержания
            ha = self._regime_hold_alpha
            self._regime_hold_ema = ha * float(side) + (1.0 - ha) * self._regime_hold_ema

            cross_rate = (sum(self._regime_cross_hist) / max(len(self._regime_cross_hist), 1)) if self._regime_cross_hist else 0.0

            # --- ATR quantile from regime_quantiles (Redis cached) ---
            atr_q = float(self._regime_atr_q)  # fallback
            try:
                # Get current timestamp
                now_ms = int(ts_ms if ts_ms > 0 else get_ny_time_millis())
                
                # Fetch quantiles from Redis (cached, throttled)
                rq = self._get_rq(str(self.symbol), now_ms)
                
                if rq is not None:
                    # Compute ATR% ratio: atr_value / price
                    atr_val = 0.0
                    if self.atr_calculator and hasattr(self.atr_calculator, 'value'):
                        atr_val = float(getattr(self.atr_calculator, 'value', 0.0) or 0.0)
                    elif hasattr(st, 'atr'):
                        atr_val = float(getattr(st, 'atr', 0.0) or 0.0)
                    
                    atrp = (atr_val / price) if (price > 0 and atr_val > 0) else 0.0
                    
                    if atrp > 0:
                        q25 = float(getattr(rq, "atrp_p25", 0.0) or 0.0)
                        q50 = float(getattr(rq, "atrp_p50", 0.0) or 0.0)
                        q75 = float(getattr(rq, "atrp_p75", 0.0) or 0.0)
                        atr_q = approx_quantile_3pt(float(atrp), q25, q50, q75)
            except Exception:
                pass  # fail-open: use fallback

            # --- ADX quantile from regime_quantiles (Redis cached) ---
            adx_q = 0.5  # fallback
            try:
                # Get ADX value from Redis adx:{symbol}
                if rq is not None:
                    adx_val = 0.0
                    if hasattr(self, 'parser') and hasattr(self.parser, 'redis'):
                        redis_client = getattr(self.parser, 'redis', None)
                        if redis_client:
                            adx_raw = redis_client.get(f"adx:{str(self.symbol).upper()}")
                            if adx_raw:
                                try:
                                    adx_val = float(adx_raw)
                                except Exception:
                                    pass
                    
                    if adx_val > 0:
                        # Compute ADX quantile using 3-point approximation
                        q40 = float(getattr(rq, "adx_p40", 0.0) or 0.0)
                        q60 = float(getattr(rq, "adx_p60", 0.0) or 0.0)
                        q75 = float(getattr(rq, "adx_p75", 0.0) or 0.0)
                        if q40 > 0 and q60 > 0 and q75 > 0:
                            # Use 3-point approximation (p40, p60, p75)
                            if adx_val <= q40:
                                adx_q = 0.40 * (adx_val / q40) if q40 > 0 else 0.0
                            elif adx_val <= q60:
                                adx_q = 0.40 + 0.20 * ((adx_val - q40) / (q60 - q40))
                            elif adx_val <= q75:
                                adx_q = 0.60 + 0.15 * ((adx_val - q60) / (q75 - q60))
                            else:
                                adx_q = 0.75 + 0.25 * min((adx_val - q75) / q75, 1.0)
                            adx_q = max(0.0, min(1.0, adx_q))
            except Exception:
                pass  # fail-open: use fallback

            f = RegimeFeatures(
                atr_q=float(atr_q),
                adx_q=float(adx_q),
                delta_ema=float(self._regime_delta_ema),
                hold_side_score=float(self._regime_hold_ema),
                vwap_cross_rate=float(cross_rate),
                vwap=float(self._regime_vwap),
                open_day=float(self._regime_open_day),
            )
            self.regime_service.update_regime(f)
            regime_state = self.regime_service.get_current_regime()

            # Запись в BucketState один раз (единый output), но с поддержкой разных форматов service state
            final_score = base_score
            final_label = base_label
            # полная версия может содержать .score и .regime
            if hasattr(regime_state, "score"):
                try:
                    final_score = float(getattr(regime_state, "score", base_score) or base_score)
                except Exception:
                    final_score = base_score
            if hasattr(regime_state, "regime"):
                try:
                    final_label = str(getattr(regime_state, "regime", base_label) or base_label)
                except Exception:
                    final_label = base_label

            st.regime_score = float(final_score)
            st.regime_label = str(final_label)

            # --- Publish regime:{symbol} for tick-centric services (CryptoOrderflowService / SMT) ---
            try:
                # Throttle publishing (e.g., once per 500ms)
                if ts_ms - int(self._regime_last_pub_ms or 0) >= self._regime_pub_gap_ms:
                    sym = str(self.symbol or "").upper()
                    if sym and hasattr(self, 'parser') and hasattr(self.parser, 'redis'):
                        redis_client = getattr(self.parser, 'redis', None)
                        if redis_client:
                            redis_client.set(f"regime:{sym}", str(final_label), ex=self._regime_redis_ttl_sec)
                            self._regime_last_pub_ms = ts_ms
            except Exception:
                pass  # fail-open: regime publishing is best-effort

        # ВАЖНО: возврат события закрытия бара для сигнального пайплайна
        # None -> бар не закрыт на этом тике
        return finished_bar, closed_bucket_ts_ms

    def _coerce_float(self, x: Any) -> float:
        try:
            if x is None: return 0.0
            if isinstance(x, (int, float)): return float(x)
            s = str(x).strip()
            return float(s) if s else 0.0
        except Exception:
            return 0.0

    def build_signal_ctx(self, pivots: Union[None, Dict[str, float], Dict[str, Any]] = None) -> OrderflowSignalContext:
        """Создание OrderflowSignalContext из текущего BucketState"""
        st = self._bucket_state

        # --- now_ts: strict timestamp normalization ---
        now_ts = int(getattr(st, "ts", 0) or 0)
        if now_ts <= 0:
            now_ts = get_ny_time_millis()

        # ---- pivots: strict normalization + store meta in BucketState (single source of truth) ----
        pivots_dict, pivots_ts_ms, pivots_date = normalize_pivots_input(pivots)
        try:
            st.pivots_ts_ms = int(pivots_ts_ms)
        except Exception:
            st.pivots_ts_ms = 0
        try:
            st.pivots_date = str(pivots_date or "")
        except Exception:
            st.pivots_date = ""

        # --- spread / mid ---
        best_bid = float(getattr(st, "best_bid", 0.0) or 0.0)
        best_ask = float(getattr(st, "best_ask", 0.0) or 0.0)
        spread_known = bool(best_bid > 0.0 and best_ask > 0.0 and best_ask >= best_bid)
        if spread_known:
            mid = 0.5 * (best_bid + best_ask)
            spread = best_ask - best_bid
            spread_bps = float((spread / mid) * 1e4) if mid > 0.0 else 0.0
        else:
            mid = float(getattr(st, "mid", 0.0) or 0.0)
            spread_bps = 0.0

        # --- nearest pivot: deterministic, based on best available ref price ---
        ref_price = float(mid if mid > 0.0 else getattr(st, "price", 0.0) or 0.0)
        npk, npp = nearest_pivot(ref_price, pivots_dict)
        try:
            st.nearest_pivot_key = str(npk or "")
        except Exception:
            st.nearest_pivot_key = ""
        try:
            st.nearest_pivot_price = float(npp or 0.0)
        except Exception:
            st.nearest_pivot_price = 0.0

        # --- spread / mid ---
        best_bid = float(getattr(st, "best_bid", 0.0) or 0.0)
        best_ask = float(getattr(st, "best_ask", 0.0) or 0.0)
        spread_known = bool(best_bid > 0.0 and best_ask > 0.0 and best_ask >= best_bid)
        if spread_known:
            mid = 0.5 * (best_bid + best_ask)
            spread = best_ask - best_bid
            spread_bps = float((spread / mid) * 1e4) if mid > 0.0 else 0.0
        else:
            mid = float(getattr(st, "mid", 0.0) or 0.0)
            spread_bps = 0.0

        # --- L2 freshness (единый источник истины: BucketState) ---
        l2_ts = int(getattr(st, "l2_ts", 0) or 0)
        l2_age_ms = int(getattr(st, "l2_age_ms", 10**9) or 10**9)
        l2_is_stale = bool(getattr(st, "l2_is_stale", True))

        # --- OBI validity: строго из st.obi_20_valid ---
        obi_20_valid = bool(getattr(st, "obi_20_valid", False))
        obi_sust_20 = bool(getattr(st, "obi_sustained_20", False))

        # --- spread gate ---
        spread_max = float(getattr(self.config, "spread_bps_max", 15.0))
        spread_ok = (not spread_known) or (spread_bps >= 0.0 and spread_bps <= spread_max)

        # --- walls ---
        wall_bid_persist = float(getattr(st, "wall_bid_persist_ratio", 0.0) or 0.0)
        wall_ask_persist = float(getattr(st, "wall_ask_persist_ratio", 0.0) or 0.0)
        wall_bid_susp = bool(getattr(st, "wall_bid_suspicious", False))
        wall_ask_susp = bool(getattr(st, "wall_ask_suspicious", False))

        # Golden L2
        golden_l2 = (
            (not l2_is_stale)
            and obi_20_valid
            and obi_sust_20
            and spread_known
            and spread_ok
            and (not wall_bid_susp)
            and (not wall_ask_susp)
        )

        # --- bar snapshot (strictly from BucketState), with sanitize ---
        def _f_attr(name: str, default: float = 0.0) -> float:
            try:
                v = getattr(st, name, default)
                fv = float(v) if v is not None else float(default)
                return fv if math.isfinite(fv) else float(default)
            except Exception:
                return float(default)

        def _i_attr(name: str, default: int = 0, *, nonneg: bool = True) -> int:
            try:
                v = int(getattr(st, name, default) or default)
                if nonneg and v < 0:
                    return 0
                return v
            except Exception:
                return 0 if nonneg else int(default)

        def _b_attr(name: str, default: bool = False) -> bool:
            try:
                return bool(getattr(st, name, default))
            except Exception:
                return bool(default)

        # bar_id: accept int-like floats, drop non-positive
        bar_id = None
        try:
            _raw_bar_id = getattr(st, "bar_id", None)
            if isinstance(_raw_bar_id, (int, float)):
                _bid = int(_raw_bar_id)
                if _bid > 0 and float(_raw_bar_id) == float(_bid):
                    bar_id = _bid
        except Exception:
            bar_id = None

        bar_ts_open_ms = _i_attr("bar_ts_open_ms", 0, nonneg=True)
        # normalize seconds->ms if needed (epoch seconds ~1e9, ms ~1e12)
        if 10**9 < bar_ts_open_ms < 10**11:
            bar_ts_open_ms *= 1000

        bar_open  = _f_attr("bar_open", 0.0)
        bar_high  = _f_attr("bar_high", 0.0)
        bar_low   = _f_attr("bar_low", 0.0)
        bar_close = _f_attr("bar_close", 0.0)
        bar_range = _f_attr("bar_range", 0.0)
        bar_range_bps = _f_attr("bar_range_bps", 0.0)
        bar_range_bps_ema = _f_attr("bar_range_bps_ema", 0.0)
        bar_range_bps_ratio_to_ema = _f_attr("bar_range_bps_ratio_to_ema", 0.0)
        bar_range_z = _f_attr("bar_range_z", 0.0)
        bar_range_last_closed_z = _f_attr("bar_range_last_closed_z", 0.0)

        # Fix inconsistent OHLC (rare, but deadly downstream)
        if bar_high > 0.0 and bar_low > 0.0 and bar_high < bar_low:
            bar_high, bar_low = bar_low, bar_high
        if bar_range <= 0.0 and bar_high > 0.0 and bar_low > 0.0:
            bar_range = bar_high - bar_low
        if bar_range < 0.0:
            bar_range = abs(bar_range)

        # diagnostics
        prev_bar_open  = _f_attr("prev_bar_open", 0.0)
        prev_bar_high  = _f_attr("prev_bar_high", 0.0)
        prev_bar_low   = _f_attr("prev_bar_low", 0.0)
        prev_bar_close = _f_attr("prev_bar_close", 0.0)
        prev_bar_range = _f_attr("prev_bar_range", 0.0)
        prev_bar_range_bps = _f_attr("prev_bar_range_bps", 0.0)
        prev_bar_range_bps_z = _f_attr("prev_bar_range_bps_z", 0.0)
        if prev_bar_range < 0.0:
            prev_bar_range = abs(prev_bar_range)

        bar_time_backwards_cnt = _i_attr("bar_time_backwards_cnt", 0, nonneg=True)
        bar_time_backwards_flag = _b_attr("bar_time_backwards_flag", False)
        bar_time_backwards_ms = _i_attr("bar_time_backwards_ms", 0, nonneg=True)
        bar_gap_bars = _i_attr("bar_gap_bars", 0, nonneg=True)
        bar_gap_flag = _b_attr("bar_gap_flag", False)
        bar_late_tick_ignored = _i_attr("bar_late_tick_ignored", 0, nonneg=True)

        # Optional compatibility: ts_utc (sec)
        ts_utc = float(now_ts) / 1000.0 if now_ts > 0 else 0.0

        # Merge any remaining dq flags
        dq0 = list(getattr(st, "data_quality_flags", []) or [])

        # ---- ctx kwargs: максимально полная стыковка ----
        ctx_kwargs = dict(
            # IDs
            symbol=self.symbol,
            ts=now_ts,
            ts_utc=ts_utc,
            price=float(getattr(st, "price", 0.0) or 0.0),

            # config metadata
            family=str(getattr(self.config, "family", "crypto_orderflow")),
            venue=str(getattr(self.config, "venue", "binance_futures")),
            timeframe_s=int(getattr(self.config, "timeframe_s", 60)),

            # core
            z_delta=float(getattr(st, "z_delta", 0.0) or 0.0),

            # OBI short/long
            obi=float(getattr(st, "obi", 0.0) or 0.0),
            obi_avg=float(getattr(st, "obi_avg", 0.0) or 0.0),
            obi_sustained=bool(getattr(st, "obi_sustained", False)),

            obi_20=float(getattr(st, "obi_20", 0.0) or 0.0),
            obi_avg_20=float(getattr(st, "obi_avg_20", 0.0) or 0.0),
            obi_sustained_20=obi_sust_20,
            obi_20_valid=bool(obi_20_valid),

            # depths/slopes/microprice
            depth_bid_5=float(getattr(st, "depth_bid_5", 0.0) or 0.0),
            depth_ask_5=float(getattr(st, "depth_ask_5", 0.0) or 0.0),
            depth_bid_20=float(getattr(st, "depth_bid_20", 0.0) or 0.0),
            depth_ask_20=float(getattr(st, "depth_ask_20", 0.0) or 0.0),
            slope_bid_20=float(getattr(st, "slope_bid_20", 0.0) or 0.0),
            slope_ask_20=float(getattr(st, "slope_ask_20", 0.0) or 0.0),
            microprice_shift_bps_20=float(getattr(st, "microprice_shift_bps_20", 0.0) or 0.0),
            microprice=float(getattr(st, "microprice", 0.0) or 0.0),

            # spread
            spread_bps=float(spread_bps),
            spread_bps_mean=float(getattr(st, "spread_bps_mean", 0.0) or 0.0),
            spread_bps_z=float(getattr(st, "spread_bps_z", 0.0) or 0.0),

            # walls
            wall_bid=bool(getattr(st, "wall_bid", False)),
            wall_ask=bool(getattr(st, "wall_ask", False)),
            wall_bid_dist_bps=float(getattr(st, "wall_bid_dist_bps", 0.0) or 0.0),
            wall_ask_dist_bps=float(getattr(st, "wall_ask_dist_bps", 0.0) or 0.0),
            wall_bid_persist_ratio=float(wall_bid_persist),
            wall_ask_persist_ratio=float(wall_ask_persist),
            wall_bid_suspicious=bool(wall_bid_susp),
            wall_ask_suspicious=bool(wall_ask_susp),

            # L2 freshness
            l2_ts=int(l2_ts),
            l2_age_ms=int(l2_age_ms),
            l2_is_stale=bool(l2_is_stale),

            # ATR / vol
            atr=float(getattr(st, "atr", 0.0) or 0.0),
            atr_14_raw=float(getattr(st, "atr_14_raw", 0.0) or 0.0),
            atr_14_bps=float(getattr(st, "atr_14_bps", 0.0) or 0.0),
            atr_14_q=float(getattr(st, "atr_14_q", 0.0) or 0.0),
            daily_atr_bps=getattr(st, "daily_atr_bps", None),

            # delta/bucket
            current_delta=float(getattr(st, "current_delta", 0.0) or 0.0),
            delta_bucket=float(getattr(st, "delta_bucket", 0.0) or 0.0),
            cvd_5m=float(getattr(st, "cvd_5m", 0.0) or 0.0),
            cvd_divergence=float(getattr(st, "cvd_divergence", 0.0) or 0.0),

            # L3-lite
            taker_buy_qty_bucket=float(getattr(st, "taker_buy_qty_bucket", 0.0) or 0.0),
            taker_sell_qty_bucket=float(getattr(st, "taker_sell_qty_bucket", 0.0) or 0.0),
            taker_buy_rate_ema=float(getattr(st, "taker_buy_rate_ema", 0.0) or 0.0),
            taker_sell_rate_ema=float(getattr(st, "taker_sell_rate_ema", 0.0) or 0.0),
            cancel_to_trade_bid=float(getattr(st, "cancel_to_trade_bid", 0.0) or 0.0),
            cancel_to_trade_ask=float(getattr(st, "cancel_to_trade_ask", 0.0) or 0.0),
            cancel_bid_rate_ema=float(getattr(st, "cancel_bid_rate_ema", 0.0) or 0.0),
            cancel_ask_rate_ema=float(getattr(st, "cancel_ask_rate_ema", 0.0) or 0.0),
            eta_fill_bid_sec=float(getattr(st, "eta_fill_bid_sec", 0.0) or 0.0),
            eta_fill_ask_sec=float(getattr(st, "eta_fill_ask_sec", 0.0) or 0.0),
            pull_ask_qty_proxy=float(getattr(st, "pull_ask_qty_proxy", 0.0) or 0.0),
            pull_bid_qty_proxy=float(getattr(st, "pull_bid_qty_proxy", 0.0) or 0.0),
            
            # P1: OFI / Churn
            ofi_val=float(getattr(st, "ofi_val", 0.0) or 0.0),
            ofi_z=float(getattr(st, "ofi_z", 0.0) or 0.0),
            book_churn_hz=float(getattr(st, "book_churn_hz", 0.0) or 0.0),
            book_churn_z=float(getattr(st, "book_churn_z", 0.0) or 0.0),

            # P1: Event Recency (age_ms)
            iceberg_age_ms=int(now_ts - int(getattr(st, "last_iceberg_ts", 0) or 0)) if getattr(st, "last_iceberg_ts", 0) > 0 else -1,
            sweep_age_ms=int(now_ts - int(getattr(st, "last_sweep_ts", 0) or 0)) if getattr(st, "last_sweep_ts", 0) > 0 else -1,
            reclaim_age_ms=int(now_ts - int(getattr(st, "last_reclaim_ts", 0) or 0)) if getattr(st, "last_reclaim_ts", 0) > 0 else -1,
            microprice_shift_age_ms=int(now_ts - int(getattr(st, "last_microprice_shift_ts", 0) or 0)) if getattr(st, "last_microprice_shift_ts", 0) > 0 else -1,
            obi_event_age_ms=int(now_ts - int(getattr(st, "last_obi_spike_ts", 0) or 0)) if getattr(st, "last_obi_spike_ts", 0) > 0 else -1,

            # touch-level
            touch_bid_tag=str(getattr(st, "touch_bid_tag", "none") or "none"),
            touch_ask_tag=str(getattr(st, "touch_ask_tag", "none") or "none"),
            touch_bid_rho=float(getattr(st, "touch_bid_rho", 0.0) or 0.0),
            touch_ask_rho=float(getattr(st, "touch_ask_rho", 0.0) or 0.0),
            touch_bid_traded_w=float(getattr(st, "touch_bid_traded_w", 0.0) or 0.0),
            touch_ask_traded_w=float(getattr(st, "touch_ask_traded_w", 0.0) or 0.0),
            touch_bid_drop_w=float(getattr(st, "touch_bid_drop_w", 0.0) or 0.0),
            touch_ask_drop_w=float(getattr(st, "touch_ask_drop_w", 0.0) or 0.0),
            touch_is_stale=bool(getattr(st, "touch_is_stale", True)),

            # burstiness
            burst_trade_count_bucket=int(getattr(st, "burst_trade_count_bucket", 0) or 0),
            burst_rate_short=float(getattr(st, "burst_rate_short", 0.0) or 0.0),
            burst_rate_long=float(getattr(st, "burst_rate_long", 0.0) or 0.0),
            burst_ratio=float(getattr(st, "burst_ratio", 0.0) or 0.0),
            burst_cv_dt=float(getattr(st, "burst_cv_dt", 0.0) or 0.0),
            burst_fano_counts=float(getattr(st, "burst_fano_counts", 0.0) or 0.0),
            burst_flip_ratio=float(getattr(st, "burst_flip_ratio", 0.0) or 0.0),

            # regime (если добавили в BucketState; иначе останется дефолт)
            regime_score=float(getattr(st, "regime_score", 0.0) or 0.0),
            regime_label=str(getattr(st, "regime_label", "mixed") or "mixed"),

            # pivots
            pivots=pivots_dict,

            # Bar range snapshot (from collected variables above)
            bar_id=bar_id,
            bar_ts_open_ms=bar_ts_open_ms,
            bar_open=bar_open,
            bar_high=bar_high,
            bar_low=bar_low,
            bar_close=bar_close,
            bar_range=bar_range,
            bar_range_bps=bar_range_bps,
            bar_range_bps_ema=bar_range_bps_ema,
            bar_range_bps_ratio_to_ema=bar_range_bps_ratio_to_ema,
            bar_range_z=bar_range_z,
            bar_range_last_closed_z=bar_range_last_closed_z,

            prev_bar_open=prev_bar_open,
            prev_bar_high=prev_bar_high,
            prev_bar_low=prev_bar_low,
            prev_bar_close=prev_bar_close,
            prev_bar_range=prev_bar_range,
            prev_bar_range_bps=prev_bar_range_bps,
            prev_bar_range_bps_z=prev_bar_range_bps_z,

            bar_time_backwards_cnt=bar_time_backwards_cnt,
            bar_time_backwards_flag=bar_time_backwards_flag,
            bar_time_backwards_ms=bar_time_backwards_ms,
            bar_gap_bars=bar_gap_bars,
            bar_gap_flag=bar_gap_flag,
            bar_late_tick_ignored=bar_late_tick_ignored,

            # Pivots meta (strictly from BucketState)
            pivots_ts_ms=int(getattr(st, "pivots_ts_ms", 0) or 0),
            pivots_date=str(getattr(st, "pivots_date", "") or ""),
            nearest_pivot_key=str(getattr(st, "nearest_pivot_key", "") or ""),
            nearest_pivot_price=float(getattr(st, "nearest_pivot_price", 0.0) or 0.0),

            # Fail-open telemetry flags
            data_quality_flags=dq0,
        )

        ctx = OrderflowSignalContext(**_filter_dataclass_kwargs(OrderflowSignalContext, ctx_kwargs))

        # --- nearest_pivot is now set in constructor above ---

        # thresholds
        thr = OrderflowSignalThresholds(
            min_bucket_trades=int(getattr(self.config, "min_bucket_trades", 0)),
            min_bucket_notional_usd=float(getattr(self.config, "min_bucket_notional_usd", 0.0)),
            min_delta_z=float(getattr(self.config, "min_delta_z", 0.0)),
            min_obi_z=float(getattr(self.config, "min_obi_z", 0.0)),
        )
        ctx.thresholds = thr

        # passes_thresholds (как “hard gate”)
        delta_ok = abs(ctx.z_delta) >= thr.min_delta_z if thr.min_delta_z > 0 else True

        obi_ref = float(ctx.obi_avg_20)
        obi_ok = abs(obi_ref) >= thr.min_obi_z if thr.min_obi_z > 0 else True

        l2_ok = (not ctx.l2_is_stale) and bool(ctx.obi_20_valid)

        trades = int(getattr(st, "burst_trade_count_bucket", 0) or 0)  # fallback если нет trades_count
        notional = float(getattr(st, "notional_usd", 0.0) or 0.0)       # если у вас есть такое поле
        bucket_ok = True
        if thr.min_bucket_trades > 0:
            bucket_ok = bucket_ok and (trades >= thr.min_bucket_trades)
        if thr.min_bucket_notional_usd > 0.0:
            bucket_ok = bucket_ok and (notional >= thr.min_bucket_notional_usd)

        ctx.passes_thresholds = bool(delta_ok and obi_ok and l2_ok and bucket_ok and spread_ok)
        ctx.golden_l2 = bool(golden_l2)

        # metrics/calibrated — безопасно только если вы добавили поля и weak_progress вычислен
        try:
            # weak_progress не вычисляется в этом модуле - не включаем пока
            if hasattr(ctx, "metrics"):
                ctx.metrics = {
                    "deltaSpike_z": ctx.z_delta,
                    "obi": ctx.obi,
                }
            if hasattr(ctx, "calibrated"):
                ctx.calibrated = {}
        except Exception:
            pass

        # ------------------------------------------------------------------
        # News enrichment (tick-loop safe):
        # We attach compact news features to ctx using the *shadow* enricher.
        # IMPORTANT:
        # - attach() must not do Redis IO; it only reads in-memory cache
        # - failures must be fail-open (never break signal generation)
        #
        # This makes ctx.news available earlier than CandidateEmitPipeline,
        # so any downstream logic (filters, risk, regime, etc.) can use it.
        # ------------------------------------------------------------------
        try:
            from news_pipeline.enricher_singleton import get_news_enricher
            from common.dq_flags import append_dq_flag  # fail-open telemetry marker
            enr = get_news_enricher()
            if enr is not None:
                enr.attach(ctx, asset_class=getattr(ctx, "asset_class", "crypto"))
                # Optional alias for older code paths:
                try:
                    ctx.news_ref = (ctx.news.ref if getattr(ctx, "news", None) else "")
                except Exception:
                    pass
            else:
                # disabled or unavailable
                pass
        except Exception:
            # fail-open: mark once for observability, but keep pipeline alive
            try:
                append_dq_flag(ctx, "news_enrich_fail")
            except Exception:
                pass
            try:
                ctx.news = None
                ctx.news_ref = ""
            except Exception:
                pass

        return ctx

    @staticmethod
    def regime_allows_signal(regime_score: float, signal_type: str) -> bool:
        """
        Hard gate: проверка, разрешен ли тип сигнала в текущем режиме рынка.

        Args:
            regime_score: score from regime service [-1..+1]
            signal_type: signal type string

        Returns:
            True if signal is allowed in current regime
        """
        # breakout-семейства только если score >= 0
        if signal_type in ("breakout", "delta_spike_breakout", "volatility_breakout", "sweep_breakout"):
            return regime_score >= 0.0
        # mean-reversion только если score <= 0
        if signal_type in ("mean_reversion", "fade_reclaim", "absorption_fade"):
            return regime_score <= 0.0
        return True

    def _update_l2_tick_staleness(self, now_ts: int) -> None:
        """Обновление устаревания L2 на основе времени тика"""
        st = self._bucket_state
        l2_ts = int(getattr(st, "l2_ts", 0) or 0)
        if l2_ts > 0:
            delta_ms = int(now_ts - l2_ts)   # signed: + если book старше тика, - если book "в будущем"
        else:
            delta_ms = 10**9

        # сырое знаковое значение
        if hasattr(st, "l2_age_ms_tick_raw"):
            st.l2_age_ms_tick_raw = delta_ms
        if hasattr(st, "l2_age_ms_raw"):
            st.l2_age_ms_raw = delta_ms

        # staleness = только если book действительно "старый"
        age_ms = int(delta_ms) if delta_ms > 0 else 0
        if hasattr(st, "l2_age_ms_tick"):
            st.l2_age_ms_tick = age_ms
        st.l2_age_ms = age_ms

        stale_ms = int(getattr(self.config, "l2_stale_ms", 2000))
        is_stale = bool(age_ms >= stale_ms)
        st.l2_is_stale = is_stale
        if hasattr(st, "l2_is_stale_now"):
            st.l2_is_stale_now = is_stale

        # skew = рассинхрон времени (в любую сторону)
        skew_thr = int(getattr(self.config, "l2_skew_tick_thr_ms", 5000))
        st.l2_skew_tick_ms = delta_ms
        
        is_skewed = bool(l2_ts > 0 and abs(delta_ms) >= skew_thr)
        st.l2_skew_tick_flag = is_skewed
        st.l2_skew_flag = is_skewed

        st.l2_skew_ms = delta_ms

    def _exec_quality_ok(self, ctx: OrderflowSignalContext, impulse_side: str) -> bool:
        """Quality gate для исполнения - валидация качества сигнала с использованием микроструктурных данных"""
        # 0) burst gate (если используете)
        if hasattr(self, '_burst_gate_ok') and not self._burst_gate_ok(ctx):
            return False

        # 1) L2 должен быть свежий
        if getattr(ctx, "l2_is_stale", True):
            return False

        # 2) OBI_20 должен быть валиден
        if not getattr(ctx, "obi_sustained_20", False) and not getattr(ctx, "obi_sustained", False):
            # "золотой" режим требует sustained, но можно смягчать
            pass

        if not getattr(ctx, "obi_20_valid", True):
            return False

        # 3) threshold по OBI EMA (лучше, чем raw)
        thr = float(getattr(self, "imbalance_min", 0.20))
        obi_ema = float(getattr(ctx, "obi_avg_20", 0.0))

        if impulse_side == "buy":
            if obi_ema < thr:
                return False
            # optional: microprice не должен противоречить
            if float(getattr(ctx, "microprice_shift_bps_20", 0.0)) < 0.0:
                return False

            # 4) Мягкая фильтрация стен: запрещать только очень проблемные стены
            ask_persist = float(getattr(ctx, "wall_ask_persist_ratio", 0.0))
            ask_susp = bool(getattr(ctx, "wall_ask_suspicious", False))
            ask_dist = float(getattr(ctx, "wall_ask_dist_bps", 1e9))

            # Запрещать только если: высоко персистентная, очень близкая, не suspicious
            persist_min = float(getattr(self.config, "wall_filter_persist_min", 0.7))
            dist_max = float(getattr(self.config, "wall_filter_dist_max_bps", 4.0))
            if (ask_persist >= persist_min) and (not ask_susp) and (ask_dist <= dist_max):
                return False

        else:  # sell
            if obi_ema > -thr:
                return False
            if float(getattr(ctx, "microprice_shift_bps_20", 0.0)) > 0.0:
                return False

            # Мягкая фильтрация стен: запрещать только очень проблемные стены
            bid_persist = float(getattr(ctx, "wall_bid_persist_ratio", 0.0))
            bid_susp = bool(getattr(ctx, "wall_bid_suspicious", False))
            bid_dist = float(getattr(ctx, "wall_bid_dist_bps", 1e9))

            # Запрещать только если: высоко персистентная, очень близкая, не suspicious
            persist_min = float(getattr(self.config, "wall_filter_persist_min", 0.7))
            dist_max = float(getattr(self.config, "wall_filter_dist_max_bps", 4.0))
            if (bid_persist >= persist_min) and (not bid_susp) and (bid_dist <= dist_max):
                return False

        return True

    def _get_wall_confidence_modifier(self, ctx: OrderflowSignalContext, impulse_side: str) -> float:
        """
        Расчет модификатора уверенности на основе анализа стен.
        Возвращает штраф (-значение) или бонус (+значение) к оценке уверенности.

        Отрицательные значения снижают уверенность, положительные — повышают.
        """
        modifier = 0.0
        penalty = float(getattr(self.config, "wall_confidence_penalty", 0.1))

        if impulse_side == "buy":
            # Для BUY сигналов проблемной является wall_ask
            ask_persist = float(getattr(ctx, "wall_ask_persist_ratio", 0.0))
            ask_susp = bool(getattr(ctx, "wall_ask_suspicious", False))
            ask_dist = float(getattr(ctx, "wall_ask_dist_bps", 1e9))

            persist_min = float(getattr(self.config, "wall_filter_persist_min", 0.7))
            dist_max = float(getattr(self.config, "wall_filter_dist_max_bps", 4.0))

            # Жесткий фильтр: очень близкая персистентная не-suspicious стена
            if (ask_persist >= persist_min) and (not ask_susp) and (ask_dist <= dist_max):
                modifier -= penalty * 2.0  # двойной штраф для очень проблемных стен
            # Мягкий штраф: близкая персистентная стена (даже если suspicious)
            elif (ask_persist >= 0.5) and (ask_dist <= dist_max * 1.5):
                modifier -= penalty * 0.5  # половинный штраф
            # Бонус: suspicious стена вдали - это хорошо (anti-spoof работает)
            elif ask_susp and ask_persist >= 0.3 and ask_dist > dist_max * 1.5:
                modifier += penalty * 0.3  # небольшой бонус за выявленный спуф

        else:  # sell
            # Для SELL сигналов проблемной является wall_bid
            bid_persist = float(getattr(ctx, "wall_bid_persist_ratio", 0.0))
            bid_susp = bool(getattr(ctx, "wall_bid_suspicious", False))
            bid_dist = float(getattr(ctx, "wall_bid_dist_bps", 1e9))

            persist_min = float(getattr(self.config, "wall_filter_persist_min", 0.7))
            dist_max = float(getattr(self.config, "wall_filter_dist_max_bps", 4.0))

            # Жесткий фильтр: очень близкая персистентная не-suspicious стена
            if (bid_persist >= persist_min) and (not bid_susp) and (bid_dist <= dist_max):
                modifier -= penalty * 2.0  # двойной штраф для очень проблемных стен
            # Мягкий штраф: близкая персистентная стена (даже если suspicious)
            elif (bid_persist >= 0.5) and (bid_dist <= dist_max * 1.5):
                modifier -= penalty * 0.5  # половинный штраф
            # Бонус: suspicious стена вдали - это хорошо (anti-spoof работает)
            elif bid_susp and bid_persist >= 0.3 and bid_dist > dist_max * 1.5:
                modifier += penalty * 0.3  # небольшой бонус за выявленный спуф

        return modifier

    def _is_golden_quality(self, ctx: OrderflowSignalContext, impulse_side: str) -> bool:
        """Check for 'golden' quality signal with strict microstructure criteria"""
        # "Золотой" quality-флаг (для усиления confidence):
        # obi_sustained_20 == True
        if not getattr(ctx, "obi_sustained_20", False):
            return False

        # spread_bps в норме (например < 10 bps)
        spread_bps = float(getattr(ctx, "spread_bps", 0.0))
        if spread_bps >= 10.0:
            return False

        # wall_* если присутствует, то персистентна и не suspicious
        if impulse_side == "buy":
            if getattr(ctx, "wall_ask", False):
                ask_persist = float(getattr(ctx, "wall_ask_persist_ratio", 0.0))
                ask_susp = bool(getattr(ctx, "wall_ask_suspicious", False))
                if ask_persist < 0.8 or ask_susp:  # stricter threshold for golden
                    return False
        else:  # sell
            if getattr(ctx, "wall_bid", False):
                bid_persist = float(getattr(ctx, "wall_bid_persist_ratio", 0.0))
                bid_susp = bool(getattr(ctx, "wall_bid_suspicious", False))
                if bid_persist < 0.8 or bid_susp:  # stricter threshold for golden
                    return False

        # microprice_shift знак "за вашу сторону"
        microprice_shift = float(getattr(ctx, "microprice_shift_bps_20", 0.0))
        if impulse_side == "buy" and microprice_shift <= 0.0:
            return False
        elif impulse_side == "sell" and microprice_shift >= 0.0:
            return False

        return True

    # NOTE: legacy L2/OBI helpers removed.
    # OBI/OBI20/bands/sustained/micro/slope/walls are computed by L2MicrostructureEngine and stored in BucketState.

    def _process_book(self, book: Union[Any, Dict[str, Any]]) -> None:
        """
        book может быть:
          1) уже распарсенным SimpleL2Snapshot
          2) старым dict: {"snapshot": SimpleL2Snapshot, "ts_ms": int, ...}
          3) сырыми fields из Redis stream (fallback)
        """
        snap = None
        if isinstance(book, SimpleL2Snapshot):
            snap = book
        elif isinstance(book, dict) and ("snapshot" in book) and ("ts_ms" in book):
            snap = book["snapshot"]
        else:
            snap = self.parser._parse_book(book)

        if not snap:
            return

        ts_ms = int(getattr(snap, "ts_ms", 0) or 0)
        if ts_ms <= 0:
            ts_ms = get_ny_time_millis()

        # Single source of truth for ALL L2 metrics:
        st = self._bucket_state
        self.l2_engine.update(snap, ts_ms, st)

        # Feed L3-lite proxy with L2 totals (top-20 depth)
        if self.l3_queue:
             self.l3_queue.on_l2_totals(
                 bid_total=float(getattr(st, "depth_bid_20", 0.0) or 0.0),
                 ask_total=float(getattr(st, "depth_ask_20", 0.0) or 0.0)
             )

        # GPU Offloading (Async Metrics)
        if self.l2_gpu_processor:
            try:
                # Prepare batch of updates from snapshot (top 20 levels usually enough for metrics)
                # Convert SimpleL2Snapshot to list of dicts suitable for L2GPUProcessor
                gpu_updates = []
                for p, s in snap.bids[:20]:
                    gpu_updates.append({'price': float(p), 'size': float(s), 'side': 'bid'})
                for p, s in snap.asks[:20]:
                    gpu_updates.append({'price': float(p), 'size': float(s), 'side': 'ask'})
                
                if gpu_updates:
                    self.l2_gpu_processor.add_l2_data(gpu_updates)
            except Exception:
                # Fail-open: GPU logging should never crash the main path
                pass

        # P1: Book Churn (Hz)
        # Simple Hz estimate: 1000 / delta_ms between updates
        if hasattr(self, "_churn_stats"):
            now_ms = int(ts_ms)
            if self._last_book_ts > 0:
                dt = now_ms - self._last_book_ts
                if dt > 0:
                    hz = 1000.0 / float(dt)
                    # Churn often implies volume change too, but update-rate is a good proxy for "activity/speed"
                    # Clamp to sane values (e.g. 100 Hz max)
                    hz = min(hz, 200.0)
                    
                    self._churn_stats.update(hz)
                    z = self._churn_stats.z(hz)
                    
                    st.book_churn_hz = float(hz)
                    st.book_churn_z = float(z)
            self._last_book_ts = now_ms

        # Синхронизируем staleness относительно ПОСЛЕДНЕГО тика (если он уже был),
        # чтобы build_signal_ctx не жил "старыми" флагами до прихода следующего тика.
        last_tick_ts = int(getattr(st, "ts", 0) or 0)
        if last_tick_ts > 0:
            # We must use current 'now' if last_tick was long ago, but using stored ts is safer for consistency
            self._update_l2_tick_staleness(last_tick_ts)
        else:
            # тиков ещё не было — считаем L2 "свежим" на момент прихода book
            st.l2_age_ms = 0
            st.l2_is_stale = False
            if hasattr(st, "l2_is_stale_now"):
                st.l2_is_stale_now = False

    def _process_l2_snapshot(self, snap: Any, ts: int) -> None:
        """Process L2 snapshot."""
        # Additional L2 processing
        pass
