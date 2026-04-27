# health_metrics.py
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

import redis


@dataclass
class SymbolBucket:
    ticks_total: int = 0
    ticks_with_l2: int = 0
    ticks_l2_stale_tick: int = 0  # Stale относительно тика (для сигналов)
    ticks_l2_stale_now: int = 0   # Stale относительно now (для SRE)

    sum_l2_age_ms: float = 0.0
    sum_l2_age_ms_tick: float = 0.0

    sum_eta_fill_ms: float = 0.0
    eta_fill_count: int = 0

    sum_burst_ratio: float = 0.0
    burst_count: int = 0

    sum_imbalance_min: float = 0.0
    imbalance_count: int = 0

    signals_emitted: int = 0
    dlq_count: int = 0

    # per-stream lag (avg over window)
    sum_book_lag_ms: float = 0.0
    book_lag_count: int = 0
    sum_ticks_lag_ms: float = 0.0
    ticks_lag_count: int = 0
    sum_l3_lag_ms: float = 0.0
    l3_lag_count: int = 0

    # pending length (latest observed in window)
    pending_book: int = 0
    pending_ticks: int = 0
    pending_l3: int = 0

    # Multi-frequency signal metrics
    signals_bar_emitted: int = 0      # 1-minute bar signals
    signals_bucket_emitted: int = 0   # bucket-based signals
    signals_bar_failed: int = 0       # failed bar signal generation
    signals_bucket_failed: int = 0    # failed bucket signal generation

    # Signal latency (end-to-end from trigger to publish)
    sum_signal_latency_bar_ms: float = 0.0
    signal_latency_bar_count: int = 0
    sum_signal_latency_bucket_ms: float = 0.0
    signal_latency_bucket_count: int = 0

    # Signal quality distribution
    signals_high_conf: int = 0    # confidence > 0.8
    signals_med_conf: int = 0     # confidence 0.5-0.8
    signals_low_conf: int = 0     # confidence < 0.5

    # Cooldown effectiveness
    cooldown_hits: int = 0        # signals blocked by cooldown
    cooldown_misses: int = 0      # signals that passed cooldown

    # Bucket event tracking
    bucket_events_total: int = 0      # total bucket closes detected
    bucket_events_processed: int = 0  # bucket events that generated signals
    bucket_events_suppressed: int = 0 # bucket events suppressed by bar close

    # Quality gate rejections
    quality_gate_bar_rejected: int = 0    # bar signals rejected by quality gates
    quality_gate_bucket_rejected: int = 0 # bucket signals rejected by quality gates


class HealthMetrics:
    def __init__(self, redis_url: str, window_sec: int = 5, redis_client: Optional[redis.Redis] = None, max_connections: int = 20):
        self._window_sec = window_sec
        self._buckets: Dict[str, SymbolBucket] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        
        if redis_client is not None:
            self._redis = redis_client
        else:
            # ✅ FIX: Use bounded pool for background sync metrics to prevent connection leaks
            self._redis = redis.Redis.from_url(
                redis_url, 
                max_connections=max_connections,
                decode_responses=True,
                socket_timeout=5.0,
                health_check_interval=0
            )


    def _get_bucket(self, symbol: str) -> SymbolBucket:
        """Get or create bucket for symbol."""
        with self._lock:
            b = self._buckets.get(symbol)
            if b is None:
                b = SymbolBucket()
                self._buckets[symbol] = b
            return b

    def on_tick(
        self,
        *,
        symbol: str,
        l2_age_ms: float,
        l2_age_ms_tick: float,
        l2_is_stale: bool,        # Относительно тика (гейт сигналов)
        l2_is_stale_now: bool,    # Относительно now (SRE/алерты)
        eta_fill_ms: Optional[float] = None,
        burst_ratio: Optional[float] = None,
        imbalance_min: Optional[float] = None,
    ) -> None:
        with self._lock:
            bucket = self._buckets.setdefault(symbol, SymbolBucket())
            bucket.ticks_total += 1

            if l2_age_ms == l2_age_ms:  # not NaN
                bucket.ticks_with_l2 += 1
                bucket.sum_l2_age_ms += l2_age_ms
                bucket.sum_l2_age_ms_tick += l2_age_ms_tick

            if l2_is_stale:
                bucket.ticks_l2_stale_tick += 1
            if l2_is_stale_now:
                bucket.ticks_l2_stale_now += 1

            if eta_fill_ms is not None:
                bucket.sum_eta_fill_ms += eta_fill_ms
                bucket.eta_fill_count += 1
            if burst_ratio is not None:
                bucket.sum_burst_ratio += burst_ratio
                bucket.burst_count += 1
            if imbalance_min is not None:
                bucket.sum_imbalance_min += imbalance_min
                bucket.imbalance_count += 1

    def on_signal_emit(self, symbol: str) -> None:
        with self._lock:
            bucket = self._buckets.setdefault(symbol, SymbolBucket())
            bucket.signals_emitted += 1

    def on_dlq(self, symbol: str) -> None:
        with self._lock:
            bucket = self._buckets.setdefault(symbol, SymbolBucket())
            bucket.dlq_count += 1

    def on_signal_bar_emit(self, symbol: str, latency_ms: Optional[float] = None, confidence: Optional[float] = None) -> None:
        """Track bar-based signal emission (1-minute bars)."""
        with self._lock:
            bucket = self._buckets.setdefault(symbol, SymbolBucket())
            bucket.signals_bar_emitted += 1
            bucket.signals_emitted += 1

            if latency_ms is not None:
                bucket.sum_signal_latency_bar_ms += latency_ms
                bucket.signal_latency_bar_count += 1

            if confidence is not None:
                if confidence > 0.8:
                    bucket.signals_high_conf += 1
                elif confidence > 0.5:
                    bucket.signals_med_conf += 1
                else:
                    bucket.signals_low_conf += 1

    def on_signal_bucket_emit(self, symbol: str, latency_ms: Optional[float] = None, confidence: Optional[float] = None) -> None:
        """Track bucket-based signal emission."""
        with self._lock:
            bucket = self._buckets.setdefault(symbol, SymbolBucket())
            bucket.signals_bucket_emitted += 1
            bucket.signals_emitted += 1

            if latency_ms is not None:
                bucket.sum_signal_latency_bucket_ms += latency_ms
                bucket.signal_latency_bucket_count += 1

            if confidence is not None:
                if confidence > 0.8:
                    bucket.signals_high_conf += 1
                elif confidence > 0.5:
                    bucket.signals_med_conf += 1
                else:
                    bucket.signals_low_conf += 1

    def on_signal_bar_failed(self, symbol: str) -> None:
        """Track bar signal generation failures."""
        with self._lock:
            bucket = self._buckets.setdefault(symbol, SymbolBucket())
            bucket.signals_bar_failed += 1

    def on_signal_bucket_failed(self, symbol: str) -> None:
        """Track bucket signal generation failures."""
        with self._lock:
            bucket = self._buckets.setdefault(symbol, SymbolBucket())
            bucket.signals_bucket_failed += 1

    def on_cooldown_hit(self, symbol: str) -> None:
        """Track cooldown blocking signals."""
        with self._lock:
            bucket = self._buckets.setdefault(symbol, SymbolBucket())
            bucket.cooldown_hits += 1

    def on_cooldown_miss(self, symbol: str) -> None:
        """Track signals that passed cooldown."""
        with self._lock:
            bucket = self._buckets.setdefault(symbol, SymbolBucket())
            bucket.cooldown_misses += 1

    def on_bucket_event(self, symbol: str, processed: bool = False, suppressed: bool = False) -> None:
        """Track bucket events (closes)."""
        with self._lock:
            bucket = self._buckets.setdefault(symbol, SymbolBucket())
            bucket.bucket_events_total += 1

            if processed:
                bucket.bucket_events_processed += 1
            if suppressed:
                bucket.bucket_events_suppressed += 1

    def on_quality_gate_rejection(self, symbol: str, signal_type: str = "bar") -> None:
        """Track signals rejected by quality gates."""
        with self._lock:
            bucket = self._buckets.setdefault(symbol, SymbolBucket())
            if signal_type.lower() == "bucket":
                bucket.quality_gate_bucket_rejected += 1
            else:
                bucket.quality_gate_bar_rejected += 1

    def inc_unified_error(self, symbol: str) -> None:
        """Инкремент счетчика ошибок unified pipeline."""
        try:
            self._redis.incr(f"orderflow:{symbol}:unified_errors_total")
        except Exception:
            # Graceful degradation - don't fail if Redis is down
            pass

    def inc_unified_fallback(self, symbol: str) -> None:
        """Инкремент счетчика fallback на legacy."""
        try:
            self._redis.incr(f"orderflow:{symbol}:unified_fallback_total")
        except Exception:
            # Graceful degradation - don't fail if Redis is down
            pass

    def on_stream_lag(self, symbol: str, stream_kind: str, lag_ms: int) -> None:
        """
        Record per-stream lag (now_ms - msg_ts) in ms.
        stream_kind: "ticks" | "book" | "l3"
        """
        lag = int(max(0, lag_ms))
        with self._lock:
            b = self._get_bucket(symbol)
            k = (stream_kind or "").lower()
            if k == "ticks":
                b.sum_ticks_lag_ms += lag
                b.ticks_lag_count += 1
            elif k == "book":
                b.sum_book_lag_ms += lag
                b.book_lag_count += 1
            elif k == "l3":
                b.sum_l3_lag_ms += lag
                b.l3_lag_count += 1

    def on_pending_len(self, symbol: str, stream: str, pending_len: int) -> None:
        """
        Pending length per stream (latest observed value in window).
        """
        try:
            p = int(pending_len)
        except Exception:
            return
        if p < 0:
            p = 0

        s = (stream or "").lower()
        if s.startswith("book"):
            kind = "book"
        elif s.startswith("ticks") or s.startswith("tick"):
            kind = "ticks"
        elif s.startswith("l3"):
            kind = "l3"
        else:
            return

        with self._lock:
            b = self._get_bucket(symbol)
            if kind == "book":
                b.pending_book = p
            elif kind == "ticks":
                b.pending_ticks = p
            else:
                b.pending_l3 = p

    def _safe_avg(self, sum_v: float, cnt: int) -> float:
        return float(sum_v / cnt) if cnt > 0 else 0.0

    def start_background_loop(self) -> None:
        thread = threading.Thread(target=self._run_loop, daemon=True)
        thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(self._window_sec)
            self._flush_snapshot()

    def _flush_snapshot(self) -> None:
        with self._lock:
            buckets = self._buckets
            self._buckets = {}

        if not buckets:
            return

        pipe = self._redis.pipeline()
        now_ms = get_ny_time_millis()
        ttl = self._window_sec * 3

        for symbol, b in buckets.items():
            if b.ticks_total == 0 and b.signals_emitted == 0 and b.dlq_count == 0:
                continue

            avg_l2_age_ms = (b.sum_l2_age_ms / b.ticks_with_l2) if b.ticks_with_l2 > 0 else 0.0
            avg_l2_age_ms_tick = (b.sum_l2_age_ms_tick / b.ticks_with_l2) if b.ticks_with_l2 > 0 else 0.0

            l2_stale_ratio_tick = (b.ticks_l2_stale_tick / b.ticks_with_l2) if b.ticks_with_l2 > 0 else 0.0
            l2_stale_ratio_now = (b.ticks_l2_stale_now / b.ticks_with_l2) if b.ticks_with_l2 > 0 else 0.0

            avg_eta_fill_ms = (b.sum_eta_fill_ms / b.eta_fill_count) if b.eta_fill_count > 0 else 0.0
            avg_burst_ratio = (b.sum_burst_ratio / b.burst_count) if b.burst_count > 0 else 0.0
            avg_imbalance_min = (b.sum_imbalance_min / b.imbalance_count) if b.imbalance_count > 0 else 0.0

            signal_emit_rate = b.signals_emitted / self._window_sec
            dlq_rate = b.dlq_count / self._window_sec

            # Multi-frequency signal rates
            signal_bar_rate = b.signals_bar_emitted / self._window_sec
            signal_bucket_rate = b.signals_bucket_emitted / self._window_sec
            signal_bar_fail_rate = b.signals_bar_failed / self._window_sec
            signal_bucket_fail_rate = b.signals_bucket_failed / self._window_sec

            # Signal latency averages
            avg_signal_latency_bar_ms = (b.sum_signal_latency_bar_ms / b.signal_latency_bar_count) if b.signal_latency_bar_count > 0 else 0.0
            avg_signal_latency_bucket_ms = (b.sum_signal_latency_bucket_ms / b.signal_latency_bucket_count) if b.signal_latency_bucket_count > 0 else 0.0

            # Cooldown effectiveness
            cooldown_hit_rate = b.cooldown_hits / self._window_sec
            cooldown_miss_rate = b.cooldown_misses / self._window_sec
            cooldown_effectiveness = (b.cooldown_hits / (b.cooldown_hits + b.cooldown_misses)) if (b.cooldown_hits + b.cooldown_misses) > 0 else 0.0

            # Bucket event rates
            bucket_event_rate = b.bucket_events_total / self._window_sec
            bucket_processed_rate = b.bucket_events_processed / self._window_sec
            bucket_suppressed_rate = b.bucket_events_suppressed / self._window_sec

            avg_ticks_lag_ms = (b.sum_ticks_lag_ms / b.ticks_lag_count) if b.ticks_lag_count > 0 else 0.0
            avg_book_lag_ms = (b.sum_book_lag_ms / b.book_lag_count) if b.book_lag_count > 0 else 0.0
            avg_l3_lag_ms = (b.sum_l3_lag_ms / b.l3_lag_count) if b.l3_lag_count > 0 else 0.0


            base_key = f"orderflow:{symbol}"

            # Legacy metrics
            pipe.set(f"{base_key}:l2_stale_ratio_tick", l2_stale_ratio_tick, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:l2_stale_ratio_now", l2_stale_ratio_now, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:signal_emit_rate", signal_emit_rate, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:dlq_rate", dlq_rate, ex=self._window_sec * 3)

            # Multi-frequency signal metrics
            pipe.set(f"{base_key}:signal_bar_rate", signal_bar_rate, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:signal_bucket_rate", signal_bucket_rate, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:signal_bar_fail_rate", signal_bar_fail_rate, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:signal_bucket_fail_rate", signal_bucket_fail_rate, ex=self._window_sec * 3)

            # Signal latency metrics
            pipe.set(f"{base_key}:avg_signal_latency_bar_ms", avg_signal_latency_bar_ms, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:avg_signal_latency_bucket_ms", avg_signal_latency_bucket_ms, ex=self._window_sec * 3)

            # Cooldown metrics
            pipe.set(f"{base_key}:cooldown_hit_rate", cooldown_hit_rate, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:cooldown_miss_rate", cooldown_miss_rate, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:cooldown_effectiveness", cooldown_effectiveness, ex=self._window_sec * 3)

            # Bucket event metrics
            pipe.set(f"{base_key}:bucket_event_rate", bucket_event_rate, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:bucket_processed_rate", bucket_processed_rate, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:bucket_suppressed_rate", bucket_suppressed_rate, ex=self._window_sec * 3)

            # Quality gate metrics
            quality_gate_bar_reject_rate = b.quality_gate_bar_rejected / self._window_sec
            quality_gate_bucket_reject_rate = b.quality_gate_bucket_rejected / self._window_sec
            pipe.set(f"{base_key}:quality_gate_bar_reject_rate", quality_gate_bar_reject_rate, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:quality_gate_bucket_reject_rate", quality_gate_bucket_reject_rate, ex=self._window_sec * 3)

            # per-stream lag and pending
            pipe.set(f"{base_key}:book_lag_ms", avg_book_lag_ms, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:ticks_lag_ms", avg_ticks_lag_ms, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:l3_lag_ms", avg_l3_lag_ms, ex=self._window_sec * 3)
            pipe.set(f"{base_key}:pending_book", int(b.pending_book), ex=self._window_sec * 3)
            pipe.set(f"{base_key}:pending_ticks", int(b.pending_ticks), ex=self._window_sec * 3)
            pipe.set(f"{base_key}:pending_l3", int(b.pending_l3), ex=self._window_sec * 3)
            pipe.set(
                f"{base_key}:pending_len",
                int(b.pending_book + b.pending_ticks + b.pending_l3),
                ex=self._window_sec * 3,
            )

            # snapshot hash
            pipe.hset(
                f"{base_key}:health_snapshot",
                mapping={
                    "ticks_total": b.ticks_total,
                    "ticks_with_l2": b.ticks_with_l2,

                    "l2_stale_ratio_tick": f"{l2_stale_ratio_tick:.6f}",
                    "l2_stale_ratio_now": f"{l2_stale_ratio_now:.6f}",
                    "avg_l2_age_ms": f"{avg_l2_age_ms:.2f}",
                    "avg_l2_age_tick_ms": f"{avg_l2_age_ms_tick:.2f}",

                    "avg_eta_fill_ms": f"{avg_eta_fill_ms:.2f}",
                    "avg_burst_ratio": f"{avg_burst_ratio:.4f}",
                    "avg_imbalance_min": f"{avg_imbalance_min:.4f}",

                    "signal_emit_rate": f"{signal_emit_rate:.4f}",
                    "dlq_rate": f"{dlq_rate:.4f}",

                    # Multi-frequency signal metrics
                    "signal_bar_rate": f"{signal_bar_rate:.4f}",
                    "signal_bucket_rate": f"{signal_bucket_rate:.4f}",
                    "signal_bar_fail_rate": f"{signal_bar_fail_rate:.4f}",
                    "signal_bucket_fail_rate": f"{signal_bucket_fail_rate:.4f}",

                    # Signal quality distribution
                    "signals_high_conf": b.signals_high_conf,
                    "signals_med_conf": b.signals_med_conf,
                    "signals_low_conf": b.signals_low_conf,

                    # Signal latency
                    "avg_signal_latency_bar_ms": f"{avg_signal_latency_bar_ms:.2f}",
                    "avg_signal_latency_bucket_ms": f"{avg_signal_latency_bucket_ms:.2f}",

                    # Cooldown metrics
                    "cooldown_hit_rate": f"{cooldown_hit_rate:.4f}",
                    "cooldown_miss_rate": f"{cooldown_miss_rate:.4f}",
                    "cooldown_effectiveness": f"{cooldown_effectiveness:.4f}",

                    # Bucket event metrics
                    "bucket_event_rate": f"{bucket_event_rate:.4f}",
                    "bucket_processed_rate": f"{bucket_processed_rate:.4f}",
                    "bucket_suppressed_rate": f"{bucket_suppressed_rate:.4f}",

                    # Quality gate metrics
                    "quality_gate_bar_rejected": b.quality_gate_bar_rejected,
                    "quality_gate_bucket_rejected": b.quality_gate_bucket_rejected,
                    "quality_gate_bar_reject_rate": f"{quality_gate_bar_reject_rate:.4f}",
                    "quality_gate_bucket_reject_rate": f"{quality_gate_bucket_reject_rate:.4f}",

                    "avg_book_lag_ms": f"{avg_book_lag_ms:.2f}",
                    "avg_ticks_lag_ms": f"{avg_ticks_lag_ms:.2f}",
                    "avg_l3_lag_ms": f"{avg_l3_lag_ms:.2f}",

                    "pending_book": int(b.pending_book),
                    "pending_ticks": int(b.pending_ticks),
                    "pending_l3": int(b.pending_l3),
                    "pending_len": int(b.pending_book + b.pending_ticks + b.pending_l3),

                    "window_sec": self._window_sec,
                    "ts": now_ms,
                },
            )

            # (опционально) TTL для hash тоже можно поставить, если хотите:
            pipe.expire(f"{base_key}:health_snapshot", ttl)

        pipe.execute()
