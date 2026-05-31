from __future__ import annotations
from core.redis_keys import RedisStreams as RS

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


import asyncio
import json
import logging
import os
import time
from typing import Any

import redis.asyncio as aioredis

from core.atr_sanity import ATRSanity
from core.atr_tf_calibrator import ATRTfCalibrator
from core.microbar import MicroBar
from core.of_confirm_engine import OFConfirmEngine

# Consolidated core imports
from services.async_signal_publisher import AsyncSignalPublisher
from services.orderflow.components.bar_processor import BarProcessor
from services.orderflow.components.book_processor import BookProcessor
from services.orderflow.components.tick_processor import TickProcessor
from services.orderflow.market_state import MarketStateService
from services.orderflow.metrics import log_silent_error
from services.orderflow.runtime import SymbolRuntime
from services.orderflow.signal_pipeline import SignalPipeline
from services.orderflow.utils import _should_sample
from services.periodic_reporter import check_and_trigger_report
from services.signal_confidence import ConfidenceConfig, ConfidenceScorer
from utils.task_manager import safe_create_task

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

# SRE metrics for gate decisions (world-class: drift + latency + exec risk)
OF_GATE_METRICS_STREAM = os.getenv("OF_GATE_METRICS_STREAM", RS.OF_GATE_METRICS)
OF_GATE_METRICS_ENABLE = os.getenv("OF_GATE_METRICS_ENABLE", "1").strip() in ("1","true","yes","on")
OF_GATE_METRICS_SAMPLE = float(os.getenv("OF_GATE_METRICS_SAMPLE", "0.10") or 0.10)  # 10% кандидатов
OF_GATE_METRICS_MAXLEN = int(os.getenv("OF_GATE_METRICS_MAXLEN", "200000") or 200000)

# Fail-open defaults to avoid exec-risk penalty becoming 0 silently
SPREAD_BPS_MISSING_DEFAULT = float(os.getenv("SPREAD_BPS_MISSING_DEFAULT", "15.0") or 15.0)
SLIPPAGE_BPS_MISSING_DEFAULT = float(os.getenv("SLIPPAGE_BPS_MISSING_DEFAULT", "4.0") or 4.0)
DATA_HEALTH_ON_SPREAD_MISSING = float(os.getenv("DATA_HEALTH_ON_SPREAD_MISSING", "0.80") or 0.80)






# Счетчик для уменьшения логов добавления символов
_symbols_added_counter = 0





# ──────────────────────────────────────────────────────────────────────────────
from utils.atr_cache import ATRCache, get_atr_cache
import contextlib

# ──────────────────────────────────────────────────────────────────────────────
# Runtime для одного символа
# ──────────────────────────────────────────────────────────────────────────────


# Optional microstructure metrics (prom)




class OrderFlowStrategy:
    def __init__(self, redis: aioredis.Redis, ticks: aioredis.Redis, publisher: AsyncSignalPublisher,
                 of_engine: OFConfirmEngine, calib_svc=None,
                 notify_client: aioredis.Redis | None = None, notify_stream: str = RS.NOTIFY_TELEGRAM):
        self.redis = redis
        self.ticks = ticks
        self.publisher = publisher
        self.of_engine = of_engine
        self.calib_svc = calib_svc
        self.notify_client = notify_client
        self.notify_stream = notify_stream
        self.logger = logging.getLogger("orderflow_strategy")

        self.atr_cache: ATRCache = get_atr_cache()
        self.market_state = MarketStateService(redis_client=self.redis, atr_cache=self.atr_cache)
        self.signal_pipeline = SignalPipeline(publisher=self.publisher, atr_cache=self.atr_cache)
        self.low_conf_counters = {}
        self.strong_gate_counters = {}
        self.dn_gate_relaxed_counters = {}  # Counter for [DN-GATE] RELAXED messages
        self.dn_gate_proxy_relaxed_counters = {}  # Counter for [DN-GATE-PROXY] RELAXED messages
        self.conf_relax_counters = {}  # Counter for [CONF-RELAX] messages
        self.adverse_continuation_counters = {}  # Counter for [ADVERSE] Continuation Verified messages
        # Simple confidence scorer for fallback usage
        self.conf_scorer = ConfidenceScorer(cfg=ConfidenceConfig(), main_z_thr=2.5)

        # Robust ATR sanity (last-good fallback + jump protection)
        # One instance per Strategy; per-symbol state is managed internally by ATRSanity.
        self._atr_sanity = ATRSanity(window=int(os.getenv("ATR_SANITY_WINDOW", "60")))

        # SRE metrics
        self.of_gate_metrics_stream = os.getenv("OF_GATE_METRICS_STREAM", RS.OF_GATE_METRICS)

        self.of_gate_metrics_enable = os.getenv("OF_GATE_METRICS_ENABLE", "1").strip().lower() in ("1", "true", "yes", "on")
        self.of_gate_metrics_sample = float(os.getenv("OF_GATE_METRICS_SAMPLE", "0.10") or 0.10)
        self.of_gate_metrics_maxlen = int(os.getenv("OF_GATE_METRICS_MAXLEN", "200000") or 200000)

        # Components
        self.atr_tf_selector = ATRTfCalibrator(candidates=[])


        self.tick_processor = TickProcessor(
            redis=self.redis,
            ticks=self.ticks,
            publisher=self.publisher,
            of_engine=self.of_engine,
            calib_svc=self.calib_svc,
            atr_cache=self.atr_cache,
            atr_sanity=self._atr_sanity,
            conf_scorer=self.conf_scorer
        )
        self.bar_processor = BarProcessor(
            redis_client=self.redis,
            ticks_client=self.ticks,
            signal_pipeline=self.signal_pipeline,
            atr_cache=self.atr_cache,
            atr_tf_selector=self.atr_tf_selector,
            calib_svc=self.calib_svc
        )

        self.book_processor = BookProcessor()

        # OFC capture for golden replay
        self._ofc_capture_enabled = os.getenv("OFC_CAPTURE", "0") == "1"
        self._capture_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.tick_processor.set_capture_queue(self._capture_queue)
        # Start capture worker if enabled (or even if disabled, to be safe/simple, but better if enabled)
        # We start it always to simplify logic, but it will just idle if disabled.
        self._capture_task = safe_create_task(self._capture_worker())


    async def _capture_worker(self) -> None:
        """
        Background worker that drains the capture queue and writes NDJSON lines to disk.
        Uses a thread executor to avoid blocking the asyncio loop during seralization and I/O.
        """
        while True:
            try:
                # Wait for next item
                path, payload = await self._capture_queue.get()

                # Write in thread pool (serialize + write)
                try:
                    if path and payload:
                        await asyncio.to_thread(self._serialize_and_append, path, payload)
                except Exception as e:
                    self.logger.error(f"Capture write failed: {e}")
                finally:
                    self._capture_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Capture worker error: {e}")
                await asyncio.sleep(1.0)

    def _serialize_and_append(self, path: str, payload: Any) -> None:
        """Blocking helper: serialize to JSON and append to file."""
        try:
            # CPU-bound serialization happens here, in the thread pool
            line = json.dumps(payload, ensure_ascii=False, default=str)
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

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
                "cooldown_reversal_sec",
                "cooldown_continuation_sec",
                "pressure_hi_sps",
                "pressure_ema_alpha",
                "cooldown_mul_thin",
                "cooldown_spread_hi_bp",
                "cooldown_mul_wide_spread",
                "cooldown_mul_pressure_hi",
                "cooldown_min_ms",
                "cooldown_max_ms",
                "burst_audit_enable",
                "burst_audit_sample",
            }
            for k, v in d.items():
                if k in allow:
                    runtime.config[k] = v
        except Exception as exc:
            log_silent_error(exc, 'config_update_failure', runtime.symbol if runtime else "unknown", '_maybe_poll_symbol_overrides')
            return

    async def _burst_audit(self, *, runtime, now_ms: int, event: str, payload: dict[str, Any], indicators: dict[str, Any], extra: dict[str, Any]) -> None:
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
                "type": "burst_audit",
                "ts_ms": str(int(now_ms)),
                "symbol": str(runtime.symbol),
                "event": str(event),
                "payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                "ind": json.dumps({
                    "scenario": indicators.get("strong_gate_scn") or "",
                    "of_score": indicators.get("of_confirm_score", 0.0),
                    "delta_z": indicators.get("delta_z", 0.0),
                    "pressure_sps": float(getattr(runtime, "pressure_sps", 0.0) or 0.0),
                    "pressure_hi": int(getattr(runtime, "pressure_hi", 0) or 0),
                    "regime": str(getattr(runtime, "last_regime", "na") or "na"),
                    "spread_bp": float(getattr(runtime, "last_spread_bps", 0.0) or 0.0),
                    "obi_age_ms": indicators.get("obi_age_ms", -1),
                    "iceberg_age_ms": indicators.get("iceberg_age_ms", -1),
                }, ensure_ascii=False, separators=(",", ":")),
                "extra": json.dumps(extra or {}, ensure_ascii=False, separators=(",", ":")),
            }
            await self.redis.xadd(self.burst_audit_stream, msg, maxlen=200000, approximate=True)
        except Exception as exc:
            log_silent_error(exc, 'audit_failure', self.symbol or "unknown", '_burst_audit')
            return



    # ── Публичные методы ──────────────────────────────────────────────────────


    # ── Динамическая загрузка символов ────────────────────────────────────────










    # ── Основные рабочие циклы ────────────────────────────────────────────────

    async def publish_signal(self, runtime: SymbolRuntime, signal: dict[str, Any]) -> None:
        """Delegate to SignalPipeline."""
        if self.signal_pipeline:
            await self.signal_pipeline.publish_signal(runtime, signal)

    async def maintain_symbol(self, runtime: SymbolRuntime) -> None:
        """
        Periodic maintenance task called from background loop (wall-clock triggers).
        Ensures reports are generated even if no trades are arriving.
        """
        try:
            now = time.time()
            # Throttle: check report triggers every 60s
            # (PeriodicReporter has its own hour/lock checks, so calling more often is safe but wasteful)
            # Use runtime for storing state
            last_ts = getattr(runtime, "last_report_check_ts", 0)
            if now - last_ts < 60:
                return

            runtime.last_report_check_ts = now

            # Run blocking check/report in thread pool
            # "CryptoOrderFlow" matches the source used in PeriodicReporter keys
            await asyncio.to_thread(
                check_and_trigger_report,
                source="CryptoOrderFlow",
                symbol=runtime.symbol,
                counter_type="time"
            )
        except Exception as e:
            # logging best-effort
            # runtime.loop_log_sampler is available on SymbolRuntime
            try:
                if runtime.loop_log_sampler.should_log("maintain_error"):
                    self.logger.warning(f"⚠️ ({runtime.symbol}) maintain_symbol failed: {e}")
            except Exception:
                pass

    async def process_tick(self, runtime: SymbolRuntime, tick: dict[str, Any]) -> dict[str, Any] | None:
        """
        Delegate to TickProcessor.
        """
        # 1. Update CVD State (Side/Delta tracking)
        # This is critical for microbar delta_sum and cvd_close
        if runtime.cvd_state:
            with contextlib.suppress(Exception):
                runtime.cvd_state.update(tick)

        # 2. Update Microbar Aggregator
        # This generates the microbars that populate events:microbar_closed
        if runtime.microbar:
            try:
                cvd_val = float(getattr(runtime.cvd_state, "cvd_tick", 0.0) or 0.0)
                closed_bars = runtime.microbar.push_tick(tick, cvd_val)
                if closed_bars:
                    for bar in closed_bars:
                        # Update runtime last bar reference
                        runtime.last_bar = bar

                        # Process closed bar (Persistence, Calibration, Publishing)
                        # We use asyncio.create_task to not block tick processing
                        safe_create_task(self._on_microbar_closed(runtime, bar))
            except Exception:
                # Log only if really needed, avoid spam on hot path
                # log_silent_error(e, "microbar_push_failed", runtime.symbol, "OrderFlowStrategy")
                pass

        # 3. Delegate to TickProcessor for signal generation
        return await self.tick_processor.process_tick(runtime, tick)

    async def process_book(self, runtime: SymbolRuntime, payload: dict[str, Any], ingest_ts_ms: int) -> bool:
        """
        Delegate to BookProcessor.
        """
        return self.book_processor.process_book(runtime, payload, ingest_ts_ms)

    async def _on_microbar_closed(self, runtime: SymbolRuntime, bar: MicroBar) -> None:
        """
        Delegate to BarProcessor.
        """
        await self.bar_processor.process_bar(runtime, bar)

