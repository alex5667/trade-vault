from __future__ import annotations

"""
BurstFlusher — фоновый flush-loop и per-symbol burst processing.

Ранее: _burst_flush_loop + _process_burst_flush внутри CryptoOrderflowService.
Теперь: отдельный класс с явными зависимостями — легко тестировать и заменять.

ENV:
  BURST_FLUSH_MODE          wall | tick | off  (default: wall)
  BURST_FLUSH_INTERVAL_MS   интервал между flush-итерациями (default: 200ms)
"""

import asyncio
import logging
import os
import random
import time
from collections.abc import Callable
from typing import Any

from handlers.crypto_orderflow.utils.log_sampler import LogSamplerFactory
from services.observability import metrics_registry  # noqa: F401 (side-effect import для init)
from services.orderflow.metrics import (
    burst_active_gauge,
    burst_flush_total,
    signals_emitted_total,
    signals_published_total,
)
from services.signal_preprocess import preprocess_signal_for_publish
from utils.task_manager import safe_create_task
from utils.time_utils import get_ny_time_millis
import contextlib

logger = logging.getLogger("burst_flusher")


class BurstFlusher:
    """Запускает фоновый wall-clock flush и предоставляет _process_burst_flush().

    Зависимости инжектируются, а не берутся из глобального сервиса:
      symbol_contexts_fn — callable → dict[str, SymbolRuntime]
      strategy           — OrderFlowStrategy | None
      gate               — SignalGate
      is_shutdown_fn     — callable → bool
    """

    def __init__(
        self,
        *,
        symbol_contexts_fn: Callable[[], dict[str, Any]],
        strategy_fn: Callable[[], Any | None],
        gate: Any,                    # SignalGate
        is_shutdown_fn: Callable[[], bool],
    ) -> None:
        self._contexts = symbol_contexts_fn
        self._strategy = strategy_fn
        self._gate = gate
        self._shutdown = is_shutdown_fn
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = safe_create_task(self._loop(), name="burst-flusher")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._task = None

    # ── Loop ─────────────────────────────────────────────────────────────────

    async def _loop(self) -> None:
        mode = os.getenv("BURST_FLUSH_MODE", "wall").lower()
        if mode == "off":
            logger.info("ℹ️ Burst wall-flush loop is OFF")
            return

        interval_ms = int(os.getenv("BURST_FLUSH_INTERVAL_MS", "200"))
        logger.info("🚀 Burst flush loop started (mode=%s interval=%dms)", mode, interval_ms)

        _sampler = LogSamplerFactory.get_sampler("BURST_LOOP_ALIVE", 10000)
        last_alive_log = 0.0

        while not self._shutdown():
            try:
                await asyncio.sleep(max(0.05, interval_ms / 1000.0))
                now_s = time.time()
                if now_s - last_alive_log > 60:
                    if _sampler.should_log("burst_loop_alive"):
                        logger.info(
                            "💓 Burst flush loop alive. symbols=%d mode=%s",
                            len(self._contexts()), mode,
                        )
                    last_alive_log = now_s

                now_wall = get_ny_time_millis()
                runtimes = list(self._contexts().values())

                for rt in runtimes:
                    await asyncio.sleep(0)  # yield: не блокируем event loop
                    await self._process_one(rt, mode, now_wall)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                if random.random() < 0.05:
                    logger.error("Burst flush loop error: %s", exc, exc_info=True)
                await asyncio.sleep(1)

    async def _process_one(self, runtime: Any, mode: str, now_wall: int) -> None:
        if not hasattr(runtime, "burst"):
            return
        try:
            now_ms = int(getattr(runtime, "last_ts_ms", 0) or 0) if mode == "tick" else now_wall
            if now_ms <= 0:
                return
            await self.process(runtime, trigger_source="wall", ts_ms=now_ms, do_publish=True)
            strat = self._strategy()
            if strat and hasattr(strat, "maintain_symbol"):
                await strat.maintain_symbol(runtime)
        except Exception as exc:
            if random.random() < 0.01:
                logger.debug("Burst flush error (%s): %s", getattr(runtime, "symbol", "?"), exc)

    # ── Public: вызывается из consume_ticks ──────────────────────────────────

    async def process(
        self,
        runtime: Any,
        trigger_source: str,
        ts_ms: int,
        do_publish: bool = True,
    ) -> dict | None:
        """Единая точка burst-проверки.

        Args:
            do_publish: True — публикует через strategy; False — возвращает сигнал
                        без отправки (вызывающий код обрабатывает публикацию сам).
        """
        if not hasattr(runtime, "burst"):
            return None

        out = None
        async with runtime.burst_mu:
            out = runtime.burst.maybe_flush(now_ts_ms=ts_ms)
            is_active = getattr(runtime.burst.st, "active", False)
            if burst_active_gauge:
                burst_active_gauge.labels(symbol=runtime.symbol).set(1 if is_active else 0)

        if out is None:
            return None

        runtime.last_signal_ts = int(ts_ms)
        with contextlib.suppress(Exception):
            runtime.pressure.record_emit(int(ts_ms))

        if burst_flush_total:
            burst_flush_total.labels(symbol=runtime.symbol, mode=trigger_source).inc()
        if signals_emitted_total:
            signals_emitted_total.labels(symbol=runtime.symbol).inc()

        _fs = LogSamplerFactory.get_sampler("BURST_FLUSH", 10000)
        if _fs.should_log(f"burst_flush_{runtime.symbol}"):
            logger.info(
                "🔥 (%s) Burst flushed via %s: dir=%s p=%.2f score=%.2f",
                runtime.symbol, trigger_source,
                out.get("direction"), out.get("entry"), out.get("burst_best_score"),
            )

        if do_publish:
            with contextlib.suppress(Exception):
                preprocess_signal_for_publish(
                    out,
                    symbol=runtime.symbol,
                    source="crypto_orderflow_service",
                    logger=logger,
                    fast_path=False,
                )

            strat = self._strategy()
            if strat and await self._gate.allows(runtime, out):
                await strat.publish_signal(runtime, out)
                if signals_published_total:
                    signals_published_total.labels(symbol=runtime.symbol).inc()

        return out
