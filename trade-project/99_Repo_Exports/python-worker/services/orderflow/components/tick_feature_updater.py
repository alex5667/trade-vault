from __future__ import annotations

import contextlib
import os
from typing import Any

from services.orderflow.configuration import _safe_int
from services.orderflow.metric_labels import TickMetricLimiter, _parse_allowlist, should_emit
from services.orderflow.metrics import (
    tick_event_age_abs_ema_ms_gauge,
    tick_event_stream_skew_abs_ema_ms_gauge,
    tick_ts_source_now_ema_gauge,
    tick_ts_source_stream_id_ema_gauge,
    tick_unknown_side_ema_gauge,
)
from services.orderflow.tick_quality_ema import TickQualityEMA

class TickFeatureUpdater:
    def __init__(self):
        self._quality_ema: TickQualityEMA | None = None
        self._metric_limiter: TickMetricLimiter | None = None
        self._metric_last_emit_ms: dict[str, int] = {}

    def update_quality_ema(
        self,
        symbol: str,
        tick: dict,
        now_ms: int,
        unknown_side: bool,
        ts_source: str,
        event_ts_ms: int,
        raw: dict,
    ) -> None:
        try:
            if self._quality_ema is None:
                tau_ms = int(os.getenv("TICK_QUALITY_EMA_TAU_MS", "300000"))
                self._quality_ema = TickQualityEMA(tau_ms=tau_ms)

            stream_ms = _safe_int(tick.get("stream_ms") or 0)
            abs_skew = abs(event_ts_ms - stream_ms) if stream_ms and event_ts_ms else 0
            abs_age = abs(now_ms - event_ts_ms) if event_ts_ms else 0

            ema = self._quality_ema.update(
                symbol=symbol,
                ts_ms=now_ms,
                unknown_side=1.0 if unknown_side else 0.0,
                ts_source=str(ts_source),
                abs_skew_ms=float(abs_skew),
                abs_age_ms=float(abs_age),
            )

            if self._metric_limiter is None:
                allow = _parse_allowlist(os.getenv("TICK_QUALITY_SYMBOL_ALLOWLIST"))
                mode = os.getenv("TICK_QUALITY_SYMBOL_LABEL_MODE", "collapse")
                ema_min = int(os.getenv("TICK_QUALITY_EMA_UPDATE_MIN_MS", "250"))
                self._metric_limiter = TickMetricLimiter(allowlist=allow, mode=mode, ema_min_update_ms=ema_min)

            sym_lbl = self._metric_limiter.label(symbol)
            if sym_lbl is not None:
                last = self._metric_last_emit_ms.get(sym_lbl, 0)
                if should_emit(now_ms, last, self._metric_limiter.ema_min_update_ms):
                    self._metric_last_emit_ms[sym_lbl] = now_ms
                    tick_unknown_side_ema_gauge.labels(symbol=sym_lbl).set(float(ema["unknown"]))
                    tick_ts_source_now_ema_gauge.labels(symbol=sym_lbl).set(float(ema["ts_now"]))
                    tick_ts_source_stream_id_ema_gauge.labels(symbol=sym_lbl).set(float(ema["ts_stream_id"]))
                    tick_event_stream_skew_abs_ema_ms_gauge.labels(symbol=sym_lbl).set(float(ema["skew_abs_ms"]))
                    tick_event_age_abs_ema_ms_gauge.labels(symbol=sym_lbl).set(float(ema["age_abs_ms"]))
        except Exception:
            pass

    def update_health_metrics(
        self, health_metrics: Any, runtime: Any, symbol: str, event_ts_ms: int, now_ms: int
    ) -> None:
        try:
            if not health_metrics:
                return
            book = runtime.get_book_snapshot()
            if book and book.timestamp_ms:
                age = max(0, event_ts_ms - book.timestamp_ms)
                health_metrics.on_tick(
                    symbol=symbol,
                    l2_age_ms=float(age),
                    l2_age_ms_tick=float(age),
                    l2_is_stale=(age > 1500),
                    l2_is_stale_now=(max(0, now_ms - book.timestamp_ms) > 1500),
                )
        except Exception:
            pass
