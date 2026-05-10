from __future__ import annotations

import contextlib
import os
import time as _time

from services.orderflow.metrics import (
    market_inactivity_lag_ms_gauge,
    market_inactivity_lag_ms_hist,
    redis_entry_lag_ms_gauge,
    redis_entry_lag_ms_hist,
    redis_entry_lag_ms_p50_gauge,
    redis_entry_lag_ms_p99_gauge,
    tick_ingest_e2e_delay_ms,
    tick_ingest_process_ms,
    worker_lag_ms_gauge,
    worker_lag_ms_hist,
    worker_lag_ms_p50_gauge,
    worker_lag_ms_p95_gauge,
    worker_lag_ms_p99_gauge,
)
from services.orderflow.components.tick_time_policy import _msgid_to_ms
from services.orderflow.configuration import _safe_int
from services.orderflow.side_policy import deterministic_sample
from utils.time_utils import get_epoch_ms as get_ny_time_millis

class TickMetricsHandler:
    def __init__(self, lag_trackers: dict, lag_export_counters: dict):
        self._lag_trackers = lag_trackers
        self._lag_export_counters = lag_export_counters
        self._ingest_latency_sample: float | None = None

    def update_lag(self, symbol: str, now_ms: int, event_ts_ms: int, max_ms: int, msg_id: str = "") -> int:
        lag_ms = 0
        try:
            lag_ms = max(0, int(now_ms - event_ts_ms))
            if worker_lag_ms_gauge:
                worker_lag_ms_gauge.labels(symbol=symbol).set(float(lag_ms))
            if worker_lag_ms_hist:
                with contextlib.suppress(Exception):
                    worker_lag_ms_hist.labels(symbol=symbol).observe(lag_ms)

            tracker = self._lag_trackers.get(symbol)
            if tracker:
                pel_cap = int(os.getenv("PEL_LAG_OUTLIER_CAP_MS", "10000"))
                clamped = min(lag_ms, max_ms)
                if clamped <= pel_cap:
                    tracker.update(clamped)
                ctr = self._lag_export_counters.get(symbol, 0) + 1
                self._lag_export_counters[symbol] = ctr
                if ctr % 200 == 0:
                    snap = tracker.snapshot()
                    if snap:
                        try:
                            worker_lag_ms_p50_gauge.labels(symbol=symbol).set(snap.p50)
                            worker_lag_ms_p95_gauge.labels(symbol=symbol).set(snap.p95)
                            worker_lag_ms_p99_gauge.labels(symbol=symbol).set(snap.p99)
                        except Exception:
                            pass

            if msg_id:
                redis_ms = _msgid_to_ms(str(msg_id))
                if redis_ms > 0:
                    r_lag = max(0, now_ms - redis_ms)
                    try:
                        redis_entry_lag_ms_gauge.labels(symbol=symbol).set(float(r_lag))
                        redis_entry_lag_ms_hist.labels(symbol=symbol).observe(float(r_lag))
                    except Exception:
                        pass
                    
                    r_tracker = self._lag_trackers.get(f"_redis_{symbol}")
                    if r_tracker is None:
                        from common.metrics2 import LagTracker
                        r_tracker = LagTracker(max_ms=max_ms)
                        self._lag_trackers[f"_redis_{symbol}"] = r_tracker
                    r_tracker.update(min(r_lag, max_ms))
                    ctr = self._lag_export_counters.get(symbol, 0)
                    if ctr % 200 == 0:
                        rsnap = r_tracker.snapshot()
                        if rsnap:
                            try:
                                redis_entry_lag_ms_p50_gauge.labels(symbol=symbol).set(rsnap.p50)
                                redis_entry_lag_ms_p99_gauge.labels(symbol=symbol).set(rsnap.p99)
                            except Exception:
                                pass

                    if event_ts_ms and event_ts_ms > 0:
                        m_lag = max(0, redis_ms - event_ts_ms)
                        try:
                            market_inactivity_lag_ms_gauge.labels(symbol=symbol).set(float(m_lag))
                            market_inactivity_lag_ms_hist.labels(symbol=symbol).observe(float(m_lag))
                        except Exception:
                            pass
        except Exception:
            lag_ms = 0
        return lag_ms

    def observe_latency(
        self,
        processed_ok: bool,
        tick: dict | None,
        msg_id: str,
        symbol: str,
        t0: float,
    ) -> None:
        try:
            if not (processed_ok and tick):
                return
            if self._ingest_latency_sample is None:
                self._ingest_latency_sample = float(os.getenv("TICK_INGEST_LATENCY_SAMPLE", "0.02"))
            key_ms = _safe_int(tick.get("event_ts_ms") or tick.get("ts_ms") or 0)
            if not key_ms:
                try:
                    key_ms = int(str(msg_id).split("-")[0])
                except Exception:
                    key_ms = get_ny_time_millis()
            if not deterministic_sample(key_ms, float(self._ingest_latency_sample)):
                return

            try:
                from services.orderflow.metric_labels import symbol_label as _sl
                sym_lbl = _sl(symbol)
            except Exception:
                sym_lbl = symbol

            dt_ms = (_time.perf_counter() - t0) * 1000.0
            tick_ingest_process_ms.labels(symbol=sym_lbl).observe(float(dt_ms))

            ev = _safe_int(tick.get("event_ts_ms") or tick.get("ts_ms") or 0)
            if ev:
                e2e = _safe_int(tick.get("ingest_ts_ms") or 0) - ev
                if e2e >= 0:
                    tick_ingest_e2e_delay_ms.labels(symbol=sym_lbl).observe(float(e2e))
        except Exception:
            pass
