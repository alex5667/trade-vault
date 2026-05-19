from __future__ import annotations

"""
TickProcessor — обрабатывает один тик: parse → DQ → timestamp → lag → dedup →
side-policy → strategy → burst → publish → latency histogram.

Ранее вся эта логика (~350 строк) жила внутри consume_ticks одной God-функции.
Теперь каждый этап — отдельный метод; зависимости инжектируются в конструктор.

Публичный API:
    processed_ok = await proc.process_tick(runtime, msg_id, fields, symbol,
                                           lag_tracker_max_ms=60_000)
"""

import contextlib
import logging
import time as _time
from collections.abc import Callable
from typing import Any

import redis.asyncio as aioredis

from common.metrics2 import LagTracker
from core.dq_policy import TickDQPolicy
from services.observability.latency_contract import observe_feature_ready_async, stamp_feature_ready
from services.orderflow.configuration import _safe_int
from services.orderflow.metrics import (
    processing_time_us,
    signals_published_total,
    tick_ts_missing_total,
    ticks_dropped_total,
    ticks_processed_total,
    ticks_read_total,
    ticks_ts_source_total,
)
from services.orderflow.utils import _fields_to_dict, _parse_tick_payload
from services.signal_preprocess import preprocess_signal_for_publish
from utils.time_utils import get_epoch_ms as get_ny_time_millis

# --- New Extracted Components ---
from services.orderflow.components.tick_time_policy import _msgid_to_ms, coerce_event_ts_ms
from services.orderflow.components.tick_deduper import is_duplicate_tick
from services.orderflow.components.unknown_side_policy import UnknownSidePolicyHandler
from services.orderflow.components.tick_quarantine_writer import TickQuarantineWriter
from services.orderflow.components.tick_feature_updater import TickFeatureUpdater
from services.orderflow.components.tick_metrics import TickMetricsHandler

logger = logging.getLogger("tick_processor")

class TickProcessor:
    """Обрабатывает один тик: parse → validate → enrich → strategy → publish.

    Создаётся один раз в CryptoOrderflowService.__init__ и переиспользуется
    для всех тиков всех символов.
    """

    def __init__(
        self,
        *,
        tick_dq_policy: TickDQPolicy,
        strategy_fn: Callable[[], Any | None],
        gate: Any,                          # SignalGate
        flusher: Any,                       # BurstFlusher
        health_metrics: Any | None,
        main_redis: aioredis.Redis,
        ticks_redis: aioredis.Redis,
        drop_on_lag: bool,
        max_lag_ms: int,
        max_ts_skew_ms: int,
        unknown_side_policy: str,
        unknown_side_quarantine_stream: str,
        unknown_side_quarantine_sample: float,
        unknown_side_quarantine_maxlen: int,
        exec_quarantine_enable: bool,
        quarantine_stream: str,
        lag_trackers: dict[str, LagTracker],
        lag_export_counters: dict[str, int],
    ) -> None:
        self._dq_policy = tick_dq_policy
        self._strategy_fn = strategy_fn
        self._gate = gate
        self._flusher = flusher
        self._health_metrics = health_metrics
        self._main = main_redis
        self._ticks = ticks_redis
        
        self._drop_on_lag = drop_on_lag
        self._max_lag_ms = max_lag_ms
        self._max_ts_skew_ms = max_ts_skew_ms
        self._exec_quarantine_enable = exec_quarantine_enable
        
        # Initialize components
        self._quarantine_writer = TickQuarantineWriter(
            main_redis=main_redis,
            ticks_redis=ticks_redis,
            unknown_side_quarantine_stream=unknown_side_quarantine_stream,
            unknown_side_quarantine_sample=unknown_side_quarantine_sample,
            unknown_side_quarantine_maxlen=unknown_side_quarantine_maxlen,
            quarantine_stream=quarantine_stream,
            side_policy=unknown_side_policy,
        )
        self._unknown_side_policy_handler = UnknownSidePolicyHandler(
            side_policy=unknown_side_policy,
            quarantine_writer=self._quarantine_writer,
        )
        self._feature_updater = TickFeatureUpdater()
        self._metrics_handler = TickMetricsHandler(
            lag_trackers=lag_trackers, 
            lag_export_counters=lag_export_counters
        )

    @classmethod
    def from_service(cls, svc: Any) -> TickProcessor:
        """Фабрика: строит TickProcessor из атрибутов CryptoOrderflowService."""
        cfg = svc._svc_cfg.tick
        return cls(
            tick_dq_policy=svc.tick_dq_policy,
            strategy_fn=lambda: svc.strategy,
            gate=svc._gate,
            flusher=svc._flusher,
            health_metrics=getattr(svc, "health_metrics", None),
            main_redis=svc.main,
            ticks_redis=svc.ticks,
            drop_on_lag=cfg.drop_on_lag,
            max_lag_ms=cfg.max_lag_ms,
            max_ts_skew_ms=cfg.max_ts_skew_ms,
            unknown_side_policy=cfg.unknown_side_policy,
            unknown_side_quarantine_stream=cfg.unknown_side_quarantine_stream,
            unknown_side_quarantine_sample=cfg.unknown_side_quarantine_sample,
            unknown_side_quarantine_maxlen=cfg.unknown_side_quarantine_maxlen,
            exec_quarantine_enable=svc.exec_quarantine_denylist_enable,
            quarantine_stream=svc.quarantine_stream,
            lag_trackers=svc._lag_trackers,
            lag_export_counters=svc._lag_export_counters,
        )

    async def process_tick(
        self,
        runtime: Any,
        msg_id: str,
        fields: Any,
        symbol: str,
        *,
        lag_tracker_max_ms: int = 60_000,
    ) -> bool:
        _t0 = _time.perf_counter()
        ticks_read_total.labels(symbol=symbol).inc()

        tick: dict | None = None
        processed_ok = False

        try:
            raw = _fields_to_dict(fields)
            tick = _parse_tick_payload(raw)
            if not tick:
                return True

            ingest_ts_ms = get_ny_time_millis()
            tick["ingest_ts_ms"] = ingest_ts_ms
            tick["ts_redis_read_ms"] = ingest_ts_ms

            now_ms = ingest_ts_ms
            payload_ts_ms = _safe_int(tick.get("ts_ms") or tick.get("event_ts_ms") or 0)
            event_ts_ms, ts_source = coerce_event_ts_ms(
                msg_id=msg_id,
                payload_ts_ms=payload_ts_ms,
                now_ms=now_ms,
                max_ts_skew_ms=self._max_ts_skew_ms,
            )

            tick["exchange_ts_ms"] = int(payload_ts_ms)
            tick["redis_stream_ts_ms"] = _msgid_to_ms(str(msg_id)) if msg_id else 0
            tick["event_ts_ms"] = event_ts_ms
            tick["ts_ms"] = event_ts_ms
            tick["ts_source"] = str(ts_source)
            tick["payload_ts_ms"] = int(payload_ts_ms)
            if payload_ts_ms <= 0:
                tick["dq_tradeable"] = False
                tick["dq_reason"] = "missing_exchange_ts"

            with contextlib.suppress(Exception):
                ticks_ts_source_total.labels(symbol=symbol, ts_source=str(ts_source)).inc()

            is_valid, dq_reason = self._dq_policy.validate(tick, ingest_ts_ms)
            if not is_valid:
                payload_ts_was_bad = (ts_source != "payload")
                try:
                    reason_label = f"{dq_reason}" if not payload_ts_was_bad else f"{dq_reason}.raw"
                    ticks_dropped_total.labels(symbol=symbol, reason=reason_label).inc()
                except Exception:
                    pass
                # G0 spec metric: bad_ts / bad_ts_unit count as missing timestamp.
                if dq_reason in ("bad_ts", "bad_ts_unit"):
                    with contextlib.suppress(Exception):
                        tick_ts_missing_total.labels(symbol=symbol).inc()
                if self._exec_quarantine_enable:
                    self._quarantine_writer.xadd_dq_quarantine(tick, dq_reason)
                return True

            try:
                from services.orderflow.side_policy import is_unknown_side_tick
                unknown_side = bool(is_unknown_side_tick(tick))
            except Exception:
                unknown_side = False

            self._feature_updater.update_quality_ema(
                symbol, tick, now_ms, unknown_side, ts_source, event_ts_ms, raw
            )

            tick["process_ts_ms"] = get_ny_time_millis()
            # runtime.last_ts_ms is owned by G0 inside strategy.process_tick — do not pre-write here,
            # otherwise G0 monotonicity/clamp/quarantine branch becomes unreachable.

            lag_ms = self._metrics_handler.update_lag(
                symbol, now_ms, event_ts_ms, lag_tracker_max_ms, msg_id=msg_id
            )

            if self._drop_on_lag and lag_ms > self._max_lag_ms:
                with contextlib.suppress(Exception):
                    ticks_dropped_total.labels(symbol=symbol, reason="lag").inc()
                return True

            if is_duplicate_tick(tick, runtime, symbol, raw, msg_id=msg_id):
                return True

            skip = await self._unknown_side_policy_handler.apply_policy(
                tick, unknown_side, symbol, msg_id, raw
            )
            if skip:
                return True

            ticks_processed_total.labels(symbol=symbol).inc()

            self._feature_updater.update_health_metrics(
                self._health_metrics, runtime, symbol, event_ts_ms, now_ms
            )

            strat = self._strategy_fn()
            if strat:
                t0_ns = _time.perf_counter_ns()
                signal = await strat.process_tick(runtime, tick, worker_lag_ms=float(lag_ms))
                try:
                    dt_us = (_time.perf_counter_ns() - t0_ns) / 1000.0
                    processing_time_us.labels(symbol=symbol).observe(float(dt_us))
                except Exception:
                    pass
            else:
                signal = None

            try:
                if signal and self._health_metrics:
                    self._health_metrics.on_signal_emit(symbol=symbol)
            except Exception:
                pass

            if not signal:
                burst = await self._flusher.process(runtime, "tick", event_ts_ms, do_publish=False)
                if burst:
                    signal = burst

            if signal and strat:
                await self._publish_signal(runtime, signal, tick, symbol, strat)

            processed_ok = True

        except Exception as exc:
            import sys
            import traceback as _tb
            sys.stderr.write(f"❌ DIRECT stderr: {symbol} tick {msg_id}:\n{_tb.format_exc()}\n")
            logger.error("❌ (%s) Crash processing tick %s: %s", symbol, msg_id, exc)
            processed_ok = await self._quarantine_writer.quarantine_poison(symbol, msg_id, fields, exc)

        finally:
            self._metrics_handler.observe_latency(processed_ok, tick, msg_id, symbol, _t0)

        return processed_ok

    async def _publish_signal(
        self, runtime: Any, signal: dict, tick: dict, symbol: str, strat: Any
    ) -> None:
        try:
            stamp_feature_ready(signal, tick=tick, now_ms=get_ny_time_millis())
            await observe_feature_ready_async(
                signal,
                redis_client=self._main,
                service="python_worker",
                symbol=symbol,
            )
        except Exception as exc:
            logger.debug("(%s) latency contract feature-ready failed: %s", symbol, exc)
        with contextlib.suppress(Exception):
            preprocess_signal_for_publish(
                signal,
                symbol=str(getattr(runtime, "symbol", "") or symbol),
                source="CryptoOrderFlow",
                logger=logger,
                fast_path=False,
            )
        if strat and await self._gate.allows(runtime, signal):
            await strat.publish_signal(runtime, signal)
            with contextlib.suppress(Exception):
                signals_published_total.labels(symbol=symbol).inc()

    async def _inject_liqmap_features(
        self,
        *,
        runtime: Any,
        now_ms: int,
        price: float,
        indicators: dict[str, Any],
    ) -> None:
        """Best-effort LiqMap snapshot -> indicator injection. Fail-open."""
        import asyncio
        from core.liqmap_features_v1 import (
            compute_liqmap_features,
            liqmap_feature_keys,
            make_liqmap_default_features,
            parse_liqmap_snapshot_v1,
        )
        try:
            if not bool(getattr(self, "liqmap_features_enable", False)):
                return

            sym = str(getattr(runtime, "symbol", "") or "").strip().upper()
            if not sym:
                return

            windows = list(getattr(self, "liqmap_features_windows", []) or [])
            if not windows:
                return

            refresh_ms = int(getattr(self, "liqmap_features_refresh_ms", 0) or 0)
            stale_ms = int(getattr(self, "liqmap_features_failopen_stale_ms", 0) or 0)
            prefix = str(getattr(self, "liqmap_snapshot_key_prefix", "liqmap:snapshot") or "liqmap:snapshot")
            near_band_bps = float(getattr(self, "liqmap_near_band_bps", 20.0) or 20.0)
            peak_min_share = float(getattr(self, "liqmap_peak_min_share", 0.05) or 0.05)

            for w in windows:
                wnd = str(w)
                ck = (sym, wnd)
                if not hasattr(self, "_liqmap_cache"):
                    self._liqmap_cache = {}
                if not hasattr(self, "_liqmap_next_refresh_ts_ms"):
                    self._liqmap_next_refresh_ts_ms = {}
                cached = self._liqmap_cache.get(ck)
                next_ts = int(self._liqmap_next_refresh_ts_ms.get(ck, 0) or 0)

                if cached and refresh_ms > 0 and now_ms < next_ts:
                    feats = cached.get("feats") if isinstance(cached, dict) else None
                    if isinstance(feats, dict) and feats:
                        with contextlib.suppress(Exception):
                            snap_ts_ms = int(cached.get("snap_ts_ms", 0) or 0)
                            if snap_ts_ms > 0:
                                feats[f"liqmap_{wnd}_age_ms"] = float(max(0, now_ms - snap_ts_ms))
                        indicators.update(feats)
                    continue

                with contextlib.suppress(Exception):
                    self._liqmap_next_refresh_ts_ms[ck] = now_ms + int(refresh_ms)

                raw = None
                with contextlib.suppress(Exception):
                    raw = await asyncio.wait_for(
                        self.redis.get(f"{prefix}:{sym}:{wnd}"),
                        timeout=0.005,
                    )

                if raw is None:
                    if isinstance(cached, dict):
                        good_ms = int(cached.get("good_ms", 0) or 0)
                        if good_ms > 0 and stale_ms > 0 and (now_ms - good_ms) <= stale_ms:
                            feats = cached.get("feats")
                            if isinstance(feats, dict) and feats:
                                with contextlib.suppress(Exception):
                                    snap_ts_ms = int(cached.get("snap_ts_ms", 0) or 0)
                                    if snap_ts_ms > 0:
                                        feats[f"liqmap_{wnd}_age_ms"] = float(max(0, now_ms - snap_ts_ms))
                                indicators.update(feats)
                                continue
                    defaults = make_liqmap_default_features([wnd])
                    indicators.update(defaults)
                    self._liqmap_cache[ck] = {
                        "fetch_ms": now_ms,
                        "good_ms": int(cached.get("good_ms", 0) or 0) if isinstance(cached, dict) else 0,
                        "snap_ts_ms": int(cached.get("snap_ts_ms", 0) or 0) if isinstance(cached, dict) else 0,
                        "feats": defaults,
                    }
                    continue

                raw_s: str
                try:
                    raw_s = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
                except Exception:
                    raw_s = str(raw)

                try:
                    snap = parse_liqmap_snapshot_v1(raw_s, expected_symbol=sym, expected_window=wnd)
                    feats = compute_liqmap_features(
                        snap,
                        price=float(price),
                        windows=(wnd,),
                        near_band_bps=float(near_band_bps),
                        peak_min_share=float(peak_min_share),
                        now_ms=now_ms,
                    )
                    for k in liqmap_feature_keys(wnd):
                        feats.setdefault(k, 0.0)
                    indicators.update(feats)
                    self._liqmap_cache[ck] = {
                        "fetch_ms": now_ms,
                        "good_ms": now_ms,
                        "snap_ts_ms": int(getattr(snap, "ts_ms", 0) or 0),
                        "feats": feats,
                    }
                except Exception:
                    defaults = make_liqmap_default_features([wnd])
                    indicators.update(defaults)
                    self._liqmap_cache[ck] = {
                        "fetch_ms": now_ms,
                        "good_ms": int(cached.get("good_ms", 0) or 0) if isinstance(cached, dict) else 0,
                        "snap_ts_ms": int(cached.get("snap_ts_ms", 0) or 0) if isinstance(cached, dict) else 0,
                        "feats": defaults,
                    }
        except Exception:
            return
