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

import json
import logging
import os
import time as _time
from typing import Any, Callable, Dict, Optional, Tuple

import redis.asyncio as aioredis

from common.metrics2 import LagTracker
from core.dq_policy import TickDQPolicy
from services.observability.latency_contract import stamp_feature_ready, observe_feature_ready_async
from services.orderflow.metrics import (
    ticks_read_total, ticks_processed_total, ticks_dropped_total,
    tick_dedup_drop_total, ticks_unknown_side_policy_total,
    ticks_unknown_side_quarantine_published_total,
    ticks_ts_source_total,
    tick_unknown_side_ema_gauge, tick_ts_source_now_ema_gauge,
    tick_ts_source_stream_id_ema_gauge,
    tick_event_stream_skew_abs_ema_ms_gauge, tick_event_age_abs_ema_ms_gauge,
    worker_lag_ms_gauge, worker_lag_ms_p50_gauge, worker_lag_ms_p95_gauge,
    worker_lag_ms_p99_gauge, worker_lag_ms_hist, processing_time_us,
    signals_published_total, tick_ingest_process_ms, tick_ingest_e2e_delay_ms,
    redis_entry_lag_ms_gauge, redis_entry_lag_ms_p50_gauge,
    redis_entry_lag_ms_p99_gauge, redis_entry_lag_ms_hist,
    market_inactivity_lag_ms_gauge, market_inactivity_lag_ms_hist,
)
from services.orderflow.metric_labels import TickMetricLimiter, _parse_allowlist, should_emit
from services.orderflow.side_policy import (
    is_unknown_side_tick, normalize_unknown_side_policy, deterministic_sample,
)
from services.orderflow.tick_quality_ema import TickQualityEMA
from services.orderflow.utils import _fields_to_dict, _parse_tick_payload, _compute_tick_uid
from services.orderflow.configuration import _safe_int
from services.signal_preprocess import preprocess_signal_for_publish
from utils.task_manager import safe_create_task
from utils.time_utils import get_epoch_ms as get_ny_time_millis

logger = logging.getLogger("tick_processor")


# ── Pure helpers ──────────────────────────────────────────────────────────────

def _msgid_to_ms(msg_id: str) -> int:
    try:
        return int(str(msg_id).split("-", 1)[0])
    except Exception:
        return 0


def coerce_event_ts_ms(
    *,
    msg_id: str,
    payload_ts_ms: int,
    now_ms: int,
    max_ts_skew_ms: int,
) -> Tuple[int, str]:
    """Детерминированный выбор event_time:
    1) tick.ts_ms если в пределах max_ts_skew_ms от wall-clock
    2) Redis stream-id ms
    3) wall-clock (last resort)
    """
    ts = _safe_int(payload_ts_ms or 0)
    if ts > 0 and abs(int(now_ms) - ts) <= int(max_ts_skew_ms):
        return ts, "payload"
    mid = _msgid_to_ms(msg_id)
    if mid > 0:
        return mid, "stream_id"
    return int(now_ms), "now"


# ── TickProcessor ─────────────────────────────────────────────────────────────

class TickProcessor:
    """Обрабатывает один тик: parse → validate → enrich → strategy → publish.

    Создаётся один раз в CryptoOrderflowService.__init__ и переиспользуется
    для всех тиков всех символов.
    """

    def __init__(
        self,
        *,
        tick_dq_policy: TickDQPolicy,
        strategy_fn: Callable[[], Optional[Any]],
        gate: Any,                          # SignalGate
        flusher: Any,                       # BurstFlusher
        health_metrics: Optional[Any],
        main_redis: aioredis.Redis,
        ticks_redis: aioredis.Redis,
        # tick config (из TickCfg)
        drop_on_lag: bool,
        max_lag_ms: int,
        max_ts_skew_ms: int,
        unknown_side_policy: str,
        unknown_side_quarantine_stream: str,
        unknown_side_quarantine_sample: float,
        unknown_side_quarantine_maxlen: int,
        exec_quarantine_enable: bool,
        quarantine_stream: str,
        # shared mutable state (dict-refs из сервиса)
        lag_trackers: Dict[str, LagTracker],
        lag_export_counters: Dict[str, int],
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
        self._side_policy = normalize_unknown_side_policy(unknown_side_policy)
        self._side_quarantine_stream = unknown_side_quarantine_stream
        self._side_quarantine_sample = unknown_side_quarantine_sample
        self._side_quarantine_maxlen = unknown_side_quarantine_maxlen
        self._exec_quarantine_enable = exec_quarantine_enable
        self._quarantine_stream = quarantine_stream

        self._lag_trackers = lag_trackers
        self._lag_export_counters = lag_export_counters

        # Lazy-init per first tick
        self._quality_ema: Optional[TickQualityEMA] = None
        self._metric_limiter: Optional[TickMetricLimiter] = None
        self._metric_last_emit_ms: Dict[str, int] = {}
        self._ingest_latency_sample: Optional[float] = None

    @classmethod
    def from_service(cls, svc: Any) -> "TickProcessor":
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

    # ── Public ────────────────────────────────────────────────────────────────

    async def process_tick(
        self,
        runtime: Any,
        msg_id: str,
        fields: Any,
        symbol: str,
        *,
        lag_tracker_max_ms: int = 60_000,
    ) -> bool:
        """Полный цикл обработки одного тика. Возвращает True если ACK следует отправить."""
        _t0 = _time.perf_counter()
        ticks_read_total.labels(symbol=symbol).inc()

        tick: Optional[Dict] = None
        processed_ok = False

        try:
            raw = _fields_to_dict(fields)
            tick = _parse_tick_payload(raw)
            if not tick:
                return True  # пустой тик → ACK без обработки

            ingest_ts_ms = get_ny_time_millis()
            tick["ingest_ts_ms"] = ingest_ts_ms
            tick["ts_redis_read_ms"] = ingest_ts_ms

            # ── Timestamp resolution (BEFORE DQ!) ────────────────────────────
            # P0-fix: DQ must validate the *resolved* event_ts_ms, not the raw payload ts.
            # Rescue path: stream_id provides a valid fallback for stale/missing payload ts.
            now_ms = ingest_ts_ms
            payload_ts_ms = _safe_int(tick.get("ts_ms") or tick.get("event_ts_ms") or 0)
            event_ts_ms, ts_source = coerce_event_ts_ms(
                msg_id=msg_id,
                payload_ts_ms=payload_ts_ms,
                now_ms=now_ms,
                max_ts_skew_ms=self._max_ts_skew_ms,
            )
            # exchange_ts_ms is immutable — always reflects the original exchange timestamp.
            # redis_stream_ts_ms is the ingestion timestamp from the Redis stream entry id.
            # Downstream consumers must use exchange_ts_ms for trading decisions, NOT
            # event_ts_ms when ts_source != "payload" (i.e. stream_id / wall-clock fallback).
            tick["exchange_ts_ms"] = int(payload_ts_ms)
            tick["redis_stream_ts_ms"] = _msgid_to_ms(str(msg_id)) if msg_id else 0
            tick["event_ts_ms"] = int(event_ts_ms)
            tick["ts_ms"] = int(event_ts_ms)
            tick["ts_source"] = str(ts_source)
            tick["payload_ts_ms"] = int(payload_ts_ms)  # preserve original for DQ quarantine
            if payload_ts_ms <= 0:
                tick["dq_tradeable"] = False
                tick["dq_reason"] = "missing_exchange_ts"

            try:
                ticks_ts_source_total.labels(symbol=str(symbol), ts_source=str(ts_source)).inc()
            except Exception:
                pass

            # ── DQ validate (on resolved event_ts_ms) ────────────────────────
            # DQ still rejects bad_ts_unit / missing_symbol but stale/future/OOO
            # are evaluated against the resolved event_ts_ms (which may come from stream_id).
            # If ts_source == "stream_id" or "now", the payload_ts_ms issue is already
            # captured in ts_source metric — DQ may still pass.
            is_valid, dq_reason = self._dq_policy.validate(tick, ingest_ts_ms)
            if not is_valid:
                # Only quarantine hard-bad payloads where ts_source==payload (i.e. rescue didn't help)
                # If ts_source is stream_id/now, the tick already passed resolution and DQ drop
                # is for other reasons (missing_symbol, bad_ts_unit in raw payload).
                payload_ts_was_bad = (ts_source != "payload")
                try:
                    reason_label = f"{dq_reason}" if not payload_ts_was_bad else f"{dq_reason}.raw"
                    ticks_dropped_total.labels(symbol=symbol, reason=reason_label).inc()
                except Exception:
                    pass
                if self._exec_quarantine_enable:
                    self._xadd_dq_quarantine(tick, dq_reason)
                return True

            # ── Unknown-side detection ───────────────────────────────────────
            try:
                unknown_side = bool(is_unknown_side_tick(tick))
            except Exception:
                unknown_side = False

            # ── Tick quality EMA ─────────────────────────────────────────────
            self._update_quality_ema(symbol, tick, now_ms, unknown_side, ts_source, event_ts_ms, raw)

            # ── process_ts_ms ────────────────────────────────────────────────
            tick["process_ts_ms"] = get_ny_time_millis()
            runtime.last_ts_ms = int(event_ts_ms)

            # ── Lag tracking ─────────────────────────────────────────────────────
            lag_ms = self._update_lag(symbol, now_ms, event_ts_ms, lag_tracker_max_ms, msg_id=msg_id)

            # ── Lag drop ─────────────────────────────────────────────────────
            if self._drop_on_lag and lag_ms > self._max_lag_ms:
                try:
                    ticks_dropped_total.labels(symbol=symbol, reason="lag").inc()
                except Exception:
                    pass
                return True

            # ── Dedup (P1-fix: pass msg_id as stream_id for fallback UID) ────
            if self._is_duplicate(tick, runtime, symbol, raw, msg_id=msg_id):
                return True

            # ── Unknown-side policy ──────────────────────────────────────────
            skip = await self._apply_side_policy(tick, unknown_side, symbol, msg_id, raw)
            if skip:
                return True

            ticks_processed_total.labels(symbol=symbol).inc()

            # ── Health metrics ───────────────────────────────────────────────
            self._update_health_metrics(runtime, symbol, event_ts_ms, now_ms)

            # ── Strategy ─────────────────────────────────────────────────────
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
                    self._health_metrics.on_signal_emit(symbol=str(symbol))
            except Exception:
                pass

            # ── Burst flush (если strategy не дала сигнал) ───────────────────
            if not signal:
                burst = await self._flusher.process(runtime, "tick", int(event_ts_ms), do_publish=False)
                if burst:
                    signal = burst

            # ── Publish ──────────────────────────────────────────────────────
            if signal and strat:
                await self._publish_signal(runtime, signal, tick, symbol, strat)

            processed_ok = True

        except Exception as exc:
            import sys, traceback as _tb
            sys.stderr.write(f"❌ DIRECT stderr: {symbol} tick {msg_id}:\n{_tb.format_exc()}\n")
            logger.error("❌ (%s) Crash processing tick %s: %s", symbol, msg_id, exc)
            processed_ok = await self._quarantine_poison(symbol, msg_id, fields, exc)

        finally:
            self._observe_latency(processed_ok, tick, msg_id, symbol, _t0)

        return processed_ok

    # ── Private: pipeline stages ──────────────────────────────────────────────

    def _xadd_dq_quarantine(self, tick: Dict, reason: str) -> None:
        q_stream = "stream:tick_dq:quarantine"
        try:
            # FIX P1: Serialize synchronously BEFORE event loop takes over
            tick_payload = json.dumps(tick)
            async def _xadd():
                try:
                    await self._main.xadd(
                        q_stream,
                        {"data": tick_payload, "reason": reason},
                        maxlen=20_000,
                        approximate=True,
                    )
                except Exception:
                    pass
            safe_create_task(_xadd())
        except Exception:
            pass

    def _update_quality_ema(
        self,
        symbol: str,
        tick: Dict,
        now_ms: int,
        unknown_side: bool,
        ts_source: str,
        event_ts_ms: int,
        raw: Dict,
    ) -> None:
        try:
            if self._quality_ema is None:
                tau_ms = int(os.getenv("TICK_QUALITY_EMA_TAU_MS", "300000"))
                self._quality_ema = TickQualityEMA(tau_ms=tau_ms)

            stream_ms = _safe_int(tick.get("stream_ms") or 0)
            abs_skew = abs(int(event_ts_ms) - stream_ms) if stream_ms and event_ts_ms else 0
            abs_age = abs(int(now_ms) - int(event_ts_ms)) if event_ts_ms else 0

            ema = self._quality_ema.update(
                symbol=str(symbol),
                ts_ms=int(now_ms),
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

            sym_lbl = self._metric_limiter.label(str(symbol))
            if sym_lbl is not None:
                last = self._metric_last_emit_ms.get(sym_lbl, 0)
                if should_emit(int(now_ms), last, int(self._metric_limiter.ema_min_update_ms)):
                    self._metric_last_emit_ms[sym_lbl] = int(now_ms)
                    tick_unknown_side_ema_gauge.labels(symbol=sym_lbl).set(float(ema["unknown"]))
                    tick_ts_source_now_ema_gauge.labels(symbol=sym_lbl).set(float(ema["ts_now"]))
                    tick_ts_source_stream_id_ema_gauge.labels(symbol=sym_lbl).set(float(ema["ts_stream_id"]))
                    tick_event_stream_skew_abs_ema_ms_gauge.labels(symbol=sym_lbl).set(float(ema["skew_abs_ms"]))
                    tick_event_age_abs_ema_ms_gauge.labels(symbol=sym_lbl).set(float(ema["age_abs_ms"]))
        except Exception:
            pass

    def _update_lag(self, symbol: str, now_ms: int, event_ts_ms: int, max_ms: int, msg_id: str = "") -> int:
        lag_ms = 0
        try:
            lag_ms = max(0, int(now_ms - int(event_ts_ms)))
            if worker_lag_ms_gauge:
                worker_lag_ms_gauge.labels(symbol=symbol).set(float(lag_ms))
            if worker_lag_ms_hist:
                try:
                    worker_lag_ms_hist.labels(symbol=symbol).observe(lag_ms)
                except Exception:
                    pass

            tracker = self._lag_trackers.get(symbol)
            if tracker:
                # Outlier cap: don't feed PEL-reclaimed ancient ticks into percentile tracker.
                # Values above cap (default 10s) are likely stale PEL entries, not real lag.
                # They're still recorded in the instant gauge above for debugging.
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

            # ── redis_entry_lag: time from Redis XADD (msg_id ms) to Python processing ──
            # Excludes Binance network RTT (~80ms). Measures Python-side scheduling only.
            # Expected: P50 ~5ms, P99 ~25ms.
            if msg_id:
                redis_ms = _msgid_to_ms(str(msg_id))
                if redis_ms > 0:
                    r_lag = max(0, int(now_ms) - redis_ms)
                    try:
                        redis_entry_lag_ms_gauge.labels(symbol=symbol).set(float(r_lag))
                        redis_entry_lag_ms_hist.labels(symbol=symbol).observe(float(r_lag))
                    except Exception:
                        pass
                    # Export P50/P99 every 200 ticks same as worker_lag
                    r_tracker = self._lag_trackers.get(f"_redis_{symbol}")
                    if r_tracker is None:
                        from common.metrics2 import LagTracker
                        r_tracker = LagTracker(max_ms=max_ms)
                        self._lag_trackers[f"_redis_{symbol}"] = r_tracker
                    r_tracker.update(min(r_lag, max_ms))
                    ctr = self._lag_export_counters.get(symbol, 0)  # reuse same counter
                    if ctr % 200 == 0:
                        rsnap = r_tracker.snapshot()
                        if rsnap:
                            try:
                                redis_entry_lag_ms_p50_gauge.labels(symbol=symbol).set(rsnap.p50)
                                redis_entry_lag_ms_p99_gauge.labels(symbol=symbol).set(rsnap.p99)
                            except Exception:
                                pass

                    # ── market_inactivity_lag: tick_event → Redis XADD (uncontrollable) ──
                    # = redis_xadd_ts - tick_event_ts = Binance tick gap + Go-ingest RTT.
                    # When this is high (e.g. 600ms for BTC), worker_lag_p99 is high
                    # due to market inactivity — NOT event loop blockage.
                    # Formula: worker_lag = market_inactivity_lag + redis_entry_lag + processing.
                    if event_ts_ms and event_ts_ms > 0:
                        m_lag = max(0, redis_ms - int(event_ts_ms))
                        try:
                            market_inactivity_lag_ms_gauge.labels(symbol=symbol).set(float(m_lag))
                            market_inactivity_lag_ms_hist.labels(symbol=symbol).observe(float(m_lag))
                        except Exception:
                            pass
        except Exception:
            lag_ms = 0
        return lag_ms


    def _is_duplicate(self, tick: Dict, runtime: Any, symbol: str, raw: Dict, *, msg_id: str = "") -> bool:
        """Market-level dedup: trade_id > content-hash(exchange_ts_ms|price|qty|side|bm).

        stream_id (Redis msg_id) is intentionally excluded from the economic dedup UID —
        a re-XADDed tick with a new stream_id but identical exchange payload must still
        be detected as a duplicate. stream_id is only relevant for ACK/PEL bookkeeping.
        """
        try:
            uid = str(tick.get("tick_uid") or "")
            if uid.startswith(tick.get("symbol", symbol).upper() + ":h") or not uid:
                # Use exchange_ts_ms (immutable) for the content hash so that the UID is
                # stable across re-XADDs (same exchange payload → same hash regardless of
                # which Redis stream entry carried it).
                exchange_ts = _safe_int(tick.get("exchange_ts_ms") or tick.get("payload_ts_ms") or tick.get("ts_ms") or 0)
                uid = _compute_tick_uid(
                    symbol=str(tick.get("symbol") or symbol),
                    trade_id=tick.get("trade_id"),
                    ts_ms=exchange_ts,
                    price_src=raw.get("price") or raw.get("last") or raw.get("mid"),
                    qty_src=raw.get("qty") or raw.get("volume"),
                    side=str(tick.get("side") or ""),
                    is_buyer_maker=tick.get("is_buyer_maker"),
                    stream_id=None,  # excluded from market-level dedupe UID
                )
                tick["tick_uid"] = uid
            if uid and runtime.is_duplicate_tick_uid(uid):
                try:
                    tick_dedup_drop_total.labels(symbol=symbol).inc()
                except Exception:
                    pass
                return True
        except Exception:
            pass
        return False

    async def _apply_side_policy(
        self, tick: Dict, unknown_side: bool, symbol: str, msg_id: str, raw: Dict
    ) -> bool:
        """Returns True if tick должен быть пропущен (drop/quarantine)."""
        if not unknown_side:
            return False
        try:
            ticks_unknown_side_policy_total.labels(symbol=str(symbol), policy=str(self._side_policy)).inc()
        except Exception:
            pass

        pol = str(self._side_policy or "ignore_delta")
        if pol in ("drop", "quarantine"):
            try:
                ticks_dropped_total.labels(symbol=symbol, reason=f"unknown_side_{pol}").inc()
            except Exception:
                pass
            if pol == "quarantine":
                await self._quarantine_unknown_side(symbol, msg_id, tick, raw)
            return True

        if pol == "ignore_delta":
            # P1-fix: set canonical downstream contract fields so consumers never need
            # to re-interpret side=UNKNOWN; they rely solely on aggressor_sign + counted_in_delta.
            try:
                tick["qty_signed"] = 0.0
                tick["aggressor_sign"] = 0
                tick["counted_in_delta"] = False
                tick["side"] = "UNKNOWN"
                tick["side_reason"] = "unknown"
            except Exception:
                pass
        return False

    async def _quarantine_unknown_side(
        self, symbol: str, msg_id: str, tick: Dict, raw_fields: Dict
    ) -> None:
        try:
            if not self._ticks:
                return
            key_ms = _safe_int(tick.get("event_ts_ms") or tick.get("ts_ms") or 0)
            if not deterministic_sample(int(key_ms), float(self._side_quarantine_sample)):
                return
            payload = {
                "symbol": str(symbol),
                "reason": "unknown_side",
                "policy": str(self._side_policy),
                "msg_id": str(msg_id),
                "tick_uid": str(tick.get("tick_uid") or ""),
                "event_ts_ms": str(_safe_int(tick.get("event_ts_ms") or 0)),
                "ts_source": str(tick.get("ts_source") or ""),
                "side": str(tick.get("side") or ""),
                "side_conf": str(tick.get("side_conf") or ""),
                "side_raw": str(tick.get("side_raw") or ""),
                "is_buyer_maker": str(tick.get("is_buyer_maker") if tick.get("is_buyer_maker") is not None else ""),
                "trade_id": str(tick.get("trade_id") or ""),
                "price": str(tick.get("price") or ""),
                "qty": str(tick.get("qty") or tick.get("volume") or ""),
            },
            try:
                payload["raw_keys"] = ",".join(sorted(list(raw_fields.keys()))[:32])
            except Exception:
                pass
            await self._ticks.xadd(
                self._side_quarantine_stream,
                payload,
                maxlen=int(self._side_quarantine_maxlen),
                approximate=True,
            )
            try:
                ticks_unknown_side_quarantine_published_total.labels(
                    symbol=str(symbol), reason="unknown_side"
                ).inc()
            except Exception:
                pass
        except Exception:
            pass

    def _update_health_metrics(
        self, runtime: Any, symbol: str, event_ts_ms: int, now_ms: int
    ) -> None:
        try:
            if not self._health_metrics:
                return
            book = runtime.get_book_snapshot()
            if book and book.timestamp_ms:
                age = max(0, event_ts_ms - book.timestamp_ms)
                self._health_metrics.on_tick(
                    symbol=str(symbol),
                    l2_age_ms=float(age),
                    l2_age_ms_tick=float(age),
                    l2_is_stale=(age > 1500),
                    l2_is_stale_now=(max(0, now_ms - book.timestamp_ms) > 1500),
                )
        except Exception:
            pass

    async def _publish_signal(
        self, runtime: Any, signal: Dict, tick: Dict, symbol: str, strat: Any
    ) -> None:
        try:
            stamp_feature_ready(signal, tick=tick, now_ms=get_ny_time_millis())
            await observe_feature_ready_async(
                signal,
                redis_client=self._main,
                service="python_worker",
                symbol=str(symbol),
            )
        except Exception as exc:
            logger.debug("(%s) latency contract feature-ready failed: %s", symbol, exc)
        try:
            preprocess_signal_for_publish(
                signal,
                symbol=str(getattr(runtime, "symbol", "") or symbol),
                source="CryptoOrderFlow",
                logger=logger,
                fast_path=False,
            )
        except Exception:
            pass
        if strat and await self._gate.allows(runtime, signal):
            await strat.publish_signal(runtime, signal)
            try:
                signals_published_total.labels(symbol=symbol).inc()
            except Exception:
                pass

    async def _quarantine_poison(
        self, symbol: str, msg_id: str, fields: Any, exc: Exception
    ) -> bool:
        try:
            await self._ticks.xadd(
                self._quarantine_stream,
                {
                    "symbol": symbol,
                    "msg_id": str(msg_id),
                    "error": str(exc)[:200],
                    "payload": json.dumps(fields, default=str)[:1000],
                },
                maxlen=5000,
            )
            logger.warning("☣️ (%s) Message %s quarantined", symbol, msg_id)
            return True
        except Exception as q_err:
            logger.error("Critical: Failed to quarantine: %s", q_err)
            return False

    def _observe_latency(
        self,
        processed_ok: bool,
        tick: Optional[Dict],
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
                sym_lbl = str(symbol)

            dt_ms = (_time.perf_counter() - t0) * 1000.0
            tick_ingest_process_ms.labels(symbol=sym_lbl).observe(float(dt_ms))

            ev = _safe_int(tick.get("event_ts_ms") or tick.get("ts_ms") or 0)
            if ev:
                e2e = _safe_int(tick.get("ingest_ts_ms") or 0) - ev
                if e2e >= 0:
                    tick_ingest_e2e_delay_ms.labels(symbol=sym_lbl).observe(float(e2e))
        except Exception:
            pass
