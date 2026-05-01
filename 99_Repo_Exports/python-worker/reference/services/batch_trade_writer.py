from __future__ import annotations
"""
services/batch_trade_writer.py
──────────────────────────────
Асинхронный batch-writer для trade_closed / trade_closed_p0.

Проблема: TradeMonitorService использует 4-поточный ThreadPoolExecutor и
вставляет каждую сделку отдельным синхронным INSERT. При резком движении
рынка (50-200 закрытий за секунду) очередь насыщается, появляются задержки
в несколько минут.

Решение: один daemon-поток собирает сделки из queue.Queue и делает
единственный batch-INSERT через psycopg2.extras.execute_values каждые
BATCH_WRITER_FLUSH_INTERVAL секунд (или при накоплении BATCH_WRITER_MAX_SIZE
записей).

Rollback: BATCH_WRITER_ENABLED=0 → BatchTradeWriter.enqueue() немедленно
вызывает save_trade_closed() синхронно (старое поведение).

ENV:
  BATCH_WRITER_ENABLED          (default 1)    — включить batch-режим
  BATCH_WRITER_MAX_SIZE         (default 500)  — flush при N записях
  BATCH_WRITER_FLUSH_INTERVAL   (default 1.0)  — flush каждые N секунд
  BATCH_WRITER_MAX_RETRIES      (default 3)    — повторы при ошибке PG
  BATCH_WRITER_QUEUE_MAXSIZE    (default 10000)— предел очереди (backpressure)
"""

import json
import logging
import os
import queue
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg2
    import psycopg2.extras
    from psycopg2.extras import Json
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore
    Json = None  # type: ignore

try:
    from prometheus_client import Counter, Gauge, Histogram

    _TM_DB_BATCH_SIZE = Histogram(
        "tm_db_batch_size",
        "Количество строк в одном batch flush",
        buckets=[1, 5, 10, 50, 100, 200, 500],
    )
    _TM_DB_FLUSH_MS = Histogram(
        "tm_db_flush_duration_ms",
        "Длительность batch flush в мс",
        buckets=[1, 5, 10, 50, 100, 500, 2000],
    )
    _TM_DB_PENDING = Gauge(
        "tm_db_batch_pending",
        "Количество сделок в очереди batch writer",
    )
    _TM_DB_DROPPED = Counter(
        "tm_db_batch_dropped_total",
        "Сделки потерянные из-за переполнения очереди",
    )
    _TM_DB_RETRIES = Counter(
        "tm_db_batch_retries_total",
        "Количество retry-попыток batch flush",
    )
    _TM_DB_FLUSHED = Counter(
        "tm_db_batch_flushed_total",
        "Суммарное количество строк записанных в Postgres через batch",
    )
    _PROM_AVAILABLE = True
except Exception:  # pragma: no cover
    _PROM_AVAILABLE = False

    class _Noop:  # type: ignore
        def labels(self, **kw):
            return self
        def observe(self, v): pass
        def set(self, v): pass
        def inc(self, v=1): pass

    _TM_DB_BATCH_SIZE = _TM_DB_FLUSH_MS = _TM_DB_PENDING = _TM_DB_DROPPED = _TM_DB_RETRIES = _TM_DB_FLUSHED = _Noop()


logger = logging.getLogger("BatchTradeWriter")

# Ленивый импорт analytics_db внутри методов чтобы избежать циклического импорта
# на старте модуля.

_ENV_ENABLED = os.getenv("BATCH_WRITER_ENABLED", "1") == "1"
_ENV_MAX_SIZE = int(os.getenv("BATCH_WRITER_MAX_SIZE", "500"))
_ENV_FLUSH_INTERVAL = float(os.getenv("BATCH_WRITER_FLUSH_INTERVAL", "1.0"))
_ENV_MAX_RETRIES = int(os.getenv("BATCH_WRITER_MAX_RETRIES", "3"))
_ENV_QUEUE_MAXSIZE = int(os.getenv("BATCH_WRITER_QUEUE_MAXSIZE", "10000"))


class BatchTradeWriter:
    """
    Потокобезопасный batch-writer закрытых сделок в Postgres.

    Жизненный цикл:
      writer = BatchTradeWriter()
      writer.start()           # запускает daemon-поток
      writer.enqueue(closed)   # O(1), non-blocking
      writer.stop()            # при shutdown (graceful)
    """

    def __init__(
        self,
        *,
        enabled: Optional[bool] = None,
        max_size: Optional[int] = None,
        flush_interval_s: Optional[float] = None,
        max_retries: Optional[int] = None,
        queue_maxsize: Optional[int] = None,
    ) -> None:
        self.enabled = enabled if enabled is not None else _ENV_ENABLED
        self.max_size = max_size if max_size is not None else _ENV_MAX_SIZE
        self.flush_interval_s = flush_interval_s if flush_interval_s is not None else _ENV_FLUSH_INTERVAL
        self.max_retries = max_retries if max_retries is not None else _ENV_MAX_RETRIES
        queue_maxsize_ = queue_maxsize if queue_maxsize is not None else _ENV_QUEUE_MAXSIZE

        self._queue: queue.Queue = queue.Queue(maxsize=queue_maxsize_)
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enqueue(self, closed: Any) -> None:
        """
        Ставит сделку в очередь (non-blocking).

        Если BATCH_WRITER_ENABLED=0 — вызывает save_trade_closed синхронно.
        При переполнении очереди — drop + метрика (fail-open, не блокируем loop).
        """
        if not self.enabled:
            # Fallback: старое поведение
            from services import analytics_db
            analytics_db.save_trade_closed(closed)
            return

        try:
            self._queue.put_nowait(closed)
            _TM_DB_PENDING.set(self._queue.qsize())
        except queue.Full:
            _TM_DB_DROPPED.inc()
            logger.warning(
                "BatchTradeWriter: очередь переполнена (maxsize=%d), сделка order_id=%s потеряна!",
                self._queue.maxsize,
                getattr(closed, "order_id", "?"),
            )

    def flush(self) -> int:
        """
        Синхронный flush всех накопленных сделок.

        Returns:
            Количество успешно записанных строк.
        """
        items: List[Any] = []
        try:
            while True:
                items.append(self._queue.get_nowait())
                if len(items) >= self.max_size:
                    break
        except queue.Empty:
            pass

        if not items:
            return 0

        _TM_DB_PENDING.set(self._queue.qsize())
        t0 = time.perf_counter()
        written = self._flush_with_retry(items)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        _TM_DB_BATCH_SIZE.observe(written)
        _TM_DB_FLUSH_MS.observe(elapsed_ms)
        _TM_DB_FLUSHED.inc(written)

        logger.debug(
            "BatchTradeWriter: flush %d/%d строк за %.1f мс",
            written, len(items), elapsed_ms,
        )
        return written

    def start(self) -> None:
        """Запускает daemon-поток flush-цикла."""
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="TM_BatchWriter",
                daemon=True,
            )
            self._thread.start()
            logger.info(
                "BatchTradeWriter запущен (enabled=%s, max_size=%d, interval=%.1fs)",
                self.enabled, self.max_size, self.flush_interval_s,
            )

    def stop(self, timeout: float = 5.0) -> None:
        """
        Graceful shutdown: дожидается flush остатков очереди.
        """
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        # Final flush before exit
        try:
            remaining = self.flush()
            if remaining:
                logger.info("BatchTradeWriter: финальный flush %d записей при stop()", remaining)
        except Exception as exc:
            logger.error("BatchTradeWriter: финальный flush не удался: %s", exc)

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        """Основной цикл daemon-потока."""
        while not self._stop_event.is_set():
            try:
                self.flush()
            except Exception as exc:
                logger.error("BatchTradeWriter: ошибка flush: %s", exc)
            self._stop_event.wait(timeout=self.flush_interval_s)

        # drain on exit
        try:
            self.flush()
        except Exception as exc:
            logger.error("BatchTradeWriter: ошибка финального flush: %s", exc)

    def _flush_with_retry(self, items: List[Any]) -> int:
        """
        Выполняет batch INSERT с повторами при ошибке PG.

        При неудаче возвращает несохранённые элементы обратно в очередь
        (в начало — LIFO-priority через put_nowait, порядок не гарантирован,
        но lossless-critical: не теряем сделки).
        """
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                return self._do_batch_insert(items)
            except Exception as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    _TM_DB_RETRIES.inc()
                    sleep_s = 0.1 * (2 ** attempt)  # exp backoff: 0.1, 0.2, 0.4
                    logger.warning(
                        "BatchTradeWriter: ошибка batch flush (попытка %d/%d), retry через %.1fs: %s",
                        attempt + 1, self.max_retries + 1, sleep_s, exc,
                    )
                    time.sleep(sleep_s)

        # Все попытки провалились — возвращаем в очередь lossless
        logger.error(
            "BatchTradeWriter: batch flush провалился после %d попыток: %s. "
            "Возвращаем %d записей в очередь.",
            self.max_retries + 1, last_exc, len(items),
        )
        for item in reversed(items):
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                _TM_DB_DROPPED.inc()
        return 0

    def _do_batch_insert(self, items: List[Any]) -> int:
        """
        Выполняет batch INSERT в trades_closed и trades_closed_p0.
        Использует psycopg2.extras.execute_values для максимальной скорости.
        """
        from services import analytics_db

        main_rows: List[Tuple] = []
        p0_rows: List[Tuple] = []

        for closed in items:
            try:
                main_rows.append(_build_main_row(closed))
                if analytics_db.ANALYTICS_P0_ENABLED:
                    p0_rows.append(_build_p0_row(closed))
            except Exception as exc:
                logger.error(
                    "BatchTradeWriter: ошибка сборки строки для order_id=%s: %s",
                    getattr(closed, "order_id", "?"), exc,
                )

        if not main_rows:
            return 0

        sql_main = """
            INSERT INTO trades_closed (
                order_id, sid, strategy, source, symbol, tf, direction,
                entry_ts_ms, exit_ts_ms, entry_price, exit_price, lot, notional_usd,
                pnl_net, pnl_gross, fees, pnl_pct,
                pnl_if_fixed_exit, baseline_exit_reason, baseline_exit_ts_ms, baseline_exit_price,
                tp1_hit, tp2_hit, tp3_hit, tp_hits, tp_before_sl,
                trailing_started, trailing_active, trailing_moves, trailing_profile,
                mfe_pnl, mae_pnl, giveback, missed_profit,
                one_r_money, r_multiple, duration_ms,
                close_reason, close_reason_raw,
                entry_tag, max_favorable_price, max_favorable_ts,
                is_final_close, remaining_qty, status,
                health_l2_stale_ratio_tick, health_l2_stale_ratio_now,
                health_avg_l2_age_ms, health_avg_l2_age_tick_ms,
                health_signal_emit_rate, health_dlq_rate,
                config_json,
                is_virtual,
                meta_enforce_cov_bucket,
                meta_enforce_applied,
            ) VALUES %s
            ON CONFLICT (order_id) DO NOTHING
        """

        sql_p0 = """
            INSERT INTO trades_closed_p0 (
                order_id,
                exit_ts,
                exit_ts_ms,
                scenario, regime, session, entry_reason,
                mae_bps, mfe_bps, time_to_mfe_ms, hold_ms,
                spread_bps_at_entry, slippage_bps_est, book_age_ms,
                features_json,
                is_virtual,
                meta_enforce_cov_bucket,
                meta_enforce_applied,
                updated_at
            ) VALUES %s
            ON CONFLICT (order_id, exit_ts)
            DO UPDATE SET
                scenario = EXCLUDED.scenario,
                regime = EXCLUDED.regime,
                session = EXCLUDED.session,
                entry_reason = EXCLUDED.entry_reason,
                mae_bps = EXCLUDED.mae_bps,
                mfe_bps = EXCLUDED.mfe_bps,
                time_to_mfe_ms = EXCLUDED.time_to_mfe_ms,
                hold_ms = EXCLUDED.hold_ms,
                spread_bps_at_entry = EXCLUDED.spread_bps_at_entry,
                slippage_bps_est = EXCLUDED.slippage_bps_est,
                book_age_ms = EXCLUDED.book_age_ms,
                features_json = EXCLUDED.features_json,
                is_virtual = EXCLUDED.is_virtual,
                meta_enforce_cov_bucket = EXCLUDED.meta_enforce_cov_bucket,
                meta_enforce_applied = EXCLUDED.meta_enforce_applied,
                updated_at = now()
        """

        with analytics_db.get_conn() as conn, conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql_main, main_rows, page_size=200)
            if p0_rows and analytics_db.ANALYTICS_P0_ENABLED:
                try:
                    # p0 использует to_timestamp() — нужна шаблонная форма
                    p0_sql_tmpl = sql_p0.replace(
                        "VALUES %s",
                        "VALUES %s",
                    )
                    # Адаптированный вариант для exit_ts: передаём как литерал to_timestamp(%s/1000.0)
                    # execute_values не поддерживает смешанные функции в шаблоне,
                    # поэтому адаптируем exit_ts_ms → datetime в Python
                    import datetime as _dt
                    p0_rows_adapted = [
                        (
                            r[0],                                            # order_id
                            _dt.datetime.utcfromtimestamp(r[1] / 1000.0),  # exit_ts (timestamp)
                            r[1],                                            # exit_ts_ms
                            *r[2:],                                          # scenario..meta_enforce_applied
                            _dt.datetime.utcnow(),                           # updated_at
                        )
                        for r in p0_rows
                    ]
                    sql_p0_adapted = """
                        INSERT INTO trades_closed_p0 (
                            order_id,
                            exit_ts,
                            exit_ts_ms,
                            scenario, regime, session, entry_reason,
                            mae_bps, mfe_bps, time_to_mfe_ms, hold_ms,
                            spread_bps_at_entry, slippage_bps_est, book_age_ms,
                            features_json,
                            is_virtual,
                            meta_enforce_cov_bucket,
                            meta_enforce_applied,
                            updated_at
                        ) VALUES %s
                        ON CONFLICT (order_id, exit_ts)
                        DO UPDATE SET
                            scenario = EXCLUDED.scenario,
                            regime = EXCLUDED.regime,
                            session = EXCLUDED.session,
                            entry_reason = EXCLUDED.entry_reason,
                            mae_bps = EXCLUDED.mae_bps,
                            mfe_bps = EXCLUDED.mfe_bps,
                            time_to_mfe_ms = EXCLUDED.time_to_mfe_ms,
                            hold_ms = EXCLUDED.hold_ms,
                            spread_bps_at_entry = EXCLUDED.spread_bps_at_entry,
                            slippage_bps_est = EXCLUDED.slippage_bps_est,
                            book_age_ms = EXCLUDED.book_age_ms,
                            features_json = EXCLUDED.features_json,
                            is_virtual = EXCLUDED.is_virtual,
                            meta_enforce_cov_bucket = EXCLUDED.meta_enforce_cov_bucket,
                            meta_enforce_applied = EXCLUDED.meta_enforce_applied,
                            updated_at = now()
                    """
                    psycopg2.extras.execute_values(cur, sql_p0_adapted, p0_rows_adapted, page_size=200)
                except Exception as p0_exc:
                    if analytics_db.ANALYTICS_P0_HARD_FAIL:
                        raise
                    logger.warning("BatchTradeWriter: P0 batch insert провалился (soft): %s", p0_exc)
            conn.commit()

        # Auto calibration (best-effort, per item)
        for closed in items:
            try:
                from services.auto_calibration_service import get_auto_calibration_service
                get_auto_calibration_service().on_trade_closed(closed.symbol, closed.source)
            except Exception:
                pass

        return len(main_rows)


# ------------------------------------------------------------------
# Row builders (pure functions, testable without DB)
# ------------------------------------------------------------------

def _build_main_row(closed: Any) -> Tuple:
    """Строит кортеж параметров для INSERT INTO trades_closed."""
    return (
        closed.order_id, closed.sid, closed.strategy, closed.source, closed.symbol, closed.tf, closed.direction,
        closed.entry_ts_ms, closed.exit_ts_ms, closed.entry_price, closed.exit_price, closed.lot, closed.notional_usd,
        closed.pnl_net, closed.pnl_gross, closed.fees, closed.pnl_pct,
        closed.pnl_if_fixed_exit,
        getattr(closed, "baseline_exit_reason", ""),
        getattr(closed, "baseline_exit_ts_ms", 0),
        getattr(closed, "baseline_exit_price", 0.0),
        closed.tp1_hit, closed.tp2_hit, closed.tp3_hit, closed.tp_hits, closed.tp_before_sl,
        closed.trailing_started, closed.trailing_active, closed.trailing_moves,
        getattr(closed, "trailing_profile", ""),
        closed.mfe_pnl, closed.mae_pnl, closed.giveback, closed.missed_profit,
        closed.one_r_money, closed.r_multiple, closed.duration_ms,
        closed.close_reason,
        getattr(closed, "close_reason_raw", ""),
        getattr(closed, "entry_tag", ""),
        getattr(closed, "max_favorable_price", 0.0),
        getattr(closed, "max_favorable_ts", 0),
        getattr(closed, "is_final_close", True),
        getattr(closed, "remaining_qty", 0.0),
        getattr(closed, "status", "closed"),
        getattr(closed, "health_l2_stale_ratio_tick", 0.0),
        getattr(closed, "health_l2_stale_ratio_now", 0.0),
        getattr(closed, "health_avg_l2_age_ms", 0.0),
        getattr(closed, "health_l2_age_tick_ms", 0.0),
        getattr(closed, "health_signal_emit_rate", 0.0),
        getattr(closed, "health_dlq_rate", 0.0),
        json.dumps((getattr(closed, "signal_payload", {}) or {}).get("config_snapshot", {})),
        getattr(closed, "is_virtual", False),
        getattr(closed, "meta_enforce_cov_bucket", ""),
        getattr(closed, "meta_enforce_applied", -1),
    )


def _build_p0_row(closed: Any) -> Tuple:
    """Строит кортеж параметров для INSERT INTO trades_closed_p0 (без exit_ts адаптации)."""
    sp = getattr(closed, "signal_payload", {}) or {}

    features: Dict[str, Any] = {}
    f1 = getattr(closed, "features", None)
    if isinstance(f1, dict):
        features = dict(f1)
    else:
        features = dict(sp.get("features") or sp.get("indicators") or {})

    # whitelist (как в analytics_db.py)
    ALLOW = {
        "delta_z", "dn_usd", "obi", "cvd_slope",
        "absorption_score", "weak_progress", "vwap_pos",
        "atr_bps", "liq_scale", "confidence",
        "adverse_bps_t",
        "spread_bps_at_entry", "book_age_ms", "slippage_bps_est",
        "data_health", "expected_slippage_bps",
        "expected_slippage_decomp_bps", "impact_proxy",
        "slip_decomp_coeff_bps", "slip_decomp_spread_bps", "slip_decomp_impact_bps",
        "exec_regime_bucket", "liq_regime_label", "vol_regime_label",
        "spread_bps_submit", "mid_px_submit",
        "taker_flow_imb", "taker_flow_imb_z",
        "taker_flow_gate_veto", "taker_flow_gate_shadow_veto",
        "taker_flow_gate_soft", "taker_flow_gate_reason",
    }
    features = {k: features[k] for k in ALLOW if k in features}
    features_str = json.dumps(features, ensure_ascii=False)
    if len(features_str) > 8000:
        PRIORITY = ["adverse_bps_t", "delta_z", "dn_usd", "obi", "weak_progress", "absorption_score", "confidence"]
        features = {k: features.get(k) for k in PRIORITY if k in features}

    if Json is not None:
        features_json = Json(features)
    else:
        features_json = json.dumps(features)  # fallback

    return (
        closed.order_id,             # [0] order_id
        closed.exit_ts_ms,           # [1] exit_ts_ms (used for exit_ts adaptation in caller)
        getattr(closed, "scenario", None) or sp.get("scenario"),   # [2]
        getattr(closed, "regime", None) or sp.get("regime"),        # [3]
        getattr(closed, "session", None) or sp.get("session"),      # [4]
        getattr(closed, "entry_reason", None) or sp.get("entry_reason"),  # [5]
        getattr(closed, "mae_bps", None),                            # [6]
        getattr(closed, "mfe_bps", None),                            # [7]
        getattr(closed, "time_to_mfe_ms", None),                     # [8]
        getattr(closed, "hold_ms", None) or getattr(closed, "duration_ms", None),  # [9]
        getattr(closed, "spread_bps_at_entry", None) or sp.get("spread_bps_at_entry") or sp.get("spread_bps"),  # [10]
        getattr(closed, "slippage_bps_est", None) or sp.get("slippage_bps_est"),   # [11]
        getattr(closed, "book_age_ms", None) or sp.get("book_age_ms"),              # [12]
        features_json,                                               # [13]
        getattr(closed, "is_virtual", False),                        # [14]
        getattr(closed, "meta_enforce_cov_bucket", ""),              # [15]
        getattr(closed, "meta_enforce_applied", -1),                 # [16]
        # NOTE: updated_at is added by the caller as now() literal
    )


# ------------------------------------------------------------------
# Singleton accessor
# ------------------------------------------------------------------

_writer_instance: Optional[BatchTradeWriter] = None
_writer_lock = threading.Lock()


def get_batch_writer() -> BatchTradeWriter:
    """
    Возвращает глобальный singleton BatchTradeWriter.
    Автоматически запускает поток при первом вызове.
    """
    global _writer_instance
    if _writer_instance is None:
        with _writer_lock:
            if _writer_instance is None:
                _writer_instance = BatchTradeWriter()
                _writer_instance.start()
    return _writer_instance
