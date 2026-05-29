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
import hashlib
import logging
import os
import queue
import threading
import time
from typing import Any

try:
    from services.horizon_contract import (
        extract_atr_tf_ms,  # type: ignore
        extract_horizon_bucket,  # type: ignore
        extract_horizon_contract_from_payload,  # type: ignore
    )
except ImportError:  # pragma: no cover
    def extract_horizon_contract_from_payload(p):  # type: ignore[misc]
        return {}
    def extract_horizon_bucket(c):  # type: ignore[misc]
        return ""
    def extract_atr_tf_ms(c):  # type: ignore[misc]
        return 0

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
        enabled: bool | None = None,
        max_size: int | None = None,
        flush_interval_s: float | None = None,
        max_retries: int | None = None,
        queue_maxsize: int | None = None,
    ) -> None:
        self.enabled = enabled if enabled is not None else _ENV_ENABLED
        self.max_size = max_size if max_size is not None else _ENV_MAX_SIZE
        self.flush_interval_s = flush_interval_s if flush_interval_s is not None else _ENV_FLUSH_INTERVAL
        self.max_retries = max_retries if max_retries is not None else _ENV_MAX_RETRIES
        queue_maxsize_ = queue_maxsize if queue_maxsize is not None else _ENV_QUEUE_MAXSIZE

        self._queue: queue.Queue = queue.Queue(maxsize=queue_maxsize_)
        self._thread: threading.Thread | None = None
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
        items: list[Any] = []
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

    def _flush_with_retry(self, items: list[Any]) -> int:
        """
        Выполняет batch INSERT с повторами при ошибке PG.

        При неудаче возвращает несохранённые элементы обратно в очередь
        (в начало — LIFO-priority через put_nowait, порядок не гарантирован,
        но lossless-critical: не теряем сделки).
        """
        last_exc: Exception | None = None
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

    def _do_batch_insert(self, items: list[Any]) -> int:
        """
        Выполняет batch INSERT в trades_closed и trades_closed_p0.
        Использует psycopg2.extras.execute_values для максимальной скорости.
        """
        from services import analytics_db

        main_rows: list[tuple] = []
        p0_rows: list[tuple] = []

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
                sc_contract_ver,
                sc_risk_horizon_bucket,
                sc_hold_target_ms,
                sc_alpha_half_life_ms,
                sc_max_signal_age_ms,
                sc_atr_age_ms,
                sc_atr_source,
                sc_atr_pct,
                sc_vol_ratio_fast_slow,
                sc_vol_ratio_z,
                health_l2_stale_ratio_tick, health_l2_stale_ratio_now,
                health_avg_l2_age_ms, health_avg_l2_age_tick_ms,
                health_signal_emit_rate, health_dlq_rate,
                config_json,
                horizon_contract,
                horizon_bucket,
                atr_tf_ms,
                is_virtual,
                meta_enforce_cov_bucket,
                meta_enforce_applied,
                live_surface_applied,
                live_surface_reason_code,
                baseline_sl_price,
                baseline_tp1_price,
                selected_sl_price,
                selected_tp1_price,
                trailing_surface_applied,
                trailing_surface_reason_code,
                baseline_trailing_offset_atr,
                selected_trailing_offset_atr,

                close_reason_detail,
                strong_gate_ok,
                atr_policy_ver,
                atr_policy_tag,
                atr_policy_source,
                atr_policy_scenario,
                atr_policy_regime,
                atr_policy_bucket,
                atr_stop_ttl_mode,
                atr_trailing_mode,
                atr_recovery_run_id,
                atr_restore_cert_id,
                atr_restore_cert_status,
                atr_policy_snapshot_json,
                atr_sel_tf, atr_sel_src, atr_sel_age_ms,
                contract_ver, hold_target_ms, alpha_half_life_ms, max_signal_age_ms,
                risk_horizon_bucket, horizon_profile_source, horizon_profile_conf, horizon_reason_code,
                atr_mode, atr_value, atr_window_n, atr_age_ms, atr_source, atr_pct,
                vol_ratio_fast_slow, vol_ratio_z,
                atr_regime_value, atr_trail_value, atr_regime_tf_ms, atr_trail_tf_ms,
                policy_mode, policy_raw,
                v_gate_reason,
                is_orphan_cleanup, exclude_from_ml_labels,
                timeout_age_ms, timeout_max_hold_ms, timeout_request_ts_ms, timeout_close_latency_ms,
                exit_order_ref, closed_trade_id,
                entry_regime, ab_arm
            ) VALUES %s
            ON CONFLICT (order_id) DO NOTHING
        """

        with analytics_db.get_conn() as conn, conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql_main, main_rows, page_size=200)  # type: ignore
            if p0_rows and analytics_db.ANALYTICS_P0_ENABLED:
                try:
                    import datetime as _dt
                    # _build_p0_row returns 25 items: [0]=order_id [1]=exit_ts_ms [2..24]=payload
                    # Adapt exit_ts_ms → datetime for exit_ts column (TimescaleDB needs timestamptz)
                    p0_rows_adapted = [
                        (
                            r[0],                                                              # order_id
                            _dt.datetime.fromtimestamp(r[1] / 1000.0, tz=_dt.timezone.utc),  # exit_ts
                            r[1],                                                              # exit_ts_ms
                            *r[2:],                                                            # [2..25] scenario..ab_arm
                            _dt.datetime.now(_dt.timezone.utc),                               # updated_at
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
                            trailing_surface_applied,
                            trailing_surface_reason_code,
                            baseline_trailing_offset_atr,
                            selected_trailing_offset_atr,
                            policy_mode,
                            policy_raw,
                            strong_gate_ok,
                            v_gate_reason,
                            ab_arm,
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
                            trailing_surface_applied = EXCLUDED.trailing_surface_applied,
                            trailing_surface_reason_code = EXCLUDED.trailing_surface_reason_code,
                            baseline_trailing_offset_atr = EXCLUDED.baseline_trailing_offset_atr,
                            selected_trailing_offset_atr = EXCLUDED.selected_trailing_offset_atr,
                            policy_mode = COALESCE(EXCLUDED.policy_mode, trades_closed_p0.policy_mode),
                            policy_raw = COALESCE(EXCLUDED.policy_raw, trades_closed_p0.policy_raw),
                            strong_gate_ok = COALESCE(EXCLUDED.strong_gate_ok, trades_closed_p0.strong_gate_ok),
                            v_gate_reason = COALESCE(EXCLUDED.v_gate_reason, trades_closed_p0.v_gate_reason),
                            ab_arm = COALESCE(EXCLUDED.ab_arm, trades_closed_p0.ab_arm),
                            updated_at = now()
                    """
                    psycopg2.extras.execute_values(cur, sql_p0_adapted, p0_rows_adapted, page_size=200)  # type: ignore
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
            # Phase 1: horizon profile dirty-mark (best-effort, non-blocking)
            try:
                from services.horizon_profile_bootstrap_service import get_horizon_profile_bootstrap_service
                get_horizon_profile_bootstrap_service().on_trade_closed(closed.symbol, closed.source)
            except Exception:
                pass

        return len(main_rows)


# ------------------------------------------------------------------
# Row builders (pure functions, testable without DB)

_ENTRY_REGIME_SENTINELS = frozenset({"", "na", "none", "null", "unknown"})


def _entry_regime_db_value(closed: Any) -> str | None:
    """Meaningful entry_regime/regime only; skip sentinels (na/none/unknown)."""
    for attr in ("entry_regime", "regime"):
        raw = getattr(closed, attr, None)
        if raw is None:
            continue
        s = str(raw).strip().lower()
        if s not in _ENTRY_REGIME_SENTINELS:
            return str(raw).strip()
    return None


def _policy_mode_raw_from_payload(sp: dict[str, Any]) -> tuple[Any, Any]:
    """Mirror analytics_db.save_trade_closed policy extraction (risk_surface_shadow).

    Fix 2026-05-27: check BOTH `sp.config_snapshot.meta` AND `sp.meta` for each
    field rather than `or`-short-circuiting on the dict. Empirically masked 47%
    of cryptoorderflow policy_mode values when config_snapshot.meta existed but
    lacked risk_surface_shadow (which lived under sp.meta).
    """
    config_snapshot = sp.get("config_snapshot") or {}
    cs_meta = config_snapshot.get("meta") or {}
    sp_meta = sp.get("meta") or {}
    rss = cs_meta.get("risk_surface_shadow") or sp_meta.get("risk_surface_shadow") or {}
    mode = (
        rss.get("mode")
        or cs_meta.get("policy_effective_mode") or sp_meta.get("policy_effective_mode")
        or cs_meta.get("policy_regime") or sp_meta.get("policy_regime")
        or cs_meta.get("policy_mode") or sp_meta.get("policy_mode")
        or sp.get("policy_mode")
        or None
    )
    raw = json.dumps(rss, ensure_ascii=False) if rss else None
    return mode, raw


def _get_metric(closed, sp, key, default):
    allow_zero = key in {
        "live_surface_applied",
        "trailing_surface_applied",
        "baseline_trailing_offset_atr",
        "selected_trailing_offset_atr",
    }
    absent = (None, "") if allow_zero else (None, "", 0, 0.0)
    val = getattr(closed, key, None)
    if val not in absent:
        return val
    if key in sp and sp[key] not in absent:
        return sp[key]
    config_snapshot = sp.get("config_snapshot") or {}
    meta_candidates = [
        sp.get("meta") or {},
        config_snapshot.get("meta") or {},
    ]
    for meta in meta_candidates:
        if key in meta and meta[key] not in absent:
            return meta[key]
        risk_surface = meta.get("risk_surface_shadow") or {}
        if key in risk_surface and risk_surface[key] not in absent:
            return risk_surface[key]
    policy_provenance = config_snapshot.get("policy_provenance") or {}
    if key in policy_provenance and policy_provenance[key] not in (None, "", "None", 0, 0.0):
        return policy_provenance[key]
    for meta in meta_candidates:
        atr_policy = sp.get("atr_policy") or meta.get("atr_policy") or {}
        if key in atr_policy and atr_policy[key] not in absent:
            return atr_policy[key]
        if "live_surface" in meta and key.startswith("live_surface_"):
            k = key.replace("live_surface_", "")
            if k in meta["live_surface"] and meta["live_surface"][k] not in absent:
                return meta["live_surface"][k]
        if "trailing_surface" in meta and key.startswith("trailing_surface_"):
            k = key.replace("trailing_surface_", "")
            if k in meta["trailing_surface"] and meta["trailing_surface"][k] not in absent:
                return meta["trailing_surface"][k]
    # Remove 'atr_policy_' prefix if not found and check again in policy_provenance
    if key.startswith("atr_policy_"):
        short_key = key.replace("atr_policy_", "")
        if short_key in policy_provenance and policy_provenance[short_key] not in (None, "", "None", 0, 0.0):
            return policy_provenance[short_key]
    return default

def _stable_closed_trade_id(closed: Any) -> str:
    existing = getattr(closed, "closed_trade_id", None)
    if existing:
        return str(existing)
    raw = "|".join(
        str(x or "")
        for x in (
            getattr(closed, "sid", ""),
            getattr(closed, "exit_order_ref", ""),
            getattr(closed, "exit_ts_ms", ""),
            getattr(closed, "close_reason", ""),
        )
    )
    return "ctid:" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]

def _build_main_row(closed: Any) -> tuple:
    """Строит кортеж параметров для INSERT INTO trades_closed."""
    sp = getattr(closed, "signal_payload", {}) or {}
    horizon_contract = extract_horizon_contract_from_payload(sp)
    horizon_bucket = extract_horizon_bucket(horizon_contract)
    atr_tf_ms_val = extract_atr_tf_ms(horizon_contract)
    policy_mode_val, policy_raw_val = _policy_mode_raw_from_payload(sp)
    if policy_mode_val is None:
        policy_mode_val = getattr(closed, "policy_mode", None)
    config_snapshot = dict(sp.get("config_snapshot", {}) or {})
    if horizon_contract:
        config_snapshot["_horizon_contract"] = horizon_contract

    # FIX: Restore analytics payload blocks into config_snapshot
    # so that Postgres generated columns (ind_*, atr_*) in trades_closed are properly populated.
    for key in ("indicators", "atr_metrics", "metrics", "meta"):
        if key in sp and sp[key] is not None:
            config_snapshot[key] = sp[key]
            
    # Include features explicitly from closed.features (similar to p0)
    features = getattr(closed, "features", None)
    if isinstance(features, dict) and features:
        config_snapshot["features"] = features
    elif "features" in sp and sp["features"]:
        config_snapshot["features"] = sp["features"]
    indicators = sp.get("indicators") or {}
    strong_gate_ok_raw = indicators.get("strong_gate_ok", indicators.get("of_confirm_ok", None))
    try:
        strong_gate_ok = bool(int(strong_gate_ok_raw)) if strong_gate_ok_raw is not None else None
    except (ValueError, TypeError):
        strong_gate_ok = None

    res = (
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
        # Phase 0.3: first-class scalar horizon/ATR columns
        _get_metric(closed, sp, "contract_ver", None) or 2,
        _get_metric(closed, sp, "risk_horizon_bucket", ""),
        _get_metric(closed, sp, "hold_target_ms", 0),
        _get_metric(closed, sp, "alpha_half_life_ms", 0),
        _get_metric(closed, sp, "max_signal_age_ms", 0),
        _get_metric(closed, sp, "atr_age_ms", 0),
        _get_metric(closed, sp, "atr_source", ""),
        _get_metric(closed, sp, "atr_pct", 0.0),
        _get_metric(closed, sp, "vol_ratio_fast_slow", 1.0),
        _get_metric(closed, sp, "vol_ratio_z", 0.0),
        getattr(closed, "health_l2_stale_ratio_tick", 0.0),
        getattr(closed, "health_l2_stale_ratio_now", 0.0),
        getattr(closed, "health_avg_l2_age_ms", 0.0),
        getattr(closed, "health_l2_age_tick_ms", 0.0),
        getattr(closed, "health_signal_emit_rate", 0.0),
        getattr(closed, "health_dlq_rate", 0.0),
        # Config Json (enriched with horizon snapshot)
        json.dumps(config_snapshot, ensure_ascii=False, sort_keys=True),
        # Horizon contract columns
        json.dumps(horizon_contract, ensure_ascii=False, sort_keys=True),
        horizon_bucket or getattr(closed, "risk_horizon_bucket", None) or None,
        (atr_tf_ms_val if "atr_profile" in horizon_contract and "atr_tf_ms" in horizon_contract.get("atr_profile", {}) else (0 if getattr(closed, "atr_tf_ms", None) == 0 else getattr(closed, "atr_tf_ms", None))),
        getattr(closed, "is_virtual", False),
        _get_metric(closed, sp, "meta_enforce_cov_bucket", ""),
        bool(getattr(closed, "meta_enforce_applied", False)) if getattr(closed, "meta_enforce_applied", None) is not None else None,
        # Phase 2.4E: live surface A/B analytics
        _get_metric(closed, sp, "live_surface_applied", None),
        _get_metric(closed, sp, "live_surface_reason_code", None),
        _get_metric(closed, sp, "baseline_sl_price", None),
        _get_metric(closed, sp, "baseline_tp1_price", None),
        _get_metric(closed, sp, "selected_sl_price", None),
        _get_metric(closed, sp, "selected_tp1_price", None),
        # Phase 2.6: trailing surface A/B analytics
        _get_metric(closed, sp, "trailing_surface_applied", None),
        _get_metric(closed, sp, "trailing_surface_reason_code", None),
        _get_metric(closed, sp, "baseline_trailing_offset_atr", 0.0),
        _get_metric(closed, sp, "selected_trailing_offset_atr", 0.0),

        # --- NEW Analytics columns ---
        getattr(closed, "close_reason_detail", ""),
        strong_gate_ok,
        _get_metric(closed, sp, "atr_policy_ver", 0),
        _get_metric(closed, sp, "atr_policy_tag", ""),
        _get_metric(closed, sp, "atr_policy_source", ""),
        _get_metric(closed, sp, "atr_policy_scenario", ""),
        _get_metric(closed, sp, "atr_policy_regime", ""),
        _get_metric(closed, sp, "atr_policy_bucket", ""),
        _get_metric(closed, sp, "atr_stop_ttl_mode", ""),
        _get_metric(closed, sp, "atr_trailing_mode", ""),
        _get_metric(closed, sp, "atr_recovery_run_id", ""),
        _get_metric(closed, sp, "atr_restore_cert_id", ""),
        _get_metric(closed, sp, "atr_restore_cert_status", ""),
        json.dumps(getattr(closed, "atr_policy_snapshot_json", {}) or {}, ensure_ascii=False) if getattr(closed, "atr_policy_snapshot_json", {}) else None,
        # ATR selector (set by domain/handlers.py)
        getattr(closed, "atr_sel_tf", ""),
        getattr(closed, "atr_sel_src", ""),
        getattr(closed, "atr_sel_age_ms", 0),
        # Horizon scalars (stamped from PositionState)
        getattr(closed, "contract_ver", 0) or 0,
        getattr(closed, "hold_target_ms", 0) or 0,
        getattr(closed, "alpha_half_life_ms", 0) or 0,
        getattr(closed, "max_signal_age_ms", 0) or 0,
        getattr(closed, "risk_horizon_bucket", "") or "",
        getattr(closed, "horizon_profile_source", "") or "",
        getattr(closed, "horizon_profile_conf", 0.0),
        getattr(closed, "horizon_reason_code", "") or "",
        getattr(closed, "atr_mode", "") or "",
        getattr(closed, "atr_value", 0.0) or getattr(closed, "atr", 0.0),
        getattr(closed, "atr_window_n", 0) or 0,
        getattr(closed, "atr_age_ms", 0) or 0,
        getattr(closed, "atr_source", "") or "",
        getattr(closed, "atr_pct", 0.0),
        getattr(closed, "vol_ratio_fast_slow", 1.0),
        getattr(closed, "vol_ratio_z", 0.0),
        getattr(closed, "atr_regime_value", 0.0),
        getattr(closed, "atr_trail_value", 0.0),
        getattr(closed, "atr_regime_tf_ms", 0) or 0,
        getattr(closed, "atr_trail_tf_ms", 0) or 0,
        policy_mode_val,
        policy_raw_val,
        _get_metric(closed, sp, "v_gate_reason", None),
        bool(getattr(closed, "is_orphan_cleanup", False)),
        bool(getattr(closed, "exclude_from_ml_labels", False)),
        getattr(closed, "timeout_age_ms", None) or None,
        getattr(closed, "timeout_max_hold_ms", None) or None,
        getattr(closed, "timeout_request_ts_ms", None) or None,
        getattr(closed, "timeout_close_latency_ms", None) or None,
        getattr(closed, "exit_order_ref", None) or (f"virt:exit:{getattr(closed, 'sid', '')}" if getattr(closed, "is_virtual", False) else None),
        _stable_closed_trade_id(closed) if getattr(closed, "is_final_close", True) else None,
        _entry_regime_db_value(closed),
        str(getattr(closed, "ab_arm", None) or sp.get("ab_arm") or "A").upper(),
    )
    return tuple(None if val == () else val for val in res)


def _build_p0_row(closed: Any) -> tuple:
    """Строит кортеж параметров для INSERT INTO trades_closed_p0 (без exit_ts адаптации)."""
    sp = getattr(closed, "signal_payload", {}) or {}

    features: dict[str, Any] = {}
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
    # Извлечение gate_ok и policy
    indicators = sp.get("indicators") or {}
    meta = sp.get("meta") or {}
    strong_gate_ok_raw = indicators.get("strong_gate_ok", indicators.get("of_confirm_ok", None))
    try:
        strong_gate_ok = bool(int(strong_gate_ok_raw)) if strong_gate_ok_raw is not None else None
    except (ValueError, TypeError):
        strong_gate_ok = None
        
    policy_mode = meta.get("policy_effective_mode") or meta.get("policy_regime") or meta.get("policy_mode") or sp.get("policy_mode") or sp.get("policy_regime") or getattr(closed, "policy_mode", None) or None
    policy_raw = json.dumps(meta, ensure_ascii=False) if meta else None

    res = (
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
        _get_metric(closed, sp, "is_virtual", False),                        # [14]
        _get_metric(closed, sp, "meta_enforce_cov_bucket", ""),              # [15]
        bool(_get_metric(closed, sp, "meta_enforce_applied", False)) if _get_metric(closed, sp, "meta_enforce_applied", None) is not None else None,  # [16]
        _get_metric(closed, sp, "trailing_surface_applied", False),          # [17]
        _get_metric(closed, sp, "trailing_surface_reason_code", None),       # [18]
        _get_metric(closed, sp, "baseline_trailing_offset_atr", 0.0),        # [19]
        _get_metric(closed, sp, "selected_trailing_offset_atr", 0.0),        # [20]
        policy_mode,                                                          # [21]
        policy_raw,                                                           # [22]
        strong_gate_ok,                                                       # [23]
        _get_metric(closed, sp, "v_gate_reason", None) or None,              # [24]
        str(getattr(closed, "ab_arm", None) or sp.get("ab_arm") or "A").upper(),  # [25]
        # NOTE: updated_at is added by the caller as now() literal
    )
    return tuple(None if val == () else val for val in res)


# ------------------------------------------------------------------
# Singleton accessor
# ------------------------------------------------------------------

_writer_instance: BatchTradeWriter | None = None
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
