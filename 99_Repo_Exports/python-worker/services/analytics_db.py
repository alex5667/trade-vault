#!/usr/bin/env python3
from __future__ import annotations

"""
Simple TimescaleDB connector for scanner_analytics.

Usage:
  export TRADES_DB_DSN="postgresql://user:pass@host:5432/scanner_analytics"
  from services.analytics_db import fetch_trades_closed
"""

import json
import logging
import math
import os
from typing import Any

import psycopg2
from psycopg2.extras import Json, RealDictCursor

logger = logging.getLogger("analytics_db")

def _sanitize_floats(obj: Any) -> Any:
    """Recursively replace NaN/Infinity with None so json.dumps produces valid JSON."""
    if isinstance(obj, float):
        return None if not math.isfinite(obj) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_floats(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        sanitized = [_sanitize_floats(v) for v in obj]
        return sanitized if isinstance(obj, list) else tuple(sanitized)
    return obj

try:
    from prometheus_client import REGISTRY as _PREG
    from prometheus_client import Counter as _PCounter

    def _pcounter(name, doc, labels=()):
        try:
            return _PCounter(name, doc, list(labels))
        except ValueError:
            return (_PREG._names_to_collectors or {}).get(name)

    _TRADES_CLOSED_MAIN_INSERT = _pcounter(
        "trades_closed_main_insert_total",
        "Successful main INSERT into trades_closed",
    )
    _TRADES_CLOSED_P0_UPSERT_FAIL = _pcounter(
        "trades_closed_p0_upsert_fail_total",
        "Failed optional upsert into trades_closed_p0 (savepoint rolled back)",
    )
except Exception:
    class _NullCounter:
        def inc(self): pass
        def labels(self, **_): return self
    _TRADES_CLOSED_MAIN_INSERT = _NullCounter()  # type: ignore[assignment]
    _TRADES_CLOSED_P0_UPSERT_FAIL = _NullCounter()  # type: ignore[assignment]

try:
    from domain.models import TradeClosed
except ImportError:
    TradeClosed = None

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
DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/scanner_analytics"
TRADES_DB_DSN = os.getenv("TRADES_DB_DSN") or os.getenv("ANALYTICS_DB_DSN", DEFAULT_DSN)

ANALYTICS_P0_ENABLED = os.getenv("ANALYTICS_P0_ENABLED", "1") == "1"
ANALYTICS_P0_HARD_FAIL = os.getenv("ANALYTICS_P0_HARD_FAIL", "0") == "1"


def _enrich_config_snapshot(closed) -> dict:
    """Build config_json dict enriched with actual TP/SL/ATR trade levels.

    Merges computed levels (tp_levels, sl, atr, tp1_price) into the
    signal_payload.config_snapshot dict so that retrospective analytics
    always have access to the levels used for the trade.
    """
    sp = getattr(closed, "signal_payload", None) or {}
    cs = dict(sp.get("config_snapshot", {}) or {}) if isinstance(sp, dict) else {}
    try:
        _tp_lvls = getattr(closed, "tp_levels", None) or []
        if _tp_lvls:
            cs["tp_levels"] = [float(x) for x in _tp_lvls]
        _sl = float(getattr(closed, "sl", 0.0) or getattr(closed, "selected_sl_price", 0.0) or 0.0)
        if _sl > 0:
            cs["sl"] = _sl
        _atr = float(getattr(closed, "atr", 0.0) or 0.0)
        if _atr > 0:
            cs["atr"] = _atr
        _tp1 = float(getattr(closed, "tp1_price", 0.0) or getattr(closed, "selected_tp1_price", 0.0) or 0.0)
        if _tp1 > 0:
            cs["tp1_price"] = _tp1
    except Exception:
        pass
    return cs

# ---------------------------------------------------------------------------
# Async batch trade writer (TRADES_BATCH_ENABLED=1)
# ---------------------------------------------------------------------------
# When enabled, save_trade_closed_async() enqueues rows into AsyncBatchWriter
# instead of blocking on a per-row INSERT.  The synchronous save_trade_closed()
# path is preserved for callers that need immediate confirmation.
_TRADES_BATCH_ENABLED = os.getenv("TRADES_BATCH_ENABLED", "0") == "1"
_TRADES_BATCH_SIZE = int(os.getenv("TRADES_BATCH_SIZE", "50"))
_TRADES_FLUSH_INTERVAL_S = float(os.getenv("TRADES_FLUSH_INTERVAL_S", "5.0"))
_trade_batch_writer = None
_trade_p0_batch_writer = None


from contextlib import contextmanager

try:
    from psycopg2 import pool
except ImportError:
    pool = None

_POOL = None

import threading

_pool_lock = threading.Lock()

def _init_pool():
    global _POOL
    if _POOL is None and pool:
        with _pool_lock:
            if _POOL is None:
                # TradeMonitor use ThreadPoolExecutor, so we MUST use ThreadedConnectionPool.
                # Default minconn=1, maxconn=15. Adjust as needed.
                try:
                    _POOL = pool.ThreadedConnectionPool(1, 15, dsn=TRADES_DB_DSN, connect_timeout=3)
                    logger.info("✅ ThreadedConnectionPool initialized for analytics DB")
                except Exception as e:
                    logger.error("❌ Failed to initialize ThreadedConnectionPool: %s", e)

@contextmanager
def get_conn():
    """Return psycopg2 connection from pool."""
    global _POOL
    if _POOL is None:
        _init_pool()

    if _POOL:
        conn = _POOL.getconn()
        try:
            yield conn
        finally:
            _POOL.putconn(conn)
    else:
        # Fallback if pool cannot be initialized (e.g. import failed)
        if os.getenv("DEBUG_DB_CONN", "0") == "1":
            print(f"[DEBUG] Connecting to DB with DSN: {TRADES_DB_DSN}")
        conn = psycopg2.connect(TRADES_DB_DSN, connect_timeout=3)
        try:
            yield conn
        finally:
            conn.close()


def init_trade_batch_writer(dsn: str = "") -> None:
    """Initialise the AsyncBatchWriter for trades_closed and trades_closed_p0.

    Call once at container startup when TRADES_BATCH_ENABLED=1.
    Idempotent — subsequent calls are no-ops.
    """
    global _trade_batch_writer, _trade_p0_batch_writer
    if _trade_batch_writer is not None:
        return
    _dsn = dsn or TRADES_DB_DSN
    if not _dsn:
        return
    try:
        from services.db_batch_writer import get_or_create_writer
        # Only core fields that map 1:1 to trades_closed columns — complex P0 enrichment
        # is still done by the synchronous save_trade_closed() path.
        _trade_batch_writer = get_or_create_writer(
            table="trades_closed",
            columns=[
                "order_id", "sid", "strategy", "source", "symbol", "tf", "direction",
                "entry_ts_ms", "exit_ts_ms", "entry_price", "exit_price", "lot", "notional_usd",
                "pnl_net", "pnl_gross", "fees", "pnl_pct",
                "pnl_if_fixed_exit", "baseline_exit_reason", "baseline_exit_ts_ms", "baseline_exit_price",
                "tp1_hit", "tp2_hit", "tp3_hit", "tp_hits", "tp_before_sl",
                "trailing_started", "trailing_active", "trailing_moves", "trailing_profile",
                "mfe_pnl", "mae_pnl", "giveback", "missed_profit",
                "one_r_money", "r_multiple", "duration_ms",
                "close_reason", "close_reason_raw", "close_reason_detail",
                "entry_tag", "max_favorable_price", "max_favorable_ts",
                "is_final_close", "remaining_qty", "status",
                "sc_contract_ver", "sc_risk_horizon_bucket",
                "sc_hold_target_ms", "sc_alpha_half_life_ms", "sc_max_signal_age_ms",
                "sc_atr_age_ms", "sc_atr_source", "sc_atr_pct",
                "sc_vol_ratio_fast_slow", "sc_vol_ratio_z",
                "health_l2_stale_ratio_tick", "health_l2_stale_ratio_now",
                "health_avg_l2_age_ms", "health_avg_l2_age_tick_ms",
                "health_signal_emit_rate", "health_dlq_rate",
                "config_json", "is_virtual",
                "meta_enforce_cov_bucket", "meta_enforce_applied",
                "atr_policy_ver", "atr_policy_tag", "atr_policy_source", "atr_policy_scenario",
                "atr_policy_regime", "atr_policy_bucket", "atr_stop_ttl_mode", "atr_trailing_mode",
                "atr_recovery_run_id", "atr_restore_cert_id", "atr_restore_cert_status",
                "atr_policy_snapshot_json",
            ],
            dsn=_dsn,
            batch_size=_TRADES_BATCH_SIZE,
            flush_interval_s=_TRADES_FLUSH_INTERVAL_S,
            on_conflict_sql="ON CONFLICT (order_id) DO NOTHING",
        )
        _trade_p0_batch_writer = get_or_create_writer(
            table="trades_closed_p0",
            columns=[
                "order_id", "exit_ts", "exit_ts_ms", "scenario", "regime", "session", "entry_reason",
                "mae_bps", "mfe_bps", "time_to_mfe_ms", "hold_ms", "spread_bps_at_entry", "slippage_bps_est",
                "book_age_ms", "features_json", "is_virtual", "meta_enforce_cov_bucket", "meta_enforce_applied", "updated_at"
            ],
            dsn=_dsn,
            batch_size=_TRADES_BATCH_SIZE,
            flush_interval_s=_TRADES_FLUSH_INTERVAL_S,
            on_conflict_sql=(
                "ON CONFLICT (order_id, exit_ts) DO UPDATE SET "
                "scenario = EXCLUDED.scenario, regime = EXCLUDED.regime, session = EXCLUDED.session, "
                "entry_reason = EXCLUDED.entry_reason, mae_bps = EXCLUDED.mae_bps, mfe_bps = EXCLUDED.mfe_bps, "
                "time_to_mfe_ms = EXCLUDED.time_to_mfe_ms, hold_ms = EXCLUDED.hold_ms, "
                "spread_bps_at_entry = EXCLUDED.spread_bps_at_entry, slippage_bps_est = EXCLUDED.slippage_bps_est, "
                "book_age_ms = EXCLUDED.book_age_ms, features_json = EXCLUDED.features_json, "
                "is_virtual = EXCLUDED.is_virtual, meta_enforce_cov_bucket = EXCLUDED.meta_enforce_cov_bucket, "
                "meta_enforce_applied = EXCLUDED.meta_enforce_applied, updated_at = now()"
            ),
        )
        import logging
        logging.getLogger("analytics_db").info(
            "[analytics_db] async trade batch writer initialised (batch=%d interval=%.1fs)",
            _TRADES_BATCH_SIZE, _TRADES_FLUSH_INTERVAL_S,
        )
    except Exception as exc:
        import logging
        logging.getLogger("analytics_db").warning(
            "[analytics_db] batch writer init failed, fallback to sync: %s", exc
        )


def _apply_filters(symbol: str | None, source: str | None) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if symbol:
        clauses.append("symbol = %s")
        params.append(symbol)
    if source:
        clauses.append("source = %s")
        params.append(source)

    if not clauses:
        return "", params
    where = "WHERE " + " AND ".join(clauses)
    return where, params


def fetch_trades_closed(
    limit: int = 1000,
    symbol: str | None = None,
    source: str | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch recent trades_closed sorted by exit_ts desc.

    Args:
        limit: max rows to return.
        symbol: optional symbol filter.
        source: optional source filter.
    """
    where_sql, params = _apply_filters(symbol, source)
    params.append(limit)

    sql = f"""
        SELECT *
        FROM trades_closed
        {where_sql}
        ORDER BY exit_ts DESC
        LIMIT %s
    """

    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()  # type: ignore


def fetch_trade_by_order_id(order_id: str) -> dict[str, Any] | None:
    """Fetch a single closed trade by its order_id."""
    sql = "SELECT * FROM trades_closed WHERE order_id = %s LIMIT 1"
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (order_id,))
        return cur.fetchone()


def fetch_signal_by_id(signal_id: str) -> dict[str, Any] | None:
    """Fetch a single signal by its signal_id from the signals table."""
    sql = "SELECT * FROM signals WHERE signal_id = %s LIMIT 1"
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (signal_id,))
        return cur.fetchone()


def fetch_daily_metrics(
    date: str | None = None,
    symbol: str | None = None,
    source: str | None = None,
    limit: int = 365,
) -> list[dict[str, Any]]:
    """Fetch recent rows from daily_metrics."""
    where_sql, params = _apply_filters(symbol, source)
    if date:
        where_sql = (where_sql + " AND " if where_sql else "WHERE ") + "date = %s"
        params.append(date)
    params.append(limit)

    sql = f"""
        SELECT *
        FROM daily_metrics
        {where_sql}
        ORDER BY date DESC
        LIMIT %s
    """

    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()  # type: ignore


def fetch_entry_tag_metrics(
    date: str | None = None,
    symbol: str | None = None,
    source: str | None = None,
    entry_tag: str | None = None,
    limit: int = 365,
) -> list[dict[str, Any]]:
    """Fetch rows from entry_tag_metrics."""
    where_sql, params = _apply_filters(symbol, source)
    if date:
        where_sql = (where_sql + " AND " if where_sql else "WHERE ") + "date = %s"
        params.append(date)
    if entry_tag:
        where_sql = (where_sql + " AND " if where_sql else "WHERE ") + "entry_tag = %s"
        params.append(entry_tag)
    params.append(limit)

    sql = f"""
        SELECT *
        FROM entry_tag_metrics
        {where_sql}
        ORDER BY date DESC
        LIMIT %s
    """

    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()  # type: ignore


def save_trade_closed(closed: TradeClosed) -> None:  # type: ignore
    """
    Save a closed trade to the analytics database.
    This function handles the mapping from TradeClosed to database columns.
    """
    if TradeClosed is None:
        raise RuntimeError("TradeClosed model not available - domain.models import failed")

    sql = """
        INSERT INTO trades_closed (
            order_id, sid, strategy, source, symbol, tf, direction,
            entry_ts_ms, exit_ts_ms, entry_price, exit_price, lot, notional_usd,
            pnl_net, pnl_gross, fees, pnl_pct,
            pnl_if_fixed_exit, baseline_exit_reason, baseline_exit_ts_ms, baseline_exit_price,
            tp1_hit, tp2_hit, tp3_hit, tp_hits, tp_before_sl,
            trailing_started, trailing_active, trailing_moves, trailing_profile,
            mfe_pnl, mae_pnl, giveback, missed_profit,
            one_r_money, r_multiple, duration_ms,
            close_reason, close_reason_raw, close_reason_detail,
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
            live_surface_applied,
            live_surface_reason_code,
            baseline_sl_price,
            baseline_tp1_price,
            selected_sl_price,
            selected_tp1_price,
            is_virtual,
            meta_enforce_cov_bucket,
            meta_enforce_applied,
            atr_policy_ver, atr_policy_tag, atr_policy_source, atr_policy_scenario,
            atr_policy_regime, atr_policy_bucket, atr_stop_ttl_mode, atr_trailing_mode,
            atr_recovery_run_id, atr_restore_cert_id, atr_restore_cert_status,
            atr_policy_snapshot_json,
            atr_sel_tf, atr_sel_src, atr_sel_age_ms,
            trailing_surface_applied, trailing_surface_reason_code,
            baseline_trailing_offset_atr, selected_trailing_offset_atr,
            strong_gate_ok,
            contract_ver, hold_target_ms, alpha_half_life_ms, max_signal_age_ms,
            risk_horizon_bucket, horizon_profile_source, horizon_profile_conf, horizon_reason_code,
            atr_mode, atr_value, atr_window_n, atr_age_ms, atr_source, atr_pct,
            vol_ratio_fast_slow, vol_ratio_z,
            atr_regime_value, atr_trail_value, atr_regime_tf_ms, atr_trail_tf_ms
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s,
            %s,
            %s,
            %s,
            %s, %s, %s, %s, %s, %s,
            %s,
            %s,
            %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (order_id) DO UPDATE SET
            exit_ts_ms = CASE
                WHEN EXCLUDED.is_final_close THEN EXCLUDED.exit_ts_ms
                ELSE trades_closed.exit_ts_ms
            END,
            exit_price = CASE
                WHEN EXCLUDED.is_final_close THEN EXCLUDED.exit_price
                ELSE trades_closed.exit_price
            END,
            pnl_net = CASE
                WHEN EXCLUDED.is_final_close THEN EXCLUDED.pnl_net
                ELSE trades_closed.pnl_net
            END,
            pnl_gross = CASE
                WHEN EXCLUDED.is_final_close THEN EXCLUDED.pnl_gross
                ELSE trades_closed.pnl_gross
            END,
            fees = EXCLUDED.fees,
            status = EXCLUDED.status,
            remaining_qty = EXCLUDED.remaining_qty,
            is_final_close = trades_closed.is_final_close OR EXCLUDED.is_final_close,
            config_json = COALESCE(EXCLUDED.config_json, trades_closed.config_json)
        WHERE EXCLUDED.is_final_close OR trades_closed.is_final_close = false
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
        ) VALUES (
            %s,
            to_timestamp(%s / 1000.0),
            %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s,
            %s,
            %s, %s,
            now()
        )
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

    # Extract baseline info if available
    baseline_exit_reason = getattr(closed, 'baseline_exit_reason', '')
    baseline_exit_ts_ms = getattr(closed, 'baseline_exit_ts_ms', 0)
    baseline_exit_price = getattr(closed, 'baseline_exit_price', 0.0)

    # Extract entry_tag if available
    entry_tag = getattr(closed, 'entry_tag', '')

    # Extract max_favorable info if available
    max_favorable_price = getattr(closed, 'max_favorable_price', 0.0)
    max_favorable_ts = getattr(closed, 'max_favorable_ts', 0)

    # Extract additional fields
    is_final_close = getattr(closed, 'is_final_close', True)
    remaining_qty = getattr(closed, 'remaining_qty', 0.0)
    status = getattr(closed, 'status', 'closed')
    trailing_profile = getattr(closed, 'trailing_profile', '')

    # Extract health metrics if available (use 0.0 as default to comply with NOT NULL constraints)
    health_l2_stale_ratio_tick = getattr(closed, 'health_l2_stale_ratio_tick', 0.0)
    health_l2_stale_ratio_now = getattr(closed, 'health_l2_stale_ratio_now', 0.0)
    health_avg_l2_age_ms = getattr(closed, 'health_avg_l2_age_ms', 0.0)
    health_avg_l2_age_tick_ms = getattr(closed, 'health_l2_age_tick_ms', 0.0)
    health_signal_emit_rate = getattr(closed, 'health_signal_emit_rate', 0.0)
    health_dlq_rate = getattr(closed, 'health_dlq_rate', 0.0)

    # Config Json + horizon contract snapshot
    signal_payload = getattr(closed, "signal_payload", {}) or {}
    horizon_contract = extract_horizon_contract_from_payload(signal_payload)
    horizon_bucket = extract_horizon_bucket(horizon_contract)
    atr_tf_ms_val = extract_atr_tf_ms(horizon_contract)
    config_snapshot = dict(signal_payload.get("config_snapshot", {}) or {})
    if horizon_contract:
        config_snapshot["_horizon_contract"] = horizon_contract

    # FIX: Enrich config_snapshot with actual computed trade levels.
    # Previously config_json only stored parameter settings (RR levels, mode, etc.)
    # but NOT the computed TP/SL/ATR values, making retrospective analysis impossible.
    try:
        _tp_lvls = getattr(closed, "tp_levels", None) or []
        if _tp_lvls:
            config_snapshot["tp_levels"] = [float(x) for x in _tp_lvls]
        _sl_val = getattr(closed, "sl", 0.0) or getattr(closed, "selected_sl_price", 0.0) or 0.0
        if float(_sl_val) > 0:
            config_snapshot["sl"] = float(_sl_val)
        _atr_val = getattr(closed, "atr", 0.0) or 0.0
        if float(_atr_val) > 0:
            config_snapshot["atr"] = float(_atr_val)
        _tp1_val = getattr(closed, "tp1_price", 0.0) or getattr(closed, "selected_tp1_price", 0.0) or 0.0
        if float(_tp1_val) > 0:
            config_snapshot["tp1_price"] = float(_tp1_val)
    except Exception:
        pass

    # Copy indicators/atr_metrics/meta from signal_payload so generated columns
    # (ind_delta_z, ind_weak_progress, ind_atr_th_bps) receive data for all paths.
    for _sp_key in ("indicators", "atr_metrics", "metrics", "meta"):
        if _sp_key in signal_payload and signal_payload[_sp_key] is not None:
            config_snapshot.setdefault(_sp_key, signal_payload[_sp_key])

    config_snapshot = _sanitize_floats(config_snapshot)
    horizon_contract = _sanitize_floats(horizon_contract)

    # Extract strong_gate_ok from signal indicators (same logic as batch_trade_writer)
    _ind = (signal_payload.get("indicators") or {})
    _sgo_raw = _ind.get("strong_gate_ok", _ind.get("of_confirm_ok", None))
    try:
        _strong_gate_ok = bool(int(_sgo_raw)) if _sgo_raw is not None else None
    except (ValueError, TypeError):
        _strong_gate_ok = None

    params = (
        closed.order_id, closed.sid, closed.strategy, closed.source, closed.symbol, closed.tf, closed.direction,
        closed.entry_ts_ms, closed.exit_ts_ms, closed.entry_price, closed.exit_price, closed.lot, closed.notional_usd,
        closed.pnl_net, closed.pnl_gross, closed.fees, closed.pnl_pct,
        closed.pnl_if_fixed_exit, baseline_exit_reason, baseline_exit_ts_ms, baseline_exit_price,
        closed.tp1_hit, closed.tp2_hit, closed.tp3_hit, closed.tp_hits, closed.tp_before_sl,
        closed.trailing_started, closed.trailing_active, closed.trailing_moves, trailing_profile,
        closed.mfe_pnl, closed.mae_pnl, closed.giveback, closed.missed_profit,
        closed.one_r_money, closed.r_multiple, closed.duration_ms,
        closed.close_reason, getattr(closed, 'close_reason_raw', ''), getattr(closed, 'close_reason_detail', ''),
        entry_tag, max_favorable_price, max_favorable_ts,
        is_final_close, remaining_qty, status,
        # Phase 0.3: first-class scalar horizon/ATR columns (additive, stored as sc_* to avoid clash with jsonb atr_tf_ms)
        getattr(closed, "contract_ver", None) or getattr(closed, "horizon_contract_ver", 2),
        getattr(closed, "risk_horizon_bucket", "") or "",
        getattr(closed, "hold_target_ms", 0) or 0,
        getattr(closed, "alpha_half_life_ms", 0) or 0,
        getattr(closed, "max_signal_age_ms", 0) or 0,
        getattr(closed, "atr_age_ms", 0) or 0,
        getattr(closed, "atr_source", "") or "",
        getattr(closed, "atr_pct", 0.0) or 0.0,
        getattr(closed, "vol_ratio_fast_slow", 1.0) if getattr(closed, "vol_ratio_fast_slow", None) is not None else 1.0,
        getattr(closed, "vol_ratio_z", 0.0) or 0.0,
        # Health metrics
        health_l2_stale_ratio_tick, health_l2_stale_ratio_now,
        health_avg_l2_age_ms, health_avg_l2_age_tick_ms,
        health_signal_emit_rate, health_dlq_rate,
        # Config Json (enriched with horizon snapshot)
        json.dumps(config_snapshot, ensure_ascii=False, sort_keys=True),
        # Horizon contract columns
        Json(horizon_contract) if Json is not None else json.dumps(horizon_contract, ensure_ascii=False),
        horizon_bucket or None,
        atr_tf_ms_val or None,
        getattr(closed, "live_surface_applied", False),
        getattr(closed, "live_surface_reason_code", ""),
        getattr(closed, "baseline_sl_price", 0.0),
        getattr(closed, "baseline_tp1_price", 0.0),
        getattr(closed, "selected_sl_price", 0.0),
        getattr(closed, "selected_tp1_price", 0.0),
        getattr(closed, "is_virtual", False),
        getattr(closed, "meta_enforce_cov_bucket", ""),
        bool(getattr(closed, "meta_enforce_applied", False)),
        getattr(closed, "atr_policy_ver", 0),
        getattr(closed, "atr_policy_tag", ""),
        getattr(closed, "atr_policy_source", ""),
        getattr(closed, "atr_policy_scenario", ""),
        getattr(closed, "atr_policy_regime", ""),
        getattr(closed, "atr_policy_bucket", ""),
        getattr(closed, "atr_stop_ttl_mode", ""),
        getattr(closed, "atr_trailing_mode", ""),
        getattr(closed, "atr_recovery_run_id", ""),
        getattr(closed, "atr_restore_cert_id", ""),
        getattr(closed, "atr_restore_cert_status", ""),
        Json(_sanitize_floats(getattr(closed, "atr_policy_snapshot_json", {}))) if Json is not None else _sanitize_floats(getattr(closed, "atr_policy_snapshot_json", {})),
        # ATR selector (already slots in TradeClosed, set by domain/handlers.py)
        getattr(closed, "atr_sel_tf", ""),
        getattr(closed, "atr_sel_src", ""),
        getattr(closed, "atr_sel_age_ms", 0),
        # Trailing surface A/B
        getattr(closed, "trailing_surface_applied", False),
        getattr(closed, "trailing_surface_reason_code", "") or "",
        getattr(closed, "baseline_trailing_offset_atr", 0.0),
        getattr(closed, "selected_trailing_offset_atr", 0.0),
        # Gate signal
        _strong_gate_ok,
        # Horizon scalars (stamped from PositionState; new TradeClosed slots fix the slots=True barrier)
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
    )

    # ---- P0 extraction (robust fallbacks) ----
    sp = getattr(closed, "signal_payload", {}) or {}

    scenario = getattr(closed, "scenario", None) or sp.get("scenario")
    regime = getattr(closed, "regime", None) or sp.get("regime")
    session = getattr(closed, "session", None) or sp.get("session")
    entry_reason = getattr(closed, "entry_reason", None) or sp.get("entry_reason")

    mae_bps = getattr(closed, "mae_bps", None)
    mfe_bps = getattr(closed, "mfe_bps", None)
    time_to_mfe_ms = getattr(closed, "time_to_mfe_ms", None)
    hold_ms = getattr(closed, "hold_ms", None) or getattr(closed, "duration_ms", None)

    spread_bps_at_entry = getattr(closed, "spread_bps_at_entry", None) or sp.get("spread_bps_at_entry") or sp.get("spread_bps")
    slippage_bps_est = getattr(closed, "slippage_bps_est", None) or sp.get("slippage_bps_est")
    book_age_ms = getattr(closed, "book_age_ms", None) or sp.get("book_age_ms")

    # features snapshot
    features: dict[str, Any] = {}
    f1 = getattr(closed, "features", None)
    if isinstance(f1, dict):
        features = dict(f1)
    else:
        features = dict(sp.get("features") or sp.get("indicators") or {})

    # whitelist + cap
    ALLOW = {
        "delta_z","dn_usd","obi","cvd_slope",
        "absorption_score","weak_progress","vwap_pos",
        "atr_bps","liq_scale","confidence",
        "adverse_bps_t",
        "spread_bps_at_entry","book_age_ms","slippage_bps_est",
        "data_health", "expected_slippage_bps",
        # Slippage decomp / execution risk (P9x)
        "expected_slippage_decomp_bps", "impact_proxy",
        "slip_decomp_coeff_bps", "slip_decomp_spread_bps", "slip_decomp_impact_bps",
        "exec_regime_bucket", "liq_regime_label", "vol_regime_label",
        "spread_bps_submit", "mid_px_submit",
        "taker_flow_imb", "taker_flow_imb_z",
        # Taker-flow contra gate (P9c) — veto/shadow/soft decisions for QA + slippage attribution
        "taker_flow_gate_veto",
        "taker_flow_gate_shadow_veto",
        "taker_flow_gate_soft",
        "taker_flow_gate_reason",
    }
    features = _sanitize_floats({k: features[k] for k in ALLOW if k in features})

    # cap: если риск раздувания JSON — урезаем
    features_json_str = json.dumps(features, ensure_ascii=False)
    if len(features_json_str) > 8000:
        PRIORITY = ["adverse_bps_t","delta_z","dn_usd","obi","weak_progress","absorption_score","confidence"]
        features2 = {k: features.get(k) for k in PRIORITY if k in features}
        features = features2  # уже урезали

    params_p0 = (
        closed.order_id,
        closed.exit_ts_ms,  # for exit_ts (to_timestamp)
        closed.exit_ts_ms,  # for exit_ts_ms
        scenario, regime, session, entry_reason,
        mae_bps, mfe_bps, time_to_mfe_ms, hold_ms,
        spread_bps_at_entry, slippage_bps_est, book_age_ms,
        Json(features),  # psycopg2.extras.Json → jsonb безопасно
        getattr(closed, "is_virtual", False),
        getattr(closed, "meta_enforce_cov_bucket", ""),
        bool(getattr(closed, "meta_enforce_applied", False))
    )

    # Sanitize parameters: replace empty tuples `()` with `None`, and unbox 1-element tuples `(x,)`
    # to prevent psycopg2 syntax errors at or near ")"
    def sanitize_tuple(tup):
        res = []
        for val in tup:
            if val == ():
                res.append(None)
            elif isinstance(val, tuple) and len(val) == 1:
                res.append(val[0])
            else:
                res.append(val)
        return tuple(res)

    params = sanitize_tuple(params)
    params_p0 = sanitize_tuple(params_p0)
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            _TRADES_CLOSED_MAIN_INSERT.inc()  # type: ignore

            if ANALYTICS_P0_ENABLED:
                cur.execute("SAVEPOINT trades_closed_p0_upsert")
                try:
                    cur.execute(sql_p0, params_p0)
                    cur.execute("RELEASE SAVEPOINT trades_closed_p0_upsert")
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT trades_closed_p0_upsert")
                    _TRADES_CLOSED_P0_UPSERT_FAIL.inc()  # type: ignore
                    if ANALYTICS_P0_HARD_FAIL:
                        raise
                    logger.warning("trades_closed_p0 upsert failed", exc_info=True)

            conn.commit()

        # Автоматическая калибровка параметров после сохранения сделки
    except Exception as e:
        import logging
        logging.getLogger("analytics_db").warning("save_trade_closed failed", exc_info=True)
        raise e
    try:
        from services.auto_calibration_service import get_auto_calibration_service
        calibration_service = get_auto_calibration_service()
        calibration_service.on_trade_closed(closed.symbol, closed.source)
    except Exception as e:
        # Не позволяем ошибкам калибровки сломать сохранение сделки
        import logging
        logging.getLogger("analytics_db").warning(f"Auto calibration failed: {e}")

    # Phase 1: horizon profile dirty-mark (best-effort)
    try:
        from services.horizon_profile_bootstrap_service import get_horizon_profile_bootstrap_service
        get_horizon_profile_bootstrap_service().on_trade_closed(closed.symbol, closed.source)
    except Exception:
        pass


def save_trade_closed_async(closed: TradeClosed) -> bool:  # type: ignore[name-defined]
    """Non-blocking version of save_trade_closed using AsyncBatchWriter.

    Enqueues the closed trade into the batch writer buffer (no DB round-trip on
    the calling thread). The batch writer flushes every TRADES_FLUSH_INTERVAL_S
    seconds or whenever the buffer reaches TRADES_BATCH_SIZE rows.

    Returns
    -------
    True  — row successfully enqueued (will be written asynchronously).
    False — batch writer not initialised; caller should fall back to
            save_trade_closed() for synchronous write.
    """
    global _trade_batch_writer, _trade_p0_batch_writer
    if _trade_batch_writer is None:
        if _TRADES_BATCH_ENABLED:
            init_trade_batch_writer()
        if _trade_batch_writer is None:
            return False

    try:
        baseline_exit_reason = getattr(closed, "baseline_exit_reason", "")
        baseline_exit_ts_ms = getattr(closed, "baseline_exit_ts_ms", 0)
        baseline_exit_price = getattr(closed, "baseline_exit_price", 0.0)
        entry_tag = getattr(closed, "entry_tag", "")
        max_favorable_price = getattr(closed, "max_favorable_price", 0.0)
        max_favorable_ts = getattr(closed, "max_favorable_ts", 0)
        is_final_close = getattr(closed, "is_final_close", True)
        remaining_qty = getattr(closed, "remaining_qty", 0.0)
        status = getattr(closed, "status", "closed")
        trailing_profile = getattr(closed, "trailing_profile", "")

        _trade_batch_writer.enqueue({
            "order_id": closed.order_id,
            "sid": closed.sid,
            "strategy": closed.strategy,
            "source": closed.source,
            "symbol": closed.symbol,
            "tf": closed.tf,
            "direction": closed.direction,
            "entry_ts_ms": closed.entry_ts_ms,
            "exit_ts_ms": closed.exit_ts_ms,
            "entry_price": closed.entry_price,
            "exit_price": closed.exit_price,
            "lot": closed.lot,
            "notional_usd": closed.notional_usd,
            "pnl_net": closed.pnl_net,
            "pnl_gross": closed.pnl_gross,
            "fees": closed.fees,
            "pnl_pct": closed.pnl_pct,
            "pnl_if_fixed_exit": closed.pnl_if_fixed_exit,
            "baseline_exit_reason": baseline_exit_reason,
            "baseline_exit_ts_ms": baseline_exit_ts_ms,
            "baseline_exit_price": baseline_exit_price,
            "tp1_hit": closed.tp1_hit,
            "tp2_hit": closed.tp2_hit,
            "tp3_hit": closed.tp3_hit,
            "tp_hits": closed.tp_hits,
            "tp_before_sl": closed.tp_before_sl,
            "trailing_started": closed.trailing_started,
            "trailing_active": closed.trailing_active,
            "trailing_moves": closed.trailing_moves,
            "trailing_profile": trailing_profile,
            "mfe_pnl": closed.mfe_pnl,
            "mae_pnl": closed.mae_pnl,
            "giveback": closed.giveback,
            "missed_profit": closed.missed_profit,
            "one_r_money": closed.one_r_money,
            "r_multiple": closed.r_multiple,
            "duration_ms": closed.duration_ms,
            "close_reason": closed.close_reason,
            "close_reason_raw": getattr(closed, "close_reason_raw", ""),
            "close_reason_detail": getattr(closed, "close_reason_detail", ""),
            "entry_tag": entry_tag,
            "max_favorable_price": max_favorable_price,
            "max_favorable_ts": max_favorable_ts,
            "is_final_close": is_final_close,
            "remaining_qty": remaining_qty,
            "status": status,
            # Phase 0.3: first-class scalar horizon/ATR columns
            "sc_contract_ver": getattr(closed, "contract_ver", None) or getattr(closed, "horizon_contract_ver", 2),
            "sc_risk_horizon_bucket": getattr(closed, "risk_horizon_bucket", "") or "",
            "sc_hold_target_ms": getattr(closed, "hold_target_ms", 0) or 0,
            "sc_alpha_half_life_ms": getattr(closed, "alpha_half_life_ms", 0) or 0,
            "sc_max_signal_age_ms": getattr(closed, "max_signal_age_ms", 0) or 0,
            "sc_atr_age_ms": getattr(closed, "atr_age_ms", 0) or 0,
            "sc_atr_source": getattr(closed, "atr_source", "") or "",
            "sc_atr_pct": getattr(closed, "atr_pct", 0.0) or 0.0,
            "sc_vol_ratio_fast_slow": getattr(closed, "vol_ratio_fast_slow", 1.0) if getattr(closed, "vol_ratio_fast_slow", None) is not None else 1.0,
            "sc_vol_ratio_z": getattr(closed, "vol_ratio_z", 0.0) or 0.0,
            "health_l2_stale_ratio_tick": getattr(closed, "health_l2_stale_ratio_tick", 0.0),
            "health_l2_stale_ratio_now": getattr(closed, "health_l2_stale_ratio_now", 0.0),
            "health_avg_l2_age_ms": getattr(closed, "health_avg_l2_age_ms", 0.0),
            "health_avg_l2_age_tick_ms": getattr(closed, "health_l2_age_tick_ms", 0.0),
            "health_signal_emit_rate": getattr(closed, "health_signal_emit_rate", 0.0),
            "health_dlq_rate": getattr(closed, "health_dlq_rate", 0.0),
            "config_json": json.dumps(
                _sanitize_floats(_enrich_config_snapshot(closed))
            ),
            "is_virtual": getattr(closed, "is_virtual", False),
            "meta_enforce_cov_bucket": getattr(closed, "meta_enforce_cov_bucket", ""),
            "meta_enforce_applied": bool(getattr(closed, "meta_enforce_applied", False)),
            "atr_policy_ver": getattr(closed, "atr_policy_ver", 0),
            "atr_policy_tag": getattr(closed, "atr_policy_tag", ""),
            "atr_policy_source": getattr(closed, "atr_policy_source", ""),
            "atr_policy_scenario": getattr(closed, "atr_policy_scenario", ""),
            "atr_policy_regime": getattr(closed, "atr_policy_regime", ""),
            "atr_policy_bucket": getattr(closed, "atr_policy_bucket", ""),
            "atr_stop_ttl_mode": getattr(closed, "atr_stop_ttl_mode", ""),
            "atr_trailing_mode": getattr(closed, "atr_trailing_mode", ""),
            "atr_recovery_run_id": getattr(closed, "atr_recovery_run_id", ""),
            "atr_restore_cert_id": getattr(closed, "atr_restore_cert_id", ""),
            "atr_restore_cert_status": getattr(closed, "atr_restore_cert_status", ""),
            "atr_policy_snapshot_json": json.dumps(_sanitize_floats(getattr(closed, "atr_policy_snapshot_json", {}))),
        })

        if _trade_p0_batch_writer is not None and ANALYTICS_P0_ENABLED:
            # Replicate P0 extraction logic
            sp = getattr(closed, "signal_payload", {}) or {}
            scenario = getattr(closed, "scenario", None) or sp.get("scenario")
            regime = getattr(closed, "regime", None) or sp.get("regime")
            session = getattr(closed, "session", None) or sp.get("session")
            entry_reason = getattr(closed, "entry_reason", None) or sp.get("entry_reason")

            mae_bps = getattr(closed, "mae_bps", None)
            mfe_bps = getattr(closed, "mfe_bps", None)
            time_to_mfe_ms = getattr(closed, "time_to_mfe_ms", None)
            hold_ms = getattr(closed, "hold_ms", None) or getattr(closed, "duration_ms", None)

            spread_bps_at_entry = getattr(closed, "spread_bps_at_entry", None) or sp.get("spread_bps_at_entry") or sp.get("spread_bps")
            slippage_bps_est = getattr(closed, "slippage_bps_est", None) or sp.get("slippage_bps_est")
            book_age_ms = getattr(closed, "book_age_ms", None) or sp.get("book_age_ms")

            features: dict[str, Any] = {}
            f1 = getattr(closed, "features", None)
            if isinstance(f1, dict):
                features = dict(f1)
            else:
                features = dict(sp.get("features") or sp.get("indicators") or {})

            ALLOW = {
                "delta_z","dn_usd","obi","cvd_slope",
                "absorption_score","weak_progress","vwap_pos",
                "atr_bps","liq_scale","confidence",
                "adverse_bps_t",
                "spread_bps_at_entry","book_age_ms","slippage_bps_est",
                "data_health", "expected_slippage_bps",
                "expected_slippage_decomp_bps", "impact_proxy",
                "slip_decomp_coeff_bps", "slip_decomp_spread_bps", "slip_decomp_impact_bps",
                "exec_regime_bucket", "liq_regime_label", "vol_regime_label",
                "spread_bps_submit", "mid_px_submit",
                "taker_flow_imb", "taker_flow_imb_z",
                "taker_flow_gate_veto", "taker_flow_gate_shadow_veto",
                "taker_flow_gate_soft", "taker_flow_gate_reason",
            }
            features = _sanitize_floats({k: features[k] for k in ALLOW if k in features})
            features_json_str = json.dumps(features, ensure_ascii=False)
            if len(features_json_str) > 8000:
                PRIORITY = ["adverse_bps_t","delta_z","dn_usd","obi","weak_progress","absorption_score","confidence"]
                features = {k: features.get(k) for k in PRIORITY if k in features}

            # Batch writers usually expect dicts, psycopg2 Json wrapper will be applied by db_batch_writer extra_adapter if needed,
            # or we can pass a json string.
            import datetime
            dt = datetime.datetime.fromtimestamp(closed.exit_ts_ms / 1000.0, tz=datetime.UTC).isoformat()

            _trade_p0_batch_writer.enqueue({
                "order_id": closed.order_id,
                "exit_ts": dt,
                "exit_ts_ms": closed.exit_ts_ms,
                "scenario": scenario,
                "regime": regime,
                "session": session,
                "entry_reason": entry_reason,
                "mae_bps": mae_bps,
                "mfe_bps": mfe_bps,
                "time_to_mfe_ms": time_to_mfe_ms,
                "hold_ms": hold_ms,
                "spread_bps_at_entry": spread_bps_at_entry,
                "slippage_bps_est": slippage_bps_est,
                "book_age_ms": book_age_ms,
                "features_json": json.dumps(features, ensure_ascii=False),
                "is_virtual": getattr(closed, "is_virtual", False),
                "meta_enforce_cov_bucket": getattr(closed, "meta_enforce_cov_bucket", ""),
                "meta_enforce_applied": bool(getattr(closed, "meta_enforce_applied", False)),
                "updated_at": dt,
            })

        return True

    except Exception as exc:
        import logging
        logging.getLogger("analytics_db").warning(
            "save_trade_closed_async enqueue failed: %s", exc
        )
        return False


def save_autopilot_proposal(
    sid: str,
    group: str,
    symbol: str,
    regime: str,
    scenario: str,
    winner_arm: str,
    edge_lcb_r: float,
    proposal_json: str,
) -> None:
    """Persist autopilot proposal to DB for audit and manual approval."""
    sql = """
        INSERT INTO autopilot_proposals (
            sid, group_name, symbol, regime, scenario, winner_arm, edge_lcb_r, proposal_json, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'proposed')
        ON CONFLICT (sid) DO UPDATE SET
            edge_lcb_r = EXCLUDED.edge_lcb_r,
            proposal_json = EXCLUDED.proposal_json,
            status = 'proposed'
    """
    params = (sid, group, symbol, regime, scenario, winner_arm, edge_lcb_r, proposal_json)
    with get_conn() as conn, conn.cursor() as cur:
        # Tables might not exist yet, let's be careful or assume they do in prod
        try:
            cur.execute(sql, params)
            conn.commit()
        except Exception as e:
            import logging
            logging.getLogger("analytics_db").warning(f"Failed to save autopilot proposal: {e}")


def update_proposal_status(sid: str, status: str) -> None:
    """Update status of a proposal (e.g. to 'applied')."""
    sql = "UPDATE autopilot_proposals SET status = %s WHERE sid = %s"
    with get_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(sql, (status, sid))
            conn.commit()
        except Exception as e:
            import logging
            logging.getLogger("analytics_db").warning(f"Failed to update proposal status: {e}")
