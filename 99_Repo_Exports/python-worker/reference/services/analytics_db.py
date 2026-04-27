#!/usr/bin/env python3
"""
Simple TimescaleDB connector for scanner_analytics.

Usage:
  export TRADES_DB_DSN="postgresql://user:pass@host:5432/scanner_analytics"
  from services.analytics_db import fetch_trades_closed
"""
from __future__ import annotations

import os
import json
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor, Json

try:
    from domain.models import TradeClosed
except ImportError:
    TradeClosed = None

DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/scanner_analytics"
TRADES_DB_DSN = os.getenv("TRADES_DB_DSN", DEFAULT_DSN)

ANALYTICS_P0_ENABLED = os.getenv("ANALYTICS_P0_ENABLED", "1") == "1"
ANALYTICS_P0_HARD_FAIL = os.getenv("ANALYTICS_P0_HARD_FAIL", "0") == "1"


from contextlib import contextmanager
try:
    from psycopg2 import pool
except ImportError:
    pool = None

_POOL = None

def _init_pool():
    global _POOL
    if _POOL is None and pool:
        # Default minconn=1, maxconn=10. Adjust as needed.
        _POOL = pool.SimpleConnectionPool(1, 10, dsn=TRADES_DB_DSN)

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
        conn = psycopg2.connect(TRADES_DB_DSN)
        try:
            yield conn
        finally:
            conn.close()


def _apply_filters(symbol: Optional[str], source: Optional[str]) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []

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
    symbol: Optional[str] = None,
    source: Optional[str] = None,
) -> List[Dict[str, Any]]:
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
        return cur.fetchall()


def fetch_trade_by_order_id(order_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a single closed trade by its order_id."""
    sql = "SELECT * FROM trades_closed WHERE order_id = %s LIMIT 1"
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (order_id,))
        return cur.fetchone()


def fetch_signal_by_id(signal_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a single signal by its signal_id from the signals table."""
    sql = "SELECT * FROM signals WHERE signal_id = %s LIMIT 1"
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (signal_id,))
        return cur.fetchone()


def fetch_daily_metrics(
    date: Optional[str] = None,
    symbol: Optional[str] = None,
    source: Optional[str] = None,
    limit: int = 365,
) -> List[Dict[str, Any]]:
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
        return cur.fetchall()


def fetch_entry_tag_metrics(
    date: Optional[str] = None,
    symbol: Optional[str] = None,
    source: Optional[str] = None,
    entry_tag: Optional[str] = None,
    limit: int = 365,
) -> List[Dict[str, Any]]:
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
        return cur.fetchall()


def save_trade_closed(closed: TradeClosed) -> None:
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
            close_reason, close_reason_raw,
            entry_tag, max_favorable_price, max_favorable_ts,
            is_final_close, remaining_qty, status,
            health_l2_stale_ratio_tick, health_l2_stale_ratio_now,
            health_avg_l2_age_ms, health_avg_l2_age_tick_ms,
            health_signal_emit_rate, health_dlq_rate,
            config_json,
            is_virtual,
            meta_enforce_cov_bucket,
            meta_enforce_applied
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s,
            %s,
            %s,
            %s
        )
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

    params = (
        closed.order_id, closed.sid, closed.strategy, closed.source, closed.symbol, closed.tf, closed.direction,
        closed.entry_ts_ms, closed.exit_ts_ms, closed.entry_price, closed.exit_price, closed.lot, closed.notional_usd,
        closed.pnl_net, closed.pnl_gross, closed.fees, closed.pnl_pct,
        closed.pnl_if_fixed_exit, baseline_exit_reason, baseline_exit_ts_ms, baseline_exit_price,
        closed.tp1_hit, closed.tp2_hit, closed.tp3_hit, closed.tp_hits, closed.tp_before_sl,
        closed.trailing_started, closed.trailing_active, closed.trailing_moves, trailing_profile,
        closed.mfe_pnl, closed.mae_pnl, closed.giveback, closed.missed_profit,
        closed.one_r_money, closed.r_multiple, closed.duration_ms,
        closed.close_reason, getattr(closed, 'close_reason_raw', ''),
        entry_tag, max_favorable_price, max_favorable_ts,
        is_final_close, remaining_qty, status,
        # Health metrics
        health_l2_stale_ratio_tick, health_l2_stale_ratio_now,
        health_avg_l2_age_ms, health_avg_l2_age_tick_ms,
        health_signal_emit_rate, health_dlq_rate,
        # Config Json
        json.dumps(getattr(closed, "signal_payload", {}).get("config_snapshot", {})),
        getattr(closed, "is_virtual", False),
        getattr(closed, "meta_enforce_cov_bucket", ""),
        getattr(closed, "meta_enforce_applied", -1)
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
    features: Dict[str, Any] = {}
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
    features = {k: features[k] for k in ALLOW if k in features}

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
        getattr(closed, "meta_enforce_applied", -1)
    )

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)

        if ANALYTICS_P0_ENABLED:
            try:
                cur.execute(sql_p0, params_p0)
            except Exception:
                # минимальный риск: не роняем основной insert
                if ANALYTICS_P0_HARD_FAIL:
                    raise
                # Log optional here? User showed empty catch. I will stick to minimal risk.
                import logging
                logging.getLogger("analytics_db").warning("P0 upsert failed silently", exc_info=True)

        conn.commit()

    # Автоматическая калибровка параметров после сохранения сделки
    try:
        from services.auto_calibration_service import get_auto_calibration_service
        calibration_service = get_auto_calibration_service()
        calibration_service.on_trade_closed(closed.symbol, closed.source)
    except Exception as e:
        # Не позволяем ошибкам калибровки сломать сохранение сделки
        import logging
        logging.getLogger("analytics_db").warning(f"Auto calibration failed: {e}")



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
