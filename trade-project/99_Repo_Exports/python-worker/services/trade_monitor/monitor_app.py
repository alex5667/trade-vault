# services/trade_monitor/monitor_app.py
"""
TradeMonitorApp — thin dependency-injection facade.

This module is the future home of the lean TradeMonitorService implementation
that delegates to the extracted modules:

    PositionStateStore  ← position_state.py
    PositionLoader      ← position_loader.py
    PnlCalculator       ← pnl_calculator.py
    TradeCloseWriter    ← trade_close_writer.py
    TradeEventEmitter   ← trade_event_emitter.py
    TimeExitPolicyAnalyzer ← timeout_exit_policy.py
    OrphanRecoveryPolicy   ← orphan_recovery_policy.py
    CloseDetector          ← close_detector.py

Current status (Phase 1):
  TradeMonitorService still lives in _monolith.py.
  This file documents the target DI wiring so that extracted classes
  can be unit-tested independently while the monolith stays intact.

Migration plan (Phase 2+):
  1. Wire PnlCalculator + TradeCloseWriter into _monolith via __init__.
  2. Replace internal _update_stats_from_dicts / _persist_closed_trade_io
     calls with delegations.
  3. Replace internal orphan loop with OrphanRecoveryPolicy.
  4. Finally delete _monolith.py.
"""
from __future__ import annotations

import logging
from typing import Any

from services.trade_monitor.close_detector import CloseDetector
from services.trade_monitor.orphan_recovery_policy import OrphanRecoveryPolicy
from services.trade_monitor.pnl_calculator import PnlCalculator
from services.trade_monitor.position_loader import PositionLoader
from services.trade_monitor.position_state import PositionStateStore
from services.trade_monitor.timeout_exit_policy import TimeExitPolicyAnalyzer
from services.trade_monitor.trade_close_writer import TradeCloseWriter
from services.trade_monitor.trade_event_emitter import TradeEventEmitter

logger = logging.getLogger(__name__)


def build_trade_monitor_components(
    redis: Any,
    repo: Any,
    db_executor: Any,
    *,
    regime_guard: Any = None,
    events_logger: Any = None,
    analytics_db: Any = None,
    batch_writer: Any = None,
    protective_mirror: Any = None,
    trailing_audit_stream: str = "",
    fsm_enabled: bool = True,
    use_symbol_locks: bool = True,
    attach_health_on_close: bool = True,
    housekeep_interval_ms: int = 30_000,
    orphan_max_price_age_ms: int = 120_000,
    log: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Construct and wire all trade monitor sub-components.

    Returns a dict of named components for easy injection into
    TradeMonitorService.__init__() or test fixtures.

    Example:
        components = build_trade_monitor_components(redis, repo, executor)
        pnl_calc = components["pnl_calc"]
        writer   = components["writer"]
        store    = components["store"]
    """
    _log = log or logger

    store = PositionStateStore(
        redis=redis,
        fsm_enabled=fsm_enabled,
        use_symbol_locks=use_symbol_locks,
        log=_log,
    )

    pnl_calc = PnlCalculator(
        redis=redis,
        regime_guard=regime_guard,
        log=_log,
    )

    writer = TradeCloseWriter(
        redis=redis,
        repo=repo,
        db_executor=db_executor,
        batch_writer=batch_writer,
        analytics_db=analytics_db,
        pnl_calc=pnl_calc,
        attach_health_on_close=attach_health_on_close,
        protective_mirror=protective_mirror,
        log=_log,
    )

    emitter = TradeEventEmitter(
        repo=repo,
        events_logger=events_logger,
        redis=redis,
        trailing_audit_stream=trailing_audit_stream,
        log=_log,
    )

    loader = PositionLoader(
        redis=redis,
        repo=repo,
        add_pos_fn=store.register,
        recover_fsm_fn=store.recover_fsm,
        get_open_symbols_fn=store.open_symbols,
        set_price_fn=store.update_last_price,
        log=_log,
    )

    time_exit_analyzer = TimeExitPolicyAnalyzer()

    orphan_policy = OrphanRecoveryPolicy(
        get_shards_fn=lambda: store.shards,
        pop_pos_fn=store.pop_pos,
        global_lock=store._lock,
        get_symbol_lock_fn=store.get_symbol_lock,
        fsm_transition_fn=store.fsm_transition,
        get_price_fn=store.get_last_price,
        get_last_housekeep_by_symbol_fn=store.get_last_housekeep_by_symbol,
        set_last_housekeep_by_symbol_fn=store.set_last_housekeep_by_symbol,
        get_last_housekeep_ms_fn=store.get_last_housekeep_ms,
        set_last_housekeep_ms_fn=store.set_last_housekeep_ms,
        cleanup_stale_prices_fn=store.cleanup_stale_prices,
        housekeep_interval_ms=housekeep_interval_ms,
        orphan_max_price_age_ms=orphan_max_price_age_ms,
        log=_log,
    )

    close_detector = CloseDetector(log=_log)

    return {
        "store": store,
        "pnl_calc": pnl_calc,
        "writer": writer,
        "emitter": emitter,
        "loader": loader,
        "time_exit_analyzer": time_exit_analyzer,
        "orphan_policy": orphan_policy,
        "close_detector": close_detector,
    }
