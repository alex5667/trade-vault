# services/trade_monitor/__init__.py
# ---------------------------------------------------------------------------
# Bootstrap re-export layer.
# All public symbols are forwarded from the canonical modules so that any
# existing import of the form:
#
#   from services.trade_monitor import TradeMonitorService
#   from services.trade_monitor import parse_open_position_hash
#   from services.trade_monitor import _extract_regime_from_signal
#   from services.trade_monitor import TM_SIGNAL_VERSION_MISMATCH
#   import services.trade_monitor as tm  (then tm.TradeMonitorService …)
#
# continues to work unchanged during and after decomposition.
# ---------------------------------------------------------------------------
from __future__ import annotations

# Phase 1: everything still lives in _monolith.  During decomposition this
# will be replaced module-by-module with direct imports from the new files.
from services.trade_monitor._monolith import (  # noqa: F401
    TradeMonitorService,
    parse_open_position_hash,
    _extract_regime_from_signal,
    TM_SIGNAL_VERSION_MISMATCH,
    TM_ORPHANS_FORCE_CLOSED,
    TM_OPEN_POSITIONS,
    TM_VIRTUAL_POSITIONS,
    TM_TICK_LATENCY_US,
    TM_ORPHAN_CLEANUP_DURATION_MS,
    TM_RG_PERSIST_PENDING,
    TM_RG_PERSIST_DROPPED,
    TM_RG_PERSIST_SUBMITTED,
    TM_RG_PERSIST_FAILED,
    TM_SIGNAL_BLOCKED_SINGLE_ACTIVE,
    TM_SIGNAL_GUARD_STALE_BYPASS,
    TM_SIMULATED_SLIPPAGE_BPS,
    EXEC_SLIPPAGE_BPS,
    TM_TICK_AGE_MS,
    TM_SIGNAL_DUPLICATE,
    TIME_BE_EXIT_DECISIONS_TOTAL,
    TIME_BE_EXIT_CLOSES_TOTAL,
    TIME_BE_EXIT_SHADOW_WOULD_CLOSE_TOTAL,
    TM_JITTER_BUFFER_SIZE,
    TM_JITTER_RELEASE_LATENCY_MS,
    _IOTask,
    _TickIOBatch,
    _canon_regime,
    _normalize_side,
    _ev_open,
    _ev_tp1_hit_external,
    _apply_entry_regime_to_position,
)

# Re-export internal helpers accessed via monkeypatch in tests
# (e.g. monkeypatch.setattr(tm, "finalize_trade", stub))
from services.trade_monitor._monolith import finalize_trade  # noqa: F401

__all__ = [
    "TradeMonitorService",
    "parse_open_position_hash",
    "_extract_regime_from_signal",
    "TM_SIGNAL_VERSION_MISMATCH",
    "TM_ORPHANS_FORCE_CLOSED",
    "TM_OPEN_POSITIONS",
    "TM_VIRTUAL_POSITIONS",
    "TM_TICK_LATENCY_US",
    "TM_ORPHAN_CLEANUP_DURATION_MS",
    "TM_RG_PERSIST_PENDING",
    "TM_RG_PERSIST_DROPPED",
    "TM_RG_PERSIST_SUBMITTED",
    "TM_RG_PERSIST_FAILED",
    "TM_SIGNAL_BLOCKED_SINGLE_ACTIVE",
    "TM_SIGNAL_GUARD_STALE_BYPASS",
    "TM_SIMULATED_SLIPPAGE_BPS",
    "EXEC_SLIPPAGE_BPS",
    "TM_TICK_AGE_MS",
    "TM_SIGNAL_DUPLICATE",
    "TIME_BE_EXIT_DECISIONS_TOTAL",
    "TIME_BE_EXIT_CLOSES_TOTAL",
    "TIME_BE_EXIT_SHADOW_WOULD_CLOSE_TOTAL",
    "TM_JITTER_BUFFER_SIZE",
    "TM_JITTER_RELEASE_LATENCY_MS",
    "_IOTask",
    "_TickIOBatch",
    "_canon_regime",
    "_normalize_side",
    "_ev_open",
    "_ev_tp1_hit_external",
    "_apply_entry_regime_to_position",
]
