"""
Domain models and handlers for trade signal processing.

This module provides:
- Signal normalization and validation
- Position state management
- Trade event processing
- Tick price handling
- P&L calculations
"""

from domain.calculators import (
    calc_missed_profit,
    calc_trailing_sl,
    duration_ms,
    pnl_pct_simple,
    round_to_point,
    update_excursions,
)
from domain.handlers import (
    EPS_QTY,
    apply_trailing_update,
    create_position,
    finalize_trade,
    process_tick,
)
from domain.models import (
    PositionState,
    Side,
    SignalNorm,
    Tick,
    TradeClosed,
    TradeEvent,
)
from domain.normalizers import (
    bucket_close_reason,
    canon_source,
    canon_strategy,
    canon_symbol,
    canon_tf,
    norm_close_reason,
    strategy_from_source,
)
from domain.tick_price import (
    build_tick,
    trigger_prices,
)

__all__ = [
    # Models,
    "SignalNorm",
    "Tick",
    "TradeEvent",
    "TradeClosed",
    "PositionState",
    "Side",
    # Normalizers,
    "canon_symbol",
    "canon_tf",
    "canon_strategy",
    "canon_source",
    "strategy_from_source",
    "norm_close_reason",
    "bucket_close_reason",
    # Tick price,
    "build_tick",
    "trigger_prices",
    # Calculators,
    "round_to_point",
    "calc_trailing_sl",
    "update_excursions",
    "pnl_pct_simple",
    "duration_ms",
    "calc_missed_profit",
    # Handlers,
    "create_position",
    "apply_trailing_update",
    "process_tick",
    "finalize_trade",
    "EPS_QTY",
]

