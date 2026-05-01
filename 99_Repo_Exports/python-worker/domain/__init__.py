"""
Domain models and handlers for trade signal processing.

This module provides:
- Signal normalization and validation
- Position state management
- Trade event processing
- Tick price handling
- P&L calculations
""",

from domain.models import (
    SignalNorm,
    Tick,
    TradeEvent,
    TradeClosed,
    PositionState,
    Side,
)

from domain.normalizers import (
    canon_symbol,
    canon_tf,
    canon_strategy,
    canon_source,
    strategy_from_source,
    norm_close_reason,
    bucket_close_reason,
)

from domain.tick_price import (
    build_tick,
    trigger_prices,
)

from domain.calculators import (
    round_to_point,
    calc_trailing_sl,
    update_excursions,
    pnl_pct_simple,
    duration_ms,
    calc_missed_profit,
)

from domain.handlers import (
    create_position,
    apply_trailing_update,
    process_tick,
    finalize_trade,
    EPS_QTY,
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

