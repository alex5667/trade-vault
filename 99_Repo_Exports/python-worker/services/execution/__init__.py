"""services.execution — Binance USDT-M Futures execution layer.

BinanceExecutor is the public façade; all heavy logic lives in
the specialised sub-modules in this package.

Backward-compatible re-export so existing code that does
    from services.binance_executor import BinanceExecutor
continues to work unchanged.
"""
from __future__ import annotations

from services.execution.binance_executor_app import BinanceExecutor  # noqa: F401

__all__ = ["BinanceExecutor"]
