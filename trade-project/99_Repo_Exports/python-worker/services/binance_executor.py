#!/usr/bin/env python3
"""binance_executor.py — backward-compatible shim.

The monolith has been decomposed into services/execution/.
This file re-exports BinanceExecutor from the new facade so that all
existing importers (workers, tests, docker entrypoints) continue to work
without any changes.

Rollback: if anything breaks, restore from binance_executor_monolith_backup.py:
    cp services/binance_executor_monolith_backup.py services/binance_executor.py

New architecture:
    services/execution/binance_executor_app.py  ← facade (orchestrator)
    services/execution/order_open_service.py
    services/execution/order_modify_service.py
    services/execution/order_cancel_service.py
    services/execution/protection_service.py
    services/execution/trailing_service.py
    services/execution/reconcile_service.py
    services/execution/active_symbol_guard.py
    services/execution/emergency_flatten_service.py
    services/execution/execution_state_store.py
    services/execution/execution_event_writer.py
    services/execution/binance_filters.py
    services/execution/binance_order_mapper.py
    services/execution/maker_tp_watchdog.py
"""
from __future__ import annotations

from services.execution.binance_executor_app import BinanceExecutor

__all__ = ["BinanceExecutor"]


def main() -> None:  # kept for entrypoint compat
    executor = BinanceExecutor()
    executor.run_forever()


if __name__ == "__main__":
    main()
