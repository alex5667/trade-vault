# services/guard.py (snippet)
# Goal: remove synchronous PG write from critical path. TradeMonitorService will submit the returned task
# into its TM_RG_DB executor with bounded pending/backpressure.

from __future__ import annotations
from typing import Callable, Optional
from datetime import datetime

class RegimeGuardService:
    # ... existing __init__ ...

    def get_persist_task(self, key, state, ts_state: datetime) -> Callable[[], None]:
        """Return a callable to run in a background executor."""
        def _task() -> None:
            # IMPORTANT:
            #  - ensure DB client/connection usage is thread-safe:
            #      * use a connection pool, or
            #      * open/close connection inside this task.
            #  - this MUST be idempotent on a natural key if possible.
            self._persist_state_change_sync(key, state, ts_state)
        return _task

    def on_signal_closed(...)-> Optional[Callable[[], None]]:
        # 1) fast in-memory computations / scoring (no IO)
        # 2) decide if we need to persist a regime transition
        # ...
        if not need_persist:
            return None
        return self.get_persist_task(key, state, closed_at)
