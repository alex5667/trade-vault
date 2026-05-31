from __future__ import annotations

import contextlib
from typing import Any

from services.orderflow.metrics import (
    ticks_dropped_total,
    ticks_side_unknown_total,
    ticks_unknown_side_policy_total,
)

class UnknownSidePolicyHandler:
    def __init__(self, side_policy: str, quarantine_writer: Any = None):
        self._side_policy = side_policy
        self._quarantine_writer = quarantine_writer

    async def apply_policy(self, tick: dict, unknown_side: bool, symbol: str, msg_id: str, raw: dict) -> bool:
        """Returns True if tick должен быть пропущен (drop/quarantine)."""
        if not unknown_side:
            return False

        # G0 spec metric: count every unknown-side tick regardless of subsequent policy decision.
        with contextlib.suppress(Exception):
            ticks_side_unknown_total.labels(symbol=symbol).inc()

        with contextlib.suppress(Exception):
            ticks_unknown_side_policy_total.labels(symbol=symbol, policy=str(self._side_policy)).inc()

        pol = str(self._side_policy or "ignore_delta")
        if pol in ("drop", "quarantine"):
            with contextlib.suppress(Exception):
                ticks_dropped_total.labels(symbol=symbol, reason=f"unknown_side_{pol}").inc()
            if pol == "quarantine" and self._quarantine_writer:
                await self._quarantine_writer.quarantine_unknown_side(symbol, msg_id, tick, raw)
            return True

        if pol == "ignore_delta":
            try:
                # P1-FIX: explicitly mark all side-related fields so downstream
                # detectors (delta, CVD) can detect and skip unknown-side ticks.
                # Previously only qty_signed=0 was set, but is_buyer_maker was left
                # untouched, causing LONG-bias in tick_processor direction logic.
                tick["qty_signed"] = 0.0
                tick["aggressor_sign"] = 0
                tick["counted_in_delta"] = False
                tick["side_known"] = False
                tick["side"] = "UNKNOWN"
                tick["side_reason"] = "unknown_side_ignore_delta"
                tick["is_buyer_maker"] = None  # Prevent LONG-bias in downstream ibm checks
            except Exception:
                pass
        return False
