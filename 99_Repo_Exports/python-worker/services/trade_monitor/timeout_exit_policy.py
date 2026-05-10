# services/trade_monitor/timeout_exit_policy.py
"""
TIME_EXIT post-trade monitoring policy.

Handles TIME_BE_EXIT / TIME_BE_EXIT_SHADOW events that originate in
TradeMonitorService.on_tick().  This is the **trading logic** layer —
it runs on every price tick and uses the current market price.

Separate from ORPHAN_RECOVERY (orphan_recovery_policy.py) which is the
SRE/infrastructure emergency layer that runs in a background housekeep thread
and may use stale prices.

Extracted from TradeMonitorService.on_tick() inner loop
(monolith lines 3992-4005) and metric dispatch.

Design:
  - Stateless.  No locks; no background threads.
  - Returns Prometheus metric labels only — actual metric `.inc()` calls stay
    in the caller so the policy module has no dependency on prometheus_client.
  - Fail-open: exceptions are swallowed at the caller.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class TimeExitDecision:
    """Describes one TIME_BE_EXIT event extracted from process_tick events."""

    event_type: str      # "TIME_BE_EXIT" | "TIME_BE_EXIT_SHADOW"
    symbol: str
    reason_raw: str
    mode: str            # "ENFORCE" | "SHADOW"
    should_close: bool   # True only for ENFORCE mode


class TimeExitPolicyAnalyzer:
    """
    Stateless extractor for TIME_BE_EXIT decisions from a list of TradeEvents.

    Usage:
        analyzer = TimeExitPolicyAnalyzer()
        decisions = analyzer.extract(events, symbol)
        for d in decisions:
            # increment Prometheus metrics, issue close, etc.
    """

    ENFORCE_EVENT_TYPE = "TIME_BE_EXIT"
    SHADOW_EVENT_TYPE = "TIME_BE_EXIT_SHADOW"

    def extract(
        self,
        events: list[Any],
        symbol: str,
    ) -> list[TimeExitDecision]:
        """
        Extract TIME_BE_EXIT decisions from a list of TradeEvent objects.

        Args:
            events  — list of TradeEvent from CloseDetector.process_tick().
            symbol  — symbol string for metric label.

        Returns:
            List of TimeExitDecision (may be empty).
        """
        decisions: list[TimeExitDecision] = []
        for ev in events or []:
            et = getattr(ev, "event_type", "")
            if et == self.SHADOW_EVENT_TYPE:
                p = ev.payload or {}
                reason = p.get("reason_raw", "unknown")
                decisions.append(
                    TimeExitDecision(
                        event_type=et,
                        symbol=symbol,
                        reason_raw=reason,
                        mode="SHADOW",
                        should_close=False,
                    )
                )
            elif et == self.ENFORCE_EVENT_TYPE:
                p = ev.payload or {}
                reason = p.get("reason_raw", "unknown")
                decisions.append(
                    TimeExitDecision(
                        event_type=et,
                        symbol=symbol,
                        reason_raw=reason,
                        mode="ENFORCE",
                        should_close=True,
                    )
                )
        return decisions

    def should_force_close(self, decisions: list[TimeExitDecision]) -> bool:
        """Return True if any ENFORCE decision is present."""
        return any(d.should_close for d in decisions)
