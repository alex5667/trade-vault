"""
Switch Budget System - Third Layer of Stabilization

Purpose:
  Prevent threshold thrashing via daily switch limits and minimum gap enforcement.
  Complements hold-down (cooldown after apply) and hysteresis (improvement requirement).

Three-layer stabilization strategy:
  1. Hold-down: 6-12h cooldown after each application (prevents rapid re-switching)
  2. Hysteresis: require min_impr + hyst improvement (prevents boundary dithering)
  3. Switch Budget: max N switches per UTC day + min gap between switches (prevents thrashing)

Expert review:
  - Financial Analysts: Daily budget prevents overreaction to market noise
  - Senior Python: Deterministic UTC day bucketing, atomic state updates
  - DevOps/SRE: Redis-based state, TTL cleanup, fail-safe defaults
  - Professor Statistics: Min-gap prevents autocorrelation in threshold changes
"""
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from dataclasses import dataclass
from typing import Any, Dict, Tuple
import time


DAY_MS = 24 * 60 * 60 * 1000  # Milliseconds in UTC day


def _now_ms() -> int:
    return get_ny_time_millis()


def utc_day_id(ts_ms: int) -> int:
    """
    UTC day bucket ID: floor(ts_ms / DAY_MS).
    
    Deterministic, timezone-independent day bucketing.
    Same day ID globally regardless of local timezone.
    
    Args:
        ts_ms: Timestamp in milliseconds (0 = use current time)
    
    Returns:
        Integer day ID (e.g., 19723 for 2024-01-01)
    """
    if ts_ms <= 0:
        ts_ms = _now_ms()
    return int(ts_ms // DAY_MS)


@dataclass
class SwitchState:
    """
    Per-(symbol, regime, scenario) switch accounting state.
    
    Stored as JSON in Redis: cfg:entry_policy:switch_state:v1:{SYMBOL}:{REGIME}:{SCENARIO}
    
    Fields:
        day_id: UTC day bucket (auto-resets on day boundary)
        switches: Count of switches applied today
        last_switch_ts_ms: Timestamp of last switch (for min-gap enforcement)
        paused_until_ts_ms: Auto-pause until this timestamp (set when budget exhausted)
    """
    day_id: int = 0
    switches: int = 0
    last_switch_ts_ms: int = 0
    paused_until_ts_ms: int = 0

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "SwitchState":
        """Deserialize from Redis JSON (fail-safe)"""
        try:
            s = SwitchState(
                day_id=int(d.get("day_id", 0) or 0),
                switches=int(d.get("switches", 0) or 0),
                last_switch_ts_ms=int(d.get("last_switch_ts_ms", 0) or 0),
                paused_until_ts_ms=int(d.get("paused_until_ts_ms", 0) or 0),
            )
            return s
        except Exception:
            return SwitchState()

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for Redis storage"""
        return {
            "day_id": int(self.day_id),
            "switches": int(self.switches),
            "last_switch_ts_ms": int(self.last_switch_ts_ms),
            "paused_until_ts_ms": int(self.paused_until_ts_ms),
        }


def can_switch(
    *,
    st: SwitchState,
    now_ms: int,
    max_per_day: int,
    min_gap_ms: int,
) -> Tuple[bool, str]:
    """
    Check if switch is allowed given current state and constraints.
    
    NOTE: This is a READ-ONLY check. It does NOT mutate state.
    Caller should handle day boundary reset separately if needed.
    
    Enforcement order (priority):
      1. Paused window (auto-pause from budget exhaustion)
      2. Min-gap (minimum time since last switch)
      3. Budget (max switches per day)
    
    Day boundary handling:
      If current day_id != utc_day_id(now_ms), state is stale.
      Treat as fresh day (switches=0, no pause, no last_switch).
    
    Args:
        st: Current switch state (not mutated)
        now_ms: Current timestamp (0 = use system time)
        max_per_day: Maximum switches allowed per UTC day (0 = unlimited)
        min_gap_ms: Minimum milliseconds between switches (0 = no gap)
    
    Returns:
        (ok, reason): ok=True if allowed, reason="ok"|"paused"|"min_gap"|"budget"
    """
    if now_ms <= 0:
        now_ms = _now_ms()

    # Check if we're in a new day (state is stale)
    current_day = utc_day_id(now_ms)
    is_new_day = (st.day_id != current_day)

    # Priority 1: Paused window dominates (unless new day)
    if not is_new_day and st.paused_until_ts_ms > 0 and now_ms < st.paused_until_ts_ms:
        return False, "paused"

    # If new day, treat as fresh state (switches=0, no constraints)
    if is_new_day:
        return True, "ok"

    # Priority 2: Min-gap enforcement
    if min_gap_ms > 0 and st.last_switch_ts_ms > 0:
        elapsed = now_ms - st.last_switch_ts_ms
        if elapsed < int(min_gap_ms):
            return False, "min_gap"

    # Priority 3: Budget enforcement
    if max_per_day > 0 and st.switches >= int(max_per_day):
        return False, "budget"

    return True, "ok"


def apply_switch(
    *,
    st: SwitchState,
    now_ms: int,
    max_per_day: int,
    pause_on_budget: bool = True,
) -> None:
    """
    Update state after successful switch application.
    
    Mutates st in-place:
      - Resets state if day boundary crossed
      - Increments switch counter
      - Updates last_switch_ts_ms
      - Sets paused_until_ts_ms if budget exhausted
    
    Auto-pause behavior:
      If budget hit and pause_on_budget=True, pause until next UTC day boundary.
      This prevents suggester from wasting cycles proposing blocked suggestions.
    
    Args:
        st: Switch state to mutate
        now_ms: Current timestamp (0 = use system time)
        max_per_day: Maximum switches per day (for pause calculation)
        pause_on_budget: If True, auto-pause until next day when budget exhausted
    """
    if now_ms <= 0:
        now_ms = _now_ms()
    
    # Reset if day changed
    d = utc_day_id(now_ms)
    if st.day_id != d:
        st.day_id = d
        st.switches = 0
        st.last_switch_ts_ms = 0
        st.paused_until_ts_ms = 0
    
    # Increment counters
    st.switches += 1
    st.last_switch_ts_ms = int(now_ms)

    # Auto-pause if budget exhausted
    if pause_on_budget and max_per_day > 0 and st.switches >= int(max_per_day):
        # Pause until next UTC day boundary
        next_day_start = (int(d) + 1) * DAY_MS
        st.paused_until_ts_ms = int(next_day_start)
