# l2_processing_service.py
"""
L2 processing functionality extracted from base_orderflow_handler.py
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from typing import Optional, Tuple, TYPE_CHECKING
import time

# from common.log import setup_logger
def setup_logger(name):
    import logging
    return logging.getLogger(name)

if TYPE_CHECKING:
    from contexts import OrderflowTickContext


class L2ProcessingService:
    """
    Service for L2 orderbook processing and staleness tracking.
    """

    def __init__(self, symbol: str, l2_max_age_ms: int = 5000, l2_skew_threshold_ms: int = 2000):
        self.symbol = symbol
        self.l2_max_age_ms = l2_max_age_ms
        self.l2_skew_threshold_ms = l2_skew_threshold_ms
        self.l2_age_cap_ms = self.l2_max_age_ms * 10  # reasonable cap for age reporting
        self.logger = setup_logger(f"L2ProcessingService:{symbol}")

        # Staleness tracking
        self._last_l2_warn_wall_ms = 0

    def _ts_to_ms(self, ts: object, *, label: str = "ts") -> int:
        """Convert timestamp to milliseconds."""
        from .utils import normalize_epoch_ms
        return normalize_epoch_ms(ts)

    def _l2_warn_allowed(self, now_wall_ms: int) -> bool:
        """Check if L2 warning is allowed (rate limited)."""
        if now_wall_ms - self._last_l2_warn_wall_ms > 30000:  # 30 seconds wall-clock
            self._last_l2_warn_wall_ms = now_wall_ms
            return True
        return False

    def _calc_l2_age_ms(self, *, tick_ts: object, book_ts: object) -> Tuple[int, int]:
        """
        Calculate L2 age metrics.

        Returns: (delta_ms_signed, skew_ms_abs)
        """
        tick_ms = self._ts_to_ms(tick_ts, label="tick_ts")
        book_ms = self._ts_to_ms(book_ts, label="book_ts")

        delta_ms = tick_ms - book_ms           # signed: + если book старше тика, - если book "в будущем"
        skew_ms = abs(delta_ms)                # рассинхрон времени в любую сторону

        return delta_ms, skew_ms

    def _update_l2_staleness(self, ctx: "OrderflowTickContext", tick_ts_ms: int) -> None:
        """
        Update L2 staleness information in context.
        Consistent with OrderFlowDataProcessor._update_l2_tick_staleness()
        """
        l2_ts = getattr(ctx, 'l2_ts', 0)
        if l2_ts <= 0:
            # No L2 data available - treat as infinitely stale
            delta_ms = 10**9
            age_pos = 10**9
        else:
            # Calculate age metrics
            delta_ms, skew_ms = self._calc_l2_age_ms(tick_ts=tick_ts_ms, book_ts=l2_ts)
            age_pos = max(delta_ms, 0)  # только "насколько устарела" (неотрицательное)

        # Update context with consistent semantics
        ctx.l2_age_ms_raw = delta_ms
        ctx.l2_age_ms = min(age_pos, self.l2_age_cap_ms)  # reasonable cap
        ctx.l2_age_ms_tick_raw = delta_ms
        ctx.l2_age_ms_tick = age_pos

        # Staleness determination
        ctx.l2_is_stale = age_pos >= self.l2_max_age_ms
        ctx.l2_is_stale_now = ctx.l2_is_stale

        # Skew detection (time synchronization issues)
        ctx.l2_skew_ms = abs(delta_ms)
        ctx.l2_skew_flag = abs(delta_ms) >= self.l2_skew_threshold_ms

        # Log warnings for stale data (rate limited by wall clock)
        now_wall_ms = get_ny_time_millis()
        if ctx.l2_is_stale and self._l2_warn_allowed(now_wall_ms):
            self.logger.warning(
                f"L2 data stale: age={age_pos}ms >= max={self.l2_max_age_ms}ms, "
                f"delta_raw={delta_ms}ms, skew={ctx.l2_skew_ms}ms, "
                f"tick_ts={tick_ts_ms}, l2_ts={l2_ts}"
            )

    def is_l2_available(self, ctx: "OrderflowTickContext") -> bool:
        """Check if L2 data is available and not stale."""
        l2_ts = getattr(ctx, 'l2_ts', 0)
        return l2_ts > 0 and not getattr(ctx, 'l2_is_stale', True)


# Test functions for L2 staleness logic
def _test_l2_staleness():
    """Test L2 staleness calculation logic."""
    service = L2ProcessingService("TEST", l2_max_age_ms=1000, l2_skew_threshold_ms=500)

    # Mock context class
    class MockContext:
        def __init__(self):
            self.l2_ts = 0
            self.l2_age_ms = 0
            self.l2_age_ms_raw = 0
            self.l2_age_ms_tick = 0
            self.l2_age_ms_tick_raw = 0
            self.l2_is_stale = False
            self.l2_is_stale_now = False
            self.l2_skew_ms = 0
            self.l2_skew_flag = False

    print("Testing L2 staleness logic...")

    # Test 1: l2_ts=1000000000000, tick_ts=1000000000500 → raw=500, age=500, stale=False (since 500 < 1000)
    ctx1 = MockContext()
    ctx1.l2_ts = 1000000000000  # Large number (> 1e10) to avoid conversion
    service._update_l2_staleness(ctx1, 1000000000500)

    assert ctx1.l2_age_ms_raw == 500, f"Expected raw=500, got {ctx1.l2_age_ms_raw}"
    assert ctx1.l2_age_ms == 500, f"Expected age=500, got {ctx1.l2_age_ms}"
    assert ctx1.l2_age_ms_tick_raw == 500, f"Expected tick_raw=500, got {ctx1.l2_age_ms_tick_raw}"
    assert ctx1.l2_age_ms_tick == 500, f"Expected tick_age=500, got {ctx1.l2_age_ms_tick}"
    assert not ctx1.l2_is_stale, f"Expected not stale, got {ctx1.l2_is_stale}"
    assert ctx1.l2_skew_ms == 500, f"Expected skew=500, got {ctx1.l2_skew_ms}"
    assert ctx1.l2_skew_flag, f"Expected skew flag (500 >= 500), got {ctx1.l2_skew_flag}"
    print("✅ Test 1 passed: Normal case (book older than tick)")

    # Test 2: l2_ts=1000000002000, tick_ts=1000000001500 → raw=-500, age=0, stale=False, skew=500, skew_flag=True
    ctx2 = MockContext()
    ctx2.l2_ts = 1000000002000  # Large number (> 1e10)
    service._update_l2_staleness(ctx2, 1000000001500)

    assert ctx2.l2_age_ms_raw == -500, f"Expected raw=-500, got {ctx2.l2_age_ms_raw}"
    assert ctx2.l2_age_ms == 0, f"Expected age=0, got {ctx2.l2_age_ms}"
    assert ctx2.l2_age_ms_tick_raw == -500, f"Expected tick_raw=-500, got {ctx2.l2_age_ms_tick_raw}"
    assert ctx2.l2_age_ms_tick == 0, f"Expected tick_age=0, got {ctx2.l2_age_ms_tick}"
    assert not ctx2.l2_is_stale, f"Expected not stale, got {ctx2.l2_is_stale}"
    assert ctx2.l2_skew_ms == 500, f"Expected skew=500, got {ctx2.l2_skew_ms}"
    assert ctx2.l2_skew_flag, f"Expected skew flag, got {ctx2.l2_skew_flag}"
    print("✅ Test 2 passed: Book in future (tick older than book)")

    # Test 3: l2_ts=0 → age=10^9, stale=True
    ctx3 = MockContext()
    ctx3.l2_ts = 0
    service._update_l2_staleness(ctx3, 1000000001500)

    expected_huge = 10**9
    assert ctx3.l2_age_ms_raw == expected_huge, f"Expected raw={expected_huge}, got {ctx3.l2_age_ms_raw}"
    assert ctx3.l2_age_ms == min(expected_huge, service.l2_age_cap_ms), f"Expected capped age, got {ctx3.l2_age_ms}"
    assert ctx3.l2_is_stale, f"Expected stale, got {ctx3.l2_is_stale}"
    print("✅ Test 3 passed: No L2 data (infinite staleness)")

    # Test 4: Very stale book (age > max_age)
    ctx4 = MockContext()
    ctx4.l2_ts = 1000000000000  # Large number
    service._update_l2_staleness(ctx4, 1000000002000)  # 2000ms difference

    assert ctx4.l2_age_ms_raw == 2000, f"Expected raw=2000, got {ctx4.l2_age_ms_raw}"
    assert ctx4.l2_age_ms == 2000, f"Expected age=2000, got {ctx4.l2_age_ms}"
    assert ctx4.l2_is_stale, f"Expected stale (2000 >= 1000), got {ctx4.l2_is_stale}"
    print("✅ Test 4 passed: Very stale book")

    print("🎉 All L2 staleness tests passed!")


if __name__ == "__main__":
    _test_l2_staleness()
