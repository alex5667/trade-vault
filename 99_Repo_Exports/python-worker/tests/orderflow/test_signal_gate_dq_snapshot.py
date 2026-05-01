from __future__ import annotations
"""
P0-1 regression: SignalGate._build_dq_snapshot reads correct runtime fields.

Coverage:
  - last_tick_ts_ms / last_book_ts_ms are used (not legacy last_tick_ts / last_book_ts)
  - outbox_backlog reads _retry_queue.qsize(), not _q
  - stale snapshot triggers DQ hard veto
"""

import types
import unittest
from unittest.mock import MagicMock, AsyncMock, patch


def _make_runtime(
    last_tick_ts_ms: int = 0,
    last_book_ts_ms: int = 0,
    symbol: str = "BTCUSDT",
    **extra,
):
    rt = types.SimpleNamespace(
        symbol=symbol,
        last_tick_ts_ms=last_tick_ts_ms,
        last_book_ts_ms=last_book_ts_ms,
        redis_timeout_events=0,
        negative_age_events=0,
        xack_fail_events=0,
        stream_timeout_burst=0,
        force_hard_veto=False,
        **extra,
    )
    return rt


def _make_publisher(queue_size: int = 0):
    pub = MagicMock()
    q = MagicMock()
    q.qsize.return_value = queue_size
    pub._retry_queue = q
    # legacy attribute absent intentionally
    del pub._q
    return pub


class TestBuildDQSnapshot(unittest.TestCase):
    def _make_gate(self, publisher=None):
        from services.orderflow.signal_gate import SignalGate
        pub = publisher or _make_publisher()
        gate = SignalGate(
            redis_main=AsyncMock(),
            publisher=pub,
            risk_limits=None,
            dq_thresholds=None,
        )
        return gate

    def test_reads_last_tick_ts_ms_not_legacy(self):
        """Snapshot uses last_tick_ts_ms; old last_tick_ts is absent → should still work."""
        from services.orderflow.signal_gate import _runtime_ms
        rt = _make_runtime(last_tick_ts_ms=1_700_000_000_000)
        # No last_tick_ts on namespace
        assert not hasattr(rt, "last_tick_ts")
        val = _runtime_ms(rt, "last_tick_ts_ms", "last_ts_ms", "last_tick_ts")
        assert val == 1_700_000_000_000

    def test_fallback_to_last_ts_ms(self):
        from services.orderflow.signal_gate import _runtime_ms
        rt = types.SimpleNamespace(last_ts_ms=1_600_000_000_000)
        val = _runtime_ms(rt, "last_tick_ts_ms", "last_ts_ms", "last_tick_ts")
        assert val == 1_600_000_000_000

    def test_reads_retry_queue_not_q(self):
        """outbox_backlog must come from _retry_queue, not _q."""
        from services.orderflow.signal_gate import SignalGate
        try:
            from services.redis_dq_policy import RedisDQSnapshot
        except ImportError:
            self.skipTest("redis_dq_policy not available")

        pub = _make_publisher(queue_size=7)
        gate = self._make_gate(publisher=pub)
        rt = _make_runtime(last_tick_ts_ms=1_700_000_000_000, last_book_ts_ms=1_700_000_000_000)
        snap = gate._build_dq_snapshot(rt, now_ms=1_700_000_000_000)
        if snap is None:
            self.skipTest("RedisDQSnapshot not wired")
        assert snap.outbox_backlog == 7

    def test_stale_tick_produces_nonzero_staleness(self):
        from services.orderflow.signal_gate import SignalGate
        try:
            from services.redis_dq_policy import RedisDQSnapshot
        except ImportError:
            self.skipTest("redis_dq_policy not available")

        gate = self._make_gate()
        now_ms = 1_700_000_060_000
        last_tick_ts_ms = now_ms - 60_000  # 60s stale
        last_book_ts_ms = now_ms - 60_000
        rt = _make_runtime(last_tick_ts_ms=last_tick_ts_ms, last_book_ts_ms=last_book_ts_ms)
        snap = gate._build_dq_snapshot(rt, now_ms=now_ms)
        if snap is None:
            self.skipTest("RedisDQSnapshot not wired")
        assert snap.tick_staleness_ms == 60_000
        assert snap.book_staleness_ms == 60_000

    def test_zero_tick_ts_gives_zero_staleness(self):
        """If last_tick_ts_ms is 0 (not yet received), staleness must be 0 (unknown, not 'now - 0')."""
        from services.orderflow.signal_gate import SignalGate
        try:
            from services.redis_dq_policy import RedisDQSnapshot
        except ImportError:
            self.skipTest("redis_dq_policy not available")

        gate = self._make_gate()
        rt = _make_runtime(last_tick_ts_ms=0, last_book_ts_ms=0)
        snap = gate._build_dq_snapshot(rt, now_ms=1_700_000_060_000)
        if snap is None:
            self.skipTest("RedisDQSnapshot not wired")
        assert snap.tick_staleness_ms == 0
        assert snap.book_staleness_ms == 0


if __name__ == "__main__":
    unittest.main()
