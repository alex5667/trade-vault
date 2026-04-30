"""
Tests for tick_processor timestamp-resolution → DQ-validate pipeline.

P0 contract under test:
  - Timestamp resolution happens BEFORE DQ validate
  - stale payload_ts + fresh stream_id → ts_source=stream_id, NOT dropped
  - future payload_ts + sane stream_id  → ts_source=stream_id, NOT dropped
  - payload_ts in seconds (bad_ts_unit)  → DQ rejects even after resolution
  - out_of_order within 2s               → pass
  - out_of_order beyond 2s              → reject
"""
from __future__ import annotations

import unittest

from services.orderflow.tick_processor import coerce_event_ts_ms, _msgid_to_ms
from core.dq_policy import TickDQPolicy


NOW_MS = 1_700_000_100_000  # fixed wall clock


# ─── coerce_event_ts_ms ──────────────────────────────────────────────────────

class TestCoerceEventTsMs(unittest.TestCase):
    MAX_SKEW = 60_000  # 60s tolerance

    def _coerce(self, payload_ts_ms: int, msg_id: str = "1700000100000-0") -> tuple[int, str]:
        return coerce_event_ts_ms(
            msg_id=msg_id
            payload_ts_ms=payload_ts_ms
            now_ms=NOW_MS
            max_ts_skew_ms=self.MAX_SKEW
        )

    def test_fresh_payload_ts_wins(self):
        ts, src = self._coerce(NOW_MS - 5_000)  # 5s old, within skew
        self.assertEqual(src, "payload")
        self.assertEqual(ts, NOW_MS - 5_000)

    def test_stale_payload_falls_back_to_stream_id(self):
        stale_ts = NOW_MS - 200_000  # 200s old, beyond skew
        ts, src = self._coerce(stale_ts, msg_id="1700000100000-0")
        self.assertEqual(src, "stream_id")
        self.assertEqual(ts, 1_700_000_100_000)  # stream_id ms

    def test_future_payload_falls_back_to_stream_id(self):
        future_ts = NOW_MS + 120_000  # 2min future
        ts, src = self._coerce(future_ts, msg_id="1700000100000-0")
        self.assertEqual(src, "stream_id")

    def test_zero_payload_falls_back_to_stream_id(self):
        ts, src = self._coerce(0, msg_id="1700000100000-1")
        self.assertEqual(src, "stream_id")

    def test_no_stream_id_falls_back_to_now(self):
        stale_ts = NOW_MS - 200_000
        ts, src = self._coerce(stale_ts, msg_id="")
        self.assertEqual(src, "now")
        self.assertEqual(ts, NOW_MS)

    def test_bad_stream_id_falls_back_to_now(self):
        stale_ts = NOW_MS - 200_000
        ts, src = self._coerce(stale_ts, msg_id="INVALID_ID")
        self.assertEqual(src, "now")

    def test_msgid_to_ms(self):
        self.assertEqual(_msgid_to_ms("1700000100000-0"), 1_700_000_100_000)
        self.assertEqual(_msgid_to_ms("1700000100000-5"), 1_700_000_100_000)
        self.assertEqual(_msgid_to_ms("INVALID"), 0)
        self.assertEqual(_msgid_to_ms(""), 0)


# ─── TickDQPolicy two-layer timestamp check ──────────────────────────────────

class TestTickDQPolicyTwoLayerTs(unittest.TestCase):
    """P0: payload_ts_ms → unit check; ts_ms (resolved) → age/skew/OOO check."""

    def setUp(self):
        self.dq = TickDQPolicy(
            max_event_age_ms=10_000
            max_future_skew_ms=2_000
            max_out_of_order_ms=2_000
        )

    def _make(self, *, ts_ms: int, payload_ts_ms: int | None = None) -> dict:
        tick: dict = {
            "symbol": "BTCUSDT"
            "ts_ms": ts_ms
        }
        if payload_ts_ms is not None:
            tick["payload_ts_ms"] = payload_ts_ms
        return tick

    def test_stale_payload_but_fresh_stream_id_passes(self):
        """
        Payload ts was stale (200s ago), but resolved ts_ms = stream_id = fresh.
        DQ must PASS — this is the key P0 scenario.
        """
        stale_raw = NOW_MS - 200_000   # raw payload (seconds-old)
        fresh_resolved = NOW_MS - 500  # resolved via stream_id
        tick = self._make(ts_ms=fresh_resolved, payload_ts_ms=stale_raw)
        ok, reason = self.dq.validate(tick, NOW_MS)
        # payload_ts_ms is stale but it's > 1e11, so no bad_ts_unit.
        # ts_ms (resolved) is fresh → should pass.
        self.assertTrue(ok, f"Expected PASS, got reason={reason}")

    def test_payload_ts_seconds_bad_ts_unit(self):
        """
        payload_ts_ms in seconds → bad_ts_unit even if ts_ms (resolved) is fresh.
        This ensures bad unit payloads are always caught regardless of resolution.
        """
        raw_seconds = 1_700_000_100   # seconds, not ms
        fresh_resolved = NOW_MS - 500
        tick = self._make(ts_ms=fresh_resolved, payload_ts_ms=raw_seconds)
        ok, reason = self.dq.validate(tick, NOW_MS)
        self.assertFalse(ok)
        self.assertEqual(reason, "bad_ts_unit")

    def test_future_payload_but_resolved_ok_passes(self):
        """Future raw ts rescued by stream_id → should pass DQ."""
        future_raw = NOW_MS + 200_000
        fresh_resolved = NOW_MS - 100
        tick = self._make(ts_ms=fresh_resolved, payload_ts_ms=future_raw)
        ok, reason = self.dq.validate(tick, NOW_MS)
        # payload_ts_ms > 1e11, bad_ts_unit doesn't fire.
        # Resolved ts_ms is fresh → should pass.
        self.assertTrue(ok, f"Expected PASS, got reason={reason}")

    def test_stale_resolved_ts_drops(self):
        """Resolved ts is also stale → DQ stale drop."""
        stale_raw = NOW_MS - 100_000
        stale_resolved = NOW_MS - 100_000
        tick = self._make(ts_ms=stale_resolved, payload_ts_ms=stale_raw)
        ok, reason = self.dq.validate(tick, NOW_MS)
        self.assertFalse(ok)
        self.assertEqual(reason, "stale")

    def test_missing_symbol_rejected(self):
        tick = {"ts_ms": NOW_MS, "payload_ts_ms": NOW_MS}
        ok, reason = self.dq.validate(tick, NOW_MS)
        self.assertFalse(ok)
        self.assertEqual(reason, "missing_symbol")

    def test_zero_payload_ts_bad_ts(self):
        tick = {"symbol": "BTCUSDT", "ts_ms": NOW_MS, "payload_ts_ms": 0}
        ok, reason = self.dq.validate(tick, NOW_MS)
        self.assertFalse(ok)
        self.assertEqual(reason, "bad_ts")

    def test_legacy_path_without_payload_ts_ms(self):
        """Backward compat: if payload_ts_ms absent, uses ts_ms for all checks."""
        tick = {"symbol": "BTCUSDT", "ts_ms": NOW_MS - 500}
        ok, reason = self.dq.validate(tick, NOW_MS)
        self.assertTrue(ok, f"Expected PASS, got reason={reason}")

    def test_ooo_within_2s_passes(self):
        """Out-of-order within 2s window should be allowed."""
        dq = TickDQPolicy(max_out_of_order_ms=2_000)
        # Establish a baseline ts
        tick1 = {"symbol": "BTCUSDT", "ts_ms": NOW_MS, "payload_ts_ms": NOW_MS}
        ok1, _ = dq.validate(tick1, NOW_MS)
        self.assertTrue(ok1)

        # Send tick 1s earlier → within OOO window
        tick2 = {"symbol": "BTCUSDT", "ts_ms": NOW_MS - 1_000, "payload_ts_ms": NOW_MS - 1_000}
        ok2, reason2 = dq.validate(tick2, NOW_MS)
        self.assertTrue(ok2, f"Expected OOO within 2s to PASS, got reason={reason2}")

    def test_ooo_beyond_2s_rejected(self):
        """Out-of-order beyond 2s window should be rejected."""
        dq = TickDQPolicy(max_out_of_order_ms=2_000)
        tick1 = {"symbol": "BTCUSDT", "ts_ms": NOW_MS, "payload_ts_ms": NOW_MS}
        dq.validate(tick1, NOW_MS)  # establish baseline

        # Send tick 3s earlier → beyond OOO window
        tick2 = {"symbol": "BTCUSDT", "ts_ms": NOW_MS - 3_000, "payload_ts_ms": NOW_MS - 3_000}
        ok, reason = dq.validate(tick2, NOW_MS)
        self.assertFalse(ok)
        self.assertEqual(reason, "out_of_order")

    def test_future_skew_rejected(self):
        """Future ts beyond skew budget is rejected."""
        future = NOW_MS + 5_000
        tick = {"symbol": "BTCUSDT", "ts_ms": future, "payload_ts_ms": future}
        ok, reason = self.dq.validate(tick, NOW_MS)
        self.assertFalse(ok)
        self.assertEqual(reason, "future_skew")


if __name__ == "__main__":
    unittest.main()
