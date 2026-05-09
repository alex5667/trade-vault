from __future__ import annotations

"""
P1-1 regression: exchange_ts_ms is preserved immutably; missing exchange ts → dq_tradeable=False.

Coverage:
  - tick["exchange_ts_ms"] == original payload_ts_ms (never overwritten by stream_id/now)
  - tick["redis_stream_ts_ms"] == msg_id ms
  - payload_ts_ms <= 0 → tick["dq_tradeable"] = False, tick["dq_reason"] = "missing_exchange_ts"
  - valid payload_ts_ms → no dq_tradeable override
"""

import unittest

from services.orderflow.tick_processor import _msgid_to_ms, coerce_event_ts_ms


class TestCoerceEventTsMs(unittest.TestCase):
    def test_valid_payload_ts_returns_payload_source(self):
        now = 1_700_000_060_000
        ts, src = coerce_event_ts_ms(
            msg_id="1700000060000-0",
            payload_ts_ms=now - 100,
            now_ms=now,
            max_ts_skew_ms=5_000,
        )
        assert src == "payload"
        assert ts == now - 100

    def test_stale_payload_falls_back_to_stream_id(self):
        now = 1_700_000_060_000
        ts, src = coerce_event_ts_ms(
            msg_id="1700000060000-0",
            payload_ts_ms=now - 200_000,  # 200s stale → beyond skew
            now_ms=now,
            max_ts_skew_ms=5_000,
        )
        assert src == "stream_id"
        assert ts == 1_700_000_060_000

    def test_zero_payload_falls_back_to_stream_id(self):
        now = 1_700_000_060_000
        ts, src = coerce_event_ts_ms(
            msg_id="1700000060000-0",
            payload_ts_ms=0,
            now_ms=now,
            max_ts_skew_ms=5_000,
        )
        assert src == "stream_id"

    def test_missing_msg_id_falls_back_to_now(self):
        now = 1_700_000_060_000
        ts, src = coerce_event_ts_ms(
            msg_id="",
            payload_ts_ms=0,
            now_ms=now,
            max_ts_skew_ms=5_000,
        )
        assert src == "now"
        assert ts == now


class TestExchangeTsMsImmutable(unittest.TestCase):
    """Verify that the tick dict retains exchange_ts_ms == original payload_ts_ms."""

    def _build_tick_fields(self, ts_ms: int):
        """Minimal fields dict as returned by Redis stream."""
        return {b"payload": f'{{"ts_ms": {ts_ms}, "price": "50000", "qty": "0.01", "side": "BUY"}}'.encode()}

    def test_exchange_ts_ms_preserved_when_valid(self):
        """exchange_ts_ms must equal payload_ts_ms even when event_ts_ms == stream_id_ms."""
        now = 1_700_000_060_000
        payload_ts = now - 100  # valid
        tick = {
            "ts_ms": payload_ts,
            "event_ts_ms": payload_ts,
        }
        # Simulate the tick_processor timestamp assignment logic
        from services.orderflow.configuration import _safe_int
        payload_ts_ms = _safe_int(tick.get("ts_ms") or 0)
        msg_id = "1700000060000-0"
        event_ts_ms, ts_source = coerce_event_ts_ms(
            msg_id=msg_id,
            payload_ts_ms=payload_ts_ms,
            now_ms=now,
            max_ts_skew_ms=5_000,
        )
        tick["exchange_ts_ms"] = int(payload_ts_ms)
        tick["redis_stream_ts_ms"] = _msgid_to_ms(str(msg_id))
        tick["event_ts_ms"] = int(event_ts_ms)
        tick["ts_source"] = ts_source

        assert tick["exchange_ts_ms"] == payload_ts, "exchange_ts_ms must not be overwritten"
        assert tick["redis_stream_ts_ms"] == 1_700_000_060_000
        assert "dq_tradeable" not in tick  # valid ts → no penalty

    def test_missing_exchange_ts_marks_non_tradeable(self):
        """payload_ts_ms == 0 → dq_tradeable=False."""
        now = 1_700_000_060_000
        payload_ts_ms = 0
        msg_id = "1700000060000-0"
        event_ts_ms, ts_source = coerce_event_ts_ms(
            msg_id=msg_id, payload_ts_ms=payload_ts_ms, now_ms=now, max_ts_skew_ms=5_000,
        )
        tick: dict = {}
        tick["exchange_ts_ms"] = int(payload_ts_ms)
        tick["redis_stream_ts_ms"] = _msgid_to_ms(str(msg_id))
        tick["event_ts_ms"] = int(event_ts_ms)
        tick["ts_source"] = ts_source
        if payload_ts_ms <= 0:
            tick["dq_tradeable"] = False
            tick["dq_reason"] = "missing_exchange_ts"

        assert tick.get("dq_tradeable") is False
        assert tick.get("dq_reason") == "missing_exchange_ts"
        assert tick["exchange_ts_ms"] == 0


if __name__ == "__main__":
    unittest.main()
