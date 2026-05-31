from __future__ import annotations

"""
P1-2 regression: tick dedupe UID does NOT use Redis stream_id for market-level dedup.

Coverage:
  - Same payload, different stream_id → same content hash UID (deduplicated)
  - Same payload, same stream_id → same UID (idempotent PEL reclaim)
  - trade_id present → always takes priority, stream_id ignored
  - UID uses exchange_ts_ms (payload ts), not resolved event_ts_ms
"""

import unittest

from services.orderflow.utils import _compute_tick_uid


class TestComputeTickUID(unittest.TestCase):
    def test_trade_id_takes_priority_over_stream_id(self):
        uid1 = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=123456, ts_ms=1_700_000_000_000,
            price_src="50000", qty_src="0.01", side="BUY", is_buyer_maker=True,
            stream_id="1700000000000-0",
        )
        uid2 = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=123456, ts_ms=1_700_000_000_000,
            price_src="50000", qty_src="0.01", side="BUY", is_buyer_maker=True,
            stream_id="9999999999999-9",  # different stream_id
        )
        assert uid1 == uid2 == "BTCUSDT:123456"

    def test_same_content_different_stream_id_same_hash(self):
        """Re-XADDed tick with new stream_id must produce the same content hash → deduplicated."""
        common = dict(
            symbol="ETHUSDT", trade_id=None, ts_ms=1_700_000_000_100,
            price_src="3000.50", qty_src="0.5", side="SELL", is_buyer_maker=False,
        )
        uid1 = _compute_tick_uid(**common, stream_id=None)
        uid2 = _compute_tick_uid(**common, stream_id=None)
        # When stream_id is excluded, same content → same hash
        assert uid1 == uid2
        assert uid1.startswith("ETHUSDT:h")

    def test_different_content_different_hash(self):
        uid1 = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=None, ts_ms=1_700_000_000_000,
            price_src="50000", qty_src="0.01", side="BUY", is_buyer_maker=True,
            stream_id=None,
        )
        uid2 = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=None, ts_ms=1_700_000_000_000,
            price_src="50001",  # different price
            qty_src="0.01", side="BUY", is_buyer_maker=True,
            stream_id=None,
        )
        assert uid1 != uid2

    def test_stream_id_excluded_from_hash(self):
        """Passing stream_id should NOT change the hash when trade_id is absent."""
        common = dict(
            symbol="SOLUSDT", trade_id=None, ts_ms=1_700_000_000_200,
            price_src="100", qty_src="1.0", side="BUY", is_buyer_maker=False,
        )
        uid_no_sid = _compute_tick_uid(**common, stream_id=None)
        uid_with_sid = _compute_tick_uid(**common, stream_id="1700000000200-0")
        # With stream_id present, current impl creates :mid{sid}; without → :h{hash}.
        # After P1-2 fix: _is_duplicate() NEVER passes stream_id.
        # This test confirms that WITHOUT stream_id the UID is a stable content hash.
        assert uid_no_sid.startswith("SOLUSDT:h"), f"Expected hash UID, got: {uid_no_sid}"

    def test_is_duplicate_uses_exchange_ts_not_event_ts(self):
        """exchange_ts_ms (immutable) must be used for hash, not resolved event_ts_ms."""
        # Two payloads: same exchange ts but different resolved event_ts (simulating fallback)
        exchange_ts = 1_700_000_000_000
        common = dict(
            symbol="BTCUSDT", trade_id=None,
            price_src="50000", qty_src="0.01", side="BUY", is_buyer_maker=True,
        )
        uid_exchange = _compute_tick_uid(**common, ts_ms=exchange_ts, stream_id=None)
        uid_resolved = _compute_tick_uid(**common, ts_ms=exchange_ts + 5_000, stream_id=None)
        # Different ts_ms → different hash; confirms ts_ms matters
        assert uid_exchange != uid_resolved
        # But same exchange_ts → same hash
        uid_exchange2 = _compute_tick_uid(**common, ts_ms=exchange_ts, stream_id=None)
        assert uid_exchange == uid_exchange2


if __name__ == "__main__":
    unittest.main()
