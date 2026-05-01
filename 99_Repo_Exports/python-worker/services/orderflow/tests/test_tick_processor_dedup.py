from __future__ import annotations
"""
Tests for tick dedup logic: _compute_tick_uid + TickDeduper.

P1 contract under test:
  - Binance trade: dedupe by symbol:trade_id
  - No trade_id + stream_id: dedupe by symbol:mid{stream_id}
  - No trade_id + no stream_id: content hash fallback
  - Same trade_id twice → duplicate
  - Same no-trade-id msg_id twice → duplicate
  - Different msg_id, same content → NOT duplicate (stream_id-aware)
"""

import unittest

from services.orderflow.utils import _compute_tick_uid
from services.orderflow.tick_dedup import TickDeduper

NOW_MS = 1_700_000_100_000


class TestComputeTickUid(unittest.TestCase):

    def test_trade_id_priority(self):
        """trade_id produces {SYMBOL}:{trade_id} — highest priority."""
        uid = _compute_tick_uid(
            symbol="BTCUSDT",
            trade_id=12345,
            ts_ms=NOW_MS,
            price_src="50000",
            qty_src="0.1",
            side="BUY",
            is_buyer_maker=False,
        )
        self.assertEqual(uid, "BTCUSDT:12345")

    def test_stream_id_fallback_when_no_trade_id(self):
        """No trade_id but stream_id → {SYMBOL}:mid{stream_id}."""
        uid = _compute_tick_uid(
            symbol="BTCUSDT",
            trade_id=None,
            ts_ms=NOW_MS,
            price_src="50000",
            qty_src="0.1",
            side="BUY",
            is_buyer_maker=False,
            stream_id="1700000100000-0",
        )
        self.assertEqual(uid, "BTCUSDT:mid1700000100000-0")

    def test_content_hash_when_no_trade_id_no_stream_id(self):
        """No trade_id, no stream_id → hash fallback {SYMBOL}:h{hex}."""
        uid = _compute_tick_uid(
            symbol="ETHUSDT",
            trade_id=None,
            ts_ms=NOW_MS,
            price_src="2500",
            qty_src="0.05",
            side="SELL",
            is_buyer_maker=True,
        )
        self.assertTrue(uid.startswith("ETHUSDT:h"), f"Got: {uid!r}")
        self.assertEqual(len(uid), len("ETHUSDT:h") + 8)

    def test_same_trade_id_produces_same_uid(self):
        """Deterministic: same trade_id always yields same uid."""
        a = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=99, ts_ms=NOW_MS,
            price_src="50000", qty_src="0.1", side="BUY", is_buyer_maker=False,
        )
        b = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=99, ts_ms=NOW_MS + 1000,  # ts doesn't matter
            price_src="99999", qty_src="0.9", side="SELL", is_buyer_maker=True,
        )
        self.assertEqual(a, b)

    def test_different_stream_id_different_uid(self):
        """Different Redis msg_id → different uid (no false dedup)."""
        uid1 = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=None, ts_ms=NOW_MS,
            price_src="50000", qty_src="0.1", side="BUY", is_buyer_maker=False,
            stream_id="1700000100000-0",
        )
        uid2 = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=None, ts_ms=NOW_MS,
            price_src="50000", qty_src="0.1", side="BUY", is_buyer_maker=False,
            stream_id="1700000100001-0",
        )
        self.assertNotEqual(uid1, uid2)

    def test_same_stream_id_same_uid_stable_for_pel(self):
        """Same Redis msg_id (PEL reclaim) → same uid → dedup fires correctly."""
        uid1 = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=None, ts_ms=NOW_MS,
            price_src="50000", qty_src="0.1", side="BUY", is_buyer_maker=False,
            stream_id="1700000100000-0",
        )
        uid2 = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=None, ts_ms=NOW_MS,
            price_src="50000", qty_src="0.1", side="BUY", is_buyer_maker=False,
            stream_id="1700000100000-0",
        )
        self.assertEqual(uid1, uid2)

    def test_trade_id_zero_falls_to_stream_id(self):
        """trade_id=0 is not valid → falls back to stream_id."""
        uid = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=0, ts_ms=NOW_MS,
            price_src="50000", qty_src="0.1", side="BUY", is_buyer_maker=False,
            stream_id="1700000100000-3",
        )
        self.assertEqual(uid, "BTCUSDT:mid1700000100000-3")

    def test_empty_symbol_fallback(self):
        """Empty symbol returns safe fallback uid."""
        uid = _compute_tick_uid(
            symbol="", trade_id=None, ts_ms=NOW_MS,
            price_src="50000", qty_src="0.1", side="BUY", is_buyer_maker=False,
        )
        self.assertTrue(uid.startswith("UNKNOWN:") or uid == "UNKNOWN:h00000000")


class TestTickDeduper(unittest.TestCase):

    def test_first_occurrence_not_duplicate(self):
        dedup = TickDeduper(max_items=100, max_age_ms=60_000)
        self.assertFalse(dedup.seen("BTCUSDT:12345", NOW_MS))

    def test_second_occurrence_is_duplicate(self):
        dedup = TickDeduper(max_items=100, max_age_ms=60_000)
        dedup.seen("BTCUSDT:12345", NOW_MS)
        self.assertTrue(dedup.seen("BTCUSDT:12345", NOW_MS + 100))

    def test_expired_entry_not_duplicate(self):
        dedup = TickDeduper(max_items=100, max_age_ms=5_000)
        dedup.seen("BTCUSDT:99", NOW_MS)
        # After 6s → evicted
        self.assertFalse(dedup.seen("BTCUSDT:99", NOW_MS + 6_000))

    def test_different_uids_not_duplicates(self):
        dedup = TickDeduper(max_items=100, max_age_ms=60_000)
        dedup.seen("BTCUSDT:1", NOW_MS)
        self.assertFalse(dedup.seen("BTCUSDT:2", NOW_MS))

    def test_max_items_eviction(self):
        """Oldest items evicted when max_items exceeded."""
        dedup = TickDeduper(max_items=3, max_age_ms=300_000)
        dedup.seen("uid:1", NOW_MS)
        dedup.seen("uid:2", NOW_MS)
        dedup.seen("uid:3", NOW_MS)
        # Adding uid:4 should evict uid:1
        dedup.seen("uid:4", NOW_MS)
        # uid:1 should no longer be in the set
        self.assertFalse(dedup.seen("uid:1", NOW_MS))

    def test_empty_uid_never_duplicate(self):
        dedup = TickDeduper(max_items=100, max_age_ms=60_000)
        self.assertFalse(dedup.seen("", NOW_MS))
        self.assertFalse(dedup.seen("", NOW_MS + 100))


class TestDedupEndToEndStream(unittest.TestCase):
    """End-to-end test: same no-trade-id + same Redis msg_id → duplicate."""

    def test_same_msg_id_deduped(self):
        """Simulates PEL re-delivery: same msg_id produces same uid → dedup fires."""
        dedup = TickDeduper(max_items=1000, max_age_ms=60_000)
        uid = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=None, ts_ms=NOW_MS,
            price_src="50000", qty_src="0.1", side="BUY", is_buyer_maker=False,
            stream_id="1700000100000-0",
        )
        first = dedup.seen(uid, NOW_MS)
        second = dedup.seen(uid, NOW_MS + 10)
        self.assertFalse(first)
        self.assertTrue(second)

    def test_different_msg_id_not_deduped(self):
        """Different Redis msg_id → different uid → not a duplicate."""
        dedup = TickDeduper(max_items=1000, max_age_ms=60_000)
        uid1 = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=None, ts_ms=NOW_MS,
            price_src="50000", qty_src="0.1", side="BUY", is_buyer_maker=False,
            stream_id="1700000100000-0",
        )
        uid2 = _compute_tick_uid(
            symbol="BTCUSDT", trade_id=None, ts_ms=NOW_MS,
            price_src="50000", qty_src="0.1", side="BUY", is_buyer_maker=False,
            stream_id="1700000100002-0",  # different msg
        )
        self.assertFalse(dedup.seen(uid1, NOW_MS))
        self.assertFalse(dedup.seen(uid2, NOW_MS + 1))


if __name__ == "__main__":
    unittest.main()
