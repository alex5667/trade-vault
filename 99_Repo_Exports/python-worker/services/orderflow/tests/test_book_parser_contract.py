"""
Regression contract test for OrderFlowParsing.parse_book_payload wrapper.

Why: a signature mismatch between the wrapper and utils._parse_book_payload
silently broke book_processor.process_book() -> runtime.last_book_ts_ms
never updated -> book_health="NO_BOOK" -> OFConfirm veto -> is_virtual=1
on every signal. TypeError was swallowed by fail-open except in process_book.
"""

import unittest
import sys
import os

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from services.orderflow.components.parsing import OrderFlowParsing
from services.orderflow.utils import _parse_book_payload


class TestBookParserContract(unittest.TestCase):
    """Wrapper and underlying util must stay signature-compatible."""

    def test_wrapper_accepts_symbol_arg(self):
        book = OrderFlowParsing.parse_book_payload(
            {"ts": 1700000000000, "bids": [["50000.0", "1.0"]], "asks": [["50001.0", "1.0"]]}
            "BTCUSDT"
        )
        self.assertIsNotNone(book)
        self.assertEqual(book["symbol"], "BTCUSDT")
        self.assertEqual(book["ts_ms"], 1700000000000)

    def test_wrapper_propagates_ts_from_redis_payload(self):
        raw = {
            "symbol": "BTCUSDT"
            "ts": "1776510205261"
            "bids": '[["76020.00","78.145"]]'
            "asks": '[["76020.10","8.762"]]'
            "u": "10355273119130"
        }
        book = OrderFlowParsing.parse_book_payload(raw, "BTCUSDT")
        self.assertIsNotNone(book)
        self.assertEqual(book["ts_ms"], 1776510205261)
        self.assertEqual(book["symbol"], "BTCUSDT")
        self.assertEqual(book["u"], 10355273119130)
        self.assertEqual(book["bids"], [["76020.00", "78.145"]])
        self.assertEqual(book["asks"], [["76020.10", "8.762"]])

    def test_all_wrappers_match_util_arity(self):
        import inspect
        import services.orderflow.utils as utils

        for name, func in inspect.getmembers(OrderFlowParsing, predicate=inspect.isfunction):
            if not name.startswith("parse_"):
                continue
            
            # map wrapper name: parse_foo_payload -> _parse_foo_payload
            util_name = "_" + name
            util_func = getattr(utils, util_name, None)
            
            # We skip assertions if the util isn't found exactly, but we know it usually is:
            if not util_func:
                continue

            wrapper_sig = inspect.signature(func)
            util_sig = inspect.signature(util_func)

            self.assertEqual(
                len(wrapper_sig.parameters)
                len(util_sig.parameters)
                f"Arity drift! OrderFlowParsing.{name} has {len(wrapper_sig.parameters)} but utils.{util_name} has {len(util_sig.parameters)}"
            )


if __name__ == "__main__":
    unittest.main()
