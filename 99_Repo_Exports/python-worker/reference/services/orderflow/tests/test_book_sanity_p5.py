"""Unit tests for P5 book sanity + BookSanityGate.

Covers:
- check_book_sanity: crossed BBO, NaN fields, negative qty, OK book
- trade_outside_bbo: detection + distance computation
- BookSanityGate: monitor / veto modes
"""

from __future__ import annotations

import math
import pytest
from services.orderflow.book_sanity import check_book_sanity, trade_outside_bbo
from services.orderflow.book_sanity_gate import BookSanityGate


class TestCheckBookSanity:
    def _book(self, bb=100.0, ba=100.1, bids=None, asks=None):
        if bids is None:
            bids = [(bb, 10.0), (bb - 0.1, 5.0)]
        if asks is None:
            asks = [(ba, 10.0), (ba + 0.1, 5.0)]
        return type("Book", (), {
            "best_bid_px": bb,
            "best_ask_px": ba,
            "top5_bids": bids,
            "top5_asks": asks,
        })()

    def test_ok_book(self):
        r = check_book_sanity(book=self._book())
        assert r.ok
        assert r.flags == []

    def test_crossed_bbo(self):
        r = check_book_sanity(book=self._book(bb=100.2, ba=100.0))
        assert not r.ok
        assert "crossed_bbo" in r.flags

    def test_missing_bbo(self):
        r = check_book_sanity(book=self._book(bb=0.0, ba=0.0))
        assert not r.ok
        assert "missing_bbo" in r.flags

    def test_nan_px(self):
        bids = [(float("nan"), 10.0), (99.0, 5.0)]
        r = check_book_sanity(book=self._book(bids=bids))
        assert not r.ok
        assert "nan_px" in r.flags

    def test_nan_depth(self):
        asks = [(100.1, float("nan")), (100.2, 5.0)]
        r = check_book_sanity(book=self._book(asks=asks))
        assert not r.ok
        assert "nan_depth" in r.flags

    def test_neg_qty(self):
        bids = [(100.0, -0.5)]
        r = check_book_sanity(book=self._book(bids=bids))
        assert not r.ok
        assert "neg_qty" in r.flags

    def test_dict_input(self):
        r = check_book_sanity(book={"best_bid_px": 100.0, "best_ask_px": 100.1, "top5_bids": [(100.0, 5.0)], "top5_asks": [(100.1, 5.0)]})
        assert r.ok

    def test_fail_open_on_bad_input(self):
        r = check_book_sanity(book=None)  # type: ignore
        assert isinstance(r.flags, list)


class TestTradeOutsideBBO:
    def test_inside_bbo_ok(self):
        outside, dist = trade_outside_bbo(trade_px=100.05, best_bid=100.0, best_ask=100.1)
        assert not outside
        assert dist == pytest.approx(0.0, abs=0.01)

    def test_trade_above_ask(self):
        outside, dist = trade_outside_bbo(trade_px=100.5, best_bid=100.0, best_ask=100.1, eps_bps=0.0)
        assert outside
        assert dist > 0

    def test_trade_below_bid(self):
        outside, dist = trade_outside_bbo(trade_px=99.5, best_bid=100.0, best_ask=100.1, eps_bps=0.0)
        assert outside
        assert dist > 0

    def test_within_eps(self):
        # 0.1 bps tolerance → trade at ask + 0.05%/ask is outside
        outside, _ = trade_outside_bbo(trade_px=100.11, best_bid=100.0, best_ask=100.1, eps_bps=2.0)
        assert not outside  # within 2bps tolerance

    def test_missing_prices_no_flag(self):
        outside, dist = trade_outside_bbo(trade_px=0.0, best_bid=100.0, best_ask=100.1)
        assert not outside


class TestBookSanityGate:
    def _gate(self, mode: str = "veto") -> BookSanityGate:
        return BookSanityGate(enabled=True, mode=mode)

    def test_no_flags_no_action(self):
        g = self._gate()
        dec = g.evaluate(indicators={}, symbol="BTCUSDT")
        assert not dec.apply
        assert not dec.veto

    def test_monitor_mode_no_veto(self):
        g = self._gate(mode="monitor")
        dec = g.evaluate(indicators={"book_sanity_flags": ["crossed_bbo"]}, symbol="BTCUSDT")
        assert dec.apply
        assert not dec.veto

    def test_veto_mode_crossed_bbo(self):
        g = self._gate(mode="veto")
        dec = g.evaluate(indicators={"book_sanity_flags": ["crossed_bbo"]}, symbol="BTCUSDT")
        assert dec.veto
        assert dec.reason_code == "VETO_BOOK_CROSS"

    def test_veto_mode_nan(self):
        g = self._gate(mode="veto")
        dec = g.evaluate(indicators={"book_sanity_flags": ["nan_depth"]}, symbol="BTCUSDT")
        assert dec.veto

    def test_non_veto_flags_in_veto_mode(self):
        # trade_outside_bbo is NOT in veto_flags set
        g = self._gate(mode="veto")
        dec = g.evaluate(indicators={"book_sanity_flags": ["trade_outside_bbo"]}, symbol="BTCUSDT")
        # trade_outside_bbo is not in _ALLOWED_FLAGS, so it gets filtered
        assert not dec.veto

    def test_csv_flags_parsing(self):
        g = self._gate(mode="veto")
        dec = g.evaluate(indicators={"book_sanity_flags": "crossed_bbo,nan_px"}, symbol="BTCUSDT")
        assert dec.veto

    def test_disabled_gate_no_action(self):
        g = BookSanityGate(enabled=False, mode="veto")
        dec = g.evaluate(indicators={"book_sanity_flags": ["crossed_bbo"]}, symbol="X")
        assert not dec.apply
        assert not dec.veto
