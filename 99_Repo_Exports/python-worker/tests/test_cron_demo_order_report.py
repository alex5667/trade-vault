"""
Tests for cron_demo_order_report.py
"""
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import pytest
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock

from tools.cron_demo_order_report import (
    DemoOrder,
    DemoStats,
    _is_demo_open,
    collect_demo_orders,
    compute_demo_stats,
    build_report_text,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fields(**kw) -> Dict[str, Any]:
    return {k: str(v) for k, v in kw.items()}


def _make_stream_entry(msg_id: str, fields: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    return (msg_id, fields)


def _redis_mock(entries: List[Tuple[str, Dict[str, Any]]]) -> MagicMock:
    """Build a Redis mock that returns entries from xrevrange (one batch, then empty)."""
    r = MagicMock()
    # First call returns entries, second returns empty to stop the loop
    r.xrevrange.side_effect = [entries, []]
    return r


# ---------------------------------------------------------------------------
# Unit: _is_demo_open
# ---------------------------------------------------------------------------

class TestIsDemoOpen:
    def test_is_virtual_true(self):
        assert _is_demo_open({"is_virtual": "true", "action": "open"})
        assert _is_demo_open({"is_virtual": "1", "action": "open"})

    def test_venue_binance_demo(self):
        assert _is_demo_open({"venue": "binance_demo", "action": "open"})

    def test_action_not_open_skipped(self):
        # SL/TP child events should be skipped
        assert not _is_demo_open({"is_virtual": "true", "action": "sl"})
        assert not _is_demo_open({"is_virtual": "true", "action": "tp"})
        assert not _is_demo_open({"is_virtual": "true", "action": "cancel"})

    def test_real_order_rejected(self):
        assert not _is_demo_open({"is_virtual": "false", "venue": "binance_prod", "action": "open"})
        assert not _is_demo_open({"action": "open"})

    def test_action_empty_treated_as_open(self):
        # No action field → treated as open (legacy events)
        assert _is_demo_open({"is_virtual": "true"})


# ---------------------------------------------------------------------------
# Unit: collect_demo_orders
# ---------------------------------------------------------------------------

class TestCollectDemoOrders:
    def _now_ms(self) -> int:
        import time
        return get_ny_time_millis()

    def test_filters_demo_entries(self):
        now = self._now_ms()
        entries = [
            _make_stream_entry(f"{now}-0", _fields(
                is_virtual="true", action="open",
                symbol="BTCUSDT", side="BUY", exec_price="50000",
                qty="0.01", scenario_v4="continuation",
                of_confirm_ok="0", of_confirm_ok_soft="1",
                execution_policy="SAFETY_FIRST", ts_ms=str(now),
            )),
            # Real order — must be excluded
            _make_stream_entry(f"{now - 1000}-0", _fields(
                is_virtual="false", action="open",
                symbol="ETHUSDT", side="SELL", exec_price="3000",
                qty="0.1", ts_ms=str(now - 1000),
            )),
        ]
        r = _redis_mock(entries)
        orders = collect_demo_orders(redis_client=r, stream="orders:exec", since_ms=now - 100_000)
        assert len(orders) == 1
        assert orders[0].symbol == "BTCUSDT"
        assert orders[0].of_confirm_ok == 0
        assert orders[0].of_confirm_ok_soft == 1

    def test_stops_at_since_ms(self):
        now = self._now_ms()
        old_ts = now - 1_000_000  # 1000 seconds ago
        entries = [
            _make_stream_entry(f"{now}-0", _fields(
                is_virtual="true", action="open", symbol="BTCUSDT",
                ts_ms=str(now),
            )),
            # Too old — should stop iteration
            _make_stream_entry(f"{old_ts}-0", _fields(
                is_virtual="true", action="open", symbol="ETHUSDT",
                ts_ms=str(old_ts),
            )),
        ]
        r = _redis_mock(entries)
        since_ms = now - 500_000  # 500 seconds ago
        orders = collect_demo_orders(redis_client=r, stream="orders:exec", since_ms=since_ms)
        # Only the recent entry should be included
        assert all(o.symbol == "BTCUSDT" for o in orders)

    def test_empty_stream(self):
        r = MagicMock()
        r.xrevrange.return_value = []
        orders = collect_demo_orders(redis_client=r, stream="orders:exec", since_ms=0)
        assert orders == []

    def test_chronological_order(self):
        now = self._now_ms()
        t1, t2, t3 = now - 3000, now - 2000, now - 1000
        entries = [
            _make_stream_entry(f"{t3}-0", _fields(is_virtual="true", action="open", symbol="C", ts_ms=str(t3))),
            _make_stream_entry(f"{t2}-0", _fields(is_virtual="true", action="open", symbol="B", ts_ms=str(t2))),
            _make_stream_entry(f"{t1}-0", _fields(is_virtual="true", action="open", symbol="A", ts_ms=str(t1))),
        ]
        r = _redis_mock(entries)
        orders = collect_demo_orders(redis_client=r, stream="orders:exec", since_ms=0)
        assert [o.symbol for o in orders] == ["A", "B", "C"]

    def test_venue_binance_demo_included(self):
        now = self._now_ms()
        entries = [
            _make_stream_entry(f"{now}-0", _fields(
                venue="binance_demo", action="open", symbol="XRPUSDT",
                ts_ms=str(now),
            )),
        ]
        r = _redis_mock(entries)
        orders = collect_demo_orders(redis_client=r, stream="orders:exec", since_ms=now - 10_000)
        assert len(orders) == 1
        assert orders[0].symbol == "XRPUSDT"


# ---------------------------------------------------------------------------
# Unit: compute_demo_stats
# ---------------------------------------------------------------------------

class TestComputeDemoStats:
    def _order(self, **kw) -> DemoOrder:
        defaults = dict(
            sid="s1", symbol="BTCUSDT", side="LONG",
            exec_price=50_000.0, qty=0.01,
            scenario_v4="continuation",
            of_confirm_ok=0, of_confirm_ok_soft=1,
            execution_policy="SAFETY_FIRST",
            ts_ms=1_700_000_000_000,
        )
        defaults.update(kw)
        return DemoOrder(**defaults)

    def test_empty(self):
        s = compute_demo_stats([], since_hours=24.0)
        assert s.n == 0
        assert s.ok_rate == 0.0
        assert s.ok_soft_rate == 0.0

    def test_basic_counts(self):
        orders = [
            self._order(symbol="BTCUSDT", side="LONG", of_confirm_ok_soft=1, scenario_v4="continuation"),
            self._order(symbol="BTCUSDT", side="SHORT", of_confirm_ok_soft=0, scenario_v4="reversal"),
            self._order(symbol="ETHUSDT", side="LONG", of_confirm_ok_soft=1, scenario_v4="continuation"),
        ]
        s = compute_demo_stats(orders, since_hours=24.0)
        assert s.n == 3
        assert s.long_count == 2
        assert s.short_count == 1
        assert "BTCUSDT" in s.by_symbol
        assert "ETHUSDT" in s.by_symbol
        assert s.by_symbol["BTCUSDT"]["n"] == 2
        assert s.by_scenario["continuation"] == 2
        assert s.by_scenario["reversal"] == 1

    def test_ok_rate_zero_for_pure_demo(self):
        orders = [self._order(of_confirm_ok=0, of_confirm_ok_soft=1) for _ in range(5)]
        s = compute_demo_stats(orders, since_hours=24.0)
        assert s.ok_rate == 0.0
        assert s.ok_soft_rate == 1.0

    def test_ok_rate_nonzero_triggers_alert(self):
        orders = [
            self._order(of_confirm_ok=1, of_confirm_ok_soft=1),
            self._order(of_confirm_ok=0, of_confirm_ok_soft=1),
        ]
        s = compute_demo_stats(orders, since_hours=24.0)
        assert s.ok_rate == 0.5  # 1 out of 2

    def test_policy_breakdown(self):
        orders = [
            self._order(execution_policy="SAFETY_FIRST"),
            self._order(execution_policy="SAFETY_FIRST"),
            self._order(execution_policy="MAKER_FIRST"),
        ]
        s = compute_demo_stats(orders, since_hours=24.0)
        assert s.by_policy["SAFETY_FIRST"] == 2
        assert s.by_policy["MAKER_FIRST"] == 1


# ---------------------------------------------------------------------------
# Unit: build_report_text
# ---------------------------------------------------------------------------

class TestBuildReportText:
    def _make_stats(self, **kw) -> DemoStats:
        defaults = dict(
            n=10,
            by_symbol={"BTCUSDT": {"n": 7, "ok": 0, "ok_soft": 7, "long": 5, "short": 2, "ok_rate": 0.0, "ok_soft_rate": 1.0}},
            by_scenario={"continuation": 7, "reversal": 3},
            by_policy={"SAFETY_FIRST": 8, "MAKER_FIRST": 2},
            ok_rate=0.0,
            ok_soft_rate=1.0,
            long_count=7,
            short_count=3,
            since_hours=24.0,
        )
        defaults.update(kw)
        return DemoStats(**defaults)

    def test_header_present(self):
        txt = build_report_text(self._make_stats(), mode="monitor", ts="20240101_120000")
        assert "<b>Demo Order Report</b>" in txt
        assert "monitor" in txt
        assert "20240101_120000" in txt

    def test_empty_report_message(self):
        s = DemoStats(n=0, by_symbol={}, by_scenario={}, by_policy={},
                      ok_rate=0.0, ok_soft_rate=0.0, long_count=0, short_count=0, since_hours=24.0)
        txt = build_report_text(s, mode="monitor", ts="ts")
        assert "нет демо-ордеров" in txt

    def test_symbol_breakdown_shown(self):
        txt = build_report_text(self._make_stats(), mode="monitor", ts="ts")
        assert "BTCUSDT" in txt
        assert "By symbol" in txt

    def test_scenario_breakdown_shown(self):
        txt = build_report_text(self._make_stats(), mode="monitor", ts="ts")
        assert "continuation" in txt
        assert "By scenario" in txt

    def test_ok_rate_alert_fires(self):
        s = self._make_stats(ok_rate=0.5)
        txt = build_report_text(s, mode="monitor", ts="ts", ok_rate_warn=0.0)
        assert "⚠️" in txt
        assert "ok_rate" in txt

    def test_ok_rate_no_alert_when_zero(self):
        s = self._make_stats(ok_rate=0.0)
        txt = build_report_text(s, mode="monitor", ts="ts", ok_rate_warn=0.0)
        assert "⚠️" not in txt

    def test_html_escape_in_symbol(self):
        s = self._make_stats(
            by_symbol={"<WEIRD>": {"n": 1, "ok": 0, "ok_soft": 1, "long": 1, "short": 0, "ok_rate": 0.0, "ok_soft_rate": 1.0}}
        )
        txt = build_report_text(s, mode="monitor", ts="ts")
        assert "<WEIRD>" not in txt  # must be escaped
        assert "&lt;WEIRD&gt;" in txt

    def test_policy_breakdown_shown(self):
        txt = build_report_text(self._make_stats(), mode="monitor", ts="ts")
        assert "SAFETY_FIRST" in txt
        assert "By execution policy" in txt
