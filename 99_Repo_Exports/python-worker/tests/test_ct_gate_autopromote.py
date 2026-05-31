"""Tests for counter_trend_gate_autopromote_v1."""
from __future__ import annotations

import json
import time
import unittest
from unittest.mock import MagicMock, patch


def _make_redis(*, mode: str = "shadow", events: list | None = None) -> MagicMock:
    r = MagicMock()
    r.get.return_value = mode
    if events is None:
        # Build a realistic stream with some SHORT and LONG shadow events
        events = []
        now_ms = int(time.time() * 1000)
        for i in range(210):
            ts_id = f"{now_ms - i * 1000}-0"
            events.append((ts_id, {"direction": "SHORT", "regime": "trending_bull",
                                    "symbol": "BTCUSDT", "kind": "iceberg", "mode": "shadow"}))
        for i in range(110):
            ts_id = f"{now_ms - i * 1000}-0"
            events.append((ts_id, {"direction": "LONG", "regime": "trending_bear",
                                    "symbol": "ETHUSDT", "kind": "continuation", "mode": "shadow"}))
    r.xrevrange.return_value = events
    return r


class TestRunOnce(unittest.TestCase):
    def _run(self, r, env_overrides=None):
        import importlib
        import sys
        # Reload module to pick up env changes
        mod_name = "orderflow_services.counter_trend_gate_autopromote_v1"
        if mod_name in sys.modules:
            del sys.modules[mod_name]
        overrides = {
            "CT_AUTOPROMOTE_ENABLED": "1",
            "CT_AUTOPROMOTE_MIN_SHADOW_SHORT": "200",
            "CT_AUTOPROMOTE_MIN_SHADOW_LONG": "100",
            "CT_AUTOPROMOTE_LOOKBACK_H": "24",
        }
        if env_overrides:
            overrides.update(env_overrides)
        with patch.dict("os.environ", overrides):
            import orderflow_services.counter_trend_gate_autopromote_v1 as svc
            return svc._run_once(r)

    def test_promotes_when_criteria_met(self):
        r = _make_redis(mode="shadow")
        outcome = self._run(r)
        self.assertEqual(outcome, "promoted")
        r.set.assert_any_call("cfg:counter_trend:mode", "enforce")

    def test_skip_when_short_insufficient(self):
        now_ms = int(time.time() * 1000)
        events = [(f"{now_ms - i*1000}-0",
                   {"direction": "SHORT", "regime": "trending_bull",
                    "symbol": "BTCUSDT", "kind": "iceberg", "mode": "shadow"})
                  for i in range(50)]  # only 50, need 200
        events += [(f"{now_ms - i*1000}-0",
                    {"direction": "LONG", "regime": "trending_bear",
                     "symbol": "ETHUSDT", "kind": "cont", "mode": "shadow"})
                   for i in range(110)]
        r = _make_redis(mode="shadow", events=events)
        outcome = self._run(r)
        self.assertEqual(outcome, "skip_short")
        r.set.assert_not_called()

    def test_skip_when_long_insufficient(self):
        now_ms = int(time.time() * 1000)
        events = [(f"{now_ms - i*1000}-0",
                   {"direction": "SHORT", "regime": "trending_bull",
                    "symbol": "BTCUSDT", "kind": "iceberg", "mode": "shadow"})
                  for i in range(210)]
        events += [(f"{now_ms - i*1000}-0",
                    {"direction": "LONG", "regime": "trending_bear",
                     "symbol": "ETHUSDT", "kind": "cont", "mode": "shadow"})
                   for i in range(30)]  # only 30, need 100
        r = _make_redis(mode="shadow", events=events)
        outcome = self._run(r)
        self.assertEqual(outcome, "skip_long")

    def test_no_promote_when_already_enforce(self):
        r = _make_redis(mode="enforce")
        outcome = self._run(r)
        self.assertEqual(outcome, "already_non_shadow")
        r.set.assert_not_called()

    def test_dryrun_does_not_write(self):
        r = _make_redis(mode="shadow")
        outcome = self._run(r, env_overrides={"CT_AUTOPROMOTE_DRYRUN": "1"})
        self.assertEqual(outcome, "dryrun")
        # Should NOT write enforce to Redis
        for call in r.set.call_args_list:
            self.assertNotEqual(call[0][1], "enforce")

    def test_lookback_filters_old_events(self):
        """Events older than lookback_h should not be counted."""
        now_ms = int(time.time() * 1000)
        old_ms = now_ms - 25 * 3600 * 1000  # 25h ago — outside 24h window
        # 300 SHORT events but all old
        events = [(f"{old_ms - i * 1000}-0",
                   {"direction": "SHORT", "regime": "trending_bull",
                    "symbol": "BTCUSDT", "kind": "iceberg", "mode": "shadow"})
                  for i in range(300)]
        r = _make_redis(mode="shadow", events=events)
        outcome = self._run(r, env_overrides={"CT_AUTOPROMOTE_LOOKBACK_H": "24"})
        self.assertEqual(outcome, "skip_short")

    def test_telegram_notification_sent_on_promote(self):
        r = _make_redis(mode="shadow")
        self._run(r)
        # xadd should be called for the Telegram stream
        xadd_streams = [c[0][0] for c in r.xadd.call_args_list]
        self.assertIn("notify:telegram", xadd_streams)

    def test_promotes_with_exact_threshold(self):
        """Exactly meeting the thresholds should promote."""
        now_ms = int(time.time() * 1000)
        events = [(f"{now_ms - i*1000}-0",
                   {"direction": "SHORT", "regime": "trending_bull",
                    "symbol": "BTCUSDT", "kind": "iceberg", "mode": "shadow"})
                  for i in range(200)]  # exactly 200
        events += [(f"{now_ms - i*1000}-0",
                    {"direction": "LONG", "regime": "trending_bear",
                     "symbol": "ETHUSDT", "kind": "cont", "mode": "shadow"})
                   for i in range(100)]  # exactly 100
        r = _make_redis(mode="shadow", events=events)
        outcome = self._run(r)
        self.assertEqual(outcome, "promoted")


class TestModeCache(unittest.TestCase):
    def test_get_mode_returns_redis_value(self):
        from services.counter_trend_runtime_overrides import _ModeCache
        cache = _ModeCache()
        r = MagicMock()
        r.get.return_value = "enforce"
        # First call hits Redis
        mode = cache.get(r, env_fallback="shadow")
        self.assertEqual(mode, "enforce")

    def test_get_mode_falls_back_on_miss(self):
        from services.counter_trend_runtime_overrides import _ModeCache
        cache = _ModeCache()
        r = MagicMock()
        r.get.return_value = None
        mode = cache.get(r, env_fallback="shadow")
        self.assertEqual(mode, "shadow")

    def test_get_mode_falls_back_on_redis_error(self):
        from services.counter_trend_runtime_overrides import _ModeCache
        cache = _ModeCache()
        r = MagicMock()
        r.get.side_effect = Exception("connection refused")
        mode = cache.get(r, env_fallback="shadow")
        self.assertEqual(mode, "shadow")


if __name__ == "__main__":
    unittest.main()
