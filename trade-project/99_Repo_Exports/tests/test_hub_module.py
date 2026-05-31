# -*- coding: utf-8 -*-
"""
Unit tests for the hub package.

All external dependencies (redis, SnapshotBuilder, FilteredSignalWriter,
OrderPushDispatcher, ParquetLabelSink, detectors) are mocked so the suite
runs without any Docker services.
"""

from __future__ import annotations

import logging
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, PropertyMock

# ---------------------------------------------------------------------------
# The modules under test (import order matters for patching)
# ---------------------------------------------------------------------------
from hub._hub_utils import HubScore, build_logger, is_near_pivot


# ===========================================================================
# Tests: _hub_utils
# ===========================================================================

class TestHubScore(unittest.TestCase):
    def test_basic_fields(self):
        hs = HubScore(confidence=0.75, dir_up=True, reason="test")
        self.assertEqual(hs.confidence, 0.75)
        self.assertIs(hs.dir_up, True)
        self.assertEqual(hs.reason, "test")
        self.assertEqual(hs.metrics, {})  # default_factory

    def test_with_metrics(self):
        m = {"z_delta": 3.1, "trigger": True}
        hs = HubScore(confidence=0.5, dir_up=False, reason="r", metrics=m)
        self.assertEqual(hs.metrics["z_delta"], 3.1)
        self.assertTrue(hs.metrics["trigger"])

    def test_dir_up_none(self):
        hs = HubScore(confidence=0.0, dir_up=None, reason="")
        self.assertIsNone(hs.dir_up)


class TestBuildLogger(unittest.TestCase):
    def test_returns_logger(self):
        log = build_logger("test.hub.utils")
        self.assertIsInstance(log, logging.Logger)

    def test_idempotent_handlers(self):
        name = "test.hub.idempotent"
        log1 = build_logger(name, "DEBUG")
        log2 = build_logger(name, "DEBUG")
        self.assertIs(log1, log2)
        self.assertEqual(len(log1.handlers), 1)

    def test_level_set(self):
        log = build_logger("test.hub.level", "WARNING")
        self.assertEqual(log.level, logging.WARNING)


class TestIsNearPivot(unittest.TestCase):
    PIVOTS = {"P": 2000.0, "R1": 2010.0, "S1": 1990.0, "cam_R3": 2030.0, "cam_S3": 1970.0}
    ATR = 10.0  # threshold = 10 * 0.5 = 5.0

    def test_price_at_pivot(self):
        self.assertTrue(is_near_pivot(2000.0, self.PIVOTS, self.ATR))

    def test_price_within_threshold(self):
        self.assertTrue(is_near_pivot(2004.0, self.PIVOTS, self.ATR))  # within 5.0 of P

    def test_price_outside_threshold(self):
        # R1=2010: |2016 - 2010| = 6.0 > threshold(5.0) → False
        # cam_R3=2030: |2016 - 2030| = 14.0 > threshold(5.0) → False
        self.assertFalse(is_near_pivot(2016.0, self.PIVOTS, self.ATR))

    def test_near_s1(self):
        self.assertTrue(is_near_pivot(1987.0, self.PIVOTS, self.ATR))  # 3.0 < 5.0 of S1

    def test_near_cam_r3(self):
        self.assertTrue(is_near_pivot(2028.0, self.PIVOTS, self.ATR))

    def test_empty_pivots(self):
        self.assertFalse(is_near_pivot(2000.0, {}, self.ATR))

    def test_zero_atr(self):
        self.assertFalse(is_near_pivot(2000.0, self.PIVOTS, 0.0))

    def test_zero_price(self):
        self.assertFalse(is_near_pivot(0.0, self.PIVOTS, self.ATR))

    def test_custom_mult(self):
        # mult=2.0 → threshold=20; price=2019 is 1 away from R1(2010) → True
        self.assertTrue(is_near_pivot(2019.0, self.PIVOTS, self.ATR, mult=2.0))


# ===========================================================================
# Helpers: build mock cfg and snap
# ===========================================================================

def _make_cfg(**kwargs):
    defaults = dict(
        symbol="XAUUSD",
        z_delta_thr=3.0,
        z_extreme_thr=4.5,
        speed_z_thr=3.0,
        poll_ms=500,
        redis_url="redis://localhost:6379/15",
        logger_name="hub.test",
        log_level="DEBUG",
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _make_snap(bid=2000.0, ask=2000.5, last=None, atr=10.0, dom=None, pivots=None, ts=None):
    return {
        "ts": ts or int(time.time() * 1000),
        "atr": atr,
        "dom": dom or [],
        "pivots": pivots or {},
        "tick": {
            "bid": bid,
            "ask": ask,
            "last": last,
            "ts": int(time.time() * 1000),
        },
    }


# ===========================================================================
# Tests: AggregatedSignalHub
# ===========================================================================

PATCH_BASE = "hub.aggregated_signal_hub"


class TestAggregatedSignalHub(unittest.TestCase):
    def _make_hub(self):
        with (
            patch(f"{PATCH_BASE}.SnapshotBuilder"),
            patch(f"{PATCH_BASE}.MicrostructureSpikeDetector"),
            patch(f"{PATCH_BASE}.OrderPushDispatcher"),
            patch(f"{PATCH_BASE}.FilteredSignalWriter"),
            patch(f"{PATCH_BASE}.ParquetLabelSink"),
        ):
            from hub.aggregated_signal_hub import AggregatedSignalHub
            r = MagicMock()
            cfg = _make_cfg()
            log = build_logger("test.base.hub", "DEBUG")
            hub = AggregatedSignalHub(r, cfg, log)
        return hub

    def test_score_below_threshold_returns_low_conf(self):
        hub = self._make_hub()
        hub.detector.update.return_value = {
            "trigger": False, "extreme": False, "z_delta": 0.0, "z_speed": 0.0, "dir_up": None
        }
        from core.smart_cluster_analyzer import SmartClusterAnalyzer
        with patch.object(SmartClusterAnalyzer, "analyze_from_dom", return_value={
            "imbalance_score": 0.0, "absorption_score": 0.0, "direction": None
        }):
            sc = hub._score(_make_snap())
        self.assertEqual(sc.confidence, 0.0)
        self.assertIsNone(sc.dir_up)

    def test_score_trigger_adds_conf(self):
        hub = self._make_hub()
        hub.detector.update.return_value = {
            "trigger": True, "extreme": False, "z_delta": 3.5, "z_speed": 3.2, "dir_up": True
        }
        from core.smart_cluster_analyzer import SmartClusterAnalyzer
        with patch.object(SmartClusterAnalyzer, "analyze_from_dom", return_value={
            "imbalance_score": 0.0, "absorption_score": 0.0, "direction": None
        }):
            sc = hub._score(_make_snap())
        self.assertAlmostEqual(sc.confidence, 0.35)
        self.assertTrue(sc.dir_up)

    def test_score_trigger_and_extreme(self):
        hub = self._make_hub()
        hub.detector.update.return_value = {
            "trigger": True, "extreme": True, "z_delta": 5.0, "z_speed": 4.5, "dir_up": False
        }
        from core.smart_cluster_analyzer import SmartClusterAnalyzer
        with patch.object(SmartClusterAnalyzer, "analyze_from_dom", return_value={
            "imbalance_score": 0.0, "absorption_score": 0.0, "direction": None
        }):
            sc = hub._score(_make_snap())
        self.assertAlmostEqual(sc.confidence, 0.50)  # 0.35 + 0.15
        self.assertFalse(sc.dir_up)

    def test_score_direction_overridden_by_cluster_buy(self):
        hub = self._make_hub()
        hub.detector.update.return_value = {
            "trigger": True, "extreme": False, "z_delta": 3.5, "z_speed": 3.2, "dir_up": None
        }
        from core.smart_cluster_analyzer import SmartClusterAnalyzer
        with patch.object(SmartClusterAnalyzer, "analyze_from_dom", return_value={
            "imbalance_score": 0.8, "absorption_score": 0.5, "direction": "buy"
        }):
            sc = hub._score(_make_snap())
        self.assertTrue(sc.dir_up)

    def test_step_skipped_below_threshold(self):
        hub = self._make_hub()
        hub.snapshot.build.return_value = _make_snap()
        hub.detector.update.return_value = {
            "trigger": False, "extreme": False, "z_delta": 0.0, "z_speed": 0.0, "dir_up": None
        }
        from core.smart_cluster_analyzer import SmartClusterAnalyzer
        with patch.object(SmartClusterAnalyzer, "analyze_from_dom", return_value={
            "imbalance_score": 0.0, "absorption_score": 0.0, "direction": None
        }):
            hub.step()
        hub.writer.write_and_push.assert_not_called()

    def test_step_emits_signal_above_threshold(self):
        hub = self._make_hub()
        snap = _make_snap(bid=2000.0, ask=2000.5, last=2000.2, atr=10.0)
        hub.snapshot.build.return_value = snap

        hub.detector.update.return_value = {
            "trigger": True, "extreme": True, "z_delta": 5.0, "z_speed": 4.5, "dir_up": True
        }
        fs_mock = MagicMock(price=2000.2, sl=1990.0, tp_levels=[2010.0], lot=0.1)
        hub.writer.write_and_push.return_value = fs_mock

        from core.smart_cluster_analyzer import SmartClusterAnalyzer
        with patch.object(SmartClusterAnalyzer, "analyze_from_dom", return_value={
            "imbalance_score": 1.0, "absorption_score": 0.6, "direction": "buy"
        }):
            hub.step()

        hub.writer.write_and_push.assert_called_once()
        call_kwargs = hub.writer.write_and_push.call_args.kwargs
        self.assertEqual(call_kwargs["side"], "LONG")
        self.assertEqual(call_kwargs["source"], "AggregatedHub")
        hub.label_sink.write.assert_called_once()


# ===========================================================================
# Tests: AggregatedSignalHubPro
# ===========================================================================

PATCH_PRO = "hub.aggregated_signal_hub_pro"


class TestAggregatedSignalHubPro(unittest.TestCase):
    def _make_hub(self):
        with (
            patch(f"{PATCH_PRO}.SnapshotBuilder"),
            patch(f"{PATCH_PRO}.MicrostructureSpikeDetector"),
            patch(f"{PATCH_PRO}.MicrostructureSpikeDetectorPro"),
            patch(f"{PATCH_PRO}.OrderPushDispatcher"),
            patch(f"{PATCH_PRO}.FilteredSignalWriter"),
            patch(f"{PATCH_PRO}.ParquetLabelSink"),
        ):
            from hub.aggregated_signal_hub_pro import AggregatedSignalHubPro
            r = MagicMock()
            cfg = _make_cfg()
            log = build_logger("test.pro.hub", "DEBUG")
            hub = AggregatedSignalHubPro(r, cfg, log)
        return hub

    def _no_trigger_pro_metrics(self, trades_count=0):
        return {
            "trades_in_window": trades_count,
            "z_delta": 0.0,
            "z_speed": 0.0,
            "z_range": 0.0,
            "svbp_imbalance": 0.0,
            "svbp_top": {},
            "trigger": False,
            "extreme": False,
            "dir_up": None,
        }

    def test_stats_initial(self):
        hub = self._make_hub()
        self.assertEqual(hub.stats["signals_total"], 0)
        self.assertEqual(hub.stats["signals_emitted"], 0)

    def test_score_uses_legacy_when_few_trades(self):
        hub = self._make_hub()
        hub.detector_legacy.update.return_value = {
            "trigger": False, "extreme": False, "z_delta": 0.0, "z_speed": 0.0, "dir_up": None
        }
        hub.detector_pro.metrics.return_value = self._no_trigger_pro_metrics(trades_count=0)

        sc = hub._score(_make_snap())
        self.assertEqual(sc.confidence, 0.0)
        self.assertEqual(hub.stats["legacy_detector_used"], 1)
        self.assertEqual(hub.stats["pro_detector_used"], 0)

    def test_score_uses_pro_when_enough_trades(self):
        hub = self._make_hub()
        hub.min_trades_for_pro = 5
        pro_metrics = self._no_trigger_pro_metrics(trades_count=10)
        pro_metrics["trigger"] = True
        pro_metrics["z_delta"] = 4.0
        pro_metrics["z_speed"] = 3.5
        pro_metrics["svbp_imbalance"] = 0.6
        pro_metrics["dir_up"] = True
        hub.detector_pro.metrics.return_value = pro_metrics

        sc = hub._score(_make_snap())
        self.assertEqual(hub.stats["pro_detector_used"], 1)
        # 0.45 (trigger) + 0.20 (svbp) + 0.05 (real_delta) = 0.70
        self.assertAlmostEqual(sc.confidence, 0.70, places=5)
        self.assertTrue(sc.dir_up)

    def test_score_dir_falls_back_to_svbp(self):
        hub = self._make_hub()
        hub.min_trades_for_pro = 5
        pro_metrics = self._no_trigger_pro_metrics(trades_count=10)
        pro_metrics["trigger"] = True
        pro_metrics["z_delta"] = 4.0
        pro_metrics["z_speed"] = 3.5
        pro_metrics["svbp_imbalance"] = -0.7
        pro_metrics["dir_up"] = None  # detector undecided
        hub.detector_pro.metrics.return_value = pro_metrics

        sc = hub._score(_make_snap())
        # svbp < 0 → dir_up = False
        self.assertFalse(sc.dir_up)

    def test_step_increments_signals_total(self):
        hub = self._make_hub()
        hub.snapshot.build.return_value = _make_snap()
        hub.r.xread.return_value = []
        hub.detector_pro.metrics.return_value = self._no_trigger_pro_metrics()
        hub.detector_legacy.update.return_value = {
            "trigger": False, "extreme": False, "z_delta": 0.0, "z_speed": 0.0, "dir_up": None
        }
        hub.step()
        self.assertEqual(hub.stats["signals_total"], 1)

    def test_step_emits_and_increments_emitted(self):
        hub = self._make_hub()
        snap = _make_snap(bid=2000.0, ask=2000.5, last=2000.2, atr=10.0)
        hub.snapshot.build.return_value = snap
        hub.r.xread.return_value = []
        hub.min_trades_for_pro = 5

        pro_metrics = self._no_trigger_pro_metrics(trades_count=10)
        pro_metrics["trigger"] = True
        pro_metrics["extreme"] = True
        pro_metrics["z_delta"] = 5.0
        pro_metrics["z_speed"] = 4.5
        pro_metrics["svbp_imbalance"] = 0.8
        pro_metrics["dir_up"] = True
        hub.detector_pro.metrics.return_value = pro_metrics

        fs_mock = MagicMock(price=2000.2, sl=1990.0, tp_levels=[2010.0], lot=0.1)
        hub.writer.write_and_push.return_value = fs_mock

        hub.step()
        self.assertEqual(hub.stats["signals_emitted"], 1)
        hub.label_sink.write.assert_called_once()
        call_kwargs = hub.writer.write_and_push.call_args.kwargs
        self.assertEqual(call_kwargs["side"], "LONG")
        self.assertEqual(call_kwargs["source"], "AggregatedHub-Pro")

    def test_step_no_signal_when_no_dir(self):
        hub = self._make_hub()
        hub.snapshot.build.return_value = _make_snap()
        hub.r.xread.return_value = []
        pro_metrics = self._no_trigger_pro_metrics(trades_count=10)
        pro_metrics["trigger"] = True
        pro_metrics["z_delta"] = 5.0
        pro_metrics["z_speed"] = 4.5
        pro_metrics["svbp_imbalance"] = 0.0
        pro_metrics["dir_up"] = None
        hub.detector_pro.metrics.return_value = pro_metrics

        hub.step()
        # dir_up is None and svbp=0 → still None → step returns early
        hub.writer.write_and_push.assert_not_called()

    def test_log_stats_does_not_raise(self):
        hub = self._make_hub()
        hub._log_stats()  # should not raise

    def test_process_trades_empty_stream(self):
        hub = self._make_hub()
        hub.r.xread.return_value = []
        result = hub._process_trades()
        self.assertEqual(result, 0)

    def test_process_trades_parses_valid_message(self):
        hub = self._make_hub()
        # Use string keys matching Redis decode_responses=True output
        hub.r.xread.return_value = [
            (
                "trades:XAUUSD",
                [("1-0", {"price": "2001.5", "qty": "0.5", "side": "buy", "ts": "1700000000000"})]
            )
        ]
        result = hub._process_trades()
        self.assertEqual(result, 1)
        hub.detector_pro.on_trade.assert_called_once_with(2001.5, 0.5, "buy", 1700000000000)

    def test_process_trades_skips_invalid_message(self):
        hub = self._make_hub()
        hub.r.xread.return_value = [
            (
                b"trades:XAUUSD",
                [(b"1-0", {b"price": b"0", b"qty": b"0", b"side": b"unknown", b"ts": b"0"})]
            )
        ]
        result = hub._process_trades()
        self.assertEqual(result, 0)
        hub.detector_pro.on_trade.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
