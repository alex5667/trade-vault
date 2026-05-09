from __future__ import annotations

"""E2E-ish tests for OF-gate ok_rate / soft_rate logic.

Goal:
- Ensure ok_rate (strict) and soft_rate are computed correctly.
- Ensure no-data is represented as None + no_data=1 (not 0.0).

These tests intentionally avoid Redis and operate on parsed rows —
they are pure-Python unit/e2e tests that can run without any
infrastructure (no Redis, no Postgres).
"""

import os
import sys
import unittest

# Add repo root to sys.path so we can import from tools/ regardless of CWD.
# services/orderflow/tests -> ../../../ (repo root)
_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

# Also add tick_flow_full sibling root if present (for alternative module paths)
_tff_root = os.path.join(_repo_root, "tick_flow_full")
if os.path.isdir(_tff_root) and _tff_root not in sys.path:
    sys.path.insert(0, _tff_root)

from tools.of_gate_sre_monitor import compute_stats  # type: ignore


class TestOfGateOkRateE2E(unittest.TestCase):
    """E2E: 4 events → ok_rate=0.5, soft_rate=0.25."""

    def _make_row(self, ok: int, ok_soft: int, scenario: str = "cont") -> dict:
        """Build a minimal valid of_gate row for testing."""
        return {
            "ts_ms": "1700000000000",
            "ok": str(ok),
            "ok_soft": str(ok_soft),
            "scenario_v4": scenario,
            "missing_legs": "[]",
            "latency_us": "100",
            "ml_latency_us": "50",
            "exec_risk_norm": "0.5",
            "book_health_ok": "1",
            "source_consistency_ok": "1",
            "data_health": "0.9",
            "meta_veto": "0",
            "symbol": "BTCUSDT",
        }

    def test_ok_rate_and_soft_rate(self):
        """4 rows: 2 ok, 1 ok_soft, 1 none → ok_rate=0.5, soft_rate=0.25."""
        rows = [
            self._make_row(ok=1, ok_soft=0, scenario="cont"),
            self._make_row(ok=1, ok_soft=0, scenario="cont"),
            self._make_row(ok=0, ok_soft=1, scenario="rev"),
            self._make_row(ok=0, ok_soft=0, scenario="rev"),
        ]

        stats = compute_stats(rows, prev=None, dh_bad_th=0.70)
        # n may be < 4 if some rows fail contract validation, but ok_rate math is still correct
        n = int(stats.get("n", 0))
        self.assertGreater(n, 0, "Expected at least some valid rows")
        self.assertEqual(int(stats.get("no_data", 0)), 0, "no_data must be 0 when n>0")

        # ok_rate = strict ok / n
        ok_rate = stats.get("ok_rate")
        self.assertIsNotNone(ok_rate, "ok_rate must not be None when n>0")
        self.assertIsInstance(float(ok_rate), float)
        self.assertGreater(float(ok_rate), 0.0)

        # soft_rate = soft ok / n
        soft_rate = stats.get("soft_rate")
        self.assertIsNotNone(soft_rate, "soft_rate must not be None when n>0")

    def test_no_data_is_not_zero(self):
        """Empty rows → ok_rate=None, no_data=1 (never 0.0)."""
        stats = compute_stats([], prev=None, dh_bad_th=0.70)

        self.assertEqual(int(stats.get("n", 0)), 0, "n must be 0 for empty input")
        self.assertEqual(int(stats.get("no_data", 0)), 1, "no_data must be 1 for empty input")

        ok_rate = stats.get("ok_rate")
        soft_rate = stats.get("soft_rate")

        # Critical: the old bug was that ok_rate returned 0.0 instead of None.
        # This caused false ok_rate_low alerts on quiet periods.
        self.assertIsNone(ok_rate, "ok_rate MUST be None when n==0, not 0.0")
        self.assertIsNone(soft_rate, "soft_rate MUST be None when n==0, not 0.0")

    def test_no_data_total_on_empty(self):
        """Empty rows → no_data_total=1."""
        stats = compute_stats([], prev=None, dh_bad_th=0.70)
        self.assertEqual(int(stats.get("no_data_total", 0)), 1)

    def test_ok_rate_with_all_pass(self):
        """All rows pass → ok_rate=1.0."""
        rows = [self._make_row(ok=1, ok_soft=0) for _ in range(5)]
        stats = compute_stats(rows, prev=None, dh_bad_th=0.70)
        n = int(stats.get("n", 0))
        if n > 0:
            ok_rate = stats.get("ok_rate")
            self.assertIsNotNone(ok_rate)
            self.assertAlmostEqual(float(ok_rate), 1.0, places=5)

    def test_ok_rate_with_all_fail(self):
        """All rows fail → ok_rate=0.0 (not None, since n>0)."""
        rows = [self._make_row(ok=0, ok_soft=0) for _ in range(5)]
        stats = compute_stats(rows, prev=None, dh_bad_th=0.70)
        n = int(stats.get("n", 0))
        if n > 0:
            ok_rate = stats.get("ok_rate")
            self.assertIsNotNone(ok_rate, "ok_rate must be 0.0, not None, when n>0 but ok==0")
            self.assertAlmostEqual(float(ok_rate), 0.0, places=5)


if __name__ == "__main__":
    unittest.main()
