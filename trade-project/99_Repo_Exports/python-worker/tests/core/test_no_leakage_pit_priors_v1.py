"""test_no_leakage_pit_priors_v1.py — ADR-0007 §R1 leakage guards.

Critical correctness tests: ensures PIT priors at time T cannot see outcomes
from trades that closed at T+ε, even if those trades' decisions occurred at
T-Δ. The materializer keys on `ts_close < T - EMBARGO_MS`.

Failure mode this prevents:
  - Training a model on signals at T using priors that include outcomes
    revealed only after T → look-ahead bias → over-optimistic backtests
"""
from __future__ import annotations

import unittest

from tools.build_pit_priors_v1 import build_pit_priors


def _trade(symbol: str, kind: str, result: str, r_mult: float,
           ts_close: int, ts_decision: int | None = None) -> dict:
    return {
        "symbol": symbol,
        "kind": kind,
        "result": result,
        "r_multiple": str(r_mult),
        "ts_close": str(ts_close),
        "ts_decision": str(ts_decision if ts_decision is not None else ts_close - 60_000),
    }


class TestPITLeakageGuards(unittest.TestCase):

    def test_trade_inside_embargo_is_excluded(self):
        """Trade closing at T-30s with embargo=1h must be excluded from priors at T."""
        T = 1_000_000_000
        embargo = 3_600_000  # 1h
        # 40 stale trades + 1 fresh trade inside embargo
        trades = [_trade("BTCUSDT", "default", "WIN", 1.5, T - embargo - 1_000_000 - i)
                  for i in range(40)]
        trades.append(_trade("BTCUSDT", "default", "LOSS", -1.0, T - 30_000))  # inside embargo

        priors = build_pit_priors(trades, as_of_ts_ms=T, embargo_ms=embargo)
        # Only 40 stale trades should be in the prior; find the BTCUSDT/default bucket
        btc_keys = [k for k in priors if k[0] == "BTCUSDT" and k[1] == "default"]
        self.assertEqual(len(btc_keys), 1, f"Expected single bucket, got {btc_keys}")
        bucket = priors[btc_keys[0]]
        self.assertEqual(bucket["sample_count"], 40.0)
        # All stale trades are WIN → winrate must be 1.0 (no LOSS leak)
        self.assertEqual(bucket["winrate"], 1.0)

    def test_trade_at_embargo_boundary_is_excluded(self):
        """ts_close == T - embargo (exact boundary) must be excluded (strict <)."""
        T = 1_000_000_000
        embargo = 3_600_000
        trades = [_trade("ETHUSDT", "default", "WIN", 1.0, T - embargo - 1000 - i)
                  for i in range(35)]
        trades.append(_trade("ETHUSDT", "default", "LOSS", -1.0, T - embargo))  # exact boundary

        priors = build_pit_priors(trades, as_of_ts_ms=T, embargo_ms=embargo)
        sample_counts = [p["sample_count"] for p in priors.values()]
        self.assertIn(35.0, sample_counts,
                      f"Trade at exact embargo boundary leaked into priors: counts={sample_counts}")

    def test_no_priors_built_when_all_trades_within_embargo(self):
        """If every trade is inside embargo, priors must be empty."""
        T = 1_000_000_000
        embargo = 3_600_000
        trades = [_trade("SOLUSDT", "default", "WIN", 1.5, T - 100_000 - i)
                  for i in range(50)]

        priors = build_pit_priors(trades, as_of_ts_ms=T, embargo_ms=embargo)
        self.assertEqual(len(priors), 0)

    def test_be_results_excluded_from_winrate(self):
        """BE (break-even) trades must not pollute winrate (stays binary WIN/LOSS)."""
        T = 1_000_000_000
        embargo = 3_600_000
        # 30 WIN + 30 LOSS + 100 BE → winrate must be 0.5 (BE excluded)
        trades = []
        base = T - embargo - 1_000_000
        for i in range(30):
            trades.append(_trade("BTCUSDT", "default", "WIN", 1.0, base - i))
        for i in range(30):
            trades.append(_trade("BTCUSDT", "default", "LOSS", -1.0, base - 1000 - i))
        for i in range(100):
            trades.append(_trade("BTCUSDT", "default", "BE", 0.0, base - 2000 - i))

        priors = build_pit_priors(trades, as_of_ts_ms=T, embargo_ms=embargo)
        # Find any non-empty bucket
        sample = next(iter(priors.values())) if priors else None
        self.assertIsNotNone(sample)
        assert sample is not None
        self.assertEqual(sample["sample_count"], 60.0,
                         f"BE trades must be excluded; got {sample['sample_count']}")
        self.assertAlmostEqual(sample["winrate"], 0.5, places=2)

    def test_session_bucket_partitioning(self):
        """Trades from different session buckets must be aggregated separately."""
        T = 1_000_000_000
        embargo = 3_600_000
        base = T - embargo - 1_000_000
        # ts_decision at 03:00 UTC (asia)
        asia_ts = (3 * 3_600_000)
        # ts_decision at 14:00 UTC (us)
        us_ts = (14 * 3_600_000)

        trades = []
        for i in range(35):
            trades.append({
                "symbol": "BTCUSDT", "kind": "default", "result": "WIN",
                "r_multiple": "1.0",
                "ts_close": str(base - i),
                "ts_decision": str(asia_ts + i),
            })
        for i in range(35):
            trades.append({
                "symbol": "BTCUSDT", "kind": "default", "result": "LOSS",
                "r_multiple": "-1.0",
                "ts_close": str(base - 100 - i),
                "ts_decision": str(us_ts + i),
            })

        priors = build_pit_priors(trades, as_of_ts_ms=T, embargo_ms=embargo)
        keys = list(priors.keys())
        # Two distinct session buckets expected
        sessions = {k[2] for k in keys if k[0] == "BTCUSDT" and k[1] == "default"}
        self.assertIn("asia", sessions)
        self.assertIn("us", sessions)
        self.assertEqual(priors[("BTCUSDT", "default", "asia")]["winrate"], 1.0)
        self.assertEqual(priors[("BTCUSDT", "default", "us")]["winrate"], 0.0)

    def test_round_trip_no_recursion_on_self(self):
        """Trade T's outcome must not appear in priors used by T's own decision."""
        # Decision at T₀, closes at T₀+30min. When we materialize priors as of
        # `as_of=T₀ - ε` (i.e., what was knowable at decision time), this trade
        # must NOT appear because ts_close > as_of_ms.
        decision_ts = 1_000_000_000
        close_ts = decision_ts + 30 * 60_000
        trade = _trade("BTCUSDT", "default", "WIN", 2.0, close_ts, decision_ts)
        # Build priors as of decision_ts (with 0 embargo to isolate the as_of bound)
        priors = build_pit_priors([trade], as_of_ts_ms=decision_ts, embargo_ms=0)
        self.assertEqual(len(priors), 0,
                         "Trade closing AFTER as_of must not appear in priors")


class TestPurgedKFoldEmbargo(unittest.TestCase):
    """ADR-0007 §R-stats: PurgedEmbargoTimeSeriesSplit invariants."""

    def test_no_train_sample_inside_purge_window(self):
        """No train index must come from t ∈ [val_start - purge, val_start)."""
        from scripts.train_ml_scorer_v3 import PurgedEmbargoTimeSeriesSplit
        import numpy as np

        # 1000 samples spaced 1s apart starting at t=0
        ts_list: list[int] = list(range(0, 1_000_000, 1_000))
        ts = np.asarray(ts_list, dtype=np.int64)
        splitter = PurgedEmbargoTimeSeriesSplit(
            n_splits=5, purge_ms=10_000, embargo_ms=5_000, min_train=10,
        )
        for train_idx, val_idx in splitter.split(ts_list):
            val_start = ts[val_idx].min()
            val_end = ts[val_idx].max()
            for ti in train_idx:
                t_train = ts[ti]
                # Train must be either fully before val (with purge) or after embargo
                self.assertFalse(
                    val_start - 10_000 <= t_train < val_start,
                    f"Train sample t={t_train} inside purge window [val_start={val_start}]",
                )
                self.assertFalse(
                    val_end < t_train <= val_end + 5_000,
                    f"Train sample t={t_train} inside embargo window [val_end={val_end}]",
                )


if __name__ == "__main__":
    unittest.main()


if __name__ == "__main__":
    unittest.main()
