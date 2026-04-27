"""
Tests for common.balance_provider — BalanceProvider.

Coverage:
  - test_redis_hot_path          fresh snapshot → returns Redis balance, caches it
  - test_redis_stale_fallback    snapshot > max_staleness → tries REST
  - test_redis_missing_fallback  key not in Redis → tries REST
  - test_redis_unavailable       Redis exception → tries REST
  - test_all_fail_static         Redis + REST fail → returns ACCOUNT_DEPOSIT_USD
  - test_in_process_cache        second call does NOT hit Redis
  - test_mode_static             BALANCE_PROVIDER_MODE=static skips Redis
  - test_mode_direct             BALANCE_PROVIDER_MODE=direct skips Redis, hits REST
  - test_from_ctx                BalanceProvider.from_ctx extracts redis/binance
"""
from __future__ import annotations

import json
import os
import time
import threading
import unittest
from unittest.mock import MagicMock, patch

# Reset global cache before each test
from common import balance_provider as _bp_mod


def _fresh_snapshot(wallet: float = 2000.0, available: float = 1800.0, age_s: float = 0.0) -> str:
    ts_ms = int((time.time() - age_s) * 1000)
    return json.dumps({
        "ts_ms": ts_ms,
        "wallet_balance": wallet,
        "available_balance": available,
    })


class TestBalanceProvider(unittest.TestCase):

    def setUp(self):
        """Clear global in-process cache before every test."""
        _bp_mod._GLOBAL_CACHE.invalidate()

    # ------------------------------------------------------------------
    # Hot path: Redis snapshot
    # ------------------------------------------------------------------

    def test_redis_hot_path(self):
        """Fresh Redis snapshot → returns wallet_balance, no REST call."""
        r = MagicMock()
        r.get.return_value = _fresh_snapshot(wallet=1971.72, available=1847.87, age_s=10)

        bp = _bp_mod.BalanceProvider(
            redis_client=r, mode="redis_first",
            max_staleness_s=300, cache_ttl_s=60,
            static_deposit=100.0,
        )
        wallet = bp.get_wallet_balance()
        self.assertAlmostEqual(wallet, 1971.72, places=1)
        r.get.assert_called_once()

    def test_redis_available_balance(self):
        r = MagicMock()
        r.get.return_value = _fresh_snapshot(wallet=1971.0, available=1847.0, age_s=5)

        bp = _bp_mod.BalanceProvider(redis_client=r, mode="redis_first",
                                      max_staleness_s=300, cache_ttl_s=60)
        avail = bp.get_available_balance()
        self.assertAlmostEqual(avail, 1847.0, places=1)

    # ------------------------------------------------------------------
    # Stale / missing Redis → cold fallback
    # ------------------------------------------------------------------

    def test_redis_stale_fallback_to_rest(self):
        """Snapshot older than max_staleness → falls back to REST."""
        r = MagicMock()
        r.get.return_value = _fresh_snapshot(wallet=1971.0, age_s=400)  # stale

        binance = MagicMock()
        binance.get_account.return_value = {
            "totalWalletBalance": "1960.00",
            "availableBalance": "1840.00",
        }

        bp = _bp_mod.BalanceProvider(
            redis_client=r, binance_client=binance,
            mode="redis_first", max_staleness_s=300, cache_ttl_s=60,
        )
        wallet = bp.get_wallet_balance()
        self.assertAlmostEqual(wallet, 1960.0, places=1)
        binance.get_account.assert_called_once()

    def test_redis_missing_fallback_to_rest(self):
        """No snapshot in Redis → falls back to REST."""
        r = MagicMock()
        r.get.return_value = None  # key absent

        binance = MagicMock()
        binance.get_account.return_value = {
            "totalWalletBalance": "1500.00",
            "availableBalance": "1400.00",
        }

        bp = _bp_mod.BalanceProvider(
            redis_client=r, binance_client=binance,
            mode="redis_first", max_staleness_s=300, cache_ttl_s=60,
        )
        wallet = bp.get_wallet_balance()
        self.assertAlmostEqual(wallet, 1500.0, places=1)

    def test_redis_exception_fallback_to_rest(self):
        """Redis connection error → falls back to REST, no exception propagated."""
        r = MagicMock()
        r.get.side_effect = ConnectionError("redis unavailable")

        binance = MagicMock()
        binance.get_account.return_value = {
            "totalWalletBalance": "1200.00",
            "availableBalance": "1100.00",
        }

        bp = _bp_mod.BalanceProvider(
            redis_client=r, binance_client=binance,
            mode="redis_first", max_staleness_s=300, cache_ttl_s=60,
        )
        wallet = bp.get_wallet_balance()
        self.assertAlmostEqual(wallet, 1200.0, places=1)

    # ------------------------------------------------------------------
    # All sources fail → static
    # ------------------------------------------------------------------

    def test_all_fail_static(self):
        """Both Redis and REST fail → returns ACCOUNT_DEPOSIT_USD."""
        r = MagicMock()
        r.get.side_effect = ConnectionError("no redis")

        binance = MagicMock()
        binance.get_account.side_effect = RuntimeError("no binance")

        with patch.dict(os.environ, {"ACCOUNT_DEPOSIT_USD": "999.0"}):
            bp = _bp_mod.BalanceProvider(
                redis_client=r, binance_client=binance,
                mode="redis_first", max_staleness_s=300, cache_ttl_s=60,
                static_deposit=999.0,
            )
            wallet = bp.get_wallet_balance()
        self.assertAlmostEqual(wallet, 999.0, places=1)

    # ------------------------------------------------------------------
    # In-process cache
    # ------------------------------------------------------------------

    def test_in_process_cache_no_double_redis(self):
        """Second call within cache TTL does NOT hit Redis."""
        r = MagicMock()
        r.get.return_value = _fresh_snapshot(wallet=2000.0, age_s=0)

        bp = _bp_mod.BalanceProvider(
            redis_client=r, mode="redis_first",
            max_staleness_s=300, cache_ttl_s=60,
        )
        bp.get_wallet_balance()  # first call → hits Redis
        bp.get_wallet_balance()  # second call → from cache
        r.get.assert_called_once()  # only ONE Redis call total

    def test_cache_invalidate(self):
        """invalidate_cache forces re-read from Redis."""
        r = MagicMock()
        r.get.return_value = _fresh_snapshot(wallet=2000.0, age_s=0)

        bp = _bp_mod.BalanceProvider(
            redis_client=r, mode="redis_first",
            max_staleness_s=300, cache_ttl_s=60,
        )
        bp.get_wallet_balance()  # call 1 → Redis
        bp.invalidate_cache()
        bp.get_wallet_balance()  # call 2 → Redis again after invalidation
        self.assertEqual(r.get.call_count, 2)

    # ------------------------------------------------------------------
    # Modes
    # ------------------------------------------------------------------

    def test_mode_static_skips_redis(self):
        """BALANCE_PROVIDER_MODE=static → never touches Redis or REST."""
        r = MagicMock()
        binance = MagicMock()

        bp = _bp_mod.BalanceProvider(
            redis_client=r, binance_client=binance,
            mode="static", static_deposit=555.0,
        )
        wallet = bp.get_wallet_balance()
        self.assertAlmostEqual(wallet, 555.0, places=1)
        r.get.assert_not_called()
        binance.get_account.assert_not_called()

    def test_mode_direct_skips_redis(self):
        """BALANCE_PROVIDER_MODE=direct → skips Redis, calls REST."""
        r = MagicMock()
        binance = MagicMock()
        binance.get_account.return_value = {
            "totalWalletBalance": "1800.0",
            "availableBalance": "1700.0",
        }

        bp = _bp_mod.BalanceProvider(
            redis_client=r, binance_client=binance,
            mode="direct", static_deposit=100.0,
        )
        wallet = bp.get_wallet_balance()
        self.assertAlmostEqual(wallet, 1800.0, places=1)
        r.get.assert_not_called()
        binance.get_account.assert_called_once()

    # ------------------------------------------------------------------
    # from_ctx
    # ------------------------------------------------------------------

    def test_from_ctx_uses_ctx_provider(self):
        """If ctx.balance_provider is set, return it directly."""
        mock_bp = MagicMock(spec=_bp_mod.BalanceProvider)
        ctx = MagicMock()
        ctx.balance_provider = mock_bp

        result = _bp_mod.BalanceProvider.from_ctx(ctx)
        self.assertIs(result, mock_bp)

    def test_from_ctx_builds_from_redis(self):
        """ctx.redis is picked up for redis_first mode."""
        r = MagicMock()
        r.get.return_value = _fresh_snapshot(wallet=1500.0, age_s=0)

        ctx = MagicMock()
        ctx.balance_provider = None
        ctx.redis = r
        ctx.binance_client = None

        with patch.dict(os.environ, {
            "BALANCE_PROVIDER_MODE": "redis_first",
            "BALANCE_MAX_STALENESS_S": "300",
            "BALANCE_CACHE_TTL_S": "60",
        }):
            bp = _bp_mod.BalanceProvider.from_ctx(ctx)
            wallet = bp.get_wallet_balance()
        self.assertAlmostEqual(wallet, 1500.0, places=1)

    # ------------------------------------------------------------------
    # Thread safety
    # ------------------------------------------------------------------

    def test_thread_safe_cache(self):
        """Concurrent calls from multiple threads should not raise."""
        r = MagicMock()
        r.get.return_value = _fresh_snapshot(wallet=2000.0, age_s=0)

        bp = _bp_mod.BalanceProvider(
            redis_client=r, mode="redis_first",
            max_staleness_s=300, cache_ttl_s=60,
        )
        results = []
        errors = []

        def worker():
            try:
                results.append(bp.get_wallet_balance())
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 20)
        for v in results:
            self.assertAlmostEqual(v, 2000.0, places=1)


if __name__ == "__main__":
    unittest.main()
