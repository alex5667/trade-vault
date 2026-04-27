from __future__ import annotations

"""Tests for services/orderflow/inline_exec_health.py — P1 bucket.

Coverage:
  * inline_is_from_cumulative_state: correctness and guard conditions
  * update_inline_exec_from_fill: partial fills accumulate on same sid, Redis keys
  * read_inline_exec_rollup_sync: reads from correct key, fallback, count guard
"""

import asyncio
import json
import math
import unittest
from types import SimpleNamespace
from typing import Any, Dict, Optional, Tuple


# ---------------------------------------------------------------------------
# Lightweight fake async Redis client (no external deps)
# ---------------------------------------------------------------------------

class _FakeRedis:
    """Minimal fake async-compatible Redis with hset / hgetall / expire / zadd / zcard / zrange / zrem / hdel / get / set."""

    def __init__(self):
        self._store: Dict[str, Any] = {}

    # hash helpers
    async def hset(self, name: str, *, mapping: Dict[str, str]) -> None:
        h = self._store.setdefault(name, {})
        h.update(mapping)

    async def hget(self, name: str, field: str) -> Optional[str]:
        return self._store.get(name, {}).get(field)

    async def hgetall(self, name: str) -> Dict[str, str]:
        return dict(self._store.get(name, {}))

    async def hdel(self, name: str, *fields: str) -> None:
        h = self._store.get(name, {})
        for f in fields:
            h.pop(f, None)

    async def expire(self, name: str, ttl: int) -> None:
        pass  # not tracked in tests

    # sorted set helpers
    async def zadd(self, name: str, mapping: Dict[str, int]) -> None:
        z = self._store.setdefault(name + ":zset", {})
        z.update(mapping)

    async def zrange(self, name: str, start: int, stop: int) -> list:
        z = self._store.get(name + ":zset", {})
        items = sorted(z.items(), key=lambda x: x[1])
        if stop == -1:
            return [k for k, _ in items[start:]]
        return [k for k, _ in items[start: stop + 1]]

    async def zcard(self, name: str) -> int:
        return len(self._store.get(name + ":zset", {}))

    async def zrem(self, name: str, *members: str) -> None:
        z = self._store.get(name + ":zset", {})
        for m in members:
            z.pop(m, None)

    # str key helpers
    async def set(self, name: str, value: Any) -> None:
        self._store[name] = str(value)

    async def get(self, name: str) -> Optional[str]:
        return self._store.get(name)

    # sync hgetall for read_inline_exec_rollup_sync tests
    def hgetall_sync(self, name: str) -> Dict[str, str]:
        return dict(self._store.get(name, {}))


class _FakeSyncRedis(_FakeRedis):
    """Sync-only subset of Redis used by read_inline_exec_rollup_sync."""

    def hgetall(self, name: str) -> Dict[str, str]:
        return dict(self._store.get(name, {}))


# ---------------------------------------------------------------------------
# Import module under test
# ---------------------------------------------------------------------------

try:
    from services.orderflow.inline_exec_health import (
        InlineExecDims,
        InlineExecPolicyDecision,
        decide_inline_exec_health,
        inline_is_from_cumulative_state,
        make_rollup_key,
        make_samples_key,
        make_sid_state_key,
        read_inline_exec_rollup_sync,
        resolve_mode,
        update_inline_exec_from_fill,
    )
    SKIP_REASON = None
except Exception as e:
    SKIP_REASON = str(e)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@unittest.skipIf(SKIP_REASON, f"import failed: {SKIP_REASON}")
class TestInlineIsFromCumulativeState(unittest.TestCase):

    def test_basic_buylong_is_positive_when_fill_above_mid(self):
        """Buy-LONG fill above decision_mid → IS should be positive (we paid more)."""
        is_bps = inline_is_from_cumulative_state(
            decision_mid=100.0,
            side="LONG",
            cum_notional=5100.0,  # fill at ~102 (above mid) * 50 qty => notional 5100
            cum_qty=50.0,
            cum_fee_usd=5.0,     # small fee
        )
        self.assertIsNotNone(is_bps)
        self.assertGreater(float(is_bps), 0.0)

    def test_basic_sell_short_is_positive_when_fill_below_mid(self):
        """Sell-SHORT fill below decision_mid → IS should be positive (we got less)."""
        is_bps = inline_is_from_cumulative_state(
            decision_mid=100.0,
            side="SHORT",
            cum_notional=4900.0,  # fill at 98 * 50 qty
            cum_qty=50.0,
            cum_fee_usd=5.0,
        )
        self.assertIsNotNone(is_bps)
        self.assertGreater(float(is_bps), 0.0)

    def test_returns_none_for_zero_qty(self):
        v = inline_is_from_cumulative_state(
            decision_mid=100.0,
            side="LONG",
            cum_notional=5000.0,
            cum_qty=0.0,
            cum_fee_usd=0.0,
        )
        self.assertIsNone(v)

    def test_returns_none_for_bad_mid(self):
        v = inline_is_from_cumulative_state(
            decision_mid=0.0,
            side="LONG",
            cum_notional=5000.0,
            cum_qty=50.0,
            cum_fee_usd=0.0,
        )
        self.assertIsNone(v)

    def test_zero_fee(self):
        """Zero fee is valid — fee_bps defaults to 0."""
        is_bps = inline_is_from_cumulative_state(
            decision_mid=100.0,
            side="LONG",
            cum_notional=5050.0,
            cum_qty=50.0,
            cum_fee_usd=0.0,
        )
        self.assertIsNotNone(is_bps)
        self.assertGreater(float(is_bps), 0.0)


@unittest.skipIf(SKIP_REASON, f"import failed: {SKIP_REASON}")
class TestUpdateInlineExecFromFill(unittest.TestCase):

    def _dims(self):
        return InlineExecDims(symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m")

    def test_single_fill_creates_rollup(self):
        r = _FakeRedis()
        dims = self._dims()
        stats = _run(update_inline_exec_from_fill(
            redis=r,
            sid="sid001",
            dims=dims,
            decision_mid=100.0,
            fill_px=100.5,    # 5bps above mid
            fill_qty=10.0,
            fee_bps=2.0,
            ts_fill_ms=1_700_000_000_000,
        ))
        self.assertIn("p95_bps", stats)
        self.assertIn("count", stats)
        self.assertEqual(int(stats["count"]), 1)
        self.assertGreater(float(stats["p95_bps"]), 0.0)

    def test_partial_fills_same_sid_accumulate_not_duplicate(self):
        """Two partial fills for same sid → only one sample (VWAP updated)."""
        r = _FakeRedis()
        dims = self._dims()
        ts = 1_700_000_000_000
        _run(update_inline_exec_from_fill(
            redis=r,
            sid="sid002",
            dims=dims,
            decision_mid=100.0,
            fill_px=100.5,
            fill_qty=5.0,
            fee_bps=2.0,
            ts_fill_ms=ts,
        ))
        stats2 = _run(update_inline_exec_from_fill(
            redis=r,
            sid="sid002",
            dims=dims,
            decision_mid=100.0,
            fill_px=101.0,
            fill_qty=5.0,
            fee_bps=2.0,
            ts_fill_ms=ts + 100,
        ))
        # Still one sample for this sid
        self.assertEqual(int(stats2["count"]), 1)

    def test_different_sids_produce_multiple_samples(self):
        r = _FakeRedis()
        dims = self._dims()
        ts = 1_700_000_000_000
        for i in range(3):
            _run(update_inline_exec_from_fill(
                redis=r,
                sid=f"sid{i:03d}",
                dims=dims,
                decision_mid=100.0,
                fill_px=100.0 + 0.5 * (i + 1),
                fill_qty=10.0,
                fee_bps=2.0,
                ts_fill_ms=ts + i * 1000,
            ))
        # Try one more to get a final stats back
        stats = _run(update_inline_exec_from_fill(
            redis=r,
            sid="sid099",
            dims=dims,
            decision_mid=100.0,
            fill_px=101.0,
            fill_qty=10.0,
            fee_bps=2.0,
            ts_fill_ms=ts + 4000,
        ))
        self.assertGreaterEqual(int(stats["count"]), 4)

    def test_returns_empty_dict_for_none_redis(self):
        dims = self._dims()
        stats = _run(update_inline_exec_from_fill(
            redis=None,
            sid="sid001",
            dims=dims,
            decision_mid=100.0,
            fill_px=100.5,
            fill_qty=10.0,
            fee_bps=2.0,
            ts_fill_ms=1_700_000_000_000,
        ))
        self.assertEqual(stats, {})

    def test_returns_empty_for_bad_inputs(self):
        r = _FakeRedis()
        dims = self._dims()
        # fill_px = 0 → invalid
        stats = _run(update_inline_exec_from_fill(
            redis=r,
            sid="sid_bad",
            dims=dims,
            decision_mid=100.0,
            fill_px=0.0,
            fill_qty=10.0,
            fee_bps=2.0,
            ts_fill_ms=1_700_000_000_000,
        ))
        self.assertEqual(stats, {})

    def test_rollup_key_written_to_redis(self):
        r = _FakeRedis()
        dims = self._dims()
        _run(update_inline_exec_from_fill(
            redis=r,
            sid="sid005",
            dims=dims,
            decision_mid=100.0,
            fill_px=100.5,
            fill_qty=10.0,
            fee_bps=2.0,
            ts_fill_ms=1_700_000_000_000,
        ))
        rkey = make_rollup_key(dims.norm(), include_session=True)
        h = _run(r.hgetall(rkey))
        self.assertIn("p95_bps", h)

    def test_max_samples_bound_respected(self):
        r = _FakeRedis()
        dims = self._dims()
        max_s = 5
        ts = 1_700_000_000_000
        for i in range(max_s + 3):  # push 8 samples
            _run(update_inline_exec_from_fill(
                redis=r,
                sid=f"sid{i:03d}",
                dims=dims,
                decision_mid=100.0,
                fill_px=100.5,
                fill_qty=10.0,
                fee_bps=2.0,
                ts_fill_ms=ts + i * 1000,
                max_samples=max_s,
            ))
        stats = _run(update_inline_exec_from_fill(
            redis=r,
            sid="oversample_final",
            dims=dims,
            decision_mid=100.0,
            fill_px=100.5,
            fill_qty=10.0,
            fee_bps=2.0,
            ts_fill_ms=ts + 9000,
            max_samples=max_s,
        ))
        self.assertLessEqual(int(stats["count"]), max_s + 1)


@unittest.skipIf(SKIP_REASON, f"import failed: {SKIP_REASON}")
class TestReadInlineExecRollupSync(unittest.TestCase):

    def _dims(self):
        return InlineExecDims(symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m")

    def test_returns_empty_for_none_redis(self):
        result = read_inline_exec_rollup_sync(
            None, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m"
        )
        self.assertEqual(result, {})

    def test_reads_session_aware_key_first(self):
        r = _FakeSyncRedis()
        dims = self._dims().norm()
        key = make_rollup_key(dims, include_session=True)
        r._store[key] = {
            "p95_bps": "7.5",
            "p50_bps": "4.0",
            "ema_bps": "5.0",
            "count": "10",
            "updated_at_ms": "1700000000000",
        }
        result = read_inline_exec_rollup_sync(
            r, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m"
        )
        self.assertAlmostEqual(float(result["p95_bps"]), 7.5)
        self.assertAlmostEqual(float(result["p50_bps"]), 4.0)
        self.assertEqual(int(result["count"]), 10)
        self.assertEqual(float(result["session_exact"]), 1.0)

    def test_min_count_guard(self):
        r = _FakeSyncRedis()
        dims = self._dims().norm()
        key = make_rollup_key(dims, include_session=True)
        r._store[key] = {
            "p95_bps": "7.5",
            "count": "1",
        }
        result = read_inline_exec_rollup_sync(
            r, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m",
            min_count=5,
        )
        self.assertEqual(result, {})

    def test_fallback_to_aggregate_key(self):
        r = _FakeSyncRedis()
        dims = self._dims().norm()
        # Only populate aggregate (no-session) key
        agg_key = make_rollup_key(dims, include_session=False)
        r._store[agg_key] = {
            "p95_bps": "3.0",
            "count": "8",
        }
        result = read_inline_exec_rollup_sync(
            r, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m", min_count=1,
        )
        self.assertAlmostEqual(float(result["p95_bps"]), 3.0)
        self.assertEqual(float(result["session_exact"]), 0.0)


@unittest.skipIf(SKIP_REASON, f"import failed: {SKIP_REASON}")
class TestDecideInlineExecHealth(unittest.TestCase):

    def test_below_warn_returns_ok(self):
        dec = decide_inline_exec_health(
            p95_bps=2.0, warn_bps=5.0, crit_bps=10.0,
            perm_impact_p95_bps=float("nan"), max_perm_impact_p95_bps=6.0,
            mode="veto",
        )
        self.assertFalse(dec.apply)
        self.assertFalse(dec.veto)

    def test_monitor_mode_no_veto(self):
        dec = decide_inline_exec_health(
            p95_bps=12.0, warn_bps=5.0, crit_bps=10.0,
            perm_impact_p95_bps=7.0, max_perm_impact_p95_bps=5.0,
            mode="monitor",
        )
        self.assertTrue(dec.apply)
        self.assertFalse(dec.veto)

    def test_veto_mode_triggers_when_both_bad(self):
        dec = decide_inline_exec_health(
            p95_bps=12.0, warn_bps=5.0, crit_bps=10.0,
            perm_impact_p95_bps=7.0, max_perm_impact_p95_bps=5.0,
            mode="veto",
        )
        self.assertTrue(dec.veto)
        self.assertIn("VETO", dec.reason_code)

    def test_veto_mode_no_veto_when_perm_ok(self):
        """Veto mode does NOT veto unless perm_impact_p95 also exceeds threshold."""
        dec = decide_inline_exec_health(
            p95_bps=12.0, warn_bps=5.0, crit_bps=10.0,
            perm_impact_p95_bps=1.0, max_perm_impact_p95_bps=5.0,
            mode="veto",
        )
        self.assertFalse(dec.veto)

    def test_tighten_mode_above_warn(self):
        dec = decide_inline_exec_health(
            p95_bps=7.0, warn_bps=5.0, crit_bps=10.0,
            perm_impact_p95_bps=float("nan"), max_perm_impact_p95_bps=0.0,
            mode="tighten",
        )
        self.assertTrue(dec.apply)
        self.assertFalse(dec.veto)
        self.assertEqual(dec.reason_code, "INLINE_EXEC_TIGHTEN")

    def test_off_mode_always_pass(self):
        dec = decide_inline_exec_health(
            p95_bps=99.0, warn_bps=5.0, crit_bps=10.0,
            perm_impact_p95_bps=float("nan"), max_perm_impact_p95_bps=0.0,
            mode="off",
        )
        self.assertFalse(dec.apply)
        self.assertFalse(dec.veto)


@unittest.skipIf(SKIP_REASON, f"import failed: {SKIP_REASON}")
class TestResolveModeAutoProfile(unittest.TestCase):

    def test_auto_default_resolves_to_monitor(self):
        self.assertEqual(resolve_mode("auto", profile="default"), "monitor")

    def test_auto_soft_resolves_to_monitor(self):
        self.assertEqual(resolve_mode("auto", profile="soft"), "monitor")

    def test_auto_strict_resolves_to_tighten(self):
        self.assertEqual(resolve_mode("auto", profile="strict"), "tighten")

    def test_auto_hard_resolves_to_veto(self):
        self.assertEqual(resolve_mode("auto", profile="hard"), "veto")

    def test_explicit_mode_unchanged(self):
        self.assertEqual(resolve_mode("veto", profile="default"), "veto")
        self.assertEqual(resolve_mode("tighten", profile="hard"), "tighten")
        self.assertEqual(resolve_mode("off", profile="hard"), "off")


if __name__ == "__main__":
    unittest.main()
