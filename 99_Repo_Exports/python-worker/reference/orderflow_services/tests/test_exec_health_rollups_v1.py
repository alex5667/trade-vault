from __future__ import annotations

import os
import unittest
from unittest import mock

from services.orderflow.exec_health_rollups import (
    aread_exec_health_rollups,
    build_rollup_keys,
    decide_exec_health_from_env,
    read_exec_health_rollups_sync,
)


class _AsyncRedisStub:
    def __init__(self, store):
        self.store = dict(store)
        self.calls = []

    async def mget(self, keys):
        self.calls.append(list(keys))
        return [self.store.get(k) for k in keys]

    async def get(self, key):
        return self.store.get(key)


class _SyncRedisStub:
    def __init__(self, store):
        self.store = dict(store)
        self.calls = []

    def mget(self, keys):
        self.calls.append(list(keys))
        return [self.store.get(k) for k in keys]

    def get(self, key):
        return self.store.get(key)


class ExecHealthRollupsV1Test(unittest.TestCase):
    def test_bounded_keyspace_one_metric(self):
        keys = build_rollup_keys(
            metric="is_p95_bps",
            sym="BTCUSDT",
            venue="binance",
            session="eu",
            tf="1m",
            kind="breakout",
            side="LONG",
        )
        self.assertEqual(len(keys), 16)
        self.assertEqual(keys[0], "tca:is_p95_bps:BTCUSDT:binance:eu:1m:breakout:LONG")
        self.assertEqual(keys[-1], "tca:is_p95_bps:BTCUSDT:binance:all:all:all:all")

    def test_sync_reader_aggregates_worst_by_delta(self):
        store = {
            "tca:is_p95_bps:BTCUSDT:binance:eu:1m:breakout:LONG": "4.0",
            "tca:perm_impact_p95_bps:1:BTCUSDT:binance:eu:1m:breakout:LONG": "1.5",
            "tca:perm_impact_p95_bps:5:BTCUSDT:binance:eu:1m:breakout:LONG": "3.8",
            "tca:realized_spread_p50_bps:1:BTCUSDT:binance:eu:1m:breakout:LONG": "-0.5",
            "tca:realized_spread_p50_bps:5:BTCUSDT:binance:eu:1m:breakout:LONG": "-2.2",
        }
        redis = _SyncRedisStub(store)
        roll = read_exec_health_rollups_sync(
            redis=redis,
            sym="BTCUSDT",
            venue="binance",
            session="eu",
            tf="1m",
            kind="breakout",
            side="LONG",
            delta_sec_list=(1, 5),
        )
        self.assertEqual(roll["perm_impact_p95_bps"], 3.8)
        self.assertEqual(int(roll["perm_impact_p95_bps_delta_sec"]), 5)
        self.assertEqual(roll["realized_spread_p50_bps"], -2.2)
        self.assertEqual(int(roll["realized_spread_p50_bps_delta_sec"]), 5)
        self.assertEqual(len(redis.calls[0]), 16 * 5)

    def test_auto_profile_mapping(self):
        roll = {"is_p95_bps": 6.0, "perm_impact_p95_bps": 4.0}
        with mock.patch.dict(os.environ, {"EXEC_HEALTH_MODE": "auto", "GATE_PROFILE": "default", "EXEC_MAX_IS_P95_BPS": "5", "EXEC_MAX_PERM_IMPACT_P95_BPS": "3"}, clear=False):
            dec = decide_exec_health_from_env(profile="default", rollups=roll, scope="edge")
            self.assertEqual(dec.mode, "monitor")
            self.assertFalse(dec.veto)
        with mock.patch.dict(os.environ, {"EXEC_HEALTH_MODE": "auto", "GATE_PROFILE": "strict", "EXEC_MAX_IS_P95_BPS": "5", "EXEC_MAX_PERM_IMPACT_P95_BPS": "3", "EXEC_TIGHTEN_K_MULT": "1.2"}, clear=False):
            dec = decide_exec_health_from_env(profile="strict", rollups=roll, scope="edge")
            self.assertEqual(dec.mode, "tighten")
            self.assertGreater(dec.tighten_k_mult, 1.0)
        with mock.patch.dict(os.environ, {"EXEC_HEALTH_MODE": "auto", "GATE_PROFILE": "hard", "EXEC_MAX_IS_P95_BPS": "5", "EXEC_MAX_PERM_IMPACT_P95_BPS": "3"}, clear=False):
            dec = decide_exec_health_from_env(profile="hard", rollups=roll, scope="edge")
            self.assertEqual(dec.mode, "veto")
            self.assertTrue(dec.veto)
            self.assertEqual(dec.reason_code, "VETO_IMPL_SHORTFALL_P95")

    def test_async_reader_works(self):
        store = {
            "tca:is_p95_bps:ETHUSDT:binance:eu:5m:continuation:SHORT": "2.0",
            "tca:perm_impact_p95_bps:1:ETHUSDT:binance:eu:5m:continuation:SHORT": "1.0",
            "tca:realized_spread_p50_bps:1:ETHUSDT:binance:eu:5m:continuation:SHORT": "-0.1",
        }
        redis = _AsyncRedisStub(store)
        import asyncio
        roll = asyncio.run(aread_exec_health_rollups(
            redis=redis,
            sym="ETHUSDT",
            venue="binance",
            session="eu",
            tf="5m",
            kind="continuation",
            side="SHORT",
            delta_sec_list=(1,),
        ))
        self.assertEqual(roll["is_p95_bps"], 2.0)
        self.assertEqual(len(redis.calls[0]), 16 * 3)

    def test_fail_open_empty_redis(self):
        redis = _SyncRedisStub({})
        roll = read_exec_health_rollups_sync(
            redis=redis,
            sym="BTCUSDT",
            venue="binance",
            session="eu",
            tf="1m",
            kind="breakout",
            side="LONG",
        )
        self.assertEqual(roll, {})
        dec = decide_exec_health_from_env(profile="hard", rollups=roll, scope="edge")
        self.assertFalse(dec.veto)

    def test_scope_aware_mode_override(self):
        roll = {"is_p95_bps": 6.0, "perm_impact_p95_bps": 4.0}
        with mock.patch.dict(os.environ, {
            "EXEC_HEALTH_MODE": "auto",
            "GATE_PROFILE": "hard",
            "PIPELINE_EXEC_HEALTH_MODE": "monitor",
            "EXEC_MAX_IS_P95_BPS": "5",
            "EXEC_MAX_PERM_IMPACT_P95_BPS": "3",
        }, clear=False):
            # Entry: still uses global auto->hard=veto
            dec_entry = decide_exec_health_from_env(profile="hard", rollups=roll, scope="entry_policy")
            # Scope-specific override: ENTRY_EXEC_HEALTH_MODE not set -> falls back to EXEC_HEALTH_MODE=auto, profile->veto
            # Pipeline: overridden to monitor
            dec_pipeline = decide_exec_health_from_env(profile="hard", rollups=roll, scope="pipeline")
            self.assertEqual(dec_pipeline.mode, "monitor")
            self.assertFalse(dec_pipeline.veto)

    def test_off_mode_never_vetoes(self):
        roll = {"is_p95_bps": 100.0, "perm_impact_p95_bps": 100.0}
        with mock.patch.dict(os.environ, {
            "EXEC_HEALTH_MODE": "off",
            "EXEC_MAX_IS_P95_BPS": "1",
            "EXEC_MAX_PERM_IMPACT_P95_BPS": "1",
        }, clear=False):
            dec = decide_exec_health_from_env(profile="hard", rollups=roll, scope="edge")
            self.assertFalse(dec.apply)
            self.assertFalse(dec.veto)
            self.assertEqual(dec.mode, "off")


if __name__ == "__main__":
    unittest.main()
