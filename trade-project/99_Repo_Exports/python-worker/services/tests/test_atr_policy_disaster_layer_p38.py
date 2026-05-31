from __future__ import annotations

"""Unit tests for ATR Policy Phase 3.8 — Disaster Layer.

Tests:
  - verify_active_policy (all failure modes)
  - verify_policy_dict
  - rollback_to_last_good (both paths)
  - mirror_after_verified_apply
  - resolver hardening (kill_switch, last_good fallback, corruption)
  - callback_watchdog.check_once
  - chaos_drill_runner.run_once (DRY_RUN only — never EXECUTE in tests)
"""

import json
import time
import unittest
from unittest.mock import MagicMock

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

COHORT = {
    "source": "CryptoOrderFlow",
    "symbol": "BTCUSDT",
    "scenario": "breakout",
    "regime": "trend_up",
    "risk_horizon_bucket": "short",
}

VALID_POLICY = {
    **COHORT,
    "stop_ttl_mode": "canary",
    "trailing_mode": "canary",
    "reason_code": "TEST_POLICY",
    "policy_ver": 1,
    "updated_at_ms": int(time.time() * 1000),
}

ACTIVE_KEY = (
    "cfg:atr_policy:active:"
    "CryptoOrderFlow:BTCUSDT:breakout:trend_up:short"
)
LAST_GOOD_KEY = (
    "cfg:atr_policy:last_good:"
    "CryptoOrderFlow:BTCUSDT:breakout:trend_up:short"
)
KILL_SWITCH_KEY = (
    "cfg:atr_policy:kill_switch:"
    "CryptoOrderFlow:BTCUSDT:breakout:trend_up:short"
)


def _mock_redis(data: dict) -> MagicMock:
    """Build a mock redis client that returns canned data."""
    r = MagicMock()
    r.get.side_effect = lambda k: data.get(k)
    r.exists.side_effect = lambda k: int(k in data)
    r.xadd.return_value = "1-1"
    return r


# ──────────────────────────────────────────────────────────────────────────────
# verify_active_policy
# ──────────────────────────────────────────────────────────────────────────────

class TestVerifyActivePolicy(unittest.TestCase):

    def setUp(self):
        import os
        os.environ["ATR_POLICY_VERIFY_ENABLE"] = "1"

    def _call(self, redis_data: dict) -> dict:
        from services.atr_policy_post_apply_verifier import verify_active_policy
        r = _mock_redis(redis_data)
        return verify_active_policy(ACTIVE_KEY, r, publish=False)

    def test_valid_key_ok(self):
        result = self._call({ACTIVE_KEY: json.dumps(VALID_POLICY)})
        self.assertTrue(result["verified_ok"])
        self.assertEqual(result["reason_code"], "ACTIVE_POLICY_VERIFIED")

    def test_missing_key(self):
        result = self._call({})
        self.assertFalse(result["verified_ok"])
        self.assertEqual(result["reason_code"], "ACTIVE_KEY_MISSING")

    def test_corrupted_json(self):
        result = self._call({ACTIVE_KEY: '{"broken":'})
        self.assertFalse(result["verified_ok"])
        self.assertEqual(result["reason_code"], "ACTIVE_KEY_JSON_CORRUPTED")

    def test_missing_fields(self):
        policy = {**VALID_POLICY}
        del policy["stop_ttl_mode"]
        del policy["trailing_mode"]
        result = self._call({ACTIVE_KEY: json.dumps(policy)})
        self.assertFalse(result["verified_ok"])
        self.assertEqual(result["reason_code"], "ACTIVE_KEY_FIELDS_MISSING")
        self.assertIn("stop_ttl_mode", result["missing"])

    def test_invalid_stop_mode(self):
        policy = {**VALID_POLICY, "stop_ttl_mode": "unknown_mode"}
        result = self._call({ACTIVE_KEY: json.dumps(policy)})
        self.assertFalse(result["verified_ok"])
        self.assertEqual(result["reason_code"], "ACTIVE_KEY_STOP_MODE_INVALID")

    def test_invalid_trailing_mode(self):
        policy = {**VALID_POLICY, "trailing_mode": "live2"}
        result = self._call({ACTIVE_KEY: json.dumps(policy)})
        self.assertFalse(result["verified_ok"])
        self.assertEqual(result["reason_code"], "ACTIVE_KEY_TRAIL_MODE_INVALID")

    def test_kill_switch_active(self):
        ks = json.dumps({"enabled": True, "ts_ms": int(time.time() * 1000), "reason_code": "TEST"})
        result = self._call({ACTIVE_KEY: json.dumps(VALID_POLICY), KILL_SWITCH_KEY: ks})
        self.assertFalse(result["verified_ok"])
        self.assertEqual(result["reason_code"], "COHORT_KILL_SWITCHED")

    def test_kill_switch_disabled(self):
        ks = json.dumps({"enabled": False})
        result = self._call({ACTIVE_KEY: json.dumps(VALID_POLICY), KILL_SWITCH_KEY: ks})
        self.assertTrue(result["verified_ok"])


# ──────────────────────────────────────────────────────────────────────────────
# rollback_to_last_good
# ──────────────────────────────────────────────────────────────────────────────

class TestRollbackToLastGood(unittest.TestCase):

    def setUp(self):
        import os
        os.environ["ATR_POLICY_ROLLBACK_ENABLE"] = "1"
        os.environ["ATR_POLICY_ROLLBACK_ADVISORY_ONLY"] = "0"

    def _call(self, redis_data: dict, trigger="TEST") -> dict:
        from services.atr_policy_rollback_watcher import rollback_to_last_good
        r = _mock_redis(redis_data)
        r.set = MagicMock()
        r.delete = MagicMock()
        r.xadd = MagicMock(return_value="1-1")
        # Re-wire get to also handle set calls
        store = dict(redis_data)

        def _get(k):
            return store.get(k)

        def _set(k, v):
            store[k] = v

        r.get.side_effect = _get
        r.set.side_effect = _set
        return rollback_to_last_good(COHORT, r, trigger_reason=trigger)

    def test_last_good_exists(self):
        result = self._call({LAST_GOOD_KEY: json.dumps(VALID_POLICY)})
        self.assertTrue(result["rollback_ok"])
        self.assertEqual(result["reason_code"], "ROLLBACK_TO_LAST_GOOD")
        self.assertFalse(result.get("advisory_only"))

    def test_last_good_absent_kill_switch(self):
        result = self._call({})
        self.assertFalse(result["rollback_ok"])
        self.assertEqual(result["reason_code"], "NO_LAST_GOOD_KILL_SWITCHED")

    def test_advisory_only_last_good(self):
        import os
        os.environ["ATR_POLICY_ROLLBACK_ADVISORY_ONLY"] = "1"
        result = self._call({LAST_GOOD_KEY: json.dumps(VALID_POLICY)})
        self.assertTrue(result["rollback_ok"])
        self.assertTrue(result.get("advisory_only"))
        os.environ["ATR_POLICY_ROLLBACK_ADVISORY_ONLY"] = "0"

    def test_advisory_only_no_last_good(self):
        import os
        os.environ["ATR_POLICY_ROLLBACK_ADVISORY_ONLY"] = "1"
        result = self._call({})
        self.assertFalse(result["rollback_ok"])
        self.assertTrue(result.get("advisory_only"))
        os.environ["ATR_POLICY_ROLLBACK_ADVISORY_ONLY"] = "0"


# ──────────────────────────────────────────────────────────────────────────────
# mirror_after_verified_apply
# ──────────────────────────────────────────────────────────────────────────────

class TestMirrorService(unittest.TestCase):

    def setUp(self):
        import os
        os.environ["ATR_POLICY_MIRROR_ENABLE"] = "1"
        os.environ["ATR_POLICY_MIRROR_ADVISORY_ONLY"] = "0"

    def _call(self, verify_result: dict, redis_data: dict = None) -> bool:  # type: ignore
        from services.atr_policy_active_mirror_service import mirror_after_verified_apply
        r = _mock_redis(redis_data or {})
        store: dict = {}
        r.set.side_effect = lambda k, v: store.update({k: v})
        r.get.side_effect = lambda k: (redis_data or {}).get(k)
        r.xadd = MagicMock(return_value="1-1")
        return mirror_after_verified_apply(VALID_POLICY, verify_result, r)

    def test_verified_ok_writes_last_good(self):
        result = self._call({"verified_ok": True, "reason_code": "ACTIVE_POLICY_VERIFIED"})
        self.assertTrue(result)

    def test_verify_failed_no_write(self):
        result = self._call({"verified_ok": False, "reason_code": "ACTIVE_KEY_MISSING"})
        self.assertFalse(result)

    def test_kill_switch_blocks_mirror(self):
        ks = json.dumps({"enabled": True})
        result = self._call(
            {"verified_ok": True, "reason_code": "ACTIVE_POLICY_VERIFIED"},
            {KILL_SWITCH_KEY: ks},
        )
        self.assertFalse(result)

    def test_advisory_only(self):
        import os
        os.environ["ATR_POLICY_MIRROR_ADVISORY_ONLY"] = "1"
        result = self._call({"verified_ok": True, "reason_code": "ACTIVE_POLICY_VERIFIED"})
        self.assertTrue(result)
        os.environ["ATR_POLICY_MIRROR_ADVISORY_ONLY"] = "0"


# ──────────────────────────────────────────────────────────────────────────────
# Resolver hardening (Phase 3.8)
# ──────────────────────────────────────────────────────────────────────────────

class TestResolverHardening(unittest.TestCase):

    def _resolver(self, redis_data: dict):
        from services.atr_policy_resolver import ATRPolicyResolver
        r = ATRPolicyResolver()
        r._r = _mock_redis(redis_data)
        r.enable = True
        return r

    def _resolve(self, redis_data: dict) -> dict:
        resolver = self._resolver(redis_data)
        return resolver.resolve(
            source=COHORT["source"],
            symbol=COHORT["symbol"],
            scenario=COHORT["scenario"],
            regime=COHORT["regime"],
            risk_horizon_bucket=COHORT["risk_horizon_bucket"],
        )

    def test_valid_active_key(self):
        result = self._resolve({ACTIVE_KEY: json.dumps(VALID_POLICY)})
        self.assertTrue(result["hit"])
        self.assertEqual(result["stop_ttl_mode"], "canary")
        self.assertFalse(result["kill_switch_active"])
        self.assertFalse(result["last_good_used"])

    def test_kill_switch_returns_canary(self):
        ks = json.dumps({"enabled": True})
        result = self._resolve({
            ACTIVE_KEY: json.dumps(VALID_POLICY),
            KILL_SWITCH_KEY: ks,
        })
        self.assertFalse(result["hit"])
        self.assertEqual(result["reason_code"], "KILL_SWITCH_ACTIVE")
        self.assertTrue(result["kill_switch_active"])

    def test_corrupted_active_falls_back_to_last_good(self):
        result = self._resolve({
            ACTIVE_KEY: '{"broken":',
            LAST_GOOD_KEY: json.dumps(VALID_POLICY),
        })
        self.assertTrue(result["hit"])
        self.assertTrue(result["last_good_used"])
        self.assertIn("FALLBACK_LAST_GOOD", result["reason_code"])

    def test_missing_active_falls_back_to_last_good(self):
        result = self._resolve({LAST_GOOD_KEY: json.dumps(VALID_POLICY)})
        self.assertTrue(result["hit"])
        self.assertTrue(result["last_good_used"])
        self.assertEqual(result["reason_code"], "ACTIVE_MISSING_FALLBACK_LAST_GOOD")

    def test_invalid_mode_active_falls_back_to_last_good(self):
        bad_policy = {**VALID_POLICY, "stop_ttl_mode": "invalid_mode"}
        result = self._resolve({
            ACTIVE_KEY: json.dumps(bad_policy),
            LAST_GOOD_KEY: json.dumps(VALID_POLICY),
        })
        self.assertTrue(result["hit"])
        self.assertTrue(result["last_good_used"])

    def test_no_policy_anywhere_miss(self):
        result = self._resolve({})
        self.assertFalse(result["hit"])
        self.assertEqual(result["reason_code"], "ATR_POLICY_MISS")
        self.assertEqual(result["stop_ttl_mode"], "canary")


# ──────────────────────────────────────────────────────────────────────────────
# Callback watchdog
# ──────────────────────────────────────────────────────────────────────────────

class TestCallbackWatchdog(unittest.TestCase):

    def setUp(self):
        import os
        os.environ["ATR_POLICY_CALLBACK_WATCHDOG_ENABLE"] = "1"
        os.environ["ATR_POLICY_CALLBACK_WARN_SEC"] = "300"
        os.environ["ATR_POLICY_CALLBACK_CRITICAL_SEC"] = "600"

    def _call(self, redis_data: dict) -> dict:
        from services.atr_policy_callback_watchdog import check_once
        r = _mock_redis(redis_data)
        pending = redis_data.get("__pending_ids__", set())
        r.smembers.return_value = pending
        r.xadd = MagicMock(return_value="1-1")
        return check_once(r)

    def test_no_pending_backlog_ok(self):
        result = self._call({"__pending_ids__": set()})
        self.assertEqual(result["severity"], "OK")
        self.assertEqual(result["reason"], "NO_PENDING_BACKLOG")

    def test_pending_with_recent_callback_ok(self):
        # pending exists, but callback was just now
        now_ms = int(time.time() * 1000)
        pid = "abc123"
        proposal = json.dumps({
            "proposal_id": pid,
            "status": "SUBMITTED",
            "created_at_ms": now_ms - 60_000,
        })
        result = self._call({
            "__pending_ids__": {pid},
            f"cfg:proposals:atr_policy:{pid}": proposal,
            "atr_policy:telegram:last_callback_ts_ms": str(now_ms - 1_000),  # 1s ago
        })
        self.assertEqual(result["severity"], "OK")

    def test_pending_with_warn_silence(self):
        now_ms = int(time.time() * 1000)
        pid = "abc124"
        proposal = json.dumps({"proposal_id": pid, "status": "SUBMITTED", "created_at_ms": now_ms})
        # last callback was 400s ago (> 300 warn threshold)
        result = self._call({
            "__pending_ids__": {pid},
            f"cfg:proposals:atr_policy:{pid}": proposal,
            "atr_policy:telegram:last_callback_ts_ms": str(now_ms - 400_000),
        })
        self.assertEqual(result["severity"], "WARN")

    def test_pending_with_critical_silence(self):
        now_ms = int(time.time() * 1000)
        pid = "abc125"
        proposal = json.dumps({"proposal_id": pid, "status": "SUBMITTED", "created_at_ms": now_ms})
        result = self._call({
            "__pending_ids__": {pid},
            f"cfg:proposals:atr_policy:{pid}": proposal,
            "atr_policy:telegram:last_callback_ts_ms": str(now_ms - 700_000),
        })
        self.assertEqual(result["severity"], "CRITICAL")


# ──────────────────────────────────────────────────────────────────────────────
# Chaos drill runner — DRY_RUN only
# ──────────────────────────────────────────────────────────────────────────────

class TestChaosDrillRunnerDryRun(unittest.TestCase):

    def setUp(self):
        import os
        os.environ["ATR_POLICY_CHAOS_ENABLE"] = "1"
        os.environ["ATR_POLICY_CHAOS_MODE"] = "DRY_RUN"
        os.environ["ATR_POLICY_CHAOS_TARGET_JSON"] = json.dumps(COHORT)

    def _run(self, scenario: str) -> dict:
        import os
        os.environ["ATR_POLICY_CHAOS_SCENARIO"] = scenario
        from importlib import reload

        import services.atr_policy_chaos_drill_runner as m
        reload(m)
        r = _mock_redis({})
        r.set = MagicMock()
        r.delete = MagicMock()
        r.xadd = MagicMock(return_value="1-1")
        return m.run_once(r)

    def test_disabled_returns_false(self):
        import os
        os.environ["ATR_POLICY_CHAOS_ENABLE"] = "0"
        from services.atr_policy_chaos_drill_runner import run_once
        result = run_once(_mock_redis({}))
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "CHAOS_DISABLED")
        os.environ["ATR_POLICY_CHAOS_ENABLE"] = "1"

    def test_dry_run_does_not_mutate(self):
        """DRY_RUN must never call r.set() or r.delete()."""
        for scenario in [
            "TELEGRAM_CALLBACK_BLACKHOLE",
            "RECONCILE_STUCK",
            "ACTIVE_KEY_CORRUPT",
            "ACTIVE_KEY_DELETE",
            "REDIS_PARTIAL_LOSS_SIM",
        ]:
            with self.subTest(scenario=scenario):
                import os
                os.environ["ATR_POLICY_CHAOS_SCENARIO"] = scenario
                from services.atr_policy_chaos_drill_runner import run_once
                r = _mock_redis({})
                r.set = MagicMock(side_effect=Exception("should_not_be_called"))
                r.delete = MagicMock(side_effect=Exception("should_not_be_called"))
                r.xadd = MagicMock(return_value="1-1")
                result = run_once(r)
                self.assertTrue(result.get("ok"), f"DRY_RUN {scenario} returned ok=False: {result}")
                r.set.assert_not_called()
                r.delete.assert_not_called()

    def test_unknown_scenario_fails(self):
        import os
        os.environ["ATR_POLICY_CHAOS_SCENARIO"] = "NOT_A_REAL_SCENARIO"
        from services.atr_policy_chaos_drill_runner import run_once
        result = run_once(_mock_redis({}))
        self.assertFalse(result["ok"])
        self.assertIn("UNKNOWN_SCENARIO", result["reason"])

    def test_incomplete_target_fails(self):
        import os
        os.environ["ATR_POLICY_CHAOS_TARGET_JSON"] = json.dumps({"source": "X"})
        os.environ["ATR_POLICY_CHAOS_SCENARIO"] = "ACTIVE_KEY_DELETE"
        from services.atr_policy_chaos_drill_runner import run_once
        result = run_once(_mock_redis({}))
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "TARGET_INCOMPLETE")
        os.environ["ATR_POLICY_CHAOS_TARGET_JSON"] = json.dumps(COHORT)


if __name__ == "__main__":
    unittest.main()
