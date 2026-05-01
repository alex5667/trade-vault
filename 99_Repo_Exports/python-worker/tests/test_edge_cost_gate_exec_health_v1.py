from __future__ import annotations
"""
Unit tests for EdgeCostGate execution-health overlay (TCA rollups).

Tests:
  1. veto when both IS_p95 AND perm_impact_p95 exceed thresholds -> VETO_IMPL_SHORTFALL_P95
  2. no veto when only IS_p95 high (perm_impact ok) -> should NOT veto in strict logic
  3. tighten mode: slippage + tighten_add_bps must grow, no veto
  4. monitor mode: no tighten, no veto (only annotate ctx)
  5. off mode: completely skipped
  6. adverse selection veto (opt-in flag EDGE_EXEC_HEALTH_VETO_ON_ADVERSE=1)
  7. fail-open when redis is None
  8. _tca_key_candidates fallback ordering
  9. _parse_csv_ints handles edge cases

How to run:
  cd /home/alex/front/trade/scanner_infra
  PYTHONPATH=python-worker python3 -m pytest python-worker/tests/test_edge_cost_gate_exec_health_v1.py -v
  # OR (from within python-worker):
  cd python-worker && python3 -m pytest tests/test_edge_cost_gate_exec_health_v1.py -v
"""

import os
import sys
import math
import unittest

# Make Python-worker package importable regardless of CWD.
_HERE = os.path.abspath(os.path.dirname(__file__))
_PKGROOT = os.path.abspath(os.path.join(_HERE, ".."))
for _p in (_PKGROOT, os.path.join(_PKGROOT, "..")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from handlers.crypto_orderflow.utils.edge_cost_gate import (
        EdgeCostGate,
        EdgeCostGateDecision,
        _parse_csv_ints,
        _tca_key_candidates,
        _load_exec_health_rollups,
        _redis_get_float_best_effort,
    )
except ImportError:
    from python_worker.handlers.crypto_orderflow.utils.edge_cost_gate import (  # type: ignore
        EdgeCostGate,
        EdgeCostGateDecision,
        _parse_csv_ints,
        _tca_key_candidates,
        _load_exec_health_rollups,
        _redis_get_float_best_effort,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeRedis:
    """Minimal fake Redis for unit tests – GET only (no HGETALL for rollups)."""
    def __init__(self, kv: dict):
        self.kv = {k: (v if isinstance(v, bytes) else str(v).encode()) for k, v in kv.items()}

    def get(self, key: str):
        return self.kv.get(key)

    def hgetall(self, key: str) -> dict:
        return {}


class _Ctx:
    """Minimal signal context for EdgeCostGate.evaluate()."""
    def __init__(self):
        self.symbol = "BTCUSDT"
        self.venue = "binance"
        self.ts_ms = 1_741_284_125_000  # valid epoch ms
        self.session = "na"
        self.tf = "all"
        self.side = "long"
        self.entry_price = 100.0
        self.tp1_price = 102.0
        self.sl_price = 98.0
        self.bid = 99.95
        self.ask = 100.05
        self.spread_bps = (self.ask - self.bid) / ((self.ask + self.bid) / 2) * 10_000
        # For EV mode we also need tp1_hit_prob etc; gate will fail-open if missing
        self.tp1_hit_prob = 0.65
        self.tp1_hit_n = 100
        self.tp1_hit_src = "ema"


_TS_SAFE_ENV = {
    "EDGE_DISABLE_EMA": "1",
    "EDGE_TS_BAD_POLICY": "correct_skip_ema",
    "EDGE_DRIFT_TIGHTEN": "0",
    "EDGE_COST_GATE_ENABLED": "1",
    "EDGE_FEES_BPS_DEFAULT": "4.0",
    "EDGE_SLIPPAGE_BPS_DEFAULT": "4.0",
    "EDGE_SLIPPAGE_USE_SPREAD_HALF": "0",
    "EDGE_EXPECTED_MOVE_MODE": "tp1",
    "EDGE_COST_K": "1.0",
}


def _make_gate(mode: str = "tp1") -> EdgeCostGate:
    """Create a minimal EdgeCostGate with gate enabled. Always sets required env."""
    # Ensure deterministic env before from_env() reads it
    for k, v in _TS_SAFE_ENV.items():
        os.environ.setdefault(k, v)
    g = EdgeCostGate.from_env()
    g.enabled = True
    g.mode = mode  # type: ignore[assignment]
    g.apply_kinds = set()  # applies to all kinds
    return g


def _clear_env(*names):
    for n in names:
        os.environ.pop(n, None)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestExecHealthVeto(unittest.TestCase):
    """VETO_IMPL_SHORTFALL_P95: both IS_p95 AND perm_impact_p95 must exceed thresholds."""

    def setUp(self):
        _clear_env(
            "EDGE_EXEC_HEALTH_MODE", "EXEC_HEALTH_MODE",
            "EXEC_MAX_IS_P95_BPS", "EXEC_MAX_PERM_IMPACT_P95_BPS",
            "EXEC_MIN_REALIZED_SPREAD_P50_BPS",
            "EDGE_EXEC_HEALTH_TIGHTEN_ADD_CAP_BPS", "EDGE_EXEC_HEALTH_TIGHTEN_ADD_MULT",
            "EDGE_EXEC_HEALTH_VETO_ON_ADVERSE",
            "EXEC_TCA_DELTA_SEC_LIST",
        )
        # Deterministic TS/EMA env for every test in this class
        for k, v in _TS_SAFE_ENV.items():
            os.environ[k] = v

    def tearDown(self):
        _clear_env(
            "EDGE_EXEC_HEALTH_MODE", "EXEC_HEALTH_MODE",
            "EXEC_MAX_IS_P95_BPS", "EXEC_MAX_PERM_IMPACT_P95_BPS",
            "EDGE_EXEC_HEALTH_VETO_ON_ADVERSE",
            *list(_TS_SAFE_ENV.keys()),
        )

    def test_veto_both_is_and_perm_exceed(self):
        """Gate must veto with VETO_IMPL_SHORTFALL_P95 when both IS and perm_impact are high."""
        kv = {
            # IS_p95 = 20 bps, threshold 10 → is_bad=True
            "tca:is_p95_bps:BTCUSDT:binance:na:all:all:long": b"20.0",
            # perm_impact_p95 = 15 bps @ delta=1, threshold 10 → perm_bad=True
            "tca:perm_impact_p95_bps:1:BTCUSDT:binance:na:all:all:long": b"15.0",
        }
        gate = _make_gate()
        gate.redis = _FakeRedis(kv)

        os.environ["EDGE_EXEC_HEALTH_MODE"] = "veto"
        os.environ["EXEC_MAX_IS_P95_BPS"] = "10"
        os.environ["EXEC_MAX_PERM_IMPACT_P95_BPS"] = "10"

        ctx = _Ctx()
        d = gate.evaluate(ctx=ctx, kind="breakout", symbol=ctx.symbol)
        self.assertTrue(d.veto, "Expected veto when both IS and perm_impact are high")
        self.assertEqual(d.reason_code, "VETO_IMPL_SHORTFALL_P95")
        self.assertFalse(math.isnan(d.exec_is_p95_bps))
        self.assertAlmostEqual(d.exec_is_p95_bps, 20.0)
        self.assertAlmostEqual(d.exec_perm_impact_p95_bps, 15.0)

    def test_no_veto_only_is_high(self):
        """Gate must NOT veto when only IS_p95 is high but perm_impact is ok."""
        kv = {
            # IS_p95 = 20 bps, threshold 10 → is_bad=True
            "tca:is_p95_bps:BTCUSDT:binance:na:all:all:long": b"20.0",
            # perm_impact = 5 bps, threshold 10 → perm_bad=False
            "tca:perm_impact_p95_bps:1:BTCUSDT:binance:na:all:all:long": b"5.0",
        }
        gate = _make_gate()
        gate.redis = _FakeRedis(kv)

        os.environ["EDGE_EXEC_HEALTH_MODE"] = "veto"
        os.environ["EXEC_MAX_IS_P95_BPS"] = "10"
        os.environ["EXEC_MAX_PERM_IMPACT_P95_BPS"] = "10"

        ctx = _Ctx()
        d = gate.evaluate(ctx=ctx, kind="breakout", symbol=ctx.symbol)
        self.assertFalse(d.veto, "Should NOT veto when only is_bad (perm ok)")

    def test_no_veto_only_perm_high(self):
        """Gate must NOT veto when only perm_impact is high but IS is ok."""
        kv = {
            "tca:is_p95_bps:BTCUSDT:binance:na:all:all:long": b"5.0",
            "tca:perm_impact_p95_bps:1:BTCUSDT:binance:na:all:all:long": b"20.0",
        }
        gate = _make_gate()
        gate.redis = _FakeRedis(kv)

        os.environ["EDGE_EXEC_HEALTH_MODE"] = "veto"
        os.environ["EXEC_MAX_IS_P95_BPS"] = "10"
        os.environ["EXEC_MAX_PERM_IMPACT_P95_BPS"] = "10"

        ctx = _Ctx()
        d = gate.evaluate(ctx=ctx, kind="breakout", symbol=ctx.symbol)
        self.assertFalse(d.veto, "Should NOT veto when only perm_bad (IS ok)")


class TestExecHealthTighten(unittest.TestCase):
    """Tighten mode: inflates slippage by excess, bounded by cap. No hard veto."""

    def setUp(self):
        _clear_env(
            "EDGE_EXEC_HEALTH_MODE", "EXEC_HEALTH_MODE",
            "EXEC_MAX_IS_P95_BPS", "EXEC_MAX_PERM_IMPACT_P95_BPS",
            "EXEC_MIN_REALIZED_SPREAD_P50_BPS",
            "EDGE_EXEC_HEALTH_TIGHTEN_ADD_CAP_BPS", "EDGE_EXEC_HEALTH_TIGHTEN_ADD_MULT",
            "EXEC_TCA_DELTA_SEC_LIST",
        )
        for k, v in _TS_SAFE_ENV.items():
            os.environ[k] = v

    def tearDown(self):
        _clear_env("EDGE_EXEC_HEALTH_MODE", "EXEC_MAX_IS_P95_BPS", *list(_TS_SAFE_ENV.keys()))

    def test_tighten_adds_slippage_bounded_by_cap(self):
        """Tighten must add slippage excess to slip_bps, bounded by EDGE_EXEC_HEALTH_TIGHTEN_ADD_CAP_BPS."""
        kv = {
            # IS_p95 = 12, threshold 10 → excess = 2 bps
            "tca:is_p95_bps:BTCUSDT:binance:na:all:all:long": b"12.0",
            "tca:perm_impact_p95_bps:1:BTCUSDT:binance:na:all:all:long": b"1.0",
        }
        gate = _make_gate()
        gate.redis = _FakeRedis(kv)

        os.environ["EDGE_EXEC_HEALTH_MODE"] = "tighten"
        os.environ["EXEC_MAX_IS_P95_BPS"] = "10"
        os.environ["EDGE_EXEC_HEALTH_TIGHTEN_ADD_CAP_BPS"] = "8"  # cap=8
        os.environ["EDGE_EXEC_HEALTH_TIGHTEN_ADD_MULT"] = "1.0"

        ctx = _Ctx()
        d = gate.evaluate(ctx=ctx, kind="breakout", symbol=ctx.symbol)
        # No veto in tighten mode (thresholds only tighten, not hard veto)
        # Note: veto may still happen if expected_move < threshold+add, but not due to exec_health logic itself
        add_bps = getattr(ctx, "exec_health_tighten_add_bps", 0.0)
        self.assertGreater(add_bps, 0.0, "Expected tighten_add_bps > 0 when IS exceeds threshold")
        self.assertLessEqual(add_bps, 8.0, "Tighten add must be bounded by cap=8")

    def test_tighten_no_veto_code(self):
        """Veto reason must not be exec_health reason in tighten mode."""
        kv = {
            "tca:is_p95_bps:BTCUSDT:binance:na:all:all:long": b"15.0",
            "tca:perm_impact_p95_bps:1:BTCUSDT:binance:na:all:all:long": b"5.0",
        }
        gate = _make_gate()
        gate.redis = _FakeRedis(kv)

        os.environ["EDGE_EXEC_HEALTH_MODE"] = "tighten"
        os.environ["EXEC_MAX_IS_P95_BPS"] = "10"

        ctx = _Ctx()
        d = gate.evaluate(ctx=ctx, kind="breakout", symbol=ctx.symbol)
        # Even if gate vetoes (due to insufficient edge), it should NOT be exec_health veto
        if d.veto:
            self.assertNotIn("VETO_IMPL_SHORTFALL", d.reason_code)
            self.assertNotIn("VETO_ADVERSE", d.reason_code)


class TestExecHealthMonitor(unittest.TestCase):
    """Monitor mode: annotates ctx without tightening or vetoing."""

    def setUp(self):
        _clear_env(
            "EDGE_EXEC_HEALTH_MODE", "EXEC_HEALTH_MODE",
            "EXEC_MAX_IS_P95_BPS", "EXEC_MAX_PERM_IMPACT_P95_BPS",
        )
        for k, v in _TS_SAFE_ENV.items():
            os.environ[k] = v

    def tearDown(self):
        _clear_env("EDGE_EXEC_HEALTH_MODE", "EXEC_MAX_IS_P95_BPS", *list(_TS_SAFE_ENV.keys()))

    def test_monitor_no_tighten(self):
        """In monitor mode, tighten_add_bps must be 0.0 even if IS is high."""
        kv = {
            "tca:is_p95_bps:BTCUSDT:binance:na:all:all:long": b"50.0",
            "tca:perm_impact_p95_bps:1:BTCUSDT:binance:na:all:all:long": b"50.0",
        }
        gate = _make_gate()
        gate.redis = _FakeRedis(kv)

        os.environ["EDGE_EXEC_HEALTH_MODE"] = "monitor"
        os.environ["EXEC_MAX_IS_P95_BPS"] = "5"
        os.environ["EXEC_MAX_PERM_IMPACT_P95_BPS"] = "5"

        ctx = _Ctx()
        gate.evaluate(ctx=ctx, kind="breakout", symbol=ctx.symbol)
        # monitor only annotates; no tighten
        add_bps = getattr(ctx, "exec_health_tighten_add_bps", 0.0)
        self.assertEqual(add_bps, 0.0, "Monitor mode must not add slippage")
        # But must annotate exec_is_p95_bps
        self.assertAlmostEqual(getattr(ctx, "exec_is_p95_bps", float("nan")), 50.0)


class TestExecHealthOff(unittest.TestCase):
    """Off mode: overlay completely disabled, no ctx annotations."""

    def setUp(self):
        _clear_env(
            "EDGE_EXEC_HEALTH_MODE", "EXEC_HEALTH_MODE",
            "EXEC_MAX_IS_P95_BPS",
        )
        for k, v in _TS_SAFE_ENV.items():
            os.environ[k] = v

    def tearDown(self):
        _clear_env("EDGE_EXEC_HEALTH_MODE", "EXEC_MAX_IS_P95_BPS", *list(_TS_SAFE_ENV.keys()))

    def test_off_mode_no_annotations(self):
        kv = {"tca:is_p95_bps:BTCUSDT:binance:na:all:all:long": b"100.0"}
        gate = _make_gate()
        gate.redis = _FakeRedis(kv)
        os.environ["EDGE_EXEC_HEALTH_MODE"] = "off"
        os.environ["EXEC_MAX_IS_P95_BPS"] = "5"

        ctx = _Ctx()
        gate.evaluate(ctx=ctx, kind="breakout", symbol=ctx.symbol)
        # No exec_health_flags set in off mode
        flags = getattr(ctx, "exec_health_flags", "")
        self.assertEqual(flags, "", "Off mode must not set exec_health_flags")


class TestExecHealthAdverseSelection(unittest.TestCase):
    """Adverse selection veto (opt-in)."""

    def setUp(self):
        _clear_env(
            "EDGE_EXEC_HEALTH_MODE", "EXEC_HEALTH_MODE",
            "EXEC_MAX_IS_P95_BPS", "EXEC_MAX_PERM_IMPACT_P95_BPS",
            "EXEC_MIN_REALIZED_SPREAD_P50_BPS",
            "EDGE_EXEC_HEALTH_VETO_ON_ADVERSE",
            "EXEC_TCA_DELTA_SEC_LIST",
        )
        for k, v in _TS_SAFE_ENV.items():
            os.environ[k] = v

    def tearDown(self):
        _clear_env("EDGE_EXEC_HEALTH_MODE", "EXEC_MAX_IS_P95_BPS",
                   "EDGE_EXEC_HEALTH_VETO_ON_ADVERSE", *list(_TS_SAFE_ENV.keys()))

    def test_adverse_veto_opt_in(self):
        """VETO_ADVERSE_SELECTION fires when IS_high OR perm_high AND adverse_bad AND flag=1."""
        kv = {
            "tca:is_p95_bps:BTCUSDT:binance:na:all:all:long": b"20.0",  # is_bad
            "tca:realized_spread_p50_bps:1:BTCUSDT:binance:na:all:all:long": b"-5.0",  # adv_bad
            # perm NOT high:
            "tca:perm_impact_p95_bps:1:BTCUSDT:binance:na:all:all:long": b"3.0",
        }
        gate = _make_gate()
        gate.redis = _FakeRedis(kv)

        os.environ["EDGE_EXEC_HEALTH_MODE"] = "veto"
        os.environ["EXEC_MAX_IS_P95_BPS"] = "10"
        os.environ["EXEC_MAX_PERM_IMPACT_P95_BPS"] = "10"
        os.environ["EXEC_MIN_REALIZED_SPREAD_P50_BPS"] = "-2"  # adv_bad if spread_p50 < -2
        os.environ["EDGE_EXEC_HEALTH_VETO_ON_ADVERSE"] = "1"

        ctx = _Ctx()
        d = gate.evaluate(ctx=ctx, kind="breakout", symbol=ctx.symbol)
        # adverse + is_bad → VETO_ADVERSE_SELECTION
        self.assertTrue(d.veto)
        self.assertEqual(d.reason_code, "VETO_ADVERSE_SELECTION")

    def test_adverse_veto_opt_out_by_default(self):
        """Without VETO_ON_ADVERSE=1, adverse alone (with IS) should NOT veto."""
        kv = {
            "tca:is_p95_bps:BTCUSDT:binance:na:all:all:long": b"20.0",
            "tca:realized_spread_p50_bps:1:BTCUSDT:binance:na:all:all:long": b"-5.0",
            "tca:perm_impact_p95_bps:1:BTCUSDT:binance:na:all:all:long": b"3.0",
        }
        gate = _make_gate()
        gate.redis = _FakeRedis(kv)

        os.environ["EDGE_EXEC_HEALTH_MODE"] = "veto"
        os.environ["EXEC_MAX_IS_P95_BPS"] = "10"
        os.environ["EXEC_MAX_PERM_IMPACT_P95_BPS"] = "10"
        os.environ["EXEC_MIN_REALIZED_SPREAD_P50_BPS"] = "-2"
        # NOT setting EDGE_EXEC_HEALTH_VETO_ON_ADVERSE

        ctx = _Ctx()
        d = gate.evaluate(ctx=ctx, kind="breakout", symbol=ctx.symbol)
        # IS_high only (perm ok) → no veto from exec_health
        # (gate may veto for other reasons, but not VETO_ADVERSE or VETO_IMPL_SHORTFALL)
        if d.veto:
            self.assertNotIn("VETO_IMPL_SHORTFALL", d.reason_code)
            self.assertNotIn("VETO_ADVERSE", d.reason_code)


class TestExecHealthFailOpen(unittest.TestCase):
    """Fail-open: no veto/tighten when redis is None or keys absent."""

    def setUp(self):
        _clear_env(
            "EDGE_EXEC_HEALTH_MODE", "EXEC_HEALTH_MODE",
            "EXEC_MAX_IS_P95_BPS", "EXEC_MAX_PERM_IMPACT_P95_BPS",
        )
        for k, v in _TS_SAFE_ENV.items():
            os.environ[k] = v

    def tearDown(self):
        _clear_env("EDGE_EXEC_HEALTH_MODE", "EXEC_MAX_IS_P95_BPS", *list(_TS_SAFE_ENV.keys()))

    def test_no_redis_no_veto(self):
        os.environ["EDGE_EXEC_HEALTH_MODE"] = "veto"
        os.environ["EXEC_MAX_IS_P95_BPS"] = "1"
        os.environ["EXEC_MAX_PERM_IMPACT_P95_BPS"] = "1"

        gate = _make_gate()
        gate.redis = None  # no Redis at all

        ctx = _Ctx()
        d = gate.evaluate(ctx=ctx, kind="breakout", symbol=ctx.symbol)
        # Must fail-open: reason must NOT be exec_health
        if d.veto:
            self.assertNotIn("VETO_IMPL_SHORTFALL", d.reason_code)

    def test_missing_keys_no_tighten(self):
        os.environ["EDGE_EXEC_HEALTH_MODE"] = "tighten"
        os.environ["EXEC_MAX_IS_P95_BPS"] = "1"

        gate = _make_gate()
        gate.redis = _FakeRedis({})  # empty Redis

        ctx = _Ctx()
        gate.evaluate(ctx=ctx, kind="breakout", symbol=ctx.symbol)
        add_bps = getattr(ctx, "exec_health_tighten_add_bps", 0.0)
        self.assertEqual(add_bps, 0.0, "Missing keys must not tighten slippage")


class TestParseCsvInts(unittest.TestCase):
    """Unit tests for _parse_csv_ints helper."""

    def test_normal(self):
        self.assertEqual(_parse_csv_ints("1,5,10", default=()), (1, 5, 10))

    def test_floats_truncated(self):
        self.assertEqual(_parse_csv_ints("1.0,5.9", default=()), (1, 5))

    def test_empty_falls_back(self):
        self.assertEqual(_parse_csv_ints("", default=(1, 5)), (1, 5))

    def test_bad_tokens_skipped(self):
        self.assertEqual(_parse_csv_ints("1,bad,5", default=()), (1, 5))


class TestTcaKeyCandidates(unittest.TestCase):
    """_tca_key_candidates fallback ordering."""

    def test_first_key_is_most_specific(self):
        keys = _tca_key_candidates(
            metric="is_p95_bps",
            symbol="BTCUSDT", venue="binance", session="asia", tf="1m", kind="breakout", side="long",
        )
        # First key must be most specific
        self.assertIn("BTCUSDT", keys[0])
        self.assertIn("asia", keys[0])
        self.assertIn("1m", keys[0])
        self.assertIn("breakout", keys[0])
        self.assertIn("long", keys[0])

    def test_last_key_is_all_dimensions(self):
        keys = _tca_key_candidates(
            metric="is_p95_bps",
            symbol="BTCUSDT", venue="binance", session="asia", tf="1m", kind="breakout", side="long",
        )
        # Last key should have all=all
        self.assertIn("all:all:all:all", keys[-1])

    def test_no_delta_in_is_p95(self):
        keys = _tca_key_candidates(
            metric="is_p95_bps",
            symbol="BTCUSDT", venue="binance", session="na", tf="all", kind="all", side="all",
        )
        for k in keys:
            # Is p95 keys should not contain an integer delta segment
            self.assertTrue(k.startswith("tca:is_p95_bps:"), f"Unexpected key: {k}")

    def test_delta_in_perm_keys(self):
        keys = _tca_key_candidates(
            metric="perm_impact_p95_bps",
            symbol="BTCUSDT", venue="binance", session="na", tf="all", kind="all", side="all",
            delta_sec=5,
        )
        for k in keys:
            self.assertIn(":5:", k, f"Delta should appear in key: {k}")


class TestRedisGetFloatBestEffort(unittest.TestCase):
    """_redis_get_float_best_effort edge cases."""

    def test_bytes_value(self):
        r = _FakeRedis({"foo": b"3.14"})
        self.assertAlmostEqual(_redis_get_float_best_effort(r, "foo"), 3.14)

    def test_missing_key(self):
        r = _FakeRedis({})
        self.assertIsNone(_redis_get_float_best_effort(r, "missing"))

    def test_none_redis(self):
        self.assertIsNone(_redis_get_float_best_effort(None, "key"))

    def test_nan_returns_none(self):
        r = _FakeRedis({"x": b"nan"})
        # NaN float → should return None (not finite)
        result = _redis_get_float_best_effort(r, "x")
        # "nan" string → float("nan") which is not finite → None
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
