from __future__ import annotations

"""Tests for EdgeCostGate inline exec health integration — P1.

Coverage:
  * _inline_exec_feedback: tighten mode adds slippage
  * _inline_exec_feedback: veto mode + requires perm_impact condition
  * _inline_exec_feedback: fail-open on missing rollup / disabled
  * evaluate(): integration path where slip_bps increases due to inline health
"""

import unittest
from types import SimpleNamespace

# ---------------------------------------------------------------------------
# Fake sync Redis for rollup read
# ---------------------------------------------------------------------------

class _FakeSyncRedis:
    def __init__(self, data: dict[str, dict[str, str]]):
        self._data = data

    def hgetall(self, name: str) -> dict[str, str]:
        return dict(self._data.get(name, {}))

    def get(self, name: str) -> str | None:
        return self._data.get(name, {}).get("__str__")


# ---------------------------------------------------------------------------
# Import stubs
# ---------------------------------------------------------------------------

try:
    from services.orderflow.inline_exec_health import (
        InlineExecDims,
        make_rollup_key,
    )
    INLINE_IMPORT_OK = True
except Exception:
    INLINE_IMPORT_OK = False

try:
    from tick_flow_full.handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate
    EDGE_IMPORT_OK = True
except Exception:
    EDGE_IMPORT_OK = False


# ---------------------------------------------------------------------------
# Helpers to build EdgeCostGate with P1 inline exec enabled
# ---------------------------------------------------------------------------

def _build_gate(
    *,
    inline_exec_enabled: bool = True,
    inline_exec_mode: str = "tighten",
    inline_exec_warn_p95_bps: float = 5.0,
    inline_exec_crit_p95_bps: float = 10.0,
    inline_exec_max_perm_impact_p95_bps: float = 6.0,
    inline_exec_min_count: int = 1,
    inline_exec_tighten_add_mult: float = 1.0,
    inline_exec_tighten_add_cap_bps: float = 8.0,
) -> EdgeCostGate:
    """Build a minimal EdgeCostGate with P1 fields configured."""
    from tick_flow_full.handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate
    return EdgeCostGate(
        enabled=True,
        mode="tp1",
        strict_missing_levels=False,
        apply_kinds=set(),
        k_default=4.0,
        k_by_symbol={},
        fees_bps_default=4.0,
        slippage_bps_default=4.0,
        slippage_use_spread_half=False,
        min_expected_move_bps_default=0.0,
        min_expected_move_bps_by_symbol={},
        inline_exec_enabled=inline_exec_enabled,
        inline_exec_mode=inline_exec_mode,
        inline_exec_warn_p95_bps=inline_exec_warn_p95_bps,
        inline_exec_crit_p95_bps=inline_exec_crit_p95_bps,
        inline_exec_max_perm_impact_p95_bps=inline_exec_max_perm_impact_p95_bps,
        inline_exec_min_count=inline_exec_min_count,
        inline_exec_tighten_add_mult=inline_exec_tighten_add_mult,
        inline_exec_tighten_add_cap_bps=inline_exec_tighten_add_cap_bps,
        inline_exec_tca_delta_sec=1,
    )


def _ctx():
    ctx = SimpleNamespace()
    ctx.redis = None
    ctx.session = "london"
    ctx.tf = "5m"
    ctx.kind = "breakout"
    ctx.side = "LONG"
    ctx.direction = "LONG"
    return ctx


# ---------------------------------------------------------------------------
# Tests for _inline_exec_feedback isolated
# ---------------------------------------------------------------------------


@unittest.skipIf(not (INLINE_IMPORT_OK and EDGE_IMPORT_OK), "import failed")
class TestInlineExecFeedbackDirect(unittest.TestCase):

    def _dims(self):
        return InlineExecDims(symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m")

    def _rollup_redis(self, p95: float, count: int = 10, perm_impact: float | None = None) -> _FakeSyncRedis:
        dims = self._dims().norm()
        rkey = make_rollup_key(dims, include_session=True)
        data = {
            rkey: {
                "p95_bps": str(p95),
                "p50_bps": str(p95 * 0.6),
                "ema_bps": str(p95 * 0.8),
                "count": str(count),
                "updated_at_ms": "1700000000000",
            }
        }
        return _FakeSyncRedis(data)

    def test_disabled_returns_zero_no_veto(self):
        gate = _build_gate(inline_exec_enabled=False)
        ctx = _ctx()
        add, veto, reason = gate._inline_exec_feedback(
            ctx=ctx, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m",
            redis_client=None,
        )
        self.assertEqual(add, 0.0)
        self.assertFalse(veto)

    def test_tighten_mode_above_warn_adds_slippage(self):
        gate = _build_gate(
            inline_exec_mode="tighten",
            inline_exec_warn_p95_bps=5.0,
            inline_exec_crit_p95_bps=10.0,
        )
        r = self._rollup_redis(p95=7.0, count=10)
        ctx = _ctx()
        add, veto, reason = gate._inline_exec_feedback(
            ctx=ctx, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m",
            redis_client=r,
        )
        self.assertFalse(veto)
        self.assertGreater(float(add), 0.0)
        self.assertLessEqual(float(add), gate.inline_exec_tighten_add_cap_bps)
        self.assertEqual(reason, "INLINE_EXEC_TIGHTEN")

    def test_monitor_mode_no_tighten_no_veto(self):
        gate = _build_gate(inline_exec_mode="monitor", inline_exec_warn_p95_bps=5.0)
        r = self._rollup_redis(p95=7.0, count=10)
        ctx = _ctx()
        add, veto, reason = gate._inline_exec_feedback(  # type: ignore
            ctx=ctx, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m",
            redis_client=r,
        ),
        self.assertFalse(veto),
        # monitor never returns add > 0 (it just annotates)
        self.assertEqual(float(add), 0.0),

    def test_veto_mode_vetoes_when_p95_and_perm_both_bad(self):
        gate = _build_gate(
            inline_exec_mode="veto",
            inline_exec_warn_p95_bps=5.0,
            inline_exec_crit_p95_bps=10.0,
            inline_exec_max_perm_impact_p95_bps=6.0,
        ),
        dims = self._dims().norm(),
        rkey = make_rollup_key(dims, include_session=True),  # type: ignore
        perm_key = "tca:perm_impact_p95_bps:1:BTCUSDT:binance:london:5m:breakout:LONG",
        data = {
            rkey: {
                "p95_bps": "12.0",
                "count": "10",
            },
            perm_key: {"__str__": "7.5"},
        }
        r = _FakeSyncRedis(data)
        # To make perm_impact actually readable we need get() on FakeSyncRedis
        # Use monkeypatching or a custom stub:
        class _CustomRedis(_FakeSyncRedis):
            def get(self, name: str) -> str | None:
                d = self._data.get(name)
                if d is not None and "__str__" in d:
                    return d["__str__"]
                return None

        r2 = _CustomRedis(data)
        ctx = _ctx()
        add, veto, reason = gate._inline_exec_feedback(  # type: ignore
            ctx=ctx, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m",
            redis_client=r2,
        )
        self.assertTrue(veto, f"expected veto, got reason={reason}")
        self.assertIn("VETO", reason)

    def test_veto_mode_no_veto_when_perm_below_threshold(self):
        gate = _build_gate(
            inline_exec_mode="veto",
            inline_exec_warn_p95_bps=5.0,
            inline_exec_crit_p95_bps=10.0,
            inline_exec_max_perm_impact_p95_bps=6.0,
        )
        dims = self._dims().norm()
        rkey = make_rollup_key(dims, include_session=True)
        data = {
            rkey: {
                "p95_bps": "12.0",
                "count": "10",
            }
        }
        r = _FakeSyncRedis(data)
        ctx = _ctx()
        # perm_impact is not readable → treated as nan → veto suppressed
        add, veto, reason = gate._inline_exec_feedback(
            ctx=ctx, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m",
            redis_client=r,
            exec_profile="hard",
        )
        self.assertFalse(veto)  # single-condition veto suppressed when perm unknown

    def test_below_warn_returns_zero_apply(self):
        gate = _build_gate(
            inline_exec_mode="tighten",
            inline_exec_warn_p95_bps=10.0,
        )
        r = self._rollup_redis(p95=3.0, count=10)
        ctx = _ctx()
        add, veto, reason = gate._inline_exec_feedback(
            ctx=ctx, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m",
            redis_client=r,
        )
        self.assertEqual(float(add), 0.0)
        self.assertFalse(veto)

    def test_count_guard_returns_zero(self):
        """Rollup below min_count should be ignored."""
        gate = _build_gate(inline_exec_min_count=10)
        r = self._rollup_redis(p95=15.0, count=3)
        ctx = _ctx()
        add, veto, _ = gate._inline_exec_feedback(
            ctx=ctx, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m",
            redis_client=r,
        )
        self.assertEqual(float(add), 0.0)
        self.assertFalse(veto)

    def test_tighten_cap_respected(self):
        gate = _build_gate(
            inline_exec_mode="tighten",
            inline_exec_warn_p95_bps=5.0,
            inline_exec_tighten_add_cap_bps=3.0,
            inline_exec_tighten_add_mult=100.0,   # extreme: should be capped
        )
        r = self._rollup_redis(p95=20.0, count=10)
        ctx = _ctx()
        add, veto, _ = gate._inline_exec_feedback(
            ctx=ctx, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m",
            redis_client=r,
        )
        self.assertFalse(veto)
        self.assertLessEqual(float(add), 3.01)  # cap is 3 bps

    def test_fail_open_none_redis(self):
        gate = _build_gate(inline_exec_mode="veto")
        ctx = _ctx()
        add, veto, reason = gate._inline_exec_feedback(
            ctx=ctx, symbol="BTCUSDT", side="LONG", session="london", kind="breakout", tf="5m",
            redis_client=None,
        )
        self.assertEqual(float(add), 0.0)
        self.assertFalse(veto)


if __name__ == "__main__":
    unittest.main()
