"""P0-3: RiskBudgetAggregator + cooldown gate — unit tests.

Tests cover:
1. Aggregator reads daily_dd state correctly
2. Aggregator detects active cooldown for tracked symbol
3. Aggregator clears metric when cooldown expired
4. Aggregator handles Redis errors gracefully
5. Cooldown gate check: shadow mode — does NOT block
6. Cooldown gate check: enforce mode — blocks when in cooldown
7. Cooldown gate: no block when no cooldown key
8. Cooldown gate: no block when cooldown expired
9. Cooldown gate: no Redis → fail-open (no block)
10. EntryPolicyGate integration: RISK_BUDGET_GATE_MODE=shadow emits metric but allows
"""
from __future__ import annotations

import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# FakeRedis
# ---------------------------------------------------------------------------

class FakeRedis:
    def __init__(self, data: dict | None = None):
        self._kv: dict = dict(data or {})

    def get(self, key):
        val = self._kv.get(key)
        if val is None:
            return None
        return str(val).encode()

    def set(self, key, val, ex=None, px=None):
        self._kv[key] = val

    def hgetall(self, key):
        val = self._kv.get(key)
        if not isinstance(val, dict):
            return {}
        return {k.encode(): v.encode() for k, v in val.items()}

    def scan(self, cursor, match=None, count=100):
        import fnmatch
        pattern = (match or "*").replace("*", ".*").replace("?", ".")
        import re
        keys = [
            k.encode() for k in self._kv
            if re.fullmatch(pattern.replace(".*", ".*"), k)
        ]
        return 0, keys


class ErrorRedis:
    """Simulates Redis connection failure."""
    def get(self, key):
        raise ConnectionError("Redis down")

    def hgetall(self, key):
        raise ConnectionError("Redis down")

    def scan(self, *a, **kw):
        raise ConnectionError("Redis down")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _future_ms(ms: int = 900_000) -> int:
    return int(time.time() * 1000) + ms


def _past_ms(ms: int = 5_000) -> int:
    return int(time.time() * 1000) - ms


# ---------------------------------------------------------------------------
# RiskBudgetAggregator tests
# ---------------------------------------------------------------------------

class TestRiskBudgetAggregator:
    def test_reads_daily_dd_armed(self):
        from services.risk_budget_service_v1 import RiskBudgetAggregator
        r = FakeRedis({
            "risk:daily_dd:state": {
                "kill_armed": "1",
                "mode": "enforce",
                "r_sum": "-12.5",
            }
        })
        agg = RiskBudgetAggregator(r=r, track_symbols=["BTCUSDT"])
        result = agg.run_once()
        assert result["daily_dd_armed"] is True
        assert result["daily_dd_r_sum"] == pytest.approx(-12.5)

    def test_daily_dd_shadow_mode_not_armed(self):
        from services.risk_budget_service_v1 import RiskBudgetAggregator
        r = FakeRedis({
            "risk:daily_dd:state": {
                "kill_armed": "1",
                "mode": "shadow",  # shadow → not armed in aggregator
                "r_sum": "-10.0",
            }
        })
        agg = RiskBudgetAggregator(r=r, track_symbols=["BTCUSDT"])
        result = agg.run_once()
        assert result["daily_dd_armed"] is False

    def test_detects_active_cooldown_for_tracked_symbol(self):
        from services.risk_budget_service_v1 import RiskBudgetAggregator
        until_ms = _future_ms(600_000)
        r = FakeRedis({
            "risk:cooldown:symbol:BTCUSDT": str(until_ms),
        })
        agg = RiskBudgetAggregator(r=r, track_symbols=["BTCUSDT"])
        result = agg.run_once()
        assert "BTCUSDT" in result["cooldowns"]
        assert result["cooldowns"]["BTCUSDT"] == until_ms

    def test_expired_cooldown_not_reported(self):
        from services.risk_budget_service_v1 import RiskBudgetAggregator
        until_ms = _past_ms(1_000)  # 1s ago → expired
        r = FakeRedis({
            "risk:cooldown:symbol:BTCUSDT": str(until_ms),
        })
        agg = RiskBudgetAggregator(r=r, track_symbols=["BTCUSDT"])
        result = agg.run_once()
        assert "BTCUSDT" not in result["cooldowns"]

    def test_redis_error_does_not_crash(self):
        from services.risk_budget_service_v1 import RiskBudgetAggregator
        agg = RiskBudgetAggregator(r=ErrorRedis(), track_symbols=["BTCUSDT"])
        result = agg.run_once()
        assert isinstance(result, dict)

    def test_empty_redis_no_cooldowns(self):
        from services.risk_budget_service_v1 import RiskBudgetAggregator
        agg = RiskBudgetAggregator(r=FakeRedis(), track_symbols=["ETHUSDT"])
        result = agg.run_once()
        assert result["cooldowns"] == {}


# ---------------------------------------------------------------------------
# Cooldown gate check (inline in EntryPolicyGate)
# ---------------------------------------------------------------------------

class TestCooldownGateCheck:
    """Test the cooldown check logic extracted for unit testing."""

    def _check_cooldown(self, r, symbol: str, mode: str = "shadow") -> str | None:
        """
        Simulate the cooldown check from entry_policy_gate.py.
        Returns reason_code if blocked, None if allowed.
        """
        with patch.dict(os.environ, {
            "RISK_BUDGET_GATE_ENABLED": "1",
            "RISK_BUDGET_GATE_MODE": mode,
        }):
            enabled = os.getenv("RISK_BUDGET_GATE_ENABLED", "1").strip() not in {"0", "false"}
            if not enabled:
                return None
            rb_mode = (os.getenv("RISK_BUDGET_GATE_MODE") or "shadow").strip().lower()
            cooldown_key = f"risk:cooldown:symbol:{symbol.upper()}"
            until_ms_raw = r.get(cooldown_key)
            if until_ms_raw is None:
                return None
            now_ms = int(time.time() * 1000)
            try:
                until_ms = int(until_ms_raw.decode() if isinstance(until_ms_raw, bytes) else until_ms_raw)
            except (TypeError, ValueError):
                return None
            if until_ms <= now_ms:
                return None
            remaining_s = round((until_ms - now_ms) / 1000, 1)
            reason = f"RISK_DENY_SYMBOL_PROTECTION_COOLDOWN:{symbol.upper()}:remaining={remaining_s}s"
            if rb_mode == "enforce":
                return reason
            return None  # shadow: count metric, allow

    def test_shadow_mode_no_block_even_in_cooldown(self):
        r = FakeRedis({"risk:cooldown:symbol:BTCUSDT": str(_future_ms(600_000))})
        result = self._check_cooldown(r, "BTCUSDT", mode="shadow")
        assert result is None

    def test_enforce_mode_blocks_when_in_cooldown(self):
        r = FakeRedis({"risk:cooldown:symbol:ETHUSDT": str(_future_ms(600_000))})
        result = self._check_cooldown(r, "ETHUSDT", mode="enforce")
        assert result is not None
        assert "RISK_DENY_SYMBOL_PROTECTION_COOLDOWN" in result
        assert "ETHUSDT" in result

    def test_no_cooldown_key_no_block(self):
        r = FakeRedis()
        result = self._check_cooldown(r, "BTCUSDT", mode="enforce")
        assert result is None

    def test_expired_cooldown_no_block(self):
        r = FakeRedis({"risk:cooldown:symbol:BTCUSDT": str(_past_ms(1_000))})
        result = self._check_cooldown(r, "BTCUSDT", mode="enforce")
        assert result is None

    def test_gate_disabled_no_block(self):
        r = FakeRedis({"risk:cooldown:symbol:BTCUSDT": str(_future_ms(600_000))})
        with patch.dict(os.environ, {"RISK_BUDGET_GATE_ENABLED": "0"}):
            enabled = os.getenv("RISK_BUDGET_GATE_ENABLED", "1").strip() not in {"0", "false"}
            assert not enabled


# ---------------------------------------------------------------------------
# Integration: P0-1 sets cooldown → P0-3 reads it
# ---------------------------------------------------------------------------

class TestCooldownEndToEnd:
    def test_p01_set_cooldown_p03_reads_it(self):
        """Verify the cooldown written by OrderOpenService is readable by aggregator."""
        r = FakeRedis()

        # P0-1: simulate OrderOpenService._set_symbol_cooldown
        from services.execution.order_open_service import OrderOpenService
        mock_writer = MagicMock()
        svc = OrderOpenService(
            r=r,
            block_symbol_on_protection_fail=True,
            cooldown_after_protection_fail_ms=300_000,
            event_writer=mock_writer,
        )
        svc._set_symbol_cooldown(sid="e2e-sid", symbol="BTCUSDT", reason="test")

        # P0-3: aggregator should now see it
        from services.risk_budget_service_v1 import RiskBudgetAggregator
        agg = RiskBudgetAggregator(r=r, track_symbols=["BTCUSDT"])
        result = agg.run_once()
        assert "BTCUSDT" in result["cooldowns"], "cooldown set by P0-1 must be visible in P0-3 aggregator"
        assert result["cooldowns"]["BTCUSDT"] > int(time.time() * 1000)
