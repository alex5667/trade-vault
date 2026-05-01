from utils.time_utils import get_ny_time_millis
"""
G13 · Execution Gate — Comprehensive Unit Tests (v2)

Tests PASS-THROUGH mode, ENFORCE mode with dual-buffer (F3 fix),
matching logic, TTL cleanup, safeguards, edge cases, output contract,
and config parsing.
"""

import json
import time
from unittest.mock import AsyncMock, patch

import pytest

# Import the service once — metrics register into the default registry.
import services.execution_gate_service as gate_mod
from services.execution_gate_service import (
    ExecutionGateService,
    Proposal,
    Confirmation,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_proposal_fields(
    symbol: str = "BTCUSDT",
    direction: str = "long",
    is_virtual: int = 0,
    generated_at: int = None,
    extra: dict = None,
) -> dict:
    payload = {
        "symbol": symbol,
        "direction": direction,
        "is_virtual": is_virtual,
        "generated_at": generated_at or get_ny_time_millis(),
        "sid": "test-signal-001",
        "entry": 64500.0,
        "sl": 64000.0,
        "tp_levels": [65000.0, 65500.0],
        "qty": 1.0,
    },
    if extra:
        payload.update(extra)
    return {"payload": json.dumps(payload)}


def _make_confirm_fields(
    symbol: str = "BTCUSDT",
    direction: str = "long",
    ok: int = 1,
    score: float = 0.82,
    ts_ms: int = None,
) -> dict:
    payload = {
        "symbol": symbol,
        "direction": direction,
        "ok": ok,
        "score": score,
        "ts_ms": ts_ms or get_ny_time_millis(),
        "reason": "delta_z_confirmed",
    },
    return {"payload": json.dumps(payload)}


def _build_service(
    require_of_confirm: bool = False,
    ttl_s: float = 5.0,
    match_ms: int = 2000,
) -> ExecutionGateService:
    with patch.dict(
        "os.environ",
        {
            "EXEC_GATE_REQUIRE_OF_CONFIRM": "true" if require_of_confirm else "false",
            "EXEC_GATE_TTL_S": str(ttl_s),
            "EXEC_GATE_MATCH_MS": str(match_ms),
            "REDIS_URL": "redis://localhost:6379/0",
        },
    ):
        svc = ExecutionGateService()

    svc.redis = AsyncMock()
    svc.redis.rpush = AsyncMock(return_value=1)
    svc.redis.xread = AsyncMock(return_value=[])
    return svc


# ===================================================================
#  T1: PASS-THROUGH MODE (default)
# ===================================================================

class TestPassThroughMode:
    """PASS-THROUGH: require_of_confirm=false → immediate publish."""

    @pytest.mark.asyncio
    async def test_proposal_immediately_published(self):
        svc = _build_service(require_of_confirm=False)
        fields = _make_proposal_fields(symbol="BTCUSDT", direction="long")

        await svc._handle_proposal(fields)

        svc.redis.rpush.assert_called_once()
        queue_name = svc.redis.rpush.call_args[0][0]
        payload = json.loads(svc.redis.rpush.call_args[0][1])

        assert queue_name == "orders:queue:binance"
        assert payload["gate_verified"] is True
        assert payload["validation_status"] == "bypassed"
        assert payload["confirm_score"] == 1.0
        assert payload["symbol"] == "BTCUSDT"

    @pytest.mark.asyncio
    async def test_proposal_not_buffered_in_passthrough(self):
        svc = _build_service(require_of_confirm=False)
        fields = _make_proposal_fields(symbol="ETHUSDT")

        await svc._handle_proposal(fields)

        buffered = svc.proposals.get("ETHUSDT", [])
        assert len(buffered) == 0

    @pytest.mark.asyncio
    async def test_virtual_proposal_not_published_in_passthrough(self):
        svc = _build_service(require_of_confirm=False)
        fields = _make_proposal_fields(symbol="SOLUSDT", is_virtual=1)

        await svc._handle_proposal(fields)

        # Virtual orders are tracked by TradeMonitor directly, not passed to Binance queue
        svc.redis.rpush.assert_not_called()


# ===================================================================
#  T2: ENFORCE MODE — classical flow (proposal first)
# ===================================================================

class TestEnforceMode:
    """ENFORCE: require_of_confirm=true → buffer + match."""

    @pytest.mark.asyncio
    async def test_proposal_buffered_not_immediately_published(self):
        svc = _build_service(require_of_confirm=True)
        fields = _make_proposal_fields(symbol="BTCUSDT")

        await svc._handle_proposal(fields)

        svc.redis.rpush.assert_not_called()
        assert "BTCUSDT" in svc.proposals
        assert len(svc.proposals["BTCUSDT"]) == 1

    @pytest.mark.asyncio
    async def test_matching_confirmation_triggers_publish(self):
        svc = _build_service(require_of_confirm=True, match_ms=5000)
        now_ms = get_ny_time_millis()

        prop_fields = _make_proposal_fields(
            symbol="ETHUSDT", direction="long", generated_at=now_ms
        )
        await svc._handle_proposal(prop_fields)
        assert len(svc.proposals.get("ETHUSDT", [])) == 1

        confirm_fields = _make_confirm_fields(
            symbol="ETHUSDT", direction="long", ok=1, score=0.85, ts_ms=now_ms + 500
        )
        await svc._handle_confirmation(confirm_fields)

        svc.redis.rpush.assert_called_once()
        payload = json.loads(svc.redis.rpush.call_args[0][1])
        assert payload["validation_status"] == "passed"
        assert payload["gate_verified"] is True
        assert len(svc.proposals.get("ETHUSDT", [])) == 0

    @pytest.mark.asyncio
    async def test_confirmation_wrong_direction_no_match(self):
        svc = _build_service(require_of_confirm=True, match_ms=5000)
        now_ms = get_ny_time_millis()

        prop_fields = _make_proposal_fields(
            symbol="BTCUSDT", direction="long", generated_at=now_ms
        )
        await svc._handle_proposal(prop_fields)

        confirm_fields = _make_confirm_fields(
            symbol="BTCUSDT", direction="short", ok=1, ts_ms=now_ms + 100
        )
        await svc._handle_confirmation(confirm_fields)

        svc.redis.rpush.assert_not_called()
        assert len(svc.proposals["BTCUSDT"]) == 1
        # Confirmation should be buffered instead
        assert len(svc.confirmations.get("BTCUSDT", [])) == 1

    @pytest.mark.asyncio
    async def test_confirmation_outside_tolerance_no_match(self):
        svc = _build_service(require_of_confirm=True, match_ms=2000)
        now_ms = get_ny_time_millis()

        prop_fields = _make_proposal_fields(
            symbol="BTCUSDT", direction="long", generated_at=now_ms
        )
        await svc._handle_proposal(prop_fields)

        confirm_fields = _make_confirm_fields(
            symbol="BTCUSDT", direction="long", ok=1, ts_ms=now_ms + 5000
        )
        await svc._handle_confirmation(confirm_fields)

        svc.redis.rpush.assert_not_called()
        assert len(svc.proposals["BTCUSDT"]) == 1
        # Confirmation buffered (orphan)
        assert len(svc.confirmations.get("BTCUSDT", [])) == 1

    @pytest.mark.asyncio
    async def test_confirmation_wrong_symbol_no_match(self):
        svc = _build_service(require_of_confirm=True, match_ms=5000)
        now_ms = get_ny_time_millis()

        prop_fields = _make_proposal_fields(
            symbol="BTCUSDT", direction="long", generated_at=now_ms
        )
        await svc._handle_proposal(prop_fields)

        confirm_fields = _make_confirm_fields(
            symbol="ETHUSDT", direction="long", ok=1, ts_ms=now_ms
        )
        await svc._handle_confirmation(confirm_fields)

        svc.redis.rpush.assert_not_called()
        assert len(svc.proposals["BTCUSDT"]) == 1
        # Confirmation buffered under ETHUSDT
        assert len(svc.confirmations.get("ETHUSDT", [])) == 1


# ===================================================================
#  T3: SAFEGUARD — ok=0 must block
# ===================================================================

class TestSafeguard:
    """Validation safeguard: ok=0 → skip regardless of virtual flag."""

    @pytest.mark.asyncio
    async def test_ok0_real_order_blocked(self):
        svc = _build_service(require_of_confirm=True, match_ms=5000)
        now_ms = get_ny_time_millis()

        prop_fields = _make_proposal_fields(
            symbol="BTCUSDT", direction="long", generated_at=now_ms, is_virtual=0
        )
        await svc._handle_proposal(prop_fields)

        confirm_fields = _make_confirm_fields(
            symbol="BTCUSDT", direction="long", ok=0, ts_ms=now_ms + 100
        )
        await svc._handle_confirmation(confirm_fields)

        svc.redis.rpush.assert_not_called()

    @pytest.mark.asyncio
    async def test_ok0_virtual_order_also_blocked(self):
        """F2 fix: virtual orders with ok=0 must also be blocked."""
        svc = _build_service(require_of_confirm=True, match_ms=5000)
        now_ms = get_ny_time_millis()

        prop_fields = _make_proposal_fields(
            symbol="BTCUSDT", direction="short", generated_at=now_ms, is_virtual=1
        )
        await svc._handle_proposal(prop_fields)

        confirm_fields = _make_confirm_fields(
            symbol="BTCUSDT", direction="short", ok=0, ts_ms=now_ms + 100
        )
        await svc._handle_confirmation(confirm_fields)

        svc.redis.rpush.assert_not_called()

    @pytest.mark.asyncio
    async def test_ok1_virtual_order_not_published_to_executor(self):
        """Virtual orders with ok=1 should pass normally but NOT be published to the executor queue."""
        svc = _build_service(require_of_confirm=True, match_ms=5000)
        now_ms = get_ny_time_millis()

        prop_fields = _make_proposal_fields(
            symbol="BTCUSDT", direction="long", generated_at=now_ms, is_virtual=1
        )
        await svc._handle_proposal(prop_fields)

        confirm_fields = _make_confirm_fields(
            symbol="BTCUSDT", direction="long", ok=1, ts_ms=now_ms + 100
        )
        await svc._handle_confirmation(confirm_fields)

        # Virtual orders are tracked by TradeMonitor directly, not passed to Binance queue
        svc.redis.rpush.assert_not_called()


# ===================================================================
#  T4: CLEANUP / TTL
# ===================================================================

class TestCleanup:
    """TTL cleanup prunes stale proposals AND confirmations."""

    @pytest.mark.asyncio
    async def test_expired_proposals_pruned(self):
        svc = _build_service(require_of_confirm=True, ttl_s=0.1)

        fields = _make_proposal_fields(symbol="BTCUSDT")
        await svc._handle_proposal(fields)
        assert len(svc.proposals["BTCUSDT"]) == 1

        svc.proposals["BTCUSDT"][0].received_at = time.time() - 1.0

        now = time.time()
        for sym in list(svc.proposals.keys()):
            fresh = [
                p for p in svc.proposals[sym]
                if (now - p.received_at) < svc.proposal_ttl_s
            ]
            if not fresh:
                del svc.proposals[sym]
            else:
                svc.proposals[sym] = fresh

        assert "BTCUSDT" not in svc.proposals

    @pytest.mark.asyncio
    async def test_fresh_proposals_kept(self):
        svc = _build_service(require_of_confirm=True, ttl_s=60.0)

        fields = _make_proposal_fields(symbol="ETHUSDT")
        await svc._handle_proposal(fields)

        now = time.time()
        for sym in list(svc.proposals.keys()):
            fresh = [
                p for p in svc.proposals[sym]
                if (now - p.received_at) < svc.proposal_ttl_s
            ]
            if not fresh:
                del svc.proposals[sym]
            else:
                svc.proposals[sym] = fresh

        assert "ETHUSDT" in svc.proposals
        assert len(svc.proposals["ETHUSDT"]) == 1

    @pytest.mark.asyncio
    async def test_expired_confirmations_pruned(self):
        """Buffered confirmations must also expire by TTL."""
        svc = _build_service(require_of_confirm=True, ttl_s=0.1)

        confirm_fields = _make_confirm_fields(symbol="BTCUSDT", direction="long")
        await svc._handle_confirmation(confirm_fields)
        assert len(svc.confirmations.get("BTCUSDT", [])) == 1

        svc.confirmations["BTCUSDT"][0].received_at = time.time() - 1.0

        now = time.time()
        for sym in list(svc.confirmations.keys()):
            fresh = [
                c for c in svc.confirmations[sym]
                if (now - c.received_at) < svc.proposal_ttl_s
            ]
            if not fresh:
                del svc.confirmations[sym]
            else:
                svc.confirmations[sym] = fresh

        assert "BTCUSDT" not in svc.confirmations


# ===================================================================
#  T5: EDGE CASES
# ===================================================================

class TestEdgeCases:
    """Edge cases and malformed inputs."""

    @pytest.mark.asyncio
    async def test_empty_payload_ignored(self):
        svc = _build_service()
        await svc._handle_proposal({})
        svc.redis.rpush.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_symbol_ignored(self):
        svc = _build_service()
        fields = {"payload": json.dumps({"direction": "long"})}
        await svc._handle_proposal(fields)
        svc.redis.rpush.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_direction_ignored(self):
        svc = _build_service()
        fields = {"payload": json.dumps({"symbol": "BTCUSDT", "direction": "sideways"})}
        await svc._handle_proposal(fields)
        svc.redis.rpush.assert_not_called()

    @pytest.mark.asyncio
    async def test_malformed_json_ignored(self):
        svc = _build_service()
        fields = {"payload": "not-json{{{"}
        await svc._handle_proposal(fields)
        svc.redis.rpush.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_proposals_same_symbol(self):
        svc = _build_service(require_of_confirm=True)

        for _ in range(3):
            fields = _make_proposal_fields(symbol="BTCUSDT", direction="long")
            await svc._handle_proposal(fields)

        assert len(svc.proposals["BTCUSDT"]) == 3

    @pytest.mark.asyncio
    async def test_confirmation_no_pending_proposals(self):
        """Confirm arrives with no proposals — should be buffered, not crash."""
        svc = _build_service(require_of_confirm=True)
        confirm = _make_confirm_fields(symbol="BTCUSDT", direction="long", ok=1)
        await svc._handle_confirmation(confirm)
        svc.redis.rpush.assert_not_called()
        # Should be buffered
        assert len(svc.confirmations.get("BTCUSDT", [])) == 1


# ===================================================================
#  T6: OUTPUT PAYLOAD CONTRACT
# ===================================================================

class TestOutputContract:
    """Verify the output payload has all required fields."""

    @pytest.mark.asyncio
    async def test_output_has_all_gate_fields(self):
        svc = _build_service(require_of_confirm=False)
        fields = _make_proposal_fields(symbol="SOLUSDT", direction="short")

        await svc._handle_proposal(fields)

        payload = json.loads(svc.redis.rpush.call_args[0][1])

        assert "gate_verified" in payload
        assert "gate_ts_ms" in payload
        assert "confirm_score" in payload
        assert "validation_status" in payload
        assert "validation_reason" in payload
        assert "symbol" in payload
        assert "direction" in payload
        assert "sid" in payload
        assert "entry" in payload
        assert "sl" in payload
        assert "tp_levels" in payload

    @pytest.mark.asyncio
    async def test_gate_ts_is_recent(self):
        svc = _build_service(require_of_confirm=False)
        before_ms = get_ny_time_millis()
        fields = _make_proposal_fields(symbol="BTCUSDT", direction="long")

        await svc._handle_proposal(fields)

        payload = json.loads(svc.redis.rpush.call_args[0][1])
        after_ms = get_ny_time_millis()

        assert before_ms <= payload["gate_ts_ms"] <= after_ms + 100

    @pytest.mark.asyncio
    async def test_output_goes_to_binance_queue(self):
        """F4: output must go to orders:queue:binance, NOT orders:queue:mt5."""
        svc = _build_service(require_of_confirm=False)
        fields = _make_proposal_fields(symbol="BTCUSDT", direction="long")

        await svc._handle_proposal(fields)

        queue_name = svc.redis.rpush.call_args[0][0]
        assert queue_name == "orders:queue:binance"
        assert "mt5" not in queue_name


# ===================================================================
#  T7: REDIS LOADING RESILIENCE
# ===================================================================

class TestRedisLoading:
    """BusyLoadingError handling."""

    def test_is_redis_loading_true(self):
        exc = Exception("LOADING Redis is loading the dataset in memory")
        assert ExecutionGateService._is_redis_loading(exc) is True

    def test_is_redis_loading_false(self):
        exc = Exception("Connection refused")
        assert ExecutionGateService._is_redis_loading(exc) is False


# ===================================================================
#  T8: CONFIG PARSING
# ===================================================================

class TestConfigParsing:
    """Environment variable parsing."""

    def test_require_of_confirm_true_variants(self):
        for val in ("1", "true", "True", "yes", "on"):
            with patch.dict("os.environ", {"EXEC_GATE_REQUIRE_OF_CONFIRM": val}):
                svc = ExecutionGateService()
                assert svc.require_of_confirm is True, f"Failed for value: {val}"

    def test_require_of_confirm_false_variants(self):
        for val in ("0", "false", "no", "off", "random"):
            with patch.dict("os.environ", {"EXEC_GATE_REQUIRE_OF_CONFIRM": val}):
                svc = ExecutionGateService()
                assert svc.require_of_confirm is False, f"Failed for value: {val}"

    def test_ttl_float_parsing(self):
        with patch.dict("os.environ", {"EXEC_GATE_TTL_S": "3.5"}):
            svc = ExecutionGateService()
            assert svc.proposal_ttl_s == 3.5

    def test_match_ms_int_parsing(self):
        with patch.dict("os.environ", {"EXEC_GATE_MATCH_MS": "1500"}):
            svc = ExecutionGateService()
            assert svc.match_tolerance_ms == 1500


# ===================================================================
#  T9: DUAL-BUFFER — F3 FIX (confirm arrives before proposal)
# ===================================================================

class TestDualBuffer:
    """F3 fix: confirmation arrives BEFORE proposal — must still match."""

    @pytest.mark.asyncio
    async def test_confirm_first_then_proposal_matches(self):
        """Core F3 test: confirm → proposal → auto-match → publish."""
        svc = _build_service(require_of_confirm=True, match_ms=5000)
        now_ms = get_ny_time_millis()

        # 1. Confirmation arrives first (orphan)
        confirm_fields = _make_confirm_fields(
            symbol="BTCUSDT", direction="long", ok=1, score=0.9, ts_ms=now_ms
        )
        await svc._handle_confirmation(confirm_fields)

        # Should be buffered
        svc.redis.rpush.assert_not_called()
        assert len(svc.confirmations.get("BTCUSDT", [])) == 1

        # 2. Proposal arrives — should auto-match with buffered confirmation
        prop_fields = _make_proposal_fields(
            symbol="BTCUSDT", direction="long", generated_at=now_ms + 200
        )
        await svc._handle_proposal(prop_fields)

        # Now it should have matched and published
        svc.redis.rpush.assert_called_once()
        payload = json.loads(svc.redis.rpush.call_args[0][1])
        assert payload["validation_status"] == "passed"
        assert payload["gate_verified"] is True

        # Both buffers should be empty
        assert len(svc.proposals.get("BTCUSDT", [])) == 0
        assert len(svc.confirmations.get("BTCUSDT", [])) == 0

    @pytest.mark.asyncio
    async def test_confirm_first_wrong_direction_stays_buffered(self):
        """Confirm arrives first but with wrong direction — stays in buffer."""
        svc = _build_service(require_of_confirm=True, match_ms=5000)
        now_ms = get_ny_time_millis()

        confirm_fields = _make_confirm_fields(
            symbol="BTCUSDT", direction="short", ok=1, ts_ms=now_ms
        )
        await svc._handle_confirmation(confirm_fields)

        prop_fields = _make_proposal_fields(
            symbol="BTCUSDT", direction="long", generated_at=now_ms + 100
        )
        await svc._handle_proposal(prop_fields)

        # No match — different directions
        svc.redis.rpush.assert_not_called()
        # Proposal buffered waiting for its own confirmation
        assert len(svc.proposals.get("BTCUSDT", [])) == 1
        # Confirm still buffered
        assert len(svc.confirmations.get("BTCUSDT", [])) == 1

    @pytest.mark.asyncio
    async def test_confirm_first_outside_tolerance_no_match(self):
        """Confirm arrives first but too far in time — no match."""
        svc = _build_service(require_of_confirm=True, match_ms=2000)
        now_ms = get_ny_time_millis()

        confirm_fields = _make_confirm_fields(
            symbol="ETHUSDT", direction="long", ok=1, ts_ms=now_ms
        )
        await svc._handle_confirmation(confirm_fields)

        prop_fields = _make_proposal_fields(
            symbol="ETHUSDT", direction="long", generated_at=now_ms + 5000
        )
        await svc._handle_proposal(prop_fields)

        svc.redis.rpush.assert_not_called()
        assert len(svc.proposals.get("ETHUSDT", [])) == 1
        assert len(svc.confirmations.get("ETHUSDT", [])) == 1

    @pytest.mark.asyncio
    async def test_confirm_first_ok0_blocks_execution(self):
        """Confirm arrives first with ok=0 — proposal should NOT execute."""
        svc = _build_service(require_of_confirm=True, match_ms=5000)
        now_ms = get_ny_time_millis()

        confirm_fields = _make_confirm_fields(
            symbol="SOLUSDT", direction="short", ok=0, ts_ms=now_ms
        )
        await svc._handle_confirmation(confirm_fields)

        prop_fields = _make_proposal_fields(
            symbol="SOLUSDT", direction="short", generated_at=now_ms + 100
        )
        await svc._handle_proposal(prop_fields)

        # Matched but ok=0 → SAFEGUARD blocks
        svc.redis.rpush.assert_not_called()
        # Both consumed from buffers (matched, just not published)
        assert len(svc.proposals.get("SOLUSDT", [])) == 0
        assert len(svc.confirmations.get("SOLUSDT", [])) == 0

    @pytest.mark.asyncio
    async def test_multiple_confirms_buffered(self):
        """Multiple orphan confirms stack in buffer, first match wins."""
        svc = _build_service(require_of_confirm=True, match_ms=5000)
        now_ms = get_ny_time_millis()

        # Buffer 2 confirmations for same symbol/direction
        for offset in (0, 200):
            confirm_fields = _make_confirm_fields(
                symbol="BTCUSDT", direction="long", ok=1, ts_ms=now_ms + offset
            )
            await svc._handle_confirmation(confirm_fields)

        assert len(svc.confirmations.get("BTCUSDT", [])) == 2

        # Proposal matches the first one
        prop_fields = _make_proposal_fields(
            symbol="BTCUSDT", direction="long", generated_at=now_ms + 100
        )
        await svc._handle_proposal(prop_fields)

        svc.redis.rpush.assert_called_once()
        # One confirm consumed, one still buffered
        assert len(svc.confirmations.get("BTCUSDT", [])) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-x"])
