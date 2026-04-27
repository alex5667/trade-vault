"""
Unit tests for Phase 3.6 ATR Policy Guard Rails.

Tests:
  - evaluate_guardrails (APPROVE, REVOKE, REJECT, unknown)
  - Cooldown active → BLOCK
  - confirm tokens (issue / consume-once / expiry / actor mismatch)
  - arm_cooldown side-effects
"""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

# ── helpers ───────────────────────────────────────────────────────────────────


def _make_obj(
    *,
    stop_n: int = 100,
    trail_n: int = 100,
    pnl_stop: float = 2.0,
    pnl_trail: float = 2.0,
    source: str = "src",
    symbol: str = "BTCUSDT",
    scenario: str = "s1",
    regime: str = "bull",
    bucket: str = "2h",
) -> dict:
    return {
        "source": source,
        "symbol": symbol,
        "scenario": scenario,
        "regime": regime,
        "risk_horizon_bucket": bucket,
        "evidence": {
            "stop_ttl": {
                "n_canary": stop_n,
                "n_control": stop_n,
                "pnl_canary": pnl_stop + 1.0,
                "pnl_control": 1.0,
            },
            "trailing": {
                "n_canary": trail_n,
                "n_control": trail_n,
                "pnl_canary": pnl_trail + 1.0,
                "pnl_control": 1.0,
            },
        },
    }


# ── mock Redis factory ────────────────────────────────────────────────────────


def _fake_redis(
    cooldown_raw: str | None = None,
    flip_count: int = 0,
) -> MagicMock:
    r = MagicMock()
    r.get.return_value = cooldown_raw
    r.incr.return_value = flip_count + 1
    r.expire.return_value = True
    r.set.return_value = True
    r.delete.return_value = 1

    def _get_side(key: str):
        if "cooldown" in key:
            return cooldown_raw
        if "flip_count" in key:
            return str(flip_count) if flip_count else None
        if "confirm" in key:
            return cooldown_raw  # reuse for confirm tests
        return None

    r.get.side_effect = _get_side
    return r


# ═════════════════════════════════════════════════════════════════════════════
# evaluate_guardrails — APPROVE path
# ═════════════════════════════════════════════════════════════════════════════


class TestEvaluateGuardrailsApprove:
    @patch("services.atr_policy_guardrails._redis")
    def test_approve_safe_good_evidence(self, mk_redis):
        mk_redis.return_value = _fake_redis()
        from services.atr_policy_guardrails import evaluate_guardrails

        result = evaluate_guardrails(obj=_make_obj(stop_n=100, trail_n=100, pnl_stop=2.0, pnl_trail=2.0), action="APPROVE", is_active=False)
        assert result["risk_class"] == "SAFE"
        assert result["require_confirm"] is False
        assert result["reason_code"] == "ATR_POLICY_APPROVE_SAFE"

    @patch("services.atr_policy_guardrails._redis")
    def test_approve_hard_block_stop_n_too_low(self, mk_redis):
        mk_redis.return_value = _fake_redis()
        from services.atr_policy_guardrails import evaluate_guardrails

        result = evaluate_guardrails(obj=_make_obj(stop_n=5), action="APPROVE", is_active=False)
        assert result["risk_class"] == "BLOCK"
        assert result["reason_code"] == "ATR_POLICY_SAMPLE_TOO_LOW_BLOCK"
        assert result["require_confirm"] is False

    @patch("services.atr_policy_guardrails._redis")
    def test_approve_hard_block_trail_n_too_low(self, mk_redis):
        mk_redis.return_value = _fake_redis()
        from services.atr_policy_guardrails import evaluate_guardrails

        result = evaluate_guardrails(obj=_make_obj(trail_n=5), action="APPROVE", is_active=False)
        assert result["risk_class"] == "BLOCK"
        assert result["reason_code"] == "ATR_POLICY_TRAIL_SAMPLE_TOO_LOW_BLOCK"

    @patch("services.atr_policy_guardrails._hard_min_n", return_value=20)
    @patch("services.atr_policy_guardrails._soft_min_n", return_value=50)
    @patch("services.atr_policy_guardrails._redis")
    def test_approve_warn_low_sample(self, mk_redis, _smin, _hmin):
        mk_redis.return_value = _fake_redis()
        from services.atr_policy_guardrails import evaluate_guardrails

        result = evaluate_guardrails(obj=_make_obj(stop_n=30), action="APPROVE", is_active=False)
        assert result["risk_class"] == "WARN"
        assert result["require_confirm"] is True
        assert result["reason_code"] == "ATR_POLICY_LOW_SAMPLE_WARN"

    @patch("services.atr_policy_guardrails._marginal_pnl_bps", return_value=0.5)
    @patch("services.atr_policy_guardrails._redis")
    def test_approve_warn_marginal_edge(self, mk_redis, _bps):
        mk_redis.return_value = _fake_redis()
        from services.atr_policy_guardrails import evaluate_guardrails

        obj = _make_obj(stop_n=100, trail_n=100)
        # Override evidence to make pnl_delta tiny
        obj["evidence"]["stop_ttl"]["pnl_canary"] = 1.0
        obj["evidence"]["stop_ttl"]["pnl_control"] = 1.0
        obj["evidence"]["trailing"]["pnl_canary"] = 1.0
        obj["evidence"]["trailing"]["pnl_control"] = 1.0
        result = evaluate_guardrails(obj=obj, action="APPROVE", is_active=False)
        assert result["risk_class"] == "WARN"
        assert result["reason_code"] == "ATR_POLICY_MARGINAL_EDGE_WARN"
        assert result["require_confirm"] is True


# ═════════════════════════════════════════════════════════════════════════════
# evaluate_guardrails — REVOKE path
# ═════════════════════════════════════════════════════════════════════════════


class TestEvaluateGuardrailsRevoke:
    @patch("services.atr_policy_guardrails._redis")
    def test_revoke_active_requires_warn_confirm(self, mk_redis):
        mk_redis.return_value = _fake_redis(flip_count=0)
        from services.atr_policy_guardrails import evaluate_guardrails

        result = evaluate_guardrails(obj=_make_obj(), action="REVOKE", is_active=True)
        assert result["risk_class"] == "WARN"
        assert result["require_confirm"] is True
        assert result["reason_code"] == "ATR_POLICY_REVOKE_CONFIRM_REQUIRED"

    @patch("services.atr_policy_guardrails._max_flips_per_day", return_value=3)
    @patch("services.atr_policy_guardrails._redis")
    def test_revoke_flip_limit_block(self, mk_redis, _max):
        mk_redis.return_value = _fake_redis(flip_count=3)
        from services.atr_policy_guardrails import evaluate_guardrails

        result = evaluate_guardrails(obj=_make_obj(), action="REVOKE", is_active=True)
        assert result["risk_class"] == "BLOCK"
        assert result["reason_code"] == "ATR_POLICY_FLIP_LIMIT_BLOCK"
        assert result["require_confirm"] is False

    @patch("services.atr_policy_guardrails._redis")
    def test_revoke_not_active_is_safe(self, mk_redis):
        mk_redis.return_value = _fake_redis()
        from services.atr_policy_guardrails import evaluate_guardrails

        result = evaluate_guardrails(obj=_make_obj(), action="REVOKE", is_active=False)
        assert result["risk_class"] == "SAFE"
        assert result["reason_code"] == "ATR_POLICY_REVOKE_SAFE"


# ═════════════════════════════════════════════════════════════════════════════
# evaluate_guardrails — cooldown active
# ═════════════════════════════════════════════════════════════════════════════


class TestEvaluateGuardrailsCooldown:
    @patch("services.atr_policy_guardrails._redis")
    def test_cooldown_blocks_any_action(self, mk_redis):
        until_ts = int(time.time()) + 900
        cd_payload = json.dumps({"until_ts": until_ts, "actor": "alice", "action": "APPROVE", "ts": int(time.time())})
        mk_redis.return_value = _fake_redis(cooldown_raw=cd_payload)
        from services.atr_policy_guardrails import evaluate_guardrails

        for action in ("APPROVE", "REVOKE", "REJECT"):
            result = evaluate_guardrails(obj=_make_obj(), action=action, is_active=True)
            assert result["risk_class"] == "BLOCK", action
            assert result["reason_code"] == "ATR_POLICY_COOLDOWN_ACTIVE", action


# ═════════════════════════════════════════════════════════════════════════════
# evaluate_guardrails — REJECT / unknown
# ═════════════════════════════════════════════════════════════════════════════


class TestEvaluateGuardrailsMisc:
    @patch("services.atr_policy_guardrails._redis")
    def test_reject_always_safe(self, mk_redis):
        mk_redis.return_value = _fake_redis()
        from services.atr_policy_guardrails import evaluate_guardrails

        result = evaluate_guardrails(obj=_make_obj(), action="REJECT", is_active=False)
        assert result["risk_class"] == "SAFE"

    @patch("services.atr_policy_guardrails._redis")
    def test_unknown_action_blocks(self, mk_redis):
        mk_redis.return_value = _fake_redis()
        from services.atr_policy_guardrails import evaluate_guardrails

        result = evaluate_guardrails(obj=_make_obj(), action="FOOBAR", is_active=False)
        assert result["risk_class"] == "BLOCK"
        assert result["reason_code"] == "ATR_POLICY_UNKNOWN_ACTION"


# ═════════════════════════════════════════════════════════════════════════════
# confirm tokens
# ═════════════════════════════════════════════════════════════════════════════


class TestConfirmTokens:
    def _make_redis_store(self) -> dict:
        """In-memory Redis sim."""
        store: dict = {}

        def _get(key: str):
            entry = store.get(key)
            if entry is None:
                return None
            val, exp = entry
            if exp and time.time() > exp:
                del store[key]
                return None
            return val

        def _set(key: str, val: str, ex: int | None = None):
            store[key] = (val, time.time() + ex if ex else None)
            return True

        def _delete(key: str):
            store.pop(key, None)
            return 1

        r = MagicMock()
        r.get.side_effect = _get
        r.set.side_effect = _set
        r.delete.side_effect = _delete
        return r

    @patch("services.atr_policy_confirm_tokens._redis")
    def test_issue_and_consume_once(self, mk_redis):
        mk_redis.return_value = self._make_redis_store()
        from services.atr_policy_confirm_tokens import issue_confirm_token, consume_confirm_token

        token = issue_confirm_token(actor="alice", action="APPROVE", target="pid123", payload={"x": 1})
        assert len(token) == 16

        # First consume succeeds
        result = consume_confirm_token(token)
        assert result["actor"] == "alice"
        assert result["action"] == "APPROVE"
        assert result["payload"] == {"x": 1}

        # Second consume returns empty (one-time)
        result2 = consume_confirm_token(token)
        assert result2 == {}

    @patch("services.atr_policy_confirm_tokens._redis")
    def test_expired_token_returns_empty(self, mk_redis):
        store_mock = MagicMock()
        store_mock.get.return_value = None  # Simulate expired/missing
        mk_redis.return_value = store_mock
        from services.atr_policy_confirm_tokens import consume_confirm_token

        result = consume_confirm_token("nonexistent_token_00")
        assert result == {}

    @patch("services.atr_policy_confirm_tokens._redis")
    def test_actor_mismatch_detected_by_caller(self, mk_redis):
        """The callback worker checks actor; confirm_tokens itself just returns payload."""
        mk_redis.return_value = self._make_redis_store()
        from services.atr_policy_confirm_tokens import issue_confirm_token, consume_confirm_token

        token = issue_confirm_token(actor="alice", action="REVOKE", target="pid456", payload={})
        tok = consume_confirm_token(token)
        # Caller should check tok["actor"] != requesting_actor
        assert tok["actor"] == "alice"
        assert tok["actor"] != "bob"  # mismatch scenario


# ═════════════════════════════════════════════════════════════════════════════
# arm_cooldown
# ═════════════════════════════════════════════════════════════════════════════


class TestArmCooldown:
    @patch("services.atr_policy_guardrails._redis")
    def test_arm_sets_key_and_increments_flip(self, mk_redis):
        r = MagicMock()
        r.set.return_value = True
        r.incr.return_value = 1
        r.expire.return_value = True
        mk_redis.return_value = r
        from services.atr_policy_guardrails import arm_cooldown

        arm_cooldown(_make_obj(), actor="alice", action="APPROVE")
        r.set.assert_called_once()
        r.incr.assert_called_once()
        r.expire.assert_called_once()

        # Check TTL argument on set
        _, kwargs = r.set.call_args
        assert kwargs.get("ex", 0) > 0
