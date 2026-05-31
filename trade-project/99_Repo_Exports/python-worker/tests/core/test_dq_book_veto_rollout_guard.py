"""Tests for should_enforce_dq_veto() rollout guard.

Tests five scenarios:
1. dq_veto=0 → always False
2. mode=observe → always False
3. non-book bucket + enforce mode → True
4. book bucket, dq_book_veto_enabled=0 → False
5. book bucket, warmup elapsed vs not elapsed
"""
import pytest

from core.of_confirm_engine import OFConfirmEngine

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def observe_cfg():
    """Mode=observe — gate never enforces."""
    return {"dq_gate_mode": "observe"}


@pytest.fixture
def enforce_cfg():
    """Mode=enforce, book veto enabled, warmup 10s."""
    return {
        "dq_gate_mode": "enforce",
        "dq_book_veto_enabled": 1,
        "dq_book_veto_warmup_s": 10,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def _simulate(cfg, uptime_s):
    engine = OFConfirmEngine()
    engine._start_ms = engine._now_ms() - (uptime_s * 1000)
    return engine._should_apply_dq_veto(cfg)

def test_mode_observe_always_false(observe_cfg):
    """dq_gate_mode=observe → never enforce regardless of veto flag."""
    assert _simulate(observe_cfg, uptime_s=9999) is False
    assert _simulate(observe_cfg, uptime_s=9999) is False


def test_non_book_bucket_enforce_mode_true():
    """Non-book_seq bucket + enforce mode → True (no extra warmup guard)."""
    # Note: method no longer takes bucket, it only decides whether to apply the book veto block at all
    cfg = {"dq_gate_mode": "enforce", "dq_book_veto_enabled": 1}
    assert _simulate(cfg, uptime_s=0) is True


def test_book_seq_disabled_flag(enforce_cfg):
    """book_seq bucket + dq_book_veto_enabled=0 → False."""
    cfg = dict(enforce_cfg)
    cfg["dq_book_veto_enabled"] = 0
    assert _simulate(cfg, uptime_s=9999) is False


def test_book_seq_warmup_elapsed_vs_not(enforce_cfg):
    """book_seq bucket, enabled=1: warmup_not_elapsed=False, elapsed=True."""
    # Not yet warmed up
    assert _simulate(enforce_cfg, uptime_s=5) is False
    # Warmup elapsed (10s)
    assert _simulate(enforce_cfg, uptime_s=10) is True
    assert _simulate(enforce_cfg, uptime_s=100) is True


def test_dq_veto_zero_always_false(enforce_cfg):
    """dq_veto=0 → always False regardless of gate mode."""
    # Method no longer takes dq_veto level, only checks config rollout
    assert _simulate(enforce_cfg, uptime_s=9999) is True  # Enforce is active
    cfg2 = dict(enforce_cfg)
    cfg2["dq_book_veto_enabled"] = 0
    assert _simulate(cfg2, uptime_s=9999) is False
