"""Tests for should_enforce_dq_veto() rollout guard.

Tests five scenarios:
1. dq_veto=0 → always False
2. mode=observe → always False
3. non-book bucket + enforce mode → True
4. book bucket, dq_book_veto_enabled=0 → False
5. book bucket, warmup elapsed vs not elapsed
"""
import os
import sys
from pathlib import Path

# Same pattern as test_book_seq_tracker_v2_timegap_depth20_100ms.py
ROOT = Path(__file__).resolve().parents[1]  # tests/
TICK_FLOW_FULL = ROOT.parent               # tick_flow_full/
PY_WORKER = TICK_FLOW_FULL.parent          # python-worker/
CORE_DIR = PY_WORKER / "core"             # python-worker/core/

# tick_flow_full first (for any core/ aliases it may expose)
for p in [str(TICK_FLOW_FULL), str(PY_WORKER)]:
    if p not in sys.path:
        sys.path.insert(0, p)

import pytest

try:
    from core.of_confirm_engine import should_enforce_dq_veto
except Exception as exc:
    pytest.skip(f"could not import should_enforce_dq_veto: {exc}", allow_module_level=True)


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
        "dq_gate_mode": "enforce"
        "dq_book_veto_enabled": 1
        "dq_book_veto_warmup_s": 10
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_mode_observe_always_false(observe_cfg):
    """dq_gate_mode=observe → never enforce regardless of veto flag."""
    assert should_enforce_dq_veto(1, "book_seq", observe_cfg, uptime_s=9999) is False
    assert should_enforce_dq_veto(1, "atr", observe_cfg, uptime_s=9999) is False


def test_non_book_bucket_enforce_mode_true():
    """Non-book_seq bucket + enforce mode → True (no extra warmup guard)."""
    cfg = {"dq_gate_mode": "enforce"}
    assert should_enforce_dq_veto(1, "atr", cfg, uptime_s=0) is True
    assert should_enforce_dq_veto(1, "tick_age", cfg, uptime_s=100) is True


def test_book_seq_disabled_flag(enforce_cfg):
    """book_seq bucket + dq_book_veto_enabled=0 → False."""
    cfg = dict(enforce_cfg)
    cfg["dq_book_veto_enabled"] = 0
    assert should_enforce_dq_veto(1, "book_seq", cfg, uptime_s=9999) is False


def test_book_seq_warmup_elapsed_vs_not(enforce_cfg):
    """book_seq bucket, enabled=1: warmup_not_elapsed=False, elapsed=True."""
    # Not yet warmed up
    assert should_enforce_dq_veto(1, "book_seq", enforce_cfg, uptime_s=5) is False
    # Warmup elapsed (10s)
    assert should_enforce_dq_veto(1, "book_seq", enforce_cfg, uptime_s=10) is True
    assert should_enforce_dq_veto(1, "book_seq", enforce_cfg, uptime_s=100) is True


def test_dq_veto_zero_always_false(enforce_cfg):
    """dq_veto=0 → always False regardless of gate mode."""
    assert should_enforce_dq_veto(0, "book_seq", enforce_cfg, uptime_s=9999) is False
    assert should_enforce_dq_veto(0, "atr", {"dq_gate_mode": "enforce"}, uptime_s=0) is False
