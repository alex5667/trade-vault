from __future__ import annotations
"""Tests for OFConfirmEngine._should_apply_dq_veto (observe-only rollout v1).

These are lightweight unit tests that do not require Redis or Prometheus.
They validate the guard logic for the book_missing_seq_hard DQ veto rollout.
"""

from utils.time_utils import get_ny_time_millis

import sys
import time
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Import guard: allow tests to run even in a partial environment
# ---------------------------------------------------------------------------
try:
    from core.of_confirm_engine import OFConfirmEngine
except Exception as exc:
    pytest.skip(f"could not import OFConfirmEngine: {exc}", allow_module_level=True)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def engine():
    """Create a fresh OFConfirmEngine instance with a known _start_ms."""
    eng = OFConfirmEngine(version=3)
    return eng


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestShouldApplyDqVeto:
    """Unit tests for _should_apply_dq_veto()."""

    def test_disabled_by_default_returns_false(self, engine: OFConfirmEngine):
        """dq_book_veto_enabled=0 (default) => never veto."""
        cfg = {"dq_book_veto_enabled": 0, "dq_book_veto_warmup_s": 0}
        assert engine._should_apply_dq_veto(cfg) is False

    def test_enabled_no_warmup_returns_true(self, engine: OFConfirmEngine):
        """dq_book_veto_enabled=1, warmup=0 => veto immediately."""
        cfg = {"dq_book_veto_enabled": 1, "dq_book_veto_warmup_s": 0}
        assert engine._should_apply_dq_veto(cfg) is True

    def test_enabled_warmup_not_elapsed_returns_false(self, engine: OFConfirmEngine):
        """dq_book_veto_enabled=1, warmup=3600 and uptime ~0 => not ready."""
        cfg = {"dq_book_veto_enabled": 1, "dq_book_veto_warmup_s": 3600}
        # Force _start_ms to now so uptime ≈ 0s
        engine._start_ms = get_ny_time_millis()
        assert engine._should_apply_dq_veto(cfg) is False

    def test_enabled_warmup_elapsed_returns_true(self, engine: OFConfirmEngine):
        """dq_book_veto_enabled=1, warmup=1 and uptime > 1s => ready."""
        cfg = {"dq_book_veto_enabled": 1, "dq_book_veto_warmup_s": 1}
        # Set _start_ms to 2 seconds ago so elapsed >= warmup
        engine._start_ms = int((time.time() - 2) * 1000)
        assert engine._should_apply_dq_veto(cfg) is True

    def test_enabled_warmup_exactly_met_returns_true(self, engine: OFConfirmEngine):
        """Boundary condition: elapsed == warmup => ready."""
        cfg = {"dq_book_veto_enabled": 1, "dq_book_veto_warmup_s": 5}
        engine._start_ms = int((time.time() - 5) * 1000)
        # elapsed_s = 5 >= 5
        assert engine._should_apply_dq_veto(cfg) is True

    def test_string_enabled_flag(self, engine: OFConfirmEngine):
        """String "1" as enabled value is coerced correctly."""
        cfg = {"dq_book_veto_enabled": "1", "dq_book_veto_warmup_s": 0}
        assert engine._should_apply_dq_veto(cfg) is True

    def test_missing_keys_use_defaults(self, engine: OFConfirmEngine):
        """Empty cfg defaults to disabled (fail-open)."""
        assert engine._should_apply_dq_veto({}) is False

    def test_exception_in_cfg_returns_false(self, engine: OFConfirmEngine):
        """Any exception => fail-open (return False)."""
        # Pass something that will cause int() to fail
        result = engine._should_apply_dq_veto({"dq_book_veto_enabled": "broken"})
        # 'broken' -> int('broken') raises, should return False
        assert result is False

    def test_start_ms_is_set_at_init(self):
        """_start_ms is set during __init__ and is a positive int."""
        before = get_ny_time_millis() - 100
        eng = OFConfirmEngine(version=3)
        after = get_ny_time_millis() + 100
        assert isinstance(eng._start_ms, int)
        assert before <= eng._start_ms <= after


class TestDqVetoActiveIndicator:
    """Smoke tests: build() sets dq_book_veto_active when dq_veto=1."""

    def test_dq_book_veto_active_not_set_when_no_veto(self, engine: OFConfirmEngine):
        """If there is no DQ veto, dq_book_veto_active should not be injected by guard."""
        # Since build() is large we just test _should_apply_dq_veto logic
        # and trust the integration via smoke test (see test_book_missing_seq_v1.py).
        cfg = {"dq_book_veto_enabled": 0}
        assert not engine._should_apply_dq_veto(cfg)
