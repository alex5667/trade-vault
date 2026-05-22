"""Tests for _push_virtual_to_binance_queue guards (Риск №3).

Guard 1 — DQ/time/integrity/book-sanity denylist (two complementary checks):
  a) rejection_gate in _VIRTUAL_ORDER_SKIP_GATES → return before queue push.
  b) rejection_reason in _VIRTUAL_ORDER_SKIP_REASONS → return before queue push.

Guard 2 — invalid-levels:
  entry=0 or sl=0 or lot=0 → return before queue push (counter incremented).

Also covers split live/virtual confidence threshold (Риск реализации из планирования).
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# msgspec is not installed in the CI/test environment; patch core.contracts before
# importing signal_pipeline so that the strict contract check doesn't abort early.
import sys
import types as _types

_contracts_stub = _types.ModuleType("core.contracts")
_contracts_stub.SignalV1Strict = lambda **kw: None  # type: ignore[attr-defined]
sys.modules.setdefault("core.contracts", _contracts_stub)
sys.modules.setdefault("msgspec", _types.ModuleType("msgspec"))

import services.orderflow.signal_pipeline as _sp_mod
from services.orderflow.signal_pipeline import _VIRTUAL_ORDER_SKIP_REASONS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pipeline(virtual_min_conf: float = 35.0, live_min_conf: float = 70.0) -> _sp_mod.SignalPipeline:
    """Build a minimal SignalPipeline stub with only the attributes accessed by
    _push_virtual_to_binance_queue."""
    pipeline = object.__new__(_sp_mod.SignalPipeline)
    pipeline.binance_virtual_mirror_all = False
    pipeline.binance_virtual_orders_enabled = True  # shadow-only mode
    pipeline._cached_min_conf_pct = live_min_conf
    pipeline._cached_virtual_min_conf_pct = virtual_min_conf
    pipeline._cached_orders_intent_queue = "orders:queue:binance:intent"
    pipeline._cached_orders_mirror_queue = "orders:queue:binance:mirror"
    # Phase 5.4 budget gate — advisory=True so it never hard-blocks test signals
    pipeline._cached_exec_budget_advisory = True
    pipeline._cached_exec_budget_fail_policy = "OPEN"
    # Phase 5.6 portfolio gate — disabled
    pipeline._cached_portfolio_advisory = True
    pipeline._cached_portfolio_enable = False
    pipeline._cached_portfolio_fail_policy = "OPEN"
    # Phase 5.x regime-stress gate — disabled
    pipeline._cached_regime_stress_advisory = True
    pipeline._cached_regime_stress_enable = False
    pipeline._cached_regime_stress_fail_policy = "OPEN"
    # Analytics DB — not needed in unit tests
    pipeline._cached_analytics_db_dsn = ""
    # FEES_BPS_RT is a property backed by _cached_fees_bps_rt
    pipeline._cached_fees_bps_rt = 10.0
    pipeline.notify_stream = "stream:notify"
    # orchestrator.portfolio_gate is only accessed when not is_rejected_signal
    orchestrator_mock = MagicMock()
    orchestrator_mock.portfolio_gate = None
    pipeline.orchestrator = orchestrator_mock

    # Publisher with recordable rpush (actual order push) and xadd (telegram notify)
    pushes: list[tuple[str, str]] = []

    async def _rpush(queue, payload, **kw):
        pushes.append((queue, payload))

    redis_mock = MagicMock()
    redis_mock.rpush = AsyncMock(side_effect=_rpush)
    redis_mock.xadd = AsyncMock(return_value=None)  # telegram — don't fail
    publisher = MagicMock()
    publisher.r = redis_mock
    pipeline.publisher = publisher

    # send_telegram_report — fail-open stub
    async def _send_telegram(text, source="", symbol="", runtime=None):
        pass

    pipeline.send_telegram_report = _send_telegram  # type: ignore[attr-defined]

    # Attach pushes list so callers can inspect via _xadds for backward compat
    pipeline._xadds = pushes  # type: ignore[attr-defined]
    return pipeline


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


_BASE_KWARGS = dict(
    sid="test-sid",
    symbol="BTCUSDT",
    direction="LONG",
    entry=65_000.0,
    sl=64_000.0,
    tp_levels=[66_000.0],
    lot=0.01,
    ts_ms=1_716_000_000_000,
    confidence=0.40,
    enriched_signal={"validation_status": "failed", "is_virtual": 0},
    indicators={},
    is_rejected_signal=True,
)


# ---------------------------------------------------------------------------
# Guard 1: DQ reason denylist
# ---------------------------------------------------------------------------

class TestGuard1DQDenylist:
    """Guard 1: signals with a DQ/time/integrity reason_code must not reach the queue."""

    @pytest.mark.parametrize("reason", sorted(_VIRTUAL_ORDER_SKIP_REASONS))
    def test_dq_reason_blocks_virtual_order(self, reason: str) -> None:
        pipeline = _make_pipeline()
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS, "rejection_reason": reason},
        ))
        assert pipeline._xadds == [], (
            f"reason={reason!r} should block virtual order, but xadd was called"
        )

    def test_unknown_reason_not_blocked_by_guard1(self) -> None:
        """A non-DQ reason passes Guard 1 (may still be stopped by Guard 2 or conf check)."""
        pipeline = _make_pipeline(virtual_min_conf=35.0)
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS,
               "rejection_reason": "VETO_SOME_SIGNAL_GATE",
               "confidence": 0.40},  # above 35% virtual threshold
        ))
        # xadd should have been called (not blocked by Guard 1)
        assert len(pipeline._xadds) == 1

    def test_empty_rejection_reason_not_blocked(self) -> None:
        """Empty rejection_reason bypasses Guard 1 (falsy check)."""
        pipeline = _make_pipeline(virtual_min_conf=35.0)
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS, "rejection_reason": "", "confidence": 0.40},
        ))
        assert len(pipeline._xadds) == 1


# ---------------------------------------------------------------------------
# Guard 2: invalid levels
# ---------------------------------------------------------------------------

class TestGuard2InvalidLevels:
    """Guard 2: signals with zero entry/sl/lot must not reach the queue."""

    @pytest.mark.parametrize("field,value", [
        ("entry", 0.0),
        ("sl", 0.0),
        ("lot", 0.0),
    ])
    def test_zero_level_blocks_virtual_order(self, field: str, value: float) -> None:
        pipeline = _make_pipeline(virtual_min_conf=35.0)
        kwargs = {**_BASE_KWARGS, field: value, "rejection_reason": "VETO_SIGNAL_WEAK"}
        _run(pipeline._push_virtual_to_binance_queue(**kwargs))
        assert pipeline._xadds == [], (
            f"{field}=0 should block virtual order, but xadd was called"
        )

    def test_valid_levels_pass_guard2(self) -> None:
        """All levels positive → Guard 2 does not block."""
        pipeline = _make_pipeline(virtual_min_conf=35.0)
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS, "rejection_reason": "VETO_SIGNAL_WEAK", "confidence": 0.40},
        ))
        assert len(pipeline._xadds) == 1


# ---------------------------------------------------------------------------
# Split confidence threshold
# ---------------------------------------------------------------------------

class TestSplitConfidenceThreshold:
    """Live gate (CRYPTO_SIGNAL_MIN_CONF) must remain strict.
    Virtual path uses VIRTUAL_SIGNAL_MIN_CONF independently."""

    def test_below_live_above_virtual_routes_to_virtual(self) -> None:
        """confidence=0.40: below live=0.70 but above virtual=0.35 → virtual queue."""
        pipeline = _make_pipeline(virtual_min_conf=35.0, live_min_conf=70.0)
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS, "confidence": 0.40, "rejection_reason": ""},
        ))
        assert len(pipeline._xadds) == 1

    def test_below_virtual_threshold_blocked(self) -> None:
        """confidence=0.20: below virtual=0.35 → blocked even for rejected signals."""
        pipeline = _make_pipeline(virtual_min_conf=35.0, live_min_conf=70.0)
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS, "confidence": 0.20, "rejection_reason": ""},
        ))
        assert pipeline._xadds == []

    def test_virtual_threshold_equal_to_live_when_unset(self) -> None:
        """VIRTUAL_SIGNAL_MIN_CONF defaults to CRYPTO_SIGNAL_MIN_CONF when equal → same behaviour."""
        pipeline = _make_pipeline(virtual_min_conf=70.0, live_min_conf=70.0)
        # Below both thresholds → blocked
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS, "confidence": 0.40, "rejection_reason": ""},
        ))
        assert pipeline._xadds == []

    def test_dq_reason_blocks_even_with_valid_levels_and_high_confidence(self) -> None:
        """DQ guard fires before confidence/level checks — always wins."""
        pipeline = _make_pipeline(virtual_min_conf=0.0, live_min_conf=0.0)
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS,
               "confidence": 1.0,
               "rejection_reason": "VETO_BAD_TS_NOT_EPOCH"},
        ))
        assert pipeline._xadds == []


# ---------------------------------------------------------------------------
# Guard 1 extension: gate-name check (BookSanityGate)
# ---------------------------------------------------------------------------

class TestGuard1GateName:
    """Guard 1b: rejection_gate in _VIRTUAL_ORDER_SKIP_GATES blocks regardless of reason."""

    def test_book_sanity_gate_by_gate_name_blocks(self) -> None:
        """rejection_gate=BookSanityGate alone is sufficient to block."""
        pipeline = _make_pipeline(virtual_min_conf=0.0, live_min_conf=0.0)
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS,
               "confidence": 1.0,
               "rejection_reason": "",   # no reason code — gate name alone triggers
               "rejection_gate": "BookSanityGate"},
        ))
        assert pipeline._xadds == [], "BookSanityGate by gate name should block virtual order"

    def test_book_sanity_reason_codes_blocked(self) -> None:
        """BookSanityGate reason codes are in _VIRTUAL_ORDER_SKIP_REASONS."""
        from services.orderflow.signal_pipeline import _VIRTUAL_ORDER_SKIP_REASONS
        book_sanity_reasons = {
            "VETO_BOOK_SANITY", "VETO_BOOK_CROSS",
            "VETO_BOOK_NAN", "VETO_BOOK_NEG_QTY", "VETO_TRADE_OUTSIDE_BBO",
        }
        assert book_sanity_reasons <= _VIRTUAL_ORDER_SKIP_REASONS, (
            "All BookSanityGate reason codes must be in _VIRTUAL_ORDER_SKIP_REASONS"
        )

    @pytest.mark.parametrize("reason", [
        "VETO_BOOK_CROSS", "VETO_BOOK_NAN", "VETO_BOOK_NEG_QTY", "VETO_TRADE_OUTSIDE_BBO",
    ])
    def test_book_sanity_reason_blocks_virtual_order(self, reason: str) -> None:
        pipeline = _make_pipeline(virtual_min_conf=0.0, live_min_conf=0.0)
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS, "confidence": 1.0, "rejection_reason": reason},
        ))
        assert pipeline._xadds == [], f"BookSanityGate reason {reason!r} should block"

    def test_stream_integrity_gate_by_name_blocks(self) -> None:
        pipeline = _make_pipeline(virtual_min_conf=0.0, live_min_conf=0.0)
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS,
               "confidence": 1.0,
               "rejection_reason": "",
               "rejection_gate": "StreamIntegrityGate"},
        ))
        assert pipeline._xadds == []

    def test_unknown_gate_not_blocked(self) -> None:
        """Gates not in the denylist pass Guard 1b."""
        pipeline = _make_pipeline(virtual_min_conf=35.0)
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS,
               "confidence": 0.40,
               "rejection_reason": "",
               "rejection_gate": "SomeSignalGate"},
        ))
        assert len(pipeline._xadds) == 1


# ---------------------------------------------------------------------------
# Guard 2 counter: invalid-levels increment
# ---------------------------------------------------------------------------

class TestGuard2InvalidLevelsCounter:
    """Guard 2 must increment virtual_order_invalid_levels_total with correct label."""

    @pytest.mark.parametrize("field,expected_label", [
        ("entry", "zero_entry"),
        ("sl", "zero_sl"),
        ("lot", "zero_lot"),
    ])
    def test_counter_label_matches_zero_field(
        self, field: str, expected_label: str
    ) -> None:
        counter_mock = MagicMock()
        labels_mock = MagicMock()
        counter_mock.labels.return_value = labels_mock

        with patch(
            "services.orderflow.signal_pipeline._VIRTUAL_ORDER_INVALID_LEVELS_TOTAL",
            counter_mock,
        ):
            pipeline = _make_pipeline(virtual_min_conf=35.0)
            kwargs = {
                **_BASE_KWARGS,
                field: 0.0,
                "rejection_reason": "VETO_SIGNAL_WEAK",
            }
            _run(pipeline._push_virtual_to_binance_queue(**kwargs))

        counter_mock.labels.assert_called_once_with(reason=expected_label)
        labels_mock.inc.assert_called_once()


# ---------------------------------------------------------------------------
# Guard 1 counter: DQ skip increments virtual_order_skipped_bad_dq_total
# ---------------------------------------------------------------------------

class TestGuard1DQCounter:
    """Guard 1 must increment virtual_order_skipped_bad_dq_total with correct label."""

    @pytest.mark.parametrize("reason,gate,expected_label", [
        # reason-based: label is the reason code
        ("VETO_BOOK_CROSS", "", "VETO_BOOK_CROSS"),
        ("VETO_BAD_TS_NOT_EPOCH", "", "VETO_BAD_TS_NOT_EPOCH"),
        # gate-based with empty reason: label is "gate:<name>"
        ("", "BookSanityGate", "gate:BookSanityGate"),
        ("", "HardDataQualityGate", "gate:HardDataQualityGate"),
        # gate-based with unknown reason: gate wins, label uses reason code
        ("SOME_NEW_REASON", "HardDataQualityGate", "SOME_NEW_REASON"),
    ])
    def test_dq_counter_incremented(
        self, reason: str, gate: str, expected_label: str
    ) -> None:
        counter_mock = MagicMock()
        labels_mock = MagicMock()
        counter_mock.labels.return_value = labels_mock

        with patch(
            "services.orderflow.signal_pipeline._VIRTUAL_ORDER_SKIPPED_BAD_DQ_TOTAL",
            counter_mock,
        ):
            pipeline = _make_pipeline(virtual_min_conf=0.0, live_min_conf=0.0)
            _run(pipeline._push_virtual_to_binance_queue(
                **{**_BASE_KWARGS,
                   "confidence": 1.0,
                   "rejection_reason": reason,
                   "rejection_gate": gate},
            ))

        counter_mock.labels.assert_called_once_with(reason=expected_label)
        labels_mock.inc.assert_called_once()
        assert pipeline._xadds == [], "DQ skip counter: no order should be pushed"

    def test_gate_unknown_reason_skips_by_gate(self) -> None:
        """Gate-name alone is sufficient even if reason code is unknown/new."""
        pipeline = _make_pipeline(virtual_min_conf=0.0, live_min_conf=0.0)
        _run(pipeline._push_virtual_to_binance_queue(
            **{**_BASE_KWARGS,
               "confidence": 1.0,
               "rejection_reason": "SOME_FUTURE_DQ_REASON",
               "rejection_gate": "HardDataQualityGate"},
        ))
        assert pipeline._xadds == []
