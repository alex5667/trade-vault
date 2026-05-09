from __future__ import annotations

"""
tests/test_position_fsm.py — P1-9: Unit tests for explicit PositionFSM.

Coverage:
  - All legal transitions reachable from PENDING
  - InvalidTransitionError for forbidden transitions
  - Audit trail recording
  - Boolean flag sync on PositionState
  - fsm_from_position recovery helper
  - is_terminal() correctness
  - to_redis_payload() shape
"""

import time

import pytest

from domain.position_fsm import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATES,
    InvalidTransitionError,
    PositionFSM,
    PositionStatus,
    fsm_from_position,
)

# ---------------------------------------------------------------------------
# Minimal stub of PositionState (avoids importing the full domain)
# ---------------------------------------------------------------------------


class _StubPos:
    """Minimal stub matching the attributes PositionFSM reads/writes."""

    def __init__(self, pos_id: str = "test-pos-1") -> None:
        self.id = pos_id
        self.symbol = "BTCUSDT"
        self.entry_ts_ms = int(time.time() * 1000)

        # boolean flags (backward compat)
        self.closed = False
        self.tp1_hit = False
        self.tp2_hit = False
        self.tp3_hit = False
        self.tp_hits = 0
        self.tp1_touched = False
        self.tp2_touched = False
        self.trailing_started = False
        self.trailing_active = False
        self.trail_armed = False

        # FSM status string — empty so fsm_from_position uses boolean-flag inference
        self.fsm_status: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_fsm(initial: PositionStatus = PositionStatus.PENDING) -> tuple[PositionFSM, _StubPos]:
    pos = _StubPos()
    fsm = PositionFSM(pos, initial_status=initial)
    return fsm, pos


# ---------------------------------------------------------------------------
# 1. Initial state
# ---------------------------------------------------------------------------


class TestInitialState:
    def test_initial_status_is_pending(self) -> None:
        fsm, _ = make_fsm()
        assert fsm.status is PositionStatus.PENDING

    def test_not_terminal_at_pending(self) -> None:
        fsm, _ = make_fsm()
        assert not fsm.is_terminal()

    def test_fsm_status_synced_on_pos(self) -> None:
        fsm, pos = make_fsm()
        assert pos.fsm_status == "PENDING"


# ---------------------------------------------------------------------------
# 2. Legal transitions: happy path
# ---------------------------------------------------------------------------


class TestLegalTransitions:
    def test_pending_to_open(self) -> None:
        fsm, pos = make_fsm()
        rec = fsm.transition(PositionStatus.OPEN, trigger="open_position")
        assert fsm.status is PositionStatus.OPEN
        assert pos.fsm_status == "OPEN"
        assert not pos.closed
        assert rec.from_state == "PENDING"
        assert rec.to_state == "OPEN"

    def test_open_to_tp1(self) -> None:
        fsm, pos = make_fsm(PositionStatus.OPEN)
        fsm.transition(PositionStatus.TP1_HIT, trigger="tp1_hit")
        assert fsm.status is PositionStatus.TP1_HIT
        assert pos.tp1_hit is True
        assert pos.tp1_touched is True
        assert pos.tp_hits == 1

    def test_tp1_to_tp2(self) -> None:
        fsm, pos = make_fsm(PositionStatus.TP1_HIT)
        fsm.transition(PositionStatus.TP2_HIT, trigger="tp2_hit")
        assert pos.tp2_hit is True
        assert pos.tp_hits == 2

    def test_tp2_to_trailing_armed(self) -> None:
        fsm, pos = make_fsm(PositionStatus.TP2_HIT)
        fsm.transition(PositionStatus.TRAILING_ARMED, trigger="arm_trailing")
        assert pos.trail_armed is True
        assert pos.trailing_started is True

    def test_trailing_armed_to_active(self) -> None:
        fsm, pos = make_fsm(PositionStatus.TRAILING_ARMED)
        fsm.transition(PositionStatus.TRAILING_ACTIVE, trigger="trailing_move")
        assert pos.trailing_active is True

    def test_trailing_active_self_loop(self) -> None:
        fsm, _ = make_fsm(PositionStatus.TRAILING_ACTIVE)
        # Multiple trailing moves — self-transition is explicitly allowed
        for _ in range(3):
            fsm.transition(PositionStatus.TRAILING_ACTIVE, trigger="trailing_move")
        assert fsm.status is PositionStatus.TRAILING_ACTIVE

    def test_open_to_closed(self) -> None:
        fsm, pos = make_fsm(PositionStatus.OPEN)
        fsm.transition(PositionStatus.CLOSED, trigger="sl_hit")
        assert fsm.status is PositionStatus.CLOSED
        assert pos.closed is True
        assert fsm.is_terminal()

    def test_open_to_orphan_closed(self) -> None:
        fsm, pos = make_fsm(PositionStatus.OPEN)
        fsm.transition(PositionStatus.ORPHAN_CLOSED, trigger="orphan_expiry")
        assert pos.closed is True
        assert fsm.is_terminal()

    def test_trailing_active_to_closed(self) -> None:
        fsm, pos = make_fsm(PositionStatus.TRAILING_ACTIVE)
        fsm.transition(PositionStatus.CLOSED, trigger="trailing_stop_hit")
        assert fsm.is_terminal()
        assert pos.closed is True

    def test_full_happy_path(self) -> None:
        """PENDING → OPEN → TP1 → TP2 → TRAILING_ARMED → TRAILING_ACTIVE → CLOSED"""
        fsm, pos = make_fsm()
        fsm.transition(PositionStatus.OPEN, trigger="open_position")
        fsm.transition(PositionStatus.TP1_HIT, trigger="tp1_hit")
        fsm.transition(PositionStatus.TP2_HIT, trigger="tp2_hit")
        fsm.transition(PositionStatus.TRAILING_ARMED, trigger="arm_trailing")
        fsm.transition(PositionStatus.TRAILING_ACTIVE, trigger="trailing_move")
        fsm.transition(PositionStatus.CLOSED, trigger="trailing_stop_hit")

        assert fsm.is_terminal()
        assert len(fsm.trail) == 6


# ---------------------------------------------------------------------------
# 3. Illegal transitions
# ---------------------------------------------------------------------------


class TestIllegalTransitions:
    def test_pending_to_closed_raises(self) -> None:
        fsm, _ = make_fsm(PositionStatus.PENDING)
        with pytest.raises(InvalidTransitionError):
            fsm.transition(PositionStatus.CLOSED, trigger="bad")

    def test_closed_to_open_raises(self) -> None:
        fsm, _ = make_fsm(PositionStatus.CLOSED)
        with pytest.raises(InvalidTransitionError):
            fsm.transition(PositionStatus.OPEN, trigger="reopen")

    def test_tp2_to_tp1_raises(self) -> None:
        fsm, _ = make_fsm(PositionStatus.TP2_HIT)
        with pytest.raises(InvalidTransitionError):
            fsm.transition(PositionStatus.TP1_HIT, trigger="downgrade")

    def test_orphan_closed_to_anything_raises(self) -> None:
        fsm, _ = make_fsm(PositionStatus.ORPHAN_CLOSED)
        with pytest.raises(InvalidTransitionError):
            fsm.transition(PositionStatus.OPEN, trigger="bad")

    def test_error_contains_states(self) -> None:
        fsm, _ = make_fsm(PositionStatus.PENDING)
        with pytest.raises(InvalidTransitionError) as exc_info:
            fsm.transition(PositionStatus.TRAILING_ACTIVE, trigger="bad")
        err = exc_info.value
        assert err.from_state is PositionStatus.PENDING
        assert err.to_state is PositionStatus.TRAILING_ACTIVE


# ---------------------------------------------------------------------------
# 4. Audit trail
# ---------------------------------------------------------------------------


class TestAuditTrail:
    def test_trail_grows(self) -> None:
        fsm, _ = make_fsm()
        fsm.transition(PositionStatus.OPEN, trigger="open")
        fsm.transition(PositionStatus.TP1_HIT, trigger="tp1")
        assert len(fsm.trail) == 2

    def test_trail_is_copy(self) -> None:
        fsm, _ = make_fsm()
        trail_ref = fsm.trail
        fsm.transition(PositionStatus.OPEN, trigger="open")
        # Original copy must not be mutated
        assert len(trail_ref) == 0

    def test_trail_record_fields(self) -> None:
        fsm, _ = make_fsm()
        rec = fsm.transition(
            PositionStatus.OPEN,
            trigger="open_position",
            actor="test_actor",
            reason="unit test",
            price=42000.0,
        )
        assert rec.trigger == "open_position"
        assert rec.actor == "test_actor"
        assert rec.reason == "unit test"
        assert rec.meta.get("price") == 42000.0
        assert isinstance(rec.ts_ms, int)

    def test_trail_bounded(self) -> None:
        """Trail must not grow beyond MAX_TRAIL_LEN."""
        fsm, _ = make_fsm(PositionStatus.TRAILING_ACTIVE)
        limit = PositionFSM.MAX_TRAIL_LEN + 10
        for _ in range(limit):
            fsm.transition(PositionStatus.TRAILING_ACTIVE, trigger="move")
        assert len(fsm.trail) <= PositionFSM.MAX_TRAIL_LEN

    def test_trail_to_dict(self) -> None:
        fsm, _ = make_fsm()
        fsm.transition(PositionStatus.OPEN, trigger="open")
        rec = fsm.trail[0]
        d = rec.to_dict()
        assert d["from"] == "PENDING"
        assert d["to"] == "OPEN"
        assert d["trigger"] == "open"


# ---------------------------------------------------------------------------
# 5. is_terminal()
# ---------------------------------------------------------------------------


class TestIsTerminal:
    @pytest.mark.parametrize("state", [PositionStatus.CLOSED, PositionStatus.ORPHAN_CLOSED])
    def test_terminal_states(self, state: PositionStatus) -> None:
        fsm, _ = make_fsm(state)
        assert fsm.is_terminal()

    @pytest.mark.parametrize(
        "state",
        [
            PositionStatus.PENDING,
            PositionStatus.OPEN,
            PositionStatus.TP1_HIT,
            PositionStatus.TP2_HIT,
            PositionStatus.TRAILING_ARMED,
            PositionStatus.TRAILING_ACTIVE,
        ],
    )
    def test_non_terminal_states(self, state: PositionStatus) -> None:
        fsm, _ = make_fsm(state)
        assert not fsm.is_terminal()


# ---------------------------------------------------------------------------
# 6. to_redis_payload()
# ---------------------------------------------------------------------------


class TestRedisPayload:
    def test_payload_keys_present(self) -> None:
        fsm, _ = make_fsm()
        fsm.transition(PositionStatus.OPEN, trigger="open")
        payload = fsm.to_redis_payload()
        assert "pos_id" in payload
        assert "symbol" in payload
        assert "status" in payload
        assert "trail_len" in payload
        assert payload["status"] == "OPEN"
        assert payload["trail_len"] == 1


# ---------------------------------------------------------------------------
# 7. fsm_from_position (recovery)
# ---------------------------------------------------------------------------


class TestFsmFromPosition:
    def test_recover_open(self) -> None:
        pos = _StubPos()
        # fresh position — all defaults → should infer OPEN
        fsm = fsm_from_position(pos)
        assert fsm.status is PositionStatus.OPEN

    def test_recover_closed(self) -> None:
        pos = _StubPos()
        pos.closed = True
        fsm = fsm_from_position(pos)
        assert fsm.status is PositionStatus.CLOSED
        assert fsm.is_terminal()

    def test_recover_trailing_active(self) -> None:
        pos = _StubPos()
        pos.trailing_active = True
        fsm = fsm_from_position(pos)
        assert fsm.status is PositionStatus.TRAILING_ACTIVE

    def test_recover_tp1(self) -> None:
        pos = _StubPos()
        pos.tp1_hit = True
        fsm = fsm_from_position(pos)
        assert fsm.status is PositionStatus.TP1_HIT

    def test_recover_from_stored_fsm_status(self) -> None:
        """Stored fsm_status string takes priority over boolean flags."""
        pos = _StubPos()
        # flags say TP1_HIT but stored status says TP2_HIT — stored wins
        pos.tp1_hit = False
        pos.fsm_status = "TP2_HIT"
        fsm = fsm_from_position(pos)
        assert fsm.status is PositionStatus.TP2_HIT

    def test_recover_from_invalid_stored_falls_back_to_flags(self) -> None:
        pos = _StubPos()
        pos.tp2_hit = True
        pos.fsm_status = "NOT_A_VALID_STATE"
        fsm = fsm_from_position(pos)
        # falls back to flag inference
        assert fsm.status is PositionStatus.TP2_HIT


# ---------------------------------------------------------------------------
# 8. Transition table completeness
# ---------------------------------------------------------------------------


class TestTransitionTable:
    def test_all_states_have_at_least_one_outgoing(self) -> None:
        """Every non-terminal state must have at least one valid outgoing transition."""
        non_terminal = set(PositionStatus) - TERMINAL_STATES
        for state in non_terminal:
            outgoing = [t for t in ALLOWED_TRANSITIONS if t[0] is state]
            assert outgoing, f"State {state} has no outgoing transitions!"

    def test_terminal_states_match_constants(self) -> None:
        assert PositionStatus.CLOSED in TERMINAL_STATES
        assert PositionStatus.ORPHAN_CLOSED in TERMINAL_STATES
        assert PositionStatus.OPEN not in TERMINAL_STATES


# ---------------------------------------------------------------------------
# 9. force_transition (recovery bypass)
# ---------------------------------------------------------------------------


class TestForceTransition:
    def test_force_skips_allow_list(self) -> None:
        fsm, _ = make_fsm(PositionStatus.PENDING)
        # PENDING → TRAILING_ACTIVE is not in ALLOWED_TRANSITIONS
        assert (PositionStatus.PENDING, PositionStatus.TRAILING_ACTIVE) not in ALLOWED_TRANSITIONS
        rec = fsm.force_transition(
            PositionStatus.TRAILING_ACTIVE,
            trigger="recovery",
            reason="test forced",
        )
        assert fsm.status is PositionStatus.TRAILING_ACTIVE
        assert "[FORCED]" in rec.reason

    def test_force_records_in_trail(self) -> None:
        fsm, _ = make_fsm(PositionStatus.PENDING)
        fsm.force_transition(PositionStatus.CLOSED, trigger="recovery")
        assert len(fsm.trail) == 1
        assert fsm.trail[0].to_state == "CLOSED"
