from __future__ import annotations

"""
Unit tests for FreezePromotionService.

All tests are pure-function (no Redis). AsyncMock is used for Redis-bound tests.
"""
import json
from unittest.mock import AsyncMock

import pytest

from core.entry_policy_freeze import EntryPolicyFreezeV1
from services.entry_policy_freeze_promoter import FreezePromotionService, _promotion_decision
from utils.time_utils import get_ny_time_millis

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return get_ny_time_millis()


def _make_fz(
    *,
    mode: str = "shadow",
    created_offset_ms: int = -700_000,  # 700s ago by default
    promoted_ts_ms: int = 0,
    promoted_reason: str = "",
) -> EntryPolicyFreezeV1:
    now = _now_ms()
    return EntryPolicyFreezeV1(
        ver=1,
        symbol="BTCUSDT",
        group="default",
        scenario="reversal",
        until_ts_ms=now + 3_600_000,  # active for 1h
        mode=mode,
        reason_code="DATA_BAD",
        src="cb_v1",
        created_ts_ms=now + created_offset_ms,
        promoted_ts_ms=promoted_ts_ms,
        promoted_reason=promoted_reason,
    )


def _stats(
    blocked: int = 8,
    seen: int = 15,
    spread_z: float = 3.5,
    obi_age: float = 1800.0,
    pressure: float = 1.5,
) -> dict:
    return {
        "blocked_count": str(blocked),
        "seen_count": str(seen),
        "last_spread_z": str(spread_z),
        "last_obi_age_ms": str(obi_age),
        "last_pressure_sps": str(pressure),
    }


THR = dict(
    observe_ms=600_000,
    min_blocked=5,
    min_seen=10,
    bad_cnt_needed=2,
    thr_spread_z=3.0,
    thr_obi_age_ms=1500.0,
    thr_pressure=1.4,
)

# ---------------------------------------------------------------------------
# _promotion_decision – pure function tests
# ---------------------------------------------------------------------------


def test_promote_triggers_when_conditions_met():
    """Should promote when observe window passed, enough blocks, metrics bad."""
    fz = _make_fz(created_offset_ms=-700_000)
    ok, reason = _promotion_decision(fz=fz, stats=_stats(), now_ms=_now_ms(), **THR)
    assert ok is True
    assert "blocked" in reason
    assert "seen" in reason


def test_no_promote_observe_window_not_elapsed():
    """Should NOT promote if still within observation window."""
    fz = _make_fz(created_offset_ms=-100_000)  # only 100s ago
    ok, reason = _promotion_decision(fz=fz, stats=_stats(), now_ms=_now_ms(), **THR)
    assert ok is False
    assert "observe_window_not_elapsed" in reason


def test_no_promote_already_hard():
    """Should NOT promote a freeze that is already hard."""
    fz = _make_fz(mode="hard")
    ok, reason = _promotion_decision(fz=fz, stats=_stats(), now_ms=_now_ms(), **THR)
    assert ok is False
    assert "already_hard" in reason


def test_no_promote_already_promoted():
    """Should NOT promote again if promoted_ts_ms already set."""
    fz = _make_fz(promoted_ts_ms=_now_ms() - 5000, promoted_reason="prev")
    ok, reason = _promotion_decision(fz=fz, stats=_stats(), now_ms=_now_ms(), **THR)
    assert ok is False
    assert "already_promoted" in reason


def test_no_promote_not_enough_seen():
    """Should NOT promote when seen_count < min_seen."""
    fz = _make_fz()
    ok, reason = _promotion_decision(fz=fz, stats=_stats(seen=5), now_ms=_now_ms(), **THR)
    assert ok is False
    assert "not_enough_seen" in reason


def test_no_promote_not_enough_blocked():
    """Should NOT promote when blocked_count < min_blocked."""
    fz = _make_fz()
    ok, reason = _promotion_decision(fz=fz, stats=_stats(blocked=2), now_ms=_now_ms(), **THR)
    assert ok is False
    assert "not_enough_blocked" in reason


def test_no_promote_metrics_recovered():
    """Should NOT promote when market metrics have recovered."""
    fz = _make_fz()
    good_stats = _stats(spread_z=1.0, obi_age=500.0, pressure=0.5)
    ok, reason = _promotion_decision(fz=fz, stats=good_stats, now_ms=_now_ms(), **THR)
    assert ok is False
    assert "metrics_recovered" in reason


def test_no_promote_freeze_expired():
    """Should NOT promote an already-expired freeze."""
    fz = _make_fz()
    fz.until_ts_ms = _now_ms() - 1000  # expired
    ok, reason = _promotion_decision(fz=fz, stats=_stats(), now_ms=_now_ms(), **THR)
    assert ok is False
    assert "expired" in reason


def test_promote_reason_includes_metrics():
    """Promotion reason should contain block counts and metric values."""
    fz = _make_fz()
    ok, reason = _promotion_decision(fz=fz, stats=_stats(blocked=7, seen=20), now_ms=_now_ms(), **THR)
    assert ok is True
    assert "blocked_7" in reason
    assert "seen_20" in reason


# ---------------------------------------------------------------------------
# freeze schema – promoted fields round-trip
# ---------------------------------------------------------------------------


def test_freeze_promoted_fields_roundtrip():
    """promoted_ts_ms and promoted_reason should survive JSON round-trip."""
    fz = EntryPolicyFreezeV1(
        ver=1,
        symbol="ETHUSDT",
        group="default",
        scenario="continuation",
        until_ts_ms=_now_ms() + 3_600_000,
        mode="hard",
        src="freeze_promoter",
        created_ts_ms=_now_ms() - 700_000,
        promoted_ts_ms=_now_ms(),
        promoted_reason="blocked_8_seen_15_bad_cnt_2",
    )
    raw = fz.to_json()
    fz2, err = EntryPolicyFreezeV1.from_json(raw)
    assert err == ""
    assert fz2.promoted_ts_ms == fz.promoted_ts_ms
    assert fz2.promoted_reason == fz.promoted_reason


def test_freeze_backward_compat_no_promoted_fields():
    """Old freeze JSON (no promoted_* fields) should parse with defaults."""
    raw = json.dumps({
        "ver": 1, "symbol": "BTCUSDT", "group": "default",
        "scenario": "reversal", "until_ts_ms": _now_ms() + 3_600_000,
        "mode": "shadow",
    })
    fz, err = EntryPolicyFreezeV1.from_json(raw)
    assert err == ""
    assert fz.promoted_ts_ms == 0
    assert fz.promoted_reason == ""


# ---------------------------------------------------------------------------
# FreezePromotionService._do_promote – integration stub
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_do_promote_writes_hard_to_redis():
    """_do_promote should write mode=hard JSON to Redis and publish event."""
    svc = FreezePromotionService.__new__(FreezePromotionService)
    svc.r = AsyncMock()
    svc.r.set = AsyncMock()
    svc.r.xadd = AsyncMock()
    svc.ops_stream = "ops:eventlog"

    fz = _make_fz(mode="shadow")
    now = _now_ms()
    await svc._do_promote(fkey="cfg:entry_policy:freeze:v1:BTCUSDT:default:reversal",
                          fz=fz, reason="blocked_8_seen_15_bad_cnt_2", now_ms=now)

    assert svc.r.set.called, "Redis SET should be called"
    call_args = svc.r.set.call_args
    stored_json = call_args.args[1]
    stored = json.loads(stored_json)
    assert stored["mode"] == "hard"
    assert stored["promoted_ts_ms"] == now
    assert "blocked_8" in stored["promoted_reason"]

    assert svc.r.xadd.called, "ops:eventlog xadd should be called"
    xadd_args = svc.r.xadd.call_args
    event_obj = json.loads(xadd_args.args[1]["event"])
    assert event_obj["type"] == "freeze_promoted_hard"
    assert event_obj["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_fail_open_redis_set_error():
    """If Redis SET fails, _do_promote should not raise and should not xadd."""
    svc = FreezePromotionService.__new__(FreezePromotionService)
    svc.r = AsyncMock()
    svc.r.set = AsyncMock(side_effect=Exception("redis_timeout"))
    svc.r.xadd = AsyncMock()

    fz = _make_fz(mode="shadow")
    # Should not raise
    await svc._do_promote(fkey="cfg:entry_policy:freeze:v1:BTCUSDT:default:reversal",
                          fz=fz, reason="blocked_5_seen_12_bad_cnt_2", now_ms=_now_ms())
    # xadd should NOT be called (write failed)
    assert not svc.r.xadd.called
