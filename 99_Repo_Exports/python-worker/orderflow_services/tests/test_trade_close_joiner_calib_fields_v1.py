from __future__ import annotations

"""
Test: Calibration fields propagation through trade_close_joiner_worker_v5.

Verifies that calibration/shadow fields (calib, calib_kind, candidate_window_ms, etc.)
are correctly transferred from close_payload / signal_payload / decision
into the trades:closed output.
"""


import json
from typing import Any

import pytest

# ----------------------------------------------------------------
# Minimal stubs
# ----------------------------------------------------------------

_xadd_calls: list[dict[str, Any]] = []


class FakeRedis:
    """Async Redis stub that captures xadd writes."""

    def __init__(self, decision: dict[str, Any] | None = None):
        self._decision = decision or {}
        self._keys: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        if key.startswith("decision:"):
            return json.dumps(self._decision) if self._decision else None
        if key.startswith("dedup:"):
            return None  # never dedup in tests
        return self._keys.get(key)

    async def set(self, key: str, val: Any, **kw) -> bool:
        self._keys[key] = str(val)
        return True

    async def xadd(self, stream: str, fields: dict[str, Any], *, maxlen: int = 0, approximate: bool = False) -> str:
        _xadd_calls.append({"stream": stream, "fields": fields})
        return "1-0"

    async def xrange(self, *a, **kw):
        return []

    async def xrevrange(self, *a, **kw):
        return []


# ----------------------------------------------------------------
# Import the unit-under-test
# ----------------------------------------------------------------

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, "/home/alex/front/trade/scanner_infra/python-worker")

from services.orderflow.tools.trade_close_joiner_worker_v5 import _handle_close
from core.redis_keys import RedisStreams as RS

# ----------------------------------------------------------------
# Helper
# ----------------------------------------------------------------

def _build_kwargs(**overrides):
    defaults = dict(
        decision_prefix="decision:",
        trades_closed_stream=RS.TRADES_CLOSED,
        trades_closed_maxlen=100000,
        close_wait_stream=RS.TRADES_CLOSE_WAIT,
        close_wait_maxlen=100000,
        dedup_ttl_sec=86400,
        ml_replay_stream=RS.ML_REPLAY_INPUTS,
        ml_replay_maxlen=100000,
        write_ml_replay=False,
        of_inputs_stream=RS.OF_INPUTS,
        of_inputs_field="payload",
        of_inputs_sid_index_prefix="idx:of_inputs:sid:",
        of_inputs_scan_count=5000,
    )
    defaults.update(overrides)
    return defaults


# ----------------------------------------------------------------
# Tests
# ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_calib_fields_from_close_payload():
    """Calibration fields in close_payload are forwarded to trades:closed."""

    _xadd_calls.clear()

    decision = {
        "sid": "SIG001",
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "dq_state": 0,
        "drift_state": 0,
    }
    close = {
        "sid": "SIG001",
        "event": "POSITION_CLOSED",
        "position_id": "POS001",
        "close_ts_ms": 1700000000000,
        "pnl_usd": 5.0,
        # Direct calibration fields
        "calib": 1,
        "calib_kind": "cont_ctx_window",
        "calib_run_id": "run_abc123",
        "candidate_window_ms": 180000,
        "baseline_window_ms": 120000,
        "cont_ctx_age_ms": 95000,
        "paper_only": 1,
        "is_virtual": 1,
        "entry_reason": "cont_ctx_rescued",
        "parent_signal_id": "SIG000",
    }

    r = FakeRedis(decision=decision)
    ok, reason = await _handle_close(r, close, **_build_kwargs())
    assert ok is True
    assert reason == "ok"

    # Find the trades:closed write
    tc = [c for c in _xadd_calls if c["stream"] == RS.TRADES_CLOSED]
    assert len(tc) == 1

    payload = json.loads(tc[0]["fields"]["payload"])
    assert payload["calib"] == 1
    assert payload["calib_kind"] == "cont_ctx_window"
    assert payload["calib_run_id"] == "run_abc123"
    assert payload["candidate_window_ms"] == 180000
    assert payload["baseline_window_ms"] == 120000
    assert payload["cont_ctx_age_ms"] == 95000
    assert payload["paper_only"] == 1
    assert payload["is_virtual"] == 1
    assert payload["entry_reason"] == "cont_ctx_rescued"
    assert payload["parent_signal_id"] == "SIG000"


@pytest.mark.asyncio
async def test_calib_fields_from_signal_payload_nested():
    """Calibration fields inside signal_payload (nested dict) are forwarded."""

    _xadd_calls.clear()

    decision = {
        "sid": "SIG002",
        "symbol": "ETHUSDT",
        "direction": "SHORT",
        "dq_state": 0,
        "drift_state": 0,
        # signal_payload in decision with calib fields
        "signal_payload": {
            "calib": 1,
            "calib_kind": "cont_ctx_window",
            "calib_run_id": "run_xyz789",
            "candidate_window_ms": 150000,
            "baseline_window_ms": 120000,
            "shadow_only": 1,
        },
    }
    close = {
        "sid": "SIG002",
        "event": "POSITION_CLOSED",
        "position_id": "POS002",
        "close_ts_ms": 1700000001000,
        "pnl_usd": -2.0,
        # signal_payload in close as JSON string
        "signal_payload": json.dumps({
            "cont_ctx_age_ms": 130000,
            "entry_reason": "shadow_near_miss",
        }),
    }

    r = FakeRedis(decision=decision)
    ok, reason = await _handle_close(r, close, **_build_kwargs())
    assert ok is True

    tc = [c for c in _xadd_calls if c["stream"] == RS.TRADES_CLOSED]
    assert len(tc) == 1

    payload = json.loads(tc[0]["fields"]["payload"])

    # From close's signal_payload (highest priority for nested)
    assert payload["cont_ctx_age_ms"] == 130000
    assert payload["entry_reason"] == "shadow_near_miss"

    # From decision's signal_payload (fallback)
    assert payload["calib"] == 1
    assert payload["calib_kind"] == "cont_ctx_window"
    assert payload["calib_run_id"] == "run_xyz789"
    assert payload["candidate_window_ms"] == 150000
    assert payload["shadow_only"] == 1


@pytest.mark.asyncio
async def test_no_calib_fields_when_absent():
    """When no calibration fields exist, they are NOT injected (no nulls/defaults)."""

    _xadd_calls.clear()

    decision = {
        "sid": "SIG003",
        "symbol": "SOLUSDT",
        "direction": "LONG",
        "dq_state": 0,
        "drift_state": 0,
    }
    close = {
        "sid": "SIG003",
        "event": "POSITION_CLOSED",
        "position_id": "POS003",
        "close_ts_ms": 1700000002000,
        "pnl_usd": 1.0,
    }

    r = FakeRedis(decision=decision)
    ok, _ = await _handle_close(r, close, **_build_kwargs())
    assert ok is True

    tc = [c for c in _xadd_calls if c["stream"] == RS.TRADES_CLOSED]
    payload = json.loads(tc[0]["fields"]["payload"])

    # None of the calib fields should be present
    for fld in ("calib", "calib_kind", "calib_run_id", "candidate_window_ms",
                "baseline_window_ms", "paper_only", "shadow_only", "cont_ctx_age_ms"):
        assert fld not in payload, f"Unexpected field {fld} in payload"
