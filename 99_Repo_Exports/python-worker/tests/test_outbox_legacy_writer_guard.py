"""F1 regression guard: legacy flat-shape OutboxWriter must not silently write
to the canonical SIGNAL_OUTBOX stream — SignalDispatcher expects `{"data": JSON}`
and will silently DLQ flat envelopes produced by `OutboxEnvelope.to_stream_fields()`.

These tests pin two safeguards:
  1. `OUTBOX_LEGACY_FLAT_WRITE_TOTAL` increments whenever the legacy writer
     targets `RS.SIGNAL_OUTBOX`, so ops can detect drift in Prometheus.
  2. `OUTBOX_LEGACY_WRITER_BLOCK=1` hard-refuses the write (no XADD attempted)
     before any silent-loss path is reached.

If you delete or rename the F1 guard, also remove these tests *and* update
docs in CLAUDE.md (Outbox Data Contracts).
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from core.outbox_envelope import OutboxEnvelope


def _make_env(signal_id: str = "sig-f1-001") -> OutboxEnvelope:
    return OutboxEnvelope(
        signal_id=signal_id,
        kind="of_confirm",
        symbol="BTCUSDT",
        side="LONG",
        ts_ms=1_700_000_000_000,
        payload={"price": 30000.0},
    )


def _fake_redis_success() -> MagicMock:
    r = MagicMock()
    r.set.return_value = True
    r.setnx.return_value = 1
    r.xadd.return_value = b"1700000000000-0"
    return r


def test_block_mode_refuses_xadd_to_signal_outbox(monkeypatch: pytest.MonkeyPatch) -> None:
    """OUTBOX_LEGACY_WRITER_BLOCK=1 must return ok=False and skip XADD entirely."""
    monkeypatch.setenv("OUTBOX_LEGACY_WRITER_BLOCK", "1")
    from core.outbox_writer import OutboxWriter

    fake = _fake_redis_success()
    w = OutboxWriter(redis=fake, logger=MagicMock())

    res = w.write(_make_env())

    assert res.ok is False, "block mode must return ok=False"
    assert res.written is False
    assert fake.xadd.call_count == 0, (
        "block mode must NOT XADD; otherwise legacy flat shape silently lands in canonical outbox"
    )


def test_unblocked_mode_increments_legacy_counter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default (unblocked) mode allows the write but increments the drift counter."""
    monkeypatch.delenv("OUTBOX_LEGACY_WRITER_BLOCK", raising=False)
    from core.outbox_writer import OUTBOX_LEGACY_FLAT_WRITE_TOTAL, OutboxWriter
    from core.redis_keys import RedisStreams as RS

    sample = OUTBOX_LEGACY_FLAT_WRITE_TOTAL.labels(stream=RS.SIGNAL_OUTBOX, blocked="0")
    before = sample._value.get()

    fake = _fake_redis_success()
    w = OutboxWriter(redis=fake, logger=MagicMock())
    w.write(_make_env(signal_id="sig-f1-002"))

    after = sample._value.get()
    assert after == before + 1, (
        f"OUTBOX_LEGACY_FLAT_WRITE_TOTAL{{stream=SIGNAL_OUTBOX,blocked=0}} did not increment "
        f"(before={before}, after={after}). Drift detection broken — ops will not see legacy writes."
    )


def test_unblocked_mode_still_attempts_xadd(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unblocked path must still XADD (backwards-compat) — the counter is the alert,
    not the hard stop."""
    monkeypatch.delenv("OUTBOX_LEGACY_WRITER_BLOCK", raising=False)
    from core.outbox_writer import OutboxWriter

    fake = _fake_redis_success()
    w = OutboxWriter(redis=fake, logger=MagicMock())
    res = w.write(_make_env(signal_id="sig-f1-003"))

    assert fake.xadd.call_count == 1
    assert res.written is True
    assert res.entry_id is not None


def test_block_mode_does_not_increment_with_blocked_label(monkeypatch: pytest.MonkeyPatch) -> None:
    """In blocked mode the counter must increment with blocked=1 — so dashboards
    can distinguish silent legacy traffic from explicit refusals."""
    monkeypatch.setenv("OUTBOX_LEGACY_WRITER_BLOCK", "1")
    from core.outbox_writer import OUTBOX_LEGACY_FLAT_WRITE_TOTAL, OutboxWriter
    from core.redis_keys import RedisStreams as RS

    sample = OUTBOX_LEGACY_FLAT_WRITE_TOTAL.labels(stream=RS.SIGNAL_OUTBOX, blocked="1")
    before = sample._value.get()

    fake = _fake_redis_success()
    w = OutboxWriter(redis=fake, logger=MagicMock())
    w.write(_make_env(signal_id="sig-f1-004"))

    after = sample._value.get()
    assert after == before + 1
