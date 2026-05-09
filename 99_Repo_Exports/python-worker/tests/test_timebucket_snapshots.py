from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from domain.timebucket_snapshots import maybe_snapshot_time_buckets


class FakeSpec:
    def __init__(self) -> None:
        self.calls = []

    # IMPORTANT: match the real signature used in domain/handlers.py
    def pnl_money(self, entry_price: float, price: float, lot: float, direction: Any, *, symbol: str) -> float:
        self.calls.append((entry_price, price, lot, direction, symbol))
        # deterministic: positive if favorable, negative otherwise is irrelevant for snapshot writer
        return float(abs(price - entry_price) * abs(lot))


@dataclass
class FakePos:
    entry_ts_ms: int = 0
    entry_price: float = 100.0
    lot: float = 2.0
    direction: str = "long"
    symbol: str = "BTCUSDT"

    max_price_seen: float = 110.0
    min_price_seen: float = 90.0

    mfe_pnl: float | None = 20.0
    mae_pnl: float | None = 20.0

    mfe_pnl_t: dict[int, float] = field(default_factory=dict)
    mae_pnl_t: dict[int, float] = field(default_factory=dict)

    def is_long(self) -> bool:
        return str(self.direction).lower() in {"long", "buy"}


class Pos:
    def __init__(self):
        self.entry_ts_ms = 1_000_000
        self.entry_price = 100.0
        self.lot = 1.0
        self.direction = "long"
        self.symbol = "BTCUSDT"
        self.max_price_seen = 100.0
        self.min_price_seen = 100.0

    def is_long(self):
        return True


class Closed:
    pass


def test_snapshot_uses_existing_mfe_mae_without_calling_pnl_money(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EMP_TIME_SNAPSHOTS_ENABLED", "1")
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "1,2")

    spec = FakeSpec()
    pos = FakePos(entry_ts_ms=1_000)

    # after 1 minute -> bucket 60_000 should be stored
    maybe_snapshot_time_buckets(pos, ts_ms=1_000 + 60_000 + 1, spec=spec)

    assert 60_000 in pos.mfe_pnl_t
    assert 60_000 in pos.mae_pnl_t
    assert pos.mfe_pnl_t[60_000] == float(pos.mfe_pnl)
    assert pos.mae_pnl_t[60_000] == float(pos.mae_pnl)
    assert spec.calls == []


def test_snapshot_falls_back_to_signature_correct_pnl_money(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EMP_TIME_SNAPSHOTS_ENABLED", "1")
    monkeypatch.setenv("EMP_TIME_BUCKETS_MINUTES", "1")

    spec = FakeSpec()
    pos = FakePos(entry_ts_ms=1_000)
    # remove precomputed values to force fallback
    pos.mfe_pnl = None
    pos.mae_pnl = None

    maybe_snapshot_time_buckets(pos, ts_ms=1_000 + 60_000 + 1, spec=spec)

    assert 60_000 in pos.mfe_pnl_t
    assert 60_000 in pos.mae_pnl_t
    # pnl_money must be called twice (mfe + mae) with correct signature
    assert len(spec.calls) == 2
    for (entry, price, lot, direction, symbol) in spec.calls:
        assert entry == pos.entry_price
        assert lot == pos.lot
        assert direction == pos.direction
        assert symbol == pos.symbol


def test_snapshots_attached_to_closed_as_flat_fields():
    from domain.timebucket_snapshots import attach_timebucket_snapshots_to_closed

    pos = FakePos()
    pos.mfe_pnl_t = {60_000: 10.0, 120_000: 20.0}
    pos.mae_pnl_t = {60_000: -5.0, 120_000: -8.0}

    @dataclass
    class FakeClosed:
        pass

    closed = FakeClosed()
    attach_timebucket_snapshots_to_closed(pos, closed)
    assert closed.mfe_pnl_t60000 == 10.0
    assert closed.mae_pnl_t60000 == -5.0
    assert closed.mfe_pnl_t120000 == 20.0
    assert closed.mae_pnl_t120000 == -8.0
