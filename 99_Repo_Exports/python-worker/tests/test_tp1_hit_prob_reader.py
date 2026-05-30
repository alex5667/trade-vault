"""Tests for services/tp1_hit_prob_reader.py — TTL cache reader + ctx attacher."""

from __future__ import annotations

import json
import time
from types import SimpleNamespace
from typing import Any

import pytest

from services.tp1_hit_prob_reader import (
    Tp1PhitReader,
    attach_tp1_phit_to_ctx,
    reset_reader_for_tests,
)


class FakeRedis:
    """Minimal Redis double — only `.get(key)` is used by Tp1PhitReader."""

    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store: dict[str, str] = dict(store or {})
        self.calls: int = 0

    def get(self, key: str) -> str | None:
        self.calls += 1
        return self.store.get(key)


def _state_payload(*, ts_ms: int | None = None, sig: str | None = None) -> dict[str, Any]:
    return {
        "ts_ms": ts_ms if ts_ms is not None else int(time.time() * 1000),
        "window_hours": 168,
        "n_trades": 1000,
        "grid": [0.65, 1.00, 1.50],
        "min_samples": 200,
        "buckets": {
            "BTCUSDT|of|range|LONG": {
                "n_total": 350,
                "curve": {"0.65": 0.85, "1.00": 0.55, "1.50": 0.25},
                "calibration_ok": 1,
                "passes": 1,
            },
            "*|*|*|*": {
                "n_total": 9000,
                "curve": {"0.65": 0.70, "1.00": 0.45, "1.50": 0.15},
                "calibration_ok": 1,
                "passes": 1,
            },
        },
        **({"sig": sig} if sig else {}),
    }


@pytest.fixture(autouse=True)
def _reset_singleton(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_reader_for_tests()
    for k in ("TP1_PHIT_READ_ENABLED", "TP1_PHIT_HMAC_SECRET",
              "TP1_PHIT_READ_REDIS_URL", "TP1_PHIT_REDIS_URL"):
        monkeypatch.delenv(k, raising=False)
    yield
    reset_reader_for_tests()


# ---------------------------------------------------------------------------
# Reader cache behaviour
# ---------------------------------------------------------------------------


def test_reader_fetches_and_caches() -> None:
    fake = FakeRedis({"autocal:tp1_phit:state": json.dumps(_state_payload())})
    rdr = Tp1PhitReader(fake, refresh_ms=10_000)
    b1 = rdr.get_bucket(symbol="BTCUSDT", kind="of", regime="range", direction="LONG")
    b2 = rdr.get_bucket(symbol="BTCUSDT", kind="of", regime="range", direction="LONG")
    assert b1 is not None
    assert b1["curve"]["1.00"] == pytest.approx(0.55)
    assert b2 is b1 or b2 == b1
    # only one Redis hit due to TTL cache
    assert fake.calls == 1


def test_reader_fallback_to_global() -> None:
    fake = FakeRedis({"autocal:tp1_phit:state": json.dumps(_state_payload())})
    rdr = Tp1PhitReader(fake)
    b = rdr.get_bucket(symbol="DOGEUSDT", kind="of", regime="trending", direction="SHORT")
    assert b is not None
    assert b["curve"]["1.00"] == pytest.approx(0.45)


def test_reader_returns_none_when_stale() -> None:
    # ts in the past beyond stale_ms
    old = int(time.time() * 1000) - 24 * 60 * 60 * 1000
    fake = FakeRedis({"autocal:tp1_phit:state": json.dumps(_state_payload(ts_ms=old))})
    rdr = Tp1PhitReader(fake, stale_ms=60 * 1000)
    assert rdr.get_bucket(
        symbol="BTCUSDT", kind="of", regime="range", direction="LONG"
    ) is None


def test_reader_returns_none_when_empty() -> None:
    rdr = Tp1PhitReader(FakeRedis({}))
    assert rdr.get_bucket(
        symbol="BTCUSDT", kind="of", regime="range", direction="LONG"
    ) is None


def test_reader_hmac_mismatch_ignores_snapshot() -> None:
    fake = FakeRedis({"autocal:tp1_phit:state": json.dumps(_state_payload(sig="bogus"))})
    rdr = Tp1PhitReader(fake, hmac_secret="real-secret")
    assert rdr.get_bucket(
        symbol="BTCUSDT", kind="of", regime="range", direction="LONG"
    ) is None


# ---------------------------------------------------------------------------
# attach_tp1_phit_to_ctx
# ---------------------------------------------------------------------------


def test_attach_writes_curve_samples_calibration() -> None:
    fake = FakeRedis({"autocal:tp1_phit:state": json.dumps(_state_payload())})
    rdr = Tp1PhitReader(fake)
    ctx = SimpleNamespace()
    ok = attach_tp1_phit_to_ctx(
        ctx, symbol="BTCUSDT", kind="of", regime="range", direction="LONG",
        reader=rdr,
    )
    assert ok is True
    assert ctx.tp1_hit_prob_by_rr == {"0.65": 0.85, "1.00": 0.55, "1.50": 0.25}
    assert ctx.tp1_prob_samples == 350
    assert ctx.tp1_calibration_ok == 1


def test_attach_fails_when_no_reader_and_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    # TP1_PHIT_READ_ENABLED unset → singleton returns None → attach returns False
    monkeypatch.delenv("TP1_PHIT_READ_ENABLED", raising=False)
    ctx = SimpleNamespace()
    ok = attach_tp1_phit_to_ctx(
        ctx, symbol="BTCUSDT", kind="of", regime="range", direction="LONG",
    )
    assert ok is False
    assert getattr(ctx, "tp1_hit_prob_by_rr", None) is None


def test_attach_fails_when_no_matching_bucket() -> None:
    payload = _state_payload()
    # No global bucket and no BTC match → no fallback hits
    payload["buckets"] = {
        "ETHUSDT|of|range|LONG": payload["buckets"]["BTCUSDT|of|range|LONG"],
    }
    fake = FakeRedis({"autocal:tp1_phit:state": json.dumps(payload)})
    rdr = Tp1PhitReader(fake)
    ctx = SimpleNamespace()
    ok = attach_tp1_phit_to_ctx(
        ctx, symbol="DOGEUSDT", kind="of", regime="range", direction="LONG",
        reader=rdr,
    )
    assert ok is False
    assert getattr(ctx, "tp1_hit_prob_by_rr", None) is None


def test_attach_drops_invalid_curve_entries() -> None:
    payload = _state_payload()
    payload["buckets"]["BTCUSDT|of|range|LONG"]["curve"] = {
        "0.65": 0.9, "1.00": "bad", "1.50": 1.5,  # bad parse + out of range
    }
    fake = FakeRedis({"autocal:tp1_phit:state": json.dumps(payload)})
    rdr = Tp1PhitReader(fake)
    ctx = SimpleNamespace()
    ok = attach_tp1_phit_to_ctx(
        ctx, symbol="BTCUSDT", kind="of", regime="range", direction="LONG",
        reader=rdr,
    )
    assert ok is True
    assert ctx.tp1_hit_prob_by_rr == {"0.65": 0.9}
