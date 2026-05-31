"""Tests for services.signal_min_conf_applied_delta_reader."""

from __future__ import annotations

import hashlib
import hmac
import json
import time

import fakeredis
import pytest

from services.signal_min_conf_applied_delta_reader import (
    AppliedDeltaReader,
    _make_reader,
    get_reader,
)


@pytest.fixture(autouse=True)
def _reset_singleton():
    import services.signal_min_conf_applied_delta_reader as mod

    mod._READER = None
    yield
    mod._READER = None


@pytest.fixture
def r() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=False)


def _payload(
    *,
    delta: float = -0.02,
    floor: float = 0.30,
    ceiling: float = 0.85,
    phase: str = "RELAX_APPLIED",
    ts_ms: int | None = None,
    secret: str = "",
) -> str:
    p = {
        "schema_version": 1,
        "ts_ms": ts_ms if ts_ms is not None else int(time.time() * 1000),
        "group_key": "edge_stack_v1|BTCUSDT|1800000",
        "phase": phase,
        "min_conf_delta": delta,
        "min_conf_floor": floor,
        "min_conf_ceiling": ceiling,
        "reason": "test",
        "llm_summary": "",
    }
    if secret:
        body = json.dumps(
            {k: v for k, v in p.items() if k != "sig"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        p["sig"] = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return json.dumps(p)


def _key(reader: AppliedDeltaReader, kind: str, symbol: str, horizon_ms: int) -> str:
    return reader._redis_key(reader._build_group_key(kind, symbol, horizon_ms))


def test_get_delta_happy_path(r):
    reader = AppliedDeltaReader(r, refresh_ms=5_000, stale_ms=600_000)
    r.set(
        _key(reader, "edge_stack_v1", "BTCUSDT", 1_800_000),
        _payload(delta=-0.02),
    )
    out = reader.get_delta(kind="edge_stack_v1", symbol="BTCUSDT", horizon_ms=1_800_000)
    assert out is not None
    assert out.min_conf_delta == -0.02
    assert out.min_conf_floor == 0.30
    assert out.min_conf_ceiling == 0.85
    assert out.phase == "RELAX_APPLIED"


def test_get_delta_missing_key_returns_none(r):
    reader = AppliedDeltaReader(r)
    assert reader.get_delta(kind="x", symbol="Y", horizon_ms=1) is None


def test_get_delta_zero_delta_treated_as_none(r):
    reader = AppliedDeltaReader(r, refresh_ms=5_000)
    r.set(
        _key(reader, "k", "BTCUSDT", 60_000),
        _payload(delta=0.0),
    )
    assert reader.get_delta(kind="k", symbol="BTCUSDT", horizon_ms=60_000) is None


def test_get_delta_paranoid_cap_rejects_huge_delta(r):
    reader = AppliedDeltaReader(r)
    r.set(_key(reader, "k", "B", 1), _payload(delta=0.95))
    assert reader.get_delta(kind="k", symbol="B", horizon_ms=1) is None


def test_get_delta_bad_floor_ceiling_rejected(r):
    reader = AppliedDeltaReader(r)
    r.set(_key(reader, "k", "B", 1), _payload(floor=0.90, ceiling=0.30))
    assert reader.get_delta(kind="k", symbol="B", horizon_ms=1) is None


def test_get_delta_stale_payload_returns_none(r):
    # Constructor enforces stale_ms >= refresh_ms (min 5000). Make the
    # payload's ts_ms much older than both so the stale guard fires.
    reader = AppliedDeltaReader(r, refresh_ms=5_000, stale_ms=10_000)
    old_ts = int(time.time() * 1000) - 24 * 3_600_000  # 24h ago
    r.set(_key(reader, "k", "B", 1), _payload(delta=-0.02, ts_ms=old_ts))
    assert reader.get_delta(kind="k", symbol="B", horizon_ms=1) is None


def test_get_delta_hmac_required_when_secret_set(r):
    secret = "topsecret"
    # Reader A: cache locked on missing-sig payload
    reader_a = AppliedDeltaReader(r, hmac_secret=secret)
    r.set(_key(reader_a, "k", "B", 1), _payload(delta=-0.02))
    assert reader_a.get_delta(kind="k", symbol="B", horizon_ms=1) is None
    # Reader B: fresh cache, correct sig → accept
    reader_b = AppliedDeltaReader(r, hmac_secret=secret)
    r.set(_key(reader_b, "k", "B", 1), _payload(delta=-0.02, secret=secret))
    out = reader_b.get_delta(kind="k", symbol="B", horizon_ms=1)
    assert out is not None and out.min_conf_delta == -0.02


def test_get_delta_hmac_mismatch_rejected(r):
    reader = AppliedDeltaReader(r, hmac_secret="real")
    r.set(_key(reader, "k", "B", 1), _payload(delta=-0.02, secret="wrong"))
    assert reader.get_delta(kind="k", symbol="B", horizon_ms=1) is None


def test_get_delta_empty_kind_symbol_returns_none(r):
    reader = AppliedDeltaReader(r)
    assert reader.get_delta(kind="", symbol="BTCUSDT", horizon_ms=1) is None
    assert reader.get_delta(kind="k", symbol="", horizon_ms=1) is None


def test_get_delta_caches_within_refresh_window(r):
    reader = AppliedDeltaReader(r, refresh_ms=60_000)
    r.set(_key(reader, "k", "BTCUSDT", 1), _payload(delta=-0.02))
    out1 = reader.get_delta(kind="k", symbol="BTCUSDT", horizon_ms=1)
    assert out1 is not None
    # Mutate Redis — cache should still return the old value
    r.set(_key(reader, "k", "BTCUSDT", 1), _payload(delta=-0.04))
    out2 = reader.get_delta(kind="k", symbol="BTCUSDT", horizon_ms=1)
    assert out2 is not None and out2.min_conf_delta == -0.02


def test_get_delta_refreshes_after_window(r):
    reader = AppliedDeltaReader(r, refresh_ms=5_000)
    r.set(_key(reader, "k", "B", 1), _payload(delta=-0.02))
    out1 = reader.get_delta(kind="k", symbol="B", horizon_ms=1)
    assert out1 is not None
    # Roll cache clock back so refresh window has elapsed
    with reader._lock:
        for k in reader._cache:
            reader._cache[k] = type(reader._cache[k])(
                delta=reader._cache[k].delta, fetched_ms=0
            )
    r.set(_key(reader, "k", "B", 1), _payload(delta=-0.04))
    out2 = reader.get_delta(kind="k", symbol="B", horizon_ms=1)
    assert out2 is not None and out2.min_conf_delta == -0.04


def test_get_delta_garbage_payload_returns_none(r):
    reader = AppliedDeltaReader(r)
    r.set(_key(reader, "k", "B", 1), b"not-json")
    assert reader.get_delta(kind="k", symbol="B", horizon_ms=1) is None


def test_singleton_disabled_by_default(monkeypatch):
    monkeypatch.delenv("AUTOCAL_APPLIED_DELTA_READ_ENABLED", raising=False)
    assert _make_reader() is None
    assert get_reader() is None


def test_build_group_key_normalises_symbol_case():
    # static method — no Redis needed
    assert AppliedDeltaReader._build_group_key("k", "btcusdt", 60_000) == "k|BTCUSDT|60000"
    assert AppliedDeltaReader._build_group_key("", "x", 1) == ""
    assert AppliedDeltaReader._build_group_key("k", "", 1) == ""
