"""Tests for services.edge_directional_bias_overrides."""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from unittest.mock import MagicMock

import pytest

from services import edge_directional_bias_overrides as edb_ov


@pytest.fixture(autouse=True)
def _reset_reader():
    edb_ov.reset_reader_for_tests()
    yield
    edb_ov.reset_reader_for_tests()


def _build_snapshot(
    *, buckets: dict, ts_ms: int | None = None, secret: str = ""
) -> str:
    payload = {
        "schema_version": 1,
        "ts_ms": ts_ms if ts_ms is not None else int(time.time() * 1000),
        "buckets": buckets,
    }
    if secret:
        canon = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        payload["sig"] = hmac.new(secret.encode(), canon, hashlib.sha256).hexdigest()
    return json.dumps(payload)


def _reader(redis_client, **kw) -> edb_ov.EdgeDirectionalBiasReader:
    return edb_ov.EdgeDirectionalBiasReader(redis_client, refresh_ms=1000, stale_ms=3_600_000, **kw)


# ─────────────────────────────────────────────────────────────────────


def test_returns_none_when_disabled(monkeypatch):
    monkeypatch.delenv("AUTOCAL_EDGE_DIRECTIONAL_BIAS_READ_ENABLED", raising=False)
    edb_ov.reset_reader_for_tests()
    out = edb_ov.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    assert out is None


def test_returns_none_when_not_countertrend():
    redis_client = MagicMock()
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "CANARY_LOW", "bias_value": 0.03}}
    )
    r = _reader(redis_client)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=False)
    assert out is None


def test_returns_none_when_phase_observe():
    redis_client = MagicMock()
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "OBSERVE", "bias_value": 0.0}}
    )
    r = _reader(redis_client)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    assert out is None


def test_returns_bias_when_phase_canary_low():
    redis_client = MagicMock()
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "CANARY_LOW", "bias_value": 0.03}}
    )
    r = _reader(redis_client)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    assert out == pytest.approx(0.03)


def test_returns_zero_when_phase_rolled_back():
    redis_client = MagicMock()
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "ROLLED_BACK", "bias_value": 0.0}}
    )
    r = _reader(redis_client)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    assert out == pytest.approx(0.0)


def test_returns_none_when_bucket_missing():
    redis_client = MagicMock()
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "CANARY_LOW", "bias_value": 0.03}}
    )
    r = _reader(redis_client)
    out = r.get_bias_override(direction="LONG", regime="range", countertrend=True)
    assert out is None


def test_regime_alias_normalisation():
    redis_client = MagicMock()
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "CANARY_LOW", "bias_value": 0.03}}
    )
    r = _reader(redis_client)
    # Calibrator stores "trending_bull"; caller may pass "uptrend" — alias map handles it.
    out = r.get_bias_override(direction="SHORT", regime="uptrend", countertrend=True)
    assert out == pytest.approx(0.03)


def test_returns_none_when_snapshot_stale():
    redis_client = MagicMock()
    # ts_ms 10h in the past — stale_ms default in fixture is 1h
    very_old_ts = int(time.time() * 1000) - 10 * 3_600_000
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "CANARY_LOW", "bias_value": 0.03}},
        ts_ms=very_old_ts,
    )
    r = _reader(redis_client)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    assert out is None


def test_hmac_verification_accepts_signed_snapshot():
    secret = "shhhh"
    redis_client = MagicMock()
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "CANARY_LOW", "bias_value": 0.03}},
        secret=secret,
    )
    r = _reader(redis_client, hmac_secret=secret)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    assert out == pytest.approx(0.03)


def test_hmac_verification_rejects_tampered_snapshot():
    secret = "shhhh"
    redis_client = MagicMock()
    raw = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "CANARY_LOW", "bias_value": 0.03}},
        secret=secret,
    )
    # Tamper: change bias_value
    payload = json.loads(raw)
    payload["buckets"]["SHORT|trending_bull"]["bias_value"] = 0.99
    redis_client.get.return_value = json.dumps(payload)
    r = _reader(redis_client, hmac_secret=secret)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    # HMAC fail → snapshot ignored → no override
    assert out is None


def test_fail_open_on_redis_error():
    redis_client = MagicMock()
    redis_client.get.side_effect = RuntimeError("boom")
    r = _reader(redis_client)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    assert out is None


def test_fail_open_on_corrupt_json():
    redis_client = MagicMock()
    redis_client.get.return_value = "{not json"
    r = _reader(redis_client)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    assert out is None


def test_clamps_extreme_bias_values():
    redis_client = MagicMock()
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "CANARY_HIGH", "bias_value": 99.0}}
    )
    r = _reader(redis_client)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    # Defence-in-depth clamp at 0.20
    assert out == pytest.approx(0.20)


def test_snapshot_meta_age():
    redis_client = MagicMock()
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "CANARY_LOW", "bias_value": 0.03}}
    )
    r = _reader(redis_client)
    meta = r.get_snapshot_meta()
    assert meta["fresh"] is True
    assert meta["buckets_n"] == 1
    assert meta["age_ms"] is not None and meta["age_ms"] >= 0


# ─────────────────────────────────────────────────────────────────────
# Reader-side observability — proves the audit-suggested loop-monitoring
# metrics fire on every code path. Without these you can't tell "autocal
# published a bias but the hot path never read it" from the outside.
# ─────────────────────────────────────────────────────────────────────


def _read_counter(result: str) -> float:
    c = edb_ov._override_read_total
    if c is None:
        return 0.0
    try:
        # prometheus_client exposes _value.get on the labeled child.
        return c.labels(result=result)._value.get()  # type: ignore[attr-defined]
    except Exception:
        return 0.0


def test_metric_increments_on_hit():
    if edb_ov._override_read_total is None:
        pytest.skip("prometheus_client unavailable")
    before = _read_counter("hit")
    redis_client = MagicMock()
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "CANARY_LOW", "bias_value": 0.03}}
    )
    r = _reader(redis_client)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    assert out == pytest.approx(0.03)
    assert _read_counter("hit") >= before + 1


def test_metric_increments_on_observe_phase():
    if edb_ov._override_read_total is None:
        pytest.skip("prometheus_client unavailable")
    before = _read_counter("observe_phase")
    redis_client = MagicMock()
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "OBSERVE", "bias_value": 0.0}}
    )
    r = _reader(redis_client)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    assert out is None
    assert _read_counter("observe_phase") >= before + 1


def test_metric_increments_on_not_countertrend():
    if edb_ov._override_read_total is None:
        pytest.skip("prometheus_client unavailable")
    before = _read_counter("not_countertrend")
    redis_client = MagicMock()
    redis_client.get.return_value = _build_snapshot(
        buckets={"SHORT|trending_bull": {"phase": "CANARY_LOW", "bias_value": 0.03}}
    )
    r = _reader(redis_client)
    out = r.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=False)
    assert out is None
    assert _read_counter("not_countertrend") >= before + 1


def test_metric_increments_on_disabled(monkeypatch):
    if edb_ov._override_read_total is None:
        pytest.skip("prometheus_client unavailable")
    monkeypatch.delenv("AUTOCAL_EDGE_DIRECTIONAL_BIAS_READ_ENABLED", raising=False)
    edb_ov.reset_reader_for_tests()
    before = _read_counter("disabled")
    out = edb_ov.get_bias_override(direction="SHORT", regime="trending_bull", countertrend=True)
    assert out is None
    assert _read_counter("disabled") >= before + 1
