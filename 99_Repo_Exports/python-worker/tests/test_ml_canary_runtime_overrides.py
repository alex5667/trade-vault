"""Tests for core/ml_canary_runtime_overrides.py — TTL-cached reader + HMAC."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from unittest.mock import MagicMock

import pytest

from core import ml_canary_runtime_overrides as mod
from core.ml_canary_runtime_overrides import (
    MLCanaryReader,
    get_canary_rate,
    reset_reader_for_tests,
)


@pytest.fixture(autouse=True)
def _reset_singleton():
    reset_reader_for_tests()
    yield
    reset_reader_for_tests()


def _sign(body: dict, secret: str) -> dict:
    out = dict(body)
    canon = json.dumps(out, sort_keys=True, separators=(",", ":")).encode()
    out["sig"] = hmac.new(secret.encode(), canon, hashlib.sha256).hexdigest()
    return out


# ─── Disabled / fail-open ─────────────────────────────────────────────────────


class TestDisabledReader:
    def test_disabled_returns_default(self, monkeypatch):
        monkeypatch.delenv("AUTOCAL_ML_CANARY_READ_ENABLED", raising=False)
        assert get_canary_rate(0.07) == 0.07

    def test_disabled_clamps_default(self, monkeypatch):
        monkeypatch.delenv("AUTOCAL_ML_CANARY_READ_ENABLED", raising=False)
        assert get_canary_rate(2.0) == 1.0
        assert get_canary_rate(-0.5) == 0.0


# ─── Enabled with no state ────────────────────────────────────────────────────


class TestEnabledNoState:
    def test_no_state_returns_default(self):
        r = MagicMock()
        r.get.return_value = None
        reader = MLCanaryReader(r)
        assert reader.get_canary_rate(default=0.05) == 0.05


# ─── Override application ──────────────────────────────────────────────────────


class TestEnforceFlag:
    def test_enforce_0_returns_default(self):
        r = MagicMock()
        r.get.return_value = json.dumps({
            "current_rate": 0.20, "enforce": 0,
            "ts_ms": int(time.time() * 1000),
        })
        reader = MLCanaryReader(r)
        assert reader.get_canary_rate(default=0.05) == 0.05

    def test_enforce_1_returns_override(self):
        r = MagicMock()
        r.get.return_value = json.dumps({
            "current_rate": 0.20, "enforce": 1,
            "ts_ms": int(time.time() * 1000),
        })
        reader = MLCanaryReader(r)
        assert reader.get_canary_rate(default=0.05) == 0.20

    def test_override_clamped_to_unit_interval(self):
        r = MagicMock()
        r.get.return_value = json.dumps({
            "current_rate": 1.5, "enforce": 1,
            "ts_ms": int(time.time() * 1000),
        })
        reader = MLCanaryReader(r)
        assert reader.get_canary_rate(default=0.05) == 1.0


# ─── Stale snapshot ───────────────────────────────────────────────────────────


class TestStale:
    def test_stale_snapshot_falls_back_to_default(self):
        r = MagicMock()
        old_ts = int(time.time() * 1000) - 7 * 60 * 60 * 1000  # 7h ago
        r.get.return_value = json.dumps({
            "current_rate": 0.20, "enforce": 1, "ts_ms": old_ts,
        })
        reader = MLCanaryReader(r, stale_ms=6 * 60 * 60 * 1000)
        assert reader.get_canary_rate(default=0.05) == 0.05


# ─── HMAC ─────────────────────────────────────────────────────────────────────


class TestHmac:
    def test_valid_sig_applies(self):
        body = {
            "current_rate": 0.10, "enforce": 1,
            "ts_ms": int(time.time() * 1000),
        }
        signed = _sign(body, "sek")
        r = MagicMock()
        r.get.return_value = json.dumps(signed)
        reader = MLCanaryReader(r, hmac_secret="sek")
        assert reader.get_canary_rate(default=0.05) == 0.10

    def test_invalid_sig_ignored(self):
        body = {
            "current_rate": 0.10, "enforce": 1,
            "ts_ms": int(time.time() * 1000),
            "sig": "deadbeef",
        }
        r = MagicMock()
        r.get.return_value = json.dumps(body)
        reader = MLCanaryReader(r, hmac_secret="sek")
        # Bad sig → snapshot ignored → falls back to default
        assert reader.get_canary_rate(default=0.05) == 0.05

    def test_unsigned_snapshot_accepted_when_secret_unset(self):
        body = {
            "current_rate": 0.10, "enforce": 1,
            "ts_ms": int(time.time() * 1000),
        }
        r = MagicMock()
        r.get.return_value = json.dumps(body)
        reader = MLCanaryReader(r, hmac_secret="")
        assert reader.get_canary_rate(default=0.05) == 0.10


# ─── TTL cache ────────────────────────────────────────────────────────────────


class TestTtl:
    def test_refresh_called_once_per_window(self):
        r = MagicMock()
        r.get.return_value = json.dumps({
            "current_rate": 0.10, "enforce": 1,
            "ts_ms": int(time.time() * 1000),
        })
        reader = MLCanaryReader(r, refresh_ms=60_000)
        for _ in range(5):
            reader.get_canary_rate(default=0.05)
        # First call refreshes, subsequent calls cached
        assert r.get.call_count == 1

    def test_refresh_failure_falls_back_to_default(self):
        r = MagicMock()
        r.get.side_effect = RuntimeError("redis down")
        reader = MLCanaryReader(r)
        assert reader.get_canary_rate(default=0.05) == 0.05


# ─── Inspection ───────────────────────────────────────────────────────────────


class TestInspection:
    def test_state_for_inspection_returns_state(self):
        snapshot = {
            "current_rate": 0.20, "enforce": 0, "shadow_n": 100,
            "ts_ms": int(time.time() * 1000),
        }
        r = MagicMock()
        r.get.return_value = json.dumps(snapshot)
        reader = MLCanaryReader(r)
        out = reader.get_state_for_inspection()
        assert out["current_rate"] == 0.20
        assert out["shadow_n"] == 100

    def test_state_for_inspection_empty_when_stale(self):
        r = MagicMock()
        r.get.return_value = None
        reader = MLCanaryReader(r)
        assert reader.get_state_for_inspection() == {}
