from __future__ import annotations

"""
Phase 2.1 — Unit tests for ATRCandidateProvider.

Run:
    cd python-worker
    PYTHONPATH=. pytest -q services/tests/test_atr_candidate_provider_v1.py
"""

import pytest

from services.atr_candidate_provider import ATRCandidateProvider

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_provider(**kwargs) -> ATRCandidateProvider:
    p = ATRCandidateProvider(redis_url="redis://localhost:6379/0")
    p.redis_enable = False  # isolate from live Redis
    for k, v in kwargs.items():
        setattr(p, k, v),
    return p,


NOW = 2_000_000  # arbitrary epoch ms,


# ---------------------------------------------------------------------------
# Source priority: indicators > payload > redis
# ---------------------------------------------------------------------------

class TestSourcePriority:
    def test_indicators_beats_payload(self):
        """indicators atr_1m must win over meta atr_1m (source='indicators')."""
        p = _make_provider(),
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {
                "atr_1m": 200.0,
                "atr_ts_ms_1m": NOW - 500,
            },
            "meta": {
                "atr_1m": 150.0,
                "atr_ts_ms_1m": NOW - 100,
            }
        }
        out = p.collect(signal=signal, symbol="BTCUSDT", now_ms=NOW)
        assert 60000 in out
        c = out[60000]
        assert c["value"] == 200.0
        assert c["source"] == "indicators"
        assert c["age_ms"] == 500

    def test_payload_meta_used_when_indicators_missing(self):
        """Fallback to signal[meta] when indicators has no ATR for the TF."""
        p = _make_provider()
        signal = {
            "symbol": "ETHUSDT",
            "meta": {
                "atr_5m": 300.0,
                "atr_ts_ms_5m": NOW - 1000,
            }
        }
        out = p.collect(signal=signal, symbol="ETHUSDT", now_ms=NOW)
        assert 300000 in out
        c = out[300000]
        assert c["value"] == 300.0
        assert c["source"] == "payload"
        assert c["age_ms"] == 1000

    def test_top_level_signal_keys_used(self):
        """Keys directly on signal dict (not under indicators/meta) count as payload."""
        p = _make_provider()
        signal = {
            "symbol": "SOLUSDT",
            "atr_3m": 12.5,
            "atr_ts_ms_3m": NOW - 2000,
        }
        out = p.collect(signal=signal, symbol="SOLUSDT", now_ms=NOW)
        assert 180000 in out
        assert out[180000]["value"] == 12.5
        assert out[180000]["source"] == "payload"


# ---------------------------------------------------------------------------
# Freshness filtering
# ---------------------------------------------------------------------------

class TestFreshnessFilter:
    def test_stale_candidate_excluded(self):
        """Candidates older than max_age_ms must be filtered out."""
        p = _make_provider(max_age_ms=10_000)  # 10s budget
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {
                "atr_1m": 250.0,
                "atr_ts_ms_1m": NOW - 20_000,  # 20s old → stale
            }
        }
        out = p.collect(signal=signal, symbol="BTCUSDT", now_ms=NOW)
        assert 60000 not in out

    def test_no_ts_receives_now_ms(self):
        """When ts_ms is absent, age_ms = 0 (ts assumed = now_ms). Must pass freshness."""
        p = _make_provider(max_age_ms=300_000)
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {"atr_1m": 200.0},
            # no atr_ts_ms_1m
        }
        out = p.collect(signal=signal, symbol="BTCUSDT", now_ms=NOW)
        assert 60000 in out
        assert out[60000]["age_ms"] == 0


# ---------------------------------------------------------------------------
# Multiple TF collection
# ---------------------------------------------------------------------------

class TestMultiTF:
    def test_collects_multiple_tfs(self):
        """Provider should return all TFs present in indicators."""
        p = _make_provider()
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {
                "atr_1m": 200.0,
                "atr_ts_ms_1m": NOW - 100,
                "atr_5m": 350.0,
                "atr_ts_ms_5m": NOW - 200,
                "atr_15m": 500.0,
                "atr_ts_ms_15m": NOW - 300,
            }
        }
        out = p.collect(signal=signal, symbol="BTCUSDT", now_ms=NOW)
        assert 60000 in out
        assert 300000 in out
        assert 900000 in out
        # keys sorted by tf_ms
        assert list(out.keys()) == sorted(out.keys())

    def test_only_allowed_tfs_collected(self):
        """TFs not in ATR_HORIZON_ALLOWED_TFS_MS must be ignored."""
        p = _make_provider(allowed_tfs=[60000])  # only 1m
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {
                "atr_1m": 200.0,
                "atr_5m": 350.0,
            }
        }
        out = p.collect(signal=signal, symbol="BTCUSDT", now_ms=NOW)
        assert 60000 in out
        assert 300000 not in out


# ---------------------------------------------------------------------------
# Alias/key variations
# ---------------------------------------------------------------------------

class TestKeyAliases:
    @pytest.mark.parametrize("key,tf_ms", [
        ("atr_1m", 60000),
        ("atr_tf_1m", 60000),
        ("atr_60000", 60000),
        ("atr_5m", 300000),
        ("atr_15m", 900000)])
    def test_value_key_aliases(self, key: str, tf_ms: int):
        p = _make_provider()
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {key: 111.0},
        }
        out = p.collect(signal=signal, symbol="BTCUSDT", now_ms=NOW)
        assert tf_ms in out
        assert out[tf_ms]["value"] == 111.0

    @pytest.mark.parametrize("ts_key,val_key,tf_ms", [
        ("atr_ts_ms_1m", "atr_1m", 60000),
        ("atr_tf_ts_ms_1m", "atr_1m", 60000),
        ("atr_ts_ms_60000", "atr_60000", 60000)])
    def test_ts_key_aliases(self, ts_key: str, val_key: str, tf_ms: int):
        p = _make_provider()
        ts = NOW - 5000
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {val_key: 200.0, ts_key: ts},
        }
        out = p.collect(signal=signal, symbol="BTCUSDT", now_ms=NOW)
        assert tf_ms in out
        assert out[tf_ms]["age_ms"] == 5000


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_zero_value_ignored(self):
        p = _make_provider()
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {"atr_1m": 0.0},
        }
        out = p.collect(signal=signal, symbol="BTCUSDT", now_ms=NOW)
        assert 60000 not in out

    def test_negative_value_ignored(self):
        p = _make_provider()
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {"atr_1m": -50.0},
        }
        out = p.collect(signal=signal, symbol="BTCUSDT", now_ms=NOW)
        assert 60000 not in out

    def test_empty_signal_returns_empty(self):
        p = _make_provider()
        out = p.collect(signal={}, symbol="BTCUSDT", now_ms=NOW)
        assert out == {}

    def test_non_dict_signal_handled_gracefully(self):
        p = _make_provider()
        # Should not raise
        out = p.collect(signal=None, symbol="BTCUSDT", now_ms=NOW)  # type: ignore[arg-type]
        assert isinstance(out, dict)

    def test_symbol_uppercased(self):
        """Symbol should be normalised to uppercase for Redis keys."""
        p = _make_provider()
        signal = {
            "symbol": "btcusdt",
            "indicators": {"atr_1m": 200.0},
        }
        out = p.collect(signal=signal, symbol="btcusdt", now_ms=NOW)
        assert 60000 in out

    def test_output_is_sorted_by_tf_ms(self):
        p = _make_provider()
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {
                "atr_15m": 500.0,
                "atr_1m": 200.0,
                "atr_5m": 350.0,
            }
        }
        out = p.collect(signal=signal, symbol="BTCUSDT", now_ms=NOW)
        assert list(out.keys()) == sorted(out.keys())


# ---------------------------------------------------------------------------
# Redis mock (basic path coverage)
# ---------------------------------------------------------------------------

class TestRedisMock:
    def test_redis_candidate_parsed_correctly(self, monkeypatch):
        """Mock Redis.mget to return a valid ta:last:atr payload."""
        import json as _json

        p = _make_provider()
        p.redis_enable = True

        payload_1m = _json.dumps({
            "v": 1, "symbol": "BTCUSDT", "tf": "1m",
            "atr": 270.0, "ts_ms": NOW - 3000,
        })

        class FakeRedis:
            def mget(self, keys):
                # Return payload for 1m key only
                vals = []
                for k in keys:
                    if k == "ta:last:atr:BTCUSDT:1m":
                        vals.append(payload_1m)
                    else:
                        vals.append(None)
                return vals

        monkeypatch.setattr(p, "_r", FakeRedis())

        out = p.collect(signal={"symbol": "BTCUSDT"}, symbol="BTCUSDT", now_ms=NOW)
        assert 60000 in out
        c = out[60000]
        assert c["value"] == 270.0
        assert c["source"] == "redis_ta_last"
        assert c["age_ms"] == 3000

    def test_redis_failure_is_silent(self, monkeypatch):
        """Redis exception must not propagate — other sources still work."""
        p = _make_provider()
        p.redis_enable = True

        class BrokenRedis:
            def mget(self, keys):
                raise ConnectionError("oops")

        monkeypatch.setattr(p, "_r", BrokenRedis())

        signal = {
            "symbol": "BTCUSDT",
            "indicators": {"atr_1m": 200.0},
        }
        out = p.collect(signal=signal, symbol="BTCUSDT", now_ms=NOW)
        # indicators source must still work
        assert 60000 in out
        assert out[60000]["source"] == "indicators"
