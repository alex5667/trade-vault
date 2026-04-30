"""
Tests for P66 Decision Coverage KPI worker (decision_coverage_kpi_worker_v1).

Covers:
  - timestamp extraction (explicit ts fields, stream-id fallback)
  - regime derivation from dq_state / drift_state
  - per-minute bucket advance (incremental + rebuild on large gap)
  - _process_one: pipeline writes correct state fields
  - bootstrap_state: aggregates per-minute buckets via pipeline
"""
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import time
from unittest.mock import MagicMock, call, patch

import pytest

# Import helpers without triggering redis connection
from orderflow_services.decision_coverage_kpi_worker_v1 import (
    Cfg
    _advance_window
    _decision_ts_ms
    _minute
    _parse_json_maybe
    _process_one
    _regime_from_states
    _state_norm
    load_cfg
)


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------

def _make_cfg(**overrides) -> Cfg:
    """Build a Cfg with test-friendly defaults."""
    defaults = dict(
        redis_url="redis://localhost:6379/0"
        stream="decisions:final"
        group="decision_coverage_kpi_v1"
        consumer="test-consumer"
        block_ms=100
        count=10
        window_minutes=1440
        bucket_prefix="kpi:decision_coverage:bucket:"
        bucket_ttl_s=259200
        state_key="metrics:decision_coverage:state"
        claim_idle_ms=60000
        sleep_on_idle_s=0.1
        rebuild_gap_minutes=10
    )
    defaults.update(overrides)
    return Cfg(**defaults)


def _mock_redis() -> MagicMock:
    r = MagicMock()
    r.hgetall.return_value = {}
    r.pipeline.return_value.__enter__ = lambda s: s
    r.pipeline.return_value.__exit__ = MagicMock(return_value=False)
    pipe = MagicMock()
    r.pipeline.return_value = pipe
    pipe.execute.return_value = []
    pipe.hmget.return_value = []
    return r


# ---------------------------------------------------------------------------
# Unit tests: pure functions
# ---------------------------------------------------------------------------

class TestStateNorm:
    def test_known_states(self):
        assert _state_norm("ok") == "ok"
        assert _state_norm("warn") == "warn"
        assert _state_norm("BLOCK") == "block"  # case insensitive

    def test_unknown(self):
        assert _state_norm(None) == "unknown"
        assert _state_norm("") == "unknown"
        assert _state_norm("other") == "unknown"


class TestRegimeFromStates:
    def test_block_priority(self):
        assert _regime_from_states("block", "ok") == "block"
        assert _regime_from_states("ok", "block") == "block"
        assert _regime_from_states("block", "block") == "block"

    def test_warn_priority(self):
        assert _regime_from_states("warn", "ok") == "warn"
        assert _regime_from_states("ok", "warn") == "warn"

    def test_ok(self):
        assert _regime_from_states("ok", "ok") == "ok"

    def test_unknown_fallback(self):
        assert _regime_from_states("unknown", "unknown") == "unknown"
        assert _regime_from_states(None, None) == "unknown"


class TestDecisionTsMs:
    def test_explicit_ms_field(self):
        ts = get_ny_time_millis()
        result = _decision_ts_ms({"decision_ts_ms": str(ts)}, "0-0")
        assert result == ts

    def test_explicit_seconds_field_normalized(self):
        ts_s = int(time.time())  # seconds
        result = _decision_ts_ms({"ts": str(ts_s)}, "0-0")
        # Should normalize to ms
        assert result == ts_s * 1000

    def test_stream_id_fallback(self):
        stream_id = "1700000000000-0"
        result = _decision_ts_ms({}, stream_id)
        assert result == 1700000000000

    def test_explicit_ms_zero_falls_through(self):
        """ts_ms=0 should fall through to stream id."""
        stream_id = "1700000000000-0"
        result = _decision_ts_ms({"ts_ms": "0"}, stream_id)
        assert result == 1700000000000


class TestParseJsonMaybe:
    def test_dict_passthrough(self):
        d = {"state": "ok"}
        assert _parse_json_maybe(d) is d

    def test_json_string_parsed(self):
        result = _parse_json_maybe('{"state": "warn"}')
        assert result == {"state": "warn"}

    def test_plain_string_passthrough(self):
        assert _parse_json_maybe("ok") == "ok"

    def test_none(self):
        assert _parse_json_maybe(None) is None


class TestMinute:
    def test_minute_bucket(self):
        # 1700000000000 ms = minute 1700000000000 // 60000
        ts_ms = 1700000000000
        assert _minute(ts_ms) == ts_ms // 60000


# ---------------------------------------------------------------------------
# Integration-style tests with mocked Redis
# ---------------------------------------------------------------------------

class TestAdvanceWindow:
    def test_no_advance_if_same(self):
        r = _mock_redis()
        cfg = _make_cfg()
        rolling = {"ok": 10, "warn": 5, "block": 2, "unknown": 1, "total": 18}
        cur = 1000
        new_cur = _advance_window(r, cfg, cur, cur, rolling)
        assert new_cur == cur
        assert rolling["total"] == 18  # unchanged

    def test_incremental_advance_one_minute(self):
        r = _mock_redis()
        cfg = _make_cfg()
        # Outgoing bucket has: ok=3, total=5
        r.hgetall.return_value = {"ok": "3", "warn": "1", "block": "0", "unknown": "1", "total": "5"}
        rolling = {"ok": 10, "warn": 5, "block": 2, "unknown": 3, "total": 20}
        cur = _advance_window(r, cfg, 1000, 1001, rolling)
        assert cur == 1001
        # Bucket 1001-1440 = -439 fell off; subtract ok=3, warn=1, unk=1, total=5
        assert rolling["ok"] == 7
        assert rolling["total"] == 15

    def test_large_gap_triggers_rebuild(self):
        """A gap >= rebuild_gap_minutes triggers _bootstrap_state (rebuild path)."""
        r = _mock_redis()
        cfg = _make_cfg(rebuild_gap_minutes=5)
        # Bootstrap returns empty buckets
        r.hgetall.return_value = {}
        rolling = {"ok": 0, "warn": 0, "block": 0, "unknown": 0, "total": 0}
        with patch(
            "orderflow_services.decision_coverage_kpi_worker_v1._bootstrap_state"
            return_value=(2000, {"ok": 5, "warn": 0, "block": 0, "unknown": 0, "total": 5}, 0)
        ) as mock_bs:
            cur = _advance_window(r, cfg, 1000, 1010, rolling)  # gap=10 >= rebuild_gap_minutes=5
            mock_bs.assert_called_once()
            assert cur == 2000
            assert rolling["ok"] == 5


class TestProcessOne:
    def test_ok_decision_updates_state(self):
        r = _mock_redis()
        cfg = _make_cfg()
        pipe = r.pipeline.return_value
        pipe.hmget.return_value = []

        ts_ms = get_ny_time_millis()
        cur_min = _minute(ts_ms)
        rolling = {"ok": 0, "warn": 0, "block": 0, "unknown": 0, "total": 0}

        fields = {"dq_state": "ok", "drift_state": "ok", "ts_ms": str(ts_ms)}
        new_cur, new_last = _process_one(r, cfg, f"{ts_ms}-0", fields, cur_min, rolling, 0)

        assert new_cur == cur_min
        assert new_last == ts_ms
        # Rolling totals incremented
        assert rolling["ok"] == 1
        assert rolling["total"] == 1
        pipe.execute.assert_called()

    def test_block_regime_derived_from_dq(self):
        r = _mock_redis()
        cfg = _make_cfg()
        pipe = r.pipeline.return_value

        ts_ms = get_ny_time_millis()
        cur_min = _minute(ts_ms)
        rolling = {"ok": 5, "warn": 2, "block": 0, "unknown": 0, "total": 7}

        fields = {"dq_state": "block", "drift_state": "ok", "ts_ms": str(ts_ms)}
        _process_one(r, cfg, f"{ts_ms}-0", fields, cur_min, rolling, ts_ms - 1000)

        assert rolling["block"] == 1
        assert rolling["total"] == 8

    def test_nested_dq_state_dict_unwrapped(self):
        """dq_state as JSON object {"state": "warn"} should be unwrapped."""
        import json
        r = _mock_redis()
        cfg = _make_cfg()
        r.pipeline.return_value.execute.return_value = []

        ts_ms = get_ny_time_millis()
        cur_min = _minute(ts_ms)
        rolling = {"ok": 0, "warn": 0, "block": 0, "unknown": 0, "total": 0}

        fields = {
            "dq_state": json.dumps({"state": "warn"})
            "drift_state": "ok"
            "ts_ms": str(ts_ms)
        }
        _process_one(r, cfg, f"{ts_ms}-0", fields, cur_min, rolling, 0)
        assert rolling["warn"] == 1

    def test_very_old_message_skipped(self):
        """Messages older than window_minutes are ignored (ACK still happens in caller)."""
        r = _mock_redis()
        cfg = _make_cfg(window_minutes=1440)

        now_ms = get_ny_time_millis()
        cur_min = _minute(now_ms)
        # Message is older than window (cur_min - 1440 - 10 = very old)
        old_ts_ms = (cur_min - cfg.window_minutes - 10) * 60000
        rolling = {"ok": 3, "warn": 0, "block": 0, "unknown": 0, "total": 3}

        fields = {"dq_state": "ok", "drift_state": "ok", "ts_ms": str(old_ts_ms)}
        new_cur, _ = _process_one(r, cfg, f"{old_ts_ms}-0", fields, cur_min, rolling, 0)

        # No pipeline calls — message was dropped
        pipe = r.pipeline.return_value
        pipe.execute.assert_not_called()
        assert rolling["total"] == 3  # unchanged


class TestLoadCfg:
    def test_defaults(self):
        cfg = load_cfg()
        assert cfg.window_minutes == 1440
        assert cfg.bucket_ttl_s == 86400 * 3
        assert cfg.stream == "decisions:final"
        assert cfg.state_key == "metrics:decision_coverage:state"
        assert cfg.rebuild_gap_minutes == 10  # worker Cfg has no port — that's on the exporter Cfg

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("DECISION_COVERAGE_WINDOW_MINUTES", "720")
        monkeypatch.setenv("DECISION_COVERAGE_BLOCK_MS", "3000")
        cfg = load_cfg()
        assert cfg.window_minutes == 720
        assert cfg.block_ms == 3000
