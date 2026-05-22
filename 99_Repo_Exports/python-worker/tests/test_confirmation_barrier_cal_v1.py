"""Tests for orderflow_services/confirmation_barrier_cal_v1.py

Coverage:
- _parse_snapshot
- _check_ready
- _sanity_ok
- _do_promote (Redis write + Telegram)
- _notify_blocked
- _send_telegram (dedup)
- reader auto-promote via Redis promote key
- main loop integration (promote cycle)
"""
from __future__ import annotations

import json
import math
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers — synthetic snapshot builder
# ---------------------------------------------------------------------------

def _make_snap(
    bins: dict[str, dict[str, Any]] | None = None,
    ts_ms: int = 0,
) -> dict[str, Any]:
    return {
        "ts_ms": ts_ms or int(time.time() * 1000),
        "bins": bins or {},
    }


def _bin(
    committed_tau: float | None = 1.25,
    n: int = 50,
    last_apply_ms: int = 0,
) -> dict[str, Any]:
    return {
        "committed_tau": committed_tau,
        "n": n,
        "last_apply_ms": last_apply_ms or (int(time.time() * 1000) - 7 * 86_400_000),
    }


# ---------------------------------------------------------------------------
# _parse_snapshot
# ---------------------------------------------------------------------------

class TestParseSnapshot:
    def setup_method(self) -> None:
        from orderflow_services.confirmation_barrier_cal_v1 import _parse_snapshot
        self._fn = _parse_snapshot

    def test_empty_snap(self) -> None:
        result = self._fn({})
        assert result["bins"] == {}
        assert result["first_obs_ms"] == 0
        assert result["total_n"] == 0

    def test_single_bin_calibrated(self) -> None:
        now_ms = int(time.time() * 1000)
        snap = _make_snap({"BTCUSDT:breakout": _bin(1.22, 60, now_ms - 100_000)})
        result = self._fn(snap)
        assert "BTCUSDT:breakout" in result["bins"]
        assert result["bins"]["BTCUSDT:breakout"]["committed_tau"] == pytest.approx(1.22)
        assert result["bins"]["BTCUSDT:breakout"]["n"] == 60
        assert result["first_obs_ms"] > 0
        assert result["total_n"] == 60

    def test_null_committed_tau_parsed_as_none(self) -> None:
        now_ms = int(time.time() * 1000)
        snap = _make_snap({"ETHUSDT:absorption": {"committed_tau": None, "n": 20, "last_apply_ms": now_ms}})
        result = self._fn(snap)
        assert result["bins"]["ETHUSDT:absorption"]["committed_tau"] is None

    def test_invalid_tau_parsed_as_none(self) -> None:
        now_ms = int(time.time() * 1000)
        snap = _make_snap({"SOLUSDT:breakout": {"committed_tau": "bad", "n": 10, "last_apply_ms": now_ms}})
        result = self._fn(snap)
        assert result["bins"]["SOLUSDT:breakout"]["committed_tau"] is None

    def test_nan_tau_parsed_as_none(self) -> None:
        now_ms = int(time.time() * 1000)
        snap = _make_snap({"SOLUSDT:breakout": {"committed_tau": float("nan"), "n": 10, "last_apply_ms": now_ms}})
        result = self._fn(snap)
        assert result["bins"]["SOLUSDT:breakout"]["committed_tau"] is None

    def test_first_obs_ms_is_minimum_across_bins(self) -> None:
        now_ms = int(time.time() * 1000)
        old_ms = now_ms - 10 * 86_400_000
        snap = _make_snap({
            "BTCUSDT:breakout":   _bin(1.22, 50, now_ms - 1_000_000),
            "ETHUSDT:absorption": _bin(1.30, 40, old_ms),
        })
        result = self._fn(snap)
        assert result["first_obs_ms"] == old_ms

    def test_wildcard_bins_included(self) -> None:
        now_ms = int(time.time() * 1000)
        snap = _make_snap({"*:breakout": _bin(1.15, 200, now_ms - 1_000)})
        result = self._fn(snap)
        assert "*:breakout" in result["bins"]

    def test_multiple_bins_total_n(self) -> None:
        now_ms = int(time.time() * 1000)
        snap = _make_snap({
            "BTCUSDT:breakout":   _bin(1.22, 50, now_ms),
            "ETHUSDT:breakout":   _bin(1.28, 30, now_ms),
            "SOLUSDT:absorption": _bin(1.35, 25, now_ms),
        })
        result = self._fn(snap)
        assert result["total_n"] == 105

    def test_non_dict_bin_skipped(self) -> None:
        snap = _make_snap({"BTCUSDT:breakout": "garbage"})  # type: ignore[arg-type]
        result = self._fn(snap)
        assert result["bins"] == {}


# ---------------------------------------------------------------------------
# _check_ready
# ---------------------------------------------------------------------------

class TestCheckReady:
    def setup_method(self) -> None:
        from orderflow_services.confirmation_barrier_cal_v1 import _check_ready, _parse_snapshot
        self._check = _check_ready
        self._parse = _parse_snapshot

    def _parsed(self, bins: dict[str, dict[str, Any]]) -> dict[str, Any]:
        return self._parse(_make_snap(bins))

    def test_no_bins_not_ready(self) -> None:
        ready, reason = self._check(
            self._parsed({}),
            min_days=7.0, min_bins=2, min_samples_bin=30,
            now_ms=int(time.time() * 1000),
        )
        assert not ready
        assert "no_real_bins" in reason

    def test_not_enough_calibrated_bins(self) -> None:
        now_ms = int(time.time() * 1000)
        # Only 1 calibrated bin but need 2
        parsed = self._parsed({
            "BTCUSDT:breakout": _bin(1.22, 50, now_ms - 8 * 86_400_000),
            "ETHUSDT:breakout": _bin(None, 50, now_ms - 8 * 86_400_000),  # not calibrated
        })
        ready, reason = self._check(
            parsed, min_days=7.0, min_bins=2, min_samples_bin=30, now_ms=now_ms,
        )
        assert not ready
        assert "calibrated_bins" in reason

    def test_not_enough_elapsed_time(self) -> None:
        now_ms = int(time.time() * 1000)
        # Only 3 days elapsed, need 7
        parsed = self._parsed({
            "BTCUSDT:breakout":   _bin(1.22, 50, now_ms - 3 * 86_400_000),
            "ETHUSDT:absorption": _bin(1.30, 40, now_ms - 3 * 86_400_000),
        })
        ready, reason = self._check(
            parsed, min_days=7.0, min_bins=2, min_samples_bin=30, now_ms=now_ms,
        )
        assert not ready
        assert "elapsed" in reason

    def test_not_enough_samples_per_bin(self) -> None:
        now_ms = int(time.time() * 1000)
        # Enough time, calibrated, but only 10 samples (need 30)
        parsed = self._parsed({
            "BTCUSDT:breakout":   _bin(1.22, 10, now_ms - 8 * 86_400_000),
            "ETHUSDT:absorption": _bin(1.30, 10, now_ms - 8 * 86_400_000),
        })
        ready, reason = self._check(
            parsed, min_days=7.0, min_bins=2, min_samples_bin=30, now_ms=now_ms,
        )
        assert not ready
        assert "warmed_bins" in reason

    def test_all_conditions_met(self) -> None:
        now_ms = int(time.time() * 1000)
        parsed = self._parsed({
            "BTCUSDT:breakout":   _bin(1.22, 50, now_ms - 8 * 86_400_000),
            "ETHUSDT:absorption": _bin(1.30, 45, now_ms - 9 * 86_400_000),
        })
        ready, reason = self._check(
            parsed, min_days=7.0, min_bins=2, min_samples_bin=30, now_ms=now_ms,
        )
        assert ready
        assert reason == "ok"

    def test_wildcard_bins_excluded_from_real_count(self) -> None:
        now_ms = int(time.time() * 1000)
        # Only wildcard bins — should count as 0 real bins
        parsed = self._parsed({
            "*:breakout":   _bin(1.15, 200, now_ms - 8 * 86_400_000),
            "*:absorption": _bin(1.20, 200, now_ms - 8 * 86_400_000),
        })
        ready, reason = self._check(
            parsed, min_days=7.0, min_bins=2, min_samples_bin=30, now_ms=now_ms,
        )
        assert not ready
        assert "no_real_bins" in reason

    def test_no_first_obs_ms(self) -> None:
        now_ms = int(time.time() * 1000)
        # Bins with last_apply_ms=0
        parsed = self._parsed({
            "BTCUSDT:breakout":   {"committed_tau": 1.22, "n": 50, "last_apply_ms": 0},
            "ETHUSDT:absorption": {"committed_tau": 1.30, "n": 40, "last_apply_ms": 0},
        })
        ready, reason = self._check(
            parsed, min_days=7.0, min_bins=2, min_samples_bin=30, now_ms=now_ms,
        )
        assert not ready
        assert "no_first_obs" in reason

    def test_min_bins_one_is_enough(self) -> None:
        now_ms = int(time.time() * 1000)
        parsed = self._parsed({
            "BTCUSDT:breakout": _bin(1.22, 50, now_ms - 8 * 86_400_000),
        })
        ready, _ = self._check(
            parsed, min_days=7.0, min_bins=1, min_samples_bin=30, now_ms=now_ms,
        )
        assert ready


# ---------------------------------------------------------------------------
# _sanity_ok
# ---------------------------------------------------------------------------

class TestSanityOk:
    def setup_method(self) -> None:
        from orderflow_services.confirmation_barrier_cal_v1 import _sanity_ok, _parse_snapshot
        self._sanity = _sanity_ok
        self._parse = _parse_snapshot

    def test_all_good(self) -> None:
        now_ms = int(time.time() * 1000)
        parsed = self._parse(_make_snap({
            "BTCUSDT:breakout":   _bin(1.22, 50, now_ms),
            "ETHUSDT:absorption": _bin(1.30, 40, now_ms),
        }))
        ok, reason = self._sanity(parsed)
        assert ok
        assert reason == "ok"

    def test_tau_too_low(self) -> None:
        now_ms = int(time.time() * 1000)
        parsed = self._parse(_make_snap({"BTCUSDT:breakout": _bin(0.5, 50, now_ms)}))
        ok, reason = self._sanity(parsed)
        assert not ok
        assert "tau=" in reason

    def test_tau_too_high(self) -> None:
        now_ms = int(time.time() * 1000)
        parsed = self._parse(_make_snap({"BTCUSDT:breakout": _bin(5.0, 50, now_ms)}))
        ok, reason = self._sanity(parsed)
        assert not ok

    def test_none_tau_skipped(self) -> None:
        now_ms = int(time.time() * 1000)
        parsed = self._parse(_make_snap({"BTCUSDT:breakout": _bin(None, 50, now_ms)}))
        ok, _ = self._sanity(parsed)
        assert ok

    def test_wildcard_bins_skipped(self) -> None:
        now_ms = int(time.time() * 1000)
        # Wildcard with out-of-range tau is excluded from sanity check
        parsed = self._parse(_make_snap({"*:breakout": _bin(0.01, 200, now_ms)}))
        ok, _ = self._sanity(parsed)
        assert ok

    def test_border_values_ok(self) -> None:
        now_ms = int(time.time() * 1000)
        parsed = self._parse(_make_snap({
            "BTCUSDT:breakout":   _bin(1.01, 50, now_ms),  # floor
            "ETHUSDT:absorption": _bin(3.00, 40, now_ms),  # ceil
        }))
        ok, _ = self._sanity(parsed)
        assert ok


# ---------------------------------------------------------------------------
# _send_telegram + dedup
# ---------------------------------------------------------------------------

class TestSendTelegram:
    def setup_method(self) -> None:
        from orderflow_services.confirmation_barrier_cal_v1 import _send_telegram
        self._fn = _send_telegram

    def test_sends_to_stream(self) -> None:
        r = MagicMock()
        r.set.return_value = True  # dedup key not exists → proceed
        self._fn(r, notify_stream="notify:telegram", text="hello",
                 dedup_key="test_key", dedup_ttl_h=1)
        r.xadd.assert_called_once()
        call_args = r.xadd.call_args
        assert call_args[0][0] == "notify:telegram"
        fields = call_args[0][1]
        assert fields["text"] == "hello"
        assert fields["parse_mode"] == "HTML"

    def test_dedup_suppresses_second_call(self) -> None:
        r = MagicMock()
        r.set.return_value = None  # dedup key exists
        self._fn(r, notify_stream="notify:telegram", text="hello",
                 dedup_key="test_key", dedup_ttl_h=1)
        r.xadd.assert_not_called()

    def test_no_dedup_key_always_sends(self) -> None:
        r = MagicMock()
        self._fn(r, notify_stream="notify:telegram", text="hello",
                 dedup_key=None, dedup_ttl_h=1)
        r.xadd.assert_called_once()
        r.set.assert_not_called()

    def test_redis_error_in_xadd_does_not_raise(self) -> None:
        r = MagicMock()
        r.set.return_value = True
        r.xadd.side_effect = Exception("Redis down")
        # Should not raise
        self._fn(r, notify_stream="notify:telegram", text="hello",
                 dedup_key=None, dedup_ttl_h=1)


# ---------------------------------------------------------------------------
# _do_promote
# ---------------------------------------------------------------------------

class TestDoPromote:
    def setup_method(self) -> None:
        from orderflow_services.confirmation_barrier_cal_v1 import _do_promote, _parse_snapshot
        self._promote = _do_promote
        self._parse = _parse_snapshot

    def test_writes_promote_state_to_redis(self) -> None:
        now_ms = int(time.time() * 1000)
        r = MagicMock()
        r.set.return_value = True
        parsed = self._parse(_make_snap({
            "BTCUSDT:breakout":   _bin(1.22, 50, now_ms - 8 * 86_400_000),
            "ETHUSDT:absorption": _bin(1.30, 40, now_ms - 9 * 86_400_000),
        }))
        self._promote(
            r, parsed=parsed, promote_key="autocal:confirm_barrier:promote",
            snapshot_ttl=1209600, notify_stream="notify:telegram",
            dedup_ttl_h=24, now_ms=now_ms,
        )
        assert r.set.call_count >= 1
        set_call = r.set.call_args_list[0]
        key = set_call[0][0]
        assert key == "autocal:confirm_barrier:promote"
        data = json.loads(set_call[0][1])
        assert data["promoted"] is True
        assert data["promoted_ms"] == now_ms
        assert "bins_summary" in data
        assert "BTCUSDT:breakout" in data["bins_summary"]

    def test_wildcard_bins_excluded_from_summary(self) -> None:
        now_ms = int(time.time() * 1000)
        r = MagicMock()
        r.set.return_value = True
        parsed = self._parse(_make_snap({
            "BTCUSDT:breakout": _bin(1.22, 50, now_ms),
            "*:breakout":       _bin(1.15, 200, now_ms),
        }))
        self._promote(
            r, parsed=parsed, promote_key="autocal:confirm_barrier:promote",
            snapshot_ttl=1209600, notify_stream="notify:telegram",
            dedup_ttl_h=24, now_ms=now_ms,
        )
        data = json.loads(r.set.call_args_list[0][0][1])
        assert "*:breakout" not in data["bins_summary"]
        assert "BTCUSDT:breakout" in data["bins_summary"]

    def test_sends_telegram_notification(self) -> None:
        now_ms = int(time.time() * 1000)
        r = MagicMock()
        r.set.return_value = True  # dedup + promote key set
        parsed = self._parse(_make_snap({
            "BTCUSDT:breakout": _bin(1.22, 50, now_ms - 8 * 86_400_000),
        }))
        self._promote(
            r, parsed=parsed, promote_key="autocal:confirm_barrier:promote",
            snapshot_ttl=1209600, notify_stream="notify:telegram",
            dedup_ttl_h=24, now_ms=now_ms,
        )
        r.xadd.assert_called_once()
        fields = r.xadd.call_args[0][1]
        assert "ENFORCE" in fields["text"] or "enforce" in fields["text"].lower()

    def test_redis_failure_does_not_raise(self) -> None:
        now_ms = int(time.time() * 1000)
        r = MagicMock()
        r.set.side_effect = Exception("connection refused")
        parsed = self._parse(_make_snap({
            "BTCUSDT:breakout": _bin(1.22, 50, now_ms),
        }))
        # Should return without raising
        self._promote(
            r, parsed=parsed, promote_key="autocal:confirm_barrier:promote",
            snapshot_ttl=1209600, notify_stream="notify:telegram",
            dedup_ttl_h=24, now_ms=now_ms,
        )


# ---------------------------------------------------------------------------
# Reader auto-promote via Redis promote key
# ---------------------------------------------------------------------------

class TestReaderAutoPromote:
    def setup_method(self) -> None:
        from core.confirmation_barrier_reader import ConfirmationBarrierReader
        self._cls = ConfirmationBarrierReader

    def _make_reader(self, redis_client: Any = None, *, enforce: bool = False) -> Any:
        return self._cls(
            redis_client,
            cache_ttl_sec=30.0,
            enforce=enforce,
            redis_key="autocal:confirm_barrier:state",
            promote_key="autocal:confirm_barrier:promote",
        )

    def test_no_env_no_redis_shadow(self) -> None:
        reader = self._make_reader()
        assert not reader.is_enforce()

    def test_env_enforce_true(self) -> None:
        reader = self._make_reader(enforce=True)
        assert reader.is_enforce()

    def test_auto_promote_from_redis(self) -> None:
        r = MagicMock()
        promote_data = json.dumps({"promoted": True, "promoted_ms": int(time.time() * 1000)})
        r.get.return_value = promote_data

        reader = self._make_reader(redis_client=r)
        assert not reader.enforce
        assert reader.is_enforce()  # auto-promoted from Redis
        assert reader._auto_promoted is True

    def test_redis_promoted_false_not_enforced(self) -> None:
        r = MagicMock()
        r.get.return_value = json.dumps({"promoted": False})
        reader = self._make_reader(redis_client=r)
        assert not reader.is_enforce()

    def test_redis_none_response_shadow(self) -> None:
        r = MagicMock()
        r.get.return_value = None
        reader = self._make_reader(redis_client=r)
        assert not reader.is_enforce()

    def test_auto_promote_cached_sticky(self) -> None:
        r = MagicMock()
        promote_data = json.dumps({"promoted": True, "promoted_ms": int(time.time() * 1000)})
        r.get.return_value = promote_data

        reader = self._make_reader(redis_client=r)
        reader.is_enforce()  # first call — sets _auto_promoted=True
        r.get.reset_mock()

        # Second call must not hit Redis (cached)
        reader.is_enforce()
        r.get.assert_not_called()

    def test_promote_cache_ttl_respects_interval(self) -> None:
        r = MagicMock()
        r.get.return_value = json.dumps({"promoted": False})

        reader = self._make_reader(redis_client=r)
        reader._cache_ttl = 60.0
        reader.is_enforce()  # first call
        call_count_1 = r.get.call_count

        reader.is_enforce()  # within TTL — no new call
        assert r.get.call_count == call_count_1

    def test_threshold_uses_calibrated_when_auto_promoted(self) -> None:
        r = MagicMock()
        promote_data = json.dumps({"promoted": True, "promoted_ms": int(time.time() * 1000)})
        snapshot_data = json.dumps({
            "bins": {
                "BTCUSDT:breakout": {
                    "committed_tau": 1.35,
                    "n": 80,
                    "last_apply_ms": int(time.time() * 1000),
                }
            }
        })

        def _get(key: str) -> str | None:
            if "promote" in key:
                return promote_data
            return snapshot_data

        r.get.side_effect = _get
        reader = self._make_reader(redis_client=r)
        tau = reader.threshold_for("BTCUSDT", "breakout")
        assert tau == pytest.approx(1.35)

    def test_threshold_returns_default_in_shadow(self) -> None:
        r = MagicMock()
        r.get.return_value = json.dumps({"promoted": False})
        reader = self._make_reader(redis_client=r)
        tau = reader.threshold_for("BTCUSDT", "breakout")
        assert tau == pytest.approx(1.15)  # hardcoded default

    def test_redis_error_in_promote_check_returns_false(self) -> None:
        r = MagicMock()
        r.get.side_effect = Exception("connection reset")
        reader = self._make_reader(redis_client=r)
        assert not reader.is_enforce()


# ---------------------------------------------------------------------------
# Integration: full promote cycle via _check_ready + _do_promote
# ---------------------------------------------------------------------------

class TestPromoteCycle:
    def setup_method(self) -> None:
        from orderflow_services.confirmation_barrier_cal_v1 import (
            _check_ready, _do_promote, _parse_snapshot, _sanity_ok,
        )
        self._parse = _parse_snapshot
        self._check = _check_ready
        self._sanity = _sanity_ok
        self._promote = _do_promote

    def test_full_promote_cycle_happy_path(self) -> None:
        now_ms = int(time.time() * 1000)
        snap = _make_snap({
            "BTCUSDT:breakout":   _bin(1.22, 50, now_ms - 8 * 86_400_000),
            "ETHUSDT:absorption": _bin(1.30, 40, now_ms - 9 * 86_400_000),
        })
        parsed = self._parse(snap)

        ready, reason = self._check(
            parsed, min_days=7.0, min_bins=2, min_samples_bin=30, now_ms=now_ms,
        )
        assert ready, f"Expected ready but got: {reason}"

        sane, sane_reason = self._sanity(parsed)
        assert sane, f"Expected sane but got: {sane_reason}"

        r = MagicMock()
        r.set.return_value = True
        self._promote(
            r, parsed=parsed, promote_key="autocal:confirm_barrier:promote",
            snapshot_ttl=1209600, notify_stream="notify:telegram",
            dedup_ttl_h=24, now_ms=now_ms,
        )
        assert r.set.called
        written = json.loads(r.set.call_args_list[0][0][1])
        assert written["promoted"] is True

    def test_bad_tau_blocks_promote(self) -> None:
        now_ms = int(time.time() * 1000)
        snap = _make_snap({
            "BTCUSDT:breakout": _bin(0.5, 50, now_ms - 8 * 86_400_000),  # out of range
        })
        parsed = self._parse(snap)
        sane, _ = self._sanity(parsed)
        assert not sane

    def test_insufficient_elapsed_time_blocks(self) -> None:
        now_ms = int(time.time() * 1000)
        snap = _make_snap({
            "BTCUSDT:breakout":   _bin(1.22, 50, now_ms - 2 * 86_400_000),  # only 2d
            "ETHUSDT:absorption": _bin(1.30, 40, now_ms - 2 * 86_400_000),
        })
        parsed = self._parse(snap)
        ready, reason = self._check(
            parsed, min_days=7.0, min_bins=2, min_samples_bin=30, now_ms=now_ms,
        )
        assert not ready
        assert "elapsed" in reason
