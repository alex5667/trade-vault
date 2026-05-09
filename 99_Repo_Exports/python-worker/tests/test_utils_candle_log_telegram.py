from __future__ import annotations

"""Tests for utils.candle_utils, utils.helpers, utils.log_throttler, utils.telegram_notify."""

import json
import threading
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

# ---------------------------------------------------------------------------
# helpers._f / ._i
# ---------------------------------------------------------------------------
from utils.helpers import _f, _i


class TestF:
    def test_float_value(self) -> None:
        assert _f(3.14) == pytest.approx(3.14)

    def test_string_number(self) -> None:
        assert _f("2.5") == pytest.approx(2.5)

    def test_none_default(self) -> None:
        assert _f(None) == 0.0

    def test_none_custom_default(self) -> None:
        assert _f(None, -1.0) == -1.0

    def test_invalid_string(self) -> None:
        assert _f("bad", 99.0) == 99.0

    def test_integer(self) -> None:
        assert _f(42) == 42.0


class TestI:
    def test_int_value(self) -> None:
        assert _i(7) == 7

    def test_float_string(self) -> None:
        assert _i("3.9") == 3  # int(float("3.9")) == 3

    def test_none_default(self) -> None:
        assert _i(None) == 0

    def test_none_custom_default(self) -> None:
        assert _i(None, -1) == -1

    def test_invalid(self) -> None:
        assert _i("nope", 5) == 5


# ---------------------------------------------------------------------------
# candle_utils
# ---------------------------------------------------------------------------

from utils.candle_utils import average, calc_volatility


class TestCalcVolatility:
    def test_normal(self) -> None:
        kline = {"h": "110.0", "l": "90.0", "o": "100.0"}
        assert calc_volatility(kline) == pytest.approx(20.0)

    def test_zero_open_returns_zero(self) -> None:
        kline = {"h": "1.0", "l": "0.5", "o": "0.0"}
        assert calc_volatility(kline) == 0.0

    def test_float_values(self) -> None:
        kline = {"h": "100.5", "l": "99.5", "o": "100.0"}
        assert calc_volatility(kline) == pytest.approx(1.0)


class TestAverage:
    def test_empty_returns_zero(self) -> None:
        assert average([]) == 0.0

    def test_single(self) -> None:
        assert average([5.0]) == 5.0

    def test_multiple(self) -> None:
        assert average([1.0, 2.0, 3.0]) == pytest.approx(2.0)

    def test_integers(self) -> None:
        assert average([10, 20]) == pytest.approx(15.0)


# ---------------------------------------------------------------------------
# log_throttler
# ---------------------------------------------------------------------------

from utils.log_throttler import LogThrottler


class TestLogThrottler:
    def test_first_call_always_true(self) -> None:
        t = LogThrottler()
        assert t.should_log("k", every_n=100) is True

    def test_second_call_suppressed(self) -> None:
        t = LogThrottler()
        t.should_log("k")
        assert t.should_log("k", every_n=100) is False

    def test_nth_call_emitted(self) -> None:
        t = LogThrottler()
        results = [t.should_log("k", every_n=5) for _ in range(10)]
        # 1st and 5th and 10th should be True
        assert results[0] is True
        assert results[4] is True
        assert results[9] is True
        # 2nd through 4th should be False
        assert all(r is False for r in results[1:4])

    def test_get_count(self) -> None:
        t = LogThrottler()
        for _ in range(3):
            t.should_log("c")
        assert t.get_count("c") == 3

    def test_reset_counter(self) -> None:
        t = LogThrottler()
        t.should_log("r")
        t.should_log("r")
        t.reset_counter("r")
        assert t.get_count("r") == 0

    def test_thread_safe_counter(self) -> None:
        """Counter must be consistent under concurrent access."""
        t = LogThrottler()
        N = 1000

        def bump() -> None:
            for _ in range(N):
                t.should_log("x")

        threads = [threading.Thread(target=bump) for _ in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert t.get_count("x") == 4 * N

    def test_log_with_count_uses_logger(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging
        t = LogThrottler()
        with caplog.at_level(logging.INFO):
            emitted = t.log_with_count("lk", "hello world", every_n=10)
        assert emitted is True
        assert "hello world" in caplog.text

    def test_log_with_count_suppresses(self) -> None:
        t = LogThrottler()
        t.should_log("s", every_n=10)  # count=1 → emitted
        emitted = t.log_with_count("s", "msg", every_n=10)  # count=2 → suppressed
        assert emitted is False


# ---------------------------------------------------------------------------
# telegram_notify
# ---------------------------------------------------------------------------

from utils.telegram_notify import _chunks, send_telegram_message


class TestChunks:
    def test_empty_string(self) -> None:
        assert _chunks("") == []

    def test_none_coercion(self) -> None:
        assert _chunks(None) == []  # type: ignore[arg-type]

    def test_short_text_single_chunk(self) -> None:
        result = _chunks("hello", limit=10)
        assert result == ["hello"]

    def test_long_text_split(self) -> None:
        text = "A" * 25
        chunks = _chunks(text, limit=10)
        assert len(chunks) == 3
        assert chunks[0] == "A" * 10
        assert chunks[2] == "A" * 5

    def test_exact_limit(self) -> None:
        text = "B" * 10
        assert _chunks(text, limit=10) == ["B" * 10]


class TestSendTelegramMessage:
    def test_missing_env_returns_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
        assert send_telegram_message(text="hi") is False

    def test_explicit_token_and_chat(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_requests = MagicMock()
        mock_requests.post.return_value = mock_resp

        with patch("utils.telegram_notify._requests", mock_requests), \
             patch("utils.telegram_notify._REQUESTS_AVAILABLE", True):
            result = send_telegram_message(
                text="test", bot_token="tok123", chat_id="cid456"
            )

        assert result is True
        mock_requests.post.assert_called_once()
        call_json = mock_requests.post.call_args.kwargs["json"]
        assert call_json["text"] == "test"
        assert call_json["chat_id"] == "cid456"

    def test_chunked_long_message(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A message longer than 3900 chars must be sent in multiple posts."""
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_requests = MagicMock()
        mock_requests.post.return_value = mock_resp

        long_text = "X" * 8000

        with patch("utils.telegram_notify._requests", mock_requests), \
             patch("utils.telegram_notify._REQUESTS_AVAILABLE", True):
            result = send_telegram_message(
                text=long_text, bot_token="t", chat_id="c"
            )

        assert result is True
        assert mock_requests.post.call_count == 3  # ceil(8000/3900) = 3

    def test_requests_not_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
        with patch("utils.telegram_notify._REQUESTS_AVAILABLE", False):
            result = send_telegram_message(
                text="hi", bot_token="t", chat_id="c"
            )
        assert result is False


# ---------------------------------------------------------------------------
# ATRCache — DI + meta normalisation smoke tests
# ---------------------------------------------------------------------------

from utils.atr_cache import ATRCache, _normalize_tracker_tf


class TestNormalizeTrackerTf:
    @pytest.mark.parametrize("inp, expected", [
        ("1m", "M1"), ("M1", "M1"), ("5m", "M5"), ("15m", "M15"),
        ("30m", "M30"), ("1h", "H1"), ("4h", "H4"), ("1d", "D1"),
        ("", "M1"), ("unknown", "UNKNOWN"),
    ])
    def test_mapping(self, inp: str, expected: str) -> None:
        assert _normalize_tracker_tf(inp) == expected


class TestATRCacheDI:
    """Tests that use fakeredis injected via the new redis_client= parameter."""

    def _cache(self) -> tuple[ATRCache, fakeredis.FakeRedis]:
        r = fakeredis.FakeRedis(decode_responses=True)
        return ATRCache(redis_client=r), r

    def test_constructor_accepts_redis_client(self) -> None:
        r = fakeredis.FakeRedis(decode_responses=True)
        c = ATRCache(redis_client=r)
        assert c.redis_client is r

    def test_tracker_hash_src_name(self) -> None:
        c, r = self._cache()
        r.hset("ATR:BTCUSDT:M1", mapping={"atr": "42.0", "lastCloseTime": "1700000000000"})
        # Verify via get_candidates (proves hmget path works correctly)
        cands = c.get_candidates(symbol="BTCUSDT", timeframe="1m", now_ms=1700000001000)
        tracker_cand = next((x for x in cands if x["src"] == "atr_tracker"), None)
        assert tracker_cand is not None, f"atr_tracker not found in: {cands}"
        assert tracker_cand["atr"] == pytest.approx(42.0)
        assert tracker_cand["age_ms"] == 1000
        # Also verify get_with_meta picks it up when it is the only source
        v, meta = c.get_with_meta("BTCUSDT", "1m", now_ms=1700000001000)
        assert v == pytest.approx(42.0)
        assert meta["src"] == "atr_tracker"
        assert meta["age_ms"] == 1000

    def test_ta_last_tf_mismatch(self) -> None:
        c, r = self._cache()
        r.set("ta:last:atr:BTCUSDT", json.dumps({"atr": 11.0, "tf": "M5", "ts": 100}))
        cands = c.get_candidates(symbol="BTCUSDT", timeframe="1m", now_ms=200)
        ta = next(x for x in cands if x["src"] == "ta_last")
        assert ta["tf_mismatch"] == 1

    def test_prefer_src_atr_json(self) -> None:
        c, r = self._cache()
        r.set("atr:BTCUSDT:1m", "39.0")
        r.set("atr:json:BTCUSDT:1m", json.dumps({"atr": 41.0, "ts": 1000}))
        v, meta = c.get_with_meta("BTCUSDT", "1m", now_ms=2000, prefer_src="atr_json")
        assert v == pytest.approx(41.0)
        assert meta["src"] == "atr_json"

    def test_no_data_returns_none(self) -> None:
        c, r = self._cache()
        v, meta = c.get_with_meta("ETHUSDT", "1m")
        assert v is None
        assert meta["src"] == "none"

    def test_set_and_get(self) -> None:
        c, r = self._cache()
        assert c.set("SOLUSDT", "1m", 2.5) is True
        assert c.get("SOLUSDT", "1m") == pytest.approx(2.5)

    def test_set_zero_rejected(self) -> None:
        c, r = self._cache()
        assert c.set("SOLUSDT", "1m", 0.0) is False

    def test_delete(self) -> None:
        c, r = self._cache()
        c.set("BNBUSDT", "1h", 1.5)
        assert c.delete("BNBUSDT", "1h") is True

    def test_clear_all(self) -> None:
        c, r = self._cache()
        ok_a = c.set("A", "1m", 1.0)
        ok_b = c.set("B", "1m", 2.0)
        assert ok_a and ok_b, "set() must succeed"
        # Verify keys are present via scan before clearing
        present = list(r.scan_iter(match="atr:*"))
        assert len(present) >= 2, f"Expected >=2 atr: keys, got: {present}"
        deleted = c.clear_all()
        assert deleted >= 2, f"Expected >=2 deleted, got {deleted}"
