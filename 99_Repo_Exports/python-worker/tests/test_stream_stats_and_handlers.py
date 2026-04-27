"""
Unit tests for stream_statistics.py and stream_handlers.py.

Runs without Redis (all pure Python).
"""

import json
import time
import pytest

from stream_statistics import StreamStatistics
from stream_handlers import StreamMessageHandler
from stream_utils import StreamUtils


# ======================================================================
# StreamStatistics
# ======================================================================


class TestStreamStatistics:
    def _make(self) -> StreamStatistics:
        return StreamStatistics()

    def test_initial_state(self):
        s = self._make()
        assert s.get_total_messages() == 0
        assert s.get_errors_count() == 0
        assert s.get_uptime() >= 0
        assert s.get_messages_per_second() == 0.0
        assert s.get_messages_by_stream() == {}
        assert s.get_last_message_time() is None

    def test_update_stats_increments_counters(self):
        s = self._make()
        s.update_stats("stream:a", "1-1")
        s.update_stats("stream:a", "1-2")
        s.update_stats("stream:b", "2-1")

        assert s.get_total_messages() == 3
        by_stream = s.get_messages_by_stream()
        assert by_stream["stream:a"] == 2
        assert by_stream["stream:b"] == 1

    def test_update_stats_updates_last_message_time(self):
        s = self._make()
        before = time.time()
        s.update_stats("stream:a", "1-1")
        after = time.time()
        lmt = s.get_last_message_time()
        assert lmt is not None
        assert before <= lmt <= after

    def test_increment_errors(self):
        s = self._make()
        s.increment_errors()
        s.increment_errors()
        assert s.get_errors_count() == 2

    def test_messages_per_second_nonzero(self):
        s = self._make()
        # Manually add messages without real sleep
        for i in range(100):
            s.update_stats("stream:x", str(i))
        # Allow at least 1 ms to pass
        time.sleep(0.01)
        mps = s.get_messages_per_second()
        assert mps > 0.0

    def test_reset_stats_clears_all(self):
        s = self._make()
        s.update_stats("stream:a", "1-1")
        s.increment_errors()
        s.reset_stats()

        assert s.get_total_messages() == 0
        assert s.get_errors_count() == 0
        assert s.get_messages_by_stream() == {}
        assert s.get_last_message_time() is None

    def test_get_stats_summary_keys(self):
        s = self._make()
        s.update_stats("stream:a", "1-1")
        summary = s.get_stats_summary()

        assert "total_messages" in summary
        assert "errors" in summary
        assert "uptime" in summary
        assert "messages_per_second" in summary
        assert "streams_count" in summary
        assert summary["total_messages"] == 1
        assert summary["streams_count"] == 1

    def test_get_messages_by_stream_returns_copy(self):
        """Mutating the returned dict must not affect internal state."""
        s = self._make()
        s.update_stats("stream:a", "1-1")
        copy = s.get_messages_by_stream()
        copy["stream:a"] = 999
        assert s.get_messages_by_stream()["stream:a"] == 1

    def test_format_uptime_seconds(self):
        assert StreamStatistics._format_uptime(45) == "45с"

    def test_format_uptime_minutes(self):
        assert StreamStatistics._format_uptime(120) == "2м"

    def test_format_uptime_hours(self):
        result = StreamStatistics._format_uptime(7200)
        assert result == "2.0ч"

    def test_print_stats_no_crash(self, capsys):
        s = self._make()
        s.update_stats("stream:a", "1-1")
        s.print_stats()
        captured = capsys.readouterr()
        assert "СТАТИСТИКА" in captured.out


# ======================================================================
# StreamMessageHandler
# ======================================================================


class TestStreamMessageHandler:
    def _make(self) -> StreamMessageHandler:
        return StreamMessageHandler()

    def _fields(self, payload: object) -> dict:
        return {"data": json.dumps(payload)}

    def test_initial_count_zero(self):
        h = self._make()
        assert h.get_message_count() == 0

    def test_reset_count(self):
        h = self._make()
        h.process_stream_message("s", "1-1", self._fields({"type": "volatilityRange", "range": 1}))
        h.reset_message_count()
        assert h.get_message_count() == 0

    def test_count_increments_on_valid_message(self):
        h = self._make()
        h.process_stream_message("s", "1-1", self._fields({"type": "volatilityRange"}))
        assert h.get_message_count() == 1

    def test_missing_data_field_no_crash(self, capsys):
        h = self._make()
        h.process_stream_message("s", "1-1", {})
        captured = capsys.readouterr()
        assert "data" in captured.out or h.get_message_count() == 0

    def test_invalid_json_no_crash(self, capsys):
        h = self._make()
        h.process_stream_message("s", "1-1", {"data": "NOT_JSON{"})
        captured = capsys.readouterr()
        assert "JSON" in captured.out or h.get_message_count() == 0

    def test_list_data_handled_as_bulk(self, capsys):
        h = self._make()
        h.process_stream_message("s", "1-1", self._fields([{"a": 1}, {"b": 2}]))
        captured = capsys.readouterr()
        assert "Bulk" in captured.out or "bulk" in captured.out or h.get_message_count() == 1

    def test_bulk_message(self, capsys):
        h = self._make()
        payload = {"type": "bulk", "items": [{"x": 1}, {"x": 2}], "count": 2}
        h.process_stream_message("s", "1-1", self._fields(payload))
        captured = capsys.readouterr()
        assert "bulk" in captured.out.lower()

    def test_volatility_range_message(self, capsys):
        h = self._make()
        payload = {"type": "volatilityRange", "range": 0.5, "avgRange": 0.3, "volatility": 1.2}
        h.process_stream_message("s", "1-1", self._fields(payload))
        captured = capsys.readouterr()
        assert "0.5" in captured.out

    def test_gainers_message(self, capsys):
        h = self._make()
        payload = {"type": "top-gainers", "priceChangePercent": "5.2", "volume": "1000"}
        h.process_stream_message("s", "1-1", self._fields(payload))
        captured = capsys.readouterr()
        assert "5.2" in captured.out

    def test_losers_message(self, capsys):
        h = self._make()
        payload = {"type": "top-losers", "priceChangePercent": "-3.1", "volume": "500"}
        h.process_stream_message("s", "1-1", self._fields(payload))
        captured = capsys.readouterr()
        assert "3.1" in captured.out

    def test_new_pairs_message_with_pairs(self, capsys):
        h = self._make()
        payload = {"type": "ws-new-pairs", "pairs": ["BTCUSDT", "ETHUSDT"]}
        h.process_stream_message("s", "1-1", self._fields(payload))
        captured = capsys.readouterr()
        assert "BTCUSDT" in captured.out

    def test_new_pairs_message_many_pairs(self, capsys):
        h = self._make()
        pairs = [f"PAIR{i}USDT" for i in range(10)]
        payload = {"type": "ws-new-pairs", "pairs": pairs}
        h.process_stream_message("s", "1-1", self._fields(payload))
        captured = capsys.readouterr()
        assert "еще" in captured.out

    def test_unknown_type_no_crash(self):
        h = self._make()
        payload = {"type": "completely_unknown_xyz"}
        h.process_stream_message("s", "1-1", self._fields(payload))
        assert h.get_message_count() == 1  # still counted

    def test_non_dict_non_list_data_wrapped(self, capsys):
        h = self._make()
        h.process_stream_message("s", "1-1", self._fields(42))
        captured = capsys.readouterr()
        assert h.get_message_count() == 1


# ======================================================================
# StreamUtils (pure methods, no Redis)
# ======================================================================


class TestStreamUtils:
    def test_validate_stream_data_valid(self):
        assert StreamUtils.validate_stream_data({"data": "..."}) is True

    def test_validate_stream_data_missing_data_key(self):
        assert StreamUtils.validate_stream_data({"other": 1}) is False

    def test_validate_stream_data_not_dict(self):
        assert StreamUtils.validate_stream_data("string") is False  # type: ignore[arg-type]
        assert StreamUtils.validate_stream_data(None) is False  # type: ignore[arg-type]

    def test_format_message_for_logging_valid(self):
        fields = {"data": json.dumps({"type": "volatilityRange", "symbol": "BTCUSDT"})}
        result = StreamUtils.format_message_for_logging("1-1", fields)
        assert "volatilityRange" in result
        assert "BTCUSDT" in result

    def test_format_message_for_logging_invalid_json(self):
        fields = {"data": "not-json{"}
        result = StreamUtils.format_message_for_logging("1-1", fields)
        assert "1-1" in result
        assert "Error" in result

    def test_format_message_for_logging_no_data(self):
        result = StreamUtils.format_message_for_logging("1-1", {})
        assert "1-1" in result

    def test_format_stream_list_empty(self):
        assert StreamUtils.format_stream_list([]) == "нет"

    def test_format_stream_list_short(self):
        result = StreamUtils.format_stream_list(["a", "b"])
        assert result == "a, b"

    def test_format_stream_list_long(self):
        streams = [f"s{i}" for i in range(5)]
        result = StreamUtils.format_stream_list(streams)
        assert "еще" in result
        assert "2" in result
