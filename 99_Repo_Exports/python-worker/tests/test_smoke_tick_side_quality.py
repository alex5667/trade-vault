import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools.smoke_tick_side_quality import parse_tick_fields, summarize_ticks, TickSample


class TestSmokeTickSideQuality(unittest.TestCase):
    def test_parse_tick_fields_basic(self):
        fields = {
            b"symbol": b"BTCUSDT",
            b"side": b"UNKNOWN",
            b"side_conf": b"unknown",
            b"ts_source": b"payload",
            b"event_ts_ms": b"1700000000000",
            b"stream_ms": b"1700000000123",
        }
        t = parse_tick_fields(fields, msg_id="1700000000123-0", now_ms=1700000000999)
        self.assertEqual(t.symbol, "BTCUSDT")
        self.assertEqual(t.side, "UNKNOWN")
        self.assertEqual(t.side_conf, "unknown")
        self.assertEqual(t.ts_source, "payload")
        self.assertEqual(t.event_ts_ms, 1700000000000)
        self.assertEqual(t.stream_ms, 1700000000123)

    def test_summarize_counts_and_skew(self):
        now = 1700000001000
        samples = [
            TickSample(symbol="BTCUSDT", side="BUY", side_conf="explicit", ts_source="payload",
                       event_ts_ms=1700000000000, stream_ms=1700000000001, now_ms=now),
            TickSample(symbol="BTCUSDT", side="UNKNOWN", side_conf="unknown", ts_source="stream_id",
                       event_ts_ms=1700000000000, stream_ms=1700000005000, now_ms=now),
            TickSample(symbol="ETHUSDT", side="SELL", side_conf="maker", ts_source="payload",
                       event_ts_ms=1700000000000, stream_ms=0, now_ms=now),
        ]
        out = summarize_ticks(samples, max_ts_skew_ms=1000)
        self.assertEqual(out["n"], 3)
        self.assertEqual(out["by_side_conf"]["explicit"], 1)
        self.assertEqual(out["by_side_conf"]["unknown"], 1)
        self.assertEqual(out["by_side_conf"]["maker"], 1)
        self.assertEqual(out["by_side"]["BUY"], 1)
        self.assertEqual(out["by_side"]["UNKNOWN"], 1)
        self.assertEqual(out["by_side"]["SELL"], 1)
        # skew list has 2 entries (where stream_ms > 0)
        self.assertEqual(out["abs_event_stream_skew"]["n"], 2)
        # one entry exceeds threshold (5000ms)
        self.assertEqual(out["event_stream_skew_gt_threshold"]["count"], 1)

    def test_missing_event_ts(self):
        now = 1700000001000
        samples = [
            TickSample(symbol="BTCUSDT", side="UNKNOWN", side_conf="unknown", ts_source="missing",
                       event_ts_ms=0, stream_ms=1700000000000, now_ms=now),
        ]
        out = summarize_ticks(samples)
        self.assertEqual(out["missing_event_ts"], 1)
        self.assertEqual(out["abs_now_event_lag"]["n"], 0)


if __name__ == "__main__":
    unittest.main()
