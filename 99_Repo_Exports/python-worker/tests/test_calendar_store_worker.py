from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import time

from news_pipeline.calendar_store_worker import CalendarStoreWorker
from tests.fake_redis import FakeRedis  # type: ignore


def test_calendar_store_updates_next_event():
    r = FakeRedis()
    w = CalendarStoreWorker(redis=r, pg=None)

    now_ms = get_ny_time_millis()
    msg1 = {
        "uid": "e1",
        "event_ts_ms": str(now_ms + 5000),
        "ingested_ts_ms": str(now_ms),
        "country": "US",
        "currency": "USD",
        "title": "FOMC Statement",
        "importance": "3",
        "forecast": "",
        "previous": "",
        "unit": "",
        "source": "fmp",
        "payload": "{}",
    }
    w.handle_message("1-0", msg1)

    # Should update crypto/fx/metals
    assert "calendar:agg:crypto" in r.hashes
    assert int(r.hashes["calendar:agg:crypto"]["next_ts_ms"]) == now_ms + 5000

    msg2 = dict(msg1)
    msg2["uid"] = "e2"
    msg2["event_ts_ms"] = str(now_ms + 3000)  # earlier
    w.handle_message("2-0", msg2)

    assert int(r.hashes["calendar:agg:crypto"]["next_ts_ms"]) == now_ms + 3000


def test_calendar_store_persists_event_ts_ms():
    """Test that calendar store persists event_ts_ms as source of truth"""
    r = FakeRedis()
    w = CalendarStoreWorker(redis=r, pg=None)

    now_ms = get_ny_time_millis()
    event_ts = now_ms + 10000  # 10 seconds in future

    msg = {
        "uid": "e1",
        "event_ts_ms": str(event_ts),
        "ingested_ts_ms": str(now_ms),
        "country": "US",
        "currency": "USD",
        "title": "FOMC Statement",
        "importance": "3",
        "forecast": "",
        "previous": "",
        "unit": "",
        "source": "fmp",
        "payload": "{}",
    }
    w.handle_message("1-0", msg)

    # Should have event_ts_ms as canonical field
    agg = r.hashes["calendar:agg:crypto"]
    assert int(agg["event_ts_ms"]) == event_ts
    assert int(agg["next_ts_ms"]) == event_ts
    # Should still have legacy event_tminus_sec for compatibility
    expected_tminus = int((event_ts - now_ms) / 1000)
    assert int(agg["event_tminus_sec"]) == expected_tminus
    assert int(agg["updated_ts_ms"]) == now_ms
