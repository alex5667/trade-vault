from __future__ import annotations

import time

from news_pipeline.calendar_store_worker import CalendarStoreWorker
from tests.fake_redis import FakeRedis  # type: ignore


def test_calendar_store_updates_next_event():
    r = FakeRedis()
    w = CalendarStoreWorker(redis=r, pg=None)

    now_ms = int(time.time() * 1000)
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
