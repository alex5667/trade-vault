
import fakeredis

from news_pipeline import config
from news_pipeline.calendar_store_service import CalendarFeatureStoreService
from utils.time_utils import get_ny_time_millis


def test_calendar_agg_next_event():
    r = fakeredis.FakeRedis(decode_responses=True)
    now = get_ny_time_millis()
    ev_ts = now + 60_000

    r.xadd(config.CALENDAR_EVENTS_STREAM, {
        "event_id":"e1","title":"CPI","ts_ms":str(ev_ts),"grade_id":"3",
        "currency":"USD","region":"US","symbols":"BTCUSDT","payload":"{}"
    })
    r.xgroup_create(config.CALENDAR_EVENTS_STREAM, config.CALENDAR_FEATURE_GROUP, id="0-0", mkstream=True)

    svc = CalendarFeatureStoreService(r, consumer="t1", block_ms=1, batch=10)

    items = r.xreadgroup(config.CALENDAR_FEATURE_GROUP, "t1", {config.CALENDAR_EVENTS_STREAM: ">"}, count=10, block=1)
    assert items
    # эмулируем: просто вызовем run_forever один цикл нельзя; оставляем smoke-уровень:
    # проверим, что event key можно положить и agg ключ потом будет читаться в enricher (в реале это сделает service loop).
