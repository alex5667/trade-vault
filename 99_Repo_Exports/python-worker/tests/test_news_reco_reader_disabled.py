import asyncio
import os

from services.news_reco_reader.reader import ensure_started, shutdown


def test_disabled_reader_does_not_start():
    os.environ["TRADE_NEWS_RECO_READER_ENABLE"] = "0"
    r = asyncio.run(ensure_started())
    assert r is None
    asyncio.run(shutdown())
