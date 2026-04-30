import asyncio
import unittest


class DummyPublisher:
    def __init__(self):
        self.calls = []

    async def xadd_json(self, *, sink, payload, symbol, approximate=True):
        self.calls.append((sink.name, payload, symbol))
        # mimic AsyncPublishResult
        class R:
            ok = True
            busy_loading = False
        return R()


class DummySnap:
    def __init__(self, bid, ask):
        self.best_bid_px = bid
        self.best_ask_px = ask


class DummyBookState:
    def __init__(self, ts_ms, bid, ask):
        self.ts_ms = ts_ms
        self.snap = DummySnap(bid, ask)


class DummyRuntime:
    def __init__(self):
        self.symbol = "BTCUSDT"
        self.config = {"venue": "binance"}
        self.book_state = DummyBookState(1000, 99.0, 101.0)
        self.bbo_ts_last_publish_ms = 0


class TestBBOStore(unittest.TestCase):
    def test_publish_and_throttle(self):
        from services.orderflow.bbo_store import BBOStoreCfg, maybe_publish_bbo

        pub = DummyPublisher()
        rt = DummyRuntime()
        cfg = BBOStoreCfg(
            enabled=True
            stream="events:bbo_ts"
            stream_maxlen=100
            schema_version=1
            min_interval_ms=100
            symbols_allow=set()
            venue_default="binance"
        )

        async def run():
            await maybe_publish_bbo(publisher=pub, cfg=cfg, runtime=rt, book_ts_ms=1000)
            await maybe_publish_bbo(publisher=pub, cfg=cfg, runtime=rt, book_ts_ms=1050)  # throttled
            await maybe_publish_bbo(publisher=pub, cfg=cfg, runtime=rt, book_ts_ms=1200)

        asyncio.run(run())
        self.assertEqual(len(pub.calls), 2)

    def test_disabled(self):
        """When cfg.enabled=False nothing is published."""
        from services.orderflow.bbo_store import BBOStoreCfg, maybe_publish_bbo

        pub = DummyPublisher()
        rt = DummyRuntime()
        cfg = BBOStoreCfg(
            enabled=False
            stream="events:bbo_ts"
            stream_maxlen=100
            schema_version=1
            min_interval_ms=100
            symbols_allow=set()
            venue_default="binance"
        )

        async def run():
            await maybe_publish_bbo(publisher=pub, cfg=cfg, runtime=rt, book_ts_ms=1000)

        asyncio.run(run())
        self.assertEqual(len(pub.calls), 0)

    def test_symbol_allowlist(self):
        """Symbols not in allowlist are skipped."""
        from services.orderflow.bbo_store import BBOStoreCfg, maybe_publish_bbo

        pub = DummyPublisher()
        rt = DummyRuntime()
        rt.symbol = "ETHUSDT"
        cfg = BBOStoreCfg(
            enabled=True
            stream="events:bbo_ts"
            stream_maxlen=100
            schema_version=1
            min_interval_ms=100
            symbols_allow={"BTCUSDT"},  # ETHUSDT not in list
            venue_default="binance"
        )

        async def run():
            await maybe_publish_bbo(publisher=pub, cfg=cfg, runtime=rt, book_ts_ms=1000)

        asyncio.run(run())
        self.assertEqual(len(pub.calls), 0)


if __name__ == "__main__":
    unittest.main()
