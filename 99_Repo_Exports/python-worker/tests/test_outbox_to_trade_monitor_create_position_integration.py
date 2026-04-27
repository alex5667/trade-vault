import json

from core.signal_outbox import SignalOutboxPublisher, OutboxSettings
from handlers.emitter.outbox_publisher_adapter import OutboxPublisherAdapter
from runners.trade_monitor_runner import _parse_signal
from services.trade_monitor import TradeMonitorService
from domain.handlers import create_position


class FakeRedisLuaOutbox:
    """
    Minimal Redis stub for SignalOutboxPublisher.publish() Lua path:
    SET dedup_key NX PX ttl
    XADD stream ... 'data' envelope_json
    Also supports script_load/eval/evalsha.
    """
    def __init__(self):
        self.kv = {}
        self.streams = {}  # stream -> list[(id, fields)]
        self._seq = 0
        self._sha = "FAKE_SHA"

    def script_load(self, script: str):
        return self._sha

    def evalsha(self, sha, numkeys, *args):
        return self.eval("lua", numkeys, *args)

    def eval(self, script, numkeys, *args):
        # args = [dedup_key, stream_key, dedup_ttl_ms, maxlen, envelope_json]
        dedup_key = args[0]
        stream = args[1]
        envelope_json = args[4]
        if dedup_key in self.kv:
            return [0]
        self.kv[dedup_key] = "1"
        self._seq += 1
        msg_id = f"{self._seq}-0"
        self.streams.setdefault(stream, []).append((msg_id, {"data": envelope_json}))
        return [1, msg_id]


class DummySpec:
    contract_size = 1.0
    trailing_profile_default = "rocket_v1"
    trailing_min_lock_r = 0.25
    trailing_tp1_offset_atr = 0.5
    stop_atr_mult = 1.0
    rr_levels = [1.0, 2.0, 3.0]
    def risk_money(self, entry_price, sl, lot, direction):
        # simplistic: abs(entry-sl)*lot
        return abs(float(entry_price) - float(sl)) * float(lot)


def _make_tm_for_test():
    # Instantiate without calling __init__ (avoid infra deps)
    tm = TradeMonitorService.__new__(TradeMonitorService)
    tm.default_lot = 1.0
    tm.stop_atr_mult = 1.0
    tm.rr_levels = [1.0, 2.0, 3.0]
    tm._get_spec = lambda symbol: DummySpec()
    return tm


def test_outbox_adapter_to_trade_monitor_to_create_position():
    r = FakeRedisLuaOutbox()

    settings = OutboxSettings(
        outbox_stream="stream:signals:outbox_test",
        outbox_maxlen=20000,
        dedup_ttl_ms=60000,
        dedup_bucket_ms=60000,
    )
    pub = SignalOutboxPublisher(settings=settings, redis_client=r)
    adapter = OutboxPublisherAdapter(
        outbox_publisher=pub,
        default_source="CryptoOrderFlow",
        default_strategy="absorption",
        dedup_bucket_ms=60000,
        dedup_ttl_ms=60000,
    )

    # Payload that will be used as envelope JSON under 'data'
    payload = {
        "signal_id": "sid-1",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "kind": "absorption",
        "strategy_source": "CryptoOrderFlow",
        "strategy": "absorption",
        "timeframe": "1m",
        "ts": 1700000000,  # seconds on purpose: normalize_ts_ms must fix
        "price": 100.0,
        "entry": 100.0,
        "sl": 99.0,
        "tp_levels": [101.0, 102.0, 103.0],
        "trail_profile": "rocket_v1",
        "trailing_min_lock_r": 0.25,
        "trail_after_tp1": 0,
        "trail_after_tp1_reason": "LOW_MOMENTUM",
        "venue": "binance_futures",
    }

    msg_id = adapter.publish(payload)
    assert msg_id is not None

    # Simulate trade_monitor_runner stream read (fields dict)
    stream_items = r.streams["stream:signals:outbox_test"]
    assert len(stream_items) == 1
    _, fields = stream_items[0]

    raw = _parse_signal(fields)
    assert raw["symbol"] == "BTCUSDT"
    assert raw["trail_after_tp1"] == 0

    tm = _make_tm_for_test()
    sig = tm._normalize_signal(raw)
    assert sig is not None

    pos = create_position(sig, DummySpec())
    assert pos.symbol == "BTCUSDT"
    assert pos.tf == "1m"
    # trail flags must persist all the way
    assert pos.trail_after_tp1 is False
    assert pos.trail_after_tp1_reason == "LOW_MOMENTUM"
    # seconds -> ms normalization must have happened in normalize_signal
    assert int(pos.entry_ts_ms) >= 1_000_000_000_000
