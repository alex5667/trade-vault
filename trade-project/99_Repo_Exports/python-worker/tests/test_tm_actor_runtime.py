from services.trade_monitor_actor_runtime import TradeMonitorActorRuntime


class FakeCore:
    def __init__(self, shard_id: int):
        self.shard_id = shard_id
        self.seq = []

    def on_tick(self, raw):
        # record order per symbol
        self.seq.append(("tick", raw.get("symbol"), raw.get("n")))

    def on_signal(self, raw):
        self.seq.append(("signal", raw.get("symbol"), raw.get("sid")))

    def apply_external_sl_hit(self, signal_id: str, price: float, timestamp: int = 0, source=None, event_id=None):
        self.seq.append(("sl_hit", signal_id, float(price)))
        return True

    def update_trailing_sl(self, signal_id: str, new_sl: float, source=None, profile=None, event_id=None, clear_tp_levels: bool = False):
        self.seq.append(("trailing_started", signal_id, float(new_sl)))
        return True

    def apply_external_tp_hit(self, signal_id: str, tp_level: int, price: float, timestamp: int = 0, event_id=None):
        self.seq.append(("tp_hit", signal_id, int(tp_level), float(price)))
        return True


def test_same_symbol_is_serial():
    rt = TradeMonitorActorRuntime(core_factory=lambda sid: FakeCore(sid))

    # send 100 ticks for same symbol concurrently
    futs = []
    for i in range(100):
        futs.append(rt.submit_tick(symbol="BTCUSDT", raw_tick={"symbol": "BTCUSDT", "n": i}))

    for f in futs:
        f.result(timeout=2.0)

    # all went to same shard/core and preserved submit order per shard queue
    core = rt._core_for_key("BTCUSDT")
    assert core.seq == [("tick", "BTCUSDT", i) for i in range(100)]
    rt.shutdown()


def test_different_symbols_can_parallelize():
    rt = TradeMonitorActorRuntime(core_factory=lambda sid: FakeCore(sid))
    futs = []
    for i in range(200):
        sym = "BTCUSDT" if i % 2 == 0 else "ETHUSDT"
        futs.append(rt.submit_tick(symbol=sym, raw_tick={"symbol": sym, "n": i}))
    for f in futs:
        f.result(timeout=3.0)

    # Both cores should have received their respective ticks
    btc_core = rt._core_for_key("BTCUSDT")
    eth_core = rt._core_for_key("ETHUSDT")

    btc_ticks = [x for x in btc_core.seq if x[0] == "tick"]
    eth_ticks = [x for x in eth_core.seq if x[0] == "tick"]

    assert len(btc_ticks) == 100
    assert len(eth_ticks) == 100
    rt.shutdown()
