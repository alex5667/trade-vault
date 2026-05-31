from types import SimpleNamespace

# поправьте импорт под ваш проект
from handlers.main_loop_service import MainLoopService


class FakeConsumer:
    def __init__(self):
        self.calls = []

    def read_new(self, streams, count: int, block_ms: int):
        self.calls.append(("read_new", tuple(streams), count, block_ms))
        # возвращаем фиктивные сообщения
        return [SimpleNamespace(stream=streams[0], msg_id="1-0", fields={})]

    def pending_len(self, stream: str) -> int:
        self.calls.append(("pending_len", stream))
        return {"book:BTCUSDT": 11, "l3:BTCUSDT": 22, "ticks:BTCUSDT": 33}.get(stream, 0)


class DummyHM:
    def __init__(self):
        self.pending = []

    def on_pending_len(self, symbol: str, stream_kind: str, pending_len: int) -> None:
        self.pending.append((symbol, stream_kind, pending_len))


def test_main_loop_reads_in_priority_with_quotas_and_pending():
    svc = MainLoopService.__new__(MainLoopService)

    svc.symbol = "BTCUSDT"
    svc.book_stream = "book:BTCUSDT"
    svc.l3_stream = "l3:BTCUSDT"
    svc.tick_stream = "ticks:BTCUSDT"

    svc.config = SimpleNamespace(
        read_count=100,
        read_block_ms=1000,
        read_count_book=60,
        read_count_l3=20,
        read_count_tick=120,
    )

    svc.health_metrics = DummyHM()
    c = FakeConsumer()

    msgs = svc._read_priority_batch(c)

    # Проверяем порядок и аргументы read_new
    assert c.calls[0] == ("read_new", ("book:BTCUSDT",), 60, 200)   # min(1000,200)
    assert c.calls[1] == ("read_new", ("l3:BTCUSDT",), 20, 0)
    # ticks ограничивается до 50, если есть другие сообщения
    assert c.calls[2] == ("read_new", ("ticks:BTCUSDT",), 50, 0)

    # pending_len не вызывается в _read_priority_batch (теперь в _sample_pending с rate limiting)

    # сообщение вернулось (мы возвращаем по одному на read)
    assert len(msgs) == 3
