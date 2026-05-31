from types import SimpleNamespace

from handlers.main_loop_service import MainLoopService


class FakeConsumer:
    def __init__(self):
        self.calls = []

    def read_new(self, streams, count: int, block_ms: int):
        self.calls.append(("read_new", tuple(streams), count, block_ms))
        return []

    def pending_len(self, stream: str) -> int:
        self.calls.append(("pending_len", stream))
        return {"book:BTCUSDT": 11, "l3:BTCUSDT": 22, "ticks:BTCUSDT": 33}.get(stream, 0)


class HM:
    def __init__(self):
        self.pending = []

    def on_pending_len(self, symbol: str, stream_kind: str, pending_len: int) -> None:
        self.pending.append((symbol, stream_kind, int(pending_len)))


def test_main_loop_records_pending_len():
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
        pending_sample_every_ms=1000,  # Для теста
    )

    svc.health_metrics = HM()
    svc._pending_sample_last_ms = 0  # Инициализируем поле
    c = FakeConsumer()

    # Вызываем _sample_pending для тестирования pending sampling
    svc._sample_pending(c)

    assert ("BTCUSDT", "book", 11) in svc.health_metrics.pending
    assert ("BTCUSDT", "l3", 22) in svc.health_metrics.pending
    assert ("BTCUSDT", "ticks", 33) in svc.health_metrics.pending
