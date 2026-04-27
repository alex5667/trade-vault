import threading
import time

from services.sharded_serial_executor import ShardedSerialExecutor


def test_serial_order_same_key():
    ex = ShardedSerialExecutor(shards=2, queue_max=1000, submit_timeout_s=1.0)
    out = []
    lock = threading.Lock()

    def mk(i):
        def _fn():
            # emulate work
            time.sleep(0.005)
            with lock:
                out.append(i)
        return _fn

    futures = []
    for i in range(50):
        futures.append(ex.submit("BTCUSDT", mk(i), name=f"t{i}"))

    for f in futures:
        f.result(timeout=2.0)

    assert out == list(range(50))
    ex.shutdown()


def test_parallel_keys_can_interleave():
    ex = ShardedSerialExecutor(shards=4, queue_max=1000, submit_timeout_s=1.0)
    seen = set()
    lock = threading.Lock()

    def fn(k, i):
        def _():
            time.sleep(0.002)
            with lock:
                seen.add((k, i))
        return _

    futs = []
    for i in range(20):
        futs.append(ex.submit("BTCUSDT", fn("BTCUSDT", i), name="btc"))
        futs.append(ex.submit("ETHUSDT", fn("ETHUSDT", i), name="eth"))

    for f in futs:
        f.result(timeout=2.0)

    assert len(seen) == 40
    ex.shutdown()
