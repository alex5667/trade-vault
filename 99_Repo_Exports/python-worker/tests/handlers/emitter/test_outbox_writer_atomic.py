from utils.time_utils import get_ny_time_millis
import time
import pytest
import os

from handlers.emitter.outbox_writer import OutboxWriter


class FakeLogger:
    def __init__(self) -> None:
        self.warns = []
        self.exceptions = []

    def warning(self, msg: str) -> None:
        self.warns.append(str(msg))

    def exception(self, msg: str) -> None:
        self.exceptions.append(str(msg))


class FakeRedis:
    """
    Мини-Redis для атомарного пути:
      - exists/set/delete
      - xadd (streams)
      - eval (мы НЕ исполняем Lua, а имитируем тот же контракт результатов)
    """
    def __init__(self) -> None:
        self._kv = {}
        self._streams = {}
        self.fail_next_xadd = False

    def _gc(self) -> None:
        now = time.time()
        dead = [k for k, (_, exp) in self._kv.items() if exp is not None and exp <= now]
        for k in dead:
            del self._kv[k]

    def exists(self, key: str) -> int:
        self._gc()
        return 1 if key in self._kv else 0

    def set(self, key: str, value: str, nx: bool = False, xx: bool = False, ex: int | None = None):
        self._gc()
        exists = key in self._kv
        if nx and exists:
            return False
        if xx and not exists:
            return False
        exp = None if ex is None else (time.time() + float(ex))
        self._kv[key] = (value, exp)
        return True

    def delete(self, key: str) -> int:
        self._gc()
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def xadd(self, stream: str, fields: dict[str, str], maxlen: int | None = None) -> str:
        if self.fail_next_xadd:
            self.fail_next_xadd = False
            raise RuntimeError("xadd failed")
        entries = self._streams.setdefault(stream, [])
        entry_id = f"{get_ny_time_millis()}-{len(entries)}"
        entries.append((entry_id, dict(fields)))
        if maxlen and maxlen > 0 and len(entries) > maxlen:
            # trim oldest
            self._streams[stream] = entries[-maxlen:]
        return entry_id

    def eval(self, script: str, numkeys: int, *args):
        # контракт OutboxWriter._atomic_xadd:
        # KEYS: dedup_key, sem_key(or "__none__"), stream_key
        # ARGV: dedup_ttl_sec, pending_ttl_sec, signal_id, kind, symbol, ts, payload_json, maxlen, sem_ttl_sec, sem_pending_ttl_sec
        assert numkeys == 3
        dedup_key = args[0]
        sem_key = args[1]
        stream_key = args[2]
        dedup_ttl_sec = int(args[3])
        pending_ttl_sec = int(args[4])
        signal_id = str(args[5])
        kind = str(args[6])
        symbol = str(args[7])
        ts = str(args[8])
        payload_json = str(args[9])
        maxlen = int(args[10])
        sem_ttl_sec = int(args[11])
        sem_pending_ttl_sec = int(args[12])
        sem_enabled = (sem_key != "__none__")

        if self.exists(dedup_key) == 1:
            return [0]
        if sem_enabled and self.exists(sem_key) == 1:
            return [0]
        ok = self.set(dedup_key, "PENDING", nx=True, ex=pending_ttl_sec)
        if not ok:
            return [0]
        if sem_enabled:
            ok2 = self.set(sem_key, "PENDING", nx=True, ex=sem_pending_ttl_sec)
            if not ok2:
                self.delete(dedup_key)
                return [0]
        try:
            entry_id = self.xadd(
                stream_key,
                {
                    "signal_id": signal_id,
                    "kind": kind,
                    "symbol": symbol,
                    "ts": ts,
                    "payload": payload_json,
                },
                maxlen=maxlen,
            )
        except Exception as e:
            self.delete(dedup_key)
            if sem_enabled:
                self.delete(sem_key)
            return [2, str(e)]
        self.set(dedup_key, entry_id, xx=True, ex=dedup_ttl_sec)
        if sem_enabled:
            self.set(sem_key, entry_id, xx=True, ex=sem_ttl_sec)
        return [1, entry_id]


class FakePublisher:
    """
    Publisher, который НЕ должен вызываться в atomic режиме:
    мы хотим убедиться, что OutboxWriter пишет в stream напрямую через redis.eval.
    """
    def __init__(self, redis: FakeRedis, stream_name: str) -> None:
        self.redis = redis
        self.stream_name = stream_name
        self.publish_calls = 0

    def publish(self, payload: dict) -> str:
        self.publish_calls += 1
        raise AssertionError("publish() must not be called in atomic mode")


def _make_writer(redis: FakeRedis, stream: str, retries: int = 1) -> tuple[OutboxWriter, FakePublisher, FakeLogger]:
    log = FakeLogger()
    pub = FakePublisher(redis, stream_name=stream)
    w = OutboxWriter(
        publisher=pub,
        logger=log,
        retries=retries,
        retry_sleep_ms=0,
        dedup_ttl_ms=60000,
        dedup_pending_ttl_ms=60000,
        stream_key=stream,
    )
    return w, pub, log


def test_atomic_write_dedup_by_signal_id_and_no_publish_called():
    r = FakeRedis()
    w, pub, _log = _make_writer(r, "signals:outbox")

    payload = {"kind": "breakout", "symbol": "BTCUSDT", "ts": 123, "signal_id": "sid-A", "labels": {}}
    ok1 = w.write(payload=payload, signal_id="sid-A", dedup=True)
    ok2 = w.write(payload={**payload, "ts": 124}, signal_id="sid-A", dedup=True)

    assert ok1 is True
    assert ok2 is False
    assert pub.publish_calls == 0
    assert len(r._streams["signals:outbox"]) == 1


def test_atomic_write_rollbacks_dedup_on_xadd_failure_allows_retry():
    r = FakeRedis()
    w, pub, log = _make_writer(r, "signals:outbox", retries=0)

    r.fail_next_xadd = True
    payload = {"kind": "breakout", "symbol": "BTCUSDT", "ts": 123, "signal_id": "sid-B"}
    ok1 = w.write(payload=payload, signal_id="sid-B", dedup=True)
    assert ok1 is False
    assert pub.publish_calls == 0
    # dedup key должен быть удалён после xadd failure
    assert r.exists("outbox:dedup:sid-B") == 0

    ok2 = w.write(payload={**payload, "ts": 124}, signal_id="sid-B", dedup=True)
    assert ok2 is True
    assert len(r._streams["signals:outbox"]) == 1


def test_atomic_write_stores_payload_json_and_fields():
    r = FakeRedis()
    w, _pub, _log = _make_writer(r, "signals:outbox")

    payload = {"kind": "absorption", "symbol": "ETHUSDT", "ts": 555, "signal_id": "sid-C", "labels": {"x": 1}}
    ok = w.write(payload=payload, signal_id="sid-C", dedup=True)
    assert ok is True
    entry_id, fields = r._streams["signals:outbox"][0]
    assert fields["signal_id"] == "sid-C"
    assert fields["kind"] == "absorption"
    assert fields["symbol"] == "ETHUSDT"
    assert fields["ts"] == "555"
    assert isinstance(fields["payload"], str)
    assert '"signal_id":"sid-C"' in fields["payload"]


def test_atomic_semantic_dedup_blocks_duplicates_with_different_signal_ids():
    r = FakeRedis()
    log = FakeLogger()
    pub = FakePublisher(r, stream_name="signals:outbox")
    w = OutboxWriter(
        publisher=pub,
        logger=log,
        retries=1,
        retry_sleep_ms=0,
        dedup_ttl_ms=60000,
        dedup_pending_ttl_ms=60000,
        stream_key="signals:outbox",
        sem_enabled=True,
        sem_bucket_ms=1000,
        sem_level_decimals=2,
    )

    # проверяем что семантический дедуп включен
    assert w._sem_enabled is True

    payload1 = {
        "kind": "breakout",
        "symbol": "BTCUSDT",
        "ts": 1000,
        "signal_id": "sid-1",
        "side": "buy",
        "level_price": 42000.001,
    }
    payload2 = {**payload1, "signal_id": "sid-2", "ts": 1500}  # тот же bucket=1000, тот же level округлится

    ok1 = w.write(payload=payload1, signal_id="sid-1", dedup=True)
    ok2 = w.write(payload=payload2, signal_id="sid-2", dedup=True)

    assert ok1 is True
    assert ok2 is False
    assert pub.publish_calls == 0
    assert len(r._streams["signals:outbox"]) == 1


def test_atomic_semantic_dedup_allows_next_bucket():
    r = FakeRedis()
    log = FakeLogger()
    pub = FakePublisher(r, stream_name="signals:outbox")
    w = OutboxWriter(
        publisher=pub,
        logger=log,
        retries=1,
        retry_sleep_ms=0,
        dedup_ttl_ms=60000,
        dedup_pending_ttl_ms=60000,
        stream_key="signals:outbox",
        sem_enabled=True,
        sem_bucket_ms=1000,
    )

    p1 = {"kind": "breakout", "symbol": "ETHUSDT", "ts": 1000, "signal_id": "a", "side": "sell", "level_price": 2000.0}
    p2 = {"kind": "breakout", "symbol": "ETHUSDT", "ts": 2000, "signal_id": "b", "side": "sell", "level_price": 2000.0}

    assert w.write(payload=p1, signal_id="a", dedup=True) is True
    assert w.write(payload=p2, signal_id="b", dedup=True) is True
    assert len(r._streams["signals:outbox"]) == 2
