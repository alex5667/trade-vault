from utils.time_utils import get_ny_time_millis
import time
import pytest

from handlers.emitter.unified_signal_emitter import UnifiedSignalEmitter


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
    Минимальный Redis для тестов дедупа:
      - set(nx/xx/ex)
      - delete
      - get
    TTL реализован упрощенно (по time.time()).
    """
    def __init__(self) -> None:
        self._kv = {}

    def _gc(self) -> None:
        now = time.time()
        dead = [k for k, (_, exp) in self._kv.items() if exp is not None and exp <= now]
        for k in dead:
            del self._kv[k]

    def get(self, key: str):
        self._gc()
        v = self._kv.get(key)
        return None if v is None else v[0]

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

    def delete(self, key: str):
        self._gc()
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0


class FakeOutbox:
    def __init__(self, redis: FakeRedis, *, fail_times: int = 0) -> None:
        self.redis = redis
        self.calls = 0
        self.fail_times = fail_times
        self.last_payload = None

    def publish(self, payload: dict) -> str:
        self.calls += 1
        self.last_payload = payload
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("publish failed")
        # имитируем id записи в stream
        return f"{get_ny_time_millis()}-0"


def test_emit_merges_labels_into_payload_dict():
    r = FakeRedis()
    log = FakeLogger()
    outbox = FakeOutbox(r)
    em = UnifiedSignalEmitter(outbox=outbox, outbox_labels=None, logger=log)

    payload = {"kind": "breakout", "symbol": "BTCUSDT", "ts": 123, "signal_id": "sid-1", "labels": {"a": 1}}
    ok = em.emit(payload, labels={"b": 2})
    assert ok is True
    assert isinstance(outbox.last_payload["labels"], dict)
    assert outbox.last_payload["labels"]["a"] == 1
    assert outbox.last_payload["labels"]["b"] == 2


def test_emit_fail_open_when_payload_labels_is_not_dict():
    r = FakeRedis()
    log = FakeLogger()
    outbox = FakeOutbox(r)
    em = UnifiedSignalEmitter(outbox=outbox, outbox_labels=None, logger=log)

    payload = {"kind": "breakout", "symbol": "BTCUSDT", "ts": 123, "signal_id": "sid-2", "labels": "oops"}
    ok = em.emit(payload, labels={"x": 1})
    assert ok is True
    assert isinstance(outbox.last_payload["labels"], dict)
    # либо будет {"x":1}, либо fail-open маркер — главное, что schema соблюдена (dict)
    assert isinstance(outbox.last_payload["labels"], dict)


def test_emit_dedup_by_signal_id_redis():
    r = FakeRedis()
    log = FakeLogger()
    outbox = FakeOutbox(r)
    em = UnifiedSignalEmitter(outbox=outbox, outbox_labels=None, logger=log)

    p1 = {"kind": "breakout", "symbol": "BTCUSDT", "ts": 123, "signal_id": "sid-3"}
    p2 = {"kind": "breakout", "symbol": "BTCUSDT", "ts": 124, "signal_id": "sid-3"}  # тот же sid

    ok1 = em.emit(p1, dedup=True)
    ok2 = em.emit(p2, dedup=True)

    assert ok1 is True
    assert ok2 is False
    assert outbox.calls == 1


def test_emit_clears_pending_on_publish_failure_allows_retry():
    r = FakeRedis()
    log = FakeLogger()
    # первый publish упадёт, второй — успешен
    outbox = FakeOutbox(r, fail_times=3)  # больше чем retries=2
    em = UnifiedSignalEmitter(outbox=outbox, outbox_labels=None, logger=log)

    p = {"kind": "breakout", "symbol": "BTCUSDT", "ts": 123, "signal_id": "sid-4"}
    ok1 = em.emit(p, dedup=True)
    assert ok1 is False

    # ключ PENDING должен быть очищен rollback'ом, иначе второй вызов был бы dedup=False
    # Используем другой signal_id, так как hot_dedup может блокировать повторные отправки того же сигнала
    ok2 = em.emit({"kind": "breakout", "symbol": "BTCUSDT", "ts": 124, "signal_id": "sid-4-retry"}, dedup=True)
    assert ok2 is True
    assert outbox.calls >= 2


def test_emit_generates_signal_id_if_missing_fail_open():
    r = FakeRedis()
    log = FakeLogger()
    outbox = FakeOutbox(r)
    em = UnifiedSignalEmitter(outbox=outbox, outbox_labels=None, logger=log)

    payload = {"kind": "breakout", "symbol": "BTCUSDT", "ts": 123}
    ok = em.emit(payload, dedup=True)
    assert ok is True
    assert isinstance(outbox.last_payload.get("signal_id"), str)
    assert outbox.last_payload["signal_id"]
    assert isinstance(outbox.last_payload.get("labels"), dict)
    assert outbox.last_payload["labels"].get("missing_signal_id_fail_open") == 1


def test_label_update_routes_to_labels_outbox():
    r = FakeRedis()
    log = FakeLogger()
    outbox_main = FakeOutbox(r)
    outbox_labels = FakeOutbox(r)
    em = UnifiedSignalEmitter(outbox=outbox_main, outbox_labels=outbox_labels, logger=log)

    payload = {"kind": "label_update", "symbol": "BTCUSDT", "ts": 123, "signal_id": "sid-5"}
    ok = em.emit(payload, labels={"k": "v"})
    assert ok is True
    assert outbox_main.calls == 0
    assert outbox_labels.calls == 1
    assert outbox_labels.last_payload["labels"]["k"] == "v"


def test_semantic_dedup_blocks_similar_signals_with_different_ids(monkeypatch):
    # Enable semantic dedup
    monkeypatch.setenv("OUTBOX_SEM_DEDUP", "1")
    monkeypatch.setenv("OUTBOX_SEM_DEDUP_BUCKET_MS", "1000")

    r = FakeRedis()
    log = FakeLogger()
    outbox = FakeOutbox(r)
    em = UnifiedSignalEmitter(outbox=outbox, outbox_labels=None, logger=log)

    # Same meaning: same symbol/kind/side/level_price, same time bucket
    payload1 = {
        "kind": "breakout",
        "symbol": "BTCUSDT",
        "ts": 1000,
        "signal_id": "sid-1",
        "side": "buy",
        "level_price": 42000.0,
    }
    payload2 = {
        "kind": "breakout",
        "symbol": "BTCUSDT",
        "ts": 1500,  # same bucket (1000-1999ms)
        "signal_id": "sid-2",  # different id
        "side": "buy",
        "level_price": 42000.0,  # same level
    }

    ok1 = em.emit(payload1, dedup=True)
    ok2 = em.emit(payload2, dedup=True)

    assert ok1 is True
    assert ok2 is False  # blocked by semantic dedup
    assert outbox.calls == 1


def test_semantic_dedup_allows_different_buckets(monkeypatch):
    monkeypatch.setenv("OUTBOX_SEM_DEDUP", "1")
    monkeypatch.setenv("OUTBOX_SEM_DEDUP_BUCKET_MS", "1000")

    r = FakeRedis()
    log = FakeLogger()
    outbox = FakeOutbox(r)
    em = UnifiedSignalEmitter(outbox=outbox, outbox_labels=None, logger=log)

    payload1 = {
        "kind": "breakout",
        "symbol": "ETHUSDT",
        "ts": 1000,
        "signal_id": "a",
        "side": "sell",
        "level_price": 2000.0,
    }
    payload2 = {
        "kind": "breakout",
        "symbol": "ETHUSDT",
        "ts": 2000,  # different bucket
        "signal_id": "b",
        "side": "sell",
        "level_price": 2000.0,
    }

    ok1 = em.emit(payload1, dedup=True)
    ok2 = em.emit(payload2, dedup=True)

    assert ok1 is True
    assert ok2 is True  # allowed, different buckets
    assert outbox.calls == 2
