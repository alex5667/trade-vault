import json
import pytest

from core.outbox_envelope import OutboxEnvelope, make_envelope
from core.outbox_writer import OutboxWriter


class FakeRedis:
    """
    Минимальный fake Redis для unit-тестов:
      - set(nx/xx/ex)
      - delete
      - xadd
    TTL симулируем грубо: ex хранится, но не протухает автоматически (тестам не надо).
    """
    def __init__(self):
        self.kv = {}
        self.streams = {}  # name -> list[(id, fields)]
        self._seq = 0
        self.fail_xadd = False

    def set(self, key, value, nx=False, xx=False, ex=None):
        exists = key in self.kv
        if nx and exists:
            return False
        if xx and not exists:
            return False
        self.kv[key] = (value, ex)
        return True

    def delete(self, key):
        self.kv.pop(key, None)
        return 1

    def xadd(self, stream, fields, **kwargs):
        if self.fail_xadd:
            raise RuntimeError("xadd failed")
        self._seq += 1
        eid = f"{self._seq}-0"
        self.streams.setdefault(stream, []).append((eid, dict(fields)))
        return eid


class FakeLogger:
    def warning(self, msg):  # pragma: no cover
        pass
    def exception(self, msg):  # pragma: no cover
        pass


def test_outbox_writer_idempotent_by_signal_id():
    r = FakeRedis()
    w = OutboxWriter(redis=r, logger=FakeLogger(), stream_name="signals:outbox", max_retries=1)

    env = make_envelope(
        signal_id="sid-1",
        source="test-worker",
        ts_ms=123,
        kind="breakout",
        symbol="BTCUSDT",
        payload={"a": 1},
    )

    res1 = w.write(env)
    assert res1.ok and res1.written and not res1.duplicate
    assert len(r.streams["signals:outbox"]) == 1

    res2 = w.write(env)
    assert res2.ok and (not res2.written) and res2.duplicate
    assert len(r.streams["signals:outbox"]) == 1  # всё ещё 1 запись


def test_outbox_writer_cleans_placeholder_on_xadd_failure_allows_retry():
    r = FakeRedis()
    w = OutboxWriter(redis=r, logger=FakeLogger(), stream_name="signals:outbox", max_retries=1)

    env = make_envelope(
        signal_id="sid-2",
        source="test-worker",
        ts_ms=123,
        kind="absorption",
        symbol="ETHUSDT",
        payload={"x": 1},
    )

    r.fail_xadd = True
    res_fail = w.write(env)
    assert not res_fail.ok
    # placeholder должен быть удалён (иначе следующий write стал бы duplicate и сигнал потерялся)
    assert "outbox:dedup:sid-2" not in r.kv

    r.fail_xadd = False
    res_ok = w.write(env)
    assert res_ok.ok and res_ok.written
    assert len(r.streams["signals:outbox"]) == 1


def test_outbox_envelope_serializes_payload_json():
    env = make_envelope(
        signal_id="sid-3",
        source="test-worker",
        ts_ms=999,
        kind="obi_spike",
        symbol="SOLUSDT",
        payload={"labels": {"a": 1}, "nested": {"b": True}},
    )
    fields = env.to_stream_fields()
    assert "payload_json" in fields
    decoded = json.loads(fields["payload_json"])
    assert decoded["labels"]["a"] == 1
    assert decoded["nested"]["b"] is True


def test_outbox_envelope_contract_fields_present():
    """Все обязательные контрактные поля должны присутствовать в stream fields."""
    env = make_envelope(
        signal_id="sid-contract",
        source="python-worker:crypto_orderflow",
        ts_ms=1_700_000_000_000,
        kind="breakout",
        symbol="BTCUSDT",
        trace_id="trace-abc-123",
        quality_flags=["stale_tick"],
    )
    fields = env.to_stream_fields()
    # обязательные контрактные поля
    REQUIRED = {
        "schema_version", "event_id", "source", "signal_id",
        "event_time_ms", "ingest_time_ms", "trace_id", "quality_flags",
        "kind", "symbol",
    }
    missing = REQUIRED - set(fields.keys())
    assert not missing, f"Missing contract fields: {missing}"
    # проверяем значения
    assert fields["source"] == "python-worker:crypto_orderflow"
    assert fields["trace_id"] == "trace-abc-123"
    assert fields["event_time_ms"] == "1700000000000"
    assert json.loads(fields["quality_flags"]) == ["stale_tick"]
    assert len(fields["event_id"]) == 36  # UUID4
    assert int(fields["ingest_time_ms"]) > 0
    assert fields["schema_version"] == "1"


def test_outbox_envelope_auto_generates_trace_and_event_ids():
    """При отсутствии trace_id — генерируется UUID4; event_id всегда уникален (или по signal_id)."""
    env1 = make_envelope(signal_id="s1", source="svc", ts_ms=1, kind="k", symbol="X")
    env2 = make_envelope(signal_id="s2", source="svc", ts_ms=1, kind="k", symbol="X")
    f1 = env1.to_stream_fields()
    f2 = env2.to_stream_fields()
    assert f1["event_id"] != f2["event_id"]
    assert f1["trace_id"] != f2["trace_id"]
    assert len(f1["trace_id"]) == 36
