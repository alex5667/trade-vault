
import pytest

from services.signal_dispatcher import SignalDispatcher
from core.redis_keys import RedisStreams as RS


@pytest.fixture()
def dispatcher(r, monkeypatch):
    d = SignalDispatcher()
    d.redis = r
    d.simple_redis = r
    d.dual_redis = r

    d.outbox_stream = RS.SIGNAL_OUTBOX
    d.group = "g"
    d.consumer = "c"

    d.msg_done_prefix = "signal:outbox:msg_done:test"
    d.done_ttl_sec = 120

    # stub deliver: count calls
    d._deliver_calls = 0
    def fake_deliver(env, sid, *a, **k):
        d._deliver_calls += 1

    monkeypatch.setattr(d, "_deliver_targets_with_retry", fake_deliver, raising=True)

    # stub xack: count calls (без реального XGROUP)
    d._xack_calls = 0
    def fake_xack_only(*, msg_id: str):
        d._xack_calls += 1

    monkeypatch.setattr(d, "_xack_only", fake_xack_only, raising=True)
    return d


def test_ack_only_when_msg_done(dispatcher, r):
    sid = "sid1"
    msg_id = "1700000000000-0"
    env = {"sid": sid, "targets": {}, "meta": {}}

    # имитируем падение ПОСЛЕ delivery и ДО XACK в прошлом прогоне:
    dispatcher._mark_msg_done(msg_id)

    dispatcher._process_one_outbox_message(msg_id=msg_id, env=env, sid=sid)

    assert dispatcher._deliver_calls == 0  # ключевое: никаких side-effects
    assert dispatcher._xack_calls == 1


def test_normal_path_deliver_then_mark_then_ack(dispatcher, r):
    sid = "sid2"
    msg_id = "1700000000001-0"
    env = {"sid": sid, "targets": {}, "meta": {}}

    dispatcher._process_one_outbox_message(msg_id=msg_id, env=env, sid=sid)

    assert dispatcher._deliver_calls == 1
    assert dispatcher._xack_calls == 1
    assert r.exists(dispatcher._msg_done_key(msg_id)) == 1
