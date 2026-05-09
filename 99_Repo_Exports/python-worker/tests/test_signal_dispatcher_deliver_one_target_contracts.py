from __future__ import annotations

import json

import pytest

from services.signal_dispatcher import SignalDispatcher


def _mk_dispatcher_stub(monkeypatch):
    """
    Создаём dispatcher без __init__ (без предположений о конфиге),
    руками задаём только поля, которые _deliver_one_target читает в вашем фрагменте.
    """
    d = SignalDispatcher.__new__(SignalDispatcher)

    # keyspace / markers
    d.marker_prefix = "marker"
    d.delivery_marker_ttl_sec = 60

    # notify config
    d.notify_stream = "notify:stream"
    d.notify_signal_counter_key = "notify:counter"
    d.marker_gc_zset = "marker:gc"
    d.notify_signal_every_n = 5
    d._sha_dual = "sha_dual"
    d._sha_main = "sha_main"

    # redis for audit branch (в коде: self.redis)
    d.redis = object()

    # no-op адаптер notify (в вашем коде try/except вокруг него)
    d._adapt_notify_payload = lambda **kwargs: None

    # фиксируем flatten (чтобы тест не зависел от реализации)
    d._flatten_notify_fields = lambda payload: ["k1", "v1", "k2", "v2"]

    # Mock trace functions to avoid import issues
    d.ensure_env_trace = lambda *a, **k: None
    d.append_env_trace_event = lambda *a, **k: None

    # capture _evalsha_or_eval
    calls = []

    def _evalsha_or_eval(client, sha, op_name, lua_src, numkeys, *args):
        calls.append(
            {
                "client": client,
                "sha": sha,
                "op_name": op_name,
                "numkeys": int(numkeys),
                "args": list(args),
            }
        )
        return None

    d._evalsha_or_eval = _evalsha_or_eval
    return d, calls


def test_notify_missing_payload_raises(monkeypatch):
    d, calls = _mk_dispatcher_stub(monkeypatch)

    env = {}
    sid = "sid1"
    targets_obj = {"notify": None}
    meta = {}

    with pytest.raises(Exception) as e:
        d._deliver_one_target(
            env=env,
            sid=sid,
            target="notify",
            targets_obj=targets_obj,
            meta=meta,
            dual_client=object(),
            simple_client=object(),
        )

    assert "notify missing targets.notify payload" in str(e.value)
    assert calls == []


def test_notify_missing_dual_client_raises(monkeypatch):
    d, calls = _mk_dispatcher_stub(monkeypatch)

    env = {}
    sid = "sid2"
    targets_obj = {"notify": {"text": "hi"}}
    meta = {}

    with pytest.raises(Exception) as e:
        d._deliver_one_target(
            env=env,
            sid=sid,
            target="notify",
            targets_obj=targets_obj,
            meta=meta,
            dual_client=None,
            simple_client=object(),
        )

    assert "notify missing dual_client redis" in str(e.value)
    assert calls == []


def test_notify_success_calls_lua_once_with_marker_key(monkeypatch):
    d, calls = _mk_dispatcher_stub(monkeypatch)

    env = {}
    sid = "sid3"
    targets_obj = {"notify": {"text": "hi"}}
    meta = {}

    dual = object()
    d._deliver_one_target(
        env=env,
        sid=sid,
        target="notify",
        targets_obj=targets_obj,
        meta=meta,
        dual_client=dual,
        simple_client=object(),
    )

    assert env.get("trace_id") == sid  # fallback trace_id = sid
    assert len(calls) == 1
    c = calls[0]
    assert c["client"] is dual
    assert c["op_name"] == "notify_gate"
    assert c["numkeys"] == 4

    marker_key = d._delivery_key("notify", sid)
    assert marker_key == f"{d.marker_prefix}:notify:{sid}"

    # keys order from your snippet:
    # 4, marker_key, notify_stream, notify_signal_counter_key, marker_gc_zset, ...
    args = c["args"]
    assert args[0] == marker_key
    assert args[1] == d.notify_stream
    assert args[2] == d.notify_signal_counter_key
    assert args[3] == d.marker_gc_zset

    # field_count + flat
    field_count = args[10]
    flat = args[11:]
    assert field_count == "2"
    assert flat == ["k1", "v1", "k2", "v2"]


def test_signal_stream_missing_stream_raises(monkeypatch):
    d, calls = _mk_dispatcher_stub(monkeypatch)

    env = {}
    sid = "sid4"
    targets_obj = {"signal_stream_payload": {"a": 1}}
    meta = {}  # missing meta.signal_stream

    with pytest.raises(Exception) as e:
        d._deliver_one_target(
            env=env,
            sid=sid,
            target="signal_stream",
            targets_obj=targets_obj,
            meta=meta,
            dual_client=object(),
            simple_client=object(),
        )

    assert "signal_stream missing meta.signal_stream" in str(e.value)
    assert calls == []


def test_signal_stream_success_compact_json_and_sid_trace_id(monkeypatch):
    d, calls = _mk_dispatcher_stub(monkeypatch)

    env = {}  # no trace_id => trace_id becomes sid
    sid = "sid5"
    payload = {"x": 1}
    targets_obj = {"signal_stream_payload": payload}
    meta = {"signal_stream": "sig:stream"}

    simple = object()
    d._deliver_one_target(
        env=env,
        sid=sid,
        target="signal_stream",
        targets_obj=targets_obj,
        meta=meta,
        dual_client=object(),
        simple_client=simple,
    )

    assert env.get("trace_id") == sid
    assert len(calls) == 1

    c = calls[0]
    assert c["client"] is simple
    assert c["op_name"] == "deliver"
    assert c["numkeys"] == 3

    marker_key = d._delivery_key("signal_stream", sid)
    args = c["args"]

    # 3, marker_key, stream_name, marker_gc_zset, ttl, "xadd", "1000", sid, payload_json
    assert args[0] == marker_key
    assert args[1] == "sig:stream"
    assert args[2] == d.marker_gc_zset

    payload_json = args[-1]
    assert isinstance(payload_json, str)
    assert ": " not in payload_json
    assert ", " not in payload_json

    obj = json.loads(payload_json)
    assert obj["sid"] == sid
    assert obj["trace_id"] == sid  # because env.trace_id was sid
    assert obj["x"] == 1


def test_manual_missing_prereqs_raise(monkeypatch):
    d, calls = _mk_dispatcher_stub(monkeypatch)

    env = {}
    sid = "sid6"
    targets_obj = {"manual_payload": {"x": 1}}
    meta = {}  # missing manual_stream

    with pytest.raises(Exception) as e:
        d._deliver_one_target(
            env=env,
            sid=sid,
            target="manual",
            targets_obj=targets_obj,
            meta=meta,
            dual_client=object(),
            simple_client=object(),
        )

    assert "manual missing meta.manual_stream" in str(e.value)
    assert calls == []
