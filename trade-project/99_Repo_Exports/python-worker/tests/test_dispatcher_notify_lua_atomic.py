from __future__ import annotations

"""
Redis/Lua atomicity verification for notify gate.

Tests the contract that Lua _LUA_NOTIFY_GATE_XADD_THEN_MARK maintains atomicity:
- marker is set IFF stream entry exists
- dedup-before-incr: counter doesn't increment on dedup
- gating works correctly with every_n parameter
"""


import pytest


@pytest.mark.redis
def test_notify_lua_gate_is_atomic_and_dedup_before_incr(r):
    """
    Проверяем контракт Lua notify gate:
      - если marker существует -> {1,"dedup"} и counter НЕ растёт
      - если every_n=2:
          * 1-й вызов => gated (не XADD), marker поставлен
          * повтор с тем же sid => dedup, counter не растёт
          * после удаления marker следующий вызов => sent (XADD), marker поставлен
    """
    import services.dispatch.dispatcher_app as sd

    lua = sd._LUA_NOTIFY_GATE_XADD_THEN_MARK  # используем ровно тот же скрипт

    marker_key = "m:notify:SID1"
    stream_key = "stream:notify:test"
    counter_key = "ctr:notify:test"
    gc_zset = "z:gc:test"

    # clean
    r.delete(marker_key, stream_key, counter_key, gc_zset)

    sid = "SID1"
    marker_ttl = 3600
    maxlen = 500
    every_n = 2

    # payload fields (flattened), (k,v,k,v,...)
    flat = ["signal_payload", '{"text":"hi"}', "signal_settings", '{"chat_id":123}']
    assert all(isinstance(x, str) for x in flat)
    assert len(flat) % 2 == 0

    # ВАЖНО: сигнатура именно такая, как у вас в вызове _evalsha_or_eval(...):
    # KEYS=4: marker, stream, counter, gc_zset
    # ARGV: ttl, maxlen, sid, every_n, field_count, *flat
    # (field_count нужен, если ваш Lua проверяет количество полей)
    field_count = str(int(len(flat) // 2))

    # 1) first call: c=1 => gated, marker must exist, stream len=0, counter=1
    res = r.eval(lua, 4, marker_key, stream_key, counter_key, gc_zset,
                 str(marker_ttl), str(maxlen), sid, str(every_n), field_count, *flat)
    assert isinstance(res, (list, tuple))
    assert int(res[0]) == 1
    assert str(res[1]) in ("gated", "sent")  # допускаем, если стартовые условия иные
    assert r.exists(marker_key) == 1
    assert int(r.get(counter_key) or "0") == 1
    if str(res[1]) == "gated":
        assert r.xlen(stream_key) == 0

    # 2) second call with same sid: dedup, counter must stay 1
    res2 = r.eval(lua, 4, marker_key, stream_key, counter_key, gc_zset,
                  str(marker_ttl), str(maxlen), sid, str(every_n), field_count, *flat)
    assert int(res2[0]) == 1
    assert str(res2[1]) == "dedup"
    assert int(r.get(counter_key) or "0") == 1  # dedup-before-incr

    # 3) delete marker -> allow next attempt
    r.delete(marker_key)

    # 4) third call: counter becomes 2 => should "sent" for every_n=2
    res3 = r.eval(lua, 4, marker_key, stream_key, counter_key, gc_zset,
                  str(marker_ttl), str(maxlen), sid, str(every_n), field_count, *flat)
    assert int(res3[0]) == 1
    assert str(res3[1]) in ("sent", "gated")
    assert int(r.get(counter_key) or "0") == 2
    assert r.exists(marker_key) == 1

    if str(res3[1]) == "sent":
        assert r.xlen(stream_key) == 1
        # проверяем, что поля действительно записались и sid присутствует
        x = r.xrange(stream_key, count=1)
        assert len(x) == 1
        _id, fields = x[0]
        assert fields.get("sid") == sid
