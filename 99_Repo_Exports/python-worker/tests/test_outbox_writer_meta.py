from __future__ import annotations

import os
import types


class FakeRedis:
    def __init__(self, reply):
        self.reply = reply
        self.last_eval = None

    def eval(self, script, numkeys, *args):
        # сохраняем всё для ассертов
        self.last_eval = (script, numkeys, args)
        return self.reply


def _mk_writer():
    """
    Создаём OutboxWriter без __init__ (чтобы тест был независим от конструктора).
    Подставляем только то, что нужно для _atomic_xadd.
    """
    from handlers.emitter.outbox_writer import OutboxWriter

    w = OutboxWriter.__new__(OutboxWriter)
    w._logger = types.SimpleNamespace(warning=lambda *a, **k: None)
    w._dedup_ttl_ms = 30_000
    w._dedup_pending_ttl_ms = 5_000
    w._sem_ttl_ms = 30_000
    w._sem_pending_ttl_ms = 5_000
    w._maxlen = 1000

    # минимальные заглушки
    w._dedup_key = lambda sid: f"dedup:{sid}"
    w._sem_key = lambda payload: "__none__"
    w._serialize_payload = lambda obj: '{"ok":1}'
    # meta helpers из патча
    w._meta_key = lambda sid: f"{os.getenv('OUTBOX_META_PREFIX','signal:meta:')}{sid}"
    return w


def test_lua_contains_maxlen_and_meta_block():
    from handlers.emitter.outbox_writer import OutboxWriter

    lua = OutboxWriter._LUA_ATOMIC_XADD
    assert "local maxlen = tonumber(ARGV[8]) or 0" in lua
    assert "KEYS[4]" in lua
    assert "ARGV[11]" in lua and "ARGV[12]" in lua
    assert "meta sidecar" in lua or "meta_json" in lua


def test_atomic_xadd_passes_meta_as_4th_key_and_argv_11_12(monkeypatch):
    monkeypatch.setenv("OUTBOX_META_PREFIX", "signal:meta:")

    w = _mk_writer()
    r = FakeRedis([1, "123-0"])

    entry_id = w._atomic_xadd(
        r,
        stream_key="signals:test",
        payload={"kind": "breakout", "symbol": "BTCUSDT", "ts": 1},
        signal_id="abc",
        meta_json='{"config_params":{"x":1}}',
        meta_ttl_sec=60,
    )

    assert entry_id == "123-0"
    assert r.last_eval is not None

    script, numkeys, args = r.last_eval
    assert numkeys == 4

    # args = KEYS(4) + ARGV(12)
    # KEYS:
    assert args[0] == "dedup:abc"
    assert args[1] == "__none__"
    assert args[2] == "signals:test"
    assert args[3] == "signal:meta:abc"

    # ARGV[11], ARGV[12] — в конце списка аргументов
    assert args[-2] == '{"config_params":{"x":1}}'
    assert args[-1] == 60


def test_atomic_xadd_disables_meta_when_empty(monkeypatch):
    monkeypatch.setenv("OUTBOX_META_PREFIX", "signal:meta:")

    w = _mk_writer()
    r = FakeRedis([1, "1-0"])

    entry_id = w._atomic_xadd(
        r,
        stream_key="signals:test",
        payload={"kind": "k", "symbol": "S", "ts": 1},
        signal_id="sid",
        meta_json="",
        meta_ttl_sec=0,
    )

    assert entry_id == "1-0"
    script, numkeys, args = r.last_eval
    assert numkeys == 4
    # KEYS[4] должен стать "__none__"
    assert args[3] == "__none__"
