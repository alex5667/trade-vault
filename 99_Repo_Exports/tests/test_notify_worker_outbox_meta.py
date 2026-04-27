from __future__ import annotations

import json


class FakeRedis:
    def __init__(self, kv: dict[str, object]):
        self._kv = kv

    def get(self, key: str):
        return self._kv.get(key)


def test_attach_outbox_meta_adds_config_params(monkeypatch):
    monkeypatch.setenv("OUTBOX_META_PREFIX", "signal:meta:")
    monkeypatch.setenv("TG_INCLUDE_CONFIG_PARAMS", "1")

    from telegram_worker.notify_worker import _attach_outbox_meta  # путь подстройте под ваш пакет/импорт

    signal_id = "abc"
    meta_key = f"signal:meta:{signal_id}"
    meta_val = {"config_params": {"delta_window_ticks": 200, "tp_rr": 1.8}}
    r = FakeRedis({meta_key: json.dumps(meta_val).encode("utf-8")})

    entry = {"signal_payload": {"signal_id": signal_id, "symbol": "BTCUSDT"}}
    parsed = dict(entry["signal_payload"])
    raw = {}

    _attach_outbox_meta(r, entry=entry, parsed=parsed, raw=raw)

    assert "config_params" in parsed
    assert parsed["config_params"]["delta_window_ticks"] == 200
    assert raw["signal_id"] == signal_id
    assert "config_params" in raw


def test_attach_outbox_meta_respects_disable(monkeypatch):
    monkeypatch.setenv("OUTBOX_META_PREFIX", "signal:meta:")
    monkeypatch.setenv("TG_INCLUDE_CONFIG_PARAMS", "0")

    from telegram_worker.notify_worker import _attach_outbox_meta  # путь подстройте под ваш пакет/импорт

    r = FakeRedis({"signal:meta:abc": b'{"config_params":{"x":1}}'})
    entry = {"signal_payload": {"signal_id": "abc"}}
    parsed = dict(entry["signal_payload"])
    raw = {}

    _attach_outbox_meta(r, entry=entry, parsed=parsed, raw=raw)
    assert "config_params" not in parsed
    assert "config_params" not in raw


def test_attach_outbox_meta_compacts_keys(monkeypatch):
    monkeypatch.setenv("OUTBOX_META_PREFIX", "signal:meta:")
    monkeypatch.setenv("TG_INCLUDE_CONFIG_PARAMS", "1")
    monkeypatch.setenv("TG_CONFIG_PARAMS_MAX_KEYS", "2")

    from telegram_worker.notify_worker import _attach_outbox_meta  # путь подстройте под ваш пакет/импорт

    meta_val = {"config_params": {"c": 3, "b": 2, "a": 1}}
    r = FakeRedis({"signal:meta:sid": json.dumps(meta_val).encode("utf-8")})
    entry = {"signal_payload": {"signal_id": "sid"}}
    parsed = dict(entry["signal_payload"])
    raw = {}

    _attach_outbox_meta(r, entry=entry, parsed=parsed, raw=raw)
    assert isinstance(parsed["config_params"], dict)
    assert len(parsed["config_params"]) == 2
