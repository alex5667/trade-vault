from __future__ import annotations

import json


class DummyRedis:
    def __init__(self, kv):
        self.kv = dict(kv)

    def get(self, key: str):
        return self.kv.get(key)


def test_attach_outbox_meta_merges_config_params(monkeypatch):
    from telegram_worker.outbox_meta import attach_outbox_meta

    monkeypatch.setenv("OUTBOX_META_PREFIX", "outbox:meta:")

    signal_id = "abc123"
    meta = {"config_params": {"delta_z_threshold": 3.2, "min_signal_interval_sec": 60}}
    r = DummyRedis({"outbox:meta:abc123": json.dumps(meta).encode("utf-8")})

    parsed = {"signal_id": signal_id, "symbol": "BTCUSDT"}
    attach_outbox_meta(r, parsed)

    assert "signal_settings" in parsed
    assert "config_params" in parsed["signal_settings"]
    assert parsed["signal_settings"]["config_params"]["delta_z_threshold"] == 3.2


def test_attach_outbox_meta_is_fail_open(monkeypatch):
    from telegram_worker.outbox_meta import attach_outbox_meta

    monkeypatch.setenv("OUTBOX_META_PREFIX", "outbox:meta:")
    r = DummyRedis({"outbox:meta:abc123": b"not-json"})

    parsed = {"signal_id": "abc123"}
    attach_outbox_meta(r, parsed)
    # не должно упасть и не должно мусорить parsed
    assert isinstance(parsed, dict)
