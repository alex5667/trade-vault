from __future__ import annotations

from common.runtime_snapshot import RuntimeSnapshot


def test_runtime_snapshot_min_conf_per_symbol(monkeypatch):
    monkeypatch.setenv("MIN_CONF_DEFAULT", "70")
    monkeypatch.setenv("MIN_CONF_BTCUSDT", "80")
    rt = RuntimeSnapshot.load()
    assert rt.min_conf("BTCUSDT") == 80.0
    assert rt.min_conf("ETHUSDT") == 70.0


def test_runtime_snapshot_min_cf_per_symbol(monkeypatch):
    monkeypatch.setenv("MIN_CONF_FACTOR_DEFAULT", "0.45")
    monkeypatch.setenv("MIN_CONF_FACTOR_ETHUSDT", "0.60")
    rt = RuntimeSnapshot.load()
    assert rt.min_conf_factor("ETHUSDT") == 0.60
    assert rt.min_conf_factor("BTCUSDT") == 0.45
