
from common.runtime_snapshot import RuntimeSnapshot


def test_runtime_snapshot_scans_symbol_overrides(monkeypatch):
    monkeypatch.setenv("MIN_CONF_DEFAULT", "70")
    monkeypatch.setenv("MIN_CONF_FACTOR_DEFAULT", "0.45")
    monkeypatch.setenv("MIN_CONF_BTCUSDT", "80")
    monkeypatch.setenv("MIN_CONF_FACTOR_BTCUSDT", "0.55")

    rt = RuntimeSnapshot.load()
    assert rt.min_conf("BTCUSDT") == 80.0
    assert rt.min_conf_factor("BTCUSDT") == 0.55
    assert rt.min_conf("ETHUSDT") == 70.0
    assert rt.min_conf_factor("ETHUSDT") == 0.45
