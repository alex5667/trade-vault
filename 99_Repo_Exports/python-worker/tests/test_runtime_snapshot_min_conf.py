from common.runtime_snapshot import RuntimeSnapshot


def test_runtime_snapshot_min_conf_defaults(monkeypatch):
    monkeypatch.delenv("MIN_CONF_DEFAULT", raising=False)
    monkeypatch.delenv("MIN_CONF_FACTOR_DEFAULT", raising=False)
    rt = RuntimeSnapshot.load()
    assert rt.min_conf("BTCUSDT") == 70.0
    assert rt.min_conf_factor("BTCUSDT") == 0.45


def test_runtime_snapshot_symbol_override(monkeypatch):
    monkeypatch.setenv("MIN_CONF_DEFAULT", "71")
    monkeypatch.setenv("MIN_CONF_FACTOR_DEFAULT", "0.40")
    monkeypatch.setenv("MIN_CONF_BTCUSDT", "66")
    monkeypatch.setenv("MIN_CONF_FACTOR_BTCUSDT", "0.55")
    rt = RuntimeSnapshot.load()
    assert rt.min_conf("BTCUSDT") == 66.0
    assert rt.min_conf_factor("BTCUSDT") == 0.55
    assert rt.min_conf("ETHUSDT") == 71.0
    assert rt.min_conf_factor("ETHUSDT") == 0.40
