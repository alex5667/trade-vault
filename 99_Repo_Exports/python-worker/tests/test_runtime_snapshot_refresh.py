import os
import time

from common.runtime_snapshot import RuntimeSnapshot


def test_runtime_snapshot_basic_getters(monkeypatch):
    monkeypatch.setenv("RUNTIME_SNAPSHOT_REFRESH_EVERY_SEC", "0")
    monkeypatch.setenv("X_FLOAT", "1.25")
    monkeypatch.setenv("X_INT", "7")
    monkeypatch.setenv("X_BOOL", "true")
    rt = RuntimeSnapshot.load()
    assert rt.env_float("X_FLOAT", 0.0) == 1.25
    assert rt.env_int("X_INT", 0) == 7
    assert rt.env_bool("X_BOOL", False) is True
    assert rt.env_get("MISSING", "zzz") == "zzz"


def test_runtime_snapshot_refresh(monkeypatch):
    # refresh очень частый, чтобы тест был быстрым
    monkeypatch.setenv("RUNTIME_SNAPSHOT_REFRESH_EVERY_SEC", "0.01")
    monkeypatch.setenv("X", "a")
    rt = RuntimeSnapshot.load()
    assert rt.env_get("X", "") == "a"

    monkeypatch.setenv("X", "b")
    # ждём окно refresh
    time.sleep(0.02)
    rt.maybe_refresh()
    assert rt.env_get("X", "") == "b"
