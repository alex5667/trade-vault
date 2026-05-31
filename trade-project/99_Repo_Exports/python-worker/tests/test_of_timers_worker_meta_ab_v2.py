import importlib


def test_run_meta_ab_v2_disabled_noop(monkeypatch):
    m = importlib.import_module("services.of_timers_worker")
    calls = []

    def fake_run_tool(module, env=None, timeout=None):
        calls.append((module, env, timeout))
        return True

    monkeypatch.setattr(m, "run_tool", fake_run_tool)
    monkeypatch.setenv("ENABLE_META_AB_V2_NIGHTLY", "0")

    assert m.run_meta_ab_v2_nightly_job_v1() is True
    assert calls == []


def test_run_meta_ab_v2_enabled_calls_run_tool(monkeypatch):
    m = importlib.import_module("services.of_timers_worker")
    calls = []

    def fake_run_tool(module, env=None, timeout=None):
        calls.append((module, env, timeout))
        return True

    monkeypatch.setattr(m, "run_tool", fake_run_tool)
    monkeypatch.setenv("ENABLE_META_AB_V2_NIGHTLY", "1")
    monkeypatch.delenv("META_AB_V2_JOB_MODULE", raising=False)
    monkeypatch.setenv("META_AB_V2_NIGHTLY_TIMEOUT_S", "123")

    assert m.run_meta_ab_v2_nightly_job_v1() is True
    assert len(calls) == 1
    assert calls[0][0] == "services.orderflow.meta_ab_v2_nightly_job_v1"
    assert calls[0][2] == 123
