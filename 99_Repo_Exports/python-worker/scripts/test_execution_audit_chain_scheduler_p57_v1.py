"""P5.7 scheduler smoke tests: verify the scheduler loads the P5.6 checker and can run a single iteration."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


# Locate scheduler relative to this test file (scripts/)
ROOT = Path(__file__).resolve().parents[1]
MOD_PATH = ROOT / "scripts" / "run_execution_audit_chain_scheduler.py"
spec = importlib.util.spec_from_file_location("run_execution_audit_chain_scheduler", MOD_PATH)
mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_scheduler_loads_checker_module() -> None:
    """CHECKER must be loaded at import time and expose a main() callable."""
    assert mod.CHECKER is not None
    assert hasattr(mod.CHECKER, "main"), "P5.6 checker must expose a main() function"


def test_scheduler_main_single_iteration(monkeypatch) -> None:
    """main() must call run_once() exactly once when EXEC_AUDIT_LOOP_MAX_ITERATIONS=1."""
    calls = []

    def fake_run_once() -> int:
        calls.append("run")
        return 0

    monkeypatch.setattr(mod, "run_once", fake_run_once)
    monkeypatch.setenv("EXEC_AUDIT_LOOP_MAX_ITERATIONS", "1")
    monkeypatch.setenv("EXEC_AUDIT_LOOP_RUN_ON_START", "1")
    monkeypatch.setenv("EXEC_AUDIT_LOOP_INTERVAL_SECONDS", "1")
    monkeypatch.setenv("EXEC_AUDIT_LOOP_FAILURE_SLEEP_SECONDS", "1")
    rc = mod.main()
    assert rc == 0
    assert calls == ["run"]


def test_scheduler_skips_first_run_when_disabled(monkeypatch) -> None:
    """When EXEC_AUDIT_LOOP_RUN_ON_START=0, the first iteration must be skipped."""
    calls = []

    def fake_run_once() -> int:
        calls.append("run")
        return 0

    monkeypatch.setattr(mod, "run_once", fake_run_once)
    # With max_iterations=1 and run_on_start=0, the loop executes 1 iteration but skips the call.
    monkeypatch.setenv("EXEC_AUDIT_LOOP_MAX_ITERATIONS", "1")
    monkeypatch.setenv("EXEC_AUDIT_LOOP_RUN_ON_START", "0")
    monkeypatch.setenv("EXEC_AUDIT_LOOP_INTERVAL_SECONDS", "1")
    rc = mod.main()
    assert rc == 0
    assert calls == [], "first run should be skipped when EXEC_AUDIT_LOOP_RUN_ON_START=0"


def test_env_int_defaults() -> None:
    """env_int() must return the default when env var is unset or empty."""
    import os
    old = os.environ.pop("_EXEC_AUDIT_TEST_INT", None)
    try:
        assert mod.env_int("_EXEC_AUDIT_TEST_INT", 42) == 42
    finally:
        if old is not None:
            os.environ["_EXEC_AUDIT_TEST_INT"] = old


def test_env_bool_truthy_values(monkeypatch) -> None:
    """env_bool() must recognise 1/true/yes/on as truthy."""
    for val in ("1", "true", "yes", "on"):
        monkeypatch.setenv("_EXEC_AUDIT_TEST_BOOL", val)
        assert mod.env_bool("_EXEC_AUDIT_TEST_BOOL", False) is True


def test_env_bool_falsy_values(monkeypatch) -> None:
    """env_bool() must recognise 0/false/no/off as falsy."""
    for val in ("0", "false", "no", "off"):
        monkeypatch.setenv("_EXEC_AUDIT_TEST_BOOL", val)
        assert mod.env_bool("_EXEC_AUDIT_TEST_BOOL", True) is False
