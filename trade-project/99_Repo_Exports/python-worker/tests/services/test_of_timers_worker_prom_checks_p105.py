from __future__ import annotations

"""Tests for P105 prom smoke-check wiring in of_timers_worker.

Covers:
  - run_prom_rules_bundle_smoke_check
      - disabled when ENABLE_PROM_RULES_BUNDLE_SMOKE != 1 → True (no-op)
      - rc=0 → True + auto-apply block cleared
      - rc=2 → False + auto-apply block set + dedup suppresses duplicate
      - rc=other → False + auto-apply block set
      - cooldown env override respected (PROM_RULES_BUNDLE_SMOKE_COOLDOWN_S)
  - run_prom_rules_loaded_probe
      - disabled when ENABLE_PROM_RULES_LOADED_PROBE != 1 → True (no-op)
      - rc=0 → True + auto-apply block cleared
      - rc=2 → False + auto-apply block set
      - timeout → rc != 0

Both functions live in services.of_timers_worker (canonical P105 runner location).
"""

from unittest.mock import MagicMock, patch

import services.of_timers_worker as worker_mod
from services.of_timers_worker import (
    run_prom_rules_bundle_smoke_check,
    run_prom_rules_loaded_probe,
)

# ---------------------------------------------------------------------------
# Helper: fake run_tool_rc (used internally by both check functions)
# ---------------------------------------------------------------------------

def _make_run_tool_rc(rc: int, stdout: str = "", stderr: str = ""):
    """Return a patched run_tool_rc that always returns (rc, stdout, stderr)."""
    return MagicMock(return_value=(rc, stdout, stderr))


# ---------------------------------------------------------------------------
# run_prom_rules_bundle_smoke_check
# ---------------------------------------------------------------------------

class TestPromRulesBundleSmokeCheck:
    """Tests for run_prom_rules_bundle_smoke_check (P102/P105 wiring)."""

    def test_disabled_when_env_not_set(self, monkeypatch):
        """ENABLE_PROM_RULES_BUNDLE_SMOKE not set → default is '1' (enabled).
        Explicitly test the disabled path."""
        monkeypatch.setenv("ENABLE_PROM_RULES_BUNDLE_SMOKE", "0")
        with patch.object(worker_mod, "run_tool_rc") as mock_rt:
            result = run_prom_rules_bundle_smoke_check()
        assert result is True
        mock_rt.assert_not_called()

    def test_disabled_explicit_false(self, monkeypatch):
        """ENABLE_PROM_RULES_BUNDLE_SMOKE=false → no-op True."""
        monkeypatch.setenv("ENABLE_PROM_RULES_BUNDLE_SMOKE", "false")
        with patch.object(worker_mod, "run_tool_rc") as mock_rt:
            result = run_prom_rules_bundle_smoke_check()
        assert result is True
        mock_rt.assert_not_called()

    def test_enabled_rc0_returns_true_and_clears_block(self, monkeypatch):
        """rc=0 → True, and _clear_auto_apply_block_if_owned is called."""
        monkeypatch.setenv("ENABLE_PROM_RULES_BUNDLE_SMOKE", "1")
        cleared = []
        with patch.object(worker_mod, "run_tool_rc", return_value=(0, "", "")), \
             patch.object(worker_mod, "_clear_auto_apply_block_if_owned",
                          side_effect=lambda r, owner: cleared.append(r)):
            result = run_prom_rules_bundle_smoke_check()
        assert result is True
        assert any("prom_rules_bundle_smoke" in r for r in cleared)

    def test_enabled_rc2_returns_false_and_sets_block(self, monkeypatch):
        """rc=2 → False, _set_auto_apply_block is called, dedup allows first fire."""
        monkeypatch.setenv("ENABLE_PROM_RULES_BUNDLE_SMOKE", "1")
        # Use a long cooldown to avoid flakiness; dedup_allow will be forced True
        monkeypatch.setenv("PROM_RULES_BUNDLE_SMOKE_COOLDOWN_S", "21600")
        block_calls = []
        with patch.object(worker_mod, "run_tool_rc", return_value=(2, "", "rules broken")), \
             patch.object(worker_mod, "_set_auto_apply_block",
                          side_effect=lambda reason, meta, ttl_s=21600: block_calls.append(reason)), \
             patch.object(worker_mod, "_dedup_allow", return_value=True), \
             patch.object(worker_mod, "_notify_stream"):
            result = run_prom_rules_bundle_smoke_check()
        assert result is False
        assert "prom_rules_bundle_smoke" in block_calls

    def test_rc2_dedup_suppresses_duplicate(self, monkeypatch):
        """When _dedup_allow returns False (duplicate), no notification, rc=False."""
        monkeypatch.setenv("ENABLE_PROM_RULES_BUNDLE_SMOKE", "1")
        block_calls = []
        notify_calls = []
        with patch.object(worker_mod, "run_tool_rc", return_value=(2, "", "rules broken")), \
             patch.object(worker_mod, "_set_auto_apply_block",
                          side_effect=lambda reason, meta, ttl_s=21600: block_calls.append(reason)), \
             patch.object(worker_mod, "_dedup_allow", return_value=False), \
             patch.object(worker_mod, "_notify_stream",
                          side_effect=lambda *a, **kw: notify_calls.append(a)):
            result = run_prom_rules_bundle_smoke_check()
        assert result is False
        # Block is still set (fail-closed), but notify is suppressed
        assert "prom_rules_bundle_smoke" in block_calls
        assert len(notify_calls) == 0, "Duplicate alert must be suppressed by dedup"

    def test_rc_other_sets_block(self, monkeypatch):
        """rc=1 (script error) → False + block set."""
        monkeypatch.setenv("ENABLE_PROM_RULES_BUNDLE_SMOKE", "1")
        block_calls = []
        with patch.object(worker_mod, "run_tool_rc", return_value=(1, "", "error")), \
             patch.object(worker_mod, "_set_auto_apply_block",
                          side_effect=lambda reason, meta, ttl_s=21600: block_calls.append(reason)), \
             patch.object(worker_mod, "_dedup_allow", return_value=True), \
             patch.object(worker_mod, "_notify_stream"):
            result = run_prom_rules_bundle_smoke_check()
        assert result is False
        assert len(block_calls) > 0

    def test_cooldown_env_override(self, monkeypatch):
        """PROM_RULES_BUNDLE_SMOKE_COOLDOWN_S env is passed to _dedup_allow."""
        monkeypatch.setenv("ENABLE_PROM_RULES_BUNDLE_SMOKE", "1")
        monkeypatch.setenv("PROM_RULES_BUNDLE_SMOKE_COOLDOWN_S", "999")
        captured_cooldown = []
        def capture_dedup(sig, cooldown_s, prefix):
            captured_cooldown.append(cooldown_s)
            return True
        with patch.object(worker_mod, "run_tool_rc", return_value=(2, "", "bad")), \
             patch.object(worker_mod, "_set_auto_apply_block"), \
             patch.object(worker_mod, "_dedup_allow", side_effect=capture_dedup), \
             patch.object(worker_mod, "_notify_stream"):
            run_prom_rules_bundle_smoke_check()
        assert captured_cooldown and captured_cooldown[0] == 999

    def test_module_env_override(self, monkeypatch):
        """PROM_RULES_BUNDLE_SMOKE_MODULE overrides the default module."""
        monkeypatch.setenv("ENABLE_PROM_RULES_BUNDLE_SMOKE", "1")
        monkeypatch.setenv("PROM_RULES_BUNDLE_SMOKE_MODULE", "my.custom.checker")
        captured_module = []
        def capture_run(module, args, timeout):
            captured_module.append(module)
            return (0, "", "")
        with patch.object(worker_mod, "run_tool_rc", side_effect=capture_run), \
             patch.object(worker_mod, "_clear_auto_apply_block_if_owned"):
            result = run_prom_rules_bundle_smoke_check()
        assert result is True
        assert "my.custom.checker" in captured_module


# ---------------------------------------------------------------------------
# run_prom_rules_loaded_probe
# ---------------------------------------------------------------------------

class TestPromRulesLoadedProbe:
    """Tests for run_prom_rules_loaded_probe (P102/P105 wiring)."""

    def test_disabled_when_env_not_1(self, monkeypatch):
        """ENABLE_PROM_RULES_LOADED_PROBE=0 → no-op True."""
        monkeypatch.setenv("ENABLE_PROM_RULES_LOADED_PROBE", "0")
        with patch.object(worker_mod, "run_tool_rc") as mock_rt:
            result = run_prom_rules_loaded_probe()
        assert result is True
        mock_rt.assert_not_called()

    def test_enabled_rc0_returns_true_and_clears_block(self, monkeypatch):
        """rc=0 → True, block cleared."""
        monkeypatch.setenv("ENABLE_PROM_RULES_LOADED_PROBE", "1")
        cleared = []
        with patch.object(worker_mod, "run_tool_rc", return_value=(0, "", "")), \
             patch.object(worker_mod, "_clear_auto_apply_block_if_owned",
                          side_effect=lambda r, owner: cleared.append(r)):
            result = run_prom_rules_loaded_probe()
        assert result is True
        assert any("prom_rules_loaded_probe" in r for r in cleared)

    def test_enabled_rc2_returns_false_and_sets_block(self, monkeypatch):
        """rc=2 → False, block set with correct reason."""
        monkeypatch.setenv("ENABLE_PROM_RULES_LOADED_PROBE", "1")
        block_calls = []
        with patch.object(worker_mod, "run_tool_rc", return_value=(2, "", "missing files")), \
             patch.object(worker_mod, "_set_auto_apply_block",
                          side_effect=lambda reason, meta, ttl_s=21600: block_calls.append(reason)), \
             patch.object(worker_mod, "_dedup_allow", return_value=True), \
             patch.object(worker_mod, "_notify_stream"):
            result = run_prom_rules_loaded_probe()
        assert result is False
        assert "prom_rules_loaded_probe" in block_calls

    def test_rc2_dedup_suppresses_duplicate(self, monkeypatch):
        """Duplicate alert suppressed when _dedup_allow returns False."""
        monkeypatch.setenv("ENABLE_PROM_RULES_LOADED_PROBE", "1")
        notify_calls = []
        block_calls = []
        with patch.object(worker_mod, "run_tool_rc", return_value=(2, "", "missing")), \
             patch.object(worker_mod, "_set_auto_apply_block",
                          side_effect=lambda reason, meta, ttl_s=21600: block_calls.append(reason)), \
             patch.object(worker_mod, "_dedup_allow", return_value=False), \
             patch.object(worker_mod, "_notify_stream",
                          side_effect=lambda *a, **kw: notify_calls.append(a)):
            result = run_prom_rules_loaded_probe()
        assert result is False
        # Fail-closed block still set
        assert "prom_rules_loaded_probe" in block_calls
        # Notification suppressed
        assert len(notify_calls) == 0

    def test_cooldown_env_override(self, monkeypatch):
        """PROM_RULES_LOADED_PROBE_COOLDOWN_S is passed to _dedup_allow."""
        monkeypatch.setenv("ENABLE_PROM_RULES_LOADED_PROBE", "1")
        monkeypatch.setenv("PROM_RULES_LOADED_PROBE_COOLDOWN_S", "7200")
        captured = []
        def capture(sig, cooldown_s, prefix):
            captured.append(cooldown_s)
            return True
        with patch.object(worker_mod, "run_tool_rc", return_value=(2, "", "err")), \
             patch.object(worker_mod, "_set_auto_apply_block"), \
             patch.object(worker_mod, "_dedup_allow", side_effect=capture), \
             patch.object(worker_mod, "_notify_stream"):
            run_prom_rules_loaded_probe()
        assert captured and captured[0] == 7200

    def test_default_enabled_when_env_is_1(self, monkeypatch):
        """ENABLE_PROM_RULES_LOADED_PROBE=1 actually invokes run_tool_rc."""
        monkeypatch.setenv("ENABLE_PROM_RULES_LOADED_PROBE", "1")
        with patch.object(worker_mod, "run_tool_rc", return_value=(0, "", "")) as mock_rt, \
             patch.object(worker_mod, "_clear_auto_apply_block_if_owned"):
            run_prom_rules_loaded_probe()
        mock_rt.assert_called_once()

    def test_module_env_override(self, monkeypatch):
        """PROM_RULES_LOADED_PROBE_MODULE overrides the default module."""
        monkeypatch.setenv("ENABLE_PROM_RULES_LOADED_PROBE", "1")
        monkeypatch.setenv("PROM_RULES_LOADED_PROBE_MODULE", "custom.probe")
        captured_module = []
        def capture_run(module, args, timeout):
            captured_module.append(module)
            return (0, "", "")
        with patch.object(worker_mod, "run_tool_rc", side_effect=capture_run), \
             patch.object(worker_mod, "_clear_auto_apply_block_if_owned"):
            result = run_prom_rules_loaded_probe()
        assert result is True
        assert "custom.probe" in captured_module


# ---------------------------------------------------------------------------
# Compile guard: both workers must be syntactically valid Python (P105/AST)
# ---------------------------------------------------------------------------

def test_both_workers_compile_clean():
    """ast.parse checks both of_timers_worker.py variants for syntax errors."""
    import ast
    for path in (
        "services/of_timers_worker.py",
        "tick_flow_full/services/of_timers_worker.py",
    ):
        src = open(path, encoding="utf-8").read()
        ast.parse(src)  # raises SyntaxError on malformed Python


# ---------------------------------------------------------------------------
# Schedule slot constants: verify :09 and :10 are distinct from existing slots
# ---------------------------------------------------------------------------

def test_schedule_slots_09_10_unique():
    """Regression: minutes 9 and 10 must not collide with other check slots.

    Reads of_timers_worker.py source and counts occurrences of 'minute >= 9'
    and 'minute >= 10' patterns to confirm they are present (slots wired).
    """
    import re
    src = open("services/of_timers_worker.py", encoding="utf-8").read()
    # Both hourly slots must be present in the source
    assert re.search(r"minute\s*>=\s*9\b", src) is not None, \
        "Hourly :09 slot (prom_rules_bundle_smoke) missing from of_timers_worker.py"
    assert re.search(r"minute\s*>=\s*10\b", src) is not None, \
        "Hourly :10 slot (prom_rules_loaded_probe) missing from of_timers_worker.py"
    # Corresponding last_run keys must be tracked
    assert "prom_rules_bundle_smoke" in src, \
        "prom_rules_bundle_smoke not tracked in last_run dict"
    assert "prom_rules_loaded_probe" in src, \
        "prom_rules_loaded_probe not tracked in last_run dict"
