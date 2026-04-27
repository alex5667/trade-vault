"""Tests for OF gate contract smoke-check cooldown/dedup functions.

Covers:
  - _parse_smoke_output: JSON-line parsing and regex fallback
  - _dedup_allow: Redis primary path + /tmp fallback
  - run_of_gate_contract_smoke_check: disabled flag, rc=0 ok, rc=2 alert, dedup suppression
"""
import importlib
import json
import os
import time
from unittest.mock import MagicMock, patch


# --------------------------------------------------------------------------- #
# Module reference (use importlib so monkeypatching is easy)
# --------------------------------------------------------------------------- #
def _m():
    return importlib.import_module("services.of_timers_worker")


# --------------------------------------------------------------------------- #
# _parse_smoke_output
# --------------------------------------------------------------------------- #

class TestParsesSmokeOutput:
    def test_json_line_preferred(self):
        m = _m()
        stdout = 'noise\n{"bad_share": 0.05, "top_reasons": ["a", "b"]}'
        result = m._parse_smoke_output(stdout, "")
        assert result["bad_share"] == 0.05
        assert result["top_reasons"] == ["a", "b"]

    def test_last_json_line_wins(self):
        m = _m()
        stdout = '{"bad_share": 0.01}\n{"bad_share": 0.9, "top_reasons": ["x"]}'
        result = m._parse_smoke_output(stdout, "")
        assert result["bad_share"] == 0.9

    def test_regex_fallback_when_no_json(self):
        m = _m()
        stdout = "bad_share=0.123 some text top_reasons=['foo','bar']"
        result = m._parse_smoke_output(stdout, "")
        assert result["bad_share"] == 0.123

    def test_empty_output_returns_dict(self):
        result = _m()._parse_smoke_output("", "")
        assert isinstance(result, dict)
        assert result.get("bad_share") is None

    def test_raw_capped_at_2000(self):
        m = _m()
        long_out = "x" * 3000
        result = m._parse_smoke_output(long_out, "")
        assert len(result["raw"]) <= 2000

    def test_json_from_stderr_if_stdout_empty(self):
        m = _m()
        stderr = '{"bad_share": 0.07, "top_reasons": ["z"]}'
        result = m._parse_smoke_output("", stderr)
        assert result["bad_share"] == 0.07


# --------------------------------------------------------------------------- #
# _dedup_allow
# --------------------------------------------------------------------------- #

class TestDedupAllow:
    def test_redis_nx_first_call_allowed(self, monkeypatch):
        m = _m()
        fake_r = MagicMock()
        fake_r.set.return_value = True  # NX succeeded → first write
        monkeypatch.setattr(m, "_get_redis_sync", lambda: fake_r)

        allowed = m._dedup_allow("sig1", cooldown_s=3600, prefix="test:")
        assert allowed is True
        fake_r.set.assert_called_once()

    def test_redis_nx_second_call_suppressed(self, monkeypatch):
        m = _m()
        fake_r = MagicMock()
        fake_r.set.return_value = None  # NX failed → key already exists
        monkeypatch.setattr(m, "_get_redis_sync", lambda: fake_r)

        allowed = m._dedup_allow("sig1", cooldown_s=3600, prefix="test:")
        assert allowed is False

    def test_fallback_to_tmp_when_redis_unavailable(self, monkeypatch, tmp_path):
        m = _m()
        monkeypatch.setattr(m, "_get_redis_sync", lambda: None)

        sig = f"fallback_test_{time.time()}"
        # First call — should create marker and return True
        with patch("os.path.exists", return_value=False), \
             patch("builtins.open", create=True) as mock_open:
            mock_open.return_value.__enter__ = lambda s: s
            mock_open.return_value.__exit__ = MagicMock(return_value=False)
            mock_open.return_value.write = MagicMock()
            result = m._dedup_allow(sig, cooldown_s=3600, prefix="dedup:")
        assert result is True

    def test_tmp_fallback_suppresses_within_cooldown(self, monkeypatch, tmp_path):
        m = _m()
        monkeypatch.setattr(m, "_get_redis_sync", lambda: None)

        marker_path = str(tmp_path / "marker.txt")
        # Write marker now (just now = age 0)
        with open(marker_path, "w") as f:
            f.write("1")

        sig = "dup_sig"
        import hashlib
        sig_hash = hashlib.sha1(sig.encode()).hexdigest()
        expected_marker = f"/tmp/of_gate_contract_dedup_{sig_hash}.txt"

        with patch("os.path.exists", return_value=True), \
             patch("os.path.getmtime", return_value=time.time()):  # age = ~0
            result = m._dedup_allow(sig, cooldown_s=3600, prefix="dedup:")
        assert result is False

    def test_empty_signature_normalizes(self, monkeypatch):
        m = _m()
        fake_r = MagicMock()
        fake_r.set.return_value = True
        monkeypatch.setattr(m, "_get_redis_sync", lambda: fake_r)

        # Should not raise
        result = m._dedup_allow("", cooldown_s=60, prefix="p:")
        assert result is True


# --------------------------------------------------------------------------- #
# run_of_gate_contract_smoke_check
# --------------------------------------------------------------------------- #

class TestRunSmokeCheck:
    def test_disabled_returns_true(self, monkeypatch):
        m = _m()
        monkeypatch.setenv("ENABLE_OF_GATE_CONTRACT_SMOKE", "0")
        assert m.run_of_gate_contract_smoke_check() is True

    def test_disabled_via_metrics_flag(self, monkeypatch):
        m = _m()
        monkeypatch.delenv("ENABLE_OF_GATE_CONTRACT_SMOKE", raising=False)
        monkeypatch.setenv("OF_GATE_METRICS_ENABLE", "0")
        assert m.run_of_gate_contract_smoke_check() is True

    def test_rc0_returns_true_no_notify(self, monkeypatch):
        m = _m()
        monkeypatch.setenv("ENABLE_OF_GATE_CONTRACT_SMOKE", "1")

        fake_result = MagicMock()
        fake_result.returncode = 0
        fake_result.stdout = ""
        fake_result.stderr = ""

        notify_calls = []
        monkeypatch.setattr(m, "_notify_stream", lambda *a, **kw: notify_calls.append((a, kw)))

        with patch("subprocess.run", return_value=fake_result), \
             patch("os.path.exists", return_value=False):
            result = m.run_of_gate_contract_smoke_check()

        assert result is True
        assert notify_calls == []

    def test_rc2_sends_crit_alert(self, monkeypatch):
        m = _m()
        monkeypatch.setenv("ENABLE_OF_GATE_CONTRACT_SMOKE", "1")
        monkeypatch.setenv("OF_GATE_CONTRACT_SMOKE_DEDUP_ENABLE", "0")  # bypass dedup

        fake_result = MagicMock()
        fake_result.returncode = 2
        fake_result.stdout = '{"bad_share": 0.02, "top_reasons": ["missing_field"]}'
        fake_result.stderr = ""

        notify_calls = []
        monkeypatch.setattr(m, "_notify_stream", lambda *a, **kw: notify_calls.append((a, kw)))

        with patch("subprocess.run", return_value=fake_result), \
             patch("os.path.exists", return_value=False):
            result = m.run_of_gate_contract_smoke_check()

        assert result is False
        assert len(notify_calls) == 1
        _, kw = notify_calls[0]
        assert kw.get("severity") == "crit"

    def test_rc1_sends_page_alert(self, monkeypatch):
        m = _m()
        monkeypatch.setenv("ENABLE_OF_GATE_CONTRACT_SMOKE", "1")
        monkeypatch.setenv("OF_GATE_CONTRACT_SMOKE_DEDUP_ENABLE", "0")

        fake_result = MagicMock()
        fake_result.returncode = 1
        fake_result.stdout = ""
        fake_result.stderr = "error"

        notify_calls = []
        monkeypatch.setattr(m, "_notify_stream", lambda *a, **kw: notify_calls.append((a, kw)))

        with patch("subprocess.run", return_value=fake_result), \
             patch("os.path.exists", return_value=False):
            result = m.run_of_gate_contract_smoke_check()

        assert result is False
        assert len(notify_calls) == 1
        _, kw = notify_calls[0]
        assert kw.get("severity") == "page"

    def test_dedup_suppresses_second_alert(self, monkeypatch):
        m = _m()
        monkeypatch.setenv("ENABLE_OF_GATE_CONTRACT_SMOKE", "1")
        monkeypatch.setenv("OF_GATE_CONTRACT_SMOKE_DEDUP_ENABLE", "1")
        # First call: _dedup_allow returns True, second: False
        allow_seq = [True, False]
        monkeypatch.setattr(m, "_dedup_allow", lambda *a, **kw: allow_seq.pop(0))

        notify_calls = []
        monkeypatch.setattr(m, "_notify_stream", lambda *a, **kw: notify_calls.append((a, kw)))

        fake_result = MagicMock()
        fake_result.returncode = 2
        fake_result.stdout = '{"bad_share": 0.01}'
        fake_result.stderr = ""

        with patch("subprocess.run", return_value=fake_result), \
             patch("os.path.exists", return_value=False):
            m.run_of_gate_contract_smoke_check()  # first: allowed -> notified
            m.run_of_gate_contract_smoke_check()  # second: suppressed -> no notify

        assert len(notify_calls) == 1

    def test_timeout_sends_crit_alert(self, monkeypatch):
        import subprocess
        m = _m()
        monkeypatch.setenv("ENABLE_OF_GATE_CONTRACT_SMOKE", "1")

        notify_calls = []
        monkeypatch.setattr(m, "_notify_stream", lambda *a, **kw: notify_calls.append((a, kw)))

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="x", timeout=120)), \
             patch("os.path.exists", return_value=False):
            result = m.run_of_gate_contract_smoke_check()

        assert result is False
        assert len(notify_calls) == 1
        _, kw = notify_calls[0]
        assert kw.get("severity") == "crit"
        assert "timeout" in notify_calls[0][0][0].lower()
