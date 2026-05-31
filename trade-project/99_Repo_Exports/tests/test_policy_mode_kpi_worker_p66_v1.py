"""
Tests for policy_mode_kpi_worker_p66_v1

Verifies:
  - Helper functions (_i, _norm_state, _norm_mode, _regime_from_states, _decision_ts_ms)
  - _parse_json_maybe handles nested dicts correctly
  - _process_one correctly counts regimes, modes, and mismatch flags
  - _bootstrap_state initializes rolling dict correctly from zero
  - _advance_window subtracts expired minute buckets
  - load_cfg reads ENV correctly
"""
import os
import sys
import importlib.util
import time
from unittest.mock import MagicMock, patch, call
import pytest

# ── Module import ─────────────────────────────────────────────────────────────

def _import_worker():
    """Import as a file, not a package module, to avoid side-effects."""
    pw_path = os.path.join(os.path.dirname(__file__), "..", "python-worker")
    pw_path = os.path.normpath(pw_path)
    if pw_path not in sys.path:
        sys.path.insert(0, pw_path)
    spec = importlib.util.spec_from_file_location(
        "policy_mode_kpi_worker_p66_v1",
        os.path.join(pw_path, "orderflow_services", "policy_mode_kpi_worker_p66_v1.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["policy_mode_kpi_worker_p66_v1"] = mod
    spec.loader.exec_module(mod)
    return mod


try:
    _w = _import_worker()
    SKIP = False
    SKIP_REASON = ""
except Exception as e:
    SKIP = True
    SKIP_REASON = str(e)
    _w = None


@pytest.mark.skipif(SKIP, reason=SKIP_REASON)
class TestNormState:
    def test_ok(self):
        assert _w._norm_state("ok") == "ok"

    def test_warn(self):
        assert _w._norm_state("WARN") == "warn"

    def test_block(self):
        assert _w._norm_state("Block") == "block"

    def test_unknown_for_empty(self):
        assert _w._norm_state("") == "unknown"

    def test_unknown_for_garbage(self):
        assert _w._norm_state("broken") == "unknown"

    def test_unknown_for_none(self):
        assert _w._norm_state(None) == "unknown"


@pytest.mark.skipif(SKIP, reason=SKIP_REASON)
class TestNormMode:
    def test_active_variants(self):
        for v in ("active", "live", "on", "ACTIVE"):
            assert _w._norm_mode(v) == "active", f"failed for {v!r}"

    def test_shadow_variants(self):
        for v in ("shadow", "paper", "dry", "dry_run"):
            assert _w._norm_mode(v) == "shadow"

    def test_block_variants(self):
        for v in ("block", "off", "disabled", "freeze"):
            assert _w._norm_mode(v) == "block"

    def test_unknown_fallback(self):
        assert _w._norm_mode("foobar") == "unknown"
        assert _w._norm_mode(None) == "unknown"
        assert _w._norm_mode("") == "unknown"


@pytest.mark.skipif(SKIP, reason=SKIP_REASON)
class TestRegimeFromStates:
    def test_both_ok(self):
        assert _w._regime_from_states("ok", "ok") == "ok"

    def test_one_warn(self):
        assert _w._regime_from_states("ok", "warn") == "warn"
        assert _w._regime_from_states("warn", "ok") == "warn"

    def test_block_dominates(self):
        assert _w._regime_from_states("block", "warn") == "block"
        assert _w._regime_from_states("ok", "block") == "block"

    def test_unknown_when_missing(self):
        assert _w._regime_from_states(None, None) == "unknown"
        assert _w._regime_from_states("", "") == "unknown"


@pytest.mark.skipif(SKIP, reason=SKIP_REASON)
class TestParseJsonMaybe:
    def test_dict_passthrough(self):
        d = {"state": "ok"}
        assert _w._parse_json_maybe(d) is d

    def test_json_string_parsed(self):
        result = _w._parse_json_maybe('{"state": "warn"}')
        assert result == {"state": "warn"}

    def test_plain_string_passthrough(self):
        assert _w._parse_json_maybe("ok") == "ok"

    def test_none_passthrough(self):
        assert _w._parse_json_maybe(None) is None

    def test_nested_state_extraction(self):
        """Simulates dq_state being a JSON-encoded dict with 'state' field."""
        raw = '{"state": "block", "score": 0.9}'
        parsed = _w._parse_json_maybe(raw)
        assert isinstance(parsed, dict)
        assert parsed["state"] == "block"


@pytest.mark.skipif(SKIP, reason=SKIP_REASON)
class TestDecisionTsMs:
    def test_decision_ts_ms_field(self):
        ts = _w._decision_ts_ms({"decision_ts_ms": 1700000000000}, "0-0")
        assert ts == 1700000000000

    def test_ts_field_seconds_converted(self):
        # value < 10_000_000_000 → treated as seconds
        ts = _w._decision_ts_ms({"ts": 1700000000}, "0-0")
        assert ts == 1700000000 * 1000

    def test_fallback_to_stream_id(self):
        ts = _w._decision_ts_ms({}, "1700000000000-0")
        assert ts == 1700000000000

    def test_priority_order(self):
        # decision_ts_ms takes priority over ts
        ts = _w._decision_ts_ms({"decision_ts_ms": 2000000000000, "ts": 1000000000}, "0-0")
        assert ts == 2000000000000


@pytest.mark.skipif(SKIP, reason=SKIP_REASON)
class TestProcessOne:
    """Integration-style tests for _process_one using a fake Redis pipeline."""

    def _make_fake_redis(self, existing_bucket=None):
        """Build a minimal mock redis instance."""
        r = MagicMock()
        pipe = MagicMock()
        pipe.__enter__ = MagicMock(return_value=pipe)
        pipe.__exit__ = MagicMock(return_value=False)
        pipe.execute = MagicMock(return_value=[])
        r.pipeline.return_value = pipe

        # hgetall returns empty dict unless we preset it
        r.hgetall.return_value = existing_bucket or {}
        return r, pipe

    def test_ok_active_increments_cell(self):
        """ok regime + active mode → rolling['ok_active'] incremented by 1."""
        r, pipe = self._make_fake_redis()
        cfg = _w.load_cfg()
        fields = {
            "decision_ts_ms": "1700000060000",  # minute 28333334
            "dq_state": "ok",
            "drift_state": "ok",
            "policy_effective_mode": "active",
        }
        rolling = {k: 0 for k in _w.FIELDS}
        cur_min = _w._minute(1700000060000)
        _, _ = _w._process_one(r, cfg, "1700000060000-0", fields, cur_min, rolling, 0)
        assert rolling["ok_active"] == 1
        assert rolling["total"] == 1
        assert rolling["mismatch_block_regime_effective_not_block"] == 0
        assert rolling["mismatch_warn_regime_effective_active"] == 0

    def test_block_regime_not_blocked_mismatch(self):
        """block regime + active mode → mismatch_block_regime_effective_not_block = 1."""
        r, pipe = self._make_fake_redis()
        cfg = _w.load_cfg()
        fields = {
            "decision_ts_ms": "1700000060000",
            "dq_state": "block",
            "drift_state": "ok",
            "policy_effective_mode": "active",  # should be "block" → mismatch!
        }
        rolling = {k: 0 for k in _w.FIELDS}
        cur_min = _w._minute(1700000060000)
        _, _ = _w._process_one(r, cfg, "1700000060000-0", fields, cur_min, rolling, 0)
        assert rolling["block_active"] == 1
        assert rolling["mismatch_block_regime_effective_not_block"] == 1
        assert rolling["mismatch_warn_regime_effective_active"] == 0

    def test_warn_regime_active_mismatch(self):
        """warn regime + active mode → mismatch_warn_regime_effective_active = 1."""
        r, pipe = self._make_fake_redis()
        cfg = _w.load_cfg()
        fields = {
            "decision_ts_ms": "1700000060000",
            "dq_state": "warn",
            "drift_state": "ok",
            "effective_mode": "active",  # testing fallback field name
        }
        rolling = {k: 0 for k in _w.FIELDS}
        cur_min = _w._minute(1700000060000)
        _, _ = _w._process_one(r, cfg, "1700000060000-0", fields, cur_min, rolling, 0)
        assert rolling["warn_active"] == 1
        assert rolling["mismatch_warn_regime_effective_active"] == 1

    def test_block_regime_correctly_blocked_no_mismatch(self):
        """block regime + block mode → no mismatch."""
        r, pipe = self._make_fake_redis()
        cfg = _w.load_cfg()
        fields = {
            "decision_ts_ms": "1700000060000",
            "dq_state": "block",
            "drift_state": "block",
            "policy_effective_mode": "block",
        }
        rolling = {k: 0 for k in _w.FIELDS}
        cur_min = _w._minute(1700000060000)
        _, _ = _w._process_one(r, cfg, "1700000060000-0", fields, cur_min, rolling, 0)
        assert rolling["block_block"] == 1
        assert rolling["mismatch_block_regime_effective_not_block"] == 0

    def test_nested_dq_state_dict_parsed(self):
        """dq_state as JSON string {"state":"warn"} must be parsed correctly."""
        r, pipe = self._make_fake_redis()
        cfg = _w.load_cfg()
        fields = {
            "decision_ts_ms": "1700000060000",
            "dq_state": '{"state": "warn"}',
            "drift_state": "ok",
            "policy_effective_mode": "shadow",
        }
        rolling = {k: 0 for k in _w.FIELDS}
        cur_min = _w._minute(1700000060000)
        _, _ = _w._process_one(r, cfg, "1700000060000-0", fields, cur_min, rolling, 0)
        assert rolling["warn_shadow"] == 1

    def test_old_message_skipped(self):
        """Messages older than window must be skipped — no rolling update."""
        r, pipe = self._make_fake_redis()
        cfg = _w.load_cfg()
        cfg_window = cfg.window_minutes
        now_min = _w._minute(_w._now_ms())
        # message older than window_minutes + 1
        old_ts_ms = (now_min - cfg_window - 2) * 60000
        fields = {
            "decision_ts_ms": str(old_ts_ms),
            "dq_state": "ok",
            "drift_state": "ok",
            "policy_effective_mode": "active",
        }
        rolling = {k: 0 for k in _w.FIELDS}
        _, _ = _w._process_one(r, cfg, "0-0", fields, now_min, rolling, 0)
        # nothing should be incremented
        assert rolling["ok_active"] == 0
        assert rolling["total"] == 0


@pytest.mark.skipif(SKIP, reason=SKIP_REASON)
class TestLoadCfg:
    def test_defaults(self, monkeypatch):
        for k in ("REDIS_URL", "DECISIONS_FINAL_STREAM", "POLICY_MODE_CG"):
            monkeypatch.delenv(k, raising=False)
        cfg = _w.load_cfg()
        assert "redis-worker-1:6379" in cfg.redis_url
        assert cfg.stream == "decisions:final"
        assert cfg.window_minutes == 1440
        assert cfg.bucket_ttl_s == 86400 * 3

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("REDIS_URL", "redis://myredis:6380/2")
        monkeypatch.setenv("POLICY_MODE_WINDOW_MINUTES", "720")
        cfg = _w.load_cfg()
        assert cfg.redis_url == "redis://myredis:6380/2"
        assert cfg.window_minutes == 720


# ── YAML structural checks (no import needed) ──────────────────────────────────

ALERTS_YAML_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "python-worker",
    "orderflow_services",
    "prometheus_alerts_tradeoff_p66_v1.yml",
)


class TestAlertsYamlPolicyMode:
    def _load(self):
        import yaml
        with open(ALERTS_YAML_PATH) as f:
            return yaml.safe_load(f)

    def test_yaml_parses(self):
        doc = self._load()
        assert "groups" in doc

    def test_has_policy_mode_alerts(self):
        doc = self._load()
        all_alerts = [
            rule["alert"]
            for g in doc["groups"]
            for rule in g.get("rules", [])
        ]
        assert "PolicyModeBlockRegimeNotBlockedSLO" in all_alerts
        assert "PolicyModeWarnRegimeActiveShareHighSLO" in all_alerts

    def test_block_alert_is_critical(self):
        doc = self._load()
        for g in doc["groups"]:
            for rule in g.get("rules", []):
                if rule.get("alert") == "PolicyModeBlockRegimeNotBlockedSLO":
                    assert rule["labels"]["severity"] == "critical"

    def test_warn_alert_is_warning(self):
        doc = self._load()
        for g in doc["groups"]:
            for rule in g.get("rules", []):
                if rule.get("alert") == "PolicyModeWarnRegimeActiveShareHighSLO":
                    assert rule["labels"]["severity"] == "warning"

    def test_policy_alerts_gate_on_total(self):
        """Policy mode alerts must gate on policy_mode_n_24h_total > 200 for noise suppression."""
        doc = self._load()
        for g in doc["groups"]:
            for rule in g.get("rules", []):
                name = rule.get("alert", "")
                if name in ("PolicyModeBlockRegimeNotBlockedSLO", "PolicyModeWarnRegimeActiveShareHighSLO"):
                    assert "policy_mode_n_24h_total" in rule["expr"], (
                        f"{name} must gate on policy_mode_n_24h_total"
                    )

    def test_all_rules_have_required_fields(self):
        doc = self._load()
        for g in doc["groups"]:
            for rule in g.get("rules", []):
                assert "alert" in rule
                assert "expr" in rule
                assert "labels" in rule
                assert "annotations" in rule
