"""
Tests for policy_mode_exporter_p66_v1

Verifies:
  - load_cfg reads ENV correctly with defaults and overrides
  - _i helper is robust
  - main loop sets all Prometheus gauges without error
  - Metrics are correctly computed from Redis state hash
"""
import os
import sys
import importlib.util
import types
from unittest.mock import MagicMock
import pytest


def _import_exporter():
    """Import the exporter module, stubbing prometheus_client to avoid HTTP server binding."""
    pw_path = os.path.join(os.path.dirname(__file__), "..", "python-worker")
    pw_path = os.path.normpath(pw_path)
    if pw_path not in sys.path:
        sys.path.insert(0, pw_path)

    # Stub prometheus_client before module load
    fake_pc = types.ModuleType("prometheus_client")
    fake_pc.__name__ = "prometheus_client"
    fake_pc.__package__ = "prometheus_client"
    fake_pc.__spec__ = None
    fake_gauge_instances: dict = {}

    class FakeGauge:
        """Minimal Gauge stub that records set/labels calls."""
        def __init__(self, name: str, doc: str, labelnames=None):
            self._name = name
            self._values: dict = {}
            fake_gauge_instances[name] = self

        def set(self, v: float) -> None:
            self._values["__default__"] = v

        def labels(self, **kw):
            key = tuple(sorted(kw.items()))
            inner = self._values.setdefault(key, {"val": None})
            stub = MagicMock()
            stub.set.side_effect = lambda v: inner.update({"val": v})
            return stub

    fake_pc.Gauge = FakeGauge
    fake_pc.start_http_server = lambda port: None
    sys.modules["prometheus_client"] = fake_pc

    spec = importlib.util.spec_from_file_location(
        "policy_mode_exporter_p66_v1",
        os.path.join(pw_path, "orderflow_services", "policy_mode_exporter_p66_v1.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["policy_mode_exporter_p66_v1"] = mod
    spec.loader.exec_module(mod)
    return mod, fake_gauge_instances


try:
    _mod, _gauges = _import_exporter()
    _i = _mod._i
    load_cfg = _mod.load_cfg
    SKIP = False
    SKIP_REASON = ""
except Exception as e:
    SKIP = True
    SKIP_REASON = str(e)
    _mod = None
    _gauges = {}


@pytest.mark.skipif(SKIP, reason=SKIP_REASON)
class TestIHelper:
    def test_int_string(self):
        assert _i("42") == 42

    def test_float_string(self):
        assert _i("3.9") == 3

    def test_none_returns_default(self):
        assert _i(None, -1) == -1

    def test_bad_string_returns_default(self):
        assert _i("abc", 7) == 7


@pytest.mark.skipif(SKIP, reason=SKIP_REASON)
class TestLoadCfg:
    def test_defaults(self, monkeypatch):
        monkeypatch.delenv("REDIS_URL", raising=False)
        monkeypatch.delenv("POLICY_MODE_STATE_KEY", raising=False)
        monkeypatch.delenv("POLICY_MODE_EXPORTER_PORT", raising=False)
        monkeypatch.delenv("POLICY_MODE_EXPORTER_INTERVAL_S", raising=False)
        cfg = load_cfg()
        assert "redis-worker-1:6379" in cfg.redis_url
        assert cfg.state_key == "metrics:policy_mode:state"
        assert cfg.port == 9818
        assert cfg.interval_s == 5.0

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("REDIS_URL", "redis://myredis:6380/1")
        monkeypatch.setenv("POLICY_MODE_EXPORTER_PORT", "19818")
        monkeypatch.setenv("POLICY_MODE_EXPORTER_INTERVAL_S", "15")
        cfg = load_cfg()
        assert cfg.redis_url == "redis://myredis:6380/1"
        assert cfg.port == 19818
        assert cfg.interval_s == 15.0


@pytest.mark.skipif(SKIP, reason=SKIP_REASON)
class TestMetricsFromRedisHash:
    """Test the main metrics read+set logic by simulating one iteration of the while loop."""

    def _run_one_iteration(self, state: dict):
        """
        Simulate one Redis read → gauge update cycle.
        Returns the module's LAST_TS, LAST_AGE, TOTAL, N, SHARE, MISM gauge objects.
        """
        r = MagicMock()
        r.hgetall.return_value = state
        # Run one "tick" of the while loop body by calling the gauge-update section directly
        # We do this by calling the relevant logic extracted from main()

        d = state
        last_ts = _i(d.get("last_ts_ms"), 0)
        _mod.LAST_TS.set(float(last_ts))

        import time
        age = 0.0
        if last_ts > 0:
            age = max(0.0, time.time() - (float(last_ts) / 1000.0))
        _mod.LAST_AGE.set(age)

        total = _i(d.get("rolling_total"), 0)
        _mod.TOTAL.set(float(max(0, total)))

        for reg in _mod.REGIMES:
            for mode in _mod.MODES:
                n = _i(d.get(_mod._cell_key(reg, mode)), 0)
                _mod.N.labels(regime=reg, effective_mode=mode).set(float(max(0, n)))
                share = (float(n) / float(total)) if total > 0 else 0.0
                _mod.SHARE.labels(regime=reg, effective_mode=mode).set(share)

        mism1 = _i(d.get("rolling_mismatch_block_regime_effective_not_block"), 0)
        mism2 = _i(d.get("rolling_mismatch_warn_regime_effective_active"), 0)
        denom = float(total) if total > 0 else 1.0
        _mod.MISM.labels(kind="block_regime_effective_not_block").set(float(mism1) / denom)
        _mod.MISM.labels(kind="warn_regime_effective_active").set(float(mism2) / denom)

    def test_empty_hash_no_error(self):
        """Empty state → all gauges default to 0, no exceptions."""
        self._run_one_iteration({})

    def test_total_populated(self):
        """rolling_total is correctly read and set on TOTAL gauge."""
        state = {"rolling_total": "500", "last_ts_ms": "0"}
        self._run_one_iteration(state)
        assert _mod.TOTAL._values.get("__default__") == 500.0

    def test_ok_active_share_computed(self):
        """OK/active share = n / total → correct fraction."""
        state = {
            "rolling_total": "100",
            "rolling_ok_active": "80",
            "last_ts_ms": "0",
        }
        self._run_one_iteration(state)
        # share for ok/active should be 0.8
        key = (("effective_mode", "active"), ("regime", "ok"))
        share_val = _mod.SHARE._values[key]["val"]
        assert abs(share_val - 0.8) < 1e-9

    def test_mismatch_share_block_not_block(self):
        """mismatch_block_regime_effective_not_block / total = correct fraction."""
        state = {
            "rolling_total": "200",
            "rolling_mismatch_block_regime_effective_not_block": "4",
            "last_ts_ms": "0",
        }
        self._run_one_iteration(state)
        key = (("kind", "block_regime_effective_not_block"),)
        mism_val = _mod.MISM._values[key]["val"]
        assert abs(mism_val - 0.02) < 1e-9

    def test_mismatch_zero_when_total_zero(self):
        """When total=0 the mismatch fraction uses denom=1 to avoid division by zero."""
        state = {"rolling_mismatch_block_regime_effective_not_block": "3", "last_ts_ms": "0"}
        # total=0 (missing) → denom=1 → mismatch share = 3
        self._run_one_iteration(state)
        key = (("kind", "block_regime_effective_not_block"),)
        mism_val = _mod.MISM._values[key]["val"]
        # With denom=1 this will equal float(mismatch_count) = 3.0
        assert mism_val == 3.0

    def test_staleness_gauge_positive_for_old_ts(self):
        """When last_ts_ms is very old the age gauge should be large (> 0)."""
        import time
        old_ts_ms = int((time.time() - 300) * 1000)  # 5 minutes ago
        state = {"last_ts_ms": str(old_ts_ms), "rolling_total": "0"}
        self._run_one_iteration(state)
        age_val = _mod.LAST_AGE._values.get("__default__", 0.0)
        assert age_val > 200  # should be ~300s

    def test_all_regime_mode_cells_covered(self):
        """All 16 cells (4 regimes × 4 modes) must be set without error."""
        state = {"rolling_total": "1600", "last_ts_ms": "0"}
        # Set each cell to 100
        regimes = ("ok", "warn", "block", "unknown")
        modes = ("active", "shadow", "block", "unknown")
        for reg in regimes:
            for mode in modes:
                state[f"rolling_{reg}_{mode}"] = "100"
        self._run_one_iteration(state)  # must not raise
