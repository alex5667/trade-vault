"""Plan 1 — runtime override (auto-demote → SHADOW) tests.

The auto-demote watcher writes `cfg:conf_meta_gate.mode=SHADOW` to Redis;
the runtime must pick this up before the next decision. We assert:

  * effective_mode() reads Redis only once per TTL window
  * SHADOW wins over CANARY / ENFORCE
  * SHADOW does NOT override OFF / LEGACY_ONLY / KILL_SWITCH (manual states)
  * malformed override values are ignored (fail-open to cfg.mode)
"""
from __future__ import annotations

from typing import Any

from services.confidence_meta_gate.config import MetaGateConfig, MetaGateMode
from services.confidence_meta_gate.runtime import MetaGateRuntime


def _cfg(mode: MetaGateMode) -> MetaGateConfig:
    return MetaGateConfig(
        enabled=True,
        mode=mode,
        model_path="/dev/null",
        calibrator_path="/dev/null",
        canary_share=0.0,
        canary_salt="t",
        fail_mode="LEGACY",
        max_model_age_hours=24.0,
        max_calibration_ece=0.07,
        min_p_win=0.56,
        min_expected_r=0.02,
        min_expected_edge_bps=1.5,
        dq_soft_cap=0.7,
        spread_soft_cap_bps=6.0,
        slippage_soft_cap_bps=6.0,
        risk_mult_enabled=False,
        metrics_stream="m",
        decision_stream="d",
        sample_features_in_stream=False,
    )


class _FakeRedis:
    def __init__(self, mode_value: str | None = None) -> None:
        self.mode_value = mode_value
        self.hget_calls = 0

    def hget(self, key: str, field: str) -> Any:
        self.hget_calls += 1
        if key != "cfg:conf_meta_gate":
            return None
        return self.mode_value


def test_no_redis_client_returns_cfg_mode() -> None:
    rt = MetaGateRuntime(_cfg(MetaGateMode.CANARY))
    assert rt.effective_mode(None) is MetaGateMode.CANARY


def test_override_shadow_wins_over_canary() -> None:
    rt = MetaGateRuntime(_cfg(MetaGateMode.CANARY))
    rc = _FakeRedis(mode_value="SHADOW")
    assert rt.effective_mode(rc) is MetaGateMode.SHADOW


def test_override_shadow_wins_over_enforce() -> None:
    rt = MetaGateRuntime(_cfg(MetaGateMode.ENFORCE))
    rc = _FakeRedis(mode_value="SHADOW")
    assert rt.effective_mode(rc) is MetaGateMode.SHADOW


def test_override_does_not_relax_kill_switch() -> None:
    rt = MetaGateRuntime(_cfg(MetaGateMode.KILL_SWITCH))
    rc = _FakeRedis(mode_value="ENFORCE")  # malicious / accidental relax
    assert rt.effective_mode(rc) is MetaGateMode.KILL_SWITCH


def test_override_does_not_relax_off() -> None:
    rt = MetaGateRuntime(_cfg(MetaGateMode.OFF))
    rc = _FakeRedis(mode_value="ENFORCE")
    assert rt.effective_mode(rc) is MetaGateMode.OFF


def test_override_does_not_relax_legacy_only() -> None:
    rt = MetaGateRuntime(_cfg(MetaGateMode.LEGACY_ONLY))
    rc = _FakeRedis(mode_value="ENFORCE")
    assert rt.effective_mode(rc) is MetaGateMode.LEGACY_ONLY


def test_override_does_not_upgrade_shadow_to_enforce() -> None:
    rt = MetaGateRuntime(_cfg(MetaGateMode.SHADOW))
    rc = _FakeRedis(mode_value="ENFORCE")
    # Only ever tighten — SHADOW must stay SHADOW even if Redis says ENFORCE.
    assert rt.effective_mode(rc) is MetaGateMode.SHADOW


def test_override_invalid_value_falls_back_to_cfg() -> None:
    rt = MetaGateRuntime(_cfg(MetaGateMode.CANARY))
    rc = _FakeRedis(mode_value="WHATEVER")
    assert rt.effective_mode(rc) is MetaGateMode.CANARY


def test_override_empty_value_clears_cache() -> None:
    rt = MetaGateRuntime(_cfg(MetaGateMode.CANARY))
    rc = _FakeRedis(mode_value="")
    assert rt.effective_mode(rc) is MetaGateMode.CANARY


def test_override_cache_avoids_redundant_redis_reads() -> None:
    rt = MetaGateRuntime(_cfg(MetaGateMode.CANARY))
    rc = _FakeRedis(mode_value="SHADOW")
    for _ in range(20):
        rt.effective_mode(rc)
    # First call hits Redis; subsequent are cached within TTL.
    assert rc.hget_calls == 1


def test_clear_override_cache_forces_redis_reread() -> None:
    rt = MetaGateRuntime(_cfg(MetaGateMode.CANARY))
    rc = _FakeRedis(mode_value="SHADOW")
    rt.effective_mode(rc)
    rt.clear_override_cache()
    rt.effective_mode(rc)
    assert rc.hget_calls == 2
