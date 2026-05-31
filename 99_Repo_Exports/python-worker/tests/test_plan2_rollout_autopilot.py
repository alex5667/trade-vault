"""Unit tests for the Plan 2 rollout autopilot.

Pure-function coverage: stage gates + ratchet auto-tune.
Redis side-effects are tested via FakeRedis.
"""
from __future__ import annotations

from core.plan2_autopilot_flags import (
    AUTOPILOT_KEY,
    FIELD_EXPECTANCY_THRESHOLD,
    FLAG_DRIFT_PH_ENABLED,
    FLAG_PERSISTER_ENABLED,
    activated_at_field,
    is_kind_auto_demote_enabled,
    kind_demote_flag,
    read_plan2_flag,
    read_plan2_float,
)
from orderflow_services.plan2_rollout_autopilot_v1 import (
    activate_flag_sticky,
    autotune_expectancy_threshold,
    decide_stage1,
    decide_stage2,
    decide_stage3_per_kind,
    hours_since_flag_activation,
    write_expectancy_threshold,
)


# ─── FakeRedis ───────────────────────────────────────────────────────────────


class _FakeRedis:
    def __init__(self):
        self.hashes: dict[str, dict] = {}

    def hset(self, key, field=None, value=None, mapping=None, **kwargs):
        # Three call shapes used by redis-py:
        #   hset(key, mapping={...})
        #   hset(key, field, value)
        #   hset(key, field=..., value=...)
        d = self.hashes.setdefault(key, {})
        if mapping:
            d.update({k: str(v) for k, v in mapping.items()})
            return len(mapping)
        if field is not None and value is not None:
            d[field] = str(value)
            return 1
        if kwargs:
            d.update({k: str(v) for k, v in kwargs.items()})
            return len(kwargs)
        return 0

    def hsetnx(self, key, field, value) -> int:
        d = self.hashes.setdefault(key, {})
        if field in d:
            return 0
        d[field] = str(value)
        return 1

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)


# ─── Stage 1 ─────────────────────────────────────────────────────────────────


def test_stage1_blocked_when_table_missing():
    ok, reason = decide_stage1(
        table_exists=False, tracker_rows_1h=500, min_tracker_rows=100,
    )
    assert ok is False
    assert reason == "table_missing"


def test_stage1_blocked_when_tracker_idle():
    ok, reason = decide_stage1(
        table_exists=True, tracker_rows_1h=50, min_tracker_rows=100,
    )
    assert ok is False
    assert "tracker_rows_below_min" in reason


def test_stage1_passes_when_ready():
    ok, reason = decide_stage1(
        table_exists=True, tracker_rows_1h=500, min_tracker_rows=100,
    )
    assert ok is True
    assert reason == "ok"


# ─── Stage 2 ─────────────────────────────────────────────────────────────────


def _s2_defaults(**overrides):
    base = dict(
        s1_active_hours=72.0,
        persister_errors_24h=0,
        persister_rows_24h=2000,
        warn_shadow_rate_per_hour_per_kind=1.0,
        min_hours=48.0,
        max_errors=0,
        min_rows=500,
        max_warn_rate=5.0,
    )
    base.update(overrides)
    return base


def test_stage2_passes_under_ideal_conditions():
    ok, reason = decide_stage2(**_s2_defaults())
    assert ok is True
    assert reason == "ok"


def test_stage2_blocked_s1_too_young():
    ok, reason = decide_stage2(**_s2_defaults(s1_active_hours=24.0))
    assert ok is False
    assert "s1_too_young" in reason


def test_stage2_blocked_persister_errors():
    ok, reason = decide_stage2(**_s2_defaults(persister_errors_24h=5))
    assert ok is False
    assert "persister_errors" in reason


def test_stage2_blocked_too_few_rows():
    ok, reason = decide_stage2(**_s2_defaults(persister_rows_24h=100))
    assert ok is False
    assert "persisted_rows_below_min" in reason


def test_stage2_blocked_warn_spammy():
    ok, reason = decide_stage2(**_s2_defaults(warn_shadow_rate_per_hour_per_kind=10.0))
    assert ok is False
    assert "warn_shadow_too_spammy" in reason


# ─── Stage 3 per-kind ────────────────────────────────────────────────────────


def _s3_defaults(**overrides):
    base = dict(
        kind="meta_lr_blend",
        s2_active_hours=200.0,
        warn_count_7d=0,
        allowlist=["meta_lr_blend", "v14_of"],
        min_hours=168.0,
        max_warns_per_week=1,
    )
    base.update(overrides)
    return base


def test_stage3_passes_for_listed_stable_kind():
    ok, reason = decide_stage3_per_kind(**_s3_defaults())
    assert ok is True
    assert reason == "ok"


def test_stage3_blocked_not_in_allowlist():
    ok, reason = decide_stage3_per_kind(**_s3_defaults(kind="unstable_v1"))
    assert ok is False
    assert reason == "not_in_allowlist"


def test_stage3_blocked_s2_too_young():
    ok, reason = decide_stage3_per_kind(**_s3_defaults(s2_active_hours=10.0))
    assert ok is False
    assert "s2_too_young" in reason


def test_stage3_blocked_too_many_warns():
    ok, reason = decide_stage3_per_kind(**_s3_defaults(warn_count_7d=5))
    assert ok is False
    assert "too_many_warns" in reason


def test_stage3_kind_case_insensitive():
    ok, reason = decide_stage3_per_kind(
        **_s3_defaults(kind="META_LR_BLEND"),
    )
    assert ok is True


def test_stage3_blocked_empty_allowlist():
    ok, reason = decide_stage3_per_kind(**_s3_defaults(allowlist=[]))
    assert ok is False
    assert reason == "allowlist_empty"


# ─── Expectancy threshold auto-tune ──────────────────────────────────────────


def test_autotune_below_min_days_returns_none():
    out = autotune_expectancy_threshold(
        daily_expectancies=[0.0] * 5, floor=-0.10, min_days=7, current_value=0.0,
    )
    assert out is None


def test_autotune_floor_clamps_extreme_negative():
    # Very negative observations should be clamped to floor.
    out = autotune_expectancy_threshold(
        daily_expectancies=[-1.0] * 14, floor=-0.10, min_days=7, current_value=0.0,
    )
    # Ratchet: -0.10 < current 0.0 → no update
    assert out is None


def test_autotune_first_run_returns_p25():
    # No prior value → return p25.
    vals = [-0.05, -0.02, 0.0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07]
    out = autotune_expectancy_threshold(
        daily_expectancies=vals, floor=-0.10, min_days=7, current_value=None,
    )
    assert out is not None
    sorted_vals = sorted(vals)
    expected = max(sorted_vals[round(0.25 * (len(vals) - 1))], -0.10)
    assert abs(out - expected) < 1e-9


def test_autotune_ratchet_only_more_conservative():
    # Current threshold is -0.05. New observed p25 is -0.02 (less negative,
    # i.e. more conservative). → ratchet up.
    out = autotune_expectancy_threshold(
        daily_expectancies=[-0.02] * 14,
        floor=-0.10, min_days=7, current_value=-0.05,
    )
    assert out is not None
    assert out == -0.02


def test_autotune_does_not_loosen():
    # Current threshold is 0.0. New observed p25 is -0.03 (looser).
    # ratchet: do not loosen.
    out = autotune_expectancy_threshold(
        daily_expectancies=[-0.03] * 14,
        floor=-0.10, min_days=7, current_value=0.0,
    )
    assert out is None


# ─── Redis side-effects (sticky activation + timestamp) ──────────────────────


def test_activate_flag_sticky_first_set_returns_true():
    rc = _FakeRedis()
    assert activate_flag_sticky(rc, FLAG_PERSISTER_ENABLED, now_ms=1000) is True
    assert rc.hashes[AUTOPILOT_KEY][FLAG_PERSISTER_ENABLED] == "1"
    assert rc.hashes[AUTOPILOT_KEY][activated_at_field(FLAG_PERSISTER_ENABLED)] == "1000"


def test_activate_flag_sticky_second_set_returns_false():
    rc = _FakeRedis()
    activate_flag_sticky(rc, FLAG_PERSISTER_ENABLED, now_ms=1000)
    assert activate_flag_sticky(rc, FLAG_PERSISTER_ENABLED, now_ms=5000) is False
    # Timestamp must not be overwritten (sticky).
    assert rc.hashes[AUTOPILOT_KEY][activated_at_field(FLAG_PERSISTER_ENABLED)] == "1000"


def test_hours_since_activation():
    rc = _FakeRedis()
    activate_flag_sticky(rc, FLAG_PERSISTER_ENABLED, now_ms=0)
    h = hours_since_flag_activation(
        rc, FLAG_PERSISTER_ENABLED, now_ms=3 * 3_600_000,
    )
    assert abs(h - 3.0) < 1e-6


def test_hours_since_activation_returns_zero_when_unset():
    rc = _FakeRedis()
    assert hours_since_flag_activation(rc, FLAG_DRIFT_PH_ENABLED, now_ms=1) == 0.0


# ─── Reader helpers ──────────────────────────────────────────────────────────


def test_read_plan2_flag_default_false():
    rc = _FakeRedis()
    assert read_plan2_flag(rc, FLAG_PERSISTER_ENABLED) is False


def test_read_plan2_flag_true_after_activation():
    rc = _FakeRedis()
    activate_flag_sticky(rc, FLAG_PERSISTER_ENABLED, now_ms=0)
    assert read_plan2_flag(rc, FLAG_PERSISTER_ENABLED) is True


def test_read_plan2_float_default_when_missing():
    rc = _FakeRedis()
    assert read_plan2_float(rc, FIELD_EXPECTANCY_THRESHOLD, default=0.5) == 0.5


def test_read_plan2_float_returns_tuned_value():
    rc = _FakeRedis()
    write_expectancy_threshold(rc, -0.04)
    assert (
        abs(read_plan2_float(rc, FIELD_EXPECTANCY_THRESHOLD, default=0.0) + 0.04)
        < 1e-6
    )


def test_kind_demote_flag_normalizes_case():
    assert kind_demote_flag("META_LR_BLEND") == "drift_auto_demote_kind_meta_lr_blend"
    assert kind_demote_flag(" v14_of ") == "drift_auto_demote_kind_v14_of"


def test_is_kind_auto_demote_enabled_per_kind():
    rc = _FakeRedis()
    activate_flag_sticky(rc, kind_demote_flag("meta_lr_blend"), now_ms=0)
    assert is_kind_auto_demote_enabled(rc, "meta_lr_blend") is True
    # Different kind should be independent.
    assert is_kind_auto_demote_enabled(rc, "v14_of") is False
    # Empty kind never enabled.
    assert is_kind_auto_demote_enabled(rc, "") is False
