"""Unit tests for plan2_autopilot_telegram_reporter — pure logic only."""
from __future__ import annotations

from services.plan2_autopilot_telegram_reporter import (
    current_stage,
    fingerprint,
    format_report,
    should_send,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────


def _state(**overrides):
    base = {
        "now_ms": 1_000_000_000,
        "persister_enabled": False,
        "ph_enabled": False,
        "per_kind_demote": {"meta_lr_blend": False, "v14_of": False},
        "persister_age_h": 0.0,
        "ph_age_h": 0.0,
        "expectancy_threshold": 0.0,
        "tracker_xlen": 0,
        "warn_per_kind": {"meta_lr_blend": 0, "v14_of": 0},
    }
    base.update(overrides)
    return base


def _stats(**overrides):
    base = {"rows_1h": 0, "rows_24h": 0, "table_exists": False}
    base.update(overrides)
    return base


# ─── current_stage ──────────────────────────────────────────────────────────


def test_stage_zero_when_all_off():
    assert current_stage(_state()) == 0


def test_stage_one_when_persister_on():
    assert current_stage(_state(persister_enabled=True)) == 1


def test_stage_two_when_ph_on():
    assert current_stage(_state(persister_enabled=True, ph_enabled=True)) == 2


def test_stage_three_when_any_per_kind_active():
    s = _state(
        persister_enabled=True, ph_enabled=True,
        per_kind_demote={"meta_lr_blend": True, "v14_of": False},
    )
    assert current_stage(s) == 3


# ─── fingerprint ────────────────────────────────────────────────────────────


def test_fingerprint_stable_on_unchanged_state():
    s = _state(persister_enabled=True, expectancy_threshold=-0.02)
    st = _stats(table_exists=True, rows_24h=1000)
    assert fingerprint(s, st) == fingerprint(s, st)


def test_fingerprint_changes_on_stage_advance():
    s1 = _state(persister_enabled=True)
    s2 = _state(persister_enabled=True, ph_enabled=True)
    assert fingerprint(s1, _stats()) != fingerprint(s2, _stats())


def test_fingerprint_changes_on_kind_activation():
    s1 = _state(persister_enabled=True, ph_enabled=True)
    s2 = _state(
        persister_enabled=True, ph_enabled=True,
        per_kind_demote={"meta_lr_blend": True, "v14_of": False},
    )
    assert fingerprint(s1, _stats()) != fingerprint(s2, _stats())


def test_fingerprint_changes_on_threshold_ratchet():
    s1 = _state(expectancy_threshold=0.0)
    s2 = _state(expectancy_threshold=-0.05)
    assert fingerprint(s1, _stats()) != fingerprint(s2, _stats())


def test_fingerprint_ignores_threshold_jitter_below_3_decimals():
    # 0.001 rounding so noise-floor wobble does not spam notifications.
    s1 = _state(expectancy_threshold=0.00040)
    s2 = _state(expectancy_threshold=0.00045)
    assert fingerprint(s1, _stats()) == fingerprint(s2, _stats())


def test_fingerprint_ignores_telemetry_row_changes():
    # Mere row growth does not constitute a stage change.
    s = _state(persister_enabled=True)
    assert fingerprint(s, _stats(rows_24h=1000)) == fingerprint(s, _stats(rows_24h=2000))


# ─── format_report ──────────────────────────────────────────────────────────


def test_report_stage0_baseline():
    out = format_report(_state(), _stats())
    assert "Stage 0/3" in out
    assert "S1</b> persister SHADOW" in out


def test_report_stage1_active():
    s = _state(persister_enabled=True, persister_age_h=24.0)
    out = format_report(s, _stats(table_exists=True, rows_24h=1500))
    assert "Stage 1/3" in out
    assert "S1</b> persister active" in out
    assert "S2</b> page_hinkley pending" in out


def test_report_stage2_active_pending_s3():
    s = _state(
        persister_enabled=True, ph_enabled=True,
        persister_age_h=120.0, ph_age_h=50.0,
    )
    out = format_report(s, _stats(table_exists=True, rows_24h=2000))
    assert "Stage 2/3" in out
    assert "S2</b> page_hinkley active" in out
    assert "S3</b> per-kind pending" in out


def test_report_stage3_lists_active_kinds():
    s = _state(
        persister_enabled=True, ph_enabled=True,
        persister_age_h=300.0, ph_age_h=200.0,
        per_kind_demote={"meta_lr_blend": True, "v14_of": False},
    )
    out = format_report(s, _stats(table_exists=True))
    assert "Stage 3/3" in out
    assert "auto-demote: meta_lr_blend" in out
    assert "v14_of" not in out.split("auto-demote:")[1].split("\n")[0]


def test_report_shows_expectancy_threshold_when_tuned():
    s = _state(persister_enabled=True, ph_enabled=True, expectancy_threshold=-0.0432)
    out = format_report(s, _stats())
    assert "-0.0432" in out
    assert "autotuned" in out


def test_report_shows_default_when_threshold_unset():
    s = _state(persister_enabled=True)
    out = format_report(s, _stats())
    assert "0.0000 (default)" in out


def test_report_includes_telemetry_block():
    s = _state(persister_enabled=True, tracker_xlen=12345)
    st = _stats(table_exists=True, rows_1h=200, rows_24h=5000)
    out = format_report(s, st)
    assert "persister rows 1h:  200" in out
    assert "persister rows 24h: 5000" in out
    assert "tracker xlen:       12345" in out


def test_report_escapes_kind_names():
    # If a malicious or weird kind name slips in via allowlist, HTML tags must
    # not be passed through to Telegram parse_mode=HTML (causes 400).
    s = _state(
        persister_enabled=True, ph_enabled=True,
        per_kind_demote={"<script>": True},
        warn_per_kind={"<script>": 3},
    )
    out = format_report(s, _stats())
    assert "<script>" not in out
    assert "&lt;script&gt;" in out


# ─── should_send ────────────────────────────────────────────────────────────


def test_should_send_first_run():
    decision, reason = should_send(
        current_fingerprint="fp1", last_fingerprint=None,
        last_sent_ts_ms=None, now_ms=1_000_000, keepalive_hours=24.0,
    )
    assert decision is True
    assert reason == "first_run"


def test_should_send_fingerprint_changed():
    decision, reason = should_send(
        current_fingerprint="fp2", last_fingerprint="fp1",
        last_sent_ts_ms=999_000_000, now_ms=999_001_000, keepalive_hours=24.0,
    )
    assert decision is True
    assert reason == "fingerprint_changed"


def test_should_not_send_when_no_change_within_keepalive():
    now = 1_000_000_000
    decision, reason = should_send(
        current_fingerprint="fp1", last_fingerprint="fp1",
        last_sent_ts_ms=now - 3 * 3_600_000,  # 3h ago
        now_ms=now, keepalive_hours=24.0,
    )
    assert decision is False
    assert reason.startswith("no_change_age_")


def test_should_send_keepalive_after_24h():
    now = 1_000_000_000
    decision, reason = should_send(
        current_fingerprint="fp1", last_fingerprint="fp1",
        last_sent_ts_ms=now - 25 * 3_600_000,  # 25h ago
        now_ms=now, keepalive_hours=24.0,
    )
    assert decision is True
    assert reason.startswith("keepalive_")


def test_should_send_keepalive_boundary():
    now = 1_000_000_000
    decision, _ = should_send(
        current_fingerprint="fp", last_fingerprint="fp",
        last_sent_ts_ms=now - int(24.0 * 3_600_000),
        now_ms=now, keepalive_hours=24.0,
    )
    assert decision is True
