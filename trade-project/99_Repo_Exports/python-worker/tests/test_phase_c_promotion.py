"""Phase C regression tests:
  - evaluate_bucket() гейты (n / timeout / avg_r / ev_r / ci_low);
  - build_snapshot() формирует HMAC-подписанный payload, совместимый с
    _RegimeExecOverridesReader (engine читает его в runtime);
  - PromotionRunner SHADOW не публикует;
  - PromotionRunner ENFORCE без HMAC отказывается публиковать;
  - проход end-to-end: smoke по нескольким рядам.
"""

from __future__ import annotations

import hashlib
import hmac
import json

import pytest

from services.regime_exec_promotion_v1 import (
    BucketDecision,
    BucketRow,
    PromotionGates,
    PromotionRunner,
    _bucket_key_for_row,
    build_snapshot,
    evaluate_bucket,
)


def _row(**kwargs) -> BucketRow:
    defaults = dict(
        symbol="GLOBAL",
        regime_label="shock",
        scenario="trending",
        direction="LONG",
        n=500,
        win_rate=0.55,
        avg_r=0.4,
        ev_r_after_costs=0.20,
        mfe_r_p50=1.2,
        mfe_r_p90=2.0,
        mae_r_p50=0.5,
        mae_r_p90=1.0,
        timeout_rate=0.10,
    )
    defaults.update(kwargs)
    return BucketRow(**defaults)


# ────────────────────────── per-gate behaviour ──────────────────────────────────
def test_gate_n_too_small_returns_skip():
    d = evaluate_bucket(_row(n=10), PromotionGates())
    assert d.decision == "skip"
    assert "n=10" in d.reason


def test_gate_timeout_excessive_returns_skip():
    d = evaluate_bucket(_row(timeout_rate=0.85), PromotionGates())
    assert d.decision == "skip"
    assert "timeout_rate" in d.reason


def test_gate_avg_r_negative_returns_skip():
    d = evaluate_bucket(_row(avg_r=-0.5), PromotionGates())
    assert d.decision == "skip"
    assert "avg_r" in d.reason


def test_gate_ev_r_below_threshold_returns_shadow():
    d = evaluate_bucket(_row(ev_r_after_costs=0.02), PromotionGates())
    assert d.decision == "shadow"
    assert "ev_r" in d.reason


def test_gate_ci_low_negative_returns_shadow():
    """Высокий ev_r, но avg_r сильно отличается → широкая CI → ci_low<=0."""
    # При sd_proxy = |1.0 - 0.06| = 0.94, n=300, half = 1.96 * 0.94 / sqrt(300) ≈ 0.106
    # ci_low = 0.06 - 0.106 ≈ -0.046  → shadow.
    d = evaluate_bucket(_row(n=300, avg_r=1.0, ev_r_after_costs=0.06), PromotionGates())
    assert d.decision == "shadow"
    assert "ci_low" in d.reason


def test_all_gates_passed_returns_enforce_proposed():
    d = evaluate_bucket(_row(), PromotionGates())
    assert d.decision == "enforce_proposed"
    assert d.reason == "all_gates_passed"
    # Маппер для shock+trending должен выдать rocket_v1.
    assert d.proposed_policy["trail_profile"] == "rocket_v1"
    assert d.proposed_policy["tp1_target_r"] == 1.5  # mfe_p50=1.2 >=1.0 → 1.5R


def test_policy_for_range_bucket():
    d = evaluate_bucket(
        _row(regime_label="calm", scenario="range", mfe_r_p50=0.4),
        PromotionGates(),
    )
    assert d.decision == "enforce_proposed"
    assert d.proposed_policy["trail_profile"] == "range_protective"
    assert d.proposed_policy["tp1_target_r"] == 0.3


# ─────────────────────────────── snapshot ───────────────────────────────────────
def test_build_snapshot_includes_only_enforce_proposed():
    decisions = [
        BucketDecision(
            bucket_key="GLOBAL|shock|trending", decision="enforce_proposed",
            reason="ok", n=500, ev_r=0.2, avg_r=0.4, ci_low=0.1, ci_high=0.3,
            proposed_policy={"tp1_target_r": 1.5, "trail_profile": "rocket_v1"},
        ),
        BucketDecision(
            bucket_key="GLOBAL|calm|range", decision="shadow",
            reason="ev_r low", n=400, ev_r=0.02, avg_r=0.1, ci_low=0.0, ci_high=0.04,
        ),
    ]
    snap = build_snapshot(decisions, hmac_secret="")
    assert "GLOBAL|shock|trending" in snap["buckets"]
    assert "GLOBAL|calm|range" not in snap["buckets"]
    assert "sig" not in snap  # без HMAC


def test_snapshot_hmac_verifiable_by_engine_reader_format():
    """Совместимость snapshot-а с форматом _RegimeExecOverridesReader.

    Engine: canon = json.dumps(payload_без_sig, sort_keys=True, separators=(",", ":"))
    Подпись = HMAC-SHA256(secret, canon).hexdigest()
    """
    decisions = [
        BucketDecision(
            bucket_key="GLOBAL|shock|trending", decision="enforce_proposed",
            reason="ok", n=500, ev_r=0.2, avg_r=0.4, ci_low=0.1, ci_high=0.3,
            proposed_policy={"tp1_target_r": 1.5},
        ),
    ]
    secret = "test-secret"
    snap = build_snapshot(decisions, hmac_secret=secret, ts_ms=1_700_000_000_000)
    assert "sig" in snap

    payload_without_sig = {k: v for k, v in snap.items() if k != "sig"}
    canon = json.dumps(payload_without_sig, sort_keys=True, separators=(",", ":")).encode()
    expected = hmac.new(secret.encode(), canon, hashlib.sha256).hexdigest()
    assert snap["sig"] == expected


# ──────────────────────────────── runner ────────────────────────────────────────
def test_runner_shadow_does_not_publish():
    decisions: list[BucketDecision] = []
    published: list[dict] = []

    runner = PromotionRunner(
        fetch_rows=lambda: [_row()],
        write_decision=decisions.append,
        publish_snapshot=published.append,
        enforce=False,
        hmac_secret="abc",
    )
    runner.run_once()

    assert len(decisions) == 1
    assert decisions[0].decision == "enforce_proposed"
    assert published == []  # SHADOW — публикации нет


def test_runner_enforce_refuses_without_hmac():
    """Защита: enforce без HMAC секрета не публикует (защита от misconfig)."""
    published: list[dict] = []
    runner = PromotionRunner(
        fetch_rows=lambda: [_row()],
        write_decision=lambda _: None,
        publish_snapshot=published.append,
        enforce=True,
        hmac_secret="",
    )
    runner.run_once()
    assert published == []


def test_runner_enforce_with_hmac_publishes_signed_payload():
    published: list[dict] = []
    runner = PromotionRunner(
        fetch_rows=lambda: [_row()],
        write_decision=lambda _: None,
        publish_snapshot=published.append,
        enforce=True,
        hmac_secret="xyz",
    )
    runner.run_once()
    assert len(published) == 1
    snap = published[0]
    assert "sig" in snap
    assert "GLOBAL|shock|trending" in snap["buckets"]


def test_runner_mixed_rows_only_enforce_proposed_published():
    rows = [
        _row(),  # passes all gates
        _row(scenario="range", n=20),  # n too small → skip
        _row(scenario="squeeze", ev_r_after_costs=0.01),  # shadow
    ]
    published: list[dict] = []
    decisions_log: list[BucketDecision] = []
    runner = PromotionRunner(
        fetch_rows=lambda: rows,
        write_decision=decisions_log.append,
        publish_snapshot=published.append,
        enforce=True,
        hmac_secret="zzz",
    )
    runner.run_once()
    assert len(decisions_log) == 3
    assert {d.decision for d in decisions_log} == {"enforce_proposed", "skip", "shadow"}
    assert len(published[0]["buckets"]) == 1


# ─────────────────────────── bucket_key compatibility ───────────────────────────
def test_bucket_key_format_matches_engine_expectation():
    """Engine: hierarchical lookup ожидает '{SCOPE}|{VOL}|{TREND}'."""
    row = _row(regime_label="shock", scenario="trending")
    assert _bucket_key_for_row(row) == "GLOBAL|shock|trending"
