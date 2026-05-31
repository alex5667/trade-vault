"""Plan 3 / Step 4 — Optuna runtime evaluator + Redis manifest tests.

Does NOT require optuna installed — exercises only the local pure-Python
helpers in `orderflow_services.optuna_calibration_v1` (filter, ECE, Brier,
build_evaluator, publish_manifest).
"""
from __future__ import annotations

import math

from orderflow_services.optuna_calibration_v1 import (
    _compute_brier,
    _expected_calibration_error,
    _filter_rows_by_params,
    build_evaluator,
    maybe_write_to_disk,
    publish_manifest,
)


# ─── Filter ──────────────────────────────────────────────────────────────────


def test_filter_no_threshold_returns_all():
    rows = [{"calib_prob": 0.1}, {"calib_prob": 0.9}, {"calib_prob": None}]
    assert _filter_rows_by_params(rows, {"ml_p_min": 0.0}) == rows


def test_filter_drops_below_threshold():
    rows = [{"calib_prob": 0.1}, {"calib_prob": 0.9}]
    out = _filter_rows_by_params(rows, {"ml_p_min": 0.5})
    assert len(out) == 1
    assert out[0]["calib_prob"] == 0.9


def test_filter_drops_missing_calib_prob_when_gate_active():
    rows = [{"calib_prob": None}, {"calib_prob": 0.99}]
    out = _filter_rows_by_params(rows, {"ml_p_min": 0.5})
    assert len(out) == 1


def test_filter_skips_garbage_calib_prob():
    rows = [{"calib_prob": "not_a_number"}, {"calib_prob": 0.9}]
    out = _filter_rows_by_params(rows, {"ml_p_min": 0.5})
    assert len(out) == 1


# ─── Brier / ECE ────────────────────────────────────────────────────────────


def test_brier_zero_when_perfect_predictions():
    rows = [
        {"calib_prob": 1.0, "label": 1},
        {"calib_prob": 0.0, "label": -1},
    ]
    assert _compute_brier(rows) == 0.0


def test_brier_high_when_wrong():
    rows = [
        {"calib_prob": 1.0, "label": -1},
        {"calib_prob": 1.0, "label": -1},
    ]
    assert _compute_brier(rows) == 1.0


def test_brier_skips_missing_fields():
    rows = [
        {"calib_prob": 0.5, "label": 1},
        {"calib_prob": None, "label": 1},
        {"calib_prob": 0.5, "label": None},
    ]
    # Only first row counts; (0.5 - 1)^2 = 0.25
    assert _compute_brier(rows) == 0.25


def test_brier_empty_returns_zero():
    assert _compute_brier([]) == 0.0


def test_ece_zero_when_calibrated():
    """If predicted prob equals empirical win rate in every bin → ECE = 0."""
    rows: list[dict] = []
    # bin [0.0,0.1): all losers with p=0.05
    for _ in range(20):
        rows.append({"calib_prob": 0.05, "label": -1})
    # bin [0.9,1.0]: all winners with p=0.95
    for _ in range(20):
        rows.append({"calib_prob": 0.95, "label": 1})
    ece = _expected_calibration_error(rows, n_bins=10)
    # Empirical 0 vs predicted 0.05 → |0-0.05|=0.05; similarly 0.05 for top bin.
    # Weighted: 0.5*0.05 + 0.5*0.05 = 0.05 (close to perfect)
    assert ece < 0.1


def test_ece_high_when_anti_calibrated():
    rows: list[dict] = []
    for _ in range(40):
        rows.append({"calib_prob": 0.9, "label": -1})  # promised wins, all losses
    ece = _expected_calibration_error(rows, n_bins=10)
    assert ece > 0.5


def test_ece_handles_out_of_range_calib_prob():
    rows = [{"calib_prob": 1.5, "label": 1}, {"calib_prob": -0.1, "label": -1}]
    # Both excluded → ECE = 0 (no data, not a bug)
    assert _expected_calibration_error(rows) == 0.0


# ─── build_evaluator end-to-end ──────────────────────────────────────────────


def _synth_rows(n: int, win_rate: float = 0.5, base_ms: int = 0) -> list[dict]:
    rows: list[dict] = []
    for i in range(n):
        is_win = (i / n) < win_rate
        rows.append({
            "decision_time_ms": base_ms + i * 60_000,
            "resolved_time_ms": base_ms + i * 60_000 + 120_000,
            "label": 1 if is_win else -1,
            "realized_r": 1.0 if is_win else -1.0,
            "calib_prob": 0.8 if is_win else 0.2,
        })
    return rows


def test_evaluator_returns_zero_score_when_filter_eats_all():
    rows = _synth_rows(500)
    evaluator, n_total, span_days = build_evaluator(rows)
    assert n_total == 500
    res = evaluator({"ml_p_min": 0.99})  # filters out everything
    assert res.oos_trades == 0
    assert res.pass_rate == 0.0


def test_evaluator_carries_through_when_filter_keeps_most():
    rows = _synth_rows(500, win_rate=0.7)
    evaluator, _, _ = build_evaluator(rows)
    res = evaluator({"ml_p_min": 0.0})  # keep all
    assert res.oos_trades > 0
    assert res.pass_rate == 1.0
    assert math.isfinite(res.mean_oos_sharpe)
    assert math.isfinite(res.deflated_sharpe)


def test_evaluator_pass_rate_reflects_filter_strength():
    rows = _synth_rows(500, win_rate=0.7)
    evaluator, _, _ = build_evaluator(rows)
    # Threshold 0.5 keeps only the high-confidence positives (calib_prob=0.8)
    res = evaluator({"ml_p_min": 0.5})
    assert 0.0 < res.pass_rate < 1.0


# ─── Redis publish + disk write ──────────────────────────────────────────────


class _FakeRedis:
    def __init__(self, fail: bool = False):
        self.hashes: dict[str, dict] = {}
        self.expires: dict[str, int] = {}
        self.fail = fail

    def hset(self, key, mapping):
        if self.fail:
            raise RuntimeError("down")
        self.hashes.setdefault(key, {}).update(mapping)
        return len(mapping)

    def expire(self, key, ttl):
        self.expires[key] = ttl


def test_publish_manifest_writes_hash_and_expiry():
    rc = _FakeRedis()
    ok = publish_manifest(rc, "study_a", '{"x":1}')
    assert ok is True
    assert rc.hashes["optuna:manifest:study_a"]["manifest_json"] == '{"x":1}'
    assert rc.expires["optuna:manifest:study_a"] == 30 * 24 * 3600


def test_publish_manifest_fail_open_on_redis_error():
    rc = _FakeRedis(fail=True)
    assert publish_manifest(rc, "study_a", "{}") is False


def test_maybe_write_to_disk_skips_when_dir_empty():
    assert maybe_write_to_disk("", "candidate_x", "{}") is False


def test_maybe_write_to_disk_writes_file(tmp_path):
    ok = maybe_write_to_disk(str(tmp_path), "candidate_x", '{"y":2}')
    assert ok is True
    written = (tmp_path / "candidate_x.json").read_text()
    assert written == '{"y":2}'
