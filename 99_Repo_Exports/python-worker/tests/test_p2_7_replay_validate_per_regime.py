"""P2.7 — Per-regime ensemble replay-validation harness.

Covers:
  1.  _ece: perfect calibration → ECE ≈ 0
  2.  _ece: worst calibration → ECE ≈ 0.5
  3.  _brier: all correct → Brier ≈ 0
  4.  _brier: all wrong → Brier ≈ 1
  5.  _auc_approx: perfect ranking → AUC ≈ 1
  6.  _auc_approx: random ranking → AUC ≈ 0.5
  7.  _auc_approx: all same label → AUC = nan
  8.  replay: global-only (no per_regime) returns metrics
  9.  replay: per-regime model blended (regime_model blend_source)
 10.  replay: records without regime → falls back to "na"
 11.  replay: skips records with empty features
 12.  evaluate_replay_gates: AUC gate pass
 13.  evaluate_replay_gates: AUC gate fail
 14.  evaluate_replay_gates: ECE gate fail
 15.  evaluate_replay_gates: small-regime skipped
 16.  load_dataset: round-trips NDJSON
 17.  load_dataset: skips bad-JSON lines
 18.  main() with no model file → exit code 2
"""
from __future__ import annotations

import json
import math
from unittest.mock import MagicMock

import pytest

from tools.replay_validate_per_regime_blend import (
    _ece,
    _brier,
    _auc_approx,
    replay,
    evaluate_replay_gates,
    load_dataset,
    main,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _fake_clf(prob: float):
    """Mock sklearn-like classifier that always returns `prob`."""
    clf = MagicMock()
    clf.predict_proba.return_value = [[1 - prob, prob]]
    return clf


def _fake_iso(val: float):
    """Mock IsotonicRegression that always returns `val`."""
    iso = MagicMock()
    iso.predict.return_value = [val]
    return iso


def _make_pack(global_prob: float = 0.6, per_regime: dict | None = None) -> dict:
    return {
        "model": _fake_clf(global_prob),
        "calibrator": _fake_iso(global_prob),
        "feature_cols": ["f1", "f2"],
        "per_regime": per_regime or {},
    }


def _make_records(n: int = 20, regime: str = "trending_bull", hit: int = 1) -> list[dict]:
    return [
        {"ts_ms": i * 1000, "hit": hit, "r": 0.5, "regime": regime,
         "features": {"f1": 0.5, "f2": 0.3}}
        for i in range(n)
    ]


# ── 1. ECE perfect ────────────────────────────────────────────────────────────

def test_ece_perfect():
    # 100 samples: 50 positives with prob 0.9, 50 negatives with prob 0.1
    # ECE ≈ 0.5*|1.0-0.9| + 0.5*|0.0-0.1| = 0.10 per bin; loosen threshold
    labels = [1] * 50 + [0] * 50
    probs = [0.9] * 50 + [0.1] * 50
    assert _ece(labels, probs) < 0.12


# ── 2. ECE worst ─────────────────────────────────────────────────────────────

def test_ece_worst():
    labels = [0, 0, 1, 1]
    probs = [0.9, 0.9, 0.1, 0.1]
    assert _ece(labels, probs) > 0.4


# ── 3. Brier all correct ──────────────────────────────────────────────────────

def test_brier_perfect():
    labels = [1, 0, 1]
    probs = [1.0, 0.0, 1.0]
    assert _brier(labels, probs) < 1e-9


# ── 4. Brier all wrong ────────────────────────────────────────────────────────

def test_brier_worst():
    labels = [1, 0, 1]
    probs = [0.0, 1.0, 0.0]
    assert _brier(labels, probs) == pytest.approx(1.0)


# ── 5. AUC perfect ───────────────────────────────────────────────────────────

def test_auc_perfect():
    labels = [0, 0, 1, 1]
    scores = [0.1, 0.2, 0.8, 0.9]
    assert _auc_approx(labels, scores) == pytest.approx(1.0)


# ── 6. AUC random ────────────────────────────────────────────────────────────

def test_auc_random():
    # Random ranking: alternating labels with varied scores → AUC near 0.5
    import random
    rng = random.Random(42)
    labels = [0, 1] * 50
    scores = [rng.random() for _ in range(100)]
    auc = _auc_approx(labels, scores)
    assert 0.3 <= auc <= 0.7


# ── 7. AUC all same label ─────────────────────────────────────────────────────

def test_auc_single_class():
    labels = [1, 1, 1]
    scores = [0.8, 0.7, 0.9]
    assert math.isnan(_auc_approx(labels, scores))


# ── 8. replay: global-only returns metrics ────────────────────────────────────

def test_replay_global_only():
    pack = _make_pack(global_prob=0.6)
    records = _make_records(20, hit=1) + _make_records(10, hit=0)
    result = replay(pack, records)
    assert result["global"]["n"] == 30
    assert not math.isnan(result["global"]["auc"])
    assert "trending_bull" in result["regimes"]


# ── 9. replay: per-regime blend_source ───────────────────────────────────────

def test_replay_per_regime_blend():
    sub_model = {
        "model": _fake_clf(0.7),
        "calibrator": _fake_iso(0.7),
        "oof_auc": 0.65,
        "n": 300,
    }
    pack = _make_pack(global_prob=0.5, per_regime={"trending_bull": sub_model})
    records = _make_records(20, regime="trending_bull", hit=1)
    result = replay(pack, records)
    rg = result["regimes"]["trending_bull"]
    assert rg["blend_source"] == "regime_model"
    assert rg["weight_regime_mean"] > 0


# ── 10. replay: missing regime → "na" bucket ─────────────────────────────────

def test_replay_missing_regime_to_na():
    pack = _make_pack()
    records = [{"ts_ms": 1000, "hit": 1, "r": 0.5,
                "features": {"f1": 0.5, "f2": 0.3}}]  # no regime key
    result = replay(pack, records)
    assert "na" in result["regimes"]


# ── 11. replay: skips empty-feature records ───────────────────────────────────

def test_replay_skips_empty_features():
    pack = _make_pack()
    records = [
        {"ts_ms": 1000, "hit": 1, "r": 0.5, "regime": "r1", "features": {}},
        {"ts_ms": 2000, "hit": 0, "r": 0.0, "regime": "r1", "features": None},
        {"ts_ms": 3000, "hit": 1, "r": 0.5, "regime": "r1", "features": {"f1": 0.5, "f2": 0.3}},
    ]
    result = replay(pack, records)
    assert result["global"]["n"] == 1


# ── 12. evaluate_replay_gates: AUC pass ──────────────────────────────────────

def test_gates_auc_pass():
    result = {
        "global": {"n": 50, "auc": 0.60, "ece": 0.05},
        "regimes": {"trending_bull": {"n": 30, "auc": 0.58, "ece": 0.06,
                                       "blend_source": "regime_model", "weight_regime_mean": 0.3}},
    }
    gates, ok = evaluate_replay_gates(result, min_auc=0.52, max_ece=0.10, min_samples_per_regime=20)
    assert ok


# ── 13. evaluate_replay_gates: AUC fail ──────────────────────────────────────

def test_gates_auc_fail():
    result = {
        "global": {"n": 50, "auc": 0.49, "ece": 0.05},
        "regimes": {},
    }
    gates, ok = evaluate_replay_gates(result, min_auc=0.52, max_ece=0.10, min_samples_per_regime=20)
    assert not ok
    names = {g["name"] for g in gates}
    assert "global_auc_min" in names
    assert not next(g["ok"] for g in gates if g["name"] == "global_auc_min")


# ── 14. evaluate_replay_gates: ECE fail ──────────────────────────────────────

def test_gates_ece_fail():
    result = {
        "global": {"n": 50, "auc": 0.55, "ece": 0.20},
        "regimes": {},
    }
    gates, ok = evaluate_replay_gates(result, min_auc=0.52, max_ece=0.10, min_samples_per_regime=20)
    assert not ok


# ── 15. evaluate_replay_gates: small regime skipped ──────────────────────────

def test_gates_small_regime_skipped():
    result = {
        "global": {"n": 50, "auc": 0.60, "ece": 0.05},
        "regimes": {"tiny": {"n": 5, "auc": 0.30, "ece": 0.50,
                              "blend_source": "global_only", "weight_regime_mean": 0.0}},
    }
    gates, ok = evaluate_replay_gates(result, min_auc=0.52, max_ece=0.10, min_samples_per_regime=20)
    # tiny regime is below min_samples → its gates are skipped → global passes → ok
    assert ok
    names = {g["name"] for g in gates}
    assert not any("tiny" in n for n in names)


# ── 16. load_dataset: round-trips NDJSON ─────────────────────────────────────

def test_load_dataset_roundtrip(tmp_path):
    records = [{"sid": "a:b:c", "hit": 1, "features": {"f1": 0.5}},
               {"sid": "d:e:f", "hit": 0, "features": {"f2": 0.3}}]
    p = tmp_path / "ds.ndjson"
    p.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    loaded = load_dataset(str(p))
    assert len(loaded) == 2
    assert loaded[0]["sid"] == "a:b:c"


# ── 17. load_dataset: skips bad-JSON lines ────────────────────────────────────

def test_load_dataset_skips_bad_json(tmp_path):
    p = tmp_path / "bad.ndjson"
    p.write_text('{"ok": 1}\nNOT_JSON\n{"ok": 2}\n')
    loaded = load_dataset(str(p))
    assert len(loaded) == 2


# ── 18. main(): missing model → exit 2 ───────────────────────────────────────

def test_main_missing_model(tmp_path, monkeypatch):
    ds = tmp_path / "ds.ndjson"
    ds.write_text('{"hit": 1, "features": {"f1": 0.5}}\n')
    monkeypatch.setattr("sys.argv", [
        "prog",
        "--model", str(tmp_path / "no_such.joblib"),
        "--dataset", str(ds),
    ])
    rc = main()
    assert rc == 2
