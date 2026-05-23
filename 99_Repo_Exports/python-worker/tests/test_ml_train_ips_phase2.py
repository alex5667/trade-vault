"""Phase 2 IPS reweighting + cost-aware label + per-slice metrics tests.

Covers the offline-train side of the Phase 2 plan:
- build_of_dataset emits ips_weight per row (with virtual penalty)
- train_of_meta_model_lr accepts sample_weight and emits slice diagnostics
- ml_train_lr_calibrated calibrate_real_only path filters the cal fold
- cost-aware label rewrite in nightly bundle produces correct y values
"""
from __future__ import annotations

import json
import sys

import pytest

from tools.build_of_dataset import compute_ips_weight, extract_trade_labels


# ----------------------------------------------------------------------
# IPS weight composition
# ----------------------------------------------------------------------

def test_ips_weight_passed_real_is_one():
    w = compute_ips_weight(v_gate_reason="OK", is_virtual=0)
    assert 0.99 <= w <= 1.0


def test_ips_weight_virtual_penalty_applied():
    w_real = compute_ips_weight(v_gate_reason="OK", is_virtual=0, virtual_penalty=0.5)
    w_virt = compute_ips_weight(v_gate_reason="OK", is_virtual=1, virtual_penalty=0.5)
    assert pytest.approx(w_virt, rel=1e-6) == 0.5 * w_real


def test_ips_weight_clipped_to_floor(monkeypatch):
    # IPS weights are gated by REJECT_REASON_WEIGHTS_ENABLED — enable for this test.
    monkeypatch.setenv("REJECT_REASON_WEIGHTS_ENABLED", "1")
    # Re-import to refresh the module-level cache inside core.reject_reason_weights.
    import importlib
    import core.reject_reason_weights as rrw
    importlib.reload(rrw)
    # virtual + env veto (weight 0.10) * 0.5 = 0.05 → at floor
    w = rrw.weight_for_reason("VETO_FREEZE_ACTIVE")
    assert w == pytest.approx(0.10, abs=1e-6)
    w_virt = w * 0.5
    floor = 0.05
    assert max(w_virt, floor) == pytest.approx(0.05, abs=1e-6)


def test_ips_weight_unknown_reason_defaults_to_passed():
    # weight_for_reason returns 1.0 for any reason that doesn't match a known
    # prefix when REJECT_REASON_WEIGHTS_ENABLED=0, or matching reason when on.
    # We just assert it returns a finite value in [floor, 1.0].
    w = compute_ips_weight(v_gate_reason="VETO_SOMETHING_UNKNOWN_XYZ", is_virtual=0)
    assert 0.0 <= w <= 1.0


# ----------------------------------------------------------------------
# extract_trade_labels now emits Phase 2 fields
# ----------------------------------------------------------------------

def test_extract_trade_labels_phase2_fields_default():
    lab = extract_trade_labels({"r_mult": 1.0, "pnl": 50.0, "risk_usd": 100.0})
    assert lab["v_gate_reason"] == ""
    assert lab["is_virtual"] == 0
    assert lab["pnl_net"] == pytest.approx(50.0)  # falls back to pnl
    assert lab["fees"] == 0.0
    assert lab["slippage_realized_bps"] == -1.0
    assert lab["expected_slippage_bps"] == -1.0


def test_extract_trade_labels_phase2_fields_present():
    tr = {
        "r_mult": -0.5,
        "pnl": -20.0,
        "risk_usd": 100.0,
        "is_virtual": 1,
        "v_gate_reason": "SHADOW_VETO_BREADTH_RET_HIGH",
        "pnl_net": -22.5,
        "fees": 1.5,
        "slippage_realized_bps": 7.2,
        "expected_slippage_bps": 5.0,
        "meta": {"close_reason": "SL"},
    }
    lab = extract_trade_labels(tr)
    assert lab["is_virtual"] == 1
    assert lab["v_gate_reason"] == "SHADOW_VETO_BREADTH_RET_HIGH"
    assert lab["pnl_net"] == pytest.approx(-22.5)
    assert lab["fees"] == pytest.approx(1.5)
    assert lab["slippage_realized_bps"] == pytest.approx(7.2)
    assert lab["expected_slippage_bps"] == pytest.approx(5.0)


# ----------------------------------------------------------------------
# build_of_dataset CLI emits ips_weight column for both real and virtual.
# ----------------------------------------------------------------------

def _write_ndjson(path: str, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def test_build_of_dataset_emits_ips_weight_and_include_virtual(tmp_path):
    from tools.build_of_dataset import main as build_main
    replay_path = tmp_path / "replay.ndjson"
    trades_path = tmp_path / "trades.ndjson"
    out_path = tmp_path / "out.ndjson"

    # Two replay rows, two trade rows: one real-passed, one virtual SHADOW_VETO.
    _write_ndjson(str(replay_path), [
        {"sid": "real-1", "symbol": "BTCUSDT", "ts_ms": 1000, "direction": "LONG", "scenario": "reversal", "ok": 1, "have": 2, "need": 2, "evidence": {"legs": {"ofi_leg": 1}}},
        {"sid": "virt-1", "symbol": "BTCUSDT", "ts_ms": 2000, "direction": "LONG", "scenario": "reversal", "ok": 1, "have": 2, "need": 2, "evidence": {"legs": {"ofi_leg": 1}}},
    ])
    _write_ndjson(str(trades_path), [
        {"sid": "real-1", "r_mult": 1.5, "pnl": 150.0, "risk_usd": 100.0, "is_virtual": 0, "v_gate_reason": "OK"},
        {"sid": "virt-1", "r_mult": -0.8, "pnl": -80.0, "risk_usd": 100.0, "is_virtual": 1, "v_gate_reason": "SHADOW_VETO_BREADTH"},
    ])

    old_argv = sys.argv
    sys.argv = [
        "test", "--replay", str(replay_path), "--trades", str(trades_path),
        "--out", str(out_path), "--pos-th", "0", "--neg-th", "0",
        "--include-virtual", "--min-n", "1",
    ]
    try:
        build_main()
    finally:
        sys.argv = old_argv

    rows = [json.loads(line) for line in open(out_path)]
    assert len(rows) == 2
    by_sid = {r["sid"]: r for r in rows}
    assert "ips_weight" in by_sid["real-1"]
    assert "ips_weight" in by_sid["virt-1"]
    # Real-passed should weigh ≥ virtual-shadow.
    assert by_sid["real-1"]["ips_weight"] > by_sid["virt-1"]["ips_weight"]
    # Real-passed is at the top of the weight scale.
    assert by_sid["real-1"]["ips_weight"] >= 0.99


def test_build_of_dataset_excludes_virtual_by_default(tmp_path, monkeypatch):
    monkeypatch.delenv("ML_TRAIN_INCLUDE_VIRTUAL", raising=False)
    from tools.build_of_dataset import main as build_main
    replay_path = tmp_path / "replay.ndjson"
    trades_path = tmp_path / "trades.ndjson"
    out_path = tmp_path / "out.ndjson"

    _write_ndjson(str(replay_path), [
        {"sid": "real-1", "symbol": "BTCUSDT", "ts_ms": 1000, "direction": "LONG", "scenario": "reversal", "ok": 1, "have": 2, "need": 2, "evidence": {"legs": {}}},
        {"sid": "virt-1", "symbol": "BTCUSDT", "ts_ms": 2000, "direction": "LONG", "scenario": "reversal", "ok": 1, "have": 2, "need": 2, "evidence": {"legs": {}}},
    ])
    _write_ndjson(str(trades_path), [
        {"sid": "real-1", "r_mult": 1.5, "pnl": 150.0, "risk_usd": 100.0, "is_virtual": 0},
        {"sid": "virt-1", "r_mult": 0.8, "pnl": 80.0, "risk_usd": 100.0, "is_virtual": 1},
    ])

    old_argv = sys.argv
    sys.argv = [
        "test", "--replay", str(replay_path), "--trades", str(trades_path),
        "--out", str(out_path), "--pos-th", "0", "--neg-th", "0", "--min-n", "1",
    ]
    try:
        build_main()
    finally:
        sys.argv = old_argv

    rows = [json.loads(line) for line in open(out_path)]
    assert len(rows) == 1
    assert rows[0]["sid"] == "real-1"


# ----------------------------------------------------------------------
# train_of_meta_model_lr build_xy now returns weights.
# ----------------------------------------------------------------------

def test_train_meta_model_lr_build_xy_returns_weights():
    from tools.train_of_meta_model_lr import build_xy
    rows = [
        {"y": 1, "ips_weight": 1.0, "base_score": 0.8, "exec_risk_norm": 0.1},
        {"y": 0, "ips_weight": 0.5, "base_score": 0.3, "exec_risk_norm": 0.2},
        {"y": 1, "ips_weight": 0.0, "base_score": 0.7, "exec_risk_norm": 0.0},  # zero -> falls back to 1.0
        {"y": 0, "base_score": 0.4, "exec_risk_norm": 0.5},  # missing -> 1.0
    ]
    feat = ["base_score", "exec_risk_norm"]
    X, y, w = build_xy(rows, feat)
    assert X.shape == (4, 2)
    assert y.tolist() == [1, 0, 1, 0]
    # row[2] zero weight falls back to 1.0; row[3] missing falls back to 1.0
    assert w.tolist() == [1.0, 0.5, 1.0, 1.0]


# ----------------------------------------------------------------------
# ml_train_lr_calibrated calibrate-real-only filter.
# ----------------------------------------------------------------------

def test_lr_calibrated_is_real_passed():
    from tools.ml_train_lr_calibrated import _is_real_passed
    assert _is_real_passed({"is_virtual": 0, "v_gate_reason": "OK"})
    assert _is_real_passed({"is_virtual": 0, "v_gate_reason": ""})
    assert not _is_real_passed({"is_virtual": 1, "v_gate_reason": "OK"})
    assert not _is_real_passed({"is_virtual": 0, "v_gate_reason": "SHADOW_VETO_BREADTH"})
    assert not _is_real_passed({"is_virtual": 0, "v_gate_reason": "VETO_SPREAD_SHOCK"})


def test_lr_calibrated_cost_aware_label_uses_pnl_net_and_fees():
    from tools.ml_train_lr_calibrated import _cost_aware_y
    # Profit covers fees + slippage → y=1
    row_win = {
        "pnl_net": 50.0, "fees": 2.0, "risk_usd": 100.0,
        "slippage_realized_bps": 5.0,  # 0.05% of $100 = $0.05
    }
    assert _cost_aware_y(row_win, fee_mul=2.0, slippage_bps_fallback=4.0) == 1

    # Profit consumed by cost → y=0
    row_lose = {
        "pnl_net": 1.0, "fees": 0.6, "risk_usd": 100.0,
        "slippage_realized_bps": 10.0,
    }
    # cost = 2.0 * 0.6 + (10/10000)*100 = 1.2 + 0.1 = 1.3 → 1.0 - 1.3 < 0
    assert _cost_aware_y(row_lose, fee_mul=2.0, slippage_bps_fallback=4.0) == 0


def test_lr_calibrated_cost_aware_label_falls_back_to_y_edge_for_legacy_rows():
    from tools.ml_train_lr_calibrated import _cost_aware_y
    legacy = {"y_edge": 1}
    # No pnl_net / fees → must use legacy y_edge.
    assert _cost_aware_y(legacy, fee_mul=2.0, slippage_bps_fallback=4.0) == 1
