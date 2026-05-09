import sys
from pathlib import Path

SCHEMA_HASH = "e745730b0253"


# Ensure repo root is on sys.path for namespace-package import (tick_flow_full has no __init__.py).
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _build_vec(schema_ver: int, indicators: dict, confirmations=None):
    # Local import to allow env override per test.
    from tick_flow_full.core import ml_feature_schema as m

    if confirmations is not None:
        # Simulate nightly payload merge behavior
        payload = {
            "symbol": "BTCUSDT",
            "ts_ms": 1700000000000,
            "direction": "LONG",
            "scenario_v4": "reversal",
            "indicators": indicators,
            "confirmations": confirmations,
        },
        row = m.build_features(payload)
        return row.x, row.feature_names

    vec, _miss = m.build_feature_vector(
        symbol="BTCUSDT",
        ts_ms=1700000000000,
        direction="LONG",
        scenario="reversal",
        indicators=indicators,
        rule_score=0.0,
        rule_have=0,
        rule_need=0,
        cancel_spike_veto=0,
        schema_ver=schema_ver,
    )
    return vec, m.feature_names(schema_ver)


def test_schema_v1_does_not_include_confirmations():
    from tick_flow_full.core import ml_feature_schema as m

    vec, names = _build_vec(1, indicators={"delta_z": 1.0, "ofi_z": 0.0, "ofi": 0.0, "ofi_stability_score": 1.0,
                                          "exec_risk_norm": 0.0, "spread_bps": 10.0, "expected_slippage_bps": 5.0,
                                          "liq_score": 1.0, "hawkes_taker_lam": 0.0, "hawkes_cancel_lam": 0.0,
                                          "hawkes_churn_lam": 0.0,
                                          "sweep_recent": 0, "reclaim_recent": 0, "obi_stable": 0, "iceberg_strict": 0,
                                          "abs_lvl_ok": 0, "weak_progress": 0, "fp_edge_absorb": 0, "ofi_stable": 0, "ofi_dir_ok": 0})
    assert "rsi_agree" not in names
    assert "div_match" not in names
    assert len(vec) == len(m.feature_names(1))


def test_schema_v2_appends_confirmations_and_maps_indicators():

    vec, names = _build_vec(2, indicators={
        "delta_z": 1.0,
        "ofi_z": 0.0,
        "ofi": 0.0,
        "ofi_stability_score": 1.0,
        "exec_risk_norm": 0.0,
        "spread_bps": 10.0,
        "expected_slippage_bps": 5.0,
        "liq_score": 1.0,
        "hawkes_taker_lam": 0.0,
        "hawkes_cancel_lam": 0.0,
        "hawkes_churn_lam": 0.0,
        "sweep_recent": 1,
        "reclaim_recent": 0,
        "obi_stable": 0,
        "iceberg_strict": 0,
        "abs_lvl_ok": 0,
        "weak_progress": 0,
        "fp_edge_absorb": 0,
        "ofi_stable": 0,
        "ofi_dir_ok": 0,
        "rsi_agree": 1,
        "div_match": 0,
        "sweep_eqh": 1,
        "sweep_eql": 0,
    })
    assert names[-5:] == ["rsi_agree", "div_match", "sweep_any", "sweep_eqh", "sweep_eql"]
    # sweep_any derives to 1 when eqh/eql present (or sweep_recent)
    idx = {n: i for i, n in enumerate(names)}
    assert vec[idx["rsi_agree"]] == 1.0
    assert vec[idx["div_match"]] == 0.0
    assert vec[idx["sweep_any"]] == 1.0
    assert vec[idx["sweep_eqh"]] == 1.0
    assert vec[idx["sweep_eql"]] == 0.0


def test_build_features_parses_legacy_confirmations_list_under_env_v2(monkeypatch):
    # build_features() uses env-based schema version.
    monkeypatch.setenv("ML_FEATURE_SCHEMA_VERSION", "2")
    vec, names = _build_vec(
        2,
        indicators={
            "delta_z": 0.0,
            "ofi_z": 0.0,
            "ofi": 0.0,
            "ofi_stability_score": 0.0,
            "exec_risk_norm": 0.0,
            "spread_bps": 0.0,
            "expected_slippage_bps": 0.0,
            "liq_score": 0.0,
            "hawkes_taker_lam": 0.0,
            "hawkes_cancel_lam": 0.0,
            "hawkes_churn_lam": 0.0,
            "sweep_recent": 0,
            "reclaim_recent": 0,
            "obi_stable": 0,
            "iceberg_strict": 0,
            "abs_lvl_ok": 0,
            "weak_progress": 0,
            "fp_edge_absorb": 0,
            "ofi_stable": 0,
            "ofi_dir_ok": 0,
        },
        confirmations=["rsi_agree=1", "div_match=1", "sweep_eqh=1"],
    )
    idx = {n: i for i, n in enumerate(names)}
    assert vec[idx["rsi_agree"]] == 1.0
    assert vec[idx["div_match"]] == 1.0
    assert vec[idx["sweep_eqh"]] == 1.0
    # sweep_any derives from eqh/eql
    assert vec[idx["sweep_any"]] == 1.0
