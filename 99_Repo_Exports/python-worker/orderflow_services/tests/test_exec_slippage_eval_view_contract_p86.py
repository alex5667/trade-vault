from pathlib import Path


def test_exec_slippage_eval_view_contract_p86():
    sql = Path("services/archivers/sql/20260223_exec_slippage_eval_view.sql").read_text(
        encoding="utf-8", errors="replace"
    )

    # Key columns required by:
    #  - MV refresher (stats)
    #  - enforce_bucket_state_exporter DB stats query
    #  - promoter/rollback/freezer validation
    required_cols = [
        "expected_slip_model_bps",
        "expected_slip_decomp_bps",
        "slip_decomp_coeff_bps",
        "slip_decomp_spread_bps",
        "slip_decomp_impact_bps",
        "realized_slip_worse_bps",
        "slippage_residual_bps",
        "slippage_residual_model_bps",
        "edge_minus_expected_bps",
        "edge_minus_expected_model_bps",
        "exec_regime_bucket",
        "taker_flow_imb_z",
        "liq_regime_label",
        "vol_regime_label",
        "features_json",
    ]

    for col in required_cols:
        assert col in sql, f"missing {col} in view SQL"

    # Make sure residual columns are actually projected in the final SELECT (not only computed in CTE).
    assert "\n  slippage_residual_bps,\n" in sql
    assert "\n  slippage_residual_model_bps,\n" in sql

    # Guard against a common SQL bug: referencing an alias that wasn't defined in the same SELECT list.
    assert "base.*,\n  realized_slip_worse_bps" not in sql
