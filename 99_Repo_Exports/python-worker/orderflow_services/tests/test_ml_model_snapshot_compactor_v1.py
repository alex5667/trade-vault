from orderflow_services.ml_model_snapshot_compactor_v1 import (
    RegistryRow,
    RuntimeRow,
    build_snapshot,
    classify_status,
)


def test_classify_status_critical_on_missing_artifact_and_errors():
    status, reasons = classify_status(
        runtime_age_sec=100.0,
        error_rate_max=0.06,
        missing_critical_rate_max=0.0,
        latency_p95_max_ms=1.0,
        artifact_exists=False,
    )
    assert status == "critical"
    assert "ARTIFACT_MISSING" in reasons
    assert "ERROR_RATE_CRIT" in reasons


def test_build_snapshot_aggregates_runtime_rows_and_reasons():
    reg = RegistryRow(
        model_id="edge_stack_v1:champion",
        family="edge_stack_v1",
        kind="edge_stack_v1",
        artifact_uri="/var/lib/trade/ml_models/edge_stack_v1/champions/edge_stack_v1_champion.joblib",
        schema_ver="v12_of",
        schema_hash="abc",
        promotion_state="champion",
        champion_flag=True,
        owner_service="ml_confirm_gate",
        created_at_ms=1,
        promoted_at_ms=2,
        artifact_exists=True,
        artifact_age_sec=55.0,
        mode="SHADOW",
        fail_policy="OPEN",
        cfg_source="redis_json",
    )
    rows = [
        RuntimeRow(
            ts_ms=1_000,
            symbol="BTCUSDT",
            mode="SHADOW",
            latency_p50_ms=0.5,
            latency_p95_ms=6.0,
            latency_p99_ms=9.0,
            allow_rate=0.4,
            block_rate=0.1,
            abstain_rate=0.05,
            shadow_rate=0.45,
            error_rate=0.02,
            ece=0.03,
            brier=0.11,
            psi_top_json=["obi_avg_20", "spread_bps"],
            ks_top_json=["spread_bps"],
            missing_critical_rate=0.02,
            artifact_age_sec=55.0,
        ),
        RuntimeRow(
            ts_ms=1_100,
            symbol="ETHUSDT",
            mode="SHADOW",
            latency_p50_ms=0.4,
            latency_p95_ms=4.0,
            latency_p99_ms=8.0,
            allow_rate=0.5,
            block_rate=0.1,
            abstain_rate=0.05,
            shadow_rate=0.35,
            error_rate=0.01,
            ece=0.02,
            brier=0.10,
            psi_top_json=["flow_imbalance", "obi_avg_20"],
            ks_top_json=["flow_imbalance"],
            missing_critical_rate=0.01,
            artifact_age_sec=55.0,
        ),
    ]
    snap = build_snapshot(reg, rows, now_ms=301_100)
    assert snap.status == "warning"
    assert snap.symbols_seen_n == 2
    assert snap.latency_p95_max_ms == 6.0
    assert round(float(snap.allow_rate_avg or 0.0), 6) == 0.45
    assert snap.psi_top_json[:3] == ["obi_avg_20", "spread_bps", "flow_imbalance"]
    assert snap.hot_symbols_json[0] == "BTCUSDT"
    assert "LAT_P95_WARN" in snap.reason_codes_json
    assert "ERROR_RATE_WARN" in snap.reason_codes_json
    assert "MISSING_CRITICAL_WARN" in snap.reason_codes_json
