from orderflow_services.ml_training_runs_writer_v1 import _load_source_map, _normalize_row


def test_normalize_row_builds_ids_and_notes():
    row = _normalize_row(
        "edge_stack_v1",
        "metrics:edge_stack_train:last",
        {
            "run_id": "r123",
            "kind": "edge_stack_v1",
            "status": "ok",
            "model_path": "/var/lib/trade/ml_models/edge_stack_v1.joblib",
            "feature_schema_ver": "v12_of",
            "feature_cols_hash": "abc",
            "sample_n": "2500",
            "updated_ts_ms": "1710000000000",
        },
    )
    assert row.training_run_id == "edge_stack_v1:r123"
    assert row.model_id == "edge_stack_v1:r123"
    assert row.artifact_uri.endswith("edge_stack_v1.joblib")
    assert row.notes_json["schema_ver"] == "v12_of"
    assert row.notes_json["schema_hash"] == "abc"
    assert row.notes_json["sample_n"] == 2500


def test_default_source_map_contains_core_families():
    src = _load_source_map()
    assert "edge_stack_v1" in src
    assert "meta_lr" in src
    assert "ml_scorer_v2" in src
