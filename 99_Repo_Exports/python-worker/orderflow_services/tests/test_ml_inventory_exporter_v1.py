from __future__ import annotations

from orderflow_services.ml_inventory_exporter_v1 import (
    Cfg,
    _discover_ml_confirm_records,
    _discover_ml_scorer_records,
    _discover_meta_lr_records,
)


class DummyRedis:
    def __init__(self, strings=None, hashes=None):
        self._strings = strings or {}
        self._hashes = hashes or {}

    def get(self, key):
        return self._strings.get(key)

    def hgetall(self, key):
        return self._hashes.get(key, {})


def test_discover_ml_confirm_records_from_json_keys(tmp_path, monkeypatch):
    model = tmp_path / "edge.joblib"
    model.write_text("x")
    r = DummyRedis(strings={
        "cfg:ml_confirm:champion": '{"kind":"edge_stack_v1","model_path":"%s","model_ver":"run123","feature_schema_ver":"v12_of","feature_cols_hash":"abc"}' % model,
    })
    rows = _discover_ml_confirm_records(r, "svc")
    assert len(rows) == 1
    row = rows[0]
    assert row.family == "ml_confirm"
    assert row.kind == "edge_stack_v1"
    assert row.promotion_state == "champion"
    assert row.artifact_exists == 1
    assert row.schema_ver == "v12_of"
    assert row.schema_hash == "abc"


def test_discover_local_model_records_from_env(tmp_path, monkeypatch):
    scorer = tmp_path / "scorer.joblib"
    scorer.write_text("x")
    meta = tmp_path / "meta.json"
    meta.write_text("{}")
    monkeypatch.setenv("ML_SCORER_V2_MODEL_PATH", str(scorer))
    monkeypatch.setenv("META_MODEL_CHAMPION_PATH", str(meta))
    srows = _discover_ml_scorer_records("svc")
    mrows = _discover_meta_lr_records("svc")
    assert any(r.kind == "ml_scorer_v2" and r.artifact_exists == 1 for r in srows)
    assert any(r.kind == "meta_lr" and r.artifact_exists == 1 for r in mrows)
