from orderflow_services.ml_fleet_batch_review_scheduler_v1 import group_batch_items, select_suspicious_snapshots


def test_group_batch_items_respects_max_items():
    items = [{"i": i} for i in range(7)]
    groups = group_batch_items(items, max_items=3)
    assert [len(x) for x in groups] == [3, 3, 1]


def test_select_suspicious_snapshots_filters_ok_by_default(monkeypatch):
    monkeypatch.delenv("ML_BATCH_REVIEW_INCLUDE_OK", raising=False)
    rows = [
        {"status": "ok", "model_id": "a"},
        {"status": "warning", "model_id": "b"},
        {"status": "critical", "model_id": "c"},
    ]
    out = select_suspicious_snapshots(rows)
    assert {x["model_id"] for x in out} == {"b", "c"}
