import json
from pathlib import Path

from orderflow_services.ml_health_enricher_v1 import _drift_top_from_report, _read_json


def test_drift_top_from_report_orders_by_psi_and_ks(tmp_path: Path):
    path = tmp_path / "report.json"
    path.write_text(
        json.dumps(
            {
                "features": [
                    {"feature": "a", "psi": 0.1, "ks_stat": 0.2},
                    {"feature": "b", "psi": 0.7, "ks_stat": 0.1},
                    {"feature": "c", "psi": 0.4, "ks_stat": 0.9},
                ]
            }
        ),
        encoding="utf-8",
    )
    psi_top, ks_top = _drift_top_from_report(str(path))
    assert psi_top[:2] == ["b", "c"]
    assert ks_top[:2] == ["c", "a"]


def test_read_json_fail_open_on_missing_file(tmp_path: Path):
    obj = _read_json(str(tmp_path / "missing.json"))
    assert obj == {}
