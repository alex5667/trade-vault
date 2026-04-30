from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module(rel_path: str, name: str):
    root = Path(__file__).resolve().parents[1]
    p = root / 'tools' / rel_path
    spec = importlib.util.spec_from_file_location(name, p)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def test_calibration_report_writes_extended_metrics(tmp_path: Path):
    mod = _load_module('calibration_report.py', 'calibration_report')
    rows = [
        {"y": 1, "r_mult": 1.2, "indicators": {"confidence_v1": 0.8, "confidence_cal_v1": 0.75}}
        {"y": 0, "r_mult": -0.8, "indicators": {"confidence_v1": 0.2, "confidence_cal_v1": 0.25}}
    ] * 300
    in_jsonl = tmp_path / 'joined.jsonl'
    with open(in_jsonl, 'w', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row) + '\n')
    out_json = tmp_path / 'report.json'
    import sys
    old = sys.argv
    sys.argv = ['calibration_report.py', '--in_jsonl', str(in_jsonl), '--out_json', str(out_json), '--min_rows', '10']
    try:
        mod.main()
    finally:
        sys.argv = old
    obj = json.loads(out_json.read_text(encoding='utf-8'))
    assert 'mce' in obj['raw_v1']
    assert 'sharpness_mean' in obj['cal_v1']


def test_promotion_manager_compute_metrics_includes_extended_fields():
    mod = _load_module('conf_cal_promotion_manager_v1.py', 'conf_cal_promotion_manager_v1')
    y = [1, 0, 1, 0, 1, 0, 1, 0]
    p = [0.8, 0.2, 0.7, 0.3, 0.9, 0.1, 0.6, 0.4]
    rep = mod.compute_metrics(y, p)
    assert 'mce' in rep
    assert 'calibration_slope' in rep
    assert 'prob_mass_near_half' in rep
