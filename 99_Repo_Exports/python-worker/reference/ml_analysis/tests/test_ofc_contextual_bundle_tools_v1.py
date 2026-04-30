from __future__ import annotations

import json
from pathlib import Path

from ml_analysis.tools.build_ofc_contextual_bundle_v1 import build_bundle
from ml_analysis.tools.train_ofc_exec_cost_v1 import train_exec_cost_model
from ml_analysis.tools.train_ofc_rule_success_v1 import train_rule_success_model


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open('w', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def test_ofc_contextual_train_and_bundle_v1(tmp_path: Path):
    exec_rows = tmp_path / 'exec_rows.jsonl'
    rule_rows = tmp_path / 'rule_rows.jsonl'
    exec_model = tmp_path / 'exec_model.json'
    exec_report = tmp_path / 'exec_report.json'
    rule_model = tmp_path / 'rule_model.json'
    rule_report = tmp_path / 'rule_report.json'
    registry = tmp_path / 'registry'
    champion = tmp_path / 'current'

    _write_jsonl(exec_rows, [
        {'ctx_key': 'a', 'realized_slippage_bps': 2.0, 'spread_bps': 1.0, 'expected_slippage_bps': 1.2}
        {'ctx_key': 'a', 'realized_slippage_bps': 2.5, 'spread_bps': 1.0, 'expected_slippage_bps': 1.1}
        {'ctx_key': 'b', 'realized_slippage_bps': 1.0, 'spread_bps': 0.5, 'expected_slippage_bps': 0.7}
    ])
    _write_jsonl(rule_rows, [
        {'ctx_key': 'a', 'raw_score': 0.8, 'label_rule_success': 1}
        {'ctx_key': 'a', 'raw_score': 0.7, 'label_rule_success': 1}
        {'ctx_key': 'b', 'raw_score': 0.3, 'label_rule_success': 0}
        {'ctx_key': 'b', 'raw_score': 0.2, 'label_rule_success': 0}
    ])
    train_exec_cost_model(
        str(exec_rows)
        out_model_json=str(exec_model)
        out_report_json=str(exec_report)
        min_group_rows=1
    )
    train_rule_success_model(
        str(rule_rows)
        out_model_json=str(rule_model)
        out_report_json=str(rule_report)
        min_group_rows=1
    )
    out = build_bundle(
        exec_cost_model_path=str(exec_model)
        rule_success_model_path=str(rule_model)
        registry_dir=str(registry)
        promote_dir=str(champion)
    )
    assert Path(out['bundle_dir']).exists()
    assert (champion / 'manifest.json').exists()
