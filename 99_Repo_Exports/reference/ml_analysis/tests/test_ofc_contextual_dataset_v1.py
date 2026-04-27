from __future__ import annotations

import json
from pathlib import Path

from ml_analysis.tools.build_ofc_contextual_dataset_v1 import build_dataset


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open('w', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def test_build_ofc_contextual_dataset_v1(tmp_path: Path):
    decisions = tmp_path / 'decisions.jsonl'
    outcomes = tmp_path / 'outcomes.jsonl'
    out_exec = tmp_path / 'exec.jsonl'
    out_rule = tmp_path / 'rule.jsonl'
    report = tmp_path / 'report.json'
    _write_jsonl(decisions, [
        {'sid': 's1', 'symbol': 'BTCUSDT', 'direction': 'BUY', 'ctx_key': 'symbol=BTCUSDT|session=us', 'spread_bps': 1.0, 'expected_slippage_bps': 2.0, 'of_score_final': 0.7},
        {'sid': 's2', 'symbol': 'ETHUSDT', 'direction': 'SELL', 'ctx_key': 'symbol=ETHUSDT|session=eu', 'spread_bps': 0.8, 'expected_slippage_bps': 1.2, 'of_score_final': 0.4},
    ])
    _write_jsonl(outcomes, [
        {'sid': 's1', 'realized_slippage_bps': 2.4, 'pnl_bps_net': 5.0},
        {'sid': 's2', 'realized_slippage_bps': 1.5, 'pnl_bps_net': -1.0},
    ])
    rep = build_dataset(
        decisions_jsonl=str(decisions),
        outcomes_jsonl=str(outcomes),
        out_exec_cost_jsonl=str(out_exec),
        out_rule_success_jsonl=str(out_rule),
        out_report_json=str(report),
        success_bps=0.0,
    )
    assert rep['joined'] == 2
    assert out_exec.exists()
    assert out_rule.exists()
