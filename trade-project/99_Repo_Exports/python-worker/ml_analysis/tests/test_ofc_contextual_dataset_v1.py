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


def test_decision_prefixed_fields_take_priority_over_flat():
    """Regression: v7 NDJSON capture carries flat top-level `spread_bps`=0.0
    while the canonical frozen-decision snapshot lives in `decision_spread_bps`.
    The builder must prefer the `decision_*` snapshot — otherwise every exec-cost
    row gets silently zero'd out."""
    from ml_analysis.tools.build_ofc_contextual_dataset_v1 import _build_exec_cost_row

    # Mirror the actual v7 capture layout: flat decision_* alongside (stale) top-level
    decision = {
        'sid': 'crypto-of:BTCUSDT:1700000000000',
        'symbol': 'BTCUSDT', 'direction': 'LONG',
        'decision_ts_ms': 1700000000000,
        # Stale top-level values (often 0.0 in v7 capture):
        'spread_bps': 0.0,
        'expected_slippage_bps': 0.0,
        'book_staleness_ms': 0,
        # Frozen decision snapshot (the canonical source):
        'decision_spread_bps': 0.26,
        'decision_expected_slippage_bps': 0.60,
        'decision_book_staleness_ms': 120,
    }
    outcome = {'sid': decision['sid'], 'symbol': 'BTCUSDT'}
    row = _build_exec_cost_row(decision, outcome)
    assert row['spread_bps'] == 0.26, row
    assert row['expected_slippage_bps'] == 0.60, row
    assert row['book_staleness_ms'] == 120, row
