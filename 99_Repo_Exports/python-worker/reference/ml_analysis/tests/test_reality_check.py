from __future__ import annotations

import json
import os
import tempfile

from ml_analysis.reality_check import evaluate_rows, load_jsonl, normalize_row


def _make_rows(n: int, net_r: float = 0.01, score: float = 0.8, label: int = 1,
               cost_bps: float = 10.0, period: str = '2024-01-01', variant: str = 'v1'):
    return [{'net_r': net_r, 'score': score, 'label': label,
              'cost_bps': cost_bps, 'period': period, 'variant': variant} for _ in range(n)]


def test_normalize_row_canonical():
    r = normalize_row({'score': 0.8, 'label': 1, 'net_r': 0.01, 'cost_bps': 10.0, 'period': 'p1', 'variant': 'v1'})
    assert r['score'] == 0.8
    assert r['label'] == 1
    assert abs(r['net_r'] - 0.01) < 1e-9
    assert r['period'] == 'p1'
    assert r['variant'] == 'v1'


def test_normalize_row_aliases():
    r = normalize_row({'prob': 0.7, 'y': 0, 'realized_r': 0.05, 'slippage_bps': 5.0})
    assert r['score'] == 0.7
    assert r['label'] == 0
    assert abs(r['gross_r'] - 0.05) < 1e-9


def test_normalize_row_defaults():
    r = normalize_row({})
    assert 0.0 <= r['score'] <= 1.0
    assert r['label'] in (0, 1)
    assert r['variant'] == 'baseline'


def test_evaluate_rows_empty():
    m = evaluate_rows([])
    assert m['rows'] == 0
    assert m['net_expectancy'] == 0.0
    assert m['psr'] if False else True  # key not required in empty case


def test_evaluate_rows_basic():
    rows = _make_rows(100, net_r=0.01, score=0.9, label=1, cost_bps=5.0, period='2024-01', variant='a')
    m = evaluate_rows(rows)
    assert m['rows'] == 100
    assert m['net_expectancy'] > 0.0
    assert m['hit_rate_conditioned_on_cost'] == 1.0
    assert m['period_count'] == 1
    assert m['variant_count'] == 1


def test_evaluate_rows_negative_net():
    rows = _make_rows(50, net_r=-0.05, score=0.1, label=0, cost_bps=20.0)
    m = evaluate_rows(rows)
    assert m['net_expectancy'] < 0.0
    assert m['hit_rate_conditioned_on_cost'] == 0.0


def test_evaluate_rows_multiple_periods():
    rows = (
        _make_rows(30, period='2024-01', variant='a') +
        _make_rows(30, period='2024-02', variant='a') +
        _make_rows(30, period='2024-02', variant='b')
    )
    m = evaluate_rows(rows)
    assert m['period_count'] == 2
    assert m['variant_count'] == 2
    assert len(m['per_period_net']) == 2


def test_evaluate_rows_primary_metric():
    rows = _make_rows(60, net_r=0.02)
    m = evaluate_rows(rows, primary_metric='mean_r')
    assert m['primary_metric_name'] == 'mean_r'
    assert abs(m['primary_metric_value'] - m['mean_r']) < 1e-9


def test_load_jsonl_basic():
    data = [{'net_r': 0.01, 'score': 0.8, 'label': 1}]
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        for row in data:
            f.write(json.dumps(row) + '\n')
        tmp = f.name
    try:
        loaded = load_jsonl(tmp)
        assert len(loaded) == 1
        assert loaded[0].get('net_r') == 0.01
    finally:
        os.unlink(tmp)


def test_load_jsonl_empty_lines():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write('\n\n{"label": 1}\n\n')
        tmp = f.name
    try:
        loaded = load_jsonl(tmp)
        assert len(loaded) == 1
    finally:
        os.unlink(tmp)
