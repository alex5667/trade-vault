from __future__ import annotations

"""Generic strategy research evaluator helpers.

The research bundle should not be hard-wired to Sharpe only. This module turns a
JSONL dataset into a compact set of universal metrics that can back promotion
policy decisions and later be exported to Prometheus.
"""

import json
import math
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple


def _loads_payload(obj: Mapping[str, Any]) -> Dict[str, Any]:
    payload = obj.get('payload')
    if isinstance(payload, dict):
        return dict(payload)
    if isinstance(payload, str) and payload.lstrip().startswith('{'):
        try:
            parsed = json.loads(payload)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    return dict(obj)


def load_jsonl(path: str) -> List[Dict[str, Any]]:
    """Load a JSONL file, handling optional payload-envelope wrapping."""
    rows: List[Dict[str, Any]] = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if isinstance(obj, dict):
                rows.append(_loads_payload(obj))
    return rows


def _pick(d: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in d and d.get(k) not in (None, ''):
            return d.get(k)
    return default


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
    except Exception:
        return float(default)
    if not math.isfinite(f):
        return float(default)
    return float(f)


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


def normalize_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize a raw dataset row to a canonical schema.

    Accepts a wide variety of field aliases so the bundle can work with
    datasets from different pipeline stages (OOF train, live replay, backtest).
    """
    score = _to_float(_pick(row, 'score', 'prob', 'prob_cal', 'p', 'confidence', default=0.5), 0.5)
    label = _to_int(_pick(row, 'label', 'y', 'target_hit', 'hit', default=0), 0)
    gross_r = _to_float(_pick(row, 'net_r', 'realized_r', 'outcome_r', 'r', 'ret_r', 'pnl_r', default=0.0), 0.0)
    cost_bps = _to_float(_pick(row, 'cost_bps', 'slippage_bps', 'fee_bps', default=0.0), 0.0)
    # If the dataset already provides net_r we keep it. Otherwise subtract a conservative
    # bps-scaled cost proxy from R-space.
    if 'net_r' in row and row.get('net_r') not in (None, ''):
        net_r = _to_float(row.get('net_r'), 0.0)
    else:
        net_r = gross_r - (cost_bps / 10000.0)
    period = str(_pick(row, 'period', 'bucket', 'day', 'date', 'ts_bucket', default='')).strip()
    variant = str(_pick(row, 'variant', 'arm', 'model_id', 'policy_id', 'config_id', default='baseline')).strip() or 'baseline'
    return {
        'score': score,
        'label': 1 if label > 0 else 0,
        'gross_r': gross_r,
        'net_r': net_r,
        'cost_bps': cost_bps,
        'period': period,
        'variant': variant,
    }


def mean(values: Sequence[float]) -> float:
    xs = [float(v) for v in values]
    if not xs:
        return 0.0
    return sum(xs) / float(len(xs))


def downside_adjusted_return(values: Sequence[float]) -> float:
    """Sortino-like metric: mean(R) / sqrt(mean(downside^2))."""
    xs = [float(v) for v in values]
    if not xs:
        return 0.0
    downside = [min(v, 0.0) for v in xs]
    downside_sq = mean([v * v for v in downside])
    if downside_sq <= 0.0:
        return mean(xs)
    return mean(xs) / math.sqrt(downside_sq)


def entropy_binary_probs(values: Sequence[float]) -> float:
    """Mean binary entropy of model score distribution (calibration diversity signal)."""
    xs = [min(max(float(v), 1e-9), 1.0 - 1e-9) for v in values]
    if not xs:
        return 0.0
    return mean([-(p * math.log(p) + (1.0 - p) * math.log(1.0 - p)) for p in xs])


def evaluate_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    top_frac: float = 0.05,
    primary_metric: str = 'net_expectancy',
) -> Dict[str, Any]:
    """Evaluate a sequence of dataset rows and return a metrics dict.

    Returns universal metrics plus per-period/variant structures needed for PSR/PBO.
    """
    norm = [normalize_row(r) for r in rows]
    if not norm:
        return {
            'rows': 0,
            'primary_metric_name': str(primary_metric),
            'primary_metric_value': 0.0,
            'net_expectancy': 0.0,
            'precision_at_top_x': 0.0,
            'mean_r': 0.0,
            'downside_adjusted_return': 0.0,
            'hit_rate_conditioned_on_cost': 0.0,
            'avg_cost_bps': 0.0,
            'score_entropy': 0.0,
            'period_count': 0,
            'variant_count': 0,
            'per_period_net': [],
            'variant_period_matrix': {},
            'net_series': [],
        }

    ordered = sorted(norm, key=lambda r: (float(r['score']), float(r['net_r'])), reverse=True)
    top_n = max(1, int(round(len(ordered) * max(min(float(top_frac), 1.0), 0.0001))))
    top_rows = ordered[:top_n]
    net_vals = [float(r['net_r']) for r in norm]
    gross_vals = [float(r['gross_r']) for r in norm]
    cost_vals = [float(r['cost_bps']) for r in norm]

    metrics = {
        'rows': len(norm),
        'net_expectancy': mean(net_vals),
        'precision_at_top_x': mean([float(r['label']) for r in top_rows]),
        'mean_r': mean(gross_vals),
        'downside_adjusted_return': downside_adjusted_return(net_vals),
        'hit_rate_conditioned_on_cost': mean([1.0 if float(r['net_r']) > 0.0 else 0.0 for r in norm]),
        'avg_cost_bps': mean(cost_vals),
        'score_entropy': entropy_binary_probs([float(r['score']) for r in norm]),
    }
    metrics['primary_metric_name'] = str(primary_metric)
    metrics['primary_metric_value'] = float(metrics.get(str(primary_metric), metrics['net_expectancy']))

    by_period: Dict[str, List[float]] = defaultdict(list)
    by_variant_period: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
    for r in norm:
        if r['period']:
            by_period[str(r['period'])].append(float(r['net_r']))
            by_variant_period[str(r['variant'])][str(r['period'])].append(float(r['net_r']))

    per_period_net = [mean(by_period[k]) for k in sorted(by_period)]
    variant_period_matrix: Dict[str, List[float]] = {}
    if by_period and by_variant_period:
        ordered_periods = sorted(by_period)
        for variant, per_map in sorted(by_variant_period.items()):
            variant_period_matrix[variant] = [mean(per_map.get(period, [0.0])) for period in ordered_periods]

    metrics['period_count'] = len(by_period)
    metrics['variant_count'] = len(variant_period_matrix)
    metrics['per_period_net'] = per_period_net
    metrics['variant_period_matrix'] = variant_period_matrix
    metrics['net_series'] = net_vals
    return metrics
