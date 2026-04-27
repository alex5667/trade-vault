from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from ml_analysis.psr_dsr import probabilistic_sharpe_ratio, deflated_sharpe_ratio
from ml_analysis.pbo_cscv import compute_pbo
from ml_analysis.reality_check import evaluate_rows, load_jsonl


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
    except Exception:
        return float(default)
    if not math.isfinite(f):
        return float(default)
    return float(f)


def _redis():
    """Create Redis client from REDIS_URL env var. Returns None on any failure."""
    if redis is None:
        return None
    try:
        return redis.Redis.from_url(_env('REDIS_URL', 'redis://redis-worker-1:6379/0'), decode_responses=True)
    except Exception:
        return None


def _select_dataset_path(explicit: str) -> str:
    """Pick the first existing dataset path from a priority list of candidates."""
    candidates = [
        explicit,
        _env('STRATEGY_RESEARCH_STATS_DATASET_PATH', ''),
        _env('ML_EDGE_STACK_DATASET_PATH', ''),
        _env('ML_EDGE_STACK_OOF_DATASET_PATH', ''),
        '/var/lib/trade/ml_models/edge_stack_v1/dataset.ndjson',
        '/var/lib/trade/ml_models/edge_stack_v1_oof/edge_train.jsonl',
    ]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    return explicit or _env('STRATEGY_RESEARCH_STATS_DATASET_PATH', '')


def _reason_from_metrics(
    metrics: Dict[str, Any],
    *,
    min_psr: float,
    min_dsr: float,
    max_pbo: float,
    min_primary: float,
    fail_closed_missing: bool,
) -> str:
    """Derive a comma-joined reason string from metric violations."""
    problems = []
    psr = metrics.get('psr')
    dsr = metrics.get('dsr')
    pbo = metrics.get('pbo')
    primary = metrics.get('primary_metric_value')
    if psr is None and fail_closed_missing:
        problems.append('psr_missing')
    elif psr is not None and float(psr) < float(min_psr):
        problems.append('psr_low')
    if dsr is None and fail_closed_missing:
        problems.append('dsr_missing')
    elif dsr is not None and float(dsr) < float(min_dsr):
        problems.append('dsr_low')
    if pbo is None and fail_closed_missing:
        problems.append('pbo_missing')
    elif pbo is not None and float(pbo) > float(max_pbo):
        problems.append('pbo_high')
    if primary is None and fail_closed_missing:
        problems.append('primary_metric_missing')
    elif primary is not None and float(primary) < float(min_primary):
        problems.append('metric_low')
    return ','.join(problems) if problems else 'ok'


def _hset_mapping(client: Any, key: str, mapping: Dict[str, Any]) -> None:
    """Serialize and write a flat mapping to a Redis hash."""
    if client is None or not key:
        return
    payload = {
        k: json.dumps(v, separators=(',', ':')) if isinstance(v, (dict, list)) else str(v)
        for k, v in mapping.items()
    }
    client.hset(key, mapping=payload)


def main() -> int:
    ap = argparse.ArgumentParser(description='Nightly strategy research stats bundle (P6.1)')
    ap.add_argument('--dataset-path', default='')
    ap.add_argument('--out-dir', default=_env('STRATEGY_RESEARCH_STATS_OUT_DIR', '/var/lib/trade/of_reports/out/strategy_research'))
    ap.add_argument('--summary-key', default=_env('STRATEGY_RESEARCH_STATS_SUMMARY_KEY', 'metrics:strategy_research_stats:last'))
    ap.add_argument('--blocker-key', default=_env('STRATEGY_RESEARCH_STATS_BLOCKER_KEY', 'cfg:strategy_research_stats:blocker:v1'))
    ap.add_argument('--gate-mode', default=_env('STRATEGY_RESEARCH_STATS_GATE_MODE', 'report_only'))
    ap.add_argument('--primary-metric', default=_env('STRATEGY_RESEARCH_STATS_PRIMARY_METRIC', 'net_expectancy'))
    ap.add_argument('--min-psr', type=float, default=float(_env('STRATEGY_RESEARCH_STATS_MIN_PSR', '0.55')))
    ap.add_argument('--min-dsr', type=float, default=float(_env('STRATEGY_RESEARCH_STATS_MIN_DSR', '0.50')))
    ap.add_argument('--max-pbo', type=float, default=float(_env('STRATEGY_RESEARCH_STATS_MAX_PBO', '0.20')))
    ap.add_argument('--min-primary', type=float, default=float(_env('STRATEGY_RESEARCH_STATS_MIN_PRIMARY', '0.0')))
    ap.add_argument('--top-frac', type=float, default=float(_env('STRATEGY_RESEARCH_STATS_TOP_FRAC', '0.05')))
    ap.add_argument('--cscv-folds', type=int, default=int(_env('STRATEGY_RESEARCH_STATS_CSCV_FOLDS', '8')))
    ap.add_argument('--fail-closed-missing', type=int, default=int(_env('STRATEGY_RESEARCH_STATS_FAIL_CLOSED_MISSING', '0')))
    args = ap.parse_args()

    dataset_path = _select_dataset_path(args.dataset_path)
    if not dataset_path or not os.path.exists(dataset_path):
        print(json.dumps({'ok': False, 'reason': 'dataset_missing', 'dataset_path': dataset_path}, ensure_ascii=False))
        return 2

    rows = load_jsonl(dataset_path)
    metrics = evaluate_rows(rows, top_frac=args.top_frac, primary_metric=args.primary_metric)
    per_period = metrics.get('per_period_net') or []
    net_series = metrics.get('net_series') or []
    variant_matrix = metrics.get('variant_period_matrix') or {}
    # Use per-period net series for PSR/DSR when available (removes within-period noise)
    sr_series = per_period if per_period else net_series
    psr = probabilistic_sharpe_ratio(sr_series) if sr_series else 0.0
    dsr = deflated_sharpe_ratio(sr_series, n_trials=max(int(metrics.get('variant_count') or 1), 1)) if sr_series else 0.0

    # PBO requires at least 2 periods and 2 variants for meaningful CSCV splits
    pbo = None
    cscv_splits = 0.0
    chosen_variant_unique = 0.0
    if variant_matrix and int(metrics.get('period_count', 0)) >= 2 and int(metrics.get('variant_count', 0)) >= 2:
        pbo_info = compute_pbo(variant_matrix, n_folds=max(2, int(args.cscv_folds)))
        pbo = float(pbo_info.get('pbo', 0.0))
        cscv_splits = float(pbo_info.get('cscv_splits', 0.0))
        chosen_variant_unique = float(pbo_info.get('chosen_variant_unique', 0.0))
    metrics['psr'] = float(psr)
    metrics['dsr'] = float(dsr)
    metrics['pbo'] = None if pbo is None else float(pbo)
    metrics['cscv_splits'] = float(cscv_splits)
    metrics['chosen_variant_unique'] = float(chosen_variant_unique)
    metrics['dataset_path'] = dataset_path

    gate_mode = str(args.gate_mode or 'report_only').strip().lower()
    if gate_mode not in ('report_only', 'soft', 'hard'):
        gate_mode = 'report_only'
    reason = _reason_from_metrics(
        metrics,
        min_psr=args.min_psr,
        min_dsr=args.min_dsr,
        max_pbo=args.max_pbo,
        min_primary=args.min_primary,
        fail_closed_missing=bool(int(args.fail_closed_missing)),
    )
    has_violation = reason != 'ok'
    blocked = 1 if gate_mode == 'hard' and has_violation else 0
    soft_blocked = 1 if gate_mode == 'soft' and has_violation else 0
    report_only = 1 if gate_mode == 'report_only' else 0
    now_ms = int(time.time() * 1000)

    summary = {
        'updated_ts_ms': now_ms,
        'success': 1,
        'gate_mode': gate_mode,
        'report_only': report_only,
        'primary_metric_name': metrics.get('primary_metric_name', args.primary_metric),
        'primary_metric_value': _to_float(metrics.get('primary_metric_value', 0.0), 0.0),
        'net_expectancy': _to_float(metrics.get('net_expectancy', 0.0), 0.0),
        'precision_at_top_x': _to_float(metrics.get('precision_at_top_x', 0.0), 0.0),
        'mean_r': _to_float(metrics.get('mean_r', 0.0), 0.0),
        'downside_adjusted_return': _to_float(metrics.get('downside_adjusted_return', 0.0), 0.0),
        'hit_rate_conditioned_on_cost': _to_float(metrics.get('hit_rate_conditioned_on_cost', 0.0), 0.0),
        'avg_cost_bps': _to_float(metrics.get('avg_cost_bps', 0.0), 0.0),
        'score_entropy': _to_float(metrics.get('score_entropy', 0.0), 0.0),
        'rows': int(metrics.get('rows', 0)),
        'period_count': int(metrics.get('period_count', 0)),
        'variant_count': int(metrics.get('variant_count', 0)),
        'psr': _to_float(metrics.get('psr', 0.0), 0.0),
        'dsr': _to_float(metrics.get('dsr', 0.0), 0.0),
        'pbo': '' if metrics.get('pbo') is None else _to_float(metrics.get('pbo', 0.0), 0.0),
        'cscv_splits': _to_float(metrics.get('cscv_splits', 0.0), 0.0),
        'chosen_variant_unique': _to_float(metrics.get('chosen_variant_unique', 0.0), 0.0),
        'dataset_path': dataset_path,
        'blocker_reason': reason,
    }
    blocker = {
        'updated_ts_ms': now_ms,
        'gate_mode': gate_mode,
        'report_only': report_only,
        'blocked': blocked,
        'soft_blocked': soft_blocked,
        'invalid': 0,
        'reason': reason,
    }

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / 'strategy_research_stats_last.json'
    tmp = report_path.with_suffix('.tmp')
    report_obj = {'summary': summary, 'blocker': blocker}
    tmp.write_text(json.dumps(report_obj, ensure_ascii=False, indent=2, sort_keys=True), encoding='utf-8')
    tmp.replace(report_path)

    client = _redis()
    _hset_mapping(client, args.summary_key, summary)
    _hset_mapping(client, args.blocker_key, blocker)

    print(json.dumps({
        'ok': True,
        'report_path': str(report_path),
        'blocked': blocked,
        'soft_blocked': soft_blocked,
        'reason': reason,
    }, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
