#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import math
import os
import statistics
import time
from typing import Any, Dict, Iterable, Iterator, List, Optional


def _now_ms() -> int:
    return get_ny_time_millis()


def _iter_jsonl(path: str) -> Iterator[Dict[str, Any]]:
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
                yield obj


def _write_json_atomic(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or '.', exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _quantile(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    ys = sorted(float(x) for x in xs)
    if len(ys) == 1:
        return float(ys[0])
    pos = max(0.0, min(1.0, float(q))) * (len(ys) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(ys[lo])
    w = pos - lo
    return float(ys[lo] * (1.0 - w) + ys[hi] * w)


def train_exec_cost_model(rows_jsonl: str, *, out_model_json: str, out_report_json: str, min_group_rows: int = 30) -> Dict[str, Any]:
    groups: Dict[str, List[float]] = {}
    all_costs: List[float] = []
    for row in _iter_jsonl(rows_jsonl):
        ctx_key = str(row.get('ctx_key') or 'global')
        realized = _to_float(row.get('realized_slippage_bps'), 0.0)
        spread = _to_float(row.get('spread_bps'), 0.0)
        expected = _to_float(row.get('expected_slippage_bps'), 0.0)
        target = max(realized, spread, expected, 0.0)
        groups.setdefault(ctx_key, []).append(target)
        all_costs.append(target)

    global_p50 = _quantile(all_costs, 0.50)
    global_p90 = _quantile(all_costs, 0.90)
    model_groups: Dict[str, Dict[str, Any]] = {}
    for key, vals in groups.items():
        if len(vals) < int(min_group_rows):
            continue
        model_groups[key] = {
            'n': int(len(vals)),
            'cost_p50_bps': float(_quantile(vals, 0.50)),
            'cost_p90_bps': float(_quantile(vals, 0.90)),
            'exec_risk_ref_bps_ctx': float(max(_quantile(vals, 0.75), global_p50, 1e-9)),
        }

    model = {
        'kind': 'ofc_exec_cost_v1',
        'version': time.strftime('%Y%m%d_%H%M%S', time.gmtime()),
        'created_ts_ms': _now_ms(),
        'min_group_rows': int(min_group_rows),
        'defaults': {
            'cost_p50_bps': float(global_p50),
            'cost_p90_bps': float(global_p90),
            'exec_risk_ref_bps_ctx': float(max(global_p50, 1e-9)),
        },
        'groups': model_groups,
    }
    report = {
        'rows': int(len(all_costs)),
        'groups_total': int(len(groups)),
        'groups_kept': int(len(model_groups)),
        'global_p50': float(global_p50),
        'global_p90': float(global_p90),
    }
    _write_json_atomic(out_model_json, model)
    _write_json_atomic(out_report_json, report)
    return {'model': model, 'report': report}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description='Train OFC exec-cost group model from JSONL dataset')
    ap.add_argument('--rows_jsonl', required=True)
    ap.add_argument('--out_model_json', required=True)
    ap.add_argument('--out_report_json', required=True)
    ap.add_argument('--min_group_rows', type=int, default=30)
    args = ap.parse_args(argv)
    train_exec_cost_model(str(args.rows_jsonl), out_model_json=str(args.out_model_json), out_report_json=str(args.out_report_json), min_group_rows=int(args.min_group_rows))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
