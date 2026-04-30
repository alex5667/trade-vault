#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import math
import os
import time
from typing import Any, Dict, Iterator, List, Optional


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


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _clip01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return float(x)


def train_rule_success_model(rows_jsonl: str, *, out_model_json: str, out_report_json: str, min_group_rows: int = 50, beta_prior: float = 5.0) -> Dict[str, Any]:
    all_pairs: List[tuple[float, int]] = []
    groups: Dict[str, List[tuple[float, int]]] = {}
    for row in _iter_jsonl(rows_jsonl):
        ctx_key = str(row.get('ctx_key') or 'global')
        score = _clip01(_to_float(row.get('raw_score'), 0.0))
        y = 1 if _to_int(row.get('label_rule_success'), 0) == 1 else 0
        groups.setdefault(ctx_key, []).append((score, y))
        all_pairs.append((score, y))

    if all_pairs:
        global_raw_mean = sum(s for s, _ in all_pairs) / len(all_pairs)
        global_pos_rate = sum(y for _, y in all_pairs) / len(all_pairs)
    else:
        global_raw_mean = 0.5
        global_pos_rate = 0.5

    defaults = {
        'p_rule_raw': float(global_raw_mean)
        'p_rule_cal': float(global_pos_rate)
        'score_min_ctx': float(max(0.50, min(0.80, global_pos_rate)))
    }
    model_groups: Dict[str, Dict[str, Any]] = {}
    for key, vals in groups.items():
        n = len(vals)
        if n < int(min_group_rows):
            continue
        raw_mean = sum(s for s, _ in vals) / n
        pos = sum(y for _, y in vals)
        cal = (pos + float(beta_prior) * global_pos_rate) / (n + float(beta_prior))
        score_min = max(0.50, min(0.90, cal))
        model_groups[key] = {
            'n': int(n)
            'p_rule_raw': float(raw_mean)
            'p_rule_cal': float(cal)
            'score_min_ctx': float(score_min)
        }

    model = {
        'kind': 'ofc_rule_success_v1'
        'version': time.strftime('%Y%m%d_%H%M%S', time.gmtime())
        'created_ts_ms': _now_ms()
        'min_group_rows': int(min_group_rows)
        'beta_prior': float(beta_prior)
        'defaults': defaults
        'groups': model_groups
    }
    report = {
        'rows': int(len(all_pairs))
        'groups_total': int(len(groups))
        'groups_kept': int(len(model_groups))
        'global_pos_rate': float(global_pos_rate)
        'global_raw_mean': float(global_raw_mean)
    }
    _write_json_atomic(out_model_json, model)
    _write_json_atomic(out_report_json, report)
    return {'model': model, 'report': report}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description='Train OFC rule-success group model from JSONL dataset')
    ap.add_argument('--rows_jsonl', required=True)
    ap.add_argument('--out_model_json', required=True)
    ap.add_argument('--out_report_json', required=True)
    ap.add_argument('--min_group_rows', type=int, default=50)
    ap.add_argument('--beta_prior', type=float, default=5.0)
    args = ap.parse_args(argv)
    train_rule_success_model(str(args.rows_jsonl), out_model_json=str(args.out_model_json), out_report_json=str(args.out_report_json), min_group_rows=int(args.min_group_rows), beta_prior=float(args.beta_prior))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
