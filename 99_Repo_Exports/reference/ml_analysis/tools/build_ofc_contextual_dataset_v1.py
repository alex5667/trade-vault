#!/usr/bin/env python3
from __future__ import annotations

"""Build OFC contextual training datasets by joining decision records with outcome records.

Input format: newline-delimited JSON (JSONL/NDJSON).
The tool is intentionally stdlib-only so it can run in minimal environments.
"""

import argparse
import json
import os
import time
from typing import Any, Dict, Iterable, Iterator, List, Optional


def _now_ms() -> int:
    return int(time.time() * 1000)


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


def _write_jsonl(path: str, rows: Iterable[Dict[str, Any]]) -> int:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or '.', exist_ok=True)
    n = 0
    tmp = f"{path}.tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            n += 1
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return n


def _coalesce(*vals: Any, default: Any = None) -> Any:
    for v in vals:
        if v is not None and v != '':
            return v
    return default


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


def _extract_ctx_key(rec: Dict[str, Any]) -> str:
    ofc = rec.get('of_confirm') if isinstance(rec.get('of_confirm'), dict) else {}
    ev = ofc.get('evidence') if isinstance(ofc.get('evidence'), dict) else {}
    return str(_coalesce(rec.get('ctx_key'), ev.get('ctx_key'), default='global'))


def _build_exec_cost_row(decision: Dict[str, Any], outcome: Dict[str, Any]) -> Dict[str, Any]:
    ofc = decision.get('of_confirm') if isinstance(decision.get('of_confirm'), dict) else {}
    ind = ofc.get('indicators') if isinstance(ofc.get('indicators'), dict) else {}
    ev = ofc.get('evidence') if isinstance(ofc.get('evidence'), dict) else {}
    ctx_key = _extract_ctx_key(decision)
    spread_bps = _to_float(_coalesce(decision.get('spread_bps'), ind.get('spread_bps')))
    expected_slippage_bps = _to_float(_coalesce(decision.get('expected_slippage_bps'), ind.get('expected_slippage_bps')))
    realized_slippage_bps = _to_float(_coalesce(outcome.get('realized_slippage_bps'), outcome.get('slippage_bps'), outcome.get('realized_slip_worse_bps')))
    exec_risk_ref_bps = _to_float(_coalesce(decision.get('ctx_exec_risk_ref_bps'), decision.get('exec_risk_ref_bps'), ev.get('ctx_exec_risk_ref_bps')))
    return {
        'sid': str(_coalesce(decision.get('sid'), outcome.get('sid'), default='')),
        'decision_ts_ms': _to_int(_coalesce(decision.get('decision_ts_ms'), decision.get('ts_ms'), decision.get('ts'))),
        'symbol': str(_coalesce(decision.get('symbol'), outcome.get('symbol'), default='')),
        'direction': str(_coalesce(decision.get('direction'), outcome.get('direction'), default='')),
        'ctx_key': ctx_key,
        'session': str(_coalesce(decision.get('ctx_session'), decision.get('session'), default='')),
        'scenario_v4': str(_coalesce(decision.get('scenario_v4'), ind.get('scenario_v4'), default='')),
        'spread_bps': spread_bps,
        'expected_slippage_bps': expected_slippage_bps,
        'exec_risk_ref_bps': exec_risk_ref_bps,
        'realized_slippage_bps': realized_slippage_bps,
        'fill_delay_ms': _to_int(_coalesce(outcome.get('fill_delay_ms'), default=0)),
        'book_staleness_ms': _to_int(_coalesce(decision.get('book_staleness_ms'), ind.get('book_staleness_ms'), default=0)),
    }


def _build_rule_success_row(decision: Dict[str, Any], outcome: Dict[str, Any], success_bps: float) -> Dict[str, Any]:
    ofc = decision.get('of_confirm') if isinstance(decision.get('of_confirm'), dict) else {}
    ind = ofc.get('indicators') if isinstance(ofc.get('indicators'), dict) else {}
    pnl_bps_net = _to_float(_coalesce(outcome.get('pnl_bps_net'), outcome.get('net_pnl_bps'), default=0.0))
    label_rule_success = 1 if pnl_bps_net >= float(success_bps) else 0
    ctx_key = _extract_ctx_key(decision)
    raw_score = _to_float(_coalesce(decision.get('of_score_final'), decision.get('raw_score'), ofc.get('score')))
    return {
        'sid': str(_coalesce(decision.get('sid'), outcome.get('sid'), default='')),
        'decision_ts_ms': _to_int(_coalesce(decision.get('decision_ts_ms'), decision.get('ts_ms'), decision.get('ts'))),
        'symbol': str(_coalesce(decision.get('symbol'), outcome.get('symbol'), default='')),
        'direction': str(_coalesce(decision.get('direction'), outcome.get('direction'), default='')),
        'ctx_key': ctx_key,
        'session': str(_coalesce(decision.get('ctx_session'), decision.get('session'), default='')),
        'scenario_v4': str(_coalesce(decision.get('scenario_v4'), ind.get('scenario_v4'), default='')),
        'raw_score': raw_score,
        'pnl_bps_net': pnl_bps_net,
        'tp_bps': _to_float(_coalesce(decision.get('tp_bps'), decision.get('liqmap_gate_reward_bps'), ind.get('liqmap_gate_reward_bps'))),
        'sl_bps': _to_float(_coalesce(decision.get('sl_bps'), decision.get('liqmap_gate_risk_bps'), ind.get('liqmap_gate_risk_bps'))),
        'label_rule_success': label_rule_success,
        'label_edge_positive': 1 if pnl_bps_net > 0.0 else 0,
    }


def build_dataset(*, decisions_jsonl: str, outcomes_jsonl: str, out_exec_cost_jsonl: str, out_rule_success_jsonl: str, out_report_json: str, success_bps: float = 0.0) -> Dict[str, Any]:
    decisions_by_sid: Dict[str, Dict[str, Any]] = {}
    for rec in _iter_jsonl(decisions_jsonl):
        sid = str(rec.get('sid') or '')
        if sid:
            decisions_by_sid[sid] = rec

    exec_rows: List[Dict[str, Any]] = []
    rule_rows: List[Dict[str, Any]] = []
    outcomes_total = 0
    joined = 0
    pos = 0
    for out in _iter_jsonl(outcomes_jsonl):
        outcomes_total += 1
        sid = str(out.get('sid') or '')
        if not sid:
            continue
        dec = decisions_by_sid.get(sid)
        if dec is None:
            continue
        joined += 1
        exec_rows.append(_build_exec_cost_row(dec, out))
        rr = _build_rule_success_row(dec, out, float(success_bps))
        rule_rows.append(rr)
        if int(rr.get('label_rule_success', 0)) == 1:
            pos += 1

    n_exec = _write_jsonl(out_exec_cost_jsonl, exec_rows)
    n_rule = _write_jsonl(out_rule_success_jsonl, rule_rows)
    report = {
        'ts_ms': _now_ms(),
        'decisions_total': int(len(decisions_by_sid)),
        'outcomes_total': int(outcomes_total),
        'joined': int(joined),
        'exec_rows': int(n_exec),
        'rule_rows': int(n_rule),
        'pos_rate': float(pos / joined) if joined > 0 else 0.0,
        'success_bps': float(success_bps),
        'out_exec_cost_jsonl': str(out_exec_cost_jsonl),
        'out_rule_success_jsonl': str(out_rule_success_jsonl),
    }
    _write_json_atomic(out_report_json, report)
    return report


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description='Build OFC contextual training datasets from JSONL decision+outcome inputs')
    ap.add_argument('--decisions_jsonl', required=True)
    ap.add_argument('--outcomes_jsonl', required=True)
    ap.add_argument('--out_exec_cost_jsonl', required=True)
    ap.add_argument('--out_rule_success_jsonl', required=True)
    ap.add_argument('--out_report_json', required=True)
    ap.add_argument('--success_bps', type=float, default=0.0)
    args = ap.parse_args(argv)
    build_dataset(
        decisions_jsonl=str(args.decisions_jsonl),
        outcomes_jsonl=str(args.outcomes_jsonl),
        out_exec_cost_jsonl=str(args.out_exec_cost_jsonl),
        out_rule_success_jsonl=str(args.out_rule_success_jsonl),
        out_report_json=str(args.out_report_json),
        success_bps=float(args.success_bps),
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
