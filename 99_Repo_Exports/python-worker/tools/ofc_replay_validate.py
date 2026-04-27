#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import hashlib
from types import SimpleNamespace
from typing import Any, Dict, Tuple, Optional


def _ns(x: Any, *, max_depth: int = 2) -> Any:
    """Convert dict->SimpleNamespace recursively (bounded).

    We only need attribute access for a few runtime.last_* objects.
    Keep other values as-is to avoid excessive transformation.
    """
    if max_depth <= 0:
        return x
    if isinstance(x, dict):
        return SimpleNamespace(**{k: _ns(v, max_depth=max_depth - 1) for k, v in x.items()})
    if isinstance(x, list):
        return [_ns(v, max_depth=max_depth - 1) for v in x]
    return x


def _stable_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(s.encode('utf-8')).hexdigest()


def _json_safe(x: Any) -> Any:
    if x is None or isinstance(x, (str, int, float, bool)):
        return x
    if isinstance(x, dict):
        return {str(k): _json_safe(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_json_safe(v) for v in x]
    try:
        return str(x)
    except Exception:
        return None


def _build_once(row: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    from core.of_confirm_engine import OFConfirmEngine

    eng = OFConfirmEngine(version=int(row.get('engine_version', 3) or 3))

    tick = row.get('tick') or {}
    ind = row.get('indicators') or {}
    rs = row.get('runtime_snapshot') or {}
    cfg = row.get('cfg') or {}

    # Restore cancel gate state (if present)
    try:
        cgs = row.get('cancel_gate_state', None)
        if not cgs and isinstance(rs, dict):
            cgs = rs.get('cancel_gate_state', None)
        if not cgs and isinstance(ind, dict):
            cgs = ind.get('cancel_gate_state', None)
        if cgs:
            eng.restore_cancel_gate_state(cgs)
    except Exception:
        pass

    symbol = str(row.get('symbol') or tick.get('symbol') or 'UNKNOWN')
    tf = str(row.get('tf') or cfg.get('micro_tf') or '1s')
    direction = str(row.get('direction') or 'LONG').upper()

    tick_ts_ms = int(row.get('tick_ts_ms') or tick.get('ts_ms') or tick.get('ts') or 0)
    # Prefer captured deterministic time if present
    try:
        nts = int(rs.get('now_ts_ms_used', 0) or 0) if isinstance(rs, dict) else 0
        if nts > 0:
            tick_ts_ms = nts
    except Exception:
        pass

    price = float(row.get('price') or tick.get('price') or tick.get('last') or 0.0)
    delta_z = float(row.get('delta_z') or ind.get('delta_z') or 0.0)

    runtime = SimpleNamespace(
        symbol=symbol,
        config={"micro_tf": tf},
        dynamic_cfg=_json_safe(rs.get('dynamic_cfg') if isinstance(rs, dict) else None) or {},
        last_regime=(rs.get('last_regime') if isinstance(rs, dict) else None),
        liq_regime=(rs.get('liq_regime') if isinstance(rs, dict) else None),
        book_churn_hi=(rs.get('book_churn_hi') if isinstance(rs, dict) else 0),
        cont_ctx_ts_ms=(rs.get('cont_ctx_ts_ms') if isinstance(rs, dict) else 0),
        pressure_hi=(rs.get('pressure_hi') if isinstance(rs, dict) else None),
        last_bar=_ns(rs.get('last_bar') if isinstance(rs, dict) else None),
        last_obi_event=_ns(rs.get('last_obi_event') if isinstance(rs, dict) else None),
        last_iceberg_event=_ns(rs.get('last_iceberg_event') if isinstance(rs, dict) else None),
        last_ofi_event=_ns(rs.get('last_ofi_event') if isinstance(rs, dict) else None),
        last_sweep=_ns(rs.get('last_sweep') if isinstance(rs, dict) else None),
        last_reclaim=_ns(rs.get('last_reclaim') if isinstance(rs, dict) else None),
        last_wp=_ns(rs.get('last_wp') if isinstance(rs, dict) else None),
        last_div=_ns(rs.get('last_div') if isinstance(rs, dict) else None),
        last_fp_edge=_ns(rs.get('last_fp_edge') if isinstance(rs, dict) else None),
    )

    # Ensure time fallback is deterministic even if tick_ts_ms==0
    try:
        if isinstance(ind, dict) and tick_ts_ms > 0:
            ind = dict(ind)
            ind.setdefault('now_ts_ms', int(tick_ts_ms))
    except Exception:
        pass

    ofc, _dec = eng.build(
        symbol=symbol,
        tf=tf,
        direction=direction,
        tick_ts_ms=int(tick_ts_ms),
        price=float(price),
        delta_z=float(delta_z),
        runtime=runtime,
        cfg=cfg,
        indicators=ind,
        absorption=(row.get('absorption') if isinstance(row.get('absorption'), dict) else None),
    )
    out = ofc.to_dict() if ofc else {"ofc": None}
    h = _stable_hash(out)
    return h, out


def main() -> int:
    ap = argparse.ArgumentParser(description='Replay/validate OFC_CAPTURE ndjson for determinism + contract checks')
    ap.add_argument('--ndjson', required=True, help='path to OFC_CAPTURE ndjson file')
    ap.add_argument('--max-rows', type=int, default=int(os.getenv('OFC_REPLAY_MAX_ROWS', '0') or 0), help='limit rows (0=all)')
    ap.add_argument('--strict-contract', action='store_true', default=(os.getenv('OFC_REPLAY_STRICT_CONTRACT', '0').lower() in {'1','true','yes'}), help='fail if runtime_snapshot contract missing keys')
    ap.add_argument('--skip-determinism', action='store_true', default=False, help='skip double-run determinism check')
    ap.add_argument('--report-json', default=str(os.getenv('OFC_REPLAY_REPORT_JSON', '') or ''), help='optional path to write JSON report')
    args = ap.parse_args()

    from core.of_confirm_engine import OFConfirmEngine

    path = str(args.ndjson)
    max_rows = int(args.max_rows or 0)

    bad_replay = 0
    bad_det = 0
    bad_contract = 0
    n = 0
    missing_counts: Dict[str, int] = {}

    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            n += 1
            if max_rows and n > max_rows:
                break
            try:
                row = json.loads(line)
            except Exception as e:
                bad_replay += 1
                print(f'ERR parse line={n}: {e}', file=sys.stderr)
                continue

            rs = row.get('runtime_snapshot') or {}
            if isinstance(rs, dict):
                ok, missing = OFConfirmEngine.validate_runtime_snapshot_contract(rs)
                if not ok:
                    bad_contract += 1
                    for m in missing:
                        missing_counts[m] = missing_counts.get(m, 0) + 1
                    if args.strict_contract:
                        print(f'ERR contract line={n}: missing={missing}', file=sys.stderr)

            try:
                if args.skip_determinism:
                    h1, _ = _build_once(row)
                    h2 = h1
                else:
                    h1, _ = _build_once(row)
                    h2, _ = _build_once(row)
            except Exception as e:
                bad_replay += 1
                print(f'ERR replay line={n}: {e}', file=sys.stderr)
                continue

            if h1 != h2:
                bad_det += 1
                print(f'ERR nondeterministic line={n}: h1={h1[:12]} h2={h2[:12]}', file=sys.stderr)

    top_missing = sorted(missing_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:25]
    report = {
        "n_rows": int(n),
        "bad_replay": int(bad_replay),
        "bad_det": int(bad_det),
        "bad_contract": int(bad_contract),
        "top_missing": [{"path": k, "count": int(v)} for k, v in top_missing],
    }

    if args.report_json:
        try:
            with open(args.report_json, 'w', encoding='utf-8') as wf:
                wf.write(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
        except Exception as e:
            print(f'ERR write report_json: {e}', file=sys.stderr)

    print(json.dumps(report, ensure_ascii=False, sort_keys=True))

    if bad_replay:
        return 3
    if bad_det:
        return 2
    if args.strict_contract and bad_contract:
        return 4
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

