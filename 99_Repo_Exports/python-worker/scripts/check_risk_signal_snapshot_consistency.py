#!/usr/bin/env python3
"""P4.6/P4.7: Consistency check between signal payload and SQL risk_snapshot.

P4.6: Joins risk_decisions (signal_jsonb) with risk_snapshot (snapshot_jsonb)
and checks for drift/contract mismatch on four key fields:
  - execution_policy
  - planned_notional_usd
  - risk_leverage_cap
  - clamp_ratio

P4.7 extensions:
  - --out writes latest_risk_signal_consistency.json atomically
  - --repeat-threshold: quarantine SIDs with >= N mismatches (default 3)
  - --quarantine-on-repeated: auto-quarantine repeated-mismatch SIDs in Redis
  - mismatch_rate added to output for operator score computation

This should be run after deploys that change the signal publish path or risk engine
contract to ensure signal → snapshot fidelity is preserved.

Environment variables
---------------------
RISK_AUDIT_SQL_DSN              – PostgreSQL DSN (falls back to EXECUTION_JOURNAL_DSN)
RISK_CONSISTENCY_LIMIT          – max rows to check (default: 250)
RISK_CONSISTENCY_REPORT_PATH    – output JSON path
REDIS_URL                       – Redis URL for quarantine writes
ORDERS_QUARANTINE_PREFIX        – Redis quarantine key prefix
RISK_CONSISTENCY_REPEAT_THRESHOLD – min mismatch count to trigger quarantine (default: 3)
RISK_CONSISTENCY_QUARANTINE_ON_REPEATED – '1'/'true' to enable quarantine (default: off)

Usage
-----
  python3 scripts/check_risk_signal_snapshot_consistency.py
  python3 scripts/check_risk_signal_snapshot_consistency.py --dsn postgresql://... --limit 500
  python3 scripts/check_risk_signal_snapshot_consistency.py --quarantine-on-repeated
"""
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import sys
import time
from pathlib import Path

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

# P4.8: Inject repo root into sys.path so audit_code can be imported regardless of CWD.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from audit_code.risk.risk_drift_sql import RiskDriftSqlSink  # noqa: E402


def _eq_num(a: object, b: object, tol: float = 1e-6) -> bool:
    """Numeric near-equality with tolerance; falls back to equality for non-numeric values."""
    try:
        return abs(float(a) - float(b)) <= tol  # type: ignore[arg-type]
    except Exception:
        return a == b


def _write_atomic(path: Path, payload: str) -> None:
    """Write payload to path atomically via a temp file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(payload, encoding='utf-8')
    tmp.replace(path)


def _quarantine_sid(redis_client: object, sid: str, *, prefix: str, reason: str, payload: dict) -> dict:
    """Write a single SID to the Redis quarantine set, hash, and event stream.

    Uses the same schema as the execution quarantine (orders:quarantine:state:).
    """
    qprefix = prefix.rstrip(':') + ':'
    qkey = f'{qprefix}{sid}'
    redis_client.set(qkey, json.dumps(payload, ensure_ascii=False, sort_keys=True))  # type: ignore[union-attr]
    redis_client.sadd(f'{qprefix}sids', sid)  # type: ignore[union-attr]
    redis_client.xadd(  # type: ignore[union-attr]
        f'{qprefix}events',
        {'sid': sid, 'reason': reason, 'source': 'risk_consistency_checker'},
        maxlen=10000,
        approximate=True,
    )
    return {'sid': sid, 'quarantine_key': qkey, 'reason': reason}


def render_textfile(report: dict) -> str:
    """Render Prometheus textfile metrics from the consistency report (P4.8).

    Exports:
      trade_risk_signal_mismatch_rate        – fraction of checked decisions that mismatched.
      trade_risk_signal_repeated_sid_total   – number of SIDs above repeat_threshold.
    """
    lines = [
        '# HELP trade_risk_signal_mismatch_rate Fraction of checked decisions that mismatched risk snapshot.',
        '# TYPE trade_risk_signal_mismatch_rate gauge',
        f"trade_risk_signal_mismatch_rate {float(report.get('mismatch_rate') or 0.0)}",
        '# HELP trade_risk_signal_repeated_sid_total Number of repeated mismatch sid above threshold.',
        '# TYPE trade_risk_signal_repeated_sid_total gauge',
        f"trade_risk_signal_repeated_sid_total {int(len(report.get('repeated_sid') or []))}",
    ]
    return '\n'.join(lines) + '\n'


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Check consistency between signal payload persisted in '
                    'risk_decisions and SQL risk_snapshot.'
    )
    parser.add_argument(
        '--dsn',
        default=os.getenv('RISK_AUDIT_SQL_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '')),
        help='PostgreSQL DSN',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=int(os.getenv('RISK_CONSISTENCY_LIMIT', '250')),
        help='Maximum number of recent rows to check',
    )
    parser.add_argument(
        '--out',
        default=os.getenv(
            'RISK_CONSISTENCY_REPORT_PATH',
            '/var/lib/trade-runbook/reports/latest_risk_signal_consistency.json',
        ),
        help='Output JSON report path (written atomically)',
    )
    parser.add_argument(
        '--redis-url',
        default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
        help='Redis URL for quarantine writes',
    )
    parser.add_argument(
        '--quarantine-prefix',
        default=os.getenv('ORDERS_QUARANTINE_PREFIX', 'orders:quarantine:state:'),
        help='Redis key prefix for quarantine entries',
    )
    parser.add_argument(
        '--repeat-threshold',
        type=int,
        default=int(os.getenv('RISK_CONSISTENCY_REPEAT_THRESHOLD', '3')),
        help='Minimum mismatch count per SID to trigger quarantine (default: 3)',
    )
    parser.add_argument(
        '--quarantine-on-repeated',
        action='store_true',
        default=os.getenv('RISK_CONSISTENCY_QUARANTINE_ON_REPEATED', '0') not in {'0', 'false', 'False'},
        help='Auto-quarantine SIDs with repeated mismatches in Redis',
    )
    # P4.8: Prometheus textfile output for node_exporter textfile_collector.
    parser.add_argument(
        '--textfile-output',
        default=os.getenv('RISK_CONSISTENCY_TEXTFILE_PATH', ''),
        help='Path to write Prometheus textfile metrics (.prom); leave empty to disable',
    )
    args = parser.parse_args()

    if not args.dsn or psycopg is None:
        raise RuntimeError(
            'psycopg + DSN required. '
            'Set RISK_AUDIT_SQL_DSN or EXECUTION_JOURNAL_DSN environment variable.'
        )

    # P4.8: Initialise SQL sink — best-effort, never raises.
    sink = RiskDriftSqlSink.from_env()
    mismatches: list[dict] = []
    checked = 0

    # Retry DB connection with exponential backoff to handle transient PG unavailability.
    max_retries = 5
    conn = None
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg.connect(args.dsn)
            break
        except psycopg.OperationalError:
            if attempt == max_retries:
                raise
            delay = 2 ** attempt
            print(
                f'[risk-consistency] DB connect attempt {attempt}/{max_retries} failed, '
                f'retrying in {delay}s …',
                file=sys.stderr,
            )
            time.sleep(delay)

    with conn:  # type: ignore[union-attr]
        with conn.cursor() as cur:
            cur.execute(
                '''
                select a.decision_id, a.signal_id, a.symbol, a.sid, a.effective_execution_policy,
                       a.adjusted_notional_usd, a.leverage_cap, a.clamp_ratio,
                       a.signal_jsonb, s.snapshot_jsonb, s.tier, s.level,
                       s.effective_execution_policy as snap_execution_policy,
                       s.adjusted_notional_usd      as snap_adjusted_notional_usd,
                       s.leverage_cap               as snap_leverage_cap,
                       s.clamp_ratio                as snap_clamp_ratio
                from risk_decisions a
                join risk_snapshot s using (decision_id, ts)
                order by a.ts desc
                limit %s
                ''',
                (args.limit,),
            )
            for row in cur.fetchall():
                checked += 1
                (
                    decision_id, signal_id, symbol, sid,
                    exec_policy, adjusted_notional, leverage_cap, clamp_ratio,
                    signal_jsonb, snapshot_jsonb, tier, level,  # type: ignore[misc]
                    snap_exec_policy, snap_adj, snap_lev, snap_clamp,
                ) = row

                signal: dict = signal_jsonb or {}
                snap: dict = snapshot_jsonb or {}
                reasons: list[str] = []

                # Check execution_policy consistency
                sig_policy = str(signal.get('execution_policy') or exec_policy)
                db_policy  = str(snap_exec_policy or signal.get('execution_policy') or exec_policy)
                if sig_policy != db_policy:
                    reasons.append('execution_policy')

                # Check planned_notional_usd (signal) vs adjusted_notional_usd (snapshot)
                if not _eq_num(signal.get('planned_notional_usd', adjusted_notional), snap_adj):
                    reasons.append('planned_notional_usd')

                # Check risk_leverage_cap (signal) vs leverage_cap (snapshot)
                if not _eq_num(signal.get('risk_leverage_cap', leverage_cap), snap_lev):
                    reasons.append('risk_leverage_cap')

                # Check clamp_ratio from snapshot_jsonb vs snapshot table column
                snap_ratio = snap.get('clamp_ratio') if isinstance(snap, dict) else None
                if not _eq_num(
                    snap_ratio if snap_ratio is not None else clamp_ratio,
                    snap_clamp,
                ):
                    reasons.append('clamp_ratio_snapshot')

                if reasons:
                    mismatches.append({
                        'decision_id': decision_id,
                        'signal_id':   signal_id,  # P4.8: included for SQL ledger
                        'sid':         sid,
                        'symbol':      symbol,
                        'tier':        tier,
                        'level':       level,
                        'mismatches':  reasons,
                    })

    # P4.7: count repeated mismatches per SID; P4.8: retain first representative item.
    repeated_by_sid: dict[str, int] = {}
    first_for_sid: dict[str, dict] = {}
    for item in mismatches:
        sid = str(item.get('sid') or '')
        if not sid:
            continue
        repeated_by_sid[sid] = repeated_by_sid.get(sid, 0) + 1
        first_for_sid.setdefault(sid, item)

    repeated: list[dict] = []
    for sid, count in sorted(repeated_by_sid.items()):
        if count >= int(args.repeat_threshold):
            merged = dict(first_for_sid[sid])
            merged['sid'] = sid
            merged['count'] = count
            repeated.append(merged)

    # P4.7: mismatch_rate for operator score computation (computed before quarantine loop).
    mismatch_rate = (float(len(mismatches)) / float(checked)) if checked else 0.0

    # P4.7: auto-quarantine SIDs with repeated mismatches when flag is set.
    # P4.8: also write each repeated quarantine event to the SQL ledger via sink.
    quarantine_results: list[dict] = []
    if args.quarantine_on_repeated and repeated and redis is not None:
        r = redis.from_url(args.redis_url, decode_responses=True)
        now_ms = get_ny_time_millis()
        for item in repeated:
            payload = {
                'sid': item['sid'],
                'mismatch_count': item['count'],
                'reason': 'repeated_risk_consistency_mismatch',
            }
            quarantine_results.append(_quarantine_sid(
                r,
                item['sid'],
                prefix=args.quarantine_prefix,
                reason='repeated_risk_consistency_mismatch',
                payload=payload,
            ))
            # P4.8: Write to SQL ledger (best-effort, never raises).
            sink.record_quarantine({
                'decision_id':      item.get('decision_id'),
                'signal_id':        item.get('signal_id'),
                'sid':              item['sid'],
                'symbol':           item.get('symbol') or '',
                'tier':             item.get('tier') or '',
                'repeated_count':   item['count'],
                'mismatch_rate':    mismatch_rate,
                'reasons':          item.get('mismatches') or [],
                'quarantine_action': 'REPEATED_MISMATCH_QUARANTINED',
                'created_ts_ms':    now_ms,
            })

    out = {
        'checked':            checked,
        'mismatch_count':     len(mismatches),
        'mismatch_rate':      mismatch_rate,
        'mismatches':         mismatches,
        'repeated_sid':       repeated,
        'quarantine_results': quarantine_results,
        'generated_at_ms':    get_ny_time_millis(),  # P4.8: explicit generation timestamp
    }

    # Write JSON report atomically so readers never see a partial file.
    _write_atomic(Path(args.out), json.dumps(out, ensure_ascii=False, indent=2) + '\n')
    # P4.8: Optionally write Prometheus textfile metrics for node_exporter.
    if args.textfile_output:
        _write_atomic(Path(args.textfile_output), render_textfile(out))
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
