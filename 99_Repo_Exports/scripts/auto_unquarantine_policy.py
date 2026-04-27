#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore


@dataclass
class AutoUnquarantineDecision:
    sid: str
    release: bool
    reason: str
    recent_repair_count: int = 0
    recent_quarantine_count: int = 0
    quarantine_age_sec: float = 0.0


def decide_auto_unquarantine(
    *,
    sid: str,
    quarantine_payload: Dict[str, Any],
    sql_snapshot_exists: bool,
    max_recent_requarantine: int = 1,
    min_quarantine_age_sec: float = 900.0,
) -> AutoUnquarantineDecision:
    now_ms = int(time.time() * 1000)
    ts_ms = int(float(quarantine_payload.get('quarantined_ts_ms') or 0))
    age_sec = max(0.0, (now_ms - ts_ms) / 1000.0) if ts_ms else 0.0
    sev = str(quarantine_payload.get('severity') or 'unknown').lower()
    recent_req = int(quarantine_payload.get('recent_requarantine_count') or 0)
    recent_rep = int(quarantine_payload.get('recent_repair_count') or 0)

    if not sql_snapshot_exists:
        return AutoUnquarantineDecision(
            sid=sid, release=False, reason='sql_snapshot_missing',
            recent_repair_count=recent_rep, recent_quarantine_count=recent_req, quarantine_age_sec=age_sec,
        )
    if age_sec < float(min_quarantine_age_sec):
        return AutoUnquarantineDecision(
            sid=sid, release=False, reason='quarantine_too_fresh',
            recent_repair_count=recent_rep, recent_quarantine_count=recent_req, quarantine_age_sec=age_sec,
        )
    if recent_req > int(max_recent_requarantine):
        return AutoUnquarantineDecision(
            sid=sid, release=False, reason='recent_requarantine_exceeded',
            recent_repair_count=recent_rep, recent_quarantine_count=recent_req, quarantine_age_sec=age_sec,
        )
    if sev in {'critical', 'hard'}:
        return AutoUnquarantineDecision(
            sid=sid, release=False, reason='severity_too_high',
            recent_repair_count=recent_rep, recent_quarantine_count=recent_req, quarantine_age_sec=age_sec,
        )
    return AutoUnquarantineDecision(
        sid=sid, release=True, reason='healthy_after_repair',
        recent_repair_count=recent_rep, recent_quarantine_count=recent_req, quarantine_age_sec=age_sec,
    )


def _sql_snapshot_exists(dsn: str, sid: str) -> bool:
    if not dsn or psycopg is None:
        return False
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute('select 1 from execution_orders where sid=%s limit 1', (sid,))
            return cur.fetchone() is not None


def main() -> int:
    parser = argparse.ArgumentParser(description='Release quarantine sid automatically after healthy repair.')
    parser.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    parser.add_argument('--dsn', default=os.getenv('EXECUTION_JOURNAL_DSN', ''))
    parser.add_argument('--quarantine-prefix', default=os.getenv('ORDERS_QUARANTINE_PREFIX', 'orders:quarantine:state:'))
    parser.add_argument('--quarantine-sids-key', default=os.getenv('ORDERS_QUARANTINE_SIDS_KEY', 'orders:quarantine:state:sids'))
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if redis is None:
        raise RuntimeError('redis package required')
    r = redis.from_url(args.redis_url, decode_responses=True)
    sids = sorted(r.smembers(args.quarantine_sids_key) or set())
    report: List[Dict[str, Any]] = []
    for sid in sids:
        raw = r.get(f"{args.quarantine_prefix}{sid}")
        payload = json.loads(raw) if raw else {}
        ok = _sql_snapshot_exists(args.dsn, sid)
        dec = decide_auto_unquarantine(sid=sid, quarantine_payload=payload, sql_snapshot_exists=ok)
        report.append(dec.__dict__)
        if dec.release and not args.dry_run:
            r.srem(args.quarantine_sids_key, sid)
            r.delete(f"{args.quarantine_prefix}{sid}")
            r.xadd(
                'orders:quarantine:events',
                {'sid': sid, 'event': 'AUTO_UNQUARANTINED', 'reason': dec.reason, 'ts_ms': str(int(time.time() * 1000))},
                maxlen=10000, approximate=True,
            )
    print(json.dumps({'items': report}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
