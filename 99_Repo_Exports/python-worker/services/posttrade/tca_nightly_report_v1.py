from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Nightly TCA report bundle (P6 gap-closure).

Computes compact 24h/7d TCA rollups from ``tca_fill_metrics`` and publishes:
  - JSON status/report files for offline inspection
  - Redis summary hash for Prometheus exporter / alerts

The design is intentionally low-cardinality: Prometheus gets only summary counts
and worst-case values, while the detailed offenders remain in the JSON report.
"""

import argparse
import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Sequence


def _now_ms() -> int:
    return get_ny_time_millis()


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != '' else default


def _env_int(name: str, default: str) -> int:
    try:
        return int(float(_env(name, default)))
    except Exception:
        return int(float(default))


def _env_float(name: str, default: str) -> float:
    try:
        return float(_env(name, default))
    except Exception:
        return float(default)


def pick_dsn() -> str:
    return (
        os.getenv('TCA_DB_DSN')
        or os.getenv('TRADES_DB_DSN')
        or os.getenv('TIMESCALE_DSN')
        or os.getenv('ANALYTICS_DB_DSN')
        or os.getenv('ANALYTICS_DSN')
        or os.getenv('PG_DSN')
        or os.getenv('DATABASE_URL')
        or ''
    )


def _safe_ident(name: str, default: str) -> str:
    s = str(name or '').strip()
    if not s or not re.fullmatch(r'[A-Za-z0-9_\.]+', s):
        return default
    return s


def _write_json_atomic(path: str, obj: Dict[str, Any]) -> None:
    p = str(path or '').strip()
    if not p:
        return
    os.makedirs(os.path.dirname(os.path.abspath(p)), exist_ok=True)
    tmp = p + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, p)


def _connect_redis(url: str):
    try:
        import redis  # type: ignore
        return redis.Redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _pg_connect(dsn: str):
    try:
        import psycopg2  # type: ignore
        return psycopg2.connect(dsn)
    except Exception:
        return None


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return float(default)
    return float(v)


def _i(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(default)


def _fetch_rollups(cur, *, table: str, lookback_h: int, min_rows: int) -> List[Dict[str, Any]]:
    q = f"""
      select
        sym
        venue
        session
        tf
        kind
        side
        count(*)::bigint as n
        percentile_cont(0.50) within group (order by is_bps) as is_p50_bps
        percentile_cont(0.95) within group (order by is_bps) as is_p95_bps
        percentile_cont(0.99) within group (order by is_bps) as is_p99_bps
        percentile_cont(0.95) within group (order by eff_spread_bps) as eff_spread_p95_bps
        percentile_cont(0.50) within group (order by realized_spread_1s_bps) as realized_spread_1s_p50_bps
        percentile_cont(0.95) within group (order by perm_impact_1s_bps) as perm_impact_1s_p95_bps
        avg(case when realized_spread_1s_bps < 0 then 1 else 0 end) as realized_spread_1s_neg_share
      from {table}
      where ts > now() - (%s * interval '1 hour')
      group by sym, venue, session, tf, kind, side
      having count(*) >= %s
    """
    cur.execute(q, (int(lookback_h), int(min_rows)))
    rows = []
    for row in cur.fetchall() or []:
        rows.append({
            'sym': str(row[0] or '').upper()
            'venue': str(row[1] or '').lower()
            'session': str(row[2] or 'na')
            'tf': str(row[3] or 'na')
            'kind': str(row[4] or 'na')
            'side': str(row[5] or 'na').upper()
            'n': _i(row[6], 0)
            'is_p50_bps': _f(row[7], 0.0)
            'is_p95_bps': _f(row[8], 0.0)
            'is_p99_bps': _f(row[9], 0.0)
            'eff_spread_p95_bps': _f(row[10], 0.0)
            'realized_spread_1s_p50_bps': _f(row[11], 0.0)
            'perm_impact_1s_p95_bps': _f(row[12], 0.0)
            'realized_spread_1s_neg_share': _f(row[13], 0.0)
        })
    return rows


def _row_key(row: Dict[str, Any]) -> str:
    return ':'.join([
        str(row.get('sym') or '')
        str(row.get('venue') or '')
        str(row.get('session') or '')
        str(row.get('tf') or '')
        str(row.get('kind') or '')
        str(row.get('side') or '')
    ])


def _top_rows(rows: Sequence[Dict[str, Any]], *, field: str, reverse: bool, top_n: int) -> List[Dict[str, Any]]:
    ranked = sorted(rows, key=lambda r: (_f(r.get(field), 0.0), _i(r.get('n'), 0)), reverse=reverse)
    out: List[Dict[str, Any]] = []
    for row in ranked[: max(1, int(top_n))]:
        out.append({
            'key': _row_key(row)
            'value': _f(row.get(field), 0.0)
            'n': _i(row.get('n'), 0)
            'sym': str(row.get('sym') or '')
            'venue': str(row.get('venue') or '')
            'session': str(row.get('session') or '')
            'tf': str(row.get('tf') or '')
            'kind': str(row.get('kind') or '')
            'side': str(row.get('side') or '')
        })
    return out


def build_report(*, rows_24h: Sequence[Dict[str, Any]], rows_7d: Sequence[Dict[str, Any]], thresholds: Dict[str, float], top_n: int) -> Dict[str, Any]:
    def _total_rows(rows: Sequence[Dict[str, Any]]) -> int:
        return int(sum(max(0, _i(r.get('n'), 0)) for r in rows))

    max_is = _f(thresholds.get('max_is_p95_bps'), 0.0)
    max_imp = _f(thresholds.get('max_perm_impact_p95_bps'), 0.0)
    min_rs = _f(thresholds.get('min_realized_spread_p50_bps'), -999.0)
    max_eff = _f(thresholds.get('max_eff_spread_p95_bps'), 0.0)

    def _breach_counts(rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
        return {
            'is_p95': int(sum(1 for r in rows if _f(r.get('is_p95_bps'), 0.0) >= max_is)) if max_is > 0 else 0
            'perm_impact_p95': int(sum(1 for r in rows if _f(r.get('perm_impact_1s_p95_bps'), 0.0) >= max_imp)) if max_imp > 0 else 0
            'realized_spread_p50': int(sum(1 for r in rows if _f(r.get('realized_spread_1s_p50_bps'), 0.0) <= min_rs))
            'eff_spread_p95': int(sum(1 for r in rows if _f(r.get('eff_spread_p95_bps'), 0.0) >= max_eff)) if max_eff > 0 else 0
        }

    b24 = _breach_counts(rows_24h)
    b7 = _breach_counts(rows_7d)

    report = {
        'schema_name': 'tca_nightly_report'
        'schema_version': 1
        'thresholds': dict(thresholds)
        'summary': {
            'rows_24h_total': _total_rows(rows_24h)
            'rows_7d_total': _total_rows(rows_7d)
            'groups_24h': int(len(rows_24h))
            'groups_7d': int(len(rows_7d))
            'breach_groups_24h': b24
            'breach_groups_7d': b7
        }
        'top_offenders_24h': {
            'is_p95_bps': _top_rows(rows_24h, field='is_p95_bps', reverse=True, top_n=top_n)
            'perm_impact_1s_p95_bps': _top_rows(rows_24h, field='perm_impact_1s_p95_bps', reverse=True, top_n=top_n)
            'eff_spread_p95_bps': _top_rows(rows_24h, field='eff_spread_p95_bps', reverse=True, top_n=top_n)
            'adverse_realized_spread_1s_p50_bps': _top_rows(rows_24h, field='realized_spread_1s_p50_bps', reverse=False, top_n=top_n)
        }
        'top_offenders_7d': {
            'is_p95_bps': _top_rows(rows_7d, field='is_p95_bps', reverse=True, top_n=top_n)
            'perm_impact_1s_p95_bps': _top_rows(rows_7d, field='perm_impact_1s_p95_bps', reverse=True, top_n=top_n)
            'eff_spread_p95_bps': _top_rows(rows_7d, field='eff_spread_p95_bps', reverse=True, top_n=top_n)
            'adverse_realized_spread_1s_p50_bps': _top_rows(rows_7d, field='realized_spread_1s_p50_bps', reverse=False, top_n=top_n)
        }
    }
    return report


def build_summary_state(*, report: Dict[str, Any], now_ms: int, dur_ms: int, ok: bool) -> Dict[str, str]:
    s = dict((report or {}).get('summary') or {})
    top24 = dict((report or {}).get('top_offenders_24h') or {})

    def _top_value(name: str) -> float:
        rows = list(top24.get(name) or [])
        if not rows:
            return 0.0
        return _f(rows[0].get('value'), 0.0)

    return {
        'schema_name': 'tca_nightly_report_state'
        'schema_version': '1'
        'updated_ts_ms': str(int(now_ms))
        'dur_ms': str(int(dur_ms))
        'ok': '1' if ok else '0'
        'rows_24h_total': str(_i(s.get('rows_24h_total'), 0))
        'rows_7d_total': str(_i(s.get('rows_7d_total'), 0))
        'groups_24h': str(_i(s.get('groups_24h'), 0))
        'groups_7d': str(_i(s.get('groups_7d'), 0))
        'breach_is_p95_24h': str(_i(((s.get('breach_groups_24h') or {}).get('is_p95')), 0))
        'breach_perm_impact_p95_24h': str(_i(((s.get('breach_groups_24h') or {}).get('perm_impact_p95')), 0))
        'breach_realized_spread_p50_24h': str(_i(((s.get('breach_groups_24h') or {}).get('realized_spread_p50')), 0))
        'breach_eff_spread_p95_24h': str(_i(((s.get('breach_groups_24h') or {}).get('eff_spread_p95')), 0))
        'worst_is_p95_bps_24h': f"{_top_value('is_p95_bps'):.6f}"
        'worst_perm_impact_p95_bps_24h': f"{_top_value('perm_impact_1s_p95_bps'):.6f}"
        'worst_realized_spread_p50_bps_24h': f"{_top_value('adverse_realized_spread_1s_p50_bps'):.6f}"
        'worst_eff_spread_p95_bps_24h': f"{_top_value('eff_spread_p95_bps'):.6f}"
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description='Nightly TCA report bundle')
    ap.add_argument('--dsn', default=pick_dsn())
    ap.add_argument('--redis-url', default=os.getenv('REDIS_URL') or os.getenv('CRYPTO_NOTIFY_REDIS_URL') or '')
    ap.add_argument('--table', default=_safe_ident(os.getenv('TCA_NIGHTLY_SOURCE_TABLE', 'tca_fill_metrics'), 'tca_fill_metrics'))
    ap.add_argument('--min-rows', type=int, default=_env_int('TCA_NIGHTLY_MIN_ROWS_PER_GROUP', '8'))
    ap.add_argument('--lookback-24h', type=int, default=_env_int('TCA_NIGHTLY_LOOKBACK_24H', '24'))
    ap.add_argument('--lookback-7d', type=int, default=_env_int('TCA_NIGHTLY_LOOKBACK_7D_H', '168'))
    ap.add_argument('--top-n', type=int, default=_env_int('TCA_NIGHTLY_TOP_N', '10'))
    ap.add_argument('--status-path', default=os.getenv('TCA_NIGHTLY_STATUS_PATH', '/tmp/tca_nightly_report_status.json'))
    ap.add_argument('--report-path', default=os.getenv('TCA_NIGHTLY_REPORT_PATH', '/tmp/tca_nightly_report.json'))
    ap.add_argument('--state-key', default=os.getenv('TCA_NIGHTLY_REPORT_STATE_KEY', 'state:tca_nightly_report:last'))
    args = ap.parse_args(list(argv) if argv is not None else None)

    t0 = _now_ms()
    status: Dict[str, Any] = {'ts_ms': t0, 'ok': False, 'dur_ms': 0}
    dsn = str(args.dsn or '').strip()
    if not dsn:
        status['reason'] = 'missing_dsn'
        _write_json_atomic(args.status_path, status)
        return 2

    conn = _pg_connect(dsn)
    if conn is None:
        status['reason'] = 'db_connect_failed'
        _write_json_atomic(args.status_path, status)
        return 1

    try:
        cur = conn.cursor()
        rows_24h = _fetch_rollups(cur, table=args.table, lookback_h=int(args.lookback_24h), min_rows=int(args.min_rows))
        rows_7d = _fetch_rollups(cur, table=args.table, lookback_h=int(args.lookback_7d), min_rows=int(args.min_rows))
        thresholds = {
            'max_is_p95_bps': _env_float('TCA_REPORT_MAX_IS_P95_BPS', '12.0')
            'max_perm_impact_p95_bps': _env_float('TCA_REPORT_MAX_PERM_IMPACT_P95_BPS', '8.0')
            'min_realized_spread_p50_bps': _env_float('TCA_REPORT_MIN_REALIZED_SPREAD_P50_BPS', '-1.0')
            'max_eff_spread_p95_bps': _env_float('TCA_REPORT_MAX_EFF_SPREAD_P95_BPS', '6.0')
        }
        report = build_report(rows_24h=rows_24h, rows_7d=rows_7d, thresholds=thresholds, top_n=int(args.top_n))
        now_ms = _now_ms()
        dur_ms = max(0, now_ms - t0)
        status = {
            'ts_ms': now_ms
            'ok': True
            'dur_ms': dur_ms
            'rows_24h_total': int((report.get('summary') or {}).get('rows_24h_total', 0))
            'groups_24h': int((report.get('summary') or {}).get('groups_24h', 0))
        }
        _write_json_atomic(args.report_path, report)
        _write_json_atomic(args.status_path, status)

        r = _connect_redis(str(args.redis_url or ''))
        if r is not None:
            summary = build_summary_state(report=report, now_ms=now_ms, dur_ms=dur_ms, ok=True)
            try:
                r.hset(str(args.state_key), mapping=summary)
                r.expire(str(args.state_key), 60 * 60 * 48)
                r.set('state:tca_nightly_report:last_ok_ts_ms', str(now_ms), ex=60 * 60 * 48)
                r.set('state:tca_nightly_report:last_dur_ms', str(dur_ms), ex=60 * 60 * 48)
                r.set('state:tca_nightly_report:last_ok', '1', ex=60 * 60 * 48)
            except Exception:
                pass
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    raise SystemExit(main())
