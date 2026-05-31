#!/usr/bin/env python3
from __future__ import annotations

"""Risk-engine quality canary report builder (P4.5).

Queries risk_decisions over a configurable time window and computes a
composite quality score (0–100). Writes the result to a JSON report that is
served by the runbook server at /api/risk-canary/latest.

Scoring formula (deductions from 100):
  - tighten_severity = (1 - avg_clamp) × clamp_rate  (magnitude × frequency)
    × 25  (capped at 20 pts): measures actual position size reduction severity.
    Zero when no clamping (avg_clamp→1.0); max when all positions are zeroed.
    Replaces old binary `clamp_rate × 60` which over-penalized conservative
    uniform tightening on small accounts (deny_rate=0 but avg_clamp=0.07).
  - conf_deny_rate   × 200 (capped at 25 pts): high confidence denial → signals quality issue
  - deny_rate        × 80  (capped at 30 pts): high deny rate → signal quality or limit mis-config
  - avg latency > 20ms × 0.5 (capped at 20 pts): high latency → engine perf issue
  + avg_clamp > 0.90 bonus (up to 5 pts): very light tightening (<10% reduction)

Buckets:
  green  ≥ 90 (healthy)
  yellow ≥ 75 (degraded)
  red    <  75 (critical)

ENV vars:
  RISK_AUDIT_SQL_DSN          — Postgres DSN (fallback: EXECUTION_JOURNAL_DSN)
  RISK_CANARY_LOOKBACK_HOURS  — lookback window in hours (default 24)
  RISK_CANARY_REPORT_PATH     — output file path
"""

import argparse
import json
import os
from pathlib import Path

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore


def _bucket(score: float) -> str:
    """Map a 0–100 score to a traffic-light bucket string."""
    if score >= 90:
        return 'green'
    if score >= 75:
        return 'yellow'
    return 'red'


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Build risk-engine quality canary report from SQL audit tables.'
    )
    parser.add_argument(
        '--dsn',
        default=os.getenv('RISK_AUDIT_SQL_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '')),
    )
    parser.add_argument(
        '--hours',
        type=int,
        default=int(os.getenv('RISK_CANARY_LOOKBACK_HOURS', '24')),
    )
    parser.add_argument(
        '--out',
        default=os.getenv(
            'RISK_CANARY_REPORT_PATH',
            '/var/lib/trade-runbook/reports/latest_risk_engine_canary.json',
        )
    )
    args = parser.parse_args()

    if not args.dsn or psycopg is None:
        raise RuntimeError('psycopg + RISK_AUDIT_SQL_DSN required')

    import time
    max_retries = 3
    base_delay = 2.0
    row = None

    for attempt in range(max_retries):
        try:
            with psycopg.connect(args.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        with base as (
                          select * from risk_decisions
                          where ts >= (now() - (%s * interval '1 hour'))
                        )
                        select
                          count(*) as total_decisions,
                          coalesce(sum(case when clamp_ratio < 0.999 then 1 else 0 end), 0) as clamp_count,
                          coalesce(sum(case when jsonb_path_exists(reasons_jsonb, '$[*] ? (@ == "confidence_below_tier_floor")') then 1 else 0 end), 0) as confidence_denials,
                          coalesce(sum(case when allow_trade_publish then 1 else 0 end), 0) as allow_count,
                          coalesce(sum(case when not allow_trade_publish then 1 else 0 end), 0) as deny_count,
                          coalesce(avg(decision_latency_ms), 0) as decision_latency_avg_ms,
                          coalesce(avg(clamp_ratio), 1.0) as avg_clamp_ratio
                        from base
                        """,
                        (args.hours,),
                    )
                    row = cur.fetchone() or (0, 0, 0, 0, 0, 0.0, 1.0)
            break
        except psycopg.OperationalError as e:
            if attempt == max_retries - 1:
                print(f"Canary report failed (DB unavailable after {max_retries} attempts): {e}")
                return 1
            # Silently delay on early transient attempts
            time.sleep(base_delay)
            base_delay *= 2.0

    total, clamp_count, conf_denials, allow_count, deny_count, avg_lat, avg_clamp = row
    clamp_rate = (float(clamp_count) / float(total)) if total else 0.0
    deny_rate = (float(deny_count) / float(total)) if total else 0.0
    conf_rate = (float(conf_denials) / float(total)) if total else 0.0

    # Composite score: start at 100, apply deductions
    score = 100.0
    # Tightening severity: fraction of position removed × frequency.
    # avg_clamp is the fraction of requested notional KEPT (1.0=no clamp, 0.0=full clamp).
    # tighten_severity = 0 when no clamping; = 1.0 when 100% of every position is removed.
    tighten_severity = clamp_rate * max(0.0, 1.0 - float(avg_clamp))
    score -= min(tighten_severity * 25.0, 20.0)
    score -= min(conf_rate * 200.0, 25.0)
    score -= min(deny_rate * 80.0, 30.0)
    score -= min(max(0.0, float(avg_lat) - 20.0) * 0.5, 20.0)
    # Bonus: avg_clamp > 0.90 means less than 10% reduction — very light tightening
    score += min(max(0.0, (float(avg_clamp) - 0.9)) * 50.0, 5.0)

    report = {
        'window_hours': int(args.hours),
        'total_decisions': int(total),
        'allow_count': int(allow_count),
        'deny_count': int(deny_count),
        'clamp_count': int(clamp_count),
        'confidence_denials': int(conf_denials),
        'clamp_rate': float(clamp_rate),
        'deny_rate': float(deny_rate),
        'confidence_denial_rate': float(conf_rate),
        'decision_latency_avg_ms': float(avg_lat),
        'avg_clamp_ratio': float(avg_clamp),
        'score': round(max(0.0, min(100.0, score)), 2),
        'bucket': _bucket(score),
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
