#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""P5X: Auto-silence repeated risk-drift mismatch storms in Alertmanager.

Reads latest_risk_mismatch_summary.json, evaluates the 24h window
quarantine count and avg mismatch rate against configurable thresholds.
If either threshold is exceeded, creates an Alertmanager silence for
domain="risk-drift" alerts to suppress storm noise.

Default mode is dry-run (RISK_DRIFT_AUTOSILENCE_DRY_RUN=1) — no API call is made,
only the decision report is written.

Environment variables
---------------------
RUNBOOK_REPORT_DIR                        – directory with latest_risk_mismatch_summary.json
ALERTMANAGER_EXTERNAL_URL                 – Alertmanager base URL (default: http://alertmanager:9093)
RISK_DRIFT_AUTOSILENCE_QUARANTINE_THRESHOLD – quarantine_count threshold (default: 10)
RISK_DRIFT_AUTOSILENCE_MISMATCH_RATE_THRESHOLD – avg_mismatch_rate threshold (default: 0.20)
RISK_DRIFT_AUTOSILENCE_DURATION_SEC       – silence duration in seconds (default: 3600)
RISK_DRIFT_AUTOSILENCE_DRY_RUN            – 1 = dry run only (default: 1)
RISK_DRIFT_AUTOSILENCE_REPORT_PATH        – output JSON report path

Usage
-----
  python3 scripts/auto_silence_risk_drift_storm.py
  python3 scripts/auto_silence_risk_drift_storm.py --dry-run
  python3 scripts/auto_silence_risk_drift_storm.py --alertmanager-url http://alertmanager:9093 --duration-sec 7200
"""

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Dict
from urllib import request


def _load_json(path: Path) -> Dict[str, Any]:
    """Load JSON from path; return empty dict on any error."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _write_atomic(path: Path, payload: str) -> None:
    """Atomically write payload to path via a temp-file rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(payload, encoding='utf-8')
    tmp.replace(path)


def decide_autosilence(
    summary: Dict[str, Any],
    *,
    quarantine_threshold: int,
    mismatch_rate_threshold: float,
) -> Dict[str, Any]:
    """Evaluate whether a silence should be triggered.

    Examines only the 24h window rows of the mismatch summary.
    Triggers if:
      - total quarantine_count >= quarantine_threshold, OR
      - max avg_mismatch_rate >= mismatch_rate_threshold
    """
    rows = list(summary.get('rows') or [])
    rows24 = [r for r in rows if str(r.get('window_name')) == '24h']
    quarantine_count = sum(int(r.get('quarantine_count') or 0) for r in rows24)
    max_avg_rate = max(
        [float(r.get('avg_mismatch_rate') or 0.0) for r in rows24] or [0.0]
    )
    should = quarantine_count >= quarantine_threshold or max_avg_rate >= mismatch_rate_threshold
    return {
        'should_silence': should,
        'quarantine_count_24h': quarantine_count,
        'max_avg_mismatch_rate_24h': max_avg_rate,
        'reason': 'storm' if should else 'below_threshold',
    }


def create_silence(
    base_url: str, duration_sec: int, created_by: str, comment: str
) -> Dict[str, Any]:
    """Create an Alertmanager silence for domain="risk-drift" alerts.

    Returns the request payload and the Alertmanager API response body.
    """
    starts = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    ends = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(time.time() + duration_sec))
    payload = {
        'matchers': [
            {'name': 'domain', 'value': 'risk-drift', 'isRegex': False},
        ],
        'startsAt': starts,
        'endsAt': ends,
        'createdBy': created_by,
        'comment': comment,
    }
    req = request.Request(
        base_url.rstrip('/') + '/api/v2/silences',
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
    )
    with request.urlopen(req, timeout=10) as resp:  # nosec
        body = resp.read().decode('utf-8')
    return {'request': payload, 'response': json.loads(body) if body else {}}


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Auto-silence repeated risk-drift mismatch storms in Alertmanager.'
    )
    parser.add_argument(
        '--report-dir',
        default=os.getenv('RUNBOOK_REPORT_DIR', '/var/lib/trade-runbook/reports'),
    )
    parser.add_argument(
        '--alertmanager-url',
        default=os.getenv('ALERTMANAGER_EXTERNAL_URL', 'http://alertmanager:9093'),
    )
    parser.add_argument(
        '--quarantine-threshold',
        type=int,
        default=int(os.getenv('RISK_DRIFT_AUTOSILENCE_QUARANTINE_THRESHOLD', '10')),
    )
    parser.add_argument(
        '--mismatch-rate-threshold',
        type=float,
        default=float(os.getenv('RISK_DRIFT_AUTOSILENCE_MISMATCH_RATE_THRESHOLD', '0.20')),
    )
    parser.add_argument(
        '--duration-sec',
        type=int,
        default=int(os.getenv('RISK_DRIFT_AUTOSILENCE_DURATION_SEC', '3600')),
    )
    # Default dry-run=True unless explicitly disabled
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=os.getenv('RISK_DRIFT_AUTOSILENCE_DRY_RUN', '1') not in {'0', 'false', 'False'},
    )
    parser.add_argument(
        '--out',
        default=os.getenv(
            'RISK_DRIFT_AUTOSILENCE_REPORT_PATH',
            '/var/lib/trade-runbook/reports/latest_risk_drift_autosilence.json',
        ),
    )
    args = parser.parse_args()

    summary = _load_json(Path(args.report_dir) / 'latest_risk_mismatch_summary.json')
    decision = decide_autosilence(
        summary,
        quarantine_threshold=args.quarantine_threshold,
        mismatch_rate_threshold=args.mismatch_rate_threshold,
    )
    out: Dict[str, Any] = {
        'generated_at_ms': get_ny_time_millis(),
        **decision,
        'dry_run': bool(args.dry_run),
    }

    if decision['should_silence'] and not args.dry_run:
        try:
            out['silence'] = create_silence(
                args.alertmanager_url,
                args.duration_sec,
                'trade-risk-drift-autosilence',
                'Auto silence for repeated risk-drift mismatch storm',
            )
        except Exception as exc:
            out['silence_error'] = str(exc)

    _write_atomic(Path(args.out), json.dumps(out, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
