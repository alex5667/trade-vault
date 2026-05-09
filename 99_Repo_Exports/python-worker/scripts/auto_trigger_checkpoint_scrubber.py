#!/usr/bin/env python3
from __future__ import annotations

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""Auto-trigger checkpoint scrubber from health report.

P3.3-autonomy: reads the latest execution health JSON report,
decides whether to trigger the checkpoint scrubber (and optionally
the retention-guard quarantine policy), then writes
``latest_auto_scrubber.json`` to the report directory.

Decision logic
--------------
- overall_status == 'critical'  → trigger
- overall_status == 'warning'   → trigger (if --trigger-on-warning)
- retention_guard.breached_checkpoints > 0  → trigger + quarantine
- consistency.critical_mismatches > 0       → trigger

Runs under the ``trade-execution-auto-scrubber.timer`` every 10 min.

ENV
---
  REDIS_URL                        (default redis://localhost:6379/0)
  EXECUTION_JOURNAL_DSN            (optional SQL fallback DSN)
  EXECUTION_HEALTH_REPORT_PATH     (default RUNBOOK_REPORT_DIR/latest_execution_health.json)
  RUNBOOK_REPORT_DIR               (default /var/lib/trade-runbook/reports)
  EXEC_AUTONOMY_TRIGGER_ON_WARNING (default 1; set 0 to disable)
  EXEC_RETENTION_GUARD_QUARANTINE_ENABLE (default 1; set 0 to disable)
  EXEC_STREAM                      (default orders:exec)
  EXEC_REPLAY_CHECKPOINT_KEY_PREFIX (default orders:exec:replay:cursor:)
  ORDERS_STATE_KEY_PREFIX          (default orders:state:)
  ORDERS_QUARANTINE_PREFIX         (default orders:quarantine:state:)
  EXECUTION_QUARANTINE_LEDGER_DSN  (falls back to EXECUTION_JOURNAL_DSN)
  EXEC_REPLAY_SCAN_COUNT           (default 20000)
  EXEC_REPLAY_CHECKPOINT_SCRUB_SAMPLE_LIMIT (default 5000)
  EXEC_REPLAY_RETENTION_GUARD_SAMPLE_LIMIT  (default 2000)
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

# Allow direct execution without installing the package
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import scrub_replay_checkpoints as scrubber

try:
    import apply_retention_guard_quarantine as retention_quarantine
except Exception:  # pragma: no cover
    retention_quarantine = None  # type: ignore


def should_trigger(report: dict[str, Any], *, trigger_on_warning: bool = True) -> dict[str, Any]:
    """Decide whether to trigger scrub + quarantine from the health report dict.

    Returns a dict with 'trigger' (bool) and 'reasons' (list[str]).

    The decision is deterministic and stateless – it is based solely on the
    fields present in the health snapshot so that tests can exercise it without
    connecting to external services.
    """
    overall = (report.get('overall_status') or 'unknown')
    consistency = dict(report.get('consistency') or {})
    retention = dict(report.get('retention_guard') or {})

    reasons = []
    if overall == 'critical':
        reasons.append('overall_critical')
    elif trigger_on_warning and overall == 'warning':
        reasons.append('overall_warning')
    if int(retention.get('breached_checkpoints') or 0) > 0:
        reasons.append('retention_guard_breached')
    if int(consistency.get('critical_mismatches') or 0) > 0:
        reasons.append('critical_mismatches')

    return {'trigger': bool(reasons), 'reasons': reasons}


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Trigger replay checkpoint scrubber automatically from health report.'
    )
    parser.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    parser.add_argument('--journal-dsn', default=os.getenv('EXECUTION_JOURNAL_DSN', ''))
    parser.add_argument(
        '--health-report',
        default=os.getenv(
            'EXECUTION_HEALTH_REPORT_PATH',
            os.path.join(os.getenv('RUNBOOK_REPORT_DIR', '/var/lib/trade-runbook/reports'), 'latest_execution_health.json'),
        )
    )
    parser.add_argument('--report-dir', default=os.getenv('RUNBOOK_REPORT_DIR', '/var/lib/trade-runbook/reports'))
    parser.add_argument(
        '--trigger-on-warning',
        action='store_true',
        default=os.getenv('EXEC_AUTONOMY_TRIGGER_ON_WARNING', '1') != '0',
    )
    parser.add_argument(
        '--apply-retention-quarantine',
        action='store_true',
        default=os.getenv('EXEC_RETENTION_GUARD_QUARANTINE_ENABLE', '1') != '0',
    )
    args = parser.parse_args()

    import redis  # type: ignore
    r = redis.from_url(args.redis_url, decode_responses=True)

    health_path = Path(args.health_report)
    report = json.loads(health_path.read_text(encoding='utf-8')) if health_path.exists() else {}
    decision = should_trigger(report, trigger_on_warning=bool(args.trigger_on_warning))

    out: dict[str, Any] = {
        'checked_at_ms': get_ny_time_millis(),
        'health_report': str(health_path),
        'decision': decision,
        'scrub_report': None,
        'retention_quarantine_report': None,
    }

    if decision['trigger']:
        out['scrub_report'] = scrubber.run_scrub(
            r,
            exec_stream=os.getenv('EXEC_STREAM', RS.ORDERS_EXEC),
            checkpoint_prefix=os.getenv('EXEC_REPLAY_CHECKPOINT_KEY_PREFIX', 'orders:exec:replay:cursor:'),
            state_prefix=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'),
            journal_dsn=args.journal_dsn,
            scan_count=int(os.getenv('EXEC_REPLAY_SCAN_COUNT', '20000')),
            sample_limit=int(os.getenv('EXEC_REPLAY_CHECKPOINT_SCRUB_SAMPLE_LIMIT', '5000')),
            dry_run=False,
        )
        if (
            args.apply_retention_quarantine
            and retention_quarantine is not None
            and 'retention_guard_breached' in decision['reasons']
        ):
            out['retention_quarantine_report'] = retention_quarantine.run_policy(
                r,
                exec_stream=os.getenv('EXEC_STREAM', RS.ORDERS_EXEC),
                checkpoint_prefix=os.getenv('EXEC_REPLAY_CHECKPOINT_KEY_PREFIX', 'orders:exec:replay:cursor:'),
                state_prefix=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'),
                journal_dsn=args.journal_dsn,
                quarantine_prefix=os.getenv('ORDERS_QUARANTINE_PREFIX', 'orders:quarantine:state:'),
                ledger_dsn=os.getenv('EXECUTION_QUARANTINE_LEDGER_DSN', args.journal_dsn),
                sample_limit=int(os.getenv('EXEC_REPLAY_RETENTION_GUARD_SAMPLE_LIMIT', '2000')),
                scan_count=int(os.getenv('EXEC_REPLAY_SCAN_COUNT', '20000')),
                dry_run=False,
            )

    out_path = Path(args.report_dir) / 'latest_auto_scrubber.json'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
