#!/usr/bin/env python3
from __future__ import annotations
"""P4.7/P4.9/P5X: Build merged operator score from execution + replay + risk-engine canary reports.

Combines:
  - latest_canary_scoring.json       (execution canary, weight 40%)
  - latest_replay_slo_summary.json   (replay SLO, weight 30%)
  - latest_risk_engine_canary.json   (risk-engine canary, weight 30%)

Applies penalties for:
  - risk_consistency mismatch_rate (up to -10 pts)
  - health_status warning (-5 pts) or critical (-15 pts)
  - P4.9 mismatch_penalty from materialized summary (quarantine_count * 2 + avg_rate * 100, capped)
  - P5X archive_consistency_penalty from SQL archive consistency check (mismatch_count * 2.5, up to -10 pts)

Output: latest_operator_score.json served at /api/operator-score/latest

Environment variables
---------------------
RUNBOOK_REPORT_DIR          – directory to read input reports from
OPERATOR_SCORE_REPORT_PATH  – output file path

Usage
-----
  python3 scripts/build_operator_score_report.py
  python3 scripts/build_operator_score_report.py --report-dir /tmp/reports --out /tmp/score.json
"""

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict


def _load_json(path: Path) -> Dict[str, Any]:
    """Load JSON from path; return empty dict on any error."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _bucket(score: float) -> str:
    """Classify score into green/yellow/red canary bucket."""
    if score >= 90:
        return 'green'
    if score >= 75:
        return 'yellow'
    return 'red'


def _replay_score(doc: Dict[str, Any]) -> float:
    """Derive a 0-100 score from the replay SLO summary document.

    Prefers the 24h window if available.  Penalises:
      - truncation rate  (up to -30 pts)
      - retention guard  (up to -35 pts)
      - p95 replay latency over 50 ms  (up to -20 pts)
    """
    items = list(doc.get('items') or [])
    target = next((x for x in items if str(x.get('window_name')) == '24h'), items[0] if items else {})
    if not target:
        return 50.0  # no data → neutral score
    total = float(target.get('rehydrate_total') or 0.0)
    truncated = float(target.get('replay_truncated_total') or 0.0)
    retention = float(target.get('retention_guard_total') or 0.0)
    p95 = float(target.get('replay_latency_p95_ms') or 0.0)
    score = 100.0
    if total > 0:
        score -= min((truncated / total) * 100.0, 30.0)
        score -= min((retention / total) * 150.0, 35.0)
    score -= min(max(0.0, p95 - 50.0) * 0.2, 20.0)
    return max(0.0, score)


def _extract_mismatch_penalty(doc: Dict[str, Any]) -> Dict[str, float]:
    """Derive a mismatch penalty from the latest_risk_mismatch_summary.json report.

    Prefers the 24h/ALL window row.  Falls back to averaging all 24h rows.

    Penalty formula (capped to avoid over-penalisation):
      penalty = min(quarantine_count * 2, 12) + min(avg_rate * 100, 12)
    """
    rows = list(doc.get('rows') or [])
    target = next(
        (x for x in rows if str(x.get('window_name')) == '24h' and str(x.get('tier') or '').upper() == 'ALL'),
        None,
    )
    if target is None:
        target_rows = [x for x in rows if str(x.get('window_name')) == '24h']
        if target_rows:
            avg_q = sum(float(x.get('quarantine_count') or 0.0) for x in target_rows)
            avg_r = sum(float(x.get('avg_mismatch_rate') or 0.0) for x in target_rows) / len(target_rows)
        else:
            avg_q = 0.0
            avg_r = 0.0
    else:
        avg_q = float(target.get('quarantine_count') or 0.0)
        avg_r = float(target.get('avg_mismatch_rate') or 0.0)
    penalty = min(avg_q * 2.0, 12.0) + min(avg_r * 100.0, 12.0)
    return {
        'mismatch_quarantine_count_24h': avg_q,
        'mismatch_avg_rate_24h': avg_r,
        'mismatch_penalty': round(penalty, 2),
    }


def build_report(report_dir: Path) -> Dict[str, Any]:
    """Build the merged operator score report from all sub-reports in *report_dir*."""
    execution = _load_json(report_dir / 'latest_canary_scoring.json')
    replay = _load_json(report_dir / 'latest_replay_slo_summary.json')
    risk = _load_json(report_dir / 'latest_risk_engine_canary.json')
    health = _load_json(report_dir / 'latest_execution_health.json')
    risk_consistency = _load_json(report_dir / 'latest_risk_signal_consistency.json')
    # P4.9: load materialized mismatch summary for penalty computation
    mismatch_summary = _load_json(report_dir / 'latest_risk_mismatch_summary.json')
    # P5X: load archive consistency report for additional penalty
    archive_consistency = _load_json(report_dir / 'latest_risk_mismatch_archive_consistency.json')

    execution_score = float(execution.get('score') or 50.0)
    replay_score = _replay_score(replay)
    risk_score = float(risk.get('score') or 50.0)
    mismatch_rate = float(risk_consistency.get('mismatch_rate') or 0.0)
    # P4.9: extract mismatch penalty fields from materialized summary
    summary = _extract_mismatch_penalty(mismatch_summary)
    # P5X: compute archive consistency penalty (mismatch_count * 2.5, capped at 10)
    archive_mismatch_penalty = min(float(archive_consistency.get('mismatch_count') or 0.0) * 2.5, 10.0)

    # Weighted combination of component scores
    score = execution_score * 0.40 + replay_score * 0.30 + risk_score * 0.30

    # Penalty: risk consistency mismatch rate (0..1 → up to -10 pts)
    score -= min(mismatch_rate * 100.0, 10.0)
    # P4.9 penalty: aggregate mismatch drift from materialized summary
    score -= float(summary.get('mismatch_penalty') or 0.0)
    # P5X penalty: SQL archive consistency divergence
    score -= archive_mismatch_penalty

    # Penalty: health status degradation
    status = str(health.get('overall_status') or 'ok')
    if status == 'warning':
        score -= 5.0
    elif status == 'critical':
        score -= 15.0

    score = max(0.0, min(100.0, score))

    return {
        'execution_score': round(execution_score, 2),
        'replay_score': round(replay_score, 2),
        'risk_score': round(risk_score, 2),
        'risk_mismatch_rate': mismatch_rate,
        'health_status': status,
        # P4.9: spread mismatch penalty fields into output for observability
        **summary,
        # P5X: archive consistency penalty fields
        'archive_consistency_mismatch_count': int(archive_consistency.get('mismatch_count') or 0),
        'archive_consistency_penalty': round(archive_mismatch_penalty, 2),
        'score': round(score, 2),
        'bucket': _bucket(score),
    },


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Build merged operator score from execution + replay + risk-engine reports.',
    ),
    parser.add_argument(
        '--report-dir',
        default=os.getenv('RUNBOOK_REPORT_DIR', '/var/lib/trade-runbook/reports'),
        help='Directory containing input report JSON files',
    ),
    parser.add_argument(
        '--out',
        default=os.getenv(
            'OPERATOR_SCORE_REPORT_PATH',
            '/var/lib/trade-runbook/reports/latest_operator_score.json',
        ),
        help='Output path for the merged operator score JSON',
    )
    args = parser.parse_args()

    report = build_report(Path(args.report_dir))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
